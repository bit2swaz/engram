# Architecture

## Overview

engram is an asynchronous semantic memory backend for LLM-powered agents, written in Rust. It provides short-term, long-term, and core memory for agents, enabling context assembly with strict token budgeting, semantic search, and transparent memory management.

The system is designed for performance, reliability, and developer control, with all major components behind trait abstractions for easy swapping and testing.

## Data flow

in cluster mode, writes go through Raft consensus before reaching the stores. in standalone mode (NODE_ID not set), the Raft layer is absent and write handlers reach the stores directly.

reads (context, search) always go directly to the local stores on whichever node receives the request.

```mermaid
graph TD
    client["ai agent / user"] -->|rest json| axum["axum http server"]
    axum --> router["router"]

    router --> sessionhandler["session handler"]
    router --> memoryhandler["message handler"]
    router --> contexthandler["context handler"]
    router --> searchhandler["search handler"]
    router --> corememhandler["core memory handler"]
    router --> healthhandler["health handler"]

    memoryhandler -->|write| raft["raft consensus\nopenraft 0.9\ncluster mode only"]
    corememhandler -->|write| raft
    sessionhandler -->|delete| raft
    raft -.->|grpc append entries| peers["peer nodes (port 9001)"]
    raft -->|state machine apply| shortterm["short-term memory (trait)"]
    raft -->|state machine apply| coremem["core memory store (trait)"]
    raft -->|embedding job| embedqueue["background worker pool (bounded channel)"]

    shortterm --> redis[("redis")]
    shortterm --> inmem["in-memory store (test fallback)"]

    embedqueue -->|generate| embedprovider["embedding provider (trait)"]
    embedprovider -->|https| openai["openai embedding api"]

    embedqueue -->|store vector| longterm["vector store (trait)"]
    longterm --> lancedb[("lancedb")]

    coremem --> redis

    searchhandler --> embedprovider
    searchhandler --> longterm

    contexthandler --> assembler["context assembler"]
    assembler --> shortterm
    assembler --> longterm
    assembler --> coremem
    assembler --> tokencounter["token counter (trait)"]
    tokencounter --> tiktoken["tiktoken (cl100k base)"]
    assembler --> finalcontext["assembled prompt (core + long-term + short-term)"]

    healthhandler -->|200 ok| client

    observability["observability layer (tracing + prometheus)"] -->|metrics + logs| prometheus[("prometheus")]
```

## Core abstractions (traits)

All major components are behind trait abstractions, which lets implementations be swapped out and mocked in tests without changing any calling code.

`EmbeddingProvider` generates text embeddings. In production this calls OpenAI; in tests it returns deterministic fixed vectors.

`VectorStore` handles long-term memory storage and semantic search. The production implementation is LanceDB.

`ShortTermMemory` manages recent message storage, count-based trimming, and embedding status tracking. Redis and in-memory implementations are both available.

`TokenCounter` counts tokens in text. The production implementation uses `tiktoken-rs` with `cl100k_base`.

`CoreMemoryStore` manages pinned session facts. Redis and in-memory implementations are both available.

## Design decisions

| decision | alternatives | final choice & rationale |
|----------|-------------|-------------------------|
| bounded mpsc channel + worker pool for embedding | unbounded channel, inline embedding | bounded channel prevents memory blowup, and a fixed worker pool caps concurrent API calls while keeping request latency low |
| LanceDB over Milvus | Milvus, Pinecone, QDrant | LanceDB is embedded, zero-ops, Rust-native, easy to test and deploy |
| Redis for short-term/core memory | in-memory only, Postgres | Redis is fast, supports TTL, and is widely used for volatile state |
| pair-preserving trim in context assembler | naive trim, no trim | preserves dialogue integrity, prevents LLM hallucinations |
| idempotent embedding jobs | no idempotency | safe retries, prevents duplicate vectors on crash or retry |
| observability from day 1 | add later | tracing and Prometheus from the start for reliability and debugging |

## Context assembly algorithm

- allocate token budget: start with core memory (non-trimmable), then trim short-term messages to fit the remaining budget, then inject long-term memories if space remains
- derive query: use the most recent user message in trimmed short-term, else the last message, else empty
- perform semantic search: embed the query, search LanceDB, filter by similarity threshold, and take top-k
- format: each long-term memory as `Memory: {text}`
- assemble: core memory, long-term memories, then short-term messages

## Security and multi-tenancy

- current: no server-side authentication in the MVP; `OPENAI_API_KEY` is only used for outbound embedding requests
- future: API key authentication, multi-tenant support, and optional auth are planned for production

## Deployment architecture

Docker compose sets up:
- Redis: short-term and core memory
- app: Rust server, reads env vars from `.env`
- Prometheus: metrics scraping
- Grafana: dashboard (optional)

the application reads configuration from environment variables such as `REDIS_URL`, `OPENAI_API_KEY`, `LANCE_DB_PATH`, `SHORT_TERM_COUNT`, `EMBEDDING_MAX_CONCURRENCY`, `MPSC_CHANNEL_SIZE`, `RUST_LOG`, and `LOG_FORMAT`.

## Distributed cluster mode (Stage 1)

In cluster mode, multiple engram nodes form a Raft consensus group using [OpenRaft 0.9](https://github.com/datafuselabs/openraft). Cluster mode is enabled by setting `NODE_ID` in the environment. Without it, the server runs in standalone mode exactly as described above.

### Node anatomy

Each cluster node runs two servers simultaneously:

- an Axum HTTP server (default port 3000) that handles client requests
- a tonic gRPC server (default port 9001) that handles Raft protocol messages (Vote, AppendEntries)

Each node also owns its own Redis instance and LanceDB database. There is no shared storage between nodes.

### Write path

In cluster mode, every write goes through Raft before a response is returned:

1. Client sends a write (add message, add fact, or delete session) to any node
2. If the receiving node is a follower, it returns HTTP 307 with a `Location` header pointing to the leader's HTTP address
3. If the receiving node is the leader, it calls `raft.client_write()` with a `MemoryCommand`
4. OpenRaft replicates the command to a quorum of nodes via gRPC AppendEntries
5. Each node's Raft state machine applies the committed command to its local Redis

```mermaid
flowchart TD
    client["client"]
    follower["follower\nhttp :3000 / grpc :9001"]
    leader["leader\nhttp :3000 / grpc :9001"]
    r1["node 1 redis"]
    r2["node 2 redis"]
    r3["node 3 redis"]
    l1["node 1 lancedb"]
    l2["node 2 lancedb"]
    l3["node 3 lancedb"]

    client -->|"write to follower"| follower
    follower -->|"307 redirect"| client
    client -->|"write to leader"| leader
    leader -->|"raft append entries (grpc)"| r1
    leader -->|"raft append entries (grpc)"| r2
    leader -->|"raft append entries (grpc)"| r3
    r1 -.->|"embed async"| l1
    r2 -.->|"embed async"| l2
    r3 -.->|"embed async"| l3
```

The cluster also exposes management endpoints at `/cluster`, `/cluster/init`, `/cluster/add-learner`, and `/cluster/change-membership`.

### LanceDB consistency

LanceDB is per-node and eventually consistent. When a write is committed through Raft, each node's state machine enqueues an `EmbeddingJob`. Each node's background worker independently calls OpenAI and stores the result in its local LanceDB.

OpenAI text embeddings are deterministic for the same input. All nodes converge to identical vector state, with a timing lag proportional to embedding worker throughput. Semantic search results may be briefly stale on followers. This is a deliberate Stage 1 tradeoff: replicating 1536-float vectors through Raft on every write would be wasteful.

### Raft implementation

| component | description |
|-----------|-------------|
| `EngRaftLogStore` | in-memory BTreeMap log store, implements `RaftLogStorage + RaftLogReader` |
| `EngStateMachineStore` | applies committed `MemoryCommand` entries to Redis; snapshot methods return `Unsupported` |
| `EngRaftNetwork` | factory that creates per-peer gRPC connections using tonic channels |
| `EngRaftNetworkConnection` | sends Vote and AppendEntries RPCs; `send_install_snapshot` returns `Unreachable` |
| `RaftGrpcServer` | tonic service implementation that forwards incoming Raft RPCs to the local `RaftHandle` |

The log store is in-memory. It does not survive restarts. Stage 2 will replace it with a persistent store (sled or RocksDB).

### Prometheus metrics in cluster mode

Four additional metrics are exported when a node is in cluster mode:

| metric | type | description |
|--------|------|-------------|
| `engram_raft_term` | gauge | current Raft term |
| `engram_raft_commit_index` | gauge | index of the last applied log entry |
| `engram_raft_is_leader` | gauge | 1 if this node is the current leader, 0 otherwise |
| `engram_raft_leader_changes_total` | counter | number of leader changes observed by this node |

A background task watches the OpenRaft metrics channel and updates these gauges on every change.

### Stage 2 deferred items

The following are explicitly out of scope for Stage 1:

- **Log snapshotting.** Nodes that fall too far behind the log cannot rejoin automatically. They must be manually removed and re-added. Stage 2 will implement `RaftSnapshotBuilder` and `install_snapshot`.
- **Persistent log store.** Restarting a node clears its Raft log. Stage 2 replaces the in-memory BTreeMap with sled or RocksDB.
- **LanceDB replication.** Stage 2 will route vector storage through Raft so only the leader calls OpenAI, then replicates the embedding payload to followers, eliminating redundant API calls at scale.
- **Multi-tenant auth.** Cluster-aware authentication routing.