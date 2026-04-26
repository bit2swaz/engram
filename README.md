# engram

an asynchronous semantic memory backend for llm agents, written in rust.

[![build status](https://github.com/bit2swaz/engram/actions/workflows/ci.yml/badge.svg)](https://github.com/bit2swaz/engram/actions)
[![license: mit](https://img.shields.io/badge/license-mit-blue.svg)](LICENSE)
[![rust version](https://img.shields.io/badge/rust-1.72%2B-blue)](https://www.rust-lang.org/)

## overview

engram is a backend service for large language model (llm) agents. it provides three types of memory: short-term (recent messages), long-term (semantic vector search), and core memory (pinned facts). the goal is to give llm agents a transparent, efficient, and controllable way to manage context and recall information. engram is written in rust for performance and reliability. it is designed for transparency, with full control over token budgets and context assembly, and exposes all operations via a simple rest api.

engram is built for developers who want to plug in their own llm agents, run locally or in production, and have full visibility into how memory is managed. it is easy to run, test, and extend. all memory operations are behind trait abstractions, making it easy to swap implementations or mock for tests.

## architecture

```mermaid
graph td
    client[ai agent / user] -->|rest json| axiom[axum http server]
    axiom -->|request| router
    router --> sessionhandler[session handler]
    router --> memoryhandler[memory handler]
    memoryhandler -->|add message| shortterm[short-term memory trait]
    memoryhandler -->|embedding job| embedqueue[background embedding task]
    embedqueue -->|generate| embedprovider[embedding provider trait]
    embedprovider -->|call api| openai[openai embeddings]
    embedqueue -->|store vector| longterm[vector store trait]
    longterm --> lancedb[(lancedb)]
    shortterm --> redis[(redis)]
    shortterm --> inmem[in-memory fallback for tests]
    contextassembler[context assembler module] --> shortterm
    contextassembler --> longterm
    contextassembler --> coremem[core memory store]
    contextassembler --> tokencounter[token counter trait]
    contextassembler --> assembledcontext[final prompt string]
    router --> contexthandler[context handler] --> contextassembler
    router --> searchhandler[search handler] --> longterm
    router --> corememhandler[core memory handler] --> coremem
    router --> healthhandler[health handler]
    observability[observability layer] -->|metrics & traces| prometheus[(prometheus)]
```

## quickstart (local)

### prerequisites
- rust (1.72 or newer)
- docker (for redis)
- openai api key

### clone and build
```sh
git clone https://github.com/bit2swaz/engram.git
cd engram
cargo build --release
```

### start redis
```sh
docker run -d --name engram-redis -p 6379:6379 redis:7-alpine
```

### set environment variables
copy `.env.example` to `.env` and fill in your openai api key, or set them manually:
```sh
export openai_api_key=sk-your-key-here
export redis_url=redis://localhost:6379
```

### run the server
```sh
cargo run
```

### example curl commands
create a session:
```sh
curl -X POST http://localhost:3000/sessions
```
add a message:
```sh
curl -X POST http://localhost:3000/sessions/{session_id}/messages \
  -H 'content-type: application/json' \
  -d '{"role":"user","content":"hello, what is rust?"}'
```
get context:
```sh
curl http://localhost:3000/sessions/{session_id}/context
```
search:
```sh
curl -X POST http://localhost:3000/sessions/{session_id}/search \
  -H 'content-type: application/json' \
  -d '{"query":"rust async","top_k":5}'
```
add core memory:
```sh
curl -X PUT http://localhost:3000/sessions/{session_id}/core-memory \
  -H 'content-type: application/json' \
  -d '{"fact":"user prefers dark mode"}'
```
delete session:
```sh
curl -X DELETE http://localhost:3000/sessions/{session_id}
```

## quickstart (docker)

- copy `.env.example` to `.env` and fill in your openai api key
- run:
```sh
docker compose up -d
```
- wait for the health check to pass
- use the same curl examples above (replace `localhost:3000` if needed)

## api overview

| method | path                                 | description                       |
|--------|--------------------------------------|-----------------------------------|
| get    | /health                              | health check                      |
| post   | /sessions                            | create session                    |
| post   | /sessions/{session_id}/messages      | add message                       |
| get    | /sessions/{session_id}/context       | get assembled context             |
| post   | /sessions/{session_id}/search        | semantic search                   |
| put    | /sessions/{session_id}/core-memory   | add core memory fact              |
| delete | /sessions/{session_id}               | delete session                    |

see [api.md](API.md) for full details.

## configuration

all configuration is via environment variables:

| variable                | description                              | default                  |
|-------------------------|------------------------------------------|--------------------------|
| redis_url               | redis connection url                     | redis://localhost:6379   |
| openai_api_key          | openai api key (required)                |                          |
| embedding_model         | openai embedding model                   | text-embedding-3-small   |
| short_term_count        | number of recent messages to keep        | 20                       |
| similarity_threshold    | min similarity for long-term memories    | 0.7                      |
| max_tokens_default      | default max tokens for context           | 8000                     |
| rust_log                | tracing log filter                       | info                     |
| embedding_max_concurrency | max concurrent embedding jobs          | 10                       |
| mpsc_channel_size       | embedding job queue size                 | 1000                     |

## features

- short-term memory (recent messages)
- long-term semantic search (vector store)
- core memory (pinned facts)
- context assembly with token budgeting
- pair-preserving trim for dialogue
- background embedding worker
- idempotency for message ingestion
- prometheus metrics endpoint
- openapi docs and swagger ui
- debug endpoint
- optional authentication (future)

## documentation

- [api.md](API.md)
- [architecture.md](ARCHITECTURE.md)
- [comparison.md](COMPARISON.md)
- [contributing.md](CONTRIBUTING.md)
- [ssot](docs/SSOT.md)

## license

mit license. see [license](LICENSE) for details.
