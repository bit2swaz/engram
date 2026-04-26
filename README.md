# engram

an asynchronous semantic memory backend for llm agents, written in rust.

[![license: mit](https://img.shields.io/badge/license-mit-blue.svg)](LICENSE)
[![rust version](https://img.shields.io/badge/rust-1.92%2B-blue)](https://www.rust-lang.org/)

## overview

engram is a backend service for large language model (LLM) agents. it provides three types of memory: short-term (recent messages), long-term (semantic vector search), and core memory (pinned facts). the goal is to give LLM agents a transparent, efficient, and controllable way to manage context and recall information.

engram is written in rust for performance and reliability. it is designed for transparency, with full control over token budgets and context assembly, and exposes all operations via a simple REST API.

engram is built for developers who want to plug in their own LLM agents, run locally or in production, and have full visibility into how memory is managed. it is easy to run, test, and extend. all memory operations are behind trait abstractions, making it easy to swap implementations or mock for tests.

## architecture

```mermaid
graph TD
    client["ai agent / user"] -->|rest json| axum["axum http server"]
    axum --> router["router"]
    router --> sessionhandler["session handler"]
    router --> memoryhandler["message handler"]
    memoryhandler -->|add message + queue| shortterm["short-term memory trait"]
    shortterm --> redis[("redis<br/>volatile, fast")]
    shortterm --> inmem["in-memory store<br/>test fallback"]
    memoryhandler -->|embedding job<br/>bounded mpsc| embedqueue["background worker<br/>bounded channel + semaphore"]
    embedqueue -->|generate embedding| embedprovider["embedding provider trait"]
    embedprovider -->|https| openai["openai embeddings"]
    embedqueue -->|store vector + metadata| longterm["vector store trait"]
    longterm --> lancedb[("lancedb<br/>persistent ann search")]
    router --> contexthandler["context handler"]
    contexthandler --> assembler["context assembler"]
    assembler --> shortterm
    assembler --> longterm
    assembler --> coremem["core memory store trait"]
    coremem --> redis
    assembler --> tokencounter["token counter trait"]
    tokencounter --> tiktoken["tiktoken<br/>cl100k base"]
    router --> searchhandler["search handler"]
    searchhandler --> embedprovider
    searchhandler --> longterm
    router --> corememhandler["core memory handler"]
    corememhandler --> coremem
    router --> healthhandler["health handler"]
    observability["observability layer<br/>tracing + prometheus"] -->|metrics + logs| prometheus[("prometheus")]
    assembler --> finalcontext["assembled prompt<br/>core + long-term + short-term"]
```

## quickstart (local)

### prerequisites
- Rust (1.92 or newer)
- Docker (for Redis)
- OpenAI API key

### clone and build
```sh
git clone https://github.com/bit2swaz/engram.git
cd engram
cargo build --release
```

### start Redis
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

## quickstart (Docker)

- copy `.env.example` to `.env` and fill in your openai api key
- run:
```sh
docker compose up -d
```
- wait for the health check to pass
- use the same curl examples above (replace `localhost:3000` if needed)

## API overview

| method | path                                 | description                       |
|--------|--------------------------------------|-----------------------------------|
| GET    | /health                              | health check                      |
| POST   | /sessions                            | create session                    |
| POST   | /sessions/{session_id}/messages      | add message                       |
| GET    | /sessions/{session_id}/context       | get assembled context             |
| POST   | /sessions/{session_id}/search        | semantic search                   |
| PUT    | /sessions/{session_id}/core-memory   | add core memory fact              |
| DELETE | /sessions/{session_id}               | delete session                    |

see [API.md](docs/API.md) for full details.

## configuration

all configuration is via environment variables:

| variable                | description                              | default                  |
|-------------------------|------------------------------------------|--------------------------|
| redis_url               | Redis connection url                     | redis://localhost:6379   |
| openai_api_key          | OpenAI API key (required)                |                          |
| embedding_model         | OpenAI embedding model                   | text-embedding-3-small   |
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
- Prometheus metrics endpoint
- OpenAPI docs and Swagger UI
- debug endpoint
- optional authentication (future)

## documentation

- [API.md](docs/API.md)
- [ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [COMPARISON.md](docs/COMPARISON.md)
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [SSOT.md](docs/SSOT.md)

## license

MIT license. see [LICENSE](LICENSE) for details.
