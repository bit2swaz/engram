# Memory Engine Comparison: engram vs. Alternatives

## 1. Feature Matrix

| Feature                        | engram         | Zep           | Mem0          | LangChain Memory | Hindsight (Vectorize) |
|------------------------------- |:--------------:|:-------------:|:-------------:|:----------------:|:---------------------:|
| **Language**                   | Rust           | Python        | Python        | Python           | Go                    |
| **Deployment model**           | Single binary, Docker | Docker, cloud, pip | pip, Docker, cloud | pip, cloud           | Docker, cloud         |
| **Embedding flexibility**      | Yes (trait, BYO) | Yes (BYO, OpenAI, Cohere, etc.) | Yes (BYO, OpenAI, etc.) | Yes (BYO, OpenAI, etc.) | Yes (BYO, OpenAI, etc.) |
| **Context visibility**         | Full (exact prompt shown) | Partial (debug endpoint) | Partial | Partial (depends on chain) | ? |
| **Token budget control**       | Yes (per request) | Yes (configurable) | Yes (configurable) | Partial (depends on chain) | ? |
| **Trimming strategy**          | Pair-preserving | Naive/Configurable | Naive | Naive | ? |
| **Memory types**               | Short-term, long-term, core | Short, long, episodic | Short, long | Short, long, summary | Short, long, KG? |
| **Retrieval method**           | Semantic search | Semantic, BM25, hybrid | Semantic, hybrid | Semantic, retriever chain | Semantic, hybrid, KG |
| **Knowledge graph**            | No              | No            | No            | No               | Yes                   |
| **Idempotency / deduplication**| Yes (message_id, status) | Yes (message_id) | Partial | No | ? |
| **Observability**              | Prometheus, tracing | Prometheus, logs | Logs | No (manual) | Prometheus, logs |
| **Background processing**      | Async worker, bounded queue | Async worker | Async | No | Async worker |
| **Sustainability / dependencies** | Minimal (Redis, LanceDB, OpenAI) | Postgres, Redis, vector DB | Postgres, Redis, vector DB | None required | Postgres, Redis, vector DB |
| **Documentation**              | OpenAPI, Swagger, guides | OpenAPI, Swagger, guides | Guides, OpenAPI | Docs, guides | Guides, OpenAPI |
| **Latency (100 msg context)**  | 0.30 ms in-memory; 30.26 ms real-store | < 200 ms retrieval (public) | ~200 ms P50 search (public) | Not standardized | < 200 ms estimate |
| **Throughput (msg/s)**         | 60,766.78 (benchmarked) | Not disclosed | Not disclosed | Not standardized | Not disclosed |
| **Token efficiency**           | 39.99% fewer tokens vs naive full dump at 4k budget | Not disclosed | Not disclosed | Depends on chain and summarizer | Not disclosed |
| **Retrieval quality**          | Not yet measured on LongMemEval/BEAM | 71.2% via Graphiti; 63.8% LongMemEval GPT-4o | 49.0% (independent) | Not standardized | 91.4% LongMemEval |
| **License**                    | MIT            | Apache 2.0    | Apache 2.0    | MIT              | Apache 2.0            |
| **Community / maintenance**    | Active (2026)   | Active        | Active        | Active           | Active                |

> **Note:** Hindsight (Vectorize) details are based on public info as of 2026; some cells may require further verification.

## 2. Head-to-Head Performance

| System | Context Assembly Latency (100 msg) | Throughput (msg/s) | Token Efficiency (vs full-dump) | LongMemEval Score |
|--------|------------------------------------|--------------------|---------------------------------|-------------------|
| engram | 0.30 ms (in-memory), 30.26 ms (real-store) | 60,766.78 | 39.99% reduction | Not yet measured |
| Mem0   | ~200 ms (P50 search) | Not disclosed | Not disclosed | 49.0% (independent) |
| Zep    | < 200 ms (retrieval) | Not disclosed | Not disclosed | 71.2% (via Graphiti), 63.8% (LongMemEval GPT-4o) |
| Hindsight | < 200 ms (est.) | Not disclosed | Not disclosed | 91.4% (LongMemEval) |

On the currently published numbers, engram's in-memory context assembly path is hundreds of times faster than the roughly 200 ms public retrieval figures cited for comparable systems. Its real-store path remains comfortably competitive at about 30 ms while exercising actual Redis and LanceDB integrations, not placeholder mocks. The new token-efficiency measurement also shows a 39.99% reduction versus a naive full-history dump at a 4k-token budget. The main gap is retrieval-quality benchmarking: engram now has latency, throughput, and token-budget numbers, but it still needs LongMemEval or BEAM-style evaluation before making quality claims.

> **Comparison note:** The engram numbers above are direct local benchmarks of full context assembly or end-to-end request throughput. Public competitor figures are typically retrieval or search latencies, so the table should be read as directional rather than strictly apples-to-apples.

## 3. Narrative Analysis

**Where engram excels:**
- **Benchmarked performance:** Current measurements show 0.30 ms in-memory context assembly for a 100-message session, 30.26 ms with real stores, and 60,766.78 messages per second in the reduced e2e throughput run.
- **Transparency:** Developers can inspect the exact assembled context returned by the API rather than relying on hidden chain state.
- **Rust performance:** High concurrency, low memory overhead, and strong type safety.
- **Single-binary deployment:** Easy to run locally or in production; Docker and Compose supported.
- **Pair-preserving trim:** Prevents broken dialogue, a common source of LLM hallucination in naive memory engines.
- **Idempotent workers:** Message ingestion and embedding are robust to retries and crashes.
- **Observability:** Prometheus metrics and structured tracing from day one.
- **Token budget control:** Every context assembly is budgeted per request, not just globally.

**Where engram falls short today:**
- **No knowledge graph:** Unlike Hindsight, engram does not build or use a KG for retrieval.
- **No managed cloud offering:** Self-hosted only; no SaaS or managed tier.
- **Smaller community:** Newer and less widely adopted than Zep or LangChain.
- **Retrieval is single-strategy:** Only semantic search is implemented; no hybrid or BM25 yet.
- **Not yet quality-benchmarked:** There are now published latency, throughput, and token-efficiency numbers, but not yet LongMemEval, BEAM, or similar retrieval-quality benchmarks.

**Who engram is best for:**
- Rust developers and teams who want a self-hosted, debuggable, and transparent memory layer for LLM agents.
- Anyone who needs to understand and control exactly what goes into the LLM context window.
- Projects that value observability, idempotency, and explicit token budgeting over plug-and-play cloud convenience.

---

*This matrix is maintained as of May 2026. Please open an issue or PR if you spot inaccuracies or want to add a new tool!*
