# Memory Engine Comparison: engram vs. Alternatives

## 1. Feature Matrix

| Feature                        | engram         | Zep           | Mem0          | LangChain Memory | Hindsight (Vectorize) |
|------------------------------- |:--------------:|:-------------:|:-------------:|:----------------:|:---------------------:|
| **Language**                   | Rust           | Python        | Python        | Python           | Go                    |
| **Deployment model**           | Single binary, Docker | Docker, cloud, pip | pip, Docker, cloud | pip, cloud           | Docker, cloud         |
| **Fault-tolerant cluster**     | Yes (3-node Raft, OpenRaft 0.9) | No | No | No | ? |
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
| **Latency (100 msg context)**  | 0.281 ms in-memory; 21.66-29.55 ms real-store | < 200 ms retrieval (public) | ~200 ms P50 search (public) | Not standardized | < 200 ms estimate |
| **Throughput (msg/s)**         | 64,500.32 (benchmarked) | Not disclosed | Not disclosed | Not standardized | Not disclosed |
| **Token efficiency**           | 39.99% fewer tokens vs naive full dump at 4k budget | Not disclosed | Not disclosed | Depends on chain and summarizer | Not disclosed |
| **Retrieval quality**          | Preliminary LongMemEval retrieval slice (n=5) reached R@5=1.000, R@10=1.000, MRR=0.767, NDCG@10=0.826; full public scorecards still pending | 71.2% via Graphiti; 63.8% LongMemEval GPT-4o | 49.0% (independent) | Not standardized | 91.4% LongMemEval |
| **License**                    | MIT            | Apache 2.0    | Apache 2.0    | MIT              | Apache 2.0            |
| **Community / maintenance**    | Active (2026)   | Active        | Active        | Active           | Active                |

> **Note:** Hindsight (Vectorize) details are based on public info as of 2026; some cells may require further verification.

## 2. Head-to-Head Performance

| System | Context Assembly Latency (100 msg) | Throughput (msg/s) | Token Efficiency (vs full-dump) | LongMemEval Score |
|--------|------------------------------------|--------------------|---------------------------------|-------------------|
| engram | 0.281 ms (in-memory), 21.66-29.55 ms (real-store) | 64,500.32 | 39.99% reduction | Harnesses and local slices published; full scorecards pending |
| Mem0   | ~200 ms (P50 search) | Not disclosed | Not disclosed | 49.0% (independent) |
| Zep    | < 200 ms (retrieval) | Not disclosed | Not disclosed | 71.2% (via Graphiti), 63.8% (LongMemEval GPT-4o) |
| Hindsight | < 200 ms (est.) | Not disclosed | Not disclosed | 91.4% (LongMemEval) |

### Retrieval Quality (LongMemEval, Preliminary)

| System | Retrieval Metrics | QA Accuracy |
|--------|-------------------|-------------|
| engram (prelim, n=5, retrieval-only, local embedder) | R@5=1.000, R@10=1.000, MRR=0.767, NDCG@10=0.826 | Not yet measured |

On the currently published numbers, engram's in-memory context assembly path is hundreds of times faster than the roughly 200 ms public retrieval figures cited for comparable systems. Its real-store path remains comfortably competitive at 21.66-29.55 ms while exercising actual Redis and LanceDB integrations, not placeholder mocks. The token-efficiency measurement also shows a 39.99% reduction versus a naive full-history dump at a 4k-token budget. The retrieval-quality gap is narrower than before because the repository now includes dedicated LongMemEval and BEAM harnesses plus a local-embedding fallback, and the first published LongMemEval retrieval slice already shows perfect recall@5/10 with strong MRR and NDCG. The public score cells should still be treated as provisional until full runs are published against the real datasets.

> **Comparison note:** The engram numbers above are direct local benchmarks of full context assembly or end-to-end request throughput. Public competitor figures are typically retrieval or search latencies, so the table should be read as directional rather than strictly apples-to-apples.

## 3. Narrative Analysis

**Where engram excels:**
- **Benchmarked performance:** Current measurements show 0.281 ms in-memory context assembly for a 100-message session, 21.66-29.55 ms with real stores depending on workload, and 64,500.32 messages per second in the reduced e2e throughput run.
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
- **Preliminary retrieval evaluation is strong, but still tiny:** A 5-question `single-session-user` LongMemEval retrieval slice achieved perfect recall@5/10 plus MRR=0.767 and NDCG@10=0.826 with the local embedder, but the full 500-question run is still pending.

**Who engram is best for:**
- Rust developers and teams who want a self-hosted, debuggable, and transparent memory layer for LLM agents.
- Anyone who needs to understand and control exactly what goes into the LLM context window.
- Projects that value observability, idempotency, and explicit token budgeting over plug-and-play cloud convenience.

---

*This matrix is maintained as of June 2026. Please open an issue or PR if you spot inaccuracies or want to add a new tool!*
