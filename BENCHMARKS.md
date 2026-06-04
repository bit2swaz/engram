# Benchmarks

**Run date:** 2026-05-01

> **Note:** All numbers below are for standalone mode (single node, no Raft). Cluster mode adds Raft replication latency on writes (one gRPC round trip to quorum before the response returns). Cluster-mode benchmark numbers will be published after Stage 2 hardens the log store.

## Context Assembly Latency

| Scenario | Messages | Max Tokens | Median Latency | Unit |
|----------|----------|------------|----------------|------|
| small | 10 | 2048 | 50.103 | µs |
| medium | 100 | 2048 | 281.02 | µs |
| large | 1000 | 2048 | 2.4864 | ms |
| tight_budget | 50 | 2048 | 153.38 | µs |
| long_term | 5000 | 2048 | 13.415 | ms |

## End-to-End Throughput

| Metric | Value |
|--------|-------|
| Throughput (msg/s) | 64500.32 |
| P99 Context Latency (ms) | 208.24 |

## Real-Store Latency

| Scenario | Short-Term Messages | Long-Term Entries | Max Tokens | Median Latency | Unit |
|----------|---------------------|-------------------|------------|----------------|------|
| small_real | 10 | 0 | 8000 | 21.658 | ms |
| medium_real | 100 | 0 | 8000 | 22.518 | ms |
| large_real | 1000 | 0 | 8000 | 29.552 | ms |

## Retrieval Quality (LongMemEval)

A small slice of LongMemEval (5 questions) was run with a local embedding backend to validate the retrieval pipeline. Results are preliminary and will be expanded.

| Metric | Value |
|--------|-------|
| Questions evaluated | 5 |
| Recall@5 | 1.000 |
| Recall@10 | 1.000 |
| MRR | 0.767 |
| NDCG@10 | 0.826 |

These are session-level retrieval metrics from `single-session-user` questions, not QA accuracy numbers.

## Interpretation

- Context assembly latency grows with conversation size, and the current context benchmark is using a 2048-token assembly budget.
- The end-to-end throughput benchmark measures full message ingestion through background embedding completion, then samples context retrieval latency separately.
- The real-store latency benchmark captures the added overhead of Redis plus LanceDB compared with the in-memory path.
- The preliminary LongMemEval retrieval slice shows that the retrieval pipeline is functioning correctly on a real benchmark format, but the published sample is too small to treat as a final scorecard.
- Benchmark numbers are environment-sensitive; compare runs on the same machine and workload before drawing conclusions.
