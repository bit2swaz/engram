# Benchmarks

**Run date:** 2026-05-01

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

## Interpretation

- Context assembly latency grows with conversation size, and the current context benchmark is using a 2048-token assembly budget.
- The end-to-end throughput benchmark measures full message ingestion through background embedding completion, then samples context retrieval latency separately.
- The real-store latency benchmark captures the added overhead of Redis plus LanceDB compared with the in-memory path.
- Benchmark numbers are environment-sensitive; compare runs on the same machine and workload before drawing conclusions.
