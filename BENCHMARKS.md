# Benchmarks

**Run date:** 2026-05-01

## Context Assembly Latency

| Scenario | Messages | Max Tokens | Median Latency | Unit |
|----------|----------|------------|----------------|------|
| small | 10 | 2048 | 49.675 | µs |
| medium | 100 | 2048 | 300.37 | µs |
| large | 1000 | 2048 | 2.6058 | ms |
| tight_budget | 50 | 2048 | 153.70 | µs |
| long_term | 5000 | 2048 | 14.269 | ms |

## End-to-End Throughput

| Metric | Value |
|--------|-------|
| Throughput (msg/s) | 60766.78 |
| P99 Context Latency (ms) | 224.01 |

## Real-Store Latency

| Scenario | Short-Term Messages | Long-Term Entries | Max Tokens | Median Latency | Unit |
|----------|---------------------|-------------------|------------|----------------|------|
| small_real | 10 | 0 | 8000 | 30.262 | ms |
| medium_real | 100 | 0 | 8000 | 31.770 | ms |
| large_real | 1000 | 0 | 8000 | 7.0019 | ms |

## Interpretation

- Context assembly latency grows with conversation size, and the current context benchmark is using a 2048-token assembly budget.
- The end-to-end throughput benchmark measures full message ingestion through background embedding completion, then samples context retrieval latency separately.
- The real-store latency benchmark captures the added overhead of Redis plus LanceDB compared with the in-memory path.
- Benchmark numbers are environment-sensitive; compare runs on the same machine and workload before drawing conclusions.
