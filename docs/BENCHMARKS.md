# Benchmarks

**Run date:** 2026-04-27

## Context Assembly Latency

| Scenario | Messages | Max Tokens | Median Latency | Unit |\n|----------|----------|------------|----------------|------|\n| small | 10 | 8000 | µs | 52.277 |\n| medium | 100 | 8000 | µs | 293.74 |\n| large | 1000 | 8000 | ms | 2.7772 |\n| tight_budget | 50 | 1000 | µs | 159.25 |\n| long_term | 5000 long‑term entries | 8000 | ms | 14.448 |\n

## End‑to‑End Throughput

e2e throughput benchmark not available.

## Interpretation

- Context assembly scales roughly linearly with message count.
- Tight budgets reduce latency (fewer tokens to process).
- Long‑term retrieval with 5000 entries adds ~13 ms overhead.
- Throughput numbers (if present) indicate how many messages per second the system can absorb.
