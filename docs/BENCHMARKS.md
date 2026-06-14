# Benchmark Documentation

The authoritative latency and throughput report remains the generated file at the repository root: [../BENCHMARKS.md](../BENCHMARKS.md).

That file is produced by `./scripts/generate_benchmark_report.sh` and covers:

- Context assembly latency benchmarks.
- End-to-end throughput and p99 context latency.
- Real-store latency measurements using Redis and LanceDB.

Retrieval-quality benchmarking is documented separately because it depends on external benchmark datasets and produces per-run artifacts instead of a single generated markdown table.

- User-facing runbook: [QUALITY_BENCHMARKS.md](QUALITY_BENCHMARKS.md)
- Harness implementation notes: [../benchmarks/README.md](../benchmarks/README.md)

Current status:

- LongMemEval and BEAM runners are available in `benchmarks/`.
- LongMemEval data must be downloaded separately before running the harness.
- Retrieval-only benchmark slices can also run against the bundled local OpenAI-compatible embedding helper in `tools/local_embed_server.py`.
- BEAM supports the repository-style `chats/100K`, `chats/500K`, and `chats/1M` layouts.
- The Raft log store is now persistent (redb-backed as of Stage 3A). Cluster-mode performance numbers will be published in a future run once the benchmark harness is updated to target the cluster compose setup.