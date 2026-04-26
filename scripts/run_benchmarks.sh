#!/bin/bash
set -e
echo "Running benchmarks..."
cargo bench --bench context_assembly_benchmark 2>&1 | tee benchmark_results.txt
echo "Benchmark results saved to benchmark_results.txt"