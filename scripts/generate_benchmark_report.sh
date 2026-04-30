#!/bin/bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
cd "$repo_root"

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

context_log="$tmp_dir/context.log"
e2e_log="$tmp_dir/e2e.log"
real_store_log="$tmp_dir/real_store.log"

scenario_desc() {
    case "$1" in
        small) echo "10|2048" ;;
        medium) echo "100|2048" ;;
        large) echo "1000|2048" ;;
        tight_budget) echo "50|2048" ;;
        long_term) echo "5000|2048" ;;
        small_real) echo "10|0|8000" ;;
        medium_real) echo "100|0|8000" ;;
        large_real) echo "1000|0|8000" ;;
        *) echo "?|?|?" ;;
    esac
}

parse_criterion_log() {
    local log_file=$1
    awk '
        /^[[:alnum:]_]+(\/[[:alnum:]_]+\/[[:alnum:]_]+)?$/ {
            last_name = $1
            next
        }
        /time:[[:space:]]+\[/ {
            same_line_name = ($1 != "time:")
            name = same_line_name ? $1 : last_name
            split(name, parts, "/")
            scenario = (length(parts[2]) > 0) ? parts[2] : parts[1]
            if (same_line_name) {
                median = $5
                unit = $6
            } else {
                median = $4
                unit = $5
            }
            gsub(/\]/, "", unit)
            print scenario "," median "," unit
            last_name = ""
        }
    ' "$log_file"
}

render_context_table() {
    local table
    table="| Scenario | Messages | Max Tokens | Median Latency | Unit |
|----------|----------|------------|----------------|------|"

    while IFS="," read -r scenario median unit; do
        [[ -z "$scenario" ]] && continue
        IFS='|' read -r messages tokens _ <<< "$(scenario_desc "$scenario")"
        table+=$'\n'
        table+="| $scenario | $messages | $tokens | $median | $unit |"
    done < <(parse_criterion_log "$context_log")

    printf '%s\n' "$table"
}

render_real_store_table() {
    local table
    table="| Scenario | Short-Term Messages | Long-Term Entries | Max Tokens | Median Latency | Unit |
|----------|---------------------|-------------------|------------|----------------|------|"

    while IFS="," read -r scenario median unit; do
        [[ -z "$scenario" ]] && continue
        IFS='|' read -r short_count long_count max_tokens <<< "$(scenario_desc "$scenario")"
        table+=$'\n'
        table+="| $scenario | $short_count | $long_count | $max_tokens | $median | $unit |"
    done < <(parse_criterion_log "$real_store_log")

    printf '%s\n' "$table"
}

render_e2e_section() {
    if [[ ! -f "$e2e_log" ]]; then
        printf '%s\n' "e2e throughput benchmark not available."
        return
    fi

    local throughput
    local p99
    throughput=$(awk '/^Throughput:/ {print $2}' "$e2e_log" | tail -1)
    p99=$(awk '/^P99 Context Latency:/ {print $4}' "$e2e_log" | tail -1)

    if [[ -z "$throughput" || -z "$p99" ]]; then
        printf '%s\n' "e2e throughput benchmark did not produce parseable output."
        return
    fi

    cat <<EOF
| Metric | Value |
|--------|-------|
| Throughput (msg/s) | $throughput |
| P99 Context Latency (ms) | $p99 |
EOF
}

echo "Running context assembly benchmark..."
cargo bench --bench context_assembly_benchmark 2>&1 | tee "$context_log"

E2E_SECTION="e2e throughput benchmark not available."
echo "Running e2e throughput benchmark..."
if E2E_BENCH_ITERATIONS=3 \
   E2E_BENCH_TASKS=4 \
   E2E_BENCH_MESSAGES_PER_TASK=100 \
   E2E_BENCH_CONTEXT_SAMPLES=25 \
    E2E_BENCH_WORKERS=1 \
   E2E_BENCH_TIMEOUT_SECS=120 \
   cargo bench --bench e2e_throughput 2>&1 | tee "$e2e_log"; then
    E2E_SECTION=$(render_e2e_section)
else
    E2E_SECTION="e2e throughput benchmark failed to complete; see benchmark logs for details."
fi

REAL_STORE_SECTION="real-store latency benchmark not available."
if cargo bench --bench real_store_latency --no-run >/dev/null 2>&1; then
    echo "Running real-store latency benchmark..."
    if cargo bench --bench real_store_latency 2>&1 | tee "$real_store_log"; then
        REAL_STORE_SECTION=$(render_real_store_table)
    else
        REAL_STORE_SECTION="real-store latency benchmark failed to complete; see benchmark logs for details."
    fi
fi

CONTEXT_TABLE=$(render_context_table)

cat > "$repo_root/BENCHMARKS.md" <<EOF
# Benchmarks

**Run date:** $(date '+%Y-%m-%d')

## Context Assembly Latency

$CONTEXT_TABLE

## End-to-End Throughput

$E2E_SECTION

## Real-Store Latency

$REAL_STORE_SECTION

## Interpretation

- Context assembly latency grows with conversation size, and the current context benchmark is using a 2048-token assembly budget.
- The end-to-end throughput benchmark measures full message ingestion through background embedding completion, then samples context retrieval latency separately.
- The real-store latency benchmark captures the added overhead of Redis plus LanceDB compared with the in-memory path.
- Benchmark numbers are environment-sensitive; compare runs on the same machine and workload before drawing conclusions.
EOF

chmod +x "$repo_root/scripts/generate_benchmark_report.sh"
