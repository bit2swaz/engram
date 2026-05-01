# Quality Benchmarking

This directory contains the Phase 6.5 retrieval-quality harness for running engram against LongMemEval and BEAM-style datasets without changing the server API.

## Files

- `longmemeval_engram.py`: LongMemEval bridge with `retrieval` and `qa` modes.
- `beam_engram.py`: Flexible BEAM adapter with auto-detected field names and optional manual key overrides.
- `quality_common.py`: Shared engram client, embedding-queue wait logic, retrieval metrics, and OpenAI-compatible QA helper.
- `requirements.txt`: Minimal Python dependency set for these scripts.

## Prerequisites

- Python 3.10+ with `pip install -r benchmarks/requirements.txt`
- A dedicated engram instance backed by Redis and a valid embedding provider configuration
- `OPENAI_API_KEY` for QA mode, or another OpenAI-compatible endpoint exposed via `--llm-base-url`
- LongMemEval or BEAM dataset files already downloaded locally

The retrieval harness relies on the Prometheus gauge `engram_memory_embedding_queue_size` to wait until background embeddings have drained. For reliable results, run the scripts against a dedicated benchmark server rather than a shared development instance.

## LongMemEval

Retrieval-only run:

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /path/to/longmemeval_s.json \
  --mode retrieval \
  --engram-url http://localhost:3000 \
  --output-dir benchmarks/results/longmemeval
```

QA run that emits `hypothesis.jsonl` for the official evaluator:

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /path/to/longmemeval_s.json \
  --mode qa \
  --engram-url http://localhost:3000 \
  --output-dir benchmarks/results/longmemeval-qa \
  --llm-model gpt-4o
```

LongMemEval outputs:

- `retrieval_results.json`: per-question ranked items and mapped source session ids
- `retrieval_metrics.json`: aggregate retrieval metrics across the processed slice
- `retrieval_summary.md`: markdown summary for reporting or docs
- `hypothesis.jsonl`: QA hypotheses in the format expected by the official evaluator
- `qa_details.json`: question, answer, context, and hypothesis records for debugging

The bridge injects session-marker system messages by default so question dates and source session ids remain visible during QA and debugging. Disable that with `--no-inject-session-markers` if you want raw-turn-only ingestion.

## BEAM

The public BEAM layouts in the wild are less standardized than LongMemEval. The BEAM bridge therefore attempts to infer the common fields first and lets you override them when the local dataset schema differs.

Example retrieval run for a 128K tier:

```bash
python3 benchmarks/beam_engram.py \
  --dataset /path/to/beam.json \
  --mode retrieval \
  --tier 128K \
  --engram-url http://localhost:3000 \
  --output-dir benchmarks/results/beam-128k
```

Useful schema override flags:

- `--conversation-key`
- `--question-id-key`
- `--question-key`
- `--answer-key`
- `--session-ids-key`
- `--date-key`
- `--answer-session-ids-key`
- `--tier-key`

If the dataset exposes session-level gold labels, the bridge computes recall, MRR, and NDCG. If not, it still records raw retrieval results and QA outputs for downstream evaluation.

## Wrapper Script

Use `scripts/run_quality_benchmarks.sh` for a simple end-to-end retrieval run. It can clone the public benchmark repositories into `benchmarks/deps`, start engram if needed, and write outputs under `benchmarks/results`.

## Cost Notes

- Retrieval mode still incurs embedding cost because engram embeds the ingested messages.
- QA mode adds completion cost on top of embeddings.
- LongMemEval and BEAM full runs are intentionally resumable; use `--resume` after interruptions.