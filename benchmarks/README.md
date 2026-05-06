# Quality Benchmarking

This directory contains the Phase 6.5 retrieval-quality harness for running engram against LongMemEval and BEAM-style datasets without changing the server API.

## Files

- `longmemeval_engram.py`: LongMemEval bridge with `retrieval` and `qa` modes.
- `beam_engram.py`: Flexible BEAM adapter with auto-detected field names and optional manual key overrides.
- `quality_common.py`: Shared engram client, embedding-queue wait logic, retrieval metrics, and OpenAI-compatible QA helper.
- `requirements.txt`: Minimal Python dependency set for these scripts.

## Prerequisites

- Python 3.10+ with `pip install -r benchmarks/requirements.txt`
- A dedicated engram instance backed by Redis and either a real OpenAI-compatible embedding provider or the local helper in `tools/local_embed_server.py`
- `OPENAI_API_KEY` for QA mode, or another OpenAI-compatible endpoint exposed via `--llm-base-url`
- LongMemEval or BEAM benchmark assets already downloaded locally

The retrieval harness relies on the Prometheus gauge `engram_memory_embedding_queue_size` to wait until background embeddings have drained. For reliable results, run the scripts against a dedicated benchmark server rather than a shared development instance.

The recommended local target is the Docker Compose deployment on `http://127.0.0.1:3002`. Port `3000` is a common conflict point on developer machines and is not assumed by the wrapper anymore.

## LongMemEval

Retrieval-only run:

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /path/to/longmemeval_s.json \
  --mode retrieval \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/longmemeval
```

Managed local-embedding retrieval run:

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /path/to/longmemeval_s.json \
  --mode retrieval \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/longmemeval-local \
  --start-local-embed-server \
  --start-engram \
  --lance-db-path ./data/lancedb-bench-local
```

This path starts `tools/local_embed_server.py`, points engram at the local OpenAI-compatible embeddings API, and defaults the embedding width to `384`. It is the most reproducible option for CI or for laptops that are hitting hosted embedding rate limits.

QA run that emits `hypothesis.jsonl` for the official evaluator:

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /path/to/longmemeval_s.json \
  --mode qa \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/longmemeval-qa \
  --llm-model gpt-4o
```

LongMemEval dataset note:

- The cloned LongMemEval GitHub repository does not include `longmemeval_s.json`, `longmemeval_m.json`, or `longmemeval_oracle.json` in-tree.
- Download those benchmark JSONs from the official data release or cleaned benchmark release, then point `--dataset` or `LONGMEMEVAL_DATASET` at the local file.
- After QA mode finishes, run the official evaluator from the LongMemEval repo with the generated `hypothesis.jsonl`.

Example evaluator command:

```bash
cd benchmarks/deps/LongMemEval/src/evaluation
python3 evaluate_qa.py gpt-4o \
  /path/to/engram/benchmarks/results/longmemeval-qa/hypothesis.jsonl \
  ../../data/longmemeval_oracle.json
```

LongMemEval outputs:

- `retrieval_results.json`: per-question ranked items and mapped source session ids
- `retrieval_metrics.json`: aggregate retrieval metrics across the processed slice
- `retrieval_summary.md`: markdown summary for reporting or docs
- `hypothesis.jsonl`: QA hypotheses in the format expected by the official evaluator
- `qa_details.json`: question, answer, context, and hypothesis records for debugging

The bridge injects session-marker system messages by default so question dates and source session ids remain visible during QA and debugging. Disable that with `--no-inject-session-markers` if you want raw-turn-only ingestion.

## BEAM

The BEAM bridge supports two input layouts:

- A flat JSON file containing pre-flattened BEAM-style entries.
- A BEAM chat directory tree such as `chats/100K`, `chats/500K`, or `chats/1M`, including per-conversation `chat.json` and `probing_questions/probing_questions.json` files.

The repo's naming uses `100K`, `500K`, and `1M`; if you pass `--tier 128K`, the bridge maps that to the `100K` directory automatically.

Current limitation:

- The 10M BEAM layout is not yet supported by `beam_engram.py` because its plan-grouped probing-question structure differs materially from the 100K, 500K, and 1M layouts.

Example retrieval run for a 128K tier:

```bash
python3 benchmarks/beam_engram.py \
  --dataset /path/to/BEAM/chats/100K \
  --mode retrieval \
  --tier 128K \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/beam-128k
```

Example QA run with outputs arranged for BEAM's evaluator:

```bash
python3 benchmarks/beam_engram.py \
  --dataset /path/to/BEAM/chats/100K \
  --mode qa \
  --tier 128K \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/beam-128k-qa \
  --result-file-name engram_answers.json
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

If the dataset exposes session-level gold labels, the bridge computes recall, MRR, and NDCG. If not, it still records raw retrieval outputs. For BEAM chat-directory QA runs, the bridge also writes per-conversation `engram_answers.json` files and copies the relevant probing-question files into the output tree so the official BEAM evaluator can run against the generated answers.

Example BEAM evaluation command:

```bash
cd benchmarks/deps/BEAM
python -m src.evaluation.run_evaluation \
  --input_directory /path/to/engram/benchmarks/results/beam-128k-qa \
  --chat_size 100K \
  --start_index 0 \
  --end_index 10 \
  --allowed_result_files engram_answers.json
```

## Wrapper Script

Use `scripts/run_quality_benchmarks.sh` for a simple end-to-end retrieval run. It can clone the public benchmark repositories into `benchmarks/deps`, start engram if needed, and write outputs under `benchmarks/results`.

Defaults and behavior:

- `ENGRAM_URL` defaults to `http://127.0.0.1:3002`.
- `ENGRAM_START_MODE=compose` is the safest local default because it matches the repository's Docker Compose wiring.
- `LONGMEMEVAL_DATASET` must point to a real benchmark JSON file because the repo clone does not ship one.
- `RUN_BEAM=1` will sparse-clone the BEAM repo and default `BEAM_DATASET` to `chats/100K`, `chats/500K`, or `chats/1M` depending on `BEAM_TIER`.
- The wrapper still assumes your target engram already has a working embedding backend. For local-embedding smoke runs, call the Python harnesses directly with `--start-local-embed-server --start-engram`.

## Cost Notes

- Retrieval mode still incurs embedding cost because engram embeds the ingested messages.
- QA mode adds completion cost on top of embeddings.
- LongMemEval and BEAM full runs are intentionally resumable; use `--resume` after interruptions.