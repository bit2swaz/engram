# Quality Benchmarking

This document is the end-to-end runbook for engram's retrieval-quality benchmarks.

## Scope

engram currently ships two benchmark bridges:

- `benchmarks/longmemeval_engram.py` for LongMemEval retrieval and QA.
- `benchmarks/beam_engram.py` for BEAM retrieval and QA.

These scripts drive the existing engram HTTP API. They do not require extra debug endpoints or message-status APIs. Instead, they wait for background embeddings by polling the Prometheus gauge `engram_memory_embedding_queue_size` until the queue settles.

## Recommended Runtime

Use the repository's Docker Compose deployment and target `http://127.0.0.1:3002`.

Why:

- The compose file already wires Redis and the app together correctly.
- The host port is `3002`, which avoids the common case where `3000` is already occupied by another local service.
- The benchmark wrapper now assumes this port by default.

Bring it up manually if you want to inspect the service first:

```bash
docker compose up -d redis app
curl http://127.0.0.1:3002/health
```

## LongMemEval

### Dataset Acquisition

The LongMemEval GitHub repository does not include the benchmark JSON files in-tree. You need a local copy of one of the released files before running engram against it:

- `longmemeval_s.json`
- `longmemeval_m.json`
- `longmemeval_oracle.json`

Once the file is on disk, point the harness at it with `--dataset` or `LONGMEMEVAL_DATASET`. Cleaned variants such as `longmemeval_s_cleaned.json` also work as long as they preserve the standard LongMemEval fields.

### Retrieval Run

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /absolute/path/to/longmemeval_s.json \
  --mode retrieval \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/longmemeval
```

Outputs:

- `retrieval_results.json`
- `retrieval_metrics.json`
- `retrieval_summary.md`

### Local Embedding Fallback

For retrieval-only runs, you can avoid hosted embedding latency and rate limits by letting the harness start a local OpenAI-compatible embedding server and a matching engram process:

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /absolute/path/to/longmemeval_s.json \
  --mode retrieval \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/longmemeval-local \
  --start-local-embed-server \
  --start-engram \
  --lance-db-path ./data/lancedb-bench-local
```

That flow starts `tools/local_embed_server.py`, injects `OPENAI_BASE_URL` for the spawned engram process, and defaults `EMBEDDING_DIMENSION` to `384`. QA mode still needs a completion model such as `gpt-4o` for answer generation.

### QA Run

```bash
python3 benchmarks/longmemeval_engram.py \
  --dataset /absolute/path/to/longmemeval_s.json \
  --mode qa \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/longmemeval-qa \
  --llm-model gpt-4o
```

This produces `hypothesis.jsonl`, which is the input shape expected by the official LongMemEval evaluator.

### Official Evaluation

```bash
cd benchmarks/deps/LongMemEval/src/evaluation
python3 evaluate_qa.py gpt-4o \
  /absolute/path/to/engram/benchmarks/results/longmemeval-qa/hypothesis.jsonl \
  ../../data/longmemeval_oracle.json
```

## BEAM

### Supported Layouts

The BEAM bridge supports:

- A flat JSON file containing pre-flattened entries.
- A BEAM chat directory root such as `chats/100K`, `chats/500K`, or `chats/1M`.

The bridge does not yet support the 10M plan-grouped layout.

The public BEAM naming uses `100K`, `500K`, and `1M`. If you prefer thinking in 128K tiers, pass `--tier 128K` and the bridge will map that to the `100K` chat set.

### Retrieval Run

```bash
python3 benchmarks/beam_engram.py \
  --dataset /absolute/path/to/BEAM/chats/100K \
  --mode retrieval \
  --tier 128K \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/beam-128k
```

For BEAM retrieval, engram records ranked outputs per probing question. BEAM does not expose LongMemEval-style session gold labels directly, so the bridge writes raw retrieval outputs even when full recall-style metrics are unavailable.

### QA Run

```bash
python3 benchmarks/beam_engram.py \
  --dataset /absolute/path/to/BEAM/chats/100K \
  --mode qa \
  --tier 128K \
  --engram-url http://127.0.0.1:3002 \
  --output-dir benchmarks/results/beam-128k-qa \
  --result-file-name engram_answers.json
```

For BEAM chat-directory inputs, the QA runner mirrors the conversation directories under the output root, writes `engram_answers.json` files, and copies `probing_questions/probing_questions.json` so BEAM's evaluator can consume the output directory directly.

### Official Evaluation

```bash
cd benchmarks/deps/BEAM
python -m src.evaluation.run_evaluation \
  --input_directory /absolute/path/to/engram/benchmarks/results/beam-128k-qa \
  --chat_size 100K \
  --start_index 0 \
  --end_index 10 \
  --allowed_result_files engram_answers.json
```

## Wrapper Script

`scripts/run_quality_benchmarks.sh` is the quickest way to kick off a retrieval run.

Key defaults:

- `ENGRAM_URL=http://127.0.0.1:3002`
- `ENGRAM_START_MODE=compose`
- `QUALITY_MODE=retrieval`

Important caveat:

- `LONGMEMEVAL_DATASET` must point to a real local file.
- `RUN_BEAM=1` can default `BEAM_DATASET` to `benchmarks/deps/BEAM/chats/<tier>` after sparse-cloning the repo.
- For local-embedding retrieval smoke runs, prefer the direct Python harnesses; the wrapper still assumes the target engram instance already has a working embedding backend.

Example:

```bash
LONGMEMEVAL_DATASET=/absolute/path/to/longmemeval_s.json \
RUN_BEAM=1 \
BEAM_TIER=128K \
./scripts/run_quality_benchmarks.sh
```

## Current State

- The repository contains the harness and runbooks needed for LongMemEval and BEAM.
- LongMemEval is runnable as soon as a real local dataset file is available, including via the managed local-embedding path above.
- The repository now includes a GitHub Actions workflow that runs fast checks on push and pull request plus a scheduled/manual LongMemEval retrieval smoke job.
- BEAM 100K, 500K, and 1M layouts are supported; 10M remains out of scope for the current bridge.