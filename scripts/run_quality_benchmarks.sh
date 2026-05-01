#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="${ROOT_DIR}/benchmarks"
DEPS_DIR="${BENCH_DIR}/deps"
RESULTS_DIR="${BENCH_DIR}/results"
LONGMEMEVAL_REPO_DIR="${LONGMEMEVAL_REPO_DIR:-${DEPS_DIR}/LongMemEval}"
LONGMEMEVAL_DATASET="${LONGMEMEVAL_DATASET:-${LONGMEMEVAL_REPO_DIR}/data/longmemeval_s.json}"
BEAM_REPO_DIR="${BEAM_REPO_DIR:-${DEPS_DIR}/BEAM}"
BEAM_DATASET="${BEAM_DATASET:-}"
ENGRAM_URL="${ENGRAM_URL:-http://localhost:3000}"
START_ENGRAM="${START_ENGRAM:-1}"
QUALITY_MODE="${QUALITY_MODE:-retrieval}"
RUN_BEAM="${RUN_BEAM:-0}"
BEAM_TIER="${BEAM_TIER:-128K}"
LONGMEMEVAL_OUTPUT_DIR="${LONGMEMEVAL_OUTPUT_DIR:-${RESULTS_DIR}/longmemeval}"
BEAM_OUTPUT_DIR="${BEAM_OUTPUT_DIR:-${RESULTS_DIR}/beam}"
ENGRAM_COMMAND="${ENGRAM_COMMAND:-cargo run --release}"
ENGRAM_LOG="${ENGRAM_LOG:-${RESULTS_DIR}/engram-quality.log}"

mkdir -p "${DEPS_DIR}" "${RESULTS_DIR}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

if ! python3 -c 'import requests' >/dev/null 2>&1; then
  echo "Install Python dependencies first: python3 -m pip install -r benchmarks/requirements.txt" >&2
  exit 1
fi

if [[ ! -d "${LONGMEMEVAL_REPO_DIR}" ]]; then
  git clone https://github.com/xiaowu0162/LongMemEval.git "${LONGMEMEVAL_REPO_DIR}"
fi

if [[ ! -f "${LONGMEMEVAL_DATASET}" ]]; then
  echo "LongMemEval dataset not found at ${LONGMEMEVAL_DATASET}" >&2
  echo "Download the dataset locally and set LONGMEMEVAL_DATASET to the JSON file you want to run." >&2
  exit 1
fi

ENGRAM_PID=""
cleanup() {
  if [[ -n "${ENGRAM_PID}" ]] && kill -0 "${ENGRAM_PID}" >/dev/null 2>&1; then
    kill "${ENGRAM_PID}" >/dev/null 2>&1 || true
    wait "${ENGRAM_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${START_ENGRAM}" == "1" ]]; then
  if ! curl -fsS "${ENGRAM_URL}/health" >/dev/null 2>&1; then
    mkdir -p "$(dirname "${ENGRAM_LOG}")"
    pushd "${ROOT_DIR}" >/dev/null
    ${ENGRAM_COMMAND} >"${ENGRAM_LOG}" 2>&1 &
    ENGRAM_PID="$!"
    popd >/dev/null

    for _ in $(seq 1 120); do
      if curl -fsS "${ENGRAM_URL}/health" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done

    if ! curl -fsS "${ENGRAM_URL}/health" >/dev/null 2>&1; then
      echo "engram did not become healthy; see ${ENGRAM_LOG}" >&2
      exit 1
    fi
  fi
fi

python3 "${BENCH_DIR}/longmemeval_engram.py" \
  --dataset "${LONGMEMEVAL_DATASET}" \
  --mode "${QUALITY_MODE}" \
  --engram-url "${ENGRAM_URL}" \
  --output-dir "${LONGMEMEVAL_OUTPUT_DIR}"

if [[ "${RUN_BEAM}" == "1" ]]; then
  if [[ ! -d "${BEAM_REPO_DIR}" ]]; then
    git clone https://github.com/mohammadtavakoli78/BEAM.git "${BEAM_REPO_DIR}"
  fi
  if [[ -z "${BEAM_DATASET}" || ! -f "${BEAM_DATASET}" ]]; then
    echo "Set BEAM_DATASET to a local BEAM json file before enabling RUN_BEAM=1" >&2
    exit 1
  fi

  python3 "${BENCH_DIR}/beam_engram.py" \
    --dataset "${BEAM_DATASET}" \
    --mode "${QUALITY_MODE}" \
    --tier "${BEAM_TIER}" \
    --engram-url "${ENGRAM_URL}" \
    --output-dir "${BEAM_OUTPUT_DIR}"
fi

echo "Quality benchmark outputs written under ${RESULTS_DIR}"