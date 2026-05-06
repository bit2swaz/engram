#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="${ROOT_DIR}/benchmarks"
DEPS_DIR="${BENCH_DIR}/deps"
RESULTS_DIR="${BENCH_DIR}/results"
LONGMEMEVAL_REPO_DIR="${LONGMEMEVAL_REPO_DIR:-${DEPS_DIR}/LongMemEval}"
LONGMEMEVAL_DATASET="${LONGMEMEVAL_DATASET:-}"
BEAM_REPO_DIR="${BEAM_REPO_DIR:-${DEPS_DIR}/BEAM}"
BEAM_DATASET="${BEAM_DATASET:-}"
ENGRAM_URL="${ENGRAM_URL:-http://127.0.0.1:3002}"
START_ENGRAM="${START_ENGRAM:-1}"
ENGRAM_START_MODE="${ENGRAM_START_MODE:-compose}"
ENGRAM_CLEANUP="${ENGRAM_CLEANUP:-1}"
QUALITY_MODE="${QUALITY_MODE:-retrieval}"
RUN_BEAM="${RUN_BEAM:-0}"
BEAM_TIER="${BEAM_TIER:-128K}"
LONGMEMEVAL_OUTPUT_DIR="${LONGMEMEVAL_OUTPUT_DIR:-${RESULTS_DIR}/longmemeval}"
BEAM_OUTPUT_DIR="${BEAM_OUTPUT_DIR:-${RESULTS_DIR}/beam}"
ENGRAM_COMMAND="${ENGRAM_COMMAND:-env ENGRAM_BIND_ADDR=127.0.0.1:3002 cargo run --release}"
ENGRAM_LOG="${ENGRAM_LOG:-${RESULTS_DIR}/engram-quality.log}"
BEAM_RESULT_FILE_NAME="${BEAM_RESULT_FILE_NAME:-engram_answers.json}"

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

if [[ -z "${LONGMEMEVAL_DATASET}" ]]; then
  default_longmemeval_candidate="${LONGMEMEVAL_REPO_DIR}/data/longmemeval_s.json"
  if [[ -f "${default_longmemeval_candidate}" ]]; then
    LONGMEMEVAL_DATASET="${default_longmemeval_candidate}"
  fi
fi

if [[ -z "${LONGMEMEVAL_DATASET}" || ! -f "${LONGMEMEVAL_DATASET}" ]]; then
  echo "LongMemEval dataset was not found locally." >&2
  echo "The official benchmark JSONs are not bundled in the LongMemEval repo clone." >&2
  echo "Download longmemeval_s.json, longmemeval_m.json, or longmemeval_oracle.json from the official data package or cleaned release, then set LONGMEMEVAL_DATASET to that file." >&2
  exit 1
fi

ENGRAM_PID=""
STARTED_WITH_COMPOSE="0"
cleanup() {
  if [[ "${ENGRAM_CLEANUP}" != "1" ]]; then
    return
  fi
  if [[ "${STARTED_WITH_COMPOSE}" == "1" ]]; then
    docker compose down >/dev/null 2>&1 || true
  fi
  if [[ -n "${ENGRAM_PID}" ]] && kill -0 "${ENGRAM_PID}" >/dev/null 2>&1; then
    kill "${ENGRAM_PID}" >/dev/null 2>&1 || true
    wait "${ENGRAM_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${START_ENGRAM}" == "1" ]]; then
  if ! curl -fsS "${ENGRAM_URL}/health" >/dev/null 2>&1; then
    if [[ "${ENGRAM_START_MODE}" == "compose" ]]; then
      docker compose up -d redis app >/dev/null
      STARTED_WITH_COMPOSE="1"
    else
      mkdir -p "$(dirname "${ENGRAM_LOG}")"
      pushd "${ROOT_DIR}" >/dev/null
      ${ENGRAM_COMMAND} >"${ENGRAM_LOG}" 2>&1 &
      ENGRAM_PID="$!"
      popd >/dev/null
    fi

    for _ in $(seq 1 120); do
      if curl -fsS "${ENGRAM_URL}/health" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done

    if ! curl -fsS "${ENGRAM_URL}/health" >/dev/null 2>&1; then
      if [[ "${ENGRAM_START_MODE}" == "compose" ]]; then
        echo "engram did not become healthy on ${ENGRAM_URL} after docker compose startup" >&2
      else
        echo "engram did not become healthy; see ${ENGRAM_LOG}" >&2
      fi
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
  beam_source_tier="${BEAM_TIER}"
  if [[ "${beam_source_tier}" == "128K" ]]; then
    beam_source_tier="100K"
  fi

  if [[ ! -d "${BEAM_REPO_DIR}/.git" ]]; then
    git clone --filter=blob:none --sparse https://github.com/mohammadtavakoli78/BEAM.git "${BEAM_REPO_DIR}"
    git -C "${BEAM_REPO_DIR}" sparse-checkout init --cone
    git -C "${BEAM_REPO_DIR}" sparse-checkout set README.md "chats/${beam_source_tier}"
  fi

  if [[ -z "${BEAM_DATASET}" ]]; then
    default_beam_dataset="${BEAM_REPO_DIR}/chats/${beam_source_tier}"
    if [[ -d "${default_beam_dataset}" ]]; then
      BEAM_DATASET="${default_beam_dataset}"
    fi
  fi

  if [[ -z "${BEAM_DATASET}" || ! -e "${BEAM_DATASET}" ]]; then
    echo "Set BEAM_DATASET to a local BEAM dataset root or flat json file before enabling RUN_BEAM=1" >&2
    exit 1
  fi

  python3 "${BENCH_DIR}/beam_engram.py" \
    --dataset "${BEAM_DATASET}" \
    --mode "${QUALITY_MODE}" \
    --tier "${BEAM_TIER}" \
    --engram-url "${ENGRAM_URL}" \
    --output-dir "${BEAM_OUTPUT_DIR}" \
    --result-file-name "${BEAM_RESULT_FILE_NAME}"
fi

echo "Quality benchmark outputs written under ${RESULTS_DIR}"