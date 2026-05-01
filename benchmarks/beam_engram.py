#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from quality_common import (  # noqa: E402
    BenchError,
    DEFAULT_QUEUE_METRIC_NAME,
    EngramClient,
    ensure_directory,
    load_json,
    log,
    ndcg_at_k,
    ordered_unique,
    recall_all_at_k,
    recall_any_at_k,
    reciprocal_rank,
    render_retrieval_summary_markdown,
    run_openai_compatible_qa,
    save_json,
    save_jsonl,
    save_markdown,
    split_text_for_ingestion,
    start_engram_process,
    stop_process,
    summarize_retrieval_results,
)


QUESTION_ID_KEYS = ["question_id", "id", "qid"]
QUESTION_KEYS = ["question", "query", "prompt"]
ANSWER_KEYS = ["answer", "reference_answer", "ground_truth", "target"]
CONVERSATION_KEYS = [
    "haystack_sessions",
    "sessions",
    "conversation",
    "history",
    "messages",
    "dialogue",
]
SESSION_ID_KEYS = ["haystack_session_ids", "session_ids", "source_session_ids"]
DATE_KEYS = ["haystack_dates", "session_dates", "dates", "timestamps"]
TARGET_SESSION_ID_KEYS = [
    "answer_session_ids",
    "evidence_session_ids",
    "gold_session_ids",
    "target_session_ids",
]
TIER_KEYS = ["tier", "context_tier", "size_tier", "token_tier", "bucket"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BEAM-style retrieval or QA evaluation against engram.",
    )
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument(
        "--mode",
        choices=["retrieval", "qa"],
        default="retrieval",
    )
    parser.add_argument("--engram-url", default="http://localhost:3000")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--max-tokens", type=int, default=4096)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--timeout-secs", type=float, default=30.0)
    parser.add_argument("--wait-timeout-secs", type=float, default=120.0)
    parser.add_argument("--queue-metric-name", default=DEFAULT_QUEUE_METRIC_NAME)
    parser.add_argument("--queue-settle-secs", type=float, default=2.0)
    parser.add_argument("--queue-poll-interval-secs", type=float, default=1.0)
    parser.add_argument("--sleep-between-messages", type=float, default=0.0)
    parser.add_argument("--keep-sessions", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--start-engram", action="store_true")
    parser.add_argument("--engram-command", default="cargo run --release")
    parser.add_argument("--engram-cwd", type=Path, default=SCRIPT_DIR.parent)
    parser.add_argument("--tier", default="")
    parser.add_argument("--question-id-key", default="")
    parser.add_argument("--question-key", default="")
    parser.add_argument("--answer-key", default="")
    parser.add_argument("--conversation-key", default="")
    parser.add_argument("--session-ids-key", default="")
    parser.add_argument("--date-key", default="")
    parser.add_argument("--answer-session-ids-key", default="")
    parser.add_argument("--tier-key", default="")
    parser.add_argument("--max-message-chars", type=int, default=4000)
    parser.add_argument(
        "--inject-session-markers",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--llm-model", default="gpt-4o")
    parser.add_argument("--llm-base-url", default="https://api.openai.com/v1")
    parser.add_argument("--llm-api-key-env", default="OPENAI_API_KEY")
    return parser.parse_args()


def choose_key(entry: Mapping[str, Any], explicit_key: str, candidates: Sequence[str]) -> Optional[str]:
    if explicit_key:
        if explicit_key not in entry:
            raise BenchError(f"requested key {explicit_key!r} was not found in dataset entry")
        return explicit_key
    for candidate in candidates:
        if candidate in entry:
            return candidate
    return None


def load_dataset(dataset_path: Path, limit: int) -> List[Dict[str, Any]]:
    data = load_json(dataset_path)
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for candidate in ["data", "examples", "items"]:
            if isinstance(data.get(candidate), list):
                rows = data[candidate]
                break
        else:
            raise BenchError(f"unable to locate BEAM entries in {dataset_path}")
    else:
        raise BenchError(f"unsupported dataset payload in {dataset_path}")

    if limit > 0:
        return rows[:limit]
    return rows


def infer_scalar(entry: Mapping[str, Any], explicit_key: str, candidates: Sequence[str], fallback: str) -> str:
    key = choose_key(entry, explicit_key, candidates)
    if key is None:
        return fallback
    value = entry.get(key)
    return fallback if value is None else str(value)


def infer_list(entry: Mapping[str, Any], explicit_key: str, candidates: Sequence[str]) -> Optional[List[Any]]:
    key = choose_key(entry, explicit_key, candidates)
    if key is None:
        return None
    value = entry.get(key)
    if value is None:
        return None
    if not isinstance(value, list):
        raise BenchError(f"field {key!r} must be a list")
    return value


def normalize_turn(turn: Mapping[str, Any]) -> Dict[str, Any]:
    if "role" not in turn or "content" not in turn:
        raise BenchError(f"turn is missing role/content fields: {turn}")
    return {"role": str(turn["role"]), "content": str(turn["content"])}


def normalize_sessions(raw_history: Any) -> List[List[Dict[str, Any]]]:
    if isinstance(raw_history, dict):
        key = choose_key(raw_history, "", CONVERSATION_KEYS + ["turns", "items"])
        if key is None:
            raise BenchError("history dict did not contain a nested conversation field")
        return normalize_sessions(raw_history[key])

    if not isinstance(raw_history, list):
        raise BenchError("history must be a list")

    if not raw_history:
        return []

    if all(isinstance(item, dict) and "role" in item and "content" in item for item in raw_history):
        return [[normalize_turn(item) for item in raw_history]]

    if all(isinstance(item, list) for item in raw_history):
        return [[normalize_turn(turn) for turn in session] for session in raw_history]

    normalized: List[List[Dict[str, Any]]] = []
    for item in raw_history:
        if not isinstance(item, dict):
            raise BenchError(f"unsupported history item: {item!r}")
        nested_key = choose_key(item, "", ["turns", "messages", "session", "conversation"])
        if nested_key is None:
            raise BenchError("session entry did not expose turns/messages/session fields")
        nested_turns = item[nested_key]
        if not isinstance(nested_turns, list):
            raise BenchError(f"session field {nested_key!r} must be a list")
        normalized.append([normalize_turn(turn) for turn in nested_turns])
    return normalized


def extract_sessions(entry: Mapping[str, Any], args: argparse.Namespace) -> List[Tuple[str, Optional[str], List[Dict[str, Any]]]]:
    conversation_key = choose_key(entry, args.conversation_key, CONVERSATION_KEYS)
    if conversation_key is None:
        raise BenchError("unable to infer conversation field for BEAM entry")

    raw_sessions = normalize_sessions(entry[conversation_key])
    session_ids = infer_list(entry, args.session_ids_key, SESSION_ID_KEYS)
    session_dates = infer_list(entry, args.date_key, DATE_KEYS)

    if session_ids is not None and len(session_ids) != len(raw_sessions):
        raise BenchError("session id list length does not match conversation length")
    if session_dates is not None and len(session_dates) != len(raw_sessions):
        raise BenchError("session date list length does not match conversation length")

    sessions: List[Tuple[str, Optional[str], List[Dict[str, Any]]]] = []
    for index, turns in enumerate(raw_sessions, start=1):
        session_id = str(session_ids[index - 1]) if session_ids is not None else f"beam-session-{index}"
        session_date = str(session_dates[index - 1]) if session_dates is not None else None
        sessions.append((session_id, session_date, turns))
    return sessions


def should_include_entry(entry: Mapping[str, Any], args: argparse.Namespace) -> bool:
    if not args.tier:
        return True
    tier_key = choose_key(entry, args.tier_key, TIER_KEYS)
    if tier_key is None:
        raise BenchError("--tier was provided but no tier field could be inferred")
    return str(entry[tier_key]) == args.tier


def ingest_entry(
    client: EngramClient,
    entry: Mapping[str, Any],
    args: argparse.Namespace,
) -> Tuple[str, Dict[str, List[str]]]:
    engram_session_id = client.create_session()
    text_to_session_ids: Dict[str, List[str]] = {}

    for session_index, (source_session_id, session_date, turns) in enumerate(
        extract_sessions(entry, args),
        start=1,
    ):
        if args.inject_session_markers:
            marker_text = (
                f"[engram-beam] session={session_index} source_id={source_session_id} "
                f"date={session_date or 'unknown'}"
            )
            client.add_message(
                engram_session_id,
                "system",
                marker_text,
                message_id=f"beam-marker-{session_index}",
            )
            text_to_session_ids.setdefault(marker_text, []).append(source_session_id)

        for turn_index, turn in enumerate(turns, start=1):
            role = turn["role"]
            chunks = split_text_for_ingestion(turn["content"], args.max_message_chars)
            for chunk_index, chunk in enumerate(chunks, start=1):
                client.add_message(
                    engram_session_id,
                    role,
                    chunk,
                    message_id=f"beam-{session_index}-{turn_index}-{chunk_index}",
                )
                text_to_session_ids.setdefault(chunk, []).append(source_session_id)
                if args.sleep_between_messages > 0:
                    time.sleep(args.sleep_between_messages)

    client.wait_for_embeddings(
        metric_name=args.queue_metric_name,
        timeout_secs=args.wait_timeout_secs,
        settle_secs=args.queue_settle_secs,
        poll_interval_secs=args.queue_poll_interval_secs,
    )
    return engram_session_id, text_to_session_ids


def build_ranked_items(
    search_results: Sequence[Mapping[str, Any]],
    text_to_session_ids: Mapping[str, Sequence[str]],
) -> List[Dict[str, Any]]:
    ranked_items: List[Dict[str, Any]] = []
    for rank, result in enumerate(search_results, start=1):
        text = str(result.get("text", ""))
        ranked_items.append(
            {
                "rank": rank,
                "text": text,
                "score": float(result.get("score", 0.0)),
                "source_session_ids": ordered_unique(text_to_session_ids.get(text, [])),
            }
        )
    return ranked_items


def build_ranked_session_ids(ranked_items: Sequence[Mapping[str, Any]]) -> List[str]:
    ranked_session_ids: List[str] = []
    for item in ranked_items:
        ranked_session_ids.extend(item.get("source_session_ids", []))
    return ordered_unique(ranked_session_ids)


def per_entry_retrieval_metrics(
    ranked_session_ids: Sequence[str],
    answer_session_ids: Sequence[str],
) -> Dict[str, float]:
    return {
        "session_recall_any@5": recall_any_at_k(ranked_session_ids, answer_session_ids, 5),
        "session_recall_any@10": recall_any_at_k(ranked_session_ids, answer_session_ids, 10),
        "session_recall_all@5": recall_all_at_k(ranked_session_ids, answer_session_ids, 5),
        "session_recall_all@10": recall_all_at_k(ranked_session_ids, answer_session_ids, 10),
        "session_mrr": reciprocal_rank(ranked_session_ids, answer_session_ids),
        "session_ndcg@10": ndcg_at_k(ranked_session_ids, answer_session_ids, 10),
    }


def load_existing_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    data = load_json(path)
    if not isinstance(data, list):
        raise BenchError(f"expected a list in {path}")
    return data


def load_existing_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
    return rows


def render_partial_summary_markdown(
    *,
    dataset_name: str,
    tier: str,
    results_file: str,
    processed_questions: int,
) -> str:
    tier_label = tier if tier else "all"
    return "\n".join(
        [
            "# BEAM Retrieval Summary",
            "",
            f"- Dataset: `{dataset_name}`",
            f"- Tier: `{tier_label}`",
            f"- Results file: `{results_file}`",
            f"- Questions processed: {processed_questions}",
            "",
            "This run captured retrieval outputs, but the dataset did not expose session-level gold labels under the configured keys, so recall and ranking metrics were not computed.",
        ]
    ) + "\n"


def run_retrieval_mode(args: argparse.Namespace, dataset: Sequence[Mapping[str, Any]]) -> None:
    ensure_directory(args.output_dir)
    results_path = args.output_dir / "beam_retrieval_results.json"
    metrics_path = args.output_dir / "beam_retrieval_metrics.json"
    summary_path = args.output_dir / "beam_retrieval_summary.md"

    existing_results = load_existing_json(results_path) if args.resume else []
    completed_ids = {item["question_id"] for item in existing_results}
    results = list(existing_results)

    process = None
    client = EngramClient(args.engram_url, timeout_secs=args.timeout_secs)
    try:
        if args.start_engram:
            process = start_engram_process(args.engram_command, args.engram_cwd)
        client.wait_until_healthy()

        selected_entries = [entry for entry in dataset if should_include_entry(entry, args)]
        total = len(selected_entries)
        for index, entry in enumerate(selected_entries, start=1):
            question_id = infer_scalar(entry, args.question_id_key, QUESTION_ID_KEYS, f"beam-q-{index}")
            if question_id in completed_ids:
                continue
            if index == 1 or index % args.progress_every == 0:
                log(f"[BEAM][retrieval] processing {index}/{total}: {question_id}")

            engram_session_id = None
            try:
                engram_session_id, text_to_session_ids = ingest_entry(client, entry, args)
                question = infer_scalar(entry, args.question_key, QUESTION_KEYS, "")
                answer = infer_scalar(entry, args.answer_key, ANSWER_KEYS, "")
                tier_key = choose_key(entry, args.tier_key, TIER_KEYS)
                tier_value = str(entry[tier_key]) if tier_key else ""
                answer_session_ids = [
                    str(value)
                    for value in infer_list(entry, args.answer_session_ids_key, TARGET_SESSION_ID_KEYS)
                    or []
                ]
                is_abstention = bool(entry.get("is_abstention")) or question_id.endswith("_abs")

                search_results = client.search(engram_session_id, question, args.top_k)
                ranked_items = build_ranked_items(search_results, text_to_session_ids)
                ranked_session_ids = build_ranked_session_ids(ranked_items)
                metrics = per_entry_retrieval_metrics(ranked_session_ids, answer_session_ids)

                result = {
                    "question_id": question_id,
                    "question": question,
                    "answer": answer,
                    "tier": tier_value,
                    "is_abstention": is_abstention,
                    "answer_session_ids": answer_session_ids,
                    "ranked_session_ids": ranked_session_ids,
                    "ranked_items": ranked_items,
                    "metrics": metrics,
                }
                results.append(result)
                completed_ids.add(question_id)
                save_json(results_path, results)
            finally:
                if engram_session_id and not args.keep_sessions:
                    try:
                        client.delete_session(engram_session_id)
                    except BenchError as error:
                        log(f"warning: failed to delete benchmark session {engram_session_id}: {error}")

        metrics_eligible = [item for item in results if item.get("answer_session_ids")]
        if metrics_eligible:
            summary = summarize_retrieval_results(results)
            save_json(metrics_path, summary)
            save_markdown(
                summary_path,
                render_retrieval_summary_markdown(
                    "BEAM Retrieval Summary",
                    summary,
                    dataset_name=args.dataset.name,
                    results_file=results_path.name,
                ),
            )
        else:
            save_json(
                metrics_path,
                {
                    "questions_total": len(results),
                    "questions_with_gold_labels": 0,
                    "note": "No answer_session_ids-style field was available; only raw retrieval outputs were recorded.",
                },
            )
            save_markdown(
                summary_path,
                render_partial_summary_markdown(
                    dataset_name=args.dataset.name,
                    tier=args.tier,
                    results_file=results_path.name,
                    processed_questions=len(results),
                ),
            )

        log(f"BEAM retrieval results written to {results_path}")
    finally:
        stop_process(process)


def run_qa_mode(args: argparse.Namespace, dataset: Sequence[Mapping[str, Any]]) -> None:
    ensure_directory(args.output_dir)
    hypothesis_path = args.output_dir / "beam_hypothesis.jsonl"
    details_path = args.output_dir / "beam_qa_details.json"
    summary_path = args.output_dir / "beam_qa_summary.md"

    hypotheses = load_existing_jsonl(hypothesis_path) if args.resume else []
    details = load_existing_json(details_path) if args.resume else []
    completed_ids = {item["question_id"] for item in hypotheses}

    process = None
    client = EngramClient(args.engram_url, timeout_secs=args.timeout_secs)
    try:
        if args.start_engram:
            process = start_engram_process(args.engram_command, args.engram_cwd)
        client.wait_until_healthy()

        selected_entries = [entry for entry in dataset if should_include_entry(entry, args)]
        total = len(selected_entries)
        for index, entry in enumerate(selected_entries, start=1):
            question_id = infer_scalar(entry, args.question_id_key, QUESTION_ID_KEYS, f"beam-q-{index}")
            if question_id in completed_ids:
                continue
            if index == 1 or index % args.progress_every == 0:
                log(f"[BEAM][qa] processing {index}/{total}: {question_id}")

            engram_session_id = None
            try:
                engram_session_id, _ = ingest_entry(client, entry, args)
                question = infer_scalar(entry, args.question_key, QUESTION_KEYS, "")
                answer = infer_scalar(entry, args.answer_key, ANSWER_KEYS, "")
                context = client.get_context(engram_session_id, args.max_tokens)
                hypothesis = run_openai_compatible_qa(
                    question=question,
                    context=context,
                    model=args.llm_model,
                    api_key_env=args.llm_api_key_env,
                    base_url=args.llm_base_url,
                    timeout_secs=args.timeout_secs,
                )
                hypotheses.append({"question_id": question_id, "hypothesis": hypothesis})
                details.append(
                    {
                        "question_id": question_id,
                        "question": question,
                        "answer": answer,
                        "context": context,
                        "hypothesis": hypothesis,
                    }
                )
                completed_ids.add(question_id)
                save_jsonl(hypothesis_path, hypotheses)
                save_json(details_path, details)
            finally:
                if engram_session_id and not args.keep_sessions:
                    try:
                        client.delete_session(engram_session_id)
                    except BenchError as error:
                        log(f"warning: failed to delete benchmark session {engram_session_id}: {error}")

        save_markdown(
            summary_path,
            "\n".join(
                [
                    "# BEAM QA Summary",
                    "",
                    f"- Dataset: `{args.dataset.name}`",
                    f"- Tier: `{args.tier or 'all'}`",
                    f"- Hypothesis file: `{hypothesis_path.name}`",
                    f"- Detailed outputs: `{details_path.name}`",
                    f"- Questions answered: {len(hypotheses)}",
                    "",
                    "The output file is ready for BEAM-specific downstream evaluation.",
                ]
            )
            + "\n",
        )
        log(f"BEAM QA hypotheses written to {hypothesis_path}")
    finally:
        stop_process(process)


def main() -> None:
    args = parse_args()
    dataset = load_dataset(args.dataset, args.limit)
    if args.mode == "retrieval":
        run_retrieval_mode(args, dataset)
    else:
        run_qa_mode(args, dataset)


if __name__ == "__main__":
    main()