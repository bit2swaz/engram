#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
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
    start_engram_process,
    stop_process,
    summarize_retrieval_results,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run LongMemEval retrieval or QA evaluation against engram.",
    )
    parser.add_argument("--dataset", required=True, type=Path, help="Path to longmemeval_*.json")
    parser.add_argument(
        "--mode",
        choices=["retrieval", "qa"],
        default="retrieval",
        help="Evaluation mode to run.",
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
    parser.add_argument(
        "--engram-cwd",
        type=Path,
        default=SCRIPT_DIR.parent,
        help="Working directory used when --start-engram is enabled.",
    )
    parser.add_argument(
        "--inject-session-markers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Inject system messages that preserve LongMemEval session ids and dates.",
    )
    parser.add_argument("--llm-model", default="gpt-4o")
    parser.add_argument("--llm-base-url", default="https://api.openai.com/v1")
    parser.add_argument("--llm-api-key-env", default="OPENAI_API_KEY")
    return parser.parse_args()


def load_dataset(dataset_path: Path, limit: int) -> List[Dict[str, Any]]:
    data = load_json(dataset_path)
    if not isinstance(data, list):
        raise BenchError(f"dataset at {dataset_path} must be a JSON array")
    if limit > 0:
        return data[:limit]
    return data


def validate_entry(entry: Mapping[str, Any]) -> None:
    required_fields = [
        "question_id",
        "question_type",
        "question",
        "answer",
        "haystack_session_ids",
        "haystack_dates",
        "haystack_sessions",
        "answer_session_ids",
    ]
    missing = [field for field in required_fields if field not in entry]
    if missing:
        raise BenchError(f"dataset entry is missing required fields: {missing}")


def ordered_sessions(entry: Mapping[str, Any]) -> List[Tuple[str, Optional[str], List[Mapping[str, Any]]]]:
    session_ids = list(entry["haystack_session_ids"])
    session_dates = list(entry["haystack_dates"])
    sessions = list(entry["haystack_sessions"])

    if not (len(session_ids) == len(session_dates) == len(sessions)):
        raise BenchError(
            f"entry {entry['question_id']} has mismatched haystack lengths"
        )

    ordered = []
    for session_id, session_date, turns in zip(session_ids, session_dates, sessions):
        if not isinstance(turns, list):
            raise BenchError(f"session {session_id} is not a list of turns")
        ordered.append((str(session_id), str(session_date) if session_date else None, turns))
    return ordered


def ingest_entry(
    client: EngramClient,
    entry: Mapping[str, Any],
    *,
    inject_session_markers: bool,
    sleep_between_messages: float,
    wait_timeout_secs: float,
    queue_metric_name: str,
    queue_settle_secs: float,
    queue_poll_interval_secs: float,
) -> Tuple[str, Dict[str, List[str]]]:
    engram_session_id = client.create_session()
    text_to_session_ids: Dict[str, List[str]] = {}

    message_counter = 0
    for session_index, (source_session_id, session_date, turns) in enumerate(
        ordered_sessions(entry),
        start=1,
    ):
        if inject_session_markers:
            marker_text = (
                f"[engram-longmemeval] session={session_index} "
                f"source_id={source_session_id} date={session_date or 'unknown'}"
            )
            client.add_message(
                engram_session_id,
                "system",
                marker_text,
                message_id=f"marker-{session_index}",
            )
            text_to_session_ids.setdefault(marker_text, []).append(source_session_id)

        for turn_index, turn in enumerate(turns, start=1):
            role = str(turn.get("role", "user"))
            content = str(turn.get("content", "")).strip()
            if not content:
                continue
            message_counter += 1
            client.add_message(
                engram_session_id,
                role,
                content,
                message_id=f"msg-{session_index}-{turn_index}-{message_counter}",
            )
            text_to_session_ids.setdefault(content, []).append(source_session_id)
            if sleep_between_messages > 0:
                import time

                time.sleep(sleep_between_messages)

    client.wait_for_embeddings(
        metric_name=queue_metric_name,
        timeout_secs=wait_timeout_secs,
        settle_secs=queue_settle_secs,
        poll_interval_secs=queue_poll_interval_secs,
    )
    return engram_session_id, text_to_session_ids


def build_ranked_items(
    search_results: Sequence[Mapping[str, Any]],
    text_to_session_ids: Mapping[str, Sequence[str]],
) -> List[Dict[str, Any]]:
    ranked_items: List[Dict[str, Any]] = []
    for rank, result in enumerate(search_results, start=1):
        text = str(result.get("text", ""))
        score = float(result.get("score", 0.0))
        source_ids = ordered_unique(text_to_session_ids.get(text, []))
        ranked_items.append(
            {
                "rank": rank,
                "text": text,
                "score": score,
                "source_session_ids": source_ids,
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


def load_existing_retrieval_results(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    data = load_json(path)
    if not isinstance(data, list):
        raise BenchError(f"existing retrieval file at {path} must contain a list")
    return data


def load_existing_hypotheses(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            rows.append(__import__("json").loads(stripped))
    return rows


def run_retrieval_mode(args: argparse.Namespace, dataset: Sequence[Mapping[str, Any]]) -> None:
    ensure_directory(args.output_dir)
    results_path = args.output_dir / "retrieval_results.json"
    metrics_path = args.output_dir / "retrieval_metrics.json"
    summary_path = args.output_dir / "retrieval_summary.md"

    existing_results = load_existing_retrieval_results(results_path) if args.resume else []
    completed_ids = {item["question_id"] for item in existing_results}
    all_results: List[Dict[str, Any]] = list(existing_results)

    process = None
    client = EngramClient(args.engram_url, timeout_secs=args.timeout_secs)
    try:
        if args.start_engram:
            process = start_engram_process(args.engram_command, args.engram_cwd)
        client.wait_until_healthy()

        total = len(dataset)
        for index, entry in enumerate(dataset, start=1):
            validate_entry(entry)
            question_id = str(entry["question_id"])
            if question_id in completed_ids:
                continue

            if index == 1 or index % args.progress_every == 0:
                log(f"[LongMemEval][retrieval] processing {index}/{total}: {question_id}")

            engram_session_id = None
            try:
                engram_session_id, text_to_session_ids = ingest_entry(
                    client,
                    entry,
                    inject_session_markers=args.inject_session_markers,
                    sleep_between_messages=args.sleep_between_messages,
                    wait_timeout_secs=args.wait_timeout_secs,
                    queue_metric_name=args.queue_metric_name,
                    queue_settle_secs=args.queue_settle_secs,
                    queue_poll_interval_secs=args.queue_poll_interval_secs,
                )
                search_results = client.search(
                    engram_session_id, str(entry["question"]), args.top_k
                )
                ranked_items = build_ranked_items(search_results, text_to_session_ids)
                ranked_session_ids = build_ranked_session_ids(ranked_items)
                answer_session_ids = [str(value) for value in entry.get("answer_session_ids", [])]
                is_abstention = question_id.endswith("_abs")
                metrics = per_entry_retrieval_metrics(ranked_session_ids, answer_session_ids)

                result = {
                    "question_id": question_id,
                    "question_type": str(entry["question_type"]),
                    "question": str(entry["question"]),
                    "answer": str(entry["answer"]),
                    "question_date": entry.get("question_date"),
                    "is_abstention": is_abstention,
                    "answer_session_ids": answer_session_ids,
                    "ranked_session_ids": ranked_session_ids,
                    "ranked_items": ranked_items,
                    "metrics": metrics,
                }
                all_results.append(result)
                completed_ids.add(question_id)
                save_json(results_path, all_results)
            finally:
                if engram_session_id and not args.keep_sessions:
                    try:
                        client.delete_session(engram_session_id)
                    except BenchError as error:
                        log(f"warning: failed to delete benchmark session {engram_session_id}: {error}")

        summary = summarize_retrieval_results(all_results)
        save_json(metrics_path, summary)
        save_markdown(
            summary_path,
            render_retrieval_summary_markdown(
                "LongMemEval Retrieval Summary",
                summary,
                dataset_name=args.dataset.name,
                results_file=results_path.name,
            ),
        )
        log(f"LongMemEval retrieval results written to {results_path}")
        log(f"LongMemEval retrieval metrics written to {metrics_path}")
    finally:
        stop_process(process)


def run_qa_mode(args: argparse.Namespace, dataset: Sequence[Mapping[str, Any]]) -> None:
    ensure_directory(args.output_dir)
    hypothesis_path = args.output_dir / "hypothesis.jsonl"
    details_path = args.output_dir / "qa_details.json"
    summary_path = args.output_dir / "qa_summary.md"

    existing_hypotheses = load_existing_hypotheses(hypothesis_path) if args.resume else []
    completed_ids = {item["question_id"] for item in existing_hypotheses}
    details: List[Dict[str, Any]] = []
    process = None
    client = EngramClient(args.engram_url, timeout_secs=args.timeout_secs)
    try:
        if args.start_engram:
            process = start_engram_process(args.engram_command, args.engram_cwd)
        client.wait_until_healthy()

        total = len(dataset)
        hypotheses = list(existing_hypotheses)
        for index, entry in enumerate(dataset, start=1):
            validate_entry(entry)
            question_id = str(entry["question_id"])
            if question_id in completed_ids:
                continue

            if index == 1 or index % args.progress_every == 0:
                log(f"[LongMemEval][qa] processing {index}/{total}: {question_id}")

            engram_session_id = None
            try:
                engram_session_id, _ = ingest_entry(
                    client,
                    entry,
                    inject_session_markers=args.inject_session_markers,
                    sleep_between_messages=args.sleep_between_messages,
                    wait_timeout_secs=args.wait_timeout_secs,
                    queue_metric_name=args.queue_metric_name,
                    queue_settle_secs=args.queue_settle_secs,
                    queue_poll_interval_secs=args.queue_poll_interval_secs,
                )
                context = client.get_context(engram_session_id, args.max_tokens)
                hypothesis = run_openai_compatible_qa(
                    question=str(entry["question"]),
                    context=context,
                    model=args.llm_model,
                    api_key_env=args.llm_api_key_env,
                    base_url=args.llm_base_url,
                    timeout_secs=args.timeout_secs,
                    question_date=str(entry.get("question_date"))
                    if entry.get("question_date")
                    else None,
                )

                hypotheses.append({"question_id": question_id, "hypothesis": hypothesis})
                details.append(
                    {
                        "question_id": question_id,
                        "question": str(entry["question"]),
                        "answer": str(entry["answer"]),
                        "question_type": str(entry["question_type"]),
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
                    "# LongMemEval QA Summary",
                    "",
                    f"- Dataset: `{args.dataset.name}`",
                    f"- Hypothesis file: `{hypothesis_path.name}`",
                    f"- Detailed outputs: `{details_path.name}`",
                    f"- Questions answered: {len(hypotheses)}",
                    "",
                    "Run the official LongMemEval evaluator against `hypothesis.jsonl` to obtain QA accuracy.",
                ]
            )
            + "\n",
        )
        log(f"LongMemEval QA hypotheses written to {hypothesis_path}")
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