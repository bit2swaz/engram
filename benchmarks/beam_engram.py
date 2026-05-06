#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from quality_common import (  # noqa: E402
    BenchError,
    DEFAULT_LOCAL_EMBED_BASE_URL,
    DEFAULT_LOCAL_EMBED_COMMAND,
    DEFAULT_LOCAL_EMBED_ENV_OVERRIDES,
    DEFAULT_LOCAL_EMBED_HEALTH_URL,
    DEFAULT_QUEUE_METRIC_NAME,
    EngramClient,
    build_engram_env_overrides,
    ensure_directory,
    load_json,
    log,
    ndcg_at_k,
    ordered_unique,
    recall_all_at_k,
    recall_any_at_k,
    reciprocal_rank,
    render_retrieval_summary_markdown,
    resolve_embedding_dimension,
    run_openai_compatible_qa,
    save_json,
    save_jsonl,
    save_markdown,
    split_text_for_ingestion,
    start_process,
    start_engram_process,
    stop_process,
    summarize_retrieval_results,
    wait_for_http_ok,
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
BEAM_TIER_ALIASES = {
    "128K": "100K",
    "100K": "100K",
    "500K": "500K",
    "1M": "1M",
    "10M": "10M",
}
SUPPORTED_BEAM_CHAT_ROOTS = {"100K", "500K", "1M"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BEAM-style retrieval or QA evaluation against engram.",
    )
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument(
        "--dataset-format",
        choices=["auto", "json", "beam-chats"],
        default="auto",
        help="Interpret --dataset as a flat JSON file or a BEAM chat directory tree.",
    )
    parser.add_argument(
        "--mode",
        choices=["retrieval", "qa"],
        default="retrieval",
    )
    parser.add_argument("--engram-url", default="http://127.0.0.1:3002")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--max-tokens", type=int, default=4096)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--timeout-secs", type=float, default=30.0)
    parser.add_argument(
        "--health-timeout-secs",
        type=float,
        default=120.0,
        help="How long to wait for started HTTP services to become healthy.",
    )
    parser.add_argument("--wait-timeout-secs", type=float, default=120.0)
    parser.add_argument("--queue-metric-name", default=DEFAULT_QUEUE_METRIC_NAME)
    parser.add_argument("--queue-settle-secs", type=float, default=2.0)
    parser.add_argument("--queue-poll-interval-secs", type=float, default=1.0)
    parser.add_argument("--sleep-between-messages", type=float, default=0.0)
    parser.add_argument("--keep-sessions", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--start-engram", action="store_true")
    parser.add_argument(
        "--start-local-embed-server",
        action="store_true",
        help="Start a local OpenAI-compatible embedding server before starting engram.",
    )
    parser.add_argument(
        "--local-embed-command",
        default=DEFAULT_LOCAL_EMBED_COMMAND,
        help="Command used when --start-local-embed-server is enabled.",
    )
    parser.add_argument(
        "--local-embed-cwd",
        type=Path,
        default=SCRIPT_DIR.parent,
        help="Working directory used when --start-local-embed-server is enabled.",
    )
    parser.add_argument(
        "--local-embed-base-url",
        default=DEFAULT_LOCAL_EMBED_BASE_URL,
        help="OpenAI-compatible base URL exposed by the local embedding server.",
    )
    parser.add_argument(
        "--local-embed-health-url",
        default=DEFAULT_LOCAL_EMBED_HEALTH_URL,
        help="Health endpoint used to wait for the local embedding server.",
    )
    parser.add_argument(
        "--embedding-dimension",
        type=int,
        default=None,
        help="Embedding width expected by engram; defaults to 384 when --start-local-embed-server is enabled.",
    )
    parser.add_argument(
        "--lance-db-path",
        type=Path,
        default=None,
        help="Optional LANCE_DB_PATH override passed to a started engram process.",
    )
    parser.add_argument(
        "--engram-command",
        default="env ENGRAM_BIND_ADDR=127.0.0.1:3002 cargo run --release",
    )
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
    parser.add_argument(
        "--result-file-name",
        default="engram_answers.json",
        help="Result filename used when writing BEAM-style per-conversation QA outputs.",
    )
    parser.add_argument(
        "--copy-probing-questions",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Copy BEAM probing question files into the QA output tree for downstream evaluation.",
    )
    parser.add_argument("--llm-model", default="gpt-4o")
    parser.add_argument("--llm-base-url", default="https://api.openai.com/v1")
    parser.add_argument("--llm-api-key-env", default="OPENAI_API_KEY")
    return parser.parse_args()


def start_benchmark_processes(
    args: argparse.Namespace,
) -> Tuple[Optional[object], Optional[object]]:
    local_embed_process = None
    engram_process = None

    embedding_dimension = resolve_embedding_dimension(
        args.embedding_dimension,
        use_local_embed_server=args.start_local_embed_server,
    )
    engram_env_overrides = build_engram_env_overrides(
        embedding_dimension=embedding_dimension,
        openai_base_url=args.local_embed_base_url if args.start_local_embed_server else None,
        lance_db_path=args.lance_db_path,
    )

    if args.start_local_embed_server:
        local_embed_process = start_process(
            args.local_embed_command,
            args.local_embed_cwd,
            env_overrides=DEFAULT_LOCAL_EMBED_ENV_OVERRIDES,
        )
        wait_for_http_ok(
            args.local_embed_health_url,
            timeout_secs=args.health_timeout_secs,
            process=local_embed_process,
            service_name="local embedding server",
        )

    if args.start_engram:
        engram_process = start_engram_process(
            args.engram_command,
            args.engram_cwd,
            env_overrides=engram_env_overrides or None,
        )

    return local_embed_process, engram_process


def canonical_tier(value: str) -> str:
    normalized = value.strip().upper()
    return BEAM_TIER_ALIASES.get(normalized, normalized)


def choose_key(entry: Mapping[str, Any], explicit_key: str, candidates: Sequence[str]) -> Optional[str]:
    if explicit_key:
        if explicit_key not in entry:
            raise BenchError(f"requested key {explicit_key!r} was not found in dataset entry")
        return explicit_key
    for candidate in candidates:
        if candidate in entry:
            return candidate
    return None


def sort_paths(paths: Sequence[Path]) -> List[Path]:
    def sort_key(path: Path) -> Tuple[int, Any]:
        name = path.name
        if name.isdigit():
            return (0, int(name))
        return (1, name)

    return sorted(paths, key=sort_key)


def load_json_dataset(dataset_path: Path, limit: int) -> List[Dict[str, Any]]:
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


def conversation_payload_to_sessions(
    conversation_id: str,
    payload: Any,
) -> List[Tuple[str, Optional[str], List[Dict[str, Any]]]]:
    if not isinstance(payload, list):
        raise BenchError(f"chat payload for {conversation_id} must be a list")

    sessions: List[Tuple[str, Optional[str], List[Dict[str, Any]]]] = []
    for batch_index, batch in enumerate(payload, start=1):
        if not isinstance(batch, Mapping):
            raise BenchError(f"batch {batch_index} in {conversation_id} is not an object")

        turns = batch.get("turns")
        if not isinstance(turns, list):
            raise BenchError(f"batch {batch_index} in {conversation_id} is missing turns")

        messages: List[Dict[str, Any]] = []
        for turn in turns:
            if not isinstance(turn, list):
                raise BenchError(f"turn in {conversation_id} batch {batch_index} is not a list")
            for message in turn:
                if not isinstance(message, Mapping):
                    continue
                if "role" not in message or "content" not in message:
                    continue
                messages.append(normalize_turn(message))

        if not messages:
            continue

        batch_number = batch.get("batch_number", batch_index)
        session_id = f"{conversation_id}:batch-{batch_number}"
        session_date = batch.get("time_anchor")
        sessions.append((session_id, str(session_date) if session_date else None, messages))

    return sessions


def beam_root_name(dataset_path: Path) -> str:
    if dataset_path.name == "chats" and dataset_path.parent.name:
        return dataset_path.parent.name
    return dataset_path.name


def resolve_beam_chat_dirs(dataset_path: Path, tier: str) -> Tuple[Path, List[Path], str]:
    if not dataset_path.exists():
        raise BenchError(f"dataset path does not exist: {dataset_path}")

    if dataset_path.is_file():
        raise BenchError("BEAM chat layout requires a directory, not a file")

    if (dataset_path / "chat.json").exists() and (
        dataset_path / "probing_questions" / "probing_questions.json"
    ).exists():
        resolved_tier = canonical_tier(dataset_path.parent.name)
        if resolved_tier == "10M":
            raise BenchError("BEAM 10M conversations are not yet supported by this bridge")
        return dataset_path.parent, [dataset_path], resolved_tier

    candidate_root = dataset_path / "chats" if (dataset_path / "chats").is_dir() else dataset_path
    available_tiers = [
        path for path in candidate_root.iterdir() if path.is_dir() and canonical_tier(path.name) in BEAM_TIER_ALIASES.values()
    ]
    if available_tiers:
        requested_tier = canonical_tier(tier) if tier else ""
        if not requested_tier:
            if len(available_tiers) != 1:
                available = ", ".join(path.name for path in sort_paths(available_tiers))
                raise BenchError(
                    f"dataset root {candidate_root} contains multiple BEAM tiers ({available}); set --tier to choose one"
                )
            selected_root = available_tiers[0]
        else:
            selected_root = candidate_root / requested_tier
            if not selected_root.is_dir():
                raise BenchError(
                    f"tier {requested_tier} was requested but {selected_root} does not exist"
                )
        resolved_tier = canonical_tier(selected_root.name)
        if resolved_tier == "10M":
            raise BenchError("BEAM 10M conversations are not yet supported by this bridge")
        conversation_dirs = [
            path
            for path in selected_root.iterdir()
            if path.is_dir() and (path / "chat.json").exists() and (path / "probing_questions" / "probing_questions.json").exists()
        ]
        return selected_root, sort_paths(conversation_dirs), resolved_tier

    conversation_dirs = [
        path
        for path in dataset_path.iterdir()
        if path.is_dir() and (path / "chat.json").exists() and (path / "probing_questions" / "probing_questions.json").exists()
    ]
    if conversation_dirs:
        resolved_tier = canonical_tier(beam_root_name(dataset_path))
        if resolved_tier == "10M":
            raise BenchError("BEAM 10M conversations are not yet supported by this bridge")
        if resolved_tier not in SUPPORTED_BEAM_CHAT_ROOTS:
            raise BenchError(
                f"could not infer a supported BEAM tier from {dataset_path}; pass a 100K, 500K, or 1M directory or use --tier"
            )
        return dataset_path, sort_paths(conversation_dirs), resolved_tier

    raise BenchError(
        f"could not interpret {dataset_path} as BEAM chat data; expected a flat JSON file or a directory containing chat.json and probing_questions/probing_questions.json files"
    )


def load_beam_chat_dataset(dataset_path: Path, tier: str, limit: int) -> List[Dict[str, Any]]:
    _, conversation_dirs, resolved_tier = resolve_beam_chat_dirs(dataset_path, tier)
    if limit > 0:
        conversation_dirs = conversation_dirs[:limit]

    entries: List[Dict[str, Any]] = []
    for conversation_dir in conversation_dirs:
        chat_path = conversation_dir / "chat.json"
        probing_questions_path = conversation_dir / "probing_questions" / "probing_questions.json"

        sessions = conversation_payload_to_sessions(conversation_dir.name, load_json(chat_path))
        probing_questions = load_json(probing_questions_path)
        if not isinstance(probing_questions, dict):
            raise BenchError(f"probing questions at {probing_questions_path} must be an object")

        session_ids = [session_id for session_id, _, _ in sessions]
        session_dates = [session_date for _, session_date, _ in sessions]
        normalized_sessions = [session for _, _, session in sessions]

        for question_group, questions in probing_questions.items():
            if not isinstance(questions, list):
                continue
            for question_index, question in enumerate(questions):
                if not isinstance(question, Mapping):
                    continue
                question_text = str(question.get("question", "")).strip()
                if not question_text:
                    continue
                answer = ""
                if "answer" in question and question.get("answer") is not None:
                    answer = str(question["answer"])
                elif "rubric" in question:
                    answer = json.dumps(question.get("rubric", []), ensure_ascii=True)

                entries.append(
                    {
                        "question_id": f"{conversation_dir.name}:{question_group}:{question_index}",
                        "question_type": str(question_group),
                        "question": question_text,
                        "answer": answer,
                        "tier": resolved_tier,
                        "sessions": normalized_sessions,
                        "session_ids": session_ids,
                        "dates": session_dates,
                        "beam_conversation_id": conversation_dir.name,
                        "beam_question_group": str(question_group),
                        "beam_question_index": question_index,
                        "beam_probing_questions_path": str(probing_questions_path),
                    }
                )
    return entries


def load_dataset(args: argparse.Namespace) -> List[Dict[str, Any]]:
    dataset_path = args.dataset
    dataset_format = args.dataset_format

    if dataset_format == "json":
        return load_json_dataset(dataset_path, args.limit)

    if dataset_format == "beam-chats":
        return load_beam_chat_dataset(dataset_path, args.tier, args.limit)

    if dataset_path.is_file():
        return load_json_dataset(dataset_path, args.limit)
    return load_beam_chat_dataset(dataset_path, args.tier, args.limit)


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
    requested_tier = canonical_tier(args.tier)
    tier_key = choose_key(entry, args.tier_key, TIER_KEYS)
    if tier_key is None:
        raise BenchError("--tier was provided but no tier field could be inferred")
    return canonical_tier(str(entry[tier_key])) == requested_tier


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
        session_enqueued = False
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
            session_enqueued = True

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
                session_enqueued = True
                if args.sleep_between_messages > 0:
                    time.sleep(args.sleep_between_messages)

        if session_enqueued:
            client.wait_for_embeddings(
                metric_name=args.queue_metric_name,
                timeout_secs=args.wait_timeout_secs,
                settle_secs=args.queue_settle_secs,
                poll_interval_secs=args.queue_poll_interval_secs,
            )

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
            "This run captured retrieval outputs, but BEAM does not expose session-level gold labels in the same way as LongMemEval, so recall and ranking metrics were not computed for this slice.",
        ]
    ) + "\n"


def is_beam_chat_entry(entry: Mapping[str, Any]) -> bool:
    return "beam_conversation_id" in entry and "beam_probing_questions_path" in entry


def load_beam_answer_payload(path: Path) -> Dict[str, Any]:
    data = load_json(path)
    if not isinstance(data, dict):
        raise BenchError(f"BEAM probing questions file at {path} must be an object")
    payload: Dict[str, Any] = {}
    for question_group, questions in data.items():
        if not isinstance(questions, list):
            continue
        payload[question_group] = [dict(question) for question in questions if isinstance(question, Mapping)]
    return payload


def update_beam_conversation_output(
    entry: Mapping[str, Any],
    hypothesis: str,
    output_dir: Path,
    result_file_name: str,
    copy_probing_questions: bool,
) -> None:
    if not is_beam_chat_entry(entry):
        return

    conversation_id = str(entry["beam_conversation_id"])
    question_group = str(entry["beam_question_group"])
    question_index = int(entry["beam_question_index"])
    probing_questions_source = Path(str(entry["beam_probing_questions_path"]))

    conversation_dir = output_dir / conversation_id
    ensure_directory(conversation_dir)

    if copy_probing_questions:
        probing_questions_target = conversation_dir / "probing_questions" / "probing_questions.json"
        ensure_directory(probing_questions_target.parent)
        if not probing_questions_target.exists():
            shutil.copy2(probing_questions_source, probing_questions_target)

    result_path = conversation_dir / result_file_name
    if result_path.exists():
        result_payload = load_beam_answer_payload(result_path)
    else:
        result_payload = load_beam_answer_payload(probing_questions_source)

    question_group_entries = result_payload.get(question_group)
    if not isinstance(question_group_entries, list) or question_index >= len(question_group_entries):
        raise BenchError(
            f"could not locate {question_group}[{question_index}] when writing BEAM output for conversation {conversation_id}"
        )

    question_group_entries[question_index]["llm_response"] = hypothesis
    save_json(result_path, result_payload)


def run_retrieval_mode(args: argparse.Namespace, dataset: Sequence[Mapping[str, Any]]) -> None:
    ensure_directory(args.output_dir)
    results_path = args.output_dir / "beam_retrieval_results.json"
    metrics_path = args.output_dir / "beam_retrieval_metrics.json"
    summary_path = args.output_dir / "beam_retrieval_summary.md"

    existing_results = load_existing_json(results_path) if args.resume else []
    completed_ids = {item["question_id"] for item in existing_results}
    results = list(existing_results)

    local_embed_process = None
    engram_process = None
    client = EngramClient(args.engram_url, timeout_secs=args.timeout_secs)
    try:
        local_embed_process, engram_process = start_benchmark_processes(args)
        client.wait_until_healthy(
            timeout_secs=args.health_timeout_secs,
            process=engram_process,
        )

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
        stop_process(engram_process)
        stop_process(local_embed_process)


def run_qa_mode(args: argparse.Namespace, dataset: Sequence[Mapping[str, Any]]) -> None:
    ensure_directory(args.output_dir)
    hypothesis_path = args.output_dir / "beam_hypothesis.jsonl"
    details_path = args.output_dir / "beam_qa_details.json"
    summary_path = args.output_dir / "beam_qa_summary.md"

    hypotheses = load_existing_jsonl(hypothesis_path) if args.resume else []
    details = load_existing_json(details_path) if args.resume else []
    completed_ids = {item["question_id"] for item in hypotheses}

    local_embed_process = None
    engram_process = None
    client = EngramClient(args.engram_url, timeout_secs=args.timeout_secs)
    try:
        local_embed_process, engram_process = start_benchmark_processes(args)
        client.wait_until_healthy(
            timeout_secs=args.health_timeout_secs,
            process=engram_process,
        )

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
                update_beam_conversation_output(
                    entry,
                    hypothesis,
                    args.output_dir,
                    args.result_file_name,
                    args.copy_probing_questions,
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
                    f"- Per-conversation BEAM answers file: `{args.result_file_name}`",
                    "",
                    "If the input was a BEAM chat directory, the output directory now mirrors the conversation layout closely enough for BEAM's evaluator to read `engram_answers.json` alongside copied probing questions.",
                ]
            )
            + "\n",
        )
        log(f"BEAM QA hypotheses written to {hypothesis_path}")
    finally:
        stop_process(engram_process)
        stop_process(local_embed_process)


def main() -> None:
    args = parse_args()
    dataset = load_dataset(args)
    if args.mode == "retrieval":
        run_retrieval_mode(args, dataset)
    else:
        run_qa_mode(args, dataset)


if __name__ == "__main__":
    main()