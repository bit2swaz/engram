from __future__ import annotations

import json
import math
import os
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import requests


DEFAULT_TIMEOUT_SECS = 30.0
DEFAULT_HEALTH_TIMEOUT_SECS = 120.0
DEFAULT_QUEUE_SETTLE_SECS = 2.0
DEFAULT_QUEUE_POLL_INTERVAL_SECS = 1.0
DEFAULT_QUEUE_METRIC_NAME = "engram_memory_embedding_queue_size"


class BenchError(RuntimeError):
    pass


@dataclass
class RankedItem:
    text: str
    score: float
    source_ids: List[str]


def log(message: str) -> None:
    print(message, flush=True)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, payload: Any) -> None:
    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def save_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True))
            handle.write("\n")


def save_markdown(path: Path, content: str) -> None:
    ensure_directory(path.parent)
    path.write_text(content, encoding="utf-8")


def ordered_unique(values: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def parse_prometheus_metric(metrics_text: str, metric_name: str) -> Optional[float]:
    for line in metrics_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not stripped.startswith(metric_name):
            continue
        parts = stripped.split()
        if len(parts) != 2:
            continue
        try:
            return float(parts[1])
        except ValueError:
            continue
    return None


class EngramClient:
    def __init__(
        self,
        base_url: str,
        timeout_secs: float = DEFAULT_TIMEOUT_SECS,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_secs = timeout_secs
        self.session = session or requests.Session()

    def _request(
        self,
        method: str,
        path: str,
        *,
        expected_statuses: Sequence[int],
        **kwargs: Any,
    ) -> requests.Response:
        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            timeout=kwargs.pop("timeout", self.timeout_secs),
            **kwargs,
        )

        if response.status_code not in expected_statuses:
            detail = response.text.strip()
            raise BenchError(
                f"{method} {path} returned {response.status_code}: {detail}"
            )

        return response

    def wait_until_healthy(self, timeout_secs: float = DEFAULT_HEALTH_TIMEOUT_SECS) -> None:
        deadline = time.monotonic() + timeout_secs
        while time.monotonic() < deadline:
            try:
                response = self.session.get(
                    f"{self.base_url}/health", timeout=min(self.timeout_secs, 5.0)
                )
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(1.0)

        raise BenchError(
            f"engram did not become healthy within {timeout_secs:.0f}s at {self.base_url}"
        )

    def create_session(self) -> str:
        response = self._request("POST", "/sessions", expected_statuses=[200])
        body = response.json()
        session_id = body.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            raise BenchError("create_session returned no session_id")
        return session_id

    def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        *,
        message_id: Optional[str] = None,
        max_retries: int = 10,
        retry_delay_secs: float = 0.25,
    ) -> None:
        payload: Dict[str, Any] = {"role": role, "content": content}
        if message_id:
            payload["id"] = message_id

        current_delay = retry_delay_secs
        for attempt in range(max_retries):
            response = self.session.post(
                f"{self.base_url}/sessions/{session_id}/messages",
                json=payload,
                timeout=self.timeout_secs,
            )
            if response.status_code == 204:
                return
            if response.status_code == 503 and attempt < max_retries - 1:
                time.sleep(current_delay)
                current_delay = min(current_delay * 2.0, 2.0)
                continue

            detail = response.text.strip()
            raise BenchError(
                f"failed to add message after {attempt + 1} attempts: "
                f"{response.status_code} {detail}"
            )

        raise BenchError("failed to add message due to repeated queue backpressure")

    def search(self, session_id: str, query: str, top_k: int) -> List[Dict[str, Any]]:
        response = self._request(
            "POST",
            f"/sessions/{session_id}/search",
            expected_statuses=[200],
            json={"query": query, "top_k": top_k},
        )
        body = response.json()
        results = body.get("results")
        if not isinstance(results, list):
            raise BenchError("search response did not contain a results list")
        return results

    def get_context(self, session_id: str, max_tokens: int) -> str:
        response = self._request(
            "GET",
            f"/sessions/{session_id}/context",
            expected_statuses=[200],
            params={"max_tokens": max_tokens},
        )
        body = response.json()
        context = body.get("context")
        if not isinstance(context, str):
            raise BenchError("context response did not contain a string context")
        return context

    def delete_session(self, session_id: str) -> None:
        self._request("DELETE", f"/sessions/{session_id}", expected_statuses=[204])

    def metrics_text(self) -> str:
        response = self._request("GET", "/metrics", expected_statuses=[200])
        return response.text

    def wait_for_embeddings(
        self,
        *,
        metric_name: str = DEFAULT_QUEUE_METRIC_NAME,
        timeout_secs: float = 120.0,
        settle_secs: float = DEFAULT_QUEUE_SETTLE_SECS,
        poll_interval_secs: float = DEFAULT_QUEUE_POLL_INTERVAL_SECS,
    ) -> None:
        deadline = time.monotonic() + timeout_secs
        zero_since: Optional[float] = None

        while time.monotonic() < deadline:
            value = parse_prometheus_metric(self.metrics_text(), metric_name)
            if value is None:
                raise BenchError(f"metrics endpoint did not expose {metric_name}")

            if value <= 0:
                if zero_since is None:
                    zero_since = time.monotonic()
                elif time.monotonic() - zero_since >= settle_secs:
                    return
            else:
                zero_since = None

            time.sleep(poll_interval_secs)

        raise BenchError(
            f"embedding queue did not settle within {timeout_secs:.0f}s"
        )


def start_engram_process(command: str, cwd: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        shlex.split(command),
        cwd=str(cwd),
        env=os.environ.copy(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def stop_process(process: Optional[subprocess.Popen[str]]) -> None:
    if process is None or process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def reciprocal_rank(ranked_ids: Sequence[str], relevant_ids: Sequence[str]) -> float:
    relevant = set(relevant_ids)
    for index, item in enumerate(ranked_ids, start=1):
        if item in relevant:
            return 1.0 / index
    return 0.0


def recall_any_at_k(ranked_ids: Sequence[str], relevant_ids: Sequence[str], k: int) -> float:
    relevant = set(relevant_ids)
    if not relevant:
        return 0.0
    return 1.0 if any(item in relevant for item in ranked_ids[:k]) else 0.0


def recall_all_at_k(ranked_ids: Sequence[str], relevant_ids: Sequence[str], k: int) -> float:
    relevant = set(relevant_ids)
    if not relevant:
        return 0.0
    top_k = set(ranked_ids[:k])
    return 1.0 if relevant.issubset(top_k) else 0.0


def ndcg_at_k(ranked_ids: Sequence[str], relevant_ids: Sequence[str], k: int) -> float:
    relevant = set(relevant_ids)
    if not relevant:
        return 0.0

    dcg = 0.0
    for index, item in enumerate(ranked_ids[:k], start=1):
        if item in relevant:
            dcg += 1.0 / math.log2(index + 1)

    ideal_hits = min(len(relevant), k)
    idcg = sum(1.0 / math.log2(index + 1) for index in range(1, ideal_hits + 1))
    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def average(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def summarize_retrieval_results(results: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    evaluated = [
        item
        for item in results
        if not item.get("is_abstention") and item.get("answer_session_ids")
    ]
    abstentions = [item for item in results if item.get("is_abstention")]

    summary = {
        "questions_total": len(results),
        "questions_evaluated": len(evaluated),
        "abstention_questions": len(abstentions),
        "session_recall_any@5": average(
            [float(item["metrics"]["session_recall_any@5"]) for item in evaluated]
        ),
        "session_recall_any@10": average(
            [float(item["metrics"]["session_recall_any@10"]) for item in evaluated]
        ),
        "session_recall_all@5": average(
            [float(item["metrics"]["session_recall_all@5"]) for item in evaluated]
        ),
        "session_recall_all@10": average(
            [float(item["metrics"]["session_recall_all@10"]) for item in evaluated]
        ),
        "session_mrr": average(
            [float(item["metrics"]["session_mrr"]) for item in evaluated]
        ),
        "session_ndcg@10": average(
            [float(item["metrics"]["session_ndcg@10"]) for item in evaluated]
        ),
        "abstention_empty_rate": average(
            [1.0 if not item.get("ranked_session_ids") else 0.0 for item in abstentions]
        ),
    }

    by_type: Dict[str, List[Mapping[str, Any]]] = {}
    for item in evaluated:
        by_type.setdefault(str(item.get("question_type", "unknown")), []).append(item)

    summary["by_question_type"] = {
        question_type: {
            "questions": len(entries),
            "session_recall_any@5": average(
                [float(entry["metrics"]["session_recall_any@5"]) for entry in entries]
            ),
            "session_recall_any@10": average(
                [float(entry["metrics"]["session_recall_any@10"]) for entry in entries]
            ),
            "session_mrr": average(
                [float(entry["metrics"]["session_mrr"]) for entry in entries]
            ),
            "session_ndcg@10": average(
                [float(entry["metrics"]["session_ndcg@10"]) for entry in entries]
            ),
        }
        for question_type, entries in by_type.items()
    }
    return summary


def render_retrieval_summary_markdown(
    title: str,
    summary: Mapping[str, Any],
    *,
    dataset_name: str,
    results_file: str,
) -> str:
    lines = [
        f"# {title}",
        "",
        f"- Dataset: `{dataset_name}`",
        f"- Results file: `{results_file}`",
        f"- Questions processed: {summary['questions_total']}",
        f"- Questions evaluated: {summary['questions_evaluated']}",
        f"- Abstention questions: {summary['abstention_questions']}",
        "",
        "## Aggregate Metrics",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Session recall(any)@5 | {summary['session_recall_any@5']:.4f} |",
        f"| Session recall(any)@10 | {summary['session_recall_any@10']:.4f} |",
        f"| Session recall(all)@5 | {summary['session_recall_all@5']:.4f} |",
        f"| Session recall(all)@10 | {summary['session_recall_all@10']:.4f} |",
        f"| Session MRR | {summary['session_mrr']:.4f} |",
        f"| Session NDCG@10 | {summary['session_ndcg@10']:.4f} |",
        f"| Abstention empty-rate | {summary['abstention_empty_rate']:.4f} |",
        "",
        "## Per Question Type",
        "",
        "| Question Type | Questions | Recall(any)@5 | Recall(any)@10 | MRR | NDCG@10 |",
        "|---------------|-----------|---------------|----------------|-----|----------|",
    ]

    for question_type, metrics in sorted(summary["by_question_type"].items()):
        lines.append(
            "| {question_type} | {questions} | {r5:.4f} | {r10:.4f} | {mrr:.4f} | {ndcg:.4f} |".format(
                question_type=question_type,
                questions=metrics["questions"],
                r5=metrics["session_recall_any@5"],
                r10=metrics["session_recall_any@10"],
                mrr=metrics["session_mrr"],
                ndcg=metrics["session_ndcg@10"],
            )
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- These metrics are computed from engram search results using session-level relevance labels.",
            "- The official LongMemEval QA evaluator is still required for directly comparable answer-accuracy numbers.",
        ]
    )
    return "\n".join(lines) + "\n"


def split_text_for_ingestion(text: str, max_chars: int) -> List[str]:
    stripped = text.strip()
    if not stripped:
        return []
    if len(stripped) <= max_chars:
        return [stripped]

    chunks: List[str] = []
    current = ""
    paragraphs = [part.strip() for part in stripped.split("\n\n") if part.strip()]
    for paragraph in paragraphs or [stripped]:
        if len(paragraph) > max_chars:
            start = 0
            while start < len(paragraph):
                end = min(start + max_chars, len(paragraph))
                chunks.append(paragraph[start:end].strip())
                start = end
            current = ""
            continue

        if not current:
            current = paragraph
            continue

        candidate = f"{current}\n\n{paragraph}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            chunks.append(current.strip())
            current = paragraph

    if current:
        chunks.append(current.strip())
    return [chunk for chunk in chunks if chunk]


def build_qa_prompt(question: str, context: str, question_date: Optional[str] = None) -> str:
    date_line = f"Question date: {question_date}\n" if question_date else ""
    return (
        "You are answering a benchmark question using only the supplied memory context.\n"
        "If the answer is not supported by the context, reply exactly: I don't know.\n\n"
        f"{date_line}Question:\n{question}\n\n"
        f"Context:\n{context}\n\n"
        "Answer:"
    )


def run_openai_compatible_qa(
    *,
    question: str,
    context: str,
    model: str,
    api_key_env: str,
    base_url: str,
    timeout_secs: float,
    question_date: Optional[str] = None,
) -> str:
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise BenchError(
            f"environment variable {api_key_env} must be set for QA mode"
        )

    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": build_qa_prompt(question, context, question_date=question_date),
                }
            ],
            "temperature": 0,
        },
        timeout=timeout_secs,
    )
    if response.status_code != 200:
        raise BenchError(
            f"chat completion failed with {response.status_code}: {response.text.strip()}"
        )

    body = response.json()
    try:
        return str(body["choices"][0]["message"]["content"]).strip()
    except (KeyError, IndexError, TypeError) as error:
        raise BenchError(f"invalid QA response payload: {body}") from error