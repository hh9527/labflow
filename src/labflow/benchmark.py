from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .config import ControlError


SCHEMA_VERSION = 2
BENCH_ROLE_PREFIX = "bench-"
CASE_STATUSES = {
    "answered", "failed", "timeout", "error", "not_attempted",
    "clarification_exhausted",
}


SCHEMA = """
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE iterations (
    iter INTEGER PRIMARY KEY CHECK (iter >= 0),
    run_id TEXT NOT NULL UNIQUE,
    artifact TEXT NOT NULL,
    input_path TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'finalized')),
    outcome TEXT CHECK (outcome IS NULL OR outcome IN ('completed', 'failed')),
    error TEXT,
    selected_count INTEGER NOT NULL CHECK (selected_count >= 0),
    created_at INTEGER NOT NULL,
    finalized_at INTEGER
);

CREATE TABLE batches (
    iter INTEGER NOT NULL REFERENCES iterations(iter) ON DELETE CASCADE,
    batch_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    session_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'committed')),
    resolver_deleted INTEGER NOT NULL DEFAULT 0 CHECK (resolver_deleted IN (0, 1)),
    started_at INTEGER NOT NULL,
    committed_at INTEGER,
    deleted_at INTEGER,
    PRIMARY KEY (iter, batch_id),
    UNIQUE (iter, generation)
);

CREATE TABLE cases (
    iter INTEGER NOT NULL REFERENCES iterations(iter) ON DELETE CASCADE,
    case_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    question TEXT NOT NULL,
    batch_id TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'answered', 'failed', 'timeout', 'error',
        'not_attempted', 'clarification_exhausted'
    )),
    answer_json TEXT,
    error TEXT,
    PRIMARY KEY (iter, case_id),
    UNIQUE (iter, ordinal),
    FOREIGN KEY (iter, batch_id) REFERENCES batches(iter, batch_id)
);

CREATE TABLE clarifications (
    iter INTEGER NOT NULL,
    case_id TEXT NOT NULL,
    round INTEGER NOT NULL CHECK (round > 0),
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    PRIMARY KEY (iter, case_id, round),
    FOREIGN KEY (iter, case_id) REFERENCES cases(iter, case_id) ON DELETE CASCADE
);

CREATE TABLE measurements (
    iter INTEGER NOT NULL,
    case_id TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    input_tokens INTEGER NOT NULL CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL CHECK (output_tokens >= 0),
    reasoning_tokens INTEGER NOT NULL CHECK (reasoning_tokens >= 0),
    tool_calls INTEGER NOT NULL CHECK (tool_calls >= 0),
    PRIMARY KEY (iter, case_id),
    FOREIGN KEY (iter, case_id) REFERENCES cases(iter, case_id) ON DELETE CASCADE
);

CREATE TABLE interactions (
    iter INTEGER NOT NULL,
    case_id TEXT NOT NULL,
    turn INTEGER NOT NULL CHECK (turn >= 0),
    prompt TEXT NOT NULL,
    response TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    input_tokens INTEGER NOT NULL CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL CHECK (output_tokens >= 0),
    reasoning_tokens INTEGER NOT NULL CHECK (reasoning_tokens >= 0),
    tool_calls INTEGER NOT NULL CHECK (tool_calls >= 0),
    PRIMARY KEY (iter, case_id, turn),
    FOREIGN KEY (iter, case_id) REFERENCES cases(iter, case_id) ON DELETE CASCADE
);

CREATE VIEW case_summary AS
SELECT c.iter, c.case_id, c.ordinal, c.status, c.batch_id,
       m.duration_ms, m.input_tokens, m.output_tokens,
       m.reasoning_tokens, m.tool_calls,
       (SELECT COUNT(*) FROM clarifications AS x
        WHERE x.iter = c.iter AND x.case_id = c.case_id)
           AS clarification_rounds
FROM cases AS c
LEFT JOIN measurements AS m ON m.iter = c.iter AND m.case_id = c.case_id;

CREATE VIEW run_summary AS
SELECT r.iter, r.run_id, r.status, r.outcome, r.error, r.selected_count,
       SUM(CASE WHEN c.status = 'answered' THEN 1 ELSE 0 END) AS answered,
       SUM(CASE WHEN c.status != 'answered' THEN 1 ELSE 0 END) AS non_answered,
       COALESCE(SUM(m.duration_ms), 0) AS duration_ms,
       COALESCE(SUM(m.input_tokens), 0) AS input_tokens,
       COALESCE(SUM(m.output_tokens), 0) AS output_tokens,
       COALESCE(SUM(m.reasoning_tokens), 0) AS reasoning_tokens,
       COALESCE(SUM(m.tool_calls), 0) AS tool_calls
FROM iterations AS r
JOIN cases AS c ON c.iter = r.iter
LEFT JOIN measurements AS m ON m.iter = c.iter AND m.case_id = c.case_id
GROUP BY r.iter;
"""


@dataclass(frozen=True)
class BenchmarkBundle:
    path: Path
    questions: dict[str, dict[str, Any]]
    selected: tuple[str, ...]
    digest: str


def is_benchmark_role(role: str) -> bool:
    return role.startswith(BENCH_ROLE_PREFIX)


def _json_lines(path: Path) -> list[Any]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise ControlError(f"missing benchmark input: {path}", 66) from None
    except OSError as exc:
        raise ControlError(f"cannot read benchmark input {path}: {exc}", 66) from None
    values = []
    for number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            values.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ControlError(
                f"invalid benchmark input {path}:{number}: {exc.msg}"
            ) from None
    return values


def load_bundle(path: Path) -> BenchmarkBundle:
    root = path.resolve()
    if not root.is_dir():
        raise ControlError(f"benchmark input must be a directory: {path}", 66)
    question_rows = _json_lines(root / "questions.jsonl")
    questions: dict[str, dict[str, Any]] = {}
    for number, row in enumerate(question_rows, 1):
        if not isinstance(row, dict):
            raise ControlError(f"questions.jsonl:{number} must be an object")
        identity, question = row.get("id"), row.get("Q")
        if not isinstance(identity, str) or not identity:
            raise ControlError(f"questions.jsonl:{number} has invalid id")
        if not isinstance(question, str) or not question.strip():
            raise ControlError(f"questions.jsonl:{number} has invalid Q")
        if identity in questions:
            raise ControlError(f"questions.jsonl has duplicate id: {identity}")
        questions[identity] = dict(row)

    selected_rows = _json_lines(root / "selected.jsonl")
    selected: list[str] = []
    for number, row in enumerate(selected_rows, 1):
        identity = row.get("id") if isinstance(row, dict) else row
        if not isinstance(identity, str) or not identity:
            raise ControlError(f"selected.jsonl:{number} has invalid id")
        if identity in selected:
            raise ControlError(f"selected.jsonl has duplicate id: {identity}")
        if identity not in questions:
            raise ControlError(f"selected.jsonl references unknown id: {identity}")
        selected.append(identity)
    if not selected:
        raise ControlError("selected.jsonl must select at least one question")

    import hashlib
    digest = hashlib.sha256()
    for name in ("questions.jsonl", "selected.jsonl"):
        data = (root / name).read_bytes()
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(data)
        digest.update(b"\0")
    return BenchmarkBundle(root, questions, tuple(selected), digest.hexdigest())


def stage_path(execution_home: Path, artifact: str) -> Path:
    if (not artifact or "/" in artifact or "\\" in artifact
            or artifact in (".", "..")):
        raise ControlError(f"invalid benchmark artifact id: {artifact!r}")
    return execution_home / "bench" / f"{artifact}.sqlite"


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def _iter_end(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT value FROM metadata WHERE key = 'iter_end'"
    ).fetchone()
    if row is None:
        raise ControlError("invalid benchmark stage: missing iter_end")
    try:
        value = int(row[0])
    except (TypeError, ValueError):
        raise ControlError("invalid benchmark stage: malformed iter_end") from None
    if value < 0:
        raise ControlError("invalid benchmark stage: malformed iter_end")
    return value


def _current_iter(connection: sqlite3.Connection) -> int:
    iteration = _iter_end(connection)
    row = connection.execute(
        "SELECT status FROM iterations WHERE iter = ?", (iteration,)
    ).fetchone()
    if row is None or row["status"] != "running":
        raise ControlError("benchmark stage is not running")
    return iteration


def initialize_stage(
    path: Path, *, run_id: str, artifact: str, input_path: str,
    bundle: BenchmarkBundle, now: int | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(path) as connection:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != 0:
            if version != SCHEMA_VERSION:
                raise ControlError(f"unsupported benchmark stage schema: {version}")
        else:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(SCHEMA)
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES (?, ?)",
                (("schema", "labflow.benchmark-stage/v2"), ("artifact", artifact),
                 ("iter_end", "0")),
            )
        owner = connection.execute(
            "SELECT value FROM metadata WHERE key = 'artifact'"
        ).fetchone()
        if owner is None or owner[0] != artifact:
            raise ControlError("benchmark stage belongs to another artifact")
        existing = connection.execute(
            "SELECT iter, artifact, input_hash FROM iterations WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if existing is not None:
            if (existing["artifact"] != artifact
                    or existing["input_hash"] != bundle.digest):
                raise ControlError("benchmark run identity conflict")
            return
        iteration = _iter_end(connection)
        connection.execute("DELETE FROM iterations WHERE iter >= ?", (iteration,))
        connection.execute(
            "INSERT INTO iterations(iter, run_id, artifact, input_path, input_hash, "
            "status, selected_count, created_at) "
            "VALUES (?, ?, ?, ?, ?, 'running', ?, ?)",
            (iteration, run_id, artifact, input_path, bundle.digest, len(bundle.selected),
             int(time.time() * 1000) if now is None else now),
        )
        connection.executemany(
            "INSERT INTO cases(iter, case_id, ordinal, question, status) "
            "VALUES (?, ?, ?, ?, 'pending')",
            ((iteration, identity, ordinal, bundle.questions[identity]["Q"])
             for ordinal, identity in enumerate(bundle.selected)),
        )


def begin_batch(
    path: Path, *, batch_id: str, generation: int, session_id: str,
    case_ids: Iterable[str], now: int | None = None,
) -> None:
    identities = list(case_ids)
    if not identities or len(set(identities)) != len(identities):
        raise ControlError("benchmark batch requires unique case ids")
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        existing = connection.execute(
            "SELECT generation, session_id FROM batches "
            "WHERE iter = ? AND batch_id = ?", (iteration, batch_id)
        ).fetchone()
        if existing is not None:
            if tuple(existing) != (generation, session_id):
                raise ControlError(f"benchmark batch identity conflict: {batch_id}")
            return
        if connection.execute(
            "SELECT 1 FROM batches WHERE iter = ? AND status = 'active'",
            (iteration,),
        ).fetchone() is not None:
            raise ControlError("benchmark already has an active batch")
        rows = connection.execute(
            f"SELECT case_id, status FROM cases WHERE iter = ? AND case_id IN "
            f"({', '.join('?' for _ in identities)})", [iteration, *identities],
        ).fetchall()
        if len(rows) != len(identities) or any(row["status"] != "pending" for row in rows):
            raise ControlError("benchmark batch cases must be pending selected cases")
        connection.execute(
            "INSERT INTO batches(iter, batch_id, generation, session_id, status, "
            "started_at) VALUES (?, ?, ?, ?, 'active', ?)",
            (iteration, batch_id, generation, session_id,
             int(time.time() * 1000) if now is None else now),
        )
        connection.executemany(
            "UPDATE cases SET batch_id = ? WHERE iter = ? AND case_id = ?",
            ((batch_id, iteration, identity) for identity in identities),
        )


def _nonnegative(value: Any, where: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ControlError(f"{where} must be a nonnegative integer")
    return value


def commit_batch(
    path: Path, *, batch_id: str, results: Iterable[dict[str, Any]],
    now: int | None = None,
) -> None:
    values = list(results)
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        batch = connection.execute(
            "SELECT status FROM batches WHERE iter = ? AND batch_id = ?",
            (iteration, batch_id),
        ).fetchone()
        if batch is None:
            raise ControlError(f"unknown benchmark batch: {batch_id}")
        if batch["status"] == "committed":
            return
        expected = {
            row[0] for row in connection.execute(
                "SELECT case_id FROM cases WHERE iter = ? AND batch_id = ?",
                (iteration, batch_id),
            )
        }
        received = [item.get("id") if isinstance(item, dict) else None for item in values]
        if len(received) != len(set(received)) or set(received) != expected:
            raise ControlError("benchmark batch results must cover its cases exactly")
        for result in values:
            identity = result["id"]
            status = result.get("status")
            if status not in CASE_STATUSES:
                raise ControlError(f"invalid benchmark case status for {identity}: {status!r}")
            answer = result.get("answer")
            answer_json = None if answer is None else json.dumps(
                answer, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
            error = result.get("error")
            if error is not None and not isinstance(error, str):
                raise ControlError(f"benchmark case error must be text: {identity}")
            connection.execute(
                "UPDATE cases SET status = ?, answer_json = ?, error = ? "
                "WHERE iter = ? AND case_id = ?",
                (status, answer_json, error, iteration, identity),
            )
            clarifications = result.get("clarifications", [])
            if not isinstance(clarifications, list):
                raise ControlError(f"benchmark clarifications must be an array: {identity}")
            for round_number, clarification in enumerate(clarifications, 1):
                if (not isinstance(clarification, dict)
                        or not isinstance(clarification.get("question"), str)
                        or not isinstance(clarification.get("answer"), str)):
                    raise ControlError(f"invalid benchmark clarification: {identity}")
                connection.execute(
                    "INSERT INTO clarifications(iter, case_id, round, question, answer) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (iteration, identity, round_number, clarification["question"],
                     clarification["answer"]),
                )
            metrics = result.get("measurements", {})
            if not isinstance(metrics, dict):
                raise ControlError(f"benchmark measurements must be an object: {identity}")
            connection.execute(
                "INSERT INTO measurements(iter, case_id, duration_ms, input_tokens, "
                "output_tokens, reasoning_tokens, tool_calls) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (iteration, identity,
                 _nonnegative(metrics.get("duration_ms", 0), "duration_ms"),
                 _nonnegative(metrics.get("input_tokens", 0), "input_tokens"),
                 _nonnegative(metrics.get("output_tokens", 0), "output_tokens"),
                 _nonnegative(metrics.get("reasoning_tokens", 0), "reasoning_tokens"),
                 _nonnegative(metrics.get("tool_calls", 0), "tool_calls")),
            )
        connection.execute(
            "UPDATE batches SET status = 'committed', committed_at = ? "
            "WHERE iter = ? AND batch_id = ?",
            (int(time.time() * 1000) if now is None else now, iteration, batch_id),
        )


def mark_resolver_deleted(path: Path, *, batch_id: str, now: int | None = None) -> None:
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        batch = connection.execute(
            "SELECT status FROM batches WHERE iter = ? AND batch_id = ?",
            (iteration, batch_id),
        ).fetchone()
        if batch is None:
            raise ControlError(f"unknown benchmark batch: {batch_id}")
        if batch["status"] != "committed":
            raise ControlError("benchmark results must be committed before deleting resolver")
        connection.execute(
            "UPDATE batches SET resolver_deleted = 1, deleted_at = ? "
            "WHERE iter = ? AND batch_id = ?",
            (int(time.time() * 1000) if now is None else now, iteration, batch_id),
        )


def active_batch(path: Path) -> dict[str, Any] | None:
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        row = connection.execute(
            "SELECT * FROM batches WHERE iter = ? AND resolver_deleted = 0 "
            "ORDER BY generation DESC LIMIT 1", (iteration,),
        ).fetchone()
    return None if row is None else dict(row)


def pending_case_ids(path: Path, *, batch_id: str | None = None,
                     limit: int | None = None) -> list[str]:
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        where = "iter = ? AND status = 'pending'"
        parameters: list[Any] = [iteration]
        if batch_id is None:
            where += " AND batch_id IS NULL"
        else:
            where += " AND batch_id = ?"
            parameters.append(batch_id)
        statement = f"SELECT case_id FROM cases WHERE {where} ORDER BY ordinal"
        if limit is not None:
            statement += " LIMIT ?"
            parameters.append(limit)
        return [row[0] for row in connection.execute(statement, parameters)]


def case_context(path: Path, bundle: BenchmarkBundle, case_id: str) -> dict[str, Any]:
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        row = connection.execute(
            "SELECT iter, case_id, ordinal, question, batch_id, status FROM cases "
            "WHERE iter = ? AND case_id = ?", (iteration, case_id),
        ).fetchone()
        turns = int(connection.execute(
            "SELECT COUNT(*) FROM interactions WHERE iter = ? AND case_id = ?",
            (iteration, case_id),
        ).fetchone()[0])
    if row is None or case_id not in bundle.questions:
        raise ControlError(f"unknown benchmark case: {case_id}")
    private = bundle.questions[case_id]
    return {
        **dict(row),
        "turns": turns,
        "K": private.get("K"),
        "trap": private.get("trap"),
    }


def current_case_id(path: Path, *, batch_id: str) -> str | None:
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        rows = connection.execute(
            "SELECT c.case_id FROM cases AS c WHERE c.iter = ? AND c.batch_id = ? "
            "AND c.status = 'pending' AND EXISTS ("
            "SELECT 1 FROM interactions AS i WHERE i.iter = c.iter "
            "AND i.case_id = c.case_id) "
            "ORDER BY c.ordinal",
            (iteration, batch_id),
        ).fetchall()
    if len(rows) > 1:
        raise ControlError("benchmark batch has multiple current cases")
    return None if not rows else str(rows[0][0])


def record_interaction(
    path: Path, *, case_id: str, prompt: str, response: str,
    started_at: int, completed_at: int, input_tokens: int = 0,
    output_tokens: int = 0, reasoning_tokens: int = 0, tool_calls: int = 0,
) -> int:
    if completed_at < started_at:
        raise ControlError("benchmark interaction completion precedes its start")
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        row = connection.execute(
            "SELECT status, batch_id FROM cases WHERE iter = ? AND case_id = ?",
            (iteration, case_id),
        ).fetchone()
        if row is None or row["batch_id"] is None or row["status"] != "pending":
            raise ControlError(f"benchmark case is not active: {case_id}")
        batch = connection.execute(
            "SELECT status FROM batches WHERE iter = ? AND batch_id = ?",
            (iteration, row["batch_id"]),
        ).fetchone()
        if batch is None or batch["status"] != "active":
            raise ControlError(f"benchmark case batch is not active: {case_id}")
        turn = int(connection.execute(
            "SELECT COUNT(*) FROM interactions WHERE iter = ? AND case_id = ?",
            (iteration, case_id),
        ).fetchone()[0])
        connection.execute(
            "INSERT INTO interactions(iter, case_id, turn, prompt, response, started_at, "
            "completed_at, duration_ms, input_tokens, output_tokens, reasoning_tokens, "
            "tool_calls) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (iteration, case_id, turn, prompt, response, started_at, completed_at,
             completed_at - started_at,
             _nonnegative(input_tokens, "input_tokens"),
             _nonnegative(output_tokens, "output_tokens"),
             _nonnegative(reasoning_tokens, "reasoning_tokens"),
             _nonnegative(tool_calls, "tool_calls")),
        )
    return turn


def accept_case(path: Path, *, case_id: str, status_value: str = "answered",
                error: str | None = None) -> None:
    if status_value not in CASE_STATUSES:
        raise ControlError(f"invalid accepted benchmark status: {status_value}")
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        row = connection.execute(
            "SELECT status FROM cases WHERE iter = ? AND case_id = ?",
            (iteration, case_id),
        ).fetchone()
        if row is None or row["status"] != "pending":
            raise ControlError(f"benchmark case is not pending: {case_id}")
        interactions = connection.execute(
            "SELECT turn, prompt, response, duration_ms, input_tokens, output_tokens, "
            "reasoning_tokens, tool_calls FROM interactions "
            "WHERE iter = ? AND case_id = ? ORDER BY turn",
            (iteration, case_id),
        ).fetchall()
        if not interactions and status_value == "answered":
            raise ControlError(f"benchmark case has no resolver response: {case_id}")
        answer = None if not interactions else {"text": interactions[-1]["response"]}
        connection.execute(
            "UPDATE cases SET status = ?, answer_json = ?, error = ? "
            "WHERE iter = ? AND case_id = ?",
            (status_value,
             None if answer is None else json.dumps(answer, ensure_ascii=False,
                                                     separators=(",", ":")),
             error, iteration, case_id),
        )
        for interaction in interactions[1:]:
            connection.execute(
                "INSERT INTO clarifications(iter, case_id, round, question, answer) "
                "VALUES (?, ?, ?, ?, ?)",
                (iteration, case_id, interaction["turn"], interaction["prompt"],
                 interaction["response"]),
            )
        totals = {
            key: sum(int(item[key]) for item in interactions)
            for key in ("duration_ms", "input_tokens", "output_tokens",
                        "reasoning_tokens", "tool_calls")
        }
        connection.execute(
            "INSERT INTO measurements(iter, case_id, duration_ms, input_tokens, "
            "output_tokens, reasoning_tokens, tool_calls) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (iteration, case_id, *(totals[key] for key in (
                "duration_ms", "input_tokens", "output_tokens",
                "reasoning_tokens", "tool_calls",
            ))),
        )


def commit_recorded_batch(path: Path, *, batch_id: str, now: int | None = None) -> None:
    with _connect(path) as connection:
        iteration = _current_iter(connection)
        batch = connection.execute(
            "SELECT status FROM batches WHERE iter = ? AND batch_id = ?",
            (iteration, batch_id),
        ).fetchone()
        if batch is None:
            raise ControlError(f"unknown benchmark batch: {batch_id}")
        if batch["status"] == "committed":
            return
        pending = int(connection.execute(
            "SELECT COUNT(*) FROM cases WHERE iter = ? AND batch_id = ? "
            "AND status = 'pending'", (iteration, batch_id),
        ).fetchone()[0])
        if pending:
            raise ControlError(f"benchmark batch has {pending} pending case(s)")
        connection.execute(
            "UPDATE batches SET status = 'committed', committed_at = ? "
            "WHERE iter = ? AND batch_id = ?",
            (int(time.time() * 1000) if now is None else now, iteration, batch_id),
        )


def status(path: Path, *, run_id: str | None = None) -> dict[str, Any]:
    with _connect(path) as connection:
        if run_id is None:
            iteration = _current_iter(connection)
            run = connection.execute(
                "SELECT * FROM iterations WHERE iter = ?", (iteration,)
            ).fetchone()
        else:
            run = connection.execute(
                "SELECT * FROM iterations WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise ControlError(f"unknown benchmark run: {run_id}")
            iteration = int(run["iter"])
        if run is None:
            raise ControlError("invalid benchmark stage")
        counts = dict(connection.execute(
            "SELECT status, COUNT(*) FROM cases WHERE iter = ? GROUP BY status",
            (iteration,),
        ).fetchall())
        batches = [dict(row) for row in connection.execute(
            "SELECT * FROM batches WHERE iter = ? ORDER BY generation", (iteration,),
        )]
    return {
        "schema": "labflow.benchmark-status/v1",
        "run": dict(run),
        "case_statuses": counts,
        "batches": batches,
    }


def _snapshot(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    os.close(fd)
    os.unlink(temporary)
    try:
        with _connect(source) as source_connection:
            destination = sqlite3.connect(temporary)
            try:
                source_connection.backup(destination)
                destination.execute("PRAGMA journal_mode = DELETE")
                destination.commit()
            finally:
                destination.close()
        with open(temporary, "rb") as snapshot:
            os.fsync(snapshot.fileno())
        os.replace(temporary, target)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def finish_stage(path: Path, target: Path, *, outcome: str = "completed",
                 error: str | None = None, now: int | None = None,
                 run_id: str | None = None) -> None:
    if outcome not in ("completed", "failed"):
        raise ControlError(f"invalid benchmark outcome: {outcome}")
    if error is not None and not isinstance(error, str):
        raise ControlError("benchmark outcome error must be text")
    with _connect(path) as connection:
        if run_id is None:
            iteration = _iter_end(connection)
            run = connection.execute(
                "SELECT * FROM iterations WHERE iter = ?", (iteration,)
            ).fetchone()
        else:
            run = connection.execute(
                "SELECT * FROM iterations WHERE run_id = ?", (run_id,)
            ).fetchone()
            iteration = -1 if run is None else int(run["iter"])
        if run is None:
            raise ControlError("invalid benchmark stage")
        if run["status"] != "finalized":
            if iteration != _iter_end(connection):
                raise ControlError("benchmark run is not the current iteration")
            if connection.execute(
                "SELECT 1 FROM batches WHERE iter = ? "
                "AND (status != 'committed' OR resolver_deleted != 1)",
                (iteration,),
            ).fetchone() is not None:
                raise ControlError("benchmark has unfinished or live resolver batches")
            connection.execute(
                "UPDATE cases SET status = 'not_attempted' "
                "WHERE iter = ? AND status = 'pending'", (iteration,),
            )
            connection.execute(
                "UPDATE iterations SET status = 'finalized', outcome = ?, error = ?, "
                "finalized_at = ? WHERE iter = ?",
                (outcome, error, int(time.time() * 1000) if now is None else now,
                 iteration),
            )
            connection.execute(
                "UPDATE metadata SET value = ? WHERE key = 'iter_end'",
                (str(iteration + 1),),
            )
    _snapshot(path, target)


def rollback_stage(path: Path, *, run_id: str) -> bool:
    """Drop only the unsealed tail owned by run_id, retaining sealed history."""
    if not path.is_file():
        return False
    with _connect(path) as connection:
        iteration = _iter_end(connection)
        row = connection.execute(
            "SELECT run_id, status FROM iterations WHERE iter = ?", (iteration,)
        ).fetchone()
        if row is None or row["run_id"] != run_id or row["status"] != "running":
            return False
        connection.execute("DELETE FROM iterations WHERE iter >= ?", (iteration,))
    return True


def validate_artifact(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise ControlError(f"benchmark artifact is not a regular file: {path}")
    try:
        with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as connection:
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ControlError("benchmark artifact failed SQLite integrity_check")
            if int(connection.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
                raise ControlError("unsupported benchmark artifact schema")
            iter_end = _iter_end(connection)
            iteration = iter_end - 1
            run = connection.execute(
                "SELECT run_id, status, outcome, selected_count FROM iterations "
                "WHERE iter = ?", (iteration,),
            ).fetchone()
            unsealed = int(connection.execute(
                "SELECT COUNT(*) FROM iterations WHERE iter >= ?", (iter_end,)
            ).fetchone()[0])
            count = int(connection.execute(
                "SELECT COUNT(*) FROM cases WHERE iter = ?", (iteration,)
            ).fetchone()[0])
            unfinished = int(connection.execute(
                "SELECT COUNT(*) FROM cases WHERE iter = ? AND status = 'pending'",
                (iteration,),
            ).fetchone()[0])
    except sqlite3.Error as exc:
        raise ControlError(f"invalid benchmark artifact database: {exc}") from None
    if (run is None or run[1] != "finalized" or run[2] not in ("completed", "failed")
            or count != int(run[3]) or unfinished or unsealed):
        raise ControlError("benchmark artifact is not finalized")
    return {
        "schema": "labflow.benchmark-artifact/v1",
        "run_id": run[0],
        "outcome": run[2], "selected_count": count,
    }
