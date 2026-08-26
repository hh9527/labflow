from __future__ import annotations

import json
import queue
import sqlite3
import threading
import urllib.parse
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 2
EVENT_TYPES = {
    "thinking", "action", "reply",
    "session_started", "session_ended", "turn_started", "turn_ended",
    "task_started", "task_completed",
    "artifact_refreshed", "artifact_deleted",
    "host_request_opened", "host_request_resolved", "dag_revised",
}


SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS timeline (
    id TEXT PRIMARY KEY,
    execution TEXT NOT NULL,
    session TEXT,
    turn TEXT,
    role TEXT,
    task_kind TEXT CHECK (task_kind IS NULL OR task_kind IN ('artifact', 'problem')),
    task_id TEXT,
    artifact TEXT,
    dag_revision TEXT,
    type TEXT NOT NULL,
    at INTEGER NOT NULL,
    duration INTEGER NOT NULL CHECK (duration >= 0),
    tokens INTEGER,
    action TEXT,
    success INTEGER,
    command TEXT,
    exit_code INTEGER,
    summary TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    reasoning_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_write_tokens INTEGER,
    payload_json TEXT,
    CHECK ((task_kind IS NULL) = (task_id IS NULL))
);

CREATE TABLE IF NOT EXISTS action_paths (
    event_id TEXT NOT NULL REFERENCES timeline(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    PRIMARY KEY (event_id, path)
);

CREATE INDEX IF NOT EXISTS timeline_execution_at
    ON timeline(execution, at);

CREATE INDEX IF NOT EXISTS timeline_session_type_at
    ON timeline(execution, session, type, at);

CREATE INDEX IF NOT EXISTS timeline_turn_at
    ON timeline(execution, session, turn, at);

CREATE INDEX IF NOT EXISTS timeline_task_at
    ON timeline(execution, task_kind, task_id, at);

CREATE INDEX IF NOT EXISTS timeline_artifact_at
    ON timeline(execution, artifact, at);

CREATE INDEX IF NOT EXISTS timeline_dag_revision_at
    ON timeline(execution, dag_revision, at);
"""


TIMELINE_COLUMNS = (
    "id", "execution", "session", "turn", "role", "task_kind", "task_id",
    "artifact", "dag_revision", "type", "at", "duration", "tokens",
    "action", "success", "command", "exit_code", "summary", "input_tokens",
    "output_tokens", "reasoning_tokens", "cache_read_tokens", "cache_write_tokens",
    "payload_json",
)


def _migrate_v1(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = OFF")
    connection.executescript(f"""
        BEGIN IMMEDIATE;
        DROP INDEX IF EXISTS timeline_execution_at;
        DROP INDEX IF EXISTS timeline_session_type_at;
        ALTER TABLE action_paths RENAME TO action_paths_v1;
        ALTER TABLE timeline RENAME TO timeline_v1;
        {SCHEMA}
        INSERT INTO timeline (
            id, execution, session, role, type, at, duration, tokens, action,
            success, command, exit_code, summary, input_tokens, output_tokens,
            reasoning_tokens, cache_read_tokens, cache_write_tokens
        )
        SELECT id, execution, session, role, type, at, duration, tokens, action,
               success, command, exit_code, summary, input_tokens, output_tokens,
               reasoning_tokens, cache_read_tokens, cache_write_tokens
        FROM timeline_v1;
        INSERT INTO action_paths(event_id, path)
        SELECT event_id, path FROM action_paths_v1;
        DROP TABLE action_paths_v1;
        DROP TABLE timeline_v1;
        COMMIT;
    """)
    connection.execute("PRAGMA foreign_keys = ON")


def _writer_connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def _reader_connect(path: Path) -> sqlite3.Connection:
    uri = "file:" + urllib.parse.quote(str(path.resolve())) + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def schema_version(path: Path) -> int | None:
    if not path.is_file():
        return None
    with _reader_connect(path) as connection:
        return int(connection.execute("PRAGMA user_version").fetchone()[0])


def initialize(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _writer_connect(path) as connection:
        current = connection.execute("PRAGMA user_version").fetchone()[0]
        if current == 1:
            _migrate_v1(connection)
        elif current == 0:
            connection.executescript(SCHEMA)
        elif current != SCHEMA_VERSION:
            raise RuntimeError(f"unsupported Timeline schema version: {current}")
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        connection.execute(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )


def _validate(record: dict[str, Any]) -> None:
    required = ("id", "execution", "type", "at", "duration")
    if any(key not in record for key in required):
        raise ValueError("Timeline record is missing required fields")
    if record["type"] not in EVENT_TYPES:
        raise ValueError(f"invalid Timeline event type: {record['type']!r}")
    if not all(isinstance(record[key], str) and record[key] for key in
               ("id", "execution")):
        raise ValueError("Timeline identities must be nonempty strings")
    for key in ("session", "turn", "role", "task_id", "artifact", "dag_revision"):
        if key in record and (not isinstance(record[key], str) or not record[key]):
            raise ValueError(f"Timeline {key} must be a nonempty string")
    task_kind, task_id = record.get("task_kind"), record.get("task_id")
    if task_kind not in (None, "artifact", "problem"):
        raise ValueError(f"invalid Timeline task kind: {task_kind!r}")
    if (task_kind is None) != (task_id is None):
        raise ValueError("Timeline task kind and id must be provided together")
    if not isinstance(record["at"], int) or not isinstance(record["duration"], int):
        raise ValueError("Timeline at and duration must be integers")
    if record["duration"] < 0:
        raise ValueError("Timeline duration cannot be negative")
    paths = record.get("paths", [])
    if not isinstance(paths, list) or not all(isinstance(path, str) and path for path in paths):
        raise ValueError("Timeline paths must be a string array")


def append(connection: sqlite3.Connection, records: Iterable[dict[str, Any]]) -> int:
    inserted = 0
    placeholders = ", ".join("?" for _ in TIMELINE_COLUMNS)
    statement = (
        f"INSERT OR IGNORE INTO timeline ({', '.join(TIMELINE_COLUMNS)}) "
        f"VALUES ({placeholders})"
    )
    with connection:
        for record in records:
            _validate(record)
            values = []
            for column in TIMELINE_COLUMNS:
                value = record.get(column)
                if column == "success" and isinstance(value, bool):
                    value = int(value)
                if column == "payload_json" and "payload" in record:
                    value = json.dumps(record["payload"], ensure_ascii=False,
                                       separators=(",", ":"), sort_keys=True)
                values.append(value)
            cursor = connection.execute(statement, values)
            inserted += max(cursor.rowcount, 0)
            for path in record.get("paths", []):
                connection.execute(
                    "INSERT OR IGNORE INTO action_paths(event_id, path) VALUES (?, ?)",
                    (record["id"], path),
                )
    return inserted


def read(path: Path, execution: str, since: int = 0,
         event_id: str | None = None, *, session: str | None = None,
         turn: str | None = None, task_kind: str | None = None,
         task_id: str | None = None, artifact: str | None = None,
         dag_revision: str | None = None) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    if (task_kind is None) != (task_id is None):
        raise ValueError("Timeline task kind and id filters must be provided together")
    with _reader_connect(path) as connection:
        where = "execution = ? AND at >= ?"
        parameters: list[Any] = [execution, since]
        for column, value in (
            ("id", event_id), ("session", session), ("turn", turn),
            ("task_kind", task_kind), ("task_id", task_id),
            ("artifact", artifact), ("dag_revision", dag_revision),
        ):
            if value is not None:
                where += f" AND {column} = ?"
                parameters.append(value)
        rows = connection.execute(
            f"SELECT {', '.join(TIMELINE_COLUMNS)} FROM timeline "
            f"WHERE {where} ORDER BY at, id",
            parameters,
        ).fetchall()
        result = []
        for row in rows:
            value = {key: row[key] for key in TIMELINE_COLUMNS if row[key] is not None}
            if "success" in value:
                value["success"] = bool(value["success"])
            payload_json = value.pop("payload_json", None)
            if payload_json is not None:
                value["payload"] = json.loads(payload_json)
            paths = [item[0] for item in connection.execute(
                "SELECT path FROM action_paths WHERE event_id = ? ORDER BY path", (row["id"],)
            )]
            if paths:
                value["paths"] = paths
            result.append(value)
        return result


def statistics(path: Path, execution: str) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    with _reader_connect(path) as connection:
        rows = connection.execute(
            """
            SELECT session, role,
                   COUNT(*) AS events,
                   SUM(CASE WHEN type = 'thinking' THEN 1 ELSE 0 END) AS thinking_count,
                   SUM(CASE WHEN type = 'thinking' THEN duration ELSE 0 END) AS thinking_ms,
                   MAX(CASE WHEN type = 'thinking' THEN duration END) AS longest_thinking_ms,
                   SUM(CASE WHEN type = 'action' THEN 1 ELSE 0 END) AS action_count,
                   SUM(CASE WHEN type = 'action' THEN duration ELSE 0 END) AS action_ms,
                   SUM(CASE WHEN type = 'action' AND success = 0 THEN 1 ELSE 0 END) AS action_failures,
                   SUM(CASE WHEN type = 'reply' THEN 1 ELSE 0 END) AS reply_count,
                   SUM(COALESCE(input_tokens, 0)) AS input_tokens,
                   SUM(COALESCE(output_tokens, 0)) AS output_tokens,
                   SUM(COALESCE(reasoning_tokens, 0)) AS reasoning_tokens,
                   SUM(COALESCE(cache_read_tokens, 0)) AS cache_read_tokens,
                   SUM(COALESCE(cache_write_tokens, 0)) AS cache_write_tokens
            FROM timeline
            WHERE execution = ? AND type IN ('thinking', 'action', 'reply')
            GROUP BY session, role
            ORDER BY MIN(at), session
            """,
            (execution,),
        ).fetchall()
        sessions = [{key: row[key] for key in row.keys()} for row in rows]
        commands = connection.execute(
            """
            SELECT command, COUNT(*) AS count, SUM(duration) AS duration,
                   SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS failures
            FROM timeline
            WHERE execution = ? AND type = 'action' AND action = 'shell'
                  AND command IS NOT NULL
            GROUP BY command
            ORDER BY MIN(at), command
            """,
            (execution,),
        ).fetchall()
        tasks = connection.execute(
            """
            SELECT task_kind, task_id, role,
                   COUNT(DISTINCT turn) AS turns,
                   SUM(CASE WHEN type = 'thinking' THEN duration ELSE 0 END) AS thinking_ms,
                   SUM(CASE WHEN type = 'action' THEN 1 ELSE 0 END) AS action_count,
                   SUM(CASE WHEN type = 'action' THEN duration ELSE 0 END) AS action_ms,
                   SUM(COALESCE(input_tokens, 0)) AS input_tokens,
                   SUM(COALESCE(output_tokens, 0)) AS output_tokens,
                   SUM(COALESCE(reasoning_tokens, 0)) AS reasoning_tokens
            FROM timeline
            WHERE execution = ? AND task_kind IS NOT NULL
                  AND type IN ('thinking', 'action', 'reply')
            GROUP BY task_kind, task_id, role
            ORDER BY MIN(at), task_kind, task_id, role
            """,
            (execution,),
        ).fetchall()
        return {
            "schema": "labflow.timeline-stat/v1",
            "execution": execution,
            "sessions": sessions,
            "commands": [{key: row[key] for key in row.keys()} for row in commands],
            "tasks": [{key: row[key] for key in row.keys()} for row in tasks],
        }


class TimelineWriter:
    """Single asynchronous SQLite writer; the Supervisor never reads this database."""

    def __init__(self, path: Path, *, batch_size: int = 64, flush_seconds: float = .05):
        self.path = path
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.items: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=4096)
        self.failure: BaseException | None = None
        initialize(path)
        self.thread = threading.Thread(target=self._run, name="labflow-timeline", daemon=True)
        self.thread.start()

    def submit(self, records: Iterable[dict[str, Any]]) -> None:
        self.check()
        for record in records:
            self.items.put(record)

    def check(self) -> None:
        if self.failure is not None:
            raise RuntimeError(f"Timeline writer failed: {self.failure}") from self.failure

    def close(self) -> None:
        self.items.put(None)
        self.thread.join()
        self.check()

    def _run(self) -> None:
        try:
            connection = _writer_connect(self.path)
            try:
                closing = False
                while not closing:
                    first = self.items.get()
                    if first is None:
                        break
                    batch = [first]
                    while len(batch) < self.batch_size:
                        try:
                            item = self.items.get(timeout=self.flush_seconds)
                        except queue.Empty:
                            break
                        if item is None:
                            closing = True
                            break
                        batch.append(item)
                    append(connection, batch)
            finally:
                connection.close()
        except BaseException as exc:
            self.failure = exc
