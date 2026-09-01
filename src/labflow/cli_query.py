from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any

from .config import ControlError
from .project import load_execution


def parser(prog: str = "labflow query") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        prog=prog, description="Run a strictly read-only execution data query."
    )
    value.add_argument("sql")
    return value


def om_parser(prog: str = "labflow query-om") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        prog=prog, description="Lower an OM-Labflow request and query execution data read-only."
    )
    value.add_argument(
        "--explain", action="store_true",
        help="print the generated parameterized SQL and bindings without executing it",
    )
    value.add_argument("input", type=Path)
    return value


def query(home: Path, statement: str,
          bindings: list[Any] | tuple[Any, ...] = ()) -> dict[str, Any]:
    sql = statement.strip()
    if not sql or len(sql) > 10_000:
        raise ControlError("query must contain from 1 through 10000 characters", 64)
    events = home / "events.sqlite"
    states = home / "states.sqlite"
    if not events.is_file() or not states.is_file():
        raise ControlError("execution databases are not available", 75)
    uri = "file:" + urllib.parse.quote(str(events.resolve())) + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    try:
        states_uri = "file:" + urllib.parse.quote(str(states.resolve())) + "?mode=ro"
        connection.execute("ATTACH DATABASE ? AS states", (states_uri,))
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 2000")
        denied = {
            getattr(sqlite3, name) for name in (
                "SQLITE_INSERT", "SQLITE_UPDATE", "SQLITE_DELETE",
                "SQLITE_CREATE_INDEX", "SQLITE_CREATE_TABLE", "SQLITE_CREATE_TEMP_INDEX",
                "SQLITE_CREATE_TEMP_TABLE", "SQLITE_CREATE_TEMP_TRIGGER",
                "SQLITE_CREATE_TEMP_VIEW", "SQLITE_CREATE_TRIGGER", "SQLITE_CREATE_VIEW",
                "SQLITE_DROP_INDEX", "SQLITE_DROP_TABLE", "SQLITE_DROP_TEMP_INDEX",
                "SQLITE_DROP_TEMP_TABLE", "SQLITE_DROP_TEMP_TRIGGER",
                "SQLITE_DROP_TEMP_VIEW", "SQLITE_DROP_TRIGGER", "SQLITE_DROP_VIEW",
                "SQLITE_ALTER_TABLE", "SQLITE_REINDEX", "SQLITE_ANALYZE",
                "SQLITE_PRAGMA", "SQLITE_ATTACH", "SQLITE_DETACH",
                "SQLITE_TRANSACTION", "SQLITE_SAVEPOINT",
            ) if hasattr(sqlite3, name)
        }
        connection.set_authorizer(
            lambda action, _one, _two, _database, _trigger:
            sqlite3.SQLITE_DENY if action in denied else sqlite3.SQLITE_OK
        )
        deadline = time.monotonic() + 2
        connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 10_000)
        try:
            cursor = connection.execute(sql, tuple(bindings))
            rows = cursor.fetchmany(1001)
        except sqlite3.Error as exc:
            raise ControlError(f"read-only query failed: {exc}", 65) from None
        columns = [str(item[0]) for item in cursor.description or ()]
        values = [[value.hex() if isinstance(value, bytes) else value for value in row]
                  for row in rows[:1000]]
        return {
            "schema": "labflow.query/v1",
            "columns": columns,
            "rows": values,
            "truncated": len(rows) > 1000,
        }
    finally:
        connection.close()


def _json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def lower_om(input_path: Path, *, environ: dict[str, str] | os._Environ[str] = os.environ
             ) -> tuple[str, list[Any]]:
    missing = [name for name in ("TELORA_BIN", "OM_LABFLOW_PATH") if not environ.get(name)]
    if missing:
        raise ControlError(
            f"query-om is unavailable; missing environment: {', '.join(missing)}", 69,
        )
    telora = Path(environ["TELORA_BIN"]).resolve()
    ontology = Path(environ["OM_LABFLOW_PATH"]).resolve()
    stdin_source = input_path == Path("-")
    if not telora.is_file() or not os.access(telora, os.X_OK):
        raise ControlError(f"TELORA_BIN is not an executable file: {telora}", 69)
    if not ontology.is_dir():
        raise ControlError(f"OM_LABFLOW_PATH is not a directory: {ontology}", 69)
    if not stdin_source and not input_path.is_file():
        raise ControlError(f"query-om input is not a file: {input_path}", 66)
    source = "stdin+json://" if stdin_source else str(input_path.resolve())
    command = [
        str(telora), "-C", str(ontology), "eval-with", "@src/bin/query:main",
        "--source", f"input={source}",
    ]
    try:
        completed = subprocess.run(
            command, text=True, stdout=subprocess.PIPE,
            timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ControlError(f"cannot run OM-Labflow query lowering: {exc}", 75) from None
    if completed.returncode != 0:
        raise ControlError(
            f"OM-Labflow query lowering exited with status {completed.returncode}", 65,
        )
    if len(completed.stdout) > 1_000_000:
        raise ControlError("OM-Labflow query output exceeds 1000000 characters", 65)
    try:
        value = json.loads(completed.stdout, parse_constant=_json_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ControlError(f"invalid OM-Labflow query output: {exc}", 65) from None
    if not isinstance(value, dict) or set(value) != {"sql", "bindings"}:
        raise ControlError("OM-Labflow output must contain only sql and bindings", 65)
    sql, bindings = value["sql"], value["bindings"]
    if not isinstance(sql, str) or not isinstance(bindings, list):
        raise ControlError("OM-Labflow sql must be a string and bindings must be an array", 65)
    if len(bindings) > 1000 or not all(
        item is None or type(item) in (bool, int, str)
        or type(item) is float and math.isfinite(item)
        for item in bindings
    ):
        raise ControlError("OM-Labflow bindings must contain at most 1000 JSON scalars", 65)
    return sql, bindings


def query_om(home: Path, input_path: Path, *,
             environ: dict[str, str] | os._Environ[str] = os.environ) -> dict[str, Any]:
    sql, bindings = lower_om(input_path, environ=environ)
    return query(home, sql, bindings)


def main(argv: list[str] | None = None, *, prog: str = "labflow query") -> int:
    args = parser(prog).parse_args(argv)
    try:
        home, _, _ = load_execution()
        print(json.dumps(query(home, args.sql), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except ControlError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except KeyboardInterrupt:
        return 130


def om_main(argv: list[str] | None = None, *, prog: str = "labflow query-om") -> int:
    args = om_parser(prog).parse_args(argv)
    try:
        if args.explain:
            sql, bindings = lower_om(args.input)
            result = {"sql": sql, "bindings": bindings}
        else:
            home, _, _ = load_execution()
            result = query_om(home, args.input)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except ControlError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
