from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .benchmark import (
    accept_case, active_batch, begin_batch, case_context, commit_recorded_batch,
    current_case_id, finish_stage, initialize_stage, load_bundle,
    mark_resolver_deleted, pending_case_ids, stage_path, status,
    record_interaction, rollback_stage,
)
from .client import Client, OpenCodeNotFound
from .config import ControlError
from .project import load_execution, project_home
from .task_cli import TaskError, task_records


MAX_CLARIFICATIONS = 2


@dataclass(frozen=True)
class Context:
    root: Path
    home: Path
    role: str
    task_id: str
    artifact: dict[str, Any]
    artifact_name: str
    input_path: str
    asset_path: str
    bundle: Any
    stage: Path
    client: Client


def parser(prog: str = "labflow bench") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Operate one benchmark Artifact task.")
    value.add_argument("--root", type=Path)
    commands = value.add_subparsers(dest="command", required=True)
    commands.add_parser("start")
    batch = commands.add_parser("batch-start")
    batch.add_argument("--size", type=int, default=10)
    ask = commands.add_parser("next")
    ask.add_argument("--timeout", type=float, default=300)
    clarify = commands.add_parser("clarify")
    clarify.add_argument("message")
    clarify.add_argument("--timeout", type=float, default=300)
    commands.add_parser("batch-finish")
    commands.add_parser("status")
    commands.add_parser("finish")
    return value


def _context(start: Path | None) -> Context:
    root = project_home(start)
    home, manifest, config = load_execution(root)
    active = [
        task for task in task_records(root)["active"]
        if isinstance(task.get("role"), str) and task["role"].startswith("bench-")
    ]
    if len(active) != 1:
        raise ControlError("benchmark command requires exactly one active bench-* task", 75)
    task = active[0]
    targets = task.get("artifacts")
    if not isinstance(targets, list) or len(targets) != 1:
        raise ControlError("invalid active benchmark task")
    artifact_name = targets[0]
    artifact = manifest.workflow["artifacts"].get(artifact_name)
    if not isinstance(artifact, dict) or not artifact.get("benchmark"):
        raise ControlError("active task is not a benchmark artifact")
    input_path = artifact["inputs"][0]["path"]
    asset_path = artifact["assets"][0]["path"]
    bundle = load_bundle(root / input_path.rstrip("/"))
    return Context(
        root, home, task["role"], task["task_id"], artifact, artifact_name,
        input_path, asset_path, bundle, stage_path(home, artifact_name),
        Client(f"http://127.0.0.1:{config['port']}", str(root)),
    )


def _text(message: dict[str, Any]) -> str:
    return "\n".join(
        str(part.get("text", "")) for part in message.get("parts", ())
        if part.get("type") == "text" and str(part.get("text", "")).strip()
    )


def _prompt(context: Context, batch: dict[str, Any], case_id: str,
            prompt: str, timeout: float) -> dict[str, Any]:
    if timeout <= 0:
        raise ControlError("benchmark prompt timeout must be positive")
    session_id = str(batch["session_id"])
    client = Client(context.client.url, str(context.root), session_id)
    before = {
        item.get("info", {}).get("id") for item in client.messages()
        if isinstance(item, dict)
    }
    started = int(time.time() * 1000)
    client.prompt_session(session_id, prompt, agent="priv-resolver")
    deadline = time.monotonic() + timeout
    message: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        messages = client.messages()
        completed = [
            item for item in messages
            if isinstance(item, dict)
            and item.get("info", {}).get("role") == "assistant"
            and item.get("info", {}).get("id") not in before
            and isinstance(item.get("info", {}).get("time", {}).get("completed"),
                           (int, float))
        ]
        if completed:
            message = max(
                completed,
                key=lambda item: int(item["info"]["time"]["completed"]),
            )
            if client.status().get("type", "idle") == "idle":
                break
        time.sleep(.1)
    if message is None:
        try:
            client.abort_session(session_id)
        except OpenCodeNotFound:
            pass
        accept_case(
            context.stage, case_id=case_id, status_value="timeout",
            error=f"resolver timed out after {timeout:g}s",
        )
        return {
            "response": "", "finish": None, "case_status": "timeout",
            "error": f"resolver timed out after {timeout:g}s",
        }
    info = message.get("info", {})
    response = _text(message)
    if not response:
        accept_case(
            context.stage, case_id=case_id, status_value="failed",
            error="resolver returned no text",
        )
        return {
            "response": "", "finish": info.get("finish"),
            "case_status": "failed", "error": "resolver returned no text",
        }
    timing = info.get("time", {})
    created = int(timing.get("created", started))
    completed_at = int(timing.get("completed", created))
    tokens = info.get("tokens", {})
    record_interaction(
        context.stage, case_id=case_id, prompt=prompt, response=response,
        started_at=created, completed_at=completed_at,
        input_tokens=int(tokens.get("input", 0) or 0),
        output_tokens=int(tokens.get("output", 0) or 0),
        reasoning_tokens=int(tokens.get("reasoning", 0) or 0),
        tool_calls=sum(1 for part in message.get("parts", ()) if part.get("type") == "tool"),
    )
    finish = info.get("finish")
    if finish != "stop":
        accept_case(
            context.stage, case_id=case_id, status_value="failed",
            error=f"resolver turn ended without stop: {finish}",
        )
        return {
            "response": response, "finish": finish, "case_status": "failed",
            "error": f"resolver turn ended without stop: {finish}",
        }
    return {"response": response, "finish": finish, "case_status": "pending"}


def _start(context: Context) -> dict[str, Any]:
    initialize_stage(
        context.stage, run_id=context.task_id, artifact=context.artifact_name,
        input_path=context.input_path, bundle=context.bundle,
    )
    return status(context.stage, run_id=context.task_id)


def _batch_start(context: Context, size: int) -> dict[str, Any]:
    if size <= 0:
        raise ControlError("benchmark batch size must be positive")
    _start(context)
    existing = active_batch(context.stage)
    if existing is not None:
        return existing
    identities = pending_case_ids(context.stage, limit=size)
    if not identities:
        raise ControlError("benchmark has no pending cases", 75)
    current = status(context.stage)
    generation = max((item["generation"] for item in current["batches"]), default=0) + 1
    batch_id = f"batch-{generation}"
    response = context.client.create_session(
        f"{context.task_id}:{batch_id}", agent="priv-resolver",
    )
    session_id = response.get("id") if isinstance(response, dict) else None
    if not isinstance(session_id, str) or not session_id:
        raise ControlError("OpenCode returned an invalid private Resolver identity", 69)
    try:
        begin_batch(
            context.stage, batch_id=batch_id, generation=generation,
            session_id=session_id, case_ids=identities,
        )
    except BaseException:
        try:
            context.client.delete_session(session_id)
        except (ControlError, OSError):
            pass
        raise
    return active_batch(context.stage) or {}


def _next(context: Context, timeout: float) -> dict[str, Any]:
    _start(context)
    batch = active_batch(context.stage)
    if batch is None or batch["status"] != "active":
        raise ControlError("benchmark has no active Resolver batch", 75)
    current = current_case_id(context.stage, batch_id=batch["batch_id"])
    if current is not None:
        accept_case(context.stage, case_id=current)
    pending = pending_case_ids(context.stage, batch_id=batch["batch_id"], limit=1)
    if not pending:
        return {"batch_complete": True, "batch_id": batch["batch_id"]}
    case_id = pending[0]
    case = case_context(context.stage, context.bundle, case_id)
    result = _prompt(context, batch, case_id, case["question"], timeout)
    return {
        "batch_complete": False, "case_id": case_id, "question": case["question"],
        "private_knowledge": case["K"], "trap": case["trap"], **result,
    }


def _clarify(context: Context, message: str, timeout: float) -> dict[str, Any]:
    _start(context)
    batch = active_batch(context.stage)
    if batch is None or batch["status"] != "active":
        raise ControlError("benchmark has no active Resolver batch", 75)
    case_id = current_case_id(context.stage, batch_id=batch["batch_id"])
    if case_id is None:
        raise ControlError("benchmark has no current case to clarify", 75)
    case = case_context(context.stage, context.bundle, case_id)
    if case["turns"] - 1 >= MAX_CLARIFICATIONS:
        accept_case(
            context.stage, case_id=case_id,
            status_value="clarification_exhausted", error="clarification round limit",
        )
        return {"case_id": case_id, "clarification_exhausted": True}
    result = _prompt(context, batch, case_id, message, timeout)
    return {
        "case_id": case_id, "clarification_round": case["turns"],
        "private_knowledge": case["K"], "trap": case["trap"], **result,
    }


def _batch_finish(context: Context) -> dict[str, Any]:
    _start(context)
    batch = active_batch(context.stage)
    if batch is None:
        raise ControlError("benchmark has no live Resolver batch", 75)
    if batch["status"] == "active":
        current = current_case_id(context.stage, batch_id=batch["batch_id"])
        if current is not None:
            accept_case(context.stage, case_id=current)
        for case_id in pending_case_ids(context.stage, batch_id=batch["batch_id"]):
            accept_case(
                context.stage, case_id=case_id, status_value="not_attempted",
                error="batch finished before case was attempted",
            )
        commit_recorded_batch(context.stage, batch_id=batch["batch_id"])
    session_id = str(batch["session_id"])
    client = Client(context.client.url, str(context.root), session_id)
    try:
        client.delete_session(session_id)
    except OpenCodeNotFound:
        pass
    mark_resolver_deleted(context.stage, batch_id=batch["batch_id"])
    return status(context.stage)


def _finish(context: Context, *, outcome: str = "completed",
            error: str | None = None) -> dict[str, Any]:
    current = _start(context)
    if current["run"]["status"] == "running" and active_batch(context.stage) is not None:
        _batch_finish(context)
    finish_stage(
        context.stage, context.root / context.asset_path,
        outcome=outcome, error=error, run_id=context.task_id,
    )
    return {**validate_result(context), "asset": context.asset_path}


def validate_result(context: Context) -> dict[str, Any]:
    from .benchmark import validate_artifact
    return validate_artifact(context.root / context.asset_path)


def force_finish(root: Path, task_id: str, reason: str) -> dict[str, Any]:
    """Materialize a valid result for a terminal bench turn without negotiation."""
    context = _context(root)
    if context.task_id != task_id:
        raise ControlError("active benchmark task changed before finalization", 75)
    _start(context)
    return _finish(context, outcome="failed", error=reason)


def discard(root: Path, task_id: str, artifact_name: str) -> None:
    home, _, config = load_execution(root)
    path = stage_path(home, artifact_name)
    if not path.is_file():
        return
    try:
        run = status(path, run_id=task_id)["run"]
    except ControlError:
        return
    if run["status"] != "running":
        return
    batch = active_batch(path)
    if batch is not None:
        client = Client(f"http://127.0.0.1:{config['port']}", str(root))
        try:
            client.delete_session(str(batch["session_id"]))
        except OpenCodeNotFound:
            pass
    rollback_stage(path, run_id=task_id)


def main(argv: list[str] | None = None, *, prog: str = "labflow bench") -> int:
    args = parser(prog).parse_args(argv)
    try:
        context = _context(args.root.resolve() if args.root else None)
        if args.command == "start":
            result = _start(context)
        elif args.command == "batch-start":
            result = _batch_start(context, args.size)
        elif args.command == "next":
            result = _next(context, args.timeout)
        elif args.command == "clarify":
            result = _clarify(context, args.message, args.timeout)
        elif args.command == "batch-finish":
            result = _batch_finish(context)
        elif args.command == "status":
            result = _start(context)
        else:
            result = _finish(context)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (ControlError, TaskError, OSError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return getattr(exc, "code", 69)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
