from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Any

from .client import Client
from .config import ControlError, load_manifest, repository_root, sha256
from .context import Context, resolve
from .events import event_detail, pending_requests, project_events
from .lifecycle import (
    create_execution_session,
    next_session_title,
    prepare,
    probe_opencode_connection,
    refresh_workflow_artifact,
    send_round,
    verify_prepared,
)
from .metrics import collect_metrics, summarize_thread_metrics
from .observe import assistant_messages, text_parts
from .runtime_opencode import START_PROMPT, resume_prompt
from .state import (
    atomic_write,
    atomic_json,
    load_lab_config,
    load_connect_test,
    load_state,
    record_connect_test,
)
from .task_cli import (
    TaskError, remove_artifact, supersede_role_task, task_records, workflow_status,
)
from .thread_service import (
    approve_baseline,
    close_thread,
    comment_thread,
    install_bundle,
    open_thread,
    thread_records,
)


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def parser(prog: str = "labflow host") -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog=prog, description="Control a named experiment execution.")
    commands = root.add_subparsers(dest="command", required=True)
    connect = commands.add_parser("test-connect")
    connect.add_argument("lab_name")

    def target(command: argparse.ArgumentParser) -> None:
        command.add_argument("lab_name")
        command.add_argument("session_name")

    stat_command = commands.add_parser("stat")
    target(stat_command)
    status = commands.add_parser("status")
    target(status)
    status.add_argument(
        "--verbose", action="store_true",
        help="include the complete artifact graph and raw runtime states",
    )
    pull = commands.add_parser("pull")
    target(pull)
    pull.add_argument("since", nargs="?", type=int,
                      help="previous response's next_since; Unix milliseconds, defaults to call start")
    pull.add_argument("--timeout", type=float, default=60.0,
                      help="seconds to wait for a Host decision; range 0..60 (default: 60)")
    event = commands.add_parser("event")
    target(event)
    event.add_argument("event_id")
    start = commands.add_parser("start")
    start.add_argument("lab_name")
    start.add_argument("plan_id")
    start.add_argument(
        "--from", dest="from_session",
        help="inherit current artifacts and checked files from an earlier execution",
    )
    start.add_argument("--bundle", help="install a declared thread-service input bundle")
    update = commands.add_parser("update")
    target(update)
    update.add_argument("assets", nargs="+")
    update.add_argument("--force", action="store_true",
                        help="allow explicit Host replacement of role-owned output files")
    submit = commands.add_parser("submit")
    target(submit)
    submit.add_argument("artifacts", nargs="+")
    submit.add_argument("--force", action="store_true",
                        help="allow explicit Host refresh of role-owned artifacts")
    resume = commands.add_parser("resume")
    target(resume)
    resume.add_argument("role")
    resume.add_argument("--timeout", type=float, default=15.0,
                        help="seconds to wait until the role loop is observed")
    resume.add_argument("--force", action="store_true",
                        help="abort the current role turn before re-entering its loop")
    abort_sessions = commands.add_parser(
        "abort-sessions",
        help="abort active sessions belonging to a completed or retired execution",
    )
    target(abort_sessions)
    approve = commands.add_parser("approve-baseline", help="freeze a qualified role session")
    target(approve)
    approve.add_argument("role")
    open_item = commands.add_parser("open-thread", help="fork the baseline for one production problem")
    target(open_item)
    open_item.add_argument("role")
    open_item.add_argument("thread_name")
    open_item.add_argument("problem_file")
    comment = commands.add_parser("comment-thread", help="continue the active problem session")
    target(comment)
    comment.add_argument("role")
    comment.add_argument("thread_name")
    comment.add_argument("comment_file")
    close = commands.add_parser("close-thread", help="archive the active problem session")
    target(close)
    close.add_argument("role")
    return root


def _workspace(context: Context) -> Path:
    value = context.state.get("workspace")
    if not value:
        raise ControlError(f"execution is {context.state['phase']}; workspace is not ready", 75)
    return Path(value)


def _safe_relative(value: str, where: str) -> Path:
    normalized = value[:-1] if value.endswith("/") else value
    path = PurePosixPath(normalized)
    if (path.is_absolute() or not normalized
            or any(part in ("", ".", "..") for part in normalized.split("/"))):
        raise ControlError(f"unsafe {where}: {value}", 64)
    return Path(*path.parts)


def _controller_repo() -> Path:
    return repository_root(Path.cwd())


def _configure_start(repo: Path, lab_name: str, plan_id: str,
                     from_session: str | None = None,
                     bundle: str | None = None) -> dict[str, Any]:
    lab = load_lab_config(repo, lab_name)
    load_connect_test(lab_name, Path(lab["root"]))
    manifest = load_manifest(repo, plan_id)
    if manifest.execution["kind"] == "thread-service":
        if bundle is None:
            raise ControlError("thread-service start requires --bundle", 64)
        if from_session is not None:
            raise ControlError("thread-service start does not support --from", 64)
        if not Path(bundle).expanduser().is_dir():
            raise ControlError(f"bundle is not a directory: {bundle}", 66)
    elif bundle is not None:
        raise ControlError("--bundle is only valid for a thread-service plan", 64)
    return {**lab, "lab_name": lab_name,
            "bundle": str(Path(bundle).expanduser().resolve()) if bundle else None}


def _test_connect(repo: Path, lab_name: str) -> dict[str, Any]:
    lab = load_lab_config(repo, lab_name)
    workspace = Path(lab["root"]) / "connection"
    workspace.mkdir(parents=True, exist_ok=True)
    result = probe_opencode_connection(lab_name, lab["port"], workspace)
    receipt = record_connect_test(lab_name, Path(lab["root"]), result)
    return receipt


def _role_output_owners(workflow: dict[str, Any] | None, destination: str) -> list[str]:
    if not workflow:
        return []
    owners = []
    for artifact in workflow["artifacts"].values():
        owner = artifact.get("owner")
        if owner != "host" and any(_asset_contains(asset["path"], destination)
                                    for asset in artifact["assets"]):
            if owner not in owners:
                owners.append(owner)
    return owners


def _asset_contains(asset: str, path: str) -> bool:
    return path.startswith(asset) if asset.endswith("/") else path == asset


def _record_intervention(context: Context, kind: str, targets: list[dict[str, Any]]) -> dict[str, Any]:
    event = {
        "schema": "labflow.host-intervention/v1",
        "session_name": context.state["session_name"],
        "host_forced": True,
        "kind": kind,
        "recorded_at_ns": time.time_ns(),
        "targets": targets,
    }
    name = f"{event['recorded_at_ns']}-{kind}.json"
    atomic_json(context.root / "host-interventions" / name, event)
    atomic_json(_workspace(context) / "control" / "host-interventions" / name, event)
    return event


def _update(context: Context, values: list[str], *, force: bool = False) -> list[dict[str, Any]]:
    workspace = _workspace(context)
    workflow = context.state.get("workflow")
    results = []
    for value in values:
        if "=" not in value:
            raise ControlError("update operands must be <asset>=<source> or <asset>=!", 64)
        destination_name, source_name = value.split("=", 1)
        directory = destination_name.endswith("/")
        destination = workspace / _safe_relative(destination_name, "destination")
        owners = _role_output_owners(workflow, destination_name)
        if owners and not force:
            raise ControlError(
                f"role-owned output requires --force: {destination_name} ({', '.join(owners)})",
                64,
            )
        previous_hash = sha256(destination) if destination.is_file() else None
        if source_name == "!":
            existed = destination.exists() or destination.is_symlink()
            if destination.is_dir() and not destination.is_symlink():
                shutil.rmtree(destination)
            else:
                destination.unlink(missing_ok=True)
            results.append({"path": destination_name, "removed": True, "existed": existed,
                            "previous_sha256": previous_hash, "owners": owners,
                            "host_forced": bool(force and owners), "updated_at_ns": time.time_ns()})
            continue
        source = Path(source_name).expanduser()
        if not source.is_absolute():
            source = Path.cwd() / source
        if directory:
            if not source.is_dir() or source.is_symlink():
                raise ControlError(f"missing or unsafe source directory: {source_name}", 66)
            if any(path.is_symlink() for path in source.rglob("*")):
                raise ControlError(f"source directory contains a symlink: {source_name}", 66)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix=".labflow-update-", dir=destination.parent) as tmp:
                staged = Path(tmp) / "asset"
                shutil.copytree(source, staged)
                if destination.is_dir() and not destination.is_symlink():
                    shutil.rmtree(destination)
                else:
                    destination.unlink(missing_ok=True)
                os.replace(staged, destination)
            results.append({"path": destination_name, "source": source_name,
                            "owners": owners, "host_forced": bool(force and owners),
                            "updated_at_ns": time.time_ns()})
        else:
            if not source.is_file() or source.is_symlink():
                raise ControlError(f"missing or unsafe source file: {source_name}", 66)
            mode = stat.S_IMODE(source.stat().st_mode)
            atomic_write(destination, source.read_bytes(), mode)
            results.append({"path": destination_name, "source": source_name,
                            "bytes": destination.stat().st_size, "mode": f"{mode:04o}",
                            "previous_sha256": previous_hash, "sha256": sha256(destination),
                            "owners": owners, "host_forced": bool(force and owners),
                            "updated_at_ns": time.time_ns()})
    forced = [result for result in results if result["host_forced"]]
    if forced:
        for owner in sorted({owner for result in forced for owner in result["owners"]}):
            supersede_role_task(workspace, owner, "Host force-updated role-owned output")
        _record_intervention(context, "update", forced)
    return results


def _submit(context: Context, values: list[str], *, force: bool = False) -> list[dict[str, Any]]:
    workflow = context.state.get("workflow")
    workspace = _workspace(context)
    if not workflow:
        raise ControlError("execution workflow is not prepared", 75)
    results = []
    for value in values:
        remove = value.endswith("=!")
        name = value[:-2] if remove else value
        if not name:
            raise ControlError(f"invalid artifact operand: {value}", 64)
        if remove:
            try:
                results.append(remove_artifact(workspace, workflow, name, force=force))
            except TaskError as exc:
                raise ControlError(str(exc), exc.code) from None
        else:
            results.append(refresh_workflow_artifact(context, name, "submit", force=force))
    forced = [result for result in results if result.get("host_forced")]
    if forced:
        _record_intervention(context, "submit", forced)
    return results


def _loop_state(context: Context, client: Any, role: str, session_id: str) -> str | None:
    workspace = context.state.get("workspace")
    if isinstance(workspace, str) and Path(workspace).is_dir():
        if any(record.get("role") == role for record in task_records(Path(workspace))["active"]):
            return "working"
    messages = client.session_messages(session_id)
    if not isinstance(messages, list):
        return None
    expected = (f"labflow agent pull {role}", f"labflow agent pull {role}")
    for message in reversed(messages):
        if message.get("info", {}).get("role") != "assistant":
            continue
        for part in reversed(message.get("parts", [])):
            state = part.get("state", {})
            command = state.get("input", {}).get("command", "")
            if (part.get("type") == "tool" and part.get("tool") == "bash"
                    and state.get("status") in ("pending", "running")
                    and isinstance(command, str) and any(value in command for value in expected)):
                return "waiting_on_pull"
    return None


def _resume(context: Context, role: str, timeout: float = 15.0,
            *, force: bool = False) -> dict[str, Any]:
    workflow = context.state.get("workflow")
    if not workflow or role not in workflow.get("roles", []):
        raise ControlError(f"unknown workflow role: {role}", 64)
    client = context.client()
    deadline = time.monotonic() + max(timeout, 0)
    children = [child for child in client.children() if child.get("agent") == role]
    statuses = client.statuses()
    if force:
        for child in children:
            session_id = child.get("id")
            if (isinstance(session_id, str)
                    and statuses.get(session_id, {}).get("type") == "busy"):
                client.abort_session(session_id)
        statuses = client.statuses()
    running = [(child, _loop_state(context, client, role, child["id"]))
               for child in children if isinstance(child.get("id"), str)
               and statuses.get(child["id"], {}).get("type") == "busy"]
    running = [(child, state) for child, state in running if state is not None]
    if running and not force:
        child, loop_state = running[-1]
        session_id = child["id"]
        return {
            "schema": "labflow.role-resume/v2", "session_name": context.state["session_name"],
            "role": role, "action": "already_running", "session_id": session_id,
            "previous_runtime_state": statuses[session_id], "runtime_state": statuses[session_id],
            "loop_observed": True, "loop_state": loop_state,
        }

    previous_ids = {child.get("id") for child in children if isinstance(child.get("id"), str)}
    action = "resumed_existing"
    previous_runtime: dict[str, Any] = {"type": "missing"}
    if children and not force and isinstance(children[-1].get("id"), str):
        session_id = children[-1]["id"]
        previous_runtime = statuses.get(session_id, {"type": "unknown"})
        client.prompt_session(session_id, resume_prompt(role), agent=role)
    else:
        action = "recreated"
        response = client.create_session(
            next_session_title(client, f"{context.state['session_base']}.{role}",
                               context.state["lab_root"]),
            parent_id=context.state["session_id"],
        )
        session_id = response.get("id") if isinstance(response, dict) else None
        if not isinstance(session_id, str):
            raise ControlError(f"opencode did not create replacement session for {role}", 69)
        client.prompt_session(session_id, resume_prompt(role), agent=role)

    while True:
        statuses = client.statuses()
        current_children = [child for child in client.children() if child.get("agent") == role]
        loop_state = (_loop_state(context, client, role, session_id)
                      if isinstance(session_id, str) else None)
        if (action == "resumed_existing"
                and statuses.get(session_id, {}).get("type") == "busy"
                and loop_state is not None):
            current_runtime = statuses[session_id]
            break
        replacements = [child for child in current_children
                        if isinstance(child.get("id"), str) and child["id"] not in previous_ids
                        and statuses.get(child["id"], {}).get("type") == "busy"
                        and _loop_state(context, client, role, child["id"]) is not None]
        if replacements:
            session_id = replacements[-1]["id"]
            current_runtime = statuses[session_id]
            loop_state = _loop_state(context, client, role, session_id)
            action = "recreated"
            break
        if time.monotonic() >= deadline:
            if action == "resumed_existing":
                action = "recreated"
                response = client.create_session(
                    next_session_title(client, f"{context.state['session_base']}.{role}",
                                       context.state["lab_root"]),
                    parent_id=context.state["session_id"],
                )
                session_id = response.get("id") if isinstance(response, dict) else None
                if not isinstance(session_id, str):
                    raise ControlError(f"opencode did not create replacement session for {role}", 69)
                client.prompt_session(session_id, resume_prompt(role), agent=role)
                previous_ids.update(child.get("id") for child in current_children)
                deadline = time.monotonic() + max(timeout, 0)
                continue
            raise ControlError(f"timed out waiting for {role} to re-enter the pull loop", 75)
        time.sleep(.1)
    return {
        "schema": "labflow.role-resume/v2",
        "session_name": context.state["session_name"],
        "role": role,
        "session_id": session_id,
        "action": action,
        "previous_runtime_state": previous_runtime,
        "runtime_state": current_runtime,
        "loop_observed": True,
        "loop_state": loop_state,
    }


def _live_children(context: Context) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    client = context.client()
    children = client.children()
    messages = {child["id"]: client.session_messages(child["id"])
                for child in children if isinstance(child.get("id"), str)}
    return children, messages, client.statuses()


def _intervention_summary(context: Context) -> dict[str, Any]:
    directory = context.root / "host-interventions"
    events = []
    if directory.is_dir():
        for path in sorted(directory.glob("*.json")):
            try:
                events.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                raise ControlError(f"invalid Host intervention event: {path}") from None
    return {"count": len(events), "latest": events[-1] if events else None,
            "host_forced": bool(events)}


def _metrics(context: Context) -> tuple[dict[str, Any], dict[str, Any]]:
    workspace = _workspace(context)
    execution = context.state.get("execution", {"kind": "artifact-dag"})
    if execution["kind"] == "thread-service":
        client = context.client()
        statuses = client.statuses()
        service = context.state.get("thread_service", {})
        baseline = service.get("baseline")
        records = thread_records(context)
        sessions = []
        message_map: dict[str, list[dict[str, Any]]] = {}
        root_session = context.state.get("session_id")
        role = execution["role"]
        if isinstance(root_session, str):
            sessions.append({"id": root_session, "agent": role, "title": "qualification baseline"})
            message_map[root_session] = client.session_messages(root_session)
        for record in records:
            session_id = record.get("session_id")
            if not isinstance(session_id, str):
                continue
            sessions.append({"id": session_id, "agent": role,
                             "title": f"thread {record.get('name')}"})
            opened = record.get("opened_at_ms", 0)
            message_map[session_id] = [
                message for message in client.session_messages(session_id)
                if message.get("info", {}).get("time", {}).get("created", 0) >= opened
            ]
        command_definitions = context.state.get(
            "metrics", context.manifest.metrics
        ).get("roles", {}).get(role, {}).get("commands", {})
        metrics = collect_metrics(
            context.state["session_name"], context.state["phase"], workspace, sessions,
            message_map.__getitem__, context.state.get("metrics", context.manifest.metrics),
            {"active": [], "history": []},
        )
        metrics["host_interventions"] = _intervention_summary(context)
        metrics["thread_service"] = {
            "baseline": baseline,
            "baseline_metrics": (
                summarize_thread_metrics(
                    message_map.get(root_session, []), command_definitions
                ) if isinstance(root_session, str) else None
            ),
            "active": service.get("active"),
            "threads": [
                {**record, "metrics": summarize_thread_metrics(
                    message_map.get(record.get("session_id"), []), command_definitions
                )}
                for record in records
            ],
        }
        active_session = (service.get("active") or {}).get("session_id") or root_session
        agents = [{
            "role": role,
            "session_id": active_session,
            "state": "thread_active" if service.get("active") else (
                "ready" if baseline else "qualification"
            ),
            "runtime_state": statuses.get(active_session, {"type": "idle"}),
            "active_thread": service.get("active"),
        }]
        return metrics, {"agents": agents, "records": {"active": [], "history": []}}
    children, messages, statuses = _live_children(context)
    records = task_records(workspace)
    metrics = collect_metrics(
        context.state["session_name"], context.state["phase"], workspace, children,
        messages.__getitem__, context.state.get("metrics", context.manifest.metrics), records,
    )
    metrics["host_interventions"] = _intervention_summary(context)
    agents = []
    by_role = {role["agent"]: role for role in metrics["roles"]}
    active_by_role = {record.get("role"): record for record in records["active"]}
    for role in context.state.get("workflow", {}).get("roles", []):
        role_children = [item for item in children if item.get("agent") == role]
        child = next((item for item in reversed(role_children)
                      if statuses.get(item.get("id"), {}).get("type") == "busy"),
                     role_children[-1] if role_children else None)
        role_metrics = by_role.get(role, {})
        runtime_state = (statuses.get(child.get("id"), {"type": "unknown"})
                         if child else {"type": "not-started"})
        if role in active_by_role:
            workflow_state = "working"
        elif child:
            workflow_state = "waiting_on_pull"
        else:
            workflow_state = "not_started"
        agents.append({
            "role": role,
            "session_id": child.get("id") if child else None,
            "state": workflow_state,
            "runtime_state": runtime_state,
            "latest_task": role_metrics.get("latest_task"),
            "recent_responses": [
                {
                    "finish": message.get("info", {}).get("finish"),
                    "completed": message.get("info", {}).get("time", {}).get("completed"),
                    "text": "\n".join(text_parts(message)),
                }
                for message in assistant_messages(
                    messages.get(child.get("id"), []) if child else []
                )[-5:]
            ],
        })
    return metrics, {"agents": agents, "records": records}


def _status(context: Context, verbose: bool = False) -> dict[str, Any]:
    if context.state["phase"] in ("waiting", "preparing"):
        return {"session_name": context.state["session_name"], "phase": context.state["phase"],
                "workspace": context.state.get("workspace"),
                "next_host_actions": [], "agents": []}
    metrics, detail = _metrics(context)
    if context.state.get("execution", {}).get("kind") == "thread-service":
        service = context.state.get("thread_service", {})
        baseline = service.get("baseline")
        result = {
            "session_name": context.state["session_name"],
            "phase": context.state["phase"],
            "workspace": context.state.get("workspace"),
            "baseline": {
                "approved": baseline is not None,
                "approved_at": baseline.get("approved_at") if baseline else None,
            },
            "active_thread": service.get("active"),
            "threads": metrics["thread_service"]["threads"],
            "agents": detail["agents"],
            "tokens": metrics["aggregate"]["tokens"],
        }
        if not verbose:
            for agent in result["agents"]:
                agent.pop("runtime_state", None)
        return result
    workflow = context.state.get("workflow")
    artifacts = workflow_status(_workspace(context), workflow)
    submittable = [name for name, value in artifacts["artifacts"].items()
                   if value["submittable"]]
    runnable = [name for name, value in artifacts["artifacts"].items()
                if value["runnable"]]
    blocked = [{"artifact": name, "blocked_by": value["blocked_by"]}
               for name, value in artifacts["artifacts"].items()
               if value["owner"] != "host" and not value["current"] and value["blocked_by"]]
    result = {
        "session_name": context.state["session_name"],
        "phase": context.state["phase"],
        "workspace": context.state.get("workspace"),
        "artifact_summary": {
            "submittable": submittable,
            "runnable": runnable,
            "blocked": blocked,
        },
        "next_host_actions": [{
            "action": "submit_artifact",
            "artifact": name,
            "command": (f"labflow host submit {context.state['lab_name']} "
                        f"{context.state['session_name']} {name}"),
        } for name in submittable],
        "agents": detail["agents"],
        "tokens": metrics["aggregate"]["tokens"],
        "host_interventions": _intervention_summary(context),
    }
    if verbose:
        result["artifacts"] = artifacts
        result["task_records"] = detail["records"]
    else:
        for agent in result["agents"]:
            agent.pop("runtime_state", None)
            agent.pop("recent_responses", None)
    return result


def _request_snapshot(context: Context) -> list[str]:
    path = context.root / "observer" / "host-pull.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, json.JSONDecodeError):
        raise ControlError(f"invalid Host pull state: {path}") from None
    requests = value.get("requests") if isinstance(value, dict) else None
    if not isinstance(requests, list) or not all(isinstance(name, str) for name in requests):
        raise ControlError(f"invalid Host pull state: {path}")
    return requests


def _save_request_snapshot(context: Context, requests: list[str]) -> None:
    atomic_json(context.root / "observer" / "host-pull.json", {
        "schema": "labflow.host-pull-state/v1",
        "requests": requests,
        "updated_at": time.time_ns(),
    })


def _host_pull(context: Context, since_ms: int | None, timeout: float = 60.0) -> dict[str, Any]:
    workflow = context.state.get("workflow")
    if not workflow:
        raise ControlError("execution workflow is not prepared", 75)
    if since_ms is not None and since_ms < 0:
        raise ControlError("since must be a non-negative Unix millisecond timestamp", 64)
    if timeout < 0 or timeout > 60:
        raise ControlError("timeout must be between 0 and 60 seconds", 64)
    started_ms = int(time.time() * 1000)
    since = started_ms if since_ms is None else since_ms
    deadline = time.monotonic() + timeout
    reason = "timeout"
    requests = []
    previous_requests = _request_snapshot(context)
    while True:
        artifacts = workflow_status(_workspace(context), workflow)
        at_ms = int(time.time() * 1000)
        requests = pending_requests(workflow, artifacts)
        if requests != previous_requests:
            reason = "requests_changed"
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(min(.2, max(0.0, deadline - time.monotonic())))

    ended_ms = int(time.time() * 1000)
    events = project_events(context, since)
    artifacts = workflow_status(_workspace(context), workflow)
    requests = pending_requests(workflow, artifacts)
    if requests != previous_requests:
        reason = "requests_changed"
    elif reason == "requests_changed":
        reason = "state_changed"
    _save_request_snapshot(context, requests)
    next_since = max([since, *(event["at"] for event in events)])
    return {
        "schema": "labflow.host-pull/v3",
        "session_name": context.state["session_name"],
        "clock": "unix_ms",
        "since": since,
        "next_since": next_since,
        "observed_at": int(time.time() * 1000),
        "waited_ms": ended_ms - started_ms,
        "reason": reason,
        "events": events,
        "requests": requests,
    }


def _start(context: Context) -> dict[str, Any]:
    context.client().health()
    verify_prepared(context.manifest, context.state)
    if context.manifest.execution["kind"] == "thread-service":
        initial = [record for record in context.rounds() if record.get("kind") == "qualification"]
        if initial and initial[0].get("user_message_id"):
            return initial[0]
        prompt = (context.manifest.root / context.manifest.execution["start"]).read_text(
            encoding="utf-8"
        )
        return send_round(
            context, "qualification", prompt, require_empty=True,
            agent=context.manifest.execution["role"],
        )
    initial = [record for record in context.rounds() if record.get("kind") == "initial"]
    if initial and initial[0].get("user_message_id"):
        return initial[0]
    return send_round(context, "initial", START_PROMPT, require_empty=True)


def _abort_sessions(context: Context, timeout: float = 5.0) -> dict[str, Any]:
    root_session = context.state.get("session_id")
    if not isinstance(root_session, str):
        raise ControlError("execution has no session to abort", 75)
    client = context.client()
    pending = [root_session]
    sessions: list[str] = []
    while pending:
        session_id = pending.pop()
        if session_id in sessions:
            continue
        sessions.append(session_id)
        pending.extend(
            child["id"] for child in client.children(session_id)
            if isinstance(child.get("id"), str)
        )
    if context.state.get("execution", {}).get("kind") == "thread-service":
        for record in thread_records(context):
            session_id = record.get("session_id")
            if isinstance(session_id, str) and session_id not in sessions:
                sessions.append(session_id)

    statuses = client.statuses()
    active = [session_id for session_id in sessions
              if statuses.get(session_id, {}).get("type") == "busy"]
    for session_id in reversed(active):
        client.abort_session(session_id)

    deadline = time.monotonic() + max(timeout, 0)
    remaining = list(active)
    while remaining and time.monotonic() < deadline:
        statuses = client.statuses()
        remaining = [session_id for session_id in active
                     if statuses.get(session_id, {}).get("type") == "busy"]
        if remaining:
            time.sleep(.05)
    if remaining:
        raise ControlError(f"timed out aborting session(s): {', '.join(remaining)}", 75)
    return {
        "schema": "labflow.sessions-abort/v1",
        "session_name": context.state["session_name"],
        "sessions": sessions,
        "aborted": active,
        "already_idle": [session_id for session_id in sessions if session_id not in active],
    }


def main(argv: list[str] | None = None, *, prog: str = "labflow host") -> int:
    args = parser(prog).parse_args(argv)
    try:
        if args.command == "test-connect":
            emit(_test_connect(_controller_repo(), args.lab_name))
            return 0
        if args.command == "start":
            repo = _controller_repo()
            configured = _configure_start(
                repo, args.lab_name, args.plan_id, args.from_session, args.bundle
            )
            client = Client(
                f"http://127.0.0.1:{configured['port']}", configured["root"]
            )
            session_name = next_session_title(client, args.plan_id, configured["root"])
            root, state, _ = prepare(
                args.plan_id,
                session_name,
                configured["port"],
                from_session=args.from_session,
                lab_name=args.lab_name,
                lab_root=configured["root"],
            )
            execution = state.get("execution", {"kind": "artifact-dag"})
            if execution["kind"] == "thread-service":
                manifest = load_manifest(repo, args.plan_id)
                state = install_bundle(root, state, manifest, configured.get("bundle"))
            create_execution_session(root, state, session_name)
            context = resolve(args.lab_name, session_name, repo)
            emit({"session_name": session_name, **_start(context)})
            return 0
        context = resolve(args.lab_name, args.session_name, _controller_repo())
        if args.command == "status":
            emit(_status(context, args.verbose))
        elif args.command == "pull":
            emit(_host_pull(context, args.since, args.timeout))
        elif args.command == "event":
            emit(event_detail(context, args.event_id))
        elif args.command == "stat":
            emit(_metrics(context)[0])
        elif args.command == "update":
            emit(_update(context, args.assets, force=args.force))
        elif args.command == "submit":
            emit(_submit(context, args.artifacts, force=args.force))
        elif args.command == "resume":
            emit(_resume(context, args.role, args.timeout, force=args.force))
        elif args.command == "abort-sessions":
            emit(_abort_sessions(context))
        elif args.command == "approve-baseline":
            emit(approve_baseline(context, args.role))
        elif args.command == "open-thread":
            emit(open_thread(context, args.role, args.thread_name, args.problem_file))
        elif args.command == "comment-thread":
            emit(comment_thread(
                context, args.role, args.thread_name, args.comment_file
            ))
        elif args.command == "close-thread":
            emit(close_thread(context, args.role))
        return 0
    except (ControlError, TaskError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return getattr(exc, "code", 65)
    except (FileNotFoundError, PermissionError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return 66
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
