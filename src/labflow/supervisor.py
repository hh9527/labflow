from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import sys
import time
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from .client import Client
from .config import ControlError
from .events import pending_optional_requests, pending_requests
from .runtime_opencode import configure_task_role, reset_role, resume_prompt
from .state import atomic_json
from .task_cli import (
    TaskError, assign_task, clear_session_qualifications, submit, task_records,
    supersede_role_task, workflow_status,
)
from .timeline_projection import closed_message_events
from .timeline_store import TimelineWriter


@dataclass(frozen=True)
class LifecycleEvent:
    kind: str
    execution: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Effect:
    key: str
    kind: str
    execution: str
    role: str
    title: str
    backend_id: str | None = None
    artifact: str | None = None


@dataclass
class EffectState:
    status: str
    execution: str
    error: str | None = None


@dataclass
class SessionState:
    role: str
    title: str
    backend_id: str | None = None
    status: str = "missing"
    idle_epoch: int = 0
    missing_epoch: int = 0
    observed: bool = False
    error: str | None = None


@dataclass
class ExecutionState:
    title: str
    workspace: str
    root_session_id: str | None
    active: bool
    dag: bool
    roles: tuple[str, ...]
    workflow: dict[str, Any] | None = None
    dag_revision: str | None = None
    artifacts: dict[str, int] = field(default_factory=dict)
    runnable: dict[str, tuple[tuple[str, int], ...]] = field(default_factory=dict)
    requests: tuple[str, ...] = ()
    optional_requests: tuple[str, ...] = ()
    request_versions: dict[str, int] = field(default_factory=dict)
    sessions: dict[str, SessionState] = field(default_factory=dict)
    observed_sessions: dict[str, SessionState] = field(default_factory=dict)
    sessions_initialized: bool = False


@dataclass
class SupervisorState:
    executions: dict[str, ExecutionState] = field(default_factory=dict)
    effects: dict[str, EffectState] = field(default_factory=dict)
    seen_messages: set[tuple[str, str, str]] = field(default_factory=set)


def _session_dump(value: SessionState) -> dict[str, Any]:
    return {
        "role": value.role, "title": value.title, "backend_id": value.backend_id,
        "status": value.status, "idle_epoch": value.idle_epoch,
        "missing_epoch": value.missing_epoch, "observed": value.observed,
        "error": value.error,
    }


def _session_load(value: dict[str, Any]) -> SessionState:
    return SessionState(
        str(value["role"]), str(value["title"]), value.get("backend_id"),
        str(value.get("status", "missing")), int(value.get("idle_epoch", 0)),
        int(value.get("missing_epoch", 0)), bool(value.get("observed", False)),
        value.get("error"),
    )


def _state_dump(state: SupervisorState) -> dict[str, Any]:
    return {
        "executions": {
            title: {
                "title": value.title, "workspace": value.workspace,
                "root_session_id": value.root_session_id, "active": value.active,
                "dag": value.dag, "roles": list(value.roles), "workflow": value.workflow,
                "dag_revision": value.dag_revision, "artifacts": value.artifacts,
                "runnable": {role: [list(item) for item in items]
                             for role, items in value.runnable.items()},
                "requests": list(value.requests),
                "optional_requests": list(value.optional_requests),
                "request_versions": value.request_versions,
                "sessions": {role: _session_dump(session)
                             for role, session in value.sessions.items()},
                "observed_sessions": {identity: _session_dump(session)
                                      for identity, session in value.observed_sessions.items()},
                "sessions_initialized": value.sessions_initialized,
            }
            for title, value in state.executions.items()
        },
        "effects": {
            key: {"status": value.status, "execution": value.execution,
                  "error": value.error}
            for key, value in state.effects.items()
        },
        "seen_messages": [list(value) for value in sorted(state.seen_messages)],
    }


def _state_load(value: Any) -> SupervisorState:
    if not isinstance(value, dict):
        return SupervisorState()
    state = SupervisorState()
    try:
        for title, item in value.get("executions", {}).items():
            state.executions[str(title)] = ExecutionState(
                str(item["title"]), str(item["workspace"]), item.get("root_session_id"),
                bool(item["active"]), bool(item["dag"]), tuple(item.get("roles", ())),
                workflow=item.get("workflow"), dag_revision=item.get("dag_revision"),
                artifacts={str(name): int(stamp)
                           for name, stamp in item.get("artifacts", {}).items()},
                runnable={str(role): tuple((str(task[0]), int(task[1])) for task in tasks)
                          for role, tasks in item.get("runnable", {}).items()},
                requests=tuple(item.get("requests", ())),
                optional_requests=tuple(item.get("optional_requests", ())),
                request_versions={str(name): int(stamp)
                                  for name, stamp in item.get("request_versions", {}).items()},
                sessions={str(role): _session_load(session)
                          for role, session in item.get("sessions", {}).items()},
                observed_sessions={str(identity): _session_load(session)
                                   for identity, session in item.get("observed_sessions", {}).items()},
                sessions_initialized=bool(item.get("sessions_initialized", False)),
            )
        state.effects = {
            str(key): EffectState(str(item["status"]), str(item["execution"]), item.get("error"))
            for key, item in value.get("effects", {}).items()
        }
        state.seen_messages = {
            (str(item[0]), str(item[1]), str(item[2]))
            for item in value.get("seen_messages", ()) if len(item) == 3
        }
    except (KeyError, TypeError, ValueError):
        return SupervisorState()
    return state


def _effect_key(*values: Any) -> str:
    data = json.dumps(values, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(data.encode()).hexdigest()


def _workflow_revision(workflow: Any) -> str | None:
    if not isinstance(workflow, dict):
        return None
    encoded = json.dumps(
        workflow, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _milliseconds(value: Any) -> int | None:
    return value // 1_000_000 if isinstance(value, int) else None


def _message_task(value: dict[str, Any], session_id: str, role: str,
                  message: dict[str, Any]) -> tuple[str, str] | None:
    info = message.get("info", {})
    timing = info.get("time", {})
    created = int(timing.get("created", 0))
    completed = int(timing.get("completed", created))
    records = task_records(Path(value["workspace"]))
    candidates = []
    for task in (*records["history"], *records["active"]):
        targets = task.get("artifacts")
        started = _milliseconds(task.get("started_at_ns"))
        ended = _milliseconds(task.get("submitted_at_ns") or task.get("ended_at_ns"))
        if (task.get("role") == role and isinstance(targets, list) and len(targets) == 1
                and isinstance(targets[0], str) and started is not None
                and started <= completed and (ended is None or ended >= created)):
            candidates.append((started, targets[0]))
    if not candidates:
        return None
    return "artifact", max(candidates)[1]


@contextmanager
def supervisor_lock(root: Path) -> Iterator[None]:
    """Ensure one process owns reconciliation and Timeline writes for a Lab."""
    path = root / "lock"
    with path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ControlError("another Supervisor already owns this laboratory", 75) from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def control_mtime(path: Path) -> int | None:
    try:
        value = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(value.st_mode):
        raise ControlError(f"control marker is not a regular file: {path}")
    return value.st_mtime_ns


def _requested(state: SupervisorState, effect: Effect) -> bool:
    if effect.key in state.effects:
        return False
    state.effects[effect.key] = EffectState("requested", effect.execution)
    return True


def _reconcile_role(state: SupervisorState, execution: ExecutionState,
                    role: str) -> list[Effect]:
    if (not execution.active or not execution.dag or not execution.root_session_id
            or not execution.sessions_initialized):
        return []
    session = execution.sessions.setdefault(role, SessionState(role, role))
    if session.status == "missing":
        effect = Effect(
            _effect_key("create", execution.title, role, session.missing_epoch),
            "create_session", execution.title, role, role,
        )
        return [effect] if _requested(state, effect) else []
    runnable = execution.runnable.get(role, ())
    if session.status != "idle" or not runnable or not session.backend_id:
        return []
    effect = Effect(
        _effect_key("prompt", execution.title, session.title, runnable,
                    session.idle_epoch),
        "prompt_session", execution.title, role, session.title, session.backend_id,
        runnable[0][0],
    )
    return [effect] if _requested(state, effect) else []


def _reconcile_execution(state: SupervisorState,
                         execution: ExecutionState) -> list[Effect]:
    effects = []
    for role in execution.roles:
        effects.extend(_reconcile_role(state, execution, role))
    return effects


def reduce(state: SupervisorState, event: LifecycleEvent) -> list[Effect]:
    """Mutate one event-loop-owned state object and return external work."""
    if event.kind == "execution_deleted":
        state.executions.pop(event.execution, None)
        state.effects = {
            key: record for key, record in state.effects.items()
            if record.execution != event.execution
        }
        return []
    if event.kind == "execution_updated":
        data = event.data
        previous = state.executions.get(event.execution)
        execution = ExecutionState(
            event.execution,
            str(data["workspace"]),
            data.get("root_session_id"),
            bool(data.get("active")),
            bool(data.get("dag")),
            tuple(data.get("roles", ())),
            workflow=data.get("workflow"),
            dag_revision=data.get("dag_revision"),
            artifacts=previous.artifacts if previous else {},
            sessions=previous.sessions if previous else {},
            runnable=(previous.runnable if previous and data.get("dag") else {}),
            requests=(previous.requests if previous and data.get("dag") else ()),
            optional_requests=(previous.optional_requests
                               if previous and data.get("dag") else ()),
            request_versions=(previous.request_versions
                              if previous and data.get("dag") else {}),
            observed_sessions=previous.observed_sessions if previous else {},
            sessions_initialized=previous.sessions_initialized if previous else False,
        )
        state.executions[event.execution] = execution
        return []
    execution = state.executions.get(event.execution)
    if execution is None:
        return []
    if event.kind == "workflow_observed":
        execution.dag_revision = event.data.get("dag_revision")
        execution.runnable = {
            role: tuple((str(item[0]), int(item[1])) for item in items)
            for role, items in event.data.get("runnable", {}).items()
        }
        execution.requests = tuple(event.data.get("requests", ()))
        execution.optional_requests = tuple(event.data.get("optional_requests", ()))
        execution.request_versions = {
            str(name): int(version)
            for name, version in event.data.get("request_versions", {}).items()
        }
        return _reconcile_execution(state, execution)
    if event.kind == "artifact_updated":
        execution.artifacts[str(event.data["artifact"])] = int(event.data["mtime_ns"])
        return []
    if event.kind == "artifact_deleted":
        execution.artifacts.pop(str(event.data["artifact"]), None)
        return []
    if event.kind == "observed_sessions_updated":
        execution.sessions_initialized = True
        execution.observed_sessions = {
            str(item["backend_id"]): SessionState(
                str(item["role"]), str(item["title"]),
                backend_id=str(item["backend_id"]),
                status=str(item["status"]),
            )
            for item in event.data.get("sessions", ())
        }
        return []
    if event.kind == "session_observed":
        role = str(event.data["role"])
        status = str(event.data["status"])
        session = execution.sessions.setdefault(role, SessionState(role, role))
        # prompt_async may be accepted just before OpenCode reports busy.
        if (session.status == "prompted" and status == "idle"
                and not event.data.get("completed_turn")):
            return []
        if status == "idle" and session.status != "idle":
            session.idle_epoch += 1
        session.status = status
        session.title = str(event.data["title"])
        session.backend_id = str(event.data["backend_id"])
        session.observed = True
        if "error" in event.data:
            session.error = event.data.get("error")
        return []
    if event.kind == "session_missing":
        role = str(event.data["role"])
        session = execution.sessions.setdefault(role, SessionState(role, role))
        # A successful create may precede visibility in the backend's child index.
        if session.backend_id is not None and not session.observed:
            return []
        if session.status not in ("missing", "failed"):
            session.missing_epoch += 1
        session.status = "missing"
        session.backend_id = None
        session.observed = False
        return _reconcile_role(state, execution, role)
    if event.kind in ("effect_succeeded", "effect_failed"):
        key = str(event.data["key"])
        record = state.effects.get(key)
        if record is None:
            return []
        if event.kind == "effect_failed":
            record.status = "failed"
            record.error = str(event.data.get("error", "effect failed"))
            role = str(event.data["role"])
            session = execution.sessions.setdefault(role, SessionState(role, role))
            session.status = "failed"
            session.error = record.error
            return []
        record.status = "succeeded"
        role = str(event.data["role"])
        session = execution.sessions.setdefault(role, SessionState(role, role))
        if event.data.get("effect_kind") == "create_session":
            session.backend_id = str(event.data["backend_id"])
            session.title = str(event.data["title"])
            session.status = "idle"
            session.observed = False
            session.idle_epoch += 1
            return _reconcile_role(state, execution, role)
        if event.data.get("effect_kind") == "prompt_session":
            session.status = "prompted" if event.data.get("delivered", True) else "idle"
        return []
    return []


def _session_tree(client: Client, root_id: str, root_title: str,
                  root_role: str) -> list[dict[str, str]]:
    output = [{"id": root_id, "title": root_title, "role": root_role}]
    pending = [root_id]
    seen = {root_id}
    while pending:
        parent = pending.pop()
        for child in client.children(parent):
            session_id = child.get("id")
            if not isinstance(session_id, str) or session_id in seen:
                continue
            seen.add(session_id)
            pending.append(session_id)
            output.append({
                "id": session_id,
                "title": str(child.get("title") or child.get("agent") or session_id),
                "role": str(child.get("agent") or child.get("title") or "unknown"),
            })
    return output


def _observed_sessions(client: Client, root_id: str, root_title: str,
                       root_role: str) -> list[dict[str, str]]:
    return _session_tree(client, root_id, root_title, root_role)


class Supervisor:
    def __init__(self, exec_home: Path, port: int, *, poll_interval: float = .25):
        self.root = exec_home.resolve(strict=True)
        if self.root.name != ".labflow-exec":
            raise ControlError(f"invalid execution home: {self.root}", 64)
        self.server_url = f"http://127.0.0.1:{port}"
        self.poll_interval = poll_interval
        from .project import get_state, load_execution
        _, self.manifest, self.config = load_execution(self.root.parent)
        if self.config["port"] != port:
            raise ControlError(
                f"execution is configured for port {self.config['port']}, not {port}", 64
            )
        control = get_state(self.root, "active_control", {})
        if not isinstance(control, dict):
            raise ControlError("invalid active control state")
        observed = control.get("observed_mtime_ns")
        applied = control.get("applied_mtime_ns")
        error = control.get("error")
        if ((observed is not None and not isinstance(observed, int))
                or (applied is not None and not isinstance(applied, int))
                or (error is not None and not isinstance(error, str))):
            raise ControlError("invalid active control state")
        self.active_mtime: int | None = observed
        self.applied_active_mtime: int | None = applied
        self.plan_error: str | None = error
        current_active = control_mtime(self.root / "ctrl" / "active")
        self.active = bool(
            current_active is not None and current_active == observed == applied
            and error is None
        )
        self.state = _state_load(get_state(self.root, "supervisor_state"))
        self.event_error: str | None = None
        try:
            self.writer: TimelineWriter | None = TimelineWriter(self.root / "events.sqlite")
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            self.writer = None
            self.event_error = str(exc)

    def close(self) -> None:
        if self.writer is not None:
            try:
                self.writer.close()
            except (OSError, RuntimeError, sqlite3.Error) as exc:
                self.event_error = str(exc)
            self.writer = None

    def _events(self, records: list[dict[str, Any]]) -> None:
        if not records or self.writer is None:
            return
        try:
            self.writer.submit(records)
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            self.event_error = str(exc)
            self.writer = None

    def _desired(self) -> dict[str, Path]:
        return {self.manifest.plan_id: self.root}

    def _store_active_control(self) -> None:
        from .project import set_state
        set_state(self.root, "active_control", {
            "observed_mtime_ns": self.active_mtime,
            "applied_mtime_ns": self.applied_active_mtime,
            "error": self.plan_error,
        })

    def _sync_active(self) -> None:
        marker = self.root / "ctrl" / "active"
        current = control_mtime(marker)
        if current == self.active_mtime:
            return
        self.active_mtime = current
        self.active = False
        if current is None:
            self.plan_error = None
            self._store_active_control()
            return
        try:
            from .project import activate_plan, load_plan
            manifest = load_plan(self.manifest.root / "labflow-plan.toml")
            activate_plan(self.root, manifest)
        except (ControlError, OSError, ValueError) as exc:
            self.plan_error = str(exc)
            self._store_active_control()
            return
        previous_revision = _workflow_revision(self.manifest.workflow)
        current_revision = _workflow_revision(manifest.workflow)
        if current_revision != previous_revision:
            for role in set(self.manifest.workflow["roles"]) | set(manifest.workflow["roles"]):
                supersede_role_task(self.manifest.root, role, "Plan was reloaded")
        self.manifest = manifest
        self.applied_active_mtime = current
        self.plan_error = None
        self.active = True
        self._store_active_control()

    def _execution_event(self, title: str, desired: Path) -> tuple[LifecycleEvent, dict[str, Any]]:
        from .project import get_state
        manifest, config = self.manifest, self.config
        value = {
            "title": title,
            "workspace": str(manifest.root),
            "session_id": get_state(desired, "root_session_id"),
            "workflow": manifest.workflow,
            "execution": manifest.execution,
            "phase": "active" if self.active else "idle",
            "server_url": self.server_url,
            "lab_root": config["lab_root"],
        }
        workflow = value.get("workflow")
        roles = tuple(workflow.get("roles", ())) if isinstance(workflow, dict) else ()
        active = self.active and value.get("phase") in {"ready", "active", "idle"}
        event = LifecycleEvent("execution_updated", title, {
            "workspace": value["workspace"],
            "root_session_id": value.get("session_id"),
            "active": active,
            "dag": (desired / "artifacts").is_dir() and isinstance(workflow, dict),
            "roles": roles,
            "workflow": workflow,
            "dag_revision": _workflow_revision(workflow),
        })
        return event, value

    def _execution_path(self, title: str) -> Path:
        return self.root

    def _ensure_root_session(self) -> None:
        from .project import get_state, set_state
        if not self.active:
            return
        if get_state(self.root, "root_session_id") is not None:
            return
        manifest = self.manifest
        client = Client(self.server_url, str(manifest.root))
        client.health()
        candidates = [
            item for item in client.sessions()
            if isinstance(item, dict) and item.get("title") == manifest.plan_id
            and not item.get("parentID")
        ]
        if len(candidates) == 1 and isinstance(candidates[0].get("id"), str):
            set_state(self.root, "root_session_id", candidates[0]["id"])
            return
        if len(candidates) > 1:
            raise ControlError("multiple OpenCode root Sessions match this execution", 75)
        response = client.create_session(manifest.plan_id, agent="coordinator")
        session_id = response.get("id") if isinstance(response, dict) else None
        if not isinstance(session_id, str):
            raise ControlError("OpenCode returned an invalid root Session identity", 69)
        set_state(self.root, "root_session_id", session_id)

    def _workflow_event(self, title: str, value: dict[str, Any]) -> LifecycleEvent | None:
        workflow = value.get("workflow")
        if not isinstance(workflow, dict):
            return None
        artifacts = workflow_status(Path(value["workspace"]), workflow)
        runnable: dict[str, list[tuple[str, int]]] = {}
        for name, status in artifacts["artifacts"].items():
            if status["runnable"]:
                runnable.setdefault(status["owner"], []).append(
                    (name, int(status["input_mtime_ns"])),
                )
        required = pending_requests(workflow, artifacts)
        optional = pending_optional_requests(workflow, artifacts)
        return LifecycleEvent("workflow_observed", title, {
            "runnable": runnable,
            "requests": required,
            "optional_requests": optional,
            "request_versions": {
                name: int(artifacts["artifacts"][name]["input_mtime_ns"])
                for name in (*required, *optional)
            },
            "dag_revision": _workflow_revision(workflow),
        })

    def _dag_revision_record(self, title: str, workflow: LifecycleEvent) -> dict[str, Any]:
        revision = str(workflow.data["dag_revision"])
        return {
            "id": f"dag-revised:{title}:{revision}",
            "execution": title,
            "type": "dag_revised",
            "at": int(time.time() * 1000),
            "duration": 0,
            "dag_revision": revision,
        }

    def _task_records(self, title: str, value: dict[str, Any]) -> list[dict[str, Any]]:
        revision = _workflow_revision(value.get("workflow"))
        workspace = Path(value["workspace"])
        records = task_records(workspace)
        result: list[dict[str, Any]] = []
        for task in (*records["history"], *records["active"]):
            attempt = task.get("task_id")
            targets = task.get("artifacts")
            started = _milliseconds(task.get("started_at_ns"))
            if (not isinstance(attempt, str) or not isinstance(targets, list)
                    or len(targets) != 1 or not isinstance(targets[0], str)
                    or started is None):
                continue
            role = task.get("role")
            session = None
            execution = self.state.executions.get(title)
            if execution is not None and isinstance(role, str):
                known = execution.sessions.get(role)
                session = known.title if known is not None else role
            base = {
                "execution": title, "session": session, "role": role,
                "task_kind": "artifact", "task_id": targets[0],
                "dag_revision": revision, "duration": 0,
                "payload": {"attempt_id": attempt},
            }
            result.append({
                **base, "id": f"task-started:{title}:{attempt}",
                "type": "task_started", "at": started,
            })
            ended = _milliseconds(task.get("submitted_at_ns") or task.get("ended_at_ns"))
            if ended is not None:
                result.append({
                    **base, "id": f"task-completed:{title}:{attempt}",
                    "type": "task_completed", "at": ended,
                    "payload": {"attempt_id": attempt, "status": task.get("status")},
                })
        return result

    def _request_records(self, title: str, workflow: LifecycleEvent,
                         previous: tuple[set[str], set[str], dict[str, int]]) -> list[dict[str, Any]]:
        old_required, old_optional, old_versions = previous
        required = set(workflow.data.get("requests", ()))
        optional = set(workflow.data.get("optional_requests", ()))
        revision = str(workflow.data["dag_revision"])
        versions = workflow.data.get("request_versions", {})
        now = int(time.time() * 1000)
        result = []
        for optional_flag, before, after in (
            (False, old_required, required), (True, old_optional, optional),
        ):
            for kind, names in (
                ("host_request_opened", after - before),
                ("host_request_resolved", before - after),
            ):
                for name in sorted(names):
                    marker = versions.get(name, old_versions.get(name, 0))
                    result.append({
                        "id": f"{kind}:{title}:{revision}:{name}:{marker}",
                        "execution": title, "type": kind, "at": now, "duration": 0,
                        "task_kind": "artifact", "task_id": name,
                        "artifact": name, "dag_revision": revision,
                        "payload": {"optional": optional_flag},
                    })
        return result

    def _artifact_events(self, title: str, desired: Path) -> list[LifecycleEvent]:
        directory = desired / "artifacts"
        previous = self.state.executions[title].artifacts
        current = ({path.name: path.stat().st_mtime_ns for path in directory.iterdir()
                    if path.is_file() and not path.is_symlink()}
                   if directory.is_dir() else {})
        execution = self.state.executions[title]
        workflow = execution.workflow
        if isinstance(workflow, dict):
            status = workflow_status(Path(execution.workspace), workflow)["artifacts"]
            task_publications = {
                (item["artifact"], int(item["mtime_ns"]))
                for task in task_records(Path(execution.workspace))["history"]
                for item in task.get("artifacts_refreshed", ())
                if isinstance(item, dict) and isinstance(item.get("artifact"), str)
                and isinstance(item.get("mtime_ns"), int)
            }
            for name, mtime in list(current.items()):
                if previous.get(name) == mtime:
                    continue
                artifact = status.get(name)
                accepted = bool(
                    artifact is not None and (
                        (artifact["owner"] == "host" and artifact["current"])
                        or (artifact["owner"] != "host"
                            and (name, mtime) in task_publications)
                    )
                )
                if accepted:
                    continue
                path = directory / name
                old_mtime = previous.get(name)
                if old_mtime is None:
                    path.unlink(missing_ok=True)
                    current.pop(name, None)
                else:
                    os.utime(path, ns=(old_mtime, old_mtime))
                    current[name] = old_mtime
        events = [LifecycleEvent("artifact_deleted", title, {"artifact": name})
                  for name in previous.keys() - current.keys()]
        events.extend(
            LifecycleEvent("artifact_updated", title, {
                "artifact": name, "mtime_ns": mtime,
            })
            for name, mtime in current.items()
            if previous.get(name) != mtime
        )
        return events

    def _artifact_records(self, title: str,
                          events: list[LifecycleEvent]) -> list[dict[str, Any]]:
        revision = self.state.executions[title].dag_revision
        now = int(time.time() * 1000)
        result = []
        for event in events:
            name = str(event.data["artifact"])
            refreshed = event.kind == "artifact_updated"
            at = (_milliseconds(event.data.get("mtime_ns")) if refreshed else now) or now
            marker = event.data.get("mtime_ns", at)
            result.append({
                "id": f"{event.kind}:{title}:{name}:{marker}",
                "execution": title,
                "type": "artifact_refreshed" if refreshed else "artifact_deleted",
                "at": at, "duration": 0, "artifact": name,
                "task_kind": "artifact", "task_id": name,
                "dag_revision": revision,
            })
        return result

    def _observe_sessions(self, title: str, value: dict[str, Any]) -> list[Effect]:
        root_id = value.get("session_id")
        if not isinstance(root_id, str):
            return []
        root_role = "coordinator"
        client = Client(self.server_url, value["workspace"], root_id)
        statuses = client.statuses()
        sessions = _observed_sessions(client, root_id, title, root_role)
        observed = [{
            **session,
            "backend_id": session["id"],
            "status": statuses.get(session["id"], {"type": "idle"}).get("type", "idle"),
        } for session in sessions]
        reduce(self.state, LifecycleEvent(
            "observed_sessions_updated", title, {"sessions": observed},
        ))
        effects: list[Effect] = []
        completed_turns: set[str] = set()
        stopped_at: dict[str, int] = {}
        for session in sessions:
            session_id = session["id"]
            role = session["role"]
            messages = client.session_messages(session_id)
            created = [
                int(message.get("info", {}).get("time", {}).get("created"))
                for message in messages
                if isinstance(message.get("info", {}).get("time", {}).get("created"),
                              (int, float))
            ]
            self._events([{
                "id": f"session-started:{title}:{session_id}",
                "execution": title, "session": session["title"],
                "role": role, "type": "session_started",
                "at": min(created, default=int(time.time() * 1000)), "duration": 0,
                "payload": {"backend_id": session_id},
            }])
            for message in messages:
                info = message.get("info", {})
                message_id = info.get("id")
                completed = info.get("time", {}).get("completed")
                identity = (title, session["title"], str(message_id))
                if (not isinstance(message_id, str) or not isinstance(completed, (int, float))
                        or identity in self.state.seen_messages):
                    continue
                if info.get("role") == "assistant":
                    completed_turns.add(session_id)
                    completed = info.get("time", {}).get("completed")
                    if info.get("finish") == "stop" and isinstance(completed, (int, float)):
                        stopped_at[session_id] = max(
                            stopped_at.get(session_id, 0), int(completed),
                        )
                records = closed_message_events(
                    title, session["title"], role, message,
                    task=_message_task(value, session_id, role, message),
                    dag_revision=self.state.executions[title].dag_revision,
                )
                if records:
                    self._events(records)
                self.state.seen_messages.add(identity)
        if self.state.executions[title].dag:
            for role in self.state.executions[title].roles:
                matches = [session for session in sessions if session["role"] == role]
                if not matches:
                    effects.extend(reduce(self.state, LifecycleEvent(
                        "session_missing", title, {"role": role},
                    )))
                elif len(matches) == 1:
                    session = matches[0]
                    runtime_status = statuses.get(
                        session["id"], {"type": "idle"}
                    ).get("type", "idle")
                    active = [
                        item for item in task_records(Path(value["workspace"]))["active"]
                        if item.get("role") == role
                    ]
                    started = (active[0].get("started_at_ns")
                               if len(active) == 1 else None)
                    if (runtime_status == "idle" and isinstance(started, int)
                            and stopped_at.get(session["id"], 0) >= started // 1_000_000):
                        if active:
                            try:
                                submit(
                                    Path(value["workspace"]), value["workflow"], role,
                                    list(active[0]["artifacts"]),
                                )
                            except TaskError as exc:
                                self.state.executions[title].sessions.setdefault(
                                    role, SessionState(role, role)
                                ).error = str(exc)
                            else:
                                reset_role(self.manifest, role, self.root)
                    effects.extend(reduce(self.state, LifecycleEvent(
                        "session_observed", title, {
                            "role": role, "title": session["title"],
                            "backend_id": session["id"],
                            "status": runtime_status,
                            "completed_turn": session["id"] in completed_turns,
                        },
                    )))
                else:
                    effects.extend(reduce(self.state, LifecycleEvent(
                        "session_observed", title, {
                            "role": role, "title": role,
                            "backend_id": matches[0]["id"], "status": "failed",
                            "error": f"duplicate Session for role {role}",
                        },
                    )))
        return effects

    def _execute(self, effects: list[Effect]) -> None:
        pending = list(effects)
        while pending:
            effect = pending.pop(0)
            execution = self.state.executions.get(effect.execution)
            if execution is None:
                continue
            client = Client(self.server_url, execution.workspace,
                            execution.root_session_id)
            try:
                backend_id = effect.backend_id
                delivered = True
                if effect.kind == "create_session":
                    reset_role(self.manifest, effect.role, self.root)
                    if execution.workflow is not None:
                        clear_session_qualifications(
                            Path(execution.workspace), execution.workflow, effect.role,
                        )
                    response = client.create_session(
                        effect.title, parent_id=execution.root_session_id,
                        agent=effect.role,
                    )
                    backend_id = response.get("id") if isinstance(response, dict) else None
                    if not isinstance(backend_id, str):
                        raise ControlError("OpenCode returned an invalid Session identity", 69)
                elif effect.kind == "prompt_session":
                    if not backend_id or not effect.artifact or execution.workflow is None:
                        raise ControlError("task dispatch effect is incomplete")
                    task = assign_task(
                        Path(execution.workspace), execution.workflow,
                        effect.role, effect.artifact,
                    )
                    delivered = task is not None
                    if task is not None:
                        configure_task_role(
                            self.manifest, effect.role, effect.artifact,
                            self.root,
                        )
                        session = execution.sessions.setdefault(
                            effect.role, SessionState(effect.role, effect.role)
                        )
                        client.prompt_session(
                            backend_id,
                            resume_prompt(
                                effect.role,
                                execution.workflow["artifacts"][effect.artifact],
                                task,
                                session.error,
                            ),
                            agent=effect.role,
                        )
                        session.error = None
                else:
                    raise ControlError(f"unknown Supervisor effect: {effect.kind}")
                pending.extend(reduce(self.state, LifecycleEvent(
                    "effect_succeeded", effect.execution, {
                        "key": effect.key, "effect_kind": effect.kind,
                        "role": effect.role, "title": effect.title,
                        "backend_id": backend_id,
                        "delivered": delivered,
                    },
                )))
            except (ControlError, TaskError, OSError) as exc:
                reduce(self.state, LifecycleEvent(
                    "effect_failed", effect.execution, {
                        "key": effect.key, "effect_kind": effect.kind,
                        "role": effect.role, "error": str(exc),
                    },
                ))
                raise

    def _status(self) -> dict[str, Any]:
        return {
            "schema": "labflow.supervisor-status/v1",
            "updated_at": int(time.time() * 1000),
            "event_error": self.event_error,
            "plan_error": self.plan_error,
            "executions": [{
                "title": execution.title,
                "dag": execution.dag,
                "requests": list(execution.requests),
                "optional_requests": list(execution.optional_requests),
                "errors": [{
                    "role": session.role,
                    "error": session.error,
                } for session in execution.sessions.values()
                    if session.status == "failed" and session.error],
                "sessions": [{
                    "backend_id": session.backend_id,
                    "title": session.title,
                    "role": session.role,
                    "status": session.status,
                } for session in execution.observed_sessions.values()],
            } for execution in self.state.executions.values()],
        }

    def _write_host_tasks(self) -> None:
        path = self.root / "host-tasks.json"
        grouped = {
            title: {
                "tasks": list(execution.requests),
                "optional_tasks": list(execution.optional_requests),
            }
            for title, execution in sorted(self.state.executions.items())
        }
        value = next(iter(grouped.values()), {"tasks": [], "optional_tasks": []})
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            current = None
        if current != value:
            atomic_json(path, value)

    def step(self) -> None:
        self._sync_active()
        if self.writer is not None:
            try:
                self.writer.check()
            except (OSError, RuntimeError, sqlite3.Error) as exc:
                self.event_error = str(exc)
                self.writer = None
        self._ensure_root_session()
        desired = self._desired()
        for title in set(self.state.executions) - set(desired):
            reduce(self.state, LifecycleEvent("execution_deleted", title))
        for title, path in desired.items():
            event, value = self._execution_event(title, path)
            effects = reduce(self.state, event)
            artifact_events = self._artifact_events(title, path)
            self._events(self._artifact_records(title, artifact_events))
            for artifact_event in artifact_events:
                effects.extend(reduce(self.state, artifact_event))
            effects.extend(self._observe_sessions(title, value))
            workflow = (self._workflow_event(title, value)
                        if self.state.executions[title].dag else None)
            if workflow is not None:
                execution = self.state.executions[title]
                previous = (
                    set(execution.requests), set(execution.optional_requests),
                    dict(execution.request_versions),
                )
                self._events([self._dag_revision_record(title, workflow)])
                self._events(self._request_records(title, workflow, previous))
                effects.extend(reduce(self.state, workflow))
                self._events(self._task_records(title, value))
            self._execute(effects)
        from .project import set_state
        set_state(self.root, "supervisor_state", _state_dump(self.state))
        self._write_host_tasks()
        atomic_json(self.root / "supervisor-status.json", self._status())

    def run(self, *, once: bool = False, control_marker: Path | None = None,
            generation: int | None = None) -> None:
        while control_marker is None or control_mtime(control_marker) == generation:
            self.step()
            if once:
                return
            if control_marker is not None and control_mtime(control_marker) != generation:
                return
            time.sleep(self.poll_interval)


def parser(prog: str = "labflow supervisor") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Maintain the current project execution.")
    value.add_argument("--port", type=int, help="OpenCode port; required on first start")
    value.add_argument("--poll-interval", type=float, default=.25)
    value.add_argument("--once", action="store_true", help="observe and reconcile one snapshot")
    return value


def main(argv: list[str] | None = None, *, prog: str = "labflow supervisor") -> int:
    args = parser(prog).parse_args(argv)
    supervisor: Supervisor | None = None
    try:
        from .project import LAB_SCHEMA, load_execution, prepare_execution, project_home
        try:
            home, manifest, config = load_execution()
        except ControlError as exc:
            if exc.code != 75 or args.port is None:
                raise
            if isinstance(args.port, bool) or not 1 <= args.port <= 65535:
                raise ControlError("port must be from 1 through 65535", 64) from None
            project = project_home()
            lab_root = Path(tempfile.mkdtemp(prefix="labflow-", dir=tempfile.gettempdir())).resolve()
            atomic_json(lab_root / "config.json", {
                "schema": LAB_SCHEMA, "port": args.port, "root": str(lab_root),
            })
            home, manifest, config = prepare_execution(project, lab_root, args.port)
            print(
                f"Prepared {home}; start OpenCode on port {args.port}, then touch "
                f"{home / 'ctrl' / 'supervisor'} and {home / 'ctrl' / 'active'}",
                flush=True,
            )
        if args.port is not None and args.port != config["port"]:
            raise ControlError(
                f"execution is configured for port {config['port']}, not {args.port}", 64
            )
        if args.poll_interval <= 0 or args.poll_interval > 60:
            raise ControlError("poll interval must be greater than 0 and at most 60 seconds", 64)
        marker = home / "ctrl" / "supervisor"
        generation = control_mtime(marker)
        if generation is None:
            return 0
        client = Client(
            f"http://127.0.0.1:{config['port']}", str(manifest.root), timeout=.5,
        )
        while control_mtime(marker) == generation:
            try:
                client.health()
                break
            except ControlError as exc:
                if exc.code != 69:
                    raise
                time.sleep(.25)
        if control_mtime(marker) != generation:
            return 0
        with supervisor_lock(home):
            supervisor = Supervisor(home, config["port"], poll_interval=args.poll_interval)
            try:
                supervisor.run(
                    once=args.once, control_marker=marker, generation=generation,
                )
            finally:
                supervisor.close()
                supervisor = None
        return 0
    except (ControlError, RuntimeError, OSError, ValueError) as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return getattr(exc, "code", 70)
    except KeyboardInterrupt:
        return 130
    finally:
        if supervisor is not None:
            supervisor.close()


if __name__ == "__main__":
    raise SystemExit(main())
