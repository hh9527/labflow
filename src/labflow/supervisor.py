from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from .client import Client
from .config import ControlError, repository_root
from .events import pending_optional_requests, pending_requests
from .runtime_opencode import task_prompt
from .state import atomic_json, load_lab_config, load_state, validate_title
from .task_cli import (
    TaskError, assign_task, clear_session_qualifications, task_records, workflow_status,
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
    execution = value.get("execution", {})
    if execution.get("kind") == "benchmark-mode":
        benchmark = value.get("benchmark", {})
        for problem in benchmark.get("problems", ()) if isinstance(benchmark, dict) else ():
            if not isinstance(problem, dict):
                continue
            problem_id = problem.get("problem") or problem.get("id")
            started = _milliseconds(problem.get("started_at_ns"))
            ended = _milliseconds(problem.get("end_at_ns"))
            sessions = {
                problem.get("questioner_session_id"), problem.get("answerer_session_id"),
            }
            if (isinstance(problem_id, str) and session_id in sessions
                    and started is not None and started <= completed
                    and (ended is None or ended >= created)):
                return "problem", problem_id
        metadata = Path(value["workspace"]) / "ch" / "metadata.json"
        try:
            active = json.loads(metadata.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        problem_id = active.get("id") if isinstance(active, dict) else None
        started = _milliseconds(active.get("started_at_ns")) if isinstance(active, dict) else None
        if isinstance(problem_id, str) and (started is None or started <= completed):
            return "problem", problem_id
        return None

    if execution.get("kind") != "dag-mode":
        return None
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
    path = root / ".supervisor.lock"
    with path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ControlError("another Supervisor already owns this laboratory", 75) from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


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
        "dispatch_task", execution.title, role, session.title, session.backend_id,
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
        return _reconcile_execution(state, execution)
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
        session.error = event.data.get("error")
        return _reconcile_role(state, execution, role)
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
        if event.data.get("effect_kind") == "dispatch_task":
            session.status = "prompted" if event.data.get("delivered") else "idle"
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
                       root_role: str, value: dict[str, Any]) -> list[dict[str, str]]:
    sessions = _session_tree(client, root_id, root_title, root_role)
    by_id = {session["id"]: session for session in sessions}
    execution = value.get("execution", {})
    if execution.get("kind") != "benchmark-mode":
        return sessions

    referenced: dict[str, str] = {}
    benchmark = value.get("benchmark", {})
    for item in benchmark.get("sessions", ()) if isinstance(benchmark, dict) else ():
        session_id = item.get("id") if isinstance(item, dict) else None
        role = item.get("agent") if isinstance(item, dict) else None
        if isinstance(session_id, str) and isinstance(role, str):
            referenced[session_id] = role
    for record in benchmark.get("problems", ()) if isinstance(benchmark, dict) else ():
        if not isinstance(record, dict):
            continue
        for key, role in (
            ("questioner_session_id", execution.get("questioner")),
            ("answerer_session_id", execution.get("answerer")),
        ):
            session_id = record.get(key)
            if isinstance(session_id, str) and isinstance(role, str):
                referenced[session_id] = role

    catalog = {
        item["id"]: item for item in client.sessions()
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    for session_id, role in referenced.items():
        if session_id in by_id:
            continue
        item = catalog.get(session_id, {})
        session = {
            "id": session_id,
            "title": str(item.get("title") or session_id),
            "role": str(item.get("agent") or role),
        }
        sessions.append(session)
        by_id[session_id] = session
    return sessions


class Supervisor:
    def __init__(self, lab_root: Path, port: int, *, poll_interval: float = .25):
        self.root = lab_root.resolve()
        self.server_url = f"http://127.0.0.1:{port}"
        self.poll_interval = poll_interval
        self.state = SupervisorState()
        self.writer = TimelineWriter(self.root / "timeline.sqlite3")

    def close(self) -> None:
        self.writer.close()

    def _desired(self) -> dict[str, Path]:
        root = self.root / "supervisor"
        if not root.is_dir():
            return {}
        result = {}
        for path in root.iterdir():
            if path.is_dir() and not path.is_symlink():
                try:
                    result[validate_title(path.name)] = path
                except ControlError:
                    continue
        return result

    def _execution_event(self, title: str, desired: Path) -> tuple[LifecycleEvent, dict[str, Any]]:
        control = self.root / "control" / title
        value = load_state(control)
        workflow = value.get("workflow")
        roles = tuple(workflow.get("roles", ())) if isinstance(workflow, dict) else ()
        active = value.get("phase") in {"ready", "active", "idle"}
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
        current = ({path.name: path.stat().st_mtime_ns for path in directory.iterdir()
                    if path.is_file() and not path.is_symlink()}
                   if directory.is_dir() else {})
        previous = self.state.executions[title].artifacts
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
        execution = value.get("execution", {})
        root_role = (str(execution.get("questioner"))
                     if execution.get("kind") == "benchmark-mode" else "coordinator")
        client = Client(self.server_url, value["workspace"], root_id)
        statuses = client.statuses()
        sessions = _observed_sessions(client, root_id, title, root_role, value)
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
            self.writer.submit([{
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
                records = closed_message_events(
                    title, session["title"], role, message,
                    task=_message_task(value, session_id, role, message),
                    dag_revision=self.state.executions[title].dag_revision,
                )
                if records:
                    self.writer.submit(records)
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
                    effects.extend(reduce(self.state, LifecycleEvent(
                        "session_observed", title, {
                            "role": role, "title": session["title"],
                            "backend_id": session["id"],
                            "status": statuses.get(
                                session["id"], {"type": "idle"}
                            ).get("type", "idle"),
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
                elif effect.kind == "dispatch_task":
                    if not backend_id or not effect.artifact or execution.workflow is None:
                        raise ControlError("task dispatch effect is incomplete")
                    task = assign_task(
                        Path(execution.workspace), execution.workflow,
                        effect.role, effect.artifact,
                    )
                    delivered = task is not None
                    if task is not None:
                        client.prompt_session(
                            backend_id, task_prompt(effect.role, task), agent=effect.role,
                        )
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

    def step(self) -> None:
        self.writer.check()
        desired = self._desired()
        for title in set(self.state.executions) - set(desired):
            reduce(self.state, LifecycleEvent("execution_deleted", title))
        for title, path in desired.items():
            event, value = self._execution_event(title, path)
            effects = reduce(self.state, event)
            artifact_events = self._artifact_events(title, path)
            self.writer.submit(self._artifact_records(title, artifact_events))
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
                self.writer.submit([self._dag_revision_record(title, workflow)])
                self.writer.submit(self._request_records(title, workflow, previous))
                effects.extend(reduce(self.state, workflow))
                self.writer.submit(self._task_records(title, value))
            self._execute(effects)
        atomic_json(self.root / "supervisor-status.json", self._status())

    def run(self, *, once: bool = False) -> None:
        while True:
            self.step()
            if once:
                return
            time.sleep(self.poll_interval)


def parser(prog: str = "labflow supervisor") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog=prog, description="Maintain laboratory Sessions and Timeline.")
    value.add_argument("lab_name")
    value.add_argument("--poll-interval", type=float, default=.25)
    value.add_argument("--once", action="store_true", help="observe and reconcile one snapshot")
    return value


def main(argv: list[str] | None = None, *, prog: str = "labflow supervisor") -> int:
    args = parser(prog).parse_args(argv)
    supervisor: Supervisor | None = None
    try:
        repo = repository_root(Path.cwd())
        config = load_lab_config(repo, args.lab_name)
        if args.poll_interval <= 0 or args.poll_interval > 60:
            raise ControlError("poll interval must be greater than 0 and at most 60 seconds", 64)
        Client(f"http://127.0.0.1:{config['port']}", config["root"]).health()
        with supervisor_lock(Path(config["root"])):
            supervisor = Supervisor(Path(config["root"]), config["port"],
                                    poll_interval=args.poll_interval)
            try:
                supervisor.run(once=args.once)
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
