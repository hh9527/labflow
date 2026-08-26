from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from .client import Client
from .config import ControlError, Manifest, load_manifest, repository_root, sha256, validate_identifier
from .context import Context
from .external import resolve_cli, resolve_command
from .observe import latest_assistant, normalized, text_parts
from .permissions import preflight_permissions
from .runtime_opencode import ENVIRONMENT, generate as generate_opencode_adapter
from .state import (
    SCHEMA, atomic_json, atomic_write, bind_plan, execution_root, load_state, locked, now,
    save_state, validate_session_name,
)
from .task_cli import TaskError, evaluate, refresh_artifact, restore_artifacts


def opencode_environment(state: dict[str, Any]) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(ENVIRONMENT)
    return environment


def lab_sessions(client: Client, lab_root: str | Path) -> list[dict[str, Any]]:
    root = Path(lab_root).resolve()
    if not isinstance(client.workspace, str):
        return client.sessions()
    workspaces = {Path(client.workspace).resolve()}
    connection = root / "connection"
    if connection.is_dir():
        workspaces.add(connection)
    executions = root / "executions"
    if executions.is_dir():
        workspaces.update(path for path in executions.glob("*/*/runtime/ws") if path.is_dir())
    records: dict[str, dict[str, Any]] = {}
    for workspace in sorted(workspaces):
        source = client if workspace == Path(client.workspace).resolve() else Client(
            client.url, str(workspace), timeout=client.timeout
        )
        for session in source.sessions():
            session_id = session.get("id") if isinstance(session, dict) else None
            if isinstance(session_id, str):
                records[session_id] = session
    return list(records.values())


def next_session_title(client: Client, base: str, lab_root: str | Path) -> str:
    if not base or any(character.isspace() for character in base):
        raise ControlError(f"session title base must not contain whitespace: {base!r}", 64)
    pattern = re.compile(rf"^{re.escape(base)}/([1-9][0-9]*)$")
    generations = []
    for session in lab_sessions(client, lab_root):
        title = session.get("title") if isinstance(session, dict) else None
        match = pattern.fullmatch(title) if isinstance(title, str) else None
        if match:
            generations.append(int(match.group(1)))
    execution_generations = Path(lab_root).resolve() / "executions" / base
    if execution_generations.is_dir():
        generations.extend(
            int(path.name) for path in execution_generations.iterdir()
            if path.is_dir() and path.name.isdigit() and int(path.name) > 0
        )
    return f"{base}/{max(generations, default=0) + 1}"


def probe_opencode_connection(lab_name: str, port: int, workspace: Path) -> dict[str, Any]:
    """Exercise the lab daemon without preparing a real execution."""
    validate_identifier(lab_name, "lab-name")
    client = Client(f"http://127.0.0.1:{port}", str(workspace), timeout=0.5)
    health = client.health()
    title = next_session_title(client, "connect", workspace.parent)
    session = client.create_session(title)
    session_id = session.get("id") if isinstance(session, dict) else None
    if not isinstance(session_id, str) or not session_id.startswith("ses_"):
        raise ControlError("opencode connection test returned an invalid session identity")
    return {
        "health": bool(isinstance(health, dict) and health.get("healthy")),
        "session_id": session_id,
        "title": title,
    }


def git_metadata(repo: Path) -> tuple[str, bool]:
    git = resolve_cli("git")
    revision = subprocess.run([*git, "rev-parse", "HEAD"], cwd=repo, text=True, check=True, stdout=subprocess.PIPE).stdout.strip()
    dirty = bool(subprocess.run([*git, "status", "--porcelain"], cwd=repo, text=True, check=True, stdout=subprocess.PIPE).stdout)
    return revision, dirty


def _copy_file(source: Path, destination: Path, mode: int) -> None:
    if not source.is_file() or source.is_symlink(): raise ControlError(f"missing or unsafe input: {source}", 66)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination); destination.chmod(mode)


def plan_git_metadata(repo: Path, manifest: Manifest) -> tuple[str, str]:
    git = resolve_cli("git")
    relative = manifest.root.relative_to(repo)
    tracked = subprocess.run(
        [*git, "ls-files", "--error-unmatch", str(relative / manifest.manifest_name)],
        cwd=repo, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if tracked.returncode:
        raise ControlError(f"experiment plan is not tracked by the project repository: {relative}", 66)
    dirty = subprocess.run(
        [*git, "status", "--porcelain", "--", str(relative)], cwd=repo, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if dirty.returncode or dirty.stdout:
        raise ControlError("experiment plan must be clean and committed in the project repository", 65)
    revision = subprocess.run([*git, "rev-parse", "HEAD"], cwd=repo, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if revision.returncode:
        raise ControlError("experiment plan has no committed revision", 66)
    remote = subprocess.run([*git, "remote", "get-url", "origin"], cwd=repo, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    source = remote.stdout.strip() if not remote.returncode else str(repo.resolve())
    return revision.stdout.strip(), source


def _copy_plan_workspace(manifest: Manifest, workspace: Path) -> None:
    workspace.mkdir(parents=True, exist_ok=False)
    for relative in manifest.workspace:
        source = manifest.root / relative
        destination = workspace / relative
        if source.is_dir():
            for child in source.rglob("*"):
                if child.is_symlink():
                    raise ControlError(f"unsafe symlink in workspace input: {child}", 66)
            shutil.copytree(source, destination, symlinks=False)
        else:
            _copy_file(source, destination, source.stat().st_mode & 0o7777)


def verify_prepared(manifest: Manifest, state: dict[str, Any]) -> None:
    workspace = Path(state["workspace"])
    if state.get("workflow") != manifest.workflow:
        raise ControlError("workflow changed since preparation")
    if sha256(manifest.root / manifest.manifest_name) != state["input_hashes"].get(manifest.manifest_name):
        raise ControlError(f"{manifest.manifest_name} changed since preparation")
    for relative, expected in state.get("adapter_hashes", {}).items():
        path = workspace / relative
        if not path.is_file() or sha256(path) != expected:
            raise ControlError(f"generated runtime adapter changed after preparation: {relative}")
    for item in manifest.assets:
        name = str(item["path"]); destination = workspace / name
        if not destination.is_file() or destination.is_symlink() or sha256(destination) != state["asset_hashes"].get(name):
            raise ControlError(f"workspace asset changed since preparation: {name}")


def prepare(plan_id: str, session_name: str, port: int | None, assets: dict[str, str] | None = None,
            from_session: str | None = None, *, lab_name: str,
            lab_root: str) -> tuple[Path, dict[str, Any], bool]:
    repo = repository_root(); validate_identifier(plan_id, "plan-id")
    validate_identifier(lab_name, "lab-name"); validate_session_name(session_name)
    if port is not None and not 1 <= port <= 65535: raise ControlError("port must be from 1 through 65535", 64)
    selected_lab_root = Path(lab_root).resolve()
    if not selected_lab_root.is_dir():
        raise ControlError(f"lab root is unavailable: {selected_lab_root}", 75)
    manifest = load_manifest(repo, plan_id); root = bind_plan(selected_lab_root, plan_id, session_name)
    if from_session is not None:
        validate_session_name(from_session)
        if from_session == session_name:
            raise ControlError("an execution cannot inherit from itself", 64)
    with locked(root):
        if (root / "state.json").exists():
            state = load_state(root)
            if state["plan_id"] != plan_id: raise ControlError("execution plan mismatch")
            if state["phase"] in ("finished", "retired", "failed"): raise ControlError(f"session {session_name} is {state['phase']}")
            if port is not None and int(state["server_url"].rsplit(":", 1)[1]) != port: raise ControlError("execution already uses another port", 64)
            if not Path(state["workspace"]).is_dir(): raise ControlError("recorded workspace is missing", 66)
            verify_prepared(manifest, state); return root, state, False
        port = port or 4096
        revision, dirty = git_metadata(repo); plan_revision, plan_source = plan_git_metadata(repo, manifest)
        run_root = root / "runtime"; workspace = run_root / "ws"
        run_root.mkdir()
        state: dict[str, Any] = {"schema": SCHEMA, "plan_id": plan_id, "session_name": session_name, "run_id": uuid.uuid4().hex,
            "phase": "preparing", "workspace": str(workspace), "run_root": str(run_root), "session_id": None,
            "server_url": f"http://127.0.0.1:{port}", "repository_revision": revision, "repository_dirty": dirty,
            "lab_root": str(selected_lab_root),
            "lab_name": lab_name,
            "plan_revision": plan_revision, "plan_source": plan_source,
            "opencode_environment": ENVIRONMENT,
            "workflow": manifest.workflow,
            "execution": manifest.execution,
            "input_hashes": {}, "asset_hashes": {}, "next_round": 0, "active_round": None,
            "asset_overrides": dict(assets or {}),
            "from_session": from_session,
            "created_at": now(),
            "start_requested_at": now(),
            "started_at": None, "finished_at": None}
        save_state(root, state)
        try:
            _copy_plan_workspace(manifest, workspace)
            state["adapter_hashes"] = generate_opencode_adapter(manifest, workspace)
            for item in manifest.assets:
                name = str(item["path"]); source = Path((assets or {}).get(name, str(item["source"])))
                if not source.is_absolute(): source = repo / source
                if not source.is_file() and item.get("build"):
                    result = subprocess.run(resolve_command(item["build"], repo), cwd=repo)
                    if result.returncode: raise ControlError(f"asset build failed: {name}", 70)
                target = workspace / name; _copy_file(source.resolve(), target, int(str(item.get("mode", "0555")), 8)); state["asset_hashes"][name] = sha256(target)
            state["permission_preflight"] = preflight_permissions(manifest, workspace)
            state["reporting"] = manifest.reporting
            state["metrics"] = manifest.metrics
            state["input_hashes"][manifest.manifest_name] = sha256(manifest.root / manifest.manifest_name)
            if from_session is not None:
                state["inheritance"] = _inherit_execution(selected_lab_root, from_session, plan_id, workspace,
                                                            manifest.workflow)
            save_state(root, state)
        except Exception:
            state["phase"] = "failed"; save_state(root, state); raise
    return root, state, True


def _inherit_execution(lab_root: Path, source_id: str, plan_id: str, workspace: Path,
                       workflow: dict[str, Any] | None) -> dict[str, Any]:
    if workflow is None:
        raise ControlError("target experiment has no artifact workflow", 64)
    source_root = execution_root(lab_root, source_id)
    source_state = load_state(source_root)
    if source_state["plan_id"] != plan_id:
        raise ControlError("source execution uses another plan", 64)
    source_workspace_value = source_state.get("workspace")
    source_workspace = (Path(source_workspace_value)
                        if isinstance(source_workspace_value, str) and source_workspace_value
                        else source_root / "result" / "workspace")
    if not source_workspace.is_dir():
        source_workspace = source_root / "result" / "workspace"
    if not source_workspace.is_dir():
        raise ControlError(f"source execution workspace is unavailable: {source_id}", 66)
    source_workflow = source_state.get("workflow")
    if not isinstance(source_workflow, dict):
        raise ControlError("source execution has no artifact workflow", 64)
    try:
        source_status = evaluate(source_workspace, source_workflow)["artifacts"]
    except TaskError as exc:
        raise ControlError(f"cannot inspect source artifacts: {exc}", exc.code) from None

    candidates = {
        name for name, artifact in workflow["artifacts"].items()
        if (name in source_status and source_status[name]["current"]
            and _inheritance_compatible(source_workflow["artifacts"].get(name), artifact,
                                        source_status[name], source_status))
    }
    inherited: list[str] = []
    inherited_set: set[str] = set()
    copied: set[str] = set()
    while candidates - inherited_set:
        progressed = False
        for name, artifact in workflow["artifacts"].items():
            if name not in candidates or name in inherited_set:
                continue
            dependencies_ready = all(
                reference["optional"] and not source_status[reference["id"]]["stamp_mtime_ns"]
                or reference["id"] in inherited_set
                for reference in artifact["input"]
            )
            if not dependencies_ready:
                continue
            for asset in artifact["assets"]:
                if asset["level"] == 0:
                    continue
                relative = Path(asset["path"].rstrip("/"))
                source = source_workspace / relative
                target = workspace / relative
                if source.is_dir() and asset["path"].endswith("/"):
                    shutil.copytree(source, target, dirs_exist_ok=True)
                    copied.add(f"{relative.as_posix()}/")
                elif source.is_file() and not source.is_symlink():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
                    copied.add(relative.as_posix())
            inherited.append(name)
            inherited_set.add(name)
            progressed = True
        if not progressed:
            break
    try:
        restored = restore_artifacts(workspace, workflow, inherited)
    except TaskError as exc:
        raise ControlError(f"cannot restore inherited artifacts: {exc}", exc.code) from None
    return {
        "source_session": source_id,
        "source_plan_revision": source_state.get("plan_revision"),
        "artifacts": [item["artifact"] for item in restored],
        "files": sorted(copied),
        "inherited_at": now(),
    }


def _inheritance_compatible(old: Any, new: dict[str, Any], old_status: dict[str, Any],
                            source_status: dict[str, dict[str, Any]]) -> bool:
    return old == new

def refresh_workflow_artifact(context: Context, artifact: str, reason: str, *,
                              once: str | None = None, force: bool = False) -> dict[str, Any]:
    workflow = context.state.get("workflow")
    if not workflow:
        raise ControlError("execution plan has no workflow", 64)
    with locked(context.root):
        state = load_state(context.root)
        if once and state.get(once):
            return dict(state[once])
        try:
            result = refresh_artifact(Path(state["workspace"]), workflow, artifact, force=force)
        except TaskError as exc:
            raise ControlError(str(exc), exc.code) from None
        number = int(state.get("next_artifact_event", 0))
        event = {**result, "number": number, "reason": reason, "refreshed_at": now()}
        directory = context.root / "artifact-events"
        directory.mkdir(exist_ok=True)
        atomic_json(directory / f"{number:03d}-{artifact.replace('/', '-')}.json", event)
        state["next_artifact_event"] = number + 1
        if once:
            state[once] = event
        save_state(context.root, state)
        context.state = state
        return event


def create_execution_session(root: Path, state: dict[str, Any], title: str) -> dict[str, Any]:
    if state.get("session_id"): return state
    client = Client(state["server_url"], state["workspace"])
    client.health()
    response = client.create_session(title)
    session_id = response.get("id") if isinstance(response, dict) else None
    if not isinstance(session_id, str) or not session_id.startswith("ses_"): raise ControlError("opencode returned an invalid session identity")
    with locked(root):
        current = load_state(root); current["session_id"] = session_id
        current["session_base"] = title.rsplit("/", 1)[0]; current["session_title"] = title
        current["phase"] = "ready"; save_state(root, current); state = current
    return state


def live_boundary(context: Context, allow_length: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    client = context.client(); client.health(); status = client.status()
    if status.get("type", "idle") != "idle": raise ControlError(f"session is {status.get('type')}; operation requires idle", 75)
    messages = client.messages(); latest = latest_assistant(messages)
    if not latest: raise ControlError("session has no assistant completion")
    info = latest.get("info", {}); finish = info.get("finish")
    if info.get("time", {}).get("completed") is None or finish not in (("stop", "length") if allow_length else ("stop",)):
        raise ControlError(f"latest assistant message is not a completed {'stop/length' if allow_length else 'stop'}")
    return messages, latest


def _remote_user_with_text(messages: list[dict[str, Any]], text: str, after_message_id: str | None = None) -> dict[str, Any] | None:
    if after_message_id:
        indexes = [i for i, message in enumerate(messages) if message.get("info", {}).get("id") == after_message_id]
        messages = messages[indexes[-1] + 1:] if indexes else messages
    matches = [m for m in messages if m.get("info", {}).get("role") == "user" and text_parts(m) == [text]]
    return matches[-1] if matches else None


def send_round(context: Context, kind: str, text: str, *, require_empty: bool = False,
               require_finish: str = "stop", source: dict[str, Any] | None = None,
               agent: str | None = None) -> dict[str, Any]:
    digest = hashlib.sha256(text.encode()).hexdigest()
    # Observation is part of every mutation; callers do not need a separate
    # status command to close the preceding round locally.
    if context.state.get("active_round"):
        context.state, _ = reconcile(context)
    with locked(context.root):
        state = load_state(context.root); client = context.client(); client.health(); status = client.status()
        if status.get("type", "idle") != "idle": raise ControlError(f"session is {status.get('type')}; refusing to send", 75)
        messages = client.messages()
        active = state.get("active_round")
        if active:
            record_path = context.root / "rounds" / active["file"]
            record = json.loads(record_path.read_text())
            if record["digest"] != digest: raise ControlError("another round is already active")
            existing = _remote_user_with_text(messages, text, record.get("preceding_message_id"))
            if existing:
                record["user_message_id"] = existing.get("info", {}).get("id"); record["delivered_at"] = now(); atomic_json(record_path, record)
                return record
            raise ControlError("round delivery is pending reconciliation; refusing duplicate send", 75)
        if require_empty:
            if messages: raise ControlError("prepared session is not empty; refusing to start")
        else:
            _, latest = live_boundary(context, allow_length=require_finish == "length")
            if latest.get("info", {}).get("finish") != require_finish:
                raise ControlError(f"latest assistant message did not finish with {require_finish}")
        number = int(state["next_round"]); filename = f"{number:03d}-{kind}.json"
        preceding_message_id = messages[-1].get("info", {}).get("id") if messages else None
        record = {"schema": "labflow.round/v1", "number": number, "kind": kind, "digest": digest,
                  "text": text, "intent_at": now(), "delivered_at": None, "user_message_id": None,
                  "assistant_message_id": None, "completed_at": None, "finish": None, "source": source,
                  "preceding_message_id": preceding_message_id}
        (context.root / "rounds").mkdir(exist_ok=True); atomic_json(context.root / "rounds" / filename, record)
        state["active_round"] = {"number": number, "file": filename, "digest": digest}; state["phase"] = "active"
        state["next_round"] = number + 1
        if kind == "initial" and not state["started_at"]: state["started_at"] = now()
        save_state(context.root, state)
        client.prompt(text, agent=agent)
        existing = None
        for _ in range(10):
            messages = client.messages(); existing = _remote_user_with_text(messages, text, preceding_message_id)
            if existing: break
            time.sleep(.05)
        if existing:
            record["user_message_id"] = existing.get("info", {}).get("id"); record["delivered_at"] = now(); atomic_json(context.root / "rounds" / filename, record)
        return record


def reconcile(context: Context) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    messages = context.client().messages()
    with locked(context.root):
        state = load_state(context.root); active = state.get("active_round")
        if not active: return state, messages
        path = context.root / "rounds" / active["file"]; record = json.loads(path.read_text())
        user = _remote_user_with_text(messages, record["text"], record.get("preceding_message_id"))
        if user:
            record["user_message_id"] = user.get("info", {}).get("id"); record["delivered_at"] = record.get("delivered_at") or now()
            user_index = messages.index(user)
            assistants = [m for m in messages[user_index + 1:] if m.get("info", {}).get("role") == "assistant"]
            if assistants:
                last = assistants[-1]; info = last.get("info", {})
                if info.get("time", {}).get("completed") is not None and info.get("finish") in ("stop", "length"):
                    record["assistant_message_id"] = info.get("id"); record["completed_at"] = info.get("time", {}).get("completed"); record["finish"] = info.get("finish")
                    state["phase"] = "idle"; state["active_round"] = None
            atomic_json(path, record); save_state(context.root, state)
        return state, messages


def run_validation(context: Context) -> list[dict[str, Any]]:
    directory = context.root / "result" / "validation"; directory.mkdir(parents=True, exist_ok=True)
    results = []
    for item in context.manifest.validation:
        workspace = Path(context.state["workspace"]); cwd = workspace / item.get("cwd", "")
        started = now(); command = resolve_command(item["command"], cwd); result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        record = {"name": item["name"], "command": item["command"], "cwd": item.get("cwd", ""), "started_at": started, "finished_at": now(), "exit": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
        atomic_json(directory / f"{item['name']}.json", record); results.append(record)
    return results


def copy_archive(context: Context, destination: Path, *, include_process: bool = False) -> None:
    workspace = Path(context.state["workspace"]).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        workflow = context.state.get("workflow")
        minimum_level = 1 if include_process else 2
        selected: dict[str, int] = {}
        if isinstance(workflow, dict):
            status = evaluate(workspace, workflow)["artifacts"]
            for name, artifact in workflow["artifacts"].items():
                if not status[name]["current"]:
                    continue
                for asset in artifact["assets"]:
                    selected[asset["path"]] = asset["level"]
        for name, level in selected.items():
            if level < minimum_level:
                continue
            relative = Path(name.rstrip("/"))
            source = workspace / relative
            if source.is_symlink() or not source.resolve().is_relative_to(workspace):
                raise ControlError(f"unsafe archive asset: {name}")
            target = staging / relative
            if source.is_dir():
                for child in source.rglob("*"):
                    resolved = child.resolve()
                    if (child.is_symlink() and not resolved.is_file()) or not resolved.is_relative_to(workspace):
                        raise ControlError(f"unsafe symlink in archive asset: {child.relative_to(workspace)}")
                shutil.copytree(source, target, symlinks=False)
            elif source.is_file(): target.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(source, target)
            else: raise ControlError(f"missing archive asset: {name}")
        if destination.exists(): shutil.rmtree(destination)
        os.replace(staging, destination)
    finally:
        if staging.exists(): shutil.rmtree(staging)


def export_session(context: Context, session_id: str) -> Any:
    error = ""
    for attempt in range(3):
        with tempfile.TemporaryFile() as output:
            result = subprocess.run(
                [*resolve_cli("opencode"), "export", session_id, "--pure"], cwd=context.state["workspace"],
                stdout=output, stderr=subprocess.PIPE,
            )
            output.seek(0); exported = output.read()
        if result.returncode:
            error = f"opencode session export failed: {result.stderr.decode(errors='replace').strip()}"
        else:
            try:
                return json.loads(exported)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                error = f"opencode export returned malformed JSON: {exc}"
        if attempt < 2: time.sleep(0.25)
    raise ControlError(error, 70)


def finish(context: Context) -> dict[str, Any]:
    state, messages = reconcile(context)
    if state["active_round"]: raise ControlError("execution still has an active round", 75)
    live_boundary(context)
    with locked(context.root):
        state = load_state(context.root); state["phase"] = "finishing"; save_state(context.root, state)
    validation = run_validation(context)
    if any(v["exit"] and next((x.get("required", True) for x in context.manifest.validation if x["name"] == v["name"]), True) for v in validation):
        with locked(context.root): state = load_state(context.root); state["phase"] = "failed"; save_state(context.root, state)
        raise ControlError("required validation failed", 1)
    try:
        result = context.root / "result"; copy_archive(context, result / "workspace")
        raw_session = export_session(context, state["session_id"])
        atomic_json(result / "session.json", raw_session)
        children = context.client().children()
        child_exports = []
        child_dir = result / "children"; child_dir.mkdir(exist_ok=True)
        for child in children:
            child_id = child.get("id")
            if not isinstance(child_id, str): continue
            value = export_session(context, child_id); atomic_json(child_dir / f"{child_id}.json", value)
            atomic_json(child_dir / f"{child_id}.messages.json", context.client().session_messages(child_id))
            child_exports.append({"session_id": child_id, "title": child.get("title")})
        atomic_json(result / "children.json", child_exports)
        atomic_json(result / "messages.json", messages)
        final_state = dict(state); final_state["phase"] = "finished"; final_state["finished_at"] = now()
        document = normalized(final_state, messages, context.client().status(), context.rounds(), context.manifest.observe, validation)
        atomic_json(result / "query.json", document)
        summary = document["summary"]
        atomic_write(result / "RUNLOG.md", ("# Run log\n\n```json\n" + json.dumps(summary, indent=2) + "\n```\n").encode())
        atomic_write(result / "SUMMARY.md", f"# {state['session_name']} summary\n\nExecution data was frozen at {now()}.\n".encode())
    except Exception:
        with locked(context.root):
            current = load_state(context.root); current["phase"] = "idle"; save_state(context.root, current)
        raise
    with locked(context.root):
        state = load_state(context.root); state["phase"] = "finished"; state["finished_at"] = final_state["finished_at"]; save_state(context.root, state)
    return document


def safe_cleanup(state: dict[str, Any]) -> None:
    run_root = Path(state["run_root"]); workspace = Path(state["workspace"])
    lab_root = Path(state["lab_root"]).resolve()
    execution = execution_root(lab_root, state["session_name"])
    if (run_root != execution / "runtime" or workspace != run_root / "ws" or run_root.is_symlink()
            or not run_root.resolve().is_relative_to(lab_root)):
        raise ControlError("refusing unsafe temporary cleanup")
    if run_root.exists(): shutil.rmtree(run_root)
