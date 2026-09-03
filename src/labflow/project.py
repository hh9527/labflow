from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import tomllib
from pathlib import Path
from typing import Any

from .benchmark import load_bundle
from .config import ControlError, Manifest
from .state import atomic_json, atomic_write
from .task_cli import TaskError, validate_role_permissions, validate_workflow


PLAN_NAME = "labflow-plan.toml"
EXEC_NAME = ".labflow-exec"
EXEC_SCHEMA = "labflow.project-execution/v1"
LAB_SCHEMA = "labflow.lab/v2"
ROLE = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")


def project_home(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / PLAN_NAME).is_file():
            return candidate
    raise ControlError(f"cannot find {PLAN_NAME} from {current}", 66)


def execution_id(project: Path) -> str:
    root = project.resolve(strict=True)
    if not root.name or root.name in (".", ".."):
        raise ControlError(f"invalid project directory: {root}", 64)
    digest = hashlib.sha256(os.fsencode(str(root.parent))).hexdigest()[:16]
    return f"{root.name}.{digest}"


def exec_home(project: Path) -> Path:
    return project.resolve() / EXEC_NAME


def ignore_execution(root: Path) -> None:
    repository = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], cwd=root,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    if repository.returncode != 0:
        return
    repository_root = Path(repository.stdout.strip()).resolve()
    try:
        relative = root.relative_to(repository_root)
    except ValueError:
        return
    location = subprocess.run(
        ["git", "rev-parse", "--git-path", "info/exclude"], cwd=root,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    if location.returncode != 0:
        return
    exclude = Path(location.stdout.strip())
    if not exclude.is_absolute():
        exclude = root / exclude
    prefix = "" if str(relative) == "." else f"{relative.as_posix()}/"
    pattern = f"/{prefix}{EXEC_NAME}/"
    try:
        content = exclude.read_text(encoding="utf-8")
    except FileNotFoundError:
        content = ""
    except OSError as exc:
        raise ControlError(f"cannot read Git exclude file: {exc}", 73) from None
    if pattern in content.splitlines():
        return
    separator = "" if not content or content.endswith("\n") else "\n"
    try:
        atomic_write(exclude, f"{content}{separator}{pattern}\n".encode())
    except OSError as exc:
        raise ControlError(f"cannot ignore {EXEC_NAME}: {exc}", 73) from None


def _goal(path: Path, artifact: str) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ControlError(f"missing goal for {artifact}: {path}", 66) from None
    except OSError as exc:
        raise ControlError(f"cannot read goal for {artifact}: {exc}", 66) from None
    if not text.strip():
        raise ControlError(f"empty goal for {artifact}: {path}")
    heading = next((line[2:].strip() for line in text.splitlines()
                    if line.startswith("# ") and line[2:].strip()), artifact)
    return heading


def _strings(value: Any, where: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ControlError(f"{where} must be a string array")
    return list(value)


def _commands(value: Any, where: str) -> list[str]:
    commands = _strings(value, where)
    if any(command.strip() != command or "\n" in command or "\r" in command
           for command in commands):
        raise ControlError(f"{where} must contain command patterns")
    return list(dict.fromkeys(commands))


def _project_path(root: Path, value: str, where: str) -> Path:
    normalized = value.rstrip("/")
    candidate = root / normalized
    resolved = candidate.resolve()
    control = root / EXEC_NAME
    if (value == EXEC_NAME or value.startswith(f"{EXEC_NAME}/")
            or not resolved.is_relative_to(root)
            or resolved.is_relative_to(control)):
        raise ControlError(f"unsafe {where} path: {value}")
    return candidate


def load_plan(path: Path | None = None) -> Manifest:
    plan_path = (path or (project_home() / PLAN_NAME)).resolve()
    root = plan_path.parent
    if plan_path.name != PLAN_NAME:
        raise ControlError(f"plan must be named {PLAN_NAME}", 64)
    try:
        data = tomllib.loads(plan_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError(f"missing plan: {plan_path}", 66) from None
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ControlError(f"invalid {PLAN_NAME}: {exc}") from None
    if not isinstance(data, dict) or "artifacts" not in data or set(data) - {"artifacts", "roles"}:
        raise ControlError(f"{PLAN_NAME} may only contain artifacts and roles")
    raw_artifacts = data.get("artifacts")
    if not isinstance(raw_artifacts, dict) or not raw_artifacts:
        raise ControlError("artifacts must be a non-empty table")
    raw_roles = data.get("roles", {})
    if not isinstance(raw_roles, dict):
        raise ControlError("roles must be a table")

    roles: list[str] = []
    artifact_rows: dict[str, dict[str, Any]] = {}
    goals: dict[str, tuple[str, str]] = {}
    for name, raw in raw_artifacts.items():
        if not isinstance(raw, dict):
            raise ControlError(f"artifact {name} must be a table")
        unknown = set(raw) - {"goal", "requires", "inputs", "assets", "check"}
        if unknown:
            raise ControlError(
                f"unknown artifact {name} key(s): {', '.join(sorted(unknown))}"
            )
        goal = raw.get("goal")
        if goal is not None:
            if not isinstance(goal, str) or not goal:
                raise ControlError(f"artifact {name} goal must be a path")
            role = name.rsplit(".", 1)[-1] if "." in name else ""
            if not ROLE.fullmatch(role):
                raise ControlError(f"role-owned artifact has no valid role suffix: {name}")
            if role not in roles:
                roles.append(role)
            goal_path = _project_path(root, goal, "goal")
            goals[name] = (goal, _goal(goal_path, name))
        artifact_rows[name] = raw
    if not roles:
        raise ControlError("plan must contain at least one role-owned artifact with a goal")

    artifacts: dict[str, dict[str, Any]] = {}
    for name, raw in artifact_rows.items():
        goal, description = goals.get(name, (None, name))
        assets = _strings(raw.get("assets"), f"artifact {name} assets")
        checks = _strings(raw.get("check"), f"artifact {name} check")
        inputs = (_strings(raw.get("inputs"), f"artifact {name} inputs")
                  if "inputs" in raw else None)
        for value in (*(inputs or []), *assets, *checks):
            _project_path(root, value, f"artifact {name}")
        artifacts[name] = {
            "desc": description,
            "requires": _strings(raw.get("requires"), f"artifact {name} requires"),
            "assets": assets,
            "check": checks,
        }
        if inputs is not None:
            artifacts[name]["inputs"] = inputs
        if goal is not None:
            artifacts[name]["goal"] = goal

    try:
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1",
            "roles": roles,
            "artifacts": artifacts,
        })
    except TaskError as exc:
        raise ControlError(str(exc), exc.code) from None

    role_configs: dict[str, dict[str, Any]] = {}
    unknown_roles = set(raw_roles) - set(roles)
    if unknown_roles:
        raise ControlError(f"unknown role(s): {', '.join(sorted(unknown_roles))}")
    for role in roles:
        raw_role = raw_roles.get(role)
        if not isinstance(raw_role, dict):
            raise ControlError(f"role {role} must be a table")
        unknown = set(raw_role) - {"read", "write", "commands"}
        if unknown:
            raise ControlError(f"unknown role {role} key(s): {', '.join(sorted(unknown))}")
        missing = {"read", "write", "commands"} - set(raw_role)
        if missing:
            raise ControlError(
                f"role {role} must explicitly define: {', '.join(sorted(missing))}"
            )
        read = _strings(raw_role.get("read"), f"role {role} read")
        write = _strings(raw_role.get("write"), f"role {role} write")
        for value in (*read, *write):
            _project_path(root, value, f"role {role}")
        role_configs[role] = {
            "description": f"Labflow 角色 {role}。",
            "prompt": f"你是 Labflow 角色 {role}。",
            "read": read,
            "write": write,
            "commands": _commands(raw_role.get("commands"), f"role {role} commands"),
        }
    try:
        validate_role_permissions(workflow, role_configs)
    except TaskError as exc:
        raise ControlError(str(exc), exc.code) from None

    for artifact in workflow["artifacts"].values():
        if artifact.get("benchmark"):
            load_bundle(root / artifact["inputs"][0]["path"].rstrip("/"))

    identifier = execution_id(root)
    return Manifest(identifier, root, role_configs, workflow, {"kind": "dag-mode"})


def _initialize_states(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA busy_timeout = 5000;
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS task_records (
                kind TEXT NOT NULL CHECK (kind IN ('active', 'history')),
                identity TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (kind, identity)
            );
        """)
        connection.commit()
    finally:
        connection.close()


def _initialize_execution_state(home: Path) -> None:
    path = home / "states.sqlite"
    _initialize_states(path)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "INSERT OR IGNORE INTO state(key, value) VALUES ('root_session_id', 'null')"
        )
        connection.execute(
            "INSERT OR IGNORE INTO state(key, value) VALUES "
            "('active_control', '{\"applied_mtime_ns\":null,\"error\":null,"
            "\"observed_mtime_ns\":null}')"
        )
        connection.commit()
    finally:
        connection.close()


def set_state(home: Path, key: str, value: Any) -> None:
    connection = sqlite3.connect(home / "states.sqlite")
    try:
        connection.execute(
            "INSERT INTO state(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, json.dumps(value, ensure_ascii=False)),
        )
        connection.commit()
    finally:
        connection.close()


def get_state(home: Path, key: str, default: Any = None) -> Any:
    connection = sqlite3.connect(home / "states.sqlite")
    try:
        row = connection.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
    finally:
        connection.close()
    return default if row is None else json.loads(row[0])


def activate_plan(home: Path, manifest: Manifest) -> None:
    runtime_workflow = json.loads(json.dumps(manifest.workflow))
    for artifact in runtime_workflow["artifacts"].values():
        artifact.pop("owner", None)
        for field in ("inputs", "assets", "check"):
            artifact[field] = [item["path"] for item in artifact[field]]
    from .runtime_opencode import generate
    generate(manifest, home)
    atomic_json(home / "runtime.json", {
        "schema": "labflow.project-runtime/v1",
        "plan_id": manifest.plan_id,
        "roles": manifest.roles,
        "workflow": runtime_workflow,
        "execution": manifest.execution,
    })


def prepare_execution(project: Path, lab_root: Path, port: int) -> tuple[Path, Manifest, dict[str, Any]]:
    root = project.resolve(strict=True)
    manifest = load_plan(root / PLAN_NAME)
    ignore_execution(root)
    home = exec_home(root)
    if home.exists() and not home.is_dir():
        raise ControlError(f"execution home is not a directory: {home}")
    home.mkdir(exist_ok=True)
    (home / "artifacts").mkdir(exist_ok=True)
    runtime = home / "ws"
    runtime.mkdir(exist_ok=True)
    (runtime / ".opencode" / "agents").mkdir(parents=True, exist_ok=True)
    _initialize_execution_state(home)
    config = {
        "schema": EXEC_SCHEMA,
        "execution_id": manifest.plan_id,
        "project_home": str(root),
        "plan_path": str(root / PLAN_NAME),
        "lab_root": str(lab_root.resolve(strict=True)),
        "port": port,
    }
    existing_path = home / "config.json"
    if existing_path.is_file():
        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ControlError(f"invalid execution config: {exc}") from None
        for key in ("execution_id", "project_home"):
            if existing.get(key) != config[key]:
                raise ControlError(f"execution configuration disagrees on {key}")
    atomic_json(existing_path, config)
    activate_plan(home, manifest)
    return home, manifest, config


def load_execution(project: Path | None = None) -> tuple[Path, Manifest, dict[str, Any]]:
    root = project_home(project)
    home = exec_home(root)
    try:
        config = json.loads((home / "config.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError(f"project is not hosted: {root}", 75) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid execution config: {exc}") from None
    if (not isinstance(config, dict) or config.get("schema") != EXEC_SCHEMA
            or config.get("project_home") != str(root)
            or config.get("execution_id") != execution_id(root)):
        raise ControlError("invalid project execution configuration")
    try:
        runtime = json.loads((home / "runtime.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError(f"missing activated project runtime: {home}", 75) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid activated project runtime: {exc}") from None
    if (not isinstance(runtime, dict)
            or runtime.get("schema") != "labflow.project-runtime/v1"
            or runtime.get("plan_id") != config["execution_id"]):
        raise ControlError("invalid activated project runtime")
    try:
        workflow = validate_workflow(runtime.get("workflow"))
    except TaskError as exc:
        raise ControlError(f"invalid activated workflow: {exc}") from None
    roles = runtime.get("roles")
    execution = runtime.get("execution")
    if (not isinstance(roles, dict) or set(roles) != set(workflow["roles"])
            or not all(isinstance(value, dict) for value in roles.values())
            or not isinstance(execution, dict)):
        raise ControlError("invalid activated project runtime")
    _initialize_execution_state(home)
    manifest = Manifest(config["execution_id"], root, roles, workflow, execution)
    return home, manifest, config
