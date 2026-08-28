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

from .config import ControlError, Manifest
from .state import atomic_json, atomic_write
from .task_cli import TaskError, validate_workflow


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


def _ignore_execution(root: Path) -> None:
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


def _goal(path: Path, artifact: str) -> tuple[str, str]:
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
    return heading, text.strip()


def _strings(value: Any, where: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ControlError(f"{where} must be a string array")
    return list(value)


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
    if not isinstance(data, dict) or set(data) != {"artifacts"}:
        raise ControlError(f"{PLAN_NAME} may only contain artifacts")
    raw_artifacts = data.get("artifacts")
    if not isinstance(raw_artifacts, dict) or not raw_artifacts:
        raise ControlError("artifacts must be a non-empty table")

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
            goals[name] = _goal(goal_path, name)
        artifact_rows[name] = raw
    if not roles:
        raise ControlError("plan must contain at least one role-owned artifact with a goal")

    artifacts: dict[str, dict[str, Any]] = {}
    for name, raw in artifact_rows.items():
        description, instruction = goals.get(name, (name, None))
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
        if instruction is not None:
            artifacts[name]["instruction"] = instruction

    try:
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1",
            "roles": roles,
            "artifacts": artifacts,
        })
    except TaskError as exc:
        raise ControlError(str(exc), exc.code) from None

    role_configs: dict[str, dict[str, Any]] = {}
    for role in roles:
        commands: list[str] = []
        for artifact in workflow["artifacts"].values():
            if artifact["owner"] != role:
                continue
            for item in artifact["inputs"]:
                value = item["path"].rstrip("/")
                candidate = root / value
                if candidate.is_file() and os.access(candidate, os.X_OK):
                    for command in (value, f"./{value}"):
                        pattern = f"{command} *"
                        if pattern not in commands:
                            commands.append(pattern)
        role_configs[role] = {
            "description": f"完成分配给 {role} 的 Artifact 任务。",
            "prompt": f"你是 Labflow 角色 {role}。只处理 Supervisor 当前分配的唯一任务。",
            "commands": commands,
            "preflight": [],
        }

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


def prepare_execution(project: Path, lab_root: Path, port: int) -> tuple[Path, Manifest, dict[str, Any]]:
    root = project.resolve(strict=True)
    manifest = load_plan(root / PLAN_NAME)
    _ignore_execution(root)
    home = exec_home(root)
    if home.exists() and not home.is_dir():
        raise ControlError(f"execution home is not a directory: {home}")
    home.mkdir(exist_ok=True)
    (home / "ctrl").mkdir(exist_ok=True)
    (home / "artifacts").mkdir(exist_ok=True)
    runtime = home / "ws"
    runtime.mkdir(exist_ok=True)
    (runtime / ".opencode" / "agents").mkdir(parents=True, exist_ok=True)
    _initialize_states(home / "states.sqlite")
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
    runtime_workflow = json.loads(json.dumps(manifest.workflow))
    for artifact in runtime_workflow["artifacts"].values():
        artifact.pop("owner", None)
    atomic_json(home / "runtime.json", {
        "schema": "labflow.project-runtime/v1",
        "plan_id": manifest.plan_id,
        "workflow": runtime_workflow,
        "execution": manifest.execution,
    })
    from .runtime_opencode import generate
    generate(manifest, root, runtime)
    connection = sqlite3.connect(home / "states.sqlite")
    try:
        connection.execute(
            "INSERT OR IGNORE INTO state(key, value) VALUES ('root_session_id', 'null')"
        )
        connection.commit()
    finally:
        connection.close()
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
    return home, load_plan(root / PLAN_NAME), config
