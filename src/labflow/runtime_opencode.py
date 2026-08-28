from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .config import Manifest, sha256
from .state import atomic_json, atomic_write
from .task_cli import TaskError, artifact_asset_permissions


MODEL = "deepseek/deepseek-v4-flash"
ENVIRONMENT = {"OPENCODE_EXPERIMENTAL_OUTPUT_TOKEN_MAX": "128000"}
START_PROMPT = "请启动实验。"


def resume_prompt(role: str, artifact: dict[str, Any] | None = None,
                  task: dict[str, Any] | None = None, error: str | None = None) -> str:
    if artifact is None or task is None:
        return (
            f"继续完成 Supervisor 已经分配给 {role} 的唯一任务。完成工作或确信无法继续后直接"
            "结束本次执行。Supervisor 负责结算任务并投递后续工作。"
        )
    requires = ", ".join(
        f"{item['name']} ({'本轮更新' if item['fresh'] else '可用'})"
        for item in task["requires"] if item["fresh"] is not None
    ) or "none"
    inputs = ", ".join(
        f"{item['path']} ({'本轮更新' if item['updated'] else '可用'})"
        for item in task["inputs"]
    ) or "none"
    validation = f"\n交付校验要求：{error}\n" if error else ""
    return (
        f"请完成唯一任务 `{artifact['id']}`：{artifact['desc']}\n"
        f"要求：{artifact['instruction']}\n"
        f"依赖 Artifact：{requires}\n输入资产：{inputs}\n"
        f"{validation}在角色权限允许的资产内完成交付。完成工作或确信无法继续后直接结束"
        "本次执行。Supervisor 负责校验资产、结算 Artifact 并投递后续工作。"
    )


def _browse_paths(patterns: list[str]) -> list[str]:
    values: list[str] = []
    for pattern in patterns:
        base = pattern.split("/", 1)[0]
        for value in (base, pattern.removesuffix("/**").removesuffix("/*")):
            if value and value not in values:
                values.append(value)
    return values


def _rules(patterns: list[str], *, deny_manifest: bool = False) -> dict[str, str]:
    values = {"*": "deny"}
    if deny_manifest:
        values["labflow-plan.toml"] = "deny"
    values.update({pattern: "allow" for pattern in patterns})
    return values


def _path_rules(patterns: list[str], *, deny_manifest: bool = False) -> dict[str, str]:
    values = _rules(patterns, deny_manifest=deny_manifest)
    if deny_manifest:
        values["**/labflow-plan.toml"] = "deny"
    values.update({f"**/{pattern}": "allow" for pattern in patterns})
    values[".labflow-exec"] = "deny"
    values[".labflow-exec/**"] = "deny"
    values["**/.labflow-exec"] = "deny"
    values["**/.labflow-exec/**"] = "deny"
    return values


def _asset_patterns(paths: list[str]) -> list[str]:
    return [f"{path}**" if path.endswith("/") else path for path in paths]


def _role_permission(role: dict[str, Any], assets: dict[str, list[str]],
                     task: dict[str, str] | str = "deny") -> dict[str, Any]:
    read = _asset_patterns(assets["read"])
    write = _asset_patterns(assets["write"])
    return {
        "read": _path_rules(read, deny_manifest=True),
        "glob": _path_rules(read, deny_manifest=True),
        "grep": _path_rules(read, deny_manifest=True),
        "list": _path_rules(_browse_paths(read)),
        "edit": _path_rules(write, deny_manifest=True),
        "bash": _rules(role["commands"]),
        "task": task,
        "webfetch": "deny",
        "websearch": "deny",
        "external_directory": "deny",
    }


def _frontmatter(description: str, mode: str, permission: dict[str, Any]) -> str:
    return "\n".join([
        "---",
        f"description: {json.dumps(description, ensure_ascii=False)}",
        f"mode: {json.dumps(mode)}",
        f"model: {json.dumps(MODEL)}",
        f"permission: {json.dumps(permission, ensure_ascii=False, separators=(',', ':'))}",
        "---",
        "",
    ])


def _coordinator(manifest: Manifest) -> str:
    permission = {
        "read": "deny", "glob": "deny", "grep": "deny", "list": "deny",
        "edit": "deny", "bash": "deny", "task": "deny", "webfetch": "deny",
        "websearch": "deny", "external_directory": "deny",
    }
    body = (
        f"本会话是 Artifact DAG 的根会话。收到 `{START_PROMPT}` 时完成根会话初始化并结束"
        "当前 turn。Labflow Supervisor 负责创建角色 Session、投递任务和维持执行目标。"
    )
    return _frontmatter("作为 Supervisor 管理的 Artifact DAG 根会话。",
                        "primary", permission) + body + "\n"


def _task_commands(manifest: Manifest, artifact: dict[str, Any]) -> list[str]:
    commands: list[str] = list(artifact["commands"])
    for item in artifact["inputs"]:
        value = item["path"].rstrip("/")
        candidate = manifest.root / value
        if candidate.is_file() and candidate.stat().st_mode & 0o111:
            for command in (value, f"./{value}"):
                pattern = f"{command} *"
                if pattern not in commands:
                    commands.append(pattern)
    return commands


def dag_hash(manifest: Manifest) -> str:
    encoded = json.dumps(
        {"roles": manifest.roles, "workflow": manifest.workflow},
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _role_content(
    manifest: Manifest, name: str,
    assets: dict[str, list[str]], commands: list[str],
) -> bytes:
    role = manifest.roles[name]
    combined_assets = {
        "read": list(dict.fromkeys([
            *assets["read"], *role["read"], *role["write"],
        ])),
        "write": list(dict.fromkeys([*assets["write"], *role["write"]])),
    }
    combined_commands = list(dict.fromkeys([*commands, *role["commands"]]))
    permission = _role_permission(
        {**role, "commands": combined_commands}, combined_assets,
    )
    permission["bash"]["labflow agent *"] = "deny"
    permission["bash"]["./labflow agent *"] = "deny"
    return (_frontmatter(role["description"], "subagent", permission)
            + str(role["prompt"]).rstrip() + "\n").encode()


def _same_file(source: Path, target: Path) -> bool:
    try:
        source_stat = source.stat()
        target_stat = target.stat()
    except FileNotFoundError:
        return False
    return ((source_stat.st_dev, source_stat.st_ino)
            == (target_stat.st_dev, target_stat.st_ino))


def _write_snapshot(path: Path, content: bytes) -> Path:
    try:
        if path.read_bytes() == content:
            return path
    except FileNotFoundError:
        pass
    atomic_write(path, content, 0o444)
    return path


def _activate_role(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if _same_file(source, target):
        return target
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    os.close(fd)
    os.unlink(temporary)
    try:
        os.link(source, temporary)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return target


def _role_generation(manifest: Manifest, execution_home: Path) -> Path:
    return execution_home / "roles" / dag_hash(manifest)


def reset_role(manifest: Manifest, name: str, execution_home: Path) -> None:
    if name not in manifest.roles:
        return
    generation = _role_generation(manifest, execution_home)
    _activate_role(
        generation / f".idle.{name}.md",
        execution_home / "ws" / ".opencode" / "agents" / f"{name}.md",
    )


def configure_task_role(
    manifest: Manifest, role: str, artifact_name: str, execution_home: Path,
) -> None:
    artifact = manifest.workflow["artifacts"].get(artifact_name)
    if artifact is None or artifact["owner"] != role or role not in manifest.roles:
        raise TaskError(f"artifact is not owned by {role}: {artifact_name}", 64)
    _activate_role(
        _role_generation(manifest, execution_home) / f"{artifact_name}.md",
        execution_home / "ws" / ".opencode" / "agents" / f"{role}.md",
    )


def generate(manifest: Manifest, execution_home: Path) -> dict[str, str]:
    """Generate the complete OpenCode adapter from a runtime-neutral plan."""
    runtime_root = execution_home / "ws"
    agents = runtime_root / ".opencode" / "agents"
    agents.mkdir(parents=True, exist_ok=True)
    generation = _role_generation(manifest, execution_home)
    generation.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    config = runtime_root / "opencode.json"
    atomic_json(config, {
        "$schema": "https://opencode.ai/config.json",
        "default_agent": "coordinator",
        "model": MODEL,
        "permission": "deny",
    })
    generated.append(config)
    coordinator = agents / "coordinator.md"
    atomic_write(coordinator, _coordinator(manifest).encode(), 0o444)
    generated.append(coordinator)
    for name in manifest.roles:
        idle = generation / f".idle.{name}.md"
        _write_snapshot(
            idle, _role_content(manifest, name, {"read": [], "write": []}, []),
        )
        snapshots = [idle]
        for artifact_name, artifact in manifest.workflow["artifacts"].items():
            if artifact["owner"] != name:
                continue
            snapshot = generation / f"{artifact_name}.md"
            _write_snapshot(snapshot, _role_content(
                manifest, name,
                artifact_asset_permissions(manifest.workflow, artifact_name),
                _task_commands(manifest, artifact),
            ))
            snapshots.append(snapshot)
        target = agents / f"{name}.md"
        if not any(_same_file(snapshot, target) for snapshot in snapshots):
            _activate_role(idle, target)
        generated.append(target)
    expected = set(generated)
    for path in agents.glob("*.md"):
        if path not in expected and path.is_file() and not path.is_symlink():
            path.unlink()
    return {str(path.relative_to(execution_home)): sha256(path) for path in generated}
