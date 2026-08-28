from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import Manifest, sha256
from .state import atomic_json, atomic_write
from .task_cli import role_asset_permissions


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


def _dag_role_protocol(role: str) -> str:
    return (
        "\n\n# Labflow Supervisor 协议\n\n"
        "Supervisor 每次直接投递一个 Artifact 任务。一次只处理题面指定的任务，在角色权限"
        "允许的资产内完成交付。完成工作或确信无法继续后直接结束本次执行。Supervisor 负责"
        "校验资产、结算 Artifact，并在新任务可执行时再次投递。\n"
    )


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


def generate(manifest: Manifest, workspace: Path,
             runtime_root: Path | None = None) -> dict[str, str]:
    """Generate the complete OpenCode adapter from a runtime-neutral plan."""
    runtime_root = workspace if runtime_root is None else runtime_root
    agents = runtime_root / ".opencode" / "agents"
    agents.mkdir(parents=True, exist_ok=True)
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
    for name, role in manifest.roles.items():
        instructions = str(role["prompt"]) + _dag_role_protocol(name)
        assets = role_asset_permissions(manifest.workflow, name)
        permission = _role_permission(role, assets)
        permission["bash"]["labflow agent *"] = "deny"
        permission["bash"]["./labflow agent *"] = "deny"
        text = (_frontmatter(role["description"], "subagent", permission)
                + instructions.rstrip() + "\n")
        path = agents / f"{name}.md"
        atomic_write(path, text.encode(), 0o444)
        generated.append(path)
    return {str(path.relative_to(runtime_root)): sha256(path) for path in generated}
