from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import Manifest, sha256
from .state import atomic_json, atomic_write
from .task_cli import role_asset_permissions


MODEL = "deepseek/deepseek-v4-flash"
ENVIRONMENT = {"OPENCODE_EXPERIMENTAL_OUTPUT_TOKEN_MAX": "128000"}
START_PROMPT = "请启动实验角色循环。"


def resume_prompt(role: str) -> str:
    return (
        f"Host 正在恢复 {role} 的长期任务循环。立即执行 labflow agent pull {role}；"
        "领取任务后完成唯一 artifact、submit，然后继续 pull。没有工作时保持阻塞等待，"
        "每次 pull 最多等待 60 秒；返回 null 时立即再次 pull。领取任务后先查看 inputs 中的 "
        "fresh 和 assets 中的 updated，重新读取发生变化的资产。不得结束循环或返回最终答复。"
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
        values["experiment.json"] = "deny"
    values.update({pattern: "allow" for pattern in patterns})
    return values


def _path_rules(patterns: list[str], *, deny_manifest: bool = False) -> dict[str, str]:
    values = _rules(patterns, deny_manifest=deny_manifest)
    if deny_manifest:
        values["**/experiment.json"] = "deny"
    values.update({f"**/{pattern}": "allow" for pattern in patterns})
    return values


def _asset_patterns(paths: list[str]) -> list[str]:
    return [f"{path}**" if path.endswith("/") else path for path in paths]


def _role_permission(role: dict[str, Any], assets: dict[str, list[str]]) -> dict[str, Any]:
    read = _asset_patterns(assets["read"])
    write = _asset_patterns(assets["write"])
    return {
        "read": _path_rules(read, deny_manifest=True),
        "glob": _path_rules(read, deny_manifest=True),
        "grep": _path_rules(read, deny_manifest=True),
        "list": _path_rules(_browse_paths(read)),
        "edit": _path_rules(write, deny_manifest=True),
        "bash": _rules(role["commands"]),
        "task": "deny",
        "webfetch": "deny",
        "websearch": "deny",
        "external_directory": "deny",
    }


def _benchmark_role_assets(manifest: Manifest, role: str) -> dict[str, list[str]]:
    execution = manifest.execution
    if role == execution["answerer"]:
        inputs = [asset["path"] for asset in execution["input"]]
        outputs = [asset["path"] for asset in execution["output"]]
        return {"read": [*inputs, *outputs], "write": outputs}
    return {"read": [], "write": []}


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
    roles = list(manifest.roles)
    task = {"*": "deny", **{role: "allow" for role in roles}}
    permission = {
        "read": "deny", "glob": "deny", "grep": "deny", "list": "deny",
        "edit": "deny", "bash": "deny", "task": task, "webfetch": "deny",
        "websearch": "deny", "external_directory": "deny",
    }
    labels = "、".join(role.upper() for role in roles)
    launches = "、".join(roles)
    body = (
        f"收到 `{START_PROMPT}` 时，同时启动 {labels} 各一次。向每个角色只发送：\n\n"
        "`按照你的角色协议启动 labflow agent 任务循环。`\n\n"
        "全部启动调用完成后立即结束，不观察文件、不判断流程、不创建 artifact。\n\n"
        f"收到 `恢复角色 <role>` 时，确认 role 属于 {launches}，只重新启动该角色一次，"
        "并发送同一条启动消息；不要启动其他角色。"
    )
    return _frontmatter("启动和恢复由 artifact DAG 驱动的长期角色。", "primary", permission) + body + "\n"


def generate(manifest: Manifest, workspace: Path) -> dict[str, str]:
    """Generate the complete OpenCode adapter from a runtime-neutral plan."""
    agents = workspace / ".opencode" / "agents"
    agents.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    config = workspace / "opencode.json"
    primary = (manifest.execution["answerer"]
               if manifest.execution["kind"] == "benchmark-mode" else "coordinator")
    atomic_json(config, {
        "$schema": "https://opencode.ai/config.json",
        "default_agent": primary,
        "model": MODEL,
        "permission": "deny",
    })
    generated.append(config)
    runtime_manifest = workspace / "experiment.json"
    atomic_json(runtime_manifest, {
        "schema": "labflow.experiment-runtime/v1",
        "plan_id": manifest.plan_id,
        "workflow": manifest.workflow,
        "execution": manifest.execution,
    })
    generated.append(runtime_manifest)
    if manifest.execution["kind"] == "dag-mode":
        coordinator = agents / "coordinator.md"
        atomic_write(coordinator, _coordinator(manifest).encode(), 0o444)
        generated.append(coordinator)
    for name, role in manifest.roles.items():
        instructions = (manifest.root / role["instructions"]).read_text(encoding="utf-8")
        mode = "primary" if manifest.execution["kind"] == "benchmark-mode" else "subagent"
        assets = (role_asset_permissions(manifest.workflow, name)
                  if manifest.workflow is not None
                  else _benchmark_role_assets(manifest, name))
        text = (_frontmatter(role["description"], mode, _role_permission(role, assets))
                + instructions.rstrip() + "\n")
        path = agents / f"{name}.md"
        atomic_write(path, text.encode(), 0o444)
        generated.append(path)
    return {str(path.relative_to(workspace)): sha256(path) for path in generated}
