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
    inputs = ", ".join(
        f"{item['name']} ({'本轮更新' if item['fresh'] else '可用'})"
        for item in task["inputs"] if item["fresh"] is not None
    ) or "none"
    assets = ", ".join(
        f"{item['path']} ({'本轮更新' if item['updated'] else '可用'})"
        for item in task["assets"]
    ) or "none"
    validation = f"\n交付校验要求：{error}\n" if error else ""
    return (
        f"请完成唯一任务 `{artifact['id']}`：{artifact['desc']}\n"
        f"要求：{artifact['instruction']}\n"
        f"输入：{inputs}\n需要读取的资产：{assets}\n"
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


def _benchmark_role_assets(manifest: Manifest, role: str) -> dict[str, list[str]]:
    execution = manifest.execution
    if role == execution["answerer"]:
        inputs = [asset["path"] for asset in execution["input"]]
        outputs = [asset["path"] for asset in execution["output"]]
        return {"read": [*inputs, *outputs], "write": outputs}
    return {
        "read": ["problem/", "ch/q.md", "ch/k.md", "ch/metadata.json", "ch/out/"],
        "write": ["ch/out/report.md"],
    }


def _benchmark_answerer_protocol(manifest: Manifest) -> str:
    output = manifest.execution["output"][0]["path"]
    return (
        "\n\n# Labflow Benchmark 交付协议\n\n"
        f"证据文件写入 `{output}`。成功证据使用 `ok-*`，失败证据使用 `err-*`；每题选择至多"
        "一类，也可以不写证据。`report.md` 由 Questioner 负责。每题开始时清理通道中的 "
        "`ok-*` 与 `err-*`。原始提问、必要追问和澄清使用对话管道；完成后最后一条消息说明"
        "本题完成。\n"
    )


def _benchmark_questioner_protocol(manifest: Manifest) -> str:
    answerer = manifest.execution["answerer"]
    return (
        "\n\n# Labflow Benchmark 提问协议\n\n"
        "Host 会一次性准备并触发整批题目。按 Host 给出的编号顺序执行 "
        "`labflow agent start-problem <id>`，再读取通道中的 `ch/q.md`、可选 `ch/k.md` 和只读 "
        "`ch/metadata.json`。本批开始时通过 task 工具创建唯一的 "
        f"`{answerer}` 子会话，所有题目持续复用它。把 q.md 原文逐字发送给 Answerer，保持题面"
        "完整。Answerer 追问时，仅依据当前 K 作最窄澄清，并保留 K 中与追问无关的信息。"
        "Answerer 完成后读取其可选证据，综合题面与对话写出必需、非空的 "
        "`ch/out/report.md`，然后执行 `labflow agent end-problem ok|error|cancel`。归档成功后继续"
        "下一题；全部题目完成后结束。\n"
    )


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
    primary = (manifest.execution["questioner"]
               if manifest.execution["kind"] == "benchmark-mode" else "coordinator")
    atomic_json(config, {
        "$schema": "https://opencode.ai/config.json",
        "default_agent": primary,
        "model": MODEL,
        "permission": "deny",
    })
    generated.append(config)
    runtime_manifest = runtime_root / "experiment.json"
    runtime_workflow = json.loads(json.dumps(manifest.workflow))
    if runtime_workflow is not None:
        for artifact in runtime_workflow["artifacts"].values():
            artifact.pop("owner", None)
    atomic_json(runtime_manifest, {
        "schema": "labflow.experiment-runtime/v1",
        "plan_id": manifest.plan_id,
        "workflow": runtime_workflow,
        "execution": manifest.execution,
    })
    generated.append(runtime_manifest)
    if manifest.execution["kind"] == "dag-mode":
        coordinator = agents / "coordinator.md"
        atomic_write(coordinator, _coordinator(manifest).encode(), 0o444)
        generated.append(coordinator)
    for name, role in manifest.roles.items():
        instructions = (manifest.root / role["instructions"]).read_text(encoding="utf-8")
        if (manifest.execution["kind"] == "benchmark-mode"
                and name == manifest.execution["answerer"]):
            instructions += _benchmark_answerer_protocol(manifest)
        if (manifest.execution["kind"] == "benchmark-mode"
                and name == manifest.execution["questioner"]):
            instructions += _benchmark_questioner_protocol(manifest)
        if manifest.execution["kind"] == "dag-mode":
            instructions += _dag_role_protocol(name)
        mode = ("primary" if manifest.execution["kind"] == "benchmark-mode"
                and name == manifest.execution["questioner"] else "subagent")
        assets = (role_asset_permissions(manifest.workflow, name)
                  if manifest.workflow is not None
                  else _benchmark_role_assets(manifest, name))
        task = ({"*": "deny", manifest.execution["answerer"]: "allow"}
                if manifest.execution["kind"] == "benchmark-mode"
                and name == manifest.execution["questioner"] else "deny")
        permission = _role_permission(role, assets, task=task)
        if manifest.execution["kind"] == "dag-mode":
            permission["bash"]["labflow agent *"] = "deny"
            permission["bash"]["./labflow agent *"] = "deny"
        if (manifest.execution["kind"] == "benchmark-mode"
                and name == manifest.execution["answerer"]):
            permission["edit"]["ch/out/report.md"] = "deny"
            permission["edit"]["**/ch/out/report.md"] = "deny"
        text = (_frontmatter(role["description"], mode, permission)
                + instructions.rstrip() + "\n")
        path = agents / f"{name}.md"
        atomic_write(path, text.encode(), 0o444)
        generated.append(path)
    return {str(path.relative_to(runtime_root)): sha256(path) for path in generated}
