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


def task_prompt(role: str, task: dict[str, Any]) -> str:
    target = task["target"]
    freshness = {True: "本轮已更新", False: "沿用前一轮版本", None: "可选输入尚未提供"}
    inputs = "\n".join(
        f"- `{item['name']}`：{freshness[item['fresh']]}"
        for item in task["inputs"]
    ) or "- 无直接输入"
    assets = "\n".join(
        f"- `{item['path']}`：{'需要重新读取' if item['updated'] else '未发生变化'}"
        for item in task["assets"]
    ) or "- 无输入资产"
    return (
        "# Labflow 任务\n\n"
        f"目标：完成 artifact `{target['name']}`。\n\n"
        f"任务要求：{target['instruction']}\n\n"
        f"输入状态：\n{inputs}\n\n"
        f"资产状态：\n{assets}\n\n"
        "只完成这一项任务。完成后必须执行：\n\n"
        f"`labflow agent submit {role} {target['name']}`\n\n"
        "提交成功后结束当前 turn，不要领取或等待下一项任务。后续任务会由 Supervisor 主动投递。"
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
        f"证据文件只写入 `{output}`。成功证据写 `ok-*`，失败证据写 `err-*`。两类证据不得"
        "同时存在，也都可以不存在。"
        "`report.md` 由 Questioner 编写，你不得创建或修改。每题开始清理上一题的 `ok-*` 与 "
        "`err-*`。原始提问、必要追问和澄清走对话管道；完成后最后一条消息只说明本题完成。\n"
    )


def _benchmark_questioner_protocol(manifest: Manifest) -> str:
    answerer = manifest.execution["answerer"]
    return (
        "\n\n# Labflow Benchmark 提问协议\n\n"
        "Host 会一次性准备并触发整批题目。按 Host 给出的编号顺序执行 "
        "`labflow agent start-problem <id>`，再读取通道中的 `ch/q.md`、可选 `ch/k.md` 和只读 "
        "`ch/metadata.json`。本批开始时通过 task 工具创建唯一的 "
        f"`{answerer}` 子会话，所有题目持续复用它。必须把 q.md 原文逐字发送给 Answerer，"
        "不得概括、转述、改写或补充。Answerer 追问时，只依据当前 K 作最窄澄清，不得主动泄漏 "
        "K、提示解法或判断正确性。Answerer 完成后读取其可选证据，综合题面与对话写出必需、"
        "非空的 `ch/out/report.md`，然后执行 `labflow agent end-problem ok|error|cancel`。只有归档成功后"
        "才能继续下一题；全部题目完成后才结束。\n"
    )


def _dag_role_protocol(role: str) -> str:
    return (
        "\n\n# Labflow Supervisor 协议\n\n"
        f"Supervisor 会主动投递一项完整任务，其中包含目标、要求、输入变化和提交命令。"
        f"每次只完成被投递的唯一任务，并使用 `labflow agent submit {role} <artifact>` 提交。"
        "提交成功后自然结束当前 turn，不要自行领取、轮询或等待其他任务。Supervisor 会在"
        "下一项任务可执行时再次唤醒当前 Session。\n"
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
        f"收到 `{START_PROMPT}` 时立即结束当前 turn。角色 Session 的创建、恢复和任务投递"
        "全部由 Labflow Supervisor 负责；不要启动 sub-agent，不观察文件，不判断流程，不创建 "
        "artifact。"
    )
    return _frontmatter("作为 Supervisor 管理的 DAG 根会话。", "primary", permission) + body + "\n"


def generate(manifest: Manifest, workspace: Path) -> dict[str, str]:
    """Generate the complete OpenCode adapter from a runtime-neutral plan."""
    agents = workspace / ".opencode" / "agents"
    agents.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    config = workspace / "opencode.json"
    primary = (manifest.execution["questioner"]
               if manifest.execution["kind"] == "benchmark-mode" else "coordinator")
    atomic_json(config, {
        "$schema": "https://opencode.ai/config.json",
        "default_agent": primary,
        "model": MODEL,
        "permission": "deny",
    })
    generated.append(config)
    runtime_manifest = workspace / "experiment.json"
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
        if (manifest.execution["kind"] == "benchmark-mode"
                and name == manifest.execution["answerer"]):
            permission["edit"]["ch/out/report.md"] = "deny"
            permission["edit"]["**/ch/out/report.md"] = "deny"
        text = (_frontmatter(role["description"], mode, permission)
                + instructions.rstrip() + "\n")
        path = agents / f"{name}.md"
        atomic_write(path, text.encode(), 0o444)
        generated.append(path)
    return {str(path.relative_to(workspace)): sha256(path) for path in generated}
