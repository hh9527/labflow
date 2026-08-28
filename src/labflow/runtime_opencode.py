from __future__ import annotations

import hashlib
import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any

from .config import Manifest, sha256
from .state import atomic_json, atomic_write


MODEL = "deepseek/deepseek-v4-flash"
ENVIRONMENT = {"OPENCODE_EXPERIMENTAL_OUTPUT_TOKEN_MAX": "128000"}


def resume_prompt(role: str, artifact: dict[str, Any] | None = None,
                  task: dict[str, Any] | None = None, error: str | None = None,
                  *, include_checks: bool = False) -> str:
    if artifact is None or task is None:
        return (
            f"继续完成 Supervisor 已经分配给 {role} 的唯一任务。完成工作或确信无法继续后直接"
            "结束本次执行。Supervisor 负责结算任务并投递后续工作。"
        )
    target = task["target"]
    requires = "\n".join(
        f"- `{item['name']}`（{('尚不存在' if item['fresh'] is None else '已刷新' if item['fresh'] else '未改变')}）"
        for item in task["requires"]
    ) or "- 无"
    files = [
        f"- `{target['goal']}`（{'已更新' if target['goal_updated'] else '未改变'}）",
        *(
            f"- `{item['path']}`（{'已更新' if item['updated'] else '未改变'}）"
            for item in task["inputs"] if item["path"] != target["goal"]
        ),
    ]
    file_list = "\n".join(files)
    validation = f"\n\n## 上次交付校验\n\n{error}" if error else ""
    checks = ""
    if error and include_checks:
        labels = {"ready": "已就绪", "missing": "缺失", "invalid": "类型错误"}
        check_list = "\n".join(
            f"- `{item['path']}`（{labels.get(item['status'], item['status'])}）"
            for item in task.get("checks", ())
        ) or "- 无"
        checks = f"\n\n## 机械检查项\n\n{check_list}"
    return (
        f"# 任务：`{target['name']}`\n\n"
        "## 目标\n\n"
        f"按照 `{target['goal']}` 的要求完成任务，并简单回复：\n\n"
        "- 如果完成，则回复必须以“已完成任务。”开头\n"
        "- 如果无法完成任务，则回复必须以“无法完成任务。”开头\n\n"
        "## 前序任务输出\n\n"
        f"{requires}\n\n"
        "## 你需要的详细文件清单\n\n"
        f"{file_list}"
        f"{validation}"
        f"{checks}\n"
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


def _observer() -> str:
    labflow = shlex.join([os.path.abspath(sys.executable), "-m", "labflow.cli"])
    permission = {
        "read": "deny", "glob": "deny", "grep": "deny", "list": "deny",
        "edit": "deny", "bash": _rules([
            f"{labflow} host status", f"{labflow} query *", f"{labflow} query-om *",
        ]), "task": "deny", "webfetch": "deny",
        "websearch": "deny", "external_directory": "deny",
    }
    body = (
        "本会话是 Labflow 实验的只读数据观察员。根据用户的问题，使用允许的 Host 命令"
        "查询当前状态或执行只读 SQL，并解释统计结果；不要参与调度或修改项目。\n\n"
        "`labflow host status` 返回当前调度快照。`labflow query '<SQL>'` 查询两个"
        " SQLite 数据库：主库是 `events.sqlite`，附加库名为 `states`。查询最多运行 2 秒"
        "并返回 1000 行。\n\n"
        "主库 `timeline` 的核心列为：`id`, `execution`, `session`, `turn`, `role`, "
        "`task_kind`, `task_id`, `artifact`, `dag_revision`, `type`, `at`, `duration`, "
        "`tokens`, `action`, `success`, `command`, `exit_code`, `summary`, `input_tokens`, "
        "`output_tokens`, `reasoning_tokens`, `cache_read_tokens`, `cache_write_tokens`, "
        "`payload_json`。`at` 和 `duration` 单位均为毫秒。粗粒度类型包括 "
        "`task_started`, `task_completed`, `artifact_refreshed`, `host_request_opened`, "
        "`host_request_resolved`；思考、动作和回复分别是 `thinking`, `action`, `reply`。"
        "`action_paths(event_id, path)` 记录写入路径。\n\n"
        "状态库包含 `states.state(key, value)` 和 "
        "`states.task_records(kind, identity, payload)`；其中 value 和 payload 是 JSON。"
        "任务轮数通常统计 `task_started`，Token 取 input/output/reasoning 三列之和，"
        "最长思考取 `thinking.duration` 最大值，Host 批准次数统计 Host Artifact 的 "
        "`artifact_refreshed`。查询前先明确用户需要当前快照还是历史累计。若环境启用了 "
        "OM-Labflow，也可用 `labflow query-om <file.json>` 将领域请求降低并执行；该命令"
        "缺少 `TELORA_BIN` 或 `OM_LABFLOW_PATH` 时不可用。"
    )
    return _frontmatter("观察 Labflow 当前任务状态。",
                        "primary", permission) + body + "\n"


def dag_hash(manifest: Manifest) -> str:
    encoded = json.dumps(
        {"roles": manifest.roles, "workflow": manifest.workflow},
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _role_content(manifest: Manifest, name: str) -> bytes:
    role = manifest.roles[name]
    assets = {
        "read": list(dict.fromkeys([*role["read"], *role["write"]])),
        "write": role["write"],
    }
    permission = _role_permission(role, assets)
    permission["bash"]["labflow agent *"] = "deny"
    permission["bash"]["./labflow agent *"] = "deny"
    return (_frontmatter(role["description"], "subagent", permission)
            + str(role["prompt"]).rstrip() + "\n").encode()


def _write_snapshot(path: Path, content: bytes) -> Path:
    try:
        if path.read_bytes() == content:
            return path
    except FileNotFoundError:
        pass
    atomic_write(path, content, 0o444)
    return path


def generate(manifest: Manifest, execution_home: Path) -> dict[str, str]:
    """Generate the complete OpenCode adapter from a runtime-neutral plan."""
    runtime_root = execution_home / "ws"
    agents = runtime_root / ".opencode" / "agents"
    agents.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    config = runtime_root / "opencode.json"
    atomic_json(config, {
        "$schema": "https://opencode.ai/config.json",
        "default_agent": "lab-ob",
        "model": MODEL,
        "permission": "deny",
    })
    generated.append(config)
    observer = agents / "lab-ob.md"
    atomic_write(observer, _observer().encode(), 0o444)
    generated.append(observer)
    report = runtime_root / ".opencode" / "commands" / "ob.md"
    if report.exists():
        report.unlink()
    for name in manifest.roles:
        target = agents / f"{name}.md"
        _write_snapshot(target, _role_content(manifest, name))
        generated.append(target)
    expected = set(generated)
    for path in agents.glob("*.md"):
        if path not in expected and path.is_file() and not path.is_symlink():
            path.unlink()
    return {str(path.relative_to(execution_home)): sha256(path) for path in generated}
