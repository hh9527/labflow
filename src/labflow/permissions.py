from __future__ import annotations

import fnmatch
import json
import shlex
from pathlib import Path
from typing import Any

from .config import ControlError, Manifest


def _frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n") or "\n---\n" not in text[4:]:
        raise ControlError(f"agent file has no frontmatter: {path}")
    header = text[4:].split("\n---\n", 1)[0]
    values: dict[str, Any] = {}
    for line in header.splitlines():
        key, separator, raw = line.partition(":")
        if not separator:
            raise ControlError(f"invalid agent frontmatter line: {path}: {line!r}")
        raw = raw.strip()
        try:
            values[key.strip()] = json.loads(raw)
        except json.JSONDecodeError:
            values[key.strip()] = raw.strip('"')
    return values


def _reject_ask(value: Any, where: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            _reject_ask(child, f"{where}.{key}")
    elif value == "ask":
        raise ControlError(f"interactive permission remains at {where}")
    elif isinstance(value, str) and value not in ("allow", "deny"):
        raise ControlError(f"invalid permission decision at {where}: {value!r}")


def _decision(rules: Any, command: str, default: str) -> str:
    if isinstance(rules, str):
        return rules
    decision = default
    if isinstance(rules, dict):
        for pattern, value in rules.items():
            if fnmatch.fnmatchcase(command, pattern):
                decision = value
    return decision


def _preflight_family(pattern: str) -> str | None:
    try:
        tokens = shlex.split(pattern)
    except ValueError:
        return None
    if len(tokens) >= 2 and tokens[0].startswith("./bin/"):
        return " ".join(tokens[:2])
    if len(tokens) >= 3 and tokens[:2] == ["labflow", "agent"]:
        return " ".join(tokens[:3])
    return None


def preflight_permissions(manifest: Manifest, runtime_root: Path) -> dict[str, list[dict[str, str]]]:
    config = json.loads((runtime_root / "opencode.json").read_text(encoding="utf-8"))
    default = config.get("permission", "ask")
    _reject_ask(default, "opencode.permission")
    if not isinstance(default, str):
        raise ControlError("opencode.permission must be a default allow or deny decision")
    agents = runtime_root / ".opencode" / "agents"
    results: dict[str, list[dict[str, str]]] = {}
    for role, commands in manifest.permission_preflight.items():
        path = agents / f"{role}.md"
        try:
            permission = _frontmatter(path).get("permission", default)
        except FileNotFoundError:
            raise ControlError(f"missing permission-preflight role: {role}", 66) from None
        _reject_ask(permission, f"agent.{role}.permission")
        bash = permission.get("bash", default) if isinstance(permission, dict) else permission
        role_results = []
        for command in commands:
            decision = _decision(bash, command, default)
            role_results.append({"command": command, "decision": decision})
            if decision != "allow":
                raise ControlError(
                    f"permission preflight rejected {role} command ({decision}): {command}"
                )
        if isinstance(bash, dict):
            for pattern, decision in bash.items():
                family = _preflight_family(pattern)
                if (decision == "allow" and family is not None
                        and not any(command == family or command.startswith(f"{family} ")
                                    for command in commands)):
                    raise ControlError(
                        f"unexercised {role} command family is not covered by "
                        f"permission_preflight: {pattern}"
                    )
        results[role] = role_results
    for path in sorted(agents.glob("*.md")):
        permission = _frontmatter(path).get("permission", default)
        _reject_ask(permission, f"agent.{path.stem}.permission")
    return results
