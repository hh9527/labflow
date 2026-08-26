from __future__ import annotations

import os
import subprocess

from .config import ControlError
from .external import probe_direct, probe_mise, resolve_capabilities


def select_engine() -> tuple[str, list[str]]:
    override = os.environ.get("OC_QUERY_ENGINE")
    if override:
        if override not in ("jaq", "jq", "mise-jaq", "mise-jq"):
            raise ControlError(f"invalid OC_QUERY_ENGINE: {override}", 64)
        cli = override.removeprefix("mise-")
        command = probe_mise(cli) if override.startswith("mise-") else probe_direct(cli)
        if command is None: raise ControlError(f"query backend unavailable: {override}", 69)
        return override, list(command)
    command = resolve_capabilities({"query": ("jaq", "jq")})["query"]
    cli = command[-1]
    return (f"mise-{cli}" if len(command) > 1 else cli), list(command)


def run_query(document: str, expression: str | None, query_file: str | None, raw: bool) -> int:
    _, command = select_engine(); args = [*command]
    if raw: args.append("-r")
    args.extend(["-f", query_file] if query_file else [expression or "."])
    return subprocess.run(args, input=document, text=True).returncode
