from __future__ import annotations

import subprocess
from functools import lru_cache
from pathlib import Path

from .config import ControlError


def _version_succeeds(command: tuple[str, ...]) -> bool:
    try:
        result = subprocess.run(
            [*command, "--version"], text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=10,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


@lru_cache(maxsize=None)
def probe_direct(cli: str) -> tuple[str, ...] | None:
    command = (cli,)
    return command if _version_succeeds(command) else None


@lru_cache(maxsize=None)
def probe_mise(cli: str) -> tuple[str, ...] | None:
    command = ("mise", "x", "--", cli)
    return command if _version_succeeds(command) else None


@lru_cache(maxsize=None)
def resolve_cli(cli: str) -> tuple[str, ...]:
    command = probe_direct(cli) or probe_mise(cli)
    if command is None:
        raise ControlError(f"external CLI unavailable: {cli}", 69)
    return command


def resolve_capabilities(candidates: dict[str, tuple[str, ...]]) -> dict[str, tuple[str, ...]]:
    """Probe logical capabilities and return their successful command prefixes."""
    result: dict[str, tuple[str, ...]] = {}
    for capability, choices in candidates.items():
        command = next((value for cli in choices if (value := probe_direct(cli))), None)
        if command is None:
            command = next((value for cli in choices if (value := probe_mise(cli))), None)
        if command is None:
            raise ControlError(f"external CLI capability unavailable: {capability} ({', '.join(choices)})", 69)
        result[capability] = command
    return result


def resolve_command(arguments: list[str] | tuple[str, ...], cwd: Path | None = None) -> list[str]:
    if not arguments:
        raise ControlError("external command must not be empty")
    executable = arguments[0]
    if "/" in executable:
        candidate = Path(executable)
        if not candidate.is_absolute() and cwd is not None:
            candidate = cwd / candidate
        if not candidate.is_file() or not candidate.stat().st_mode & 0o111:
            raise ControlError(f"external command is missing or not executable: {executable}", 69)
        return list(arguments)
    return [*resolve_cli(executable), *arguments[1:]]
