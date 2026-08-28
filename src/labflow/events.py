from __future__ import annotations

from typing import Any


def _optional_host_inputs(workflow: dict[str, Any]) -> set[str]:
    uses: dict[str, list[bool]] = {}
    for artifact in workflow["artifacts"].values():
        for reference in artifact.get("requires", []):
            uses.setdefault(reference["id"], []).append(reference["optional"])
    return {name for name, flags in uses.items() if flags and all(flags)}


def _ready_host_artifacts(workflow: dict[str, Any], artifacts: dict[str, Any]) -> list[str]:
    return [name for name in workflow["artifacts"]
            if artifacts["artifacts"][name]["owner"] == "host"
            and not artifacts["artifacts"][name]["current"]
            and not artifacts["artifacts"][name]["blocked_by"]]


def pending_requests(workflow: dict[str, Any], artifacts: dict[str, Any]) -> list[str]:
    optional = _optional_host_inputs(workflow)
    return [name for name in _ready_host_artifacts(workflow, artifacts) if name not in optional]


def pending_optional_requests(
    workflow: dict[str, Any], artifacts: dict[str, Any],
) -> list[str]:
    optional = _optional_host_inputs(workflow)
    return [name for name in _ready_host_artifacts(workflow, artifacts) if name in optional]
