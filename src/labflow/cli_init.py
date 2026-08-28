from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from pathlib import Path

from .config import ControlError
from .project import (
    EXEC_NAME, EXEC_SCHEMA, execution_id, ignore_execution, load_plan, project_home,
)
from .state import atomic_write


SERVE_NAME = "serve"
ATTACH_NAME = "attach"
CONTROL_NAME = "control"


def parser(prog: str = "labflow init") -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        prog=prog, description="Generate the current project's Labflow control scripts."
    )
    value.add_argument("--port", type=int, required=True, help="OpenCode port")
    return value


def serve_content(port: int) -> bytes:
    labflow = shlex.join([os.path.abspath(sys.executable), "-m", "labflow.cli"])
    return f'''#!/bin/sh
set -eu

project_home=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
cd "$project_home"

{labflow} supervisor --port {port} --prepare-only

opencode_pid=
supervisor_pid=
cleanup() {{
  status=$?
  trap - EXIT HUP INT TERM
  if [ -n "$supervisor_pid" ]; then
    kill "$supervisor_pid" 2>/dev/null || true
    wait "$supervisor_pid" 2>/dev/null || true
  fi
  if [ -n "$opencode_pid" ]; then
    kill "$opencode_pid" 2>/dev/null || true
    wait "$opencode_pid" 2>/dev/null || true
  fi
  exit "$status"
}}
trap cleanup EXIT HUP INT TERM

if command -v opencode >/dev/null 2>&1; then
  set -- opencode
elif command -v mise >/dev/null 2>&1; then
  set -- mise x -- opencode
else
  echo "labflow serve: external CLI unavailable: opencode" >&2
  exit 69
fi

OPENCODE_CONFIG="$project_home/.labflow-exec/ws/opencode.json" \\
OPENCODE_CONFIG_DIR="$project_home/.labflow-exec/ws/.opencode" \\
"$@" serve --hostname 127.0.0.1 --port {port} --pure &
opencode_pid=$!

while kill -0 "$opencode_pid" 2>/dev/null; do
  while [ ! -f .labflow-exec/artifacts/_supervisor ]; do
    if ! kill -0 "$opencode_pid" 2>/dev/null; then
      wait "$opencode_pid"
      exit $?
    fi
    sleep 0.25
  done

  {labflow} supervisor --port {port} &
  supervisor_pid=$!
  if wait "$supervisor_pid"; then
    supervisor_status=0
  else
    supervisor_status=$?
  fi
  supervisor_pid=
  if [ "$supervisor_status" -ne 0 ]; then
    exit "$supervisor_status"
  fi
done

wait "$opencode_pid"
'''.encode()


def attach_content() -> bytes:
    labflow = shlex.join([os.path.abspath(sys.executable), "-m", "labflow.cli"])
    return f'''#!/bin/sh
set -eu

project_home=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
cd "$project_home"
exec {labflow} attach
'''.encode()


def control_content() -> bytes:
    return b'''#!/bin/sh
set -eu

project_home=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
artifacts="$project_home/.labflow-exec/artifacts"
mkdir -p "$artifacts"

case "${1:-status}" in
  active-on)
    touch "$artifacts/_active"
    ;;
  active-off)
    rm -f "$artifacts/_active"
    ;;
  supervisor-on)
    touch "$artifacts/_supervisor"
    ;;
  supervisor-off)
    rm -f "$artifacts/_supervisor"
    ;;
  status)
    [ -f "$artifacts/_active" ] && active=on || active=off
    [ -f "$artifacts/_supervisor" ] && supervisor=on || supervisor=off
    if [ -f "$artifacts/_system-blocked" ]; then
      if [ -f "$artifacts/_active" ] && [ "$artifacts/_active" -nt "$artifacts/_system-blocked" ]; then
        blocked=acknowledged
      else
        blocked=on
      fi
    else
      blocked=off
    fi
    printf 'active=%s supervisor=%s system-blocked=%s\n' \
      "$active" "$supervisor" "$blocked"
    ;;
  *)
    echo "usage: $0 {status|active-on|active-off|supervisor-on|supervisor-off}" >&2
    exit 64
    ;;
esac
'''


def generate(root: Path, port: int) -> tuple[Path, Path]:
    if isinstance(port, bool) or not 1 <= port <= 65535:
        raise ControlError("port must be from 1 through 65535", 64)
    project = root.resolve(strict=True)
    load_plan(project / "labflow-plan.toml")
    ignore_execution(project)
    control = project / EXEC_NAME
    config_path = control / "config.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        config = None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid execution config: {exc}", 73) from None
    if config is not None and (
        not isinstance(config, dict)
        or config.get("schema") != EXEC_SCHEMA
        or config.get("execution_id") != execution_id(project)
        or config.get("project_home") != str(project)
    ):
        raise ControlError("invalid project execution configuration", 73)
    if isinstance(config, dict) and config.get("port") != port:
        raise ControlError(
            f"execution is configured for port {config.get('port')}, not {port}", 64
        )
    scripts = (
        (control / "bin" / SERVE_NAME, serve_content(port)),
        (control / "bin" / ATTACH_NAME, attach_content()),
        (control / "bin" / CONTROL_NAME, control_content()),
    )
    for target, content in scripts:
        try:
            atomic_write(target, content, 0o755)
        except OSError as exc:
            raise ControlError(f"cannot write control script: {exc}", 73) from None
    return scripts[0][0], scripts[1][0]


def main(argv: list[str] | None = None, *, prog: str = "labflow init") -> int:
    args = parser(prog).parse_args(argv)
    try:
        serve, attach = generate(project_home(), args.port)
        print(f"Generated {serve}, {attach}, and {serve.parent / CONTROL_NAME}")
        return 0
    except ControlError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return exc.code
    except OSError as exc:
        print(f"{prog}: {exc}", file=sys.stderr)
        return 69
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
