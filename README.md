# Labflow

Labflow runs one artifact-driven Plan in its own project directory. OpenCode and the Supervisor are
ordinary external processes owned by the operator. The Supervisor prepares and maintains the hidden
execution control plane; Host control is entirely file based.

## Run

Put `labflow-plan.toml` in the project root and start the Supervisor first:

```bash
cd /path/to/project
labflow supervisor --port 4199
```

On first start, the Supervisor creates `.labflow-exec`, prints its location, and waits for OpenCode.
Start OpenCode from `prj-home` in another terminal using the generated configuration:

```bash
OPENCODE_CONFIG="$PWD/.labflow-exec/ws/opencode.json" \
OPENCODE_CONFIG_DIR="$PWD/.labflow-exec/ws/.opencode" \
opencode serve --hostname 127.0.0.1 --port 4199 --pure
```

Host commands only read Supervisor file projections:

```bash
labflow host status
labflow host pull
labflow attach
```

Host control operations are ordinary project file operations: copy or atomically replace Assets,
touch `.labflow-exec/artifacts/<artifact>`, and update markers under `.labflow-exec/ctrl`. Host never
starts, stops, or calls OpenCode. Initialization does not create either control marker; publish
initial Host Artifacts and explicitly start the Supervisor and reconciliation with:

```bash
touch .labflow-exec/artifacts/<artifact>
touch .labflow-exec/ctrl/supervisor
touch .labflow-exec/ctrl/active
```

Once external processes are stopped, the temporary Lab service directory recorded in
`.labflow-exec/config.json` can be reclaimed with:

```bash
labflow lab remove /tmp/labflow-...
```

## Layout

The project itself is always the Agent workspace. Business files and Assets stay in place; Labflow
does not copy or back them up.

```text
prj-home/
  labflow-plan.toml
  <project files and Assets>
  .labflow-exec/
    config.json
    lock
    runtime.json
    host-tasks.json
    supervisor-status.json
    states.sqlite
    events.sqlite
    ctrl/
      active
      supervisor
    artifacts/
      <artifact>
    roles/
      <dag-hash>/
        <artifact>.md
        .idle.<role>.md
    ws/
      opencode.json
      .opencode/
        agents/
```

`.labflow-exec` is the execution control plane. It must be ignored by version control and cannot be
declared as a Plan input, Asset, check, or goal. Generated OpenCode configuration lives under
`.labflow-exec/ws`, but every OpenCode request uses `prj-home` as its `directory`. The server starts
with:

```text
OPENCODE_CONFIG=<prj-home>/.labflow-exec/ws/opencode.json
OPENCODE_CONFIG_DIR=<prj-home>/.labflow-exec/ws/.opencode
```

`states.sqlite` stores recoverable mutable state such as root Session identity and task records.
`events.sqlite` stores the summarized event stream. Artifact facts remain empty timestamped files so
the Host can publish one directly with `touch .labflow-exec/artifacts/<artifact>`.

`ctrl/active` is also the Plan activation boundary. While it is absent, Supervisor observes and
settles existing work but creates no new Session or prompt effects. Creating it or changing its mtime
causes Supervisor to reread `labflow-plan.toml`, regenerate `runtime.json` and OpenCode Agent files,
and atomically switch to the new DAG. Editing the Plan without touching `ctrl/active` has no effect.
Deleting the marker stops new scheduling without discarding the last valid Plan. An invalid Plan
leaves scheduling stopped and appears as `plan_error` in `supervisor-status.json` until the Plan is
fixed and `ctrl/active` is touched again.

One `labflow supervisor` invocation runs one generation: it records the mtime of
`ctrl/supervisor`, then exits when that marker is touched or deleted. If the marker is absent, the
command only prepares the execution and returns. A process-independent shell loop can keep it
available:

```bash
# The first call prepares .labflow-exec and returns when no marker exists.
labflow supervisor --port 4199
while :; do
  while [ ! -f .labflow-exec/ctrl/supervisor ]; do sleep 0.25; done
  labflow supervisor --port 4199
done
```

The launcher knows nothing about Supervisor state or OpenCode; it only waits for a regular file and
starts a command. Touching `ctrl/supervisor` therefore restarts Supervisor through the next loop
iteration, while deleting it stops Supervisor until Host recreates it. Host affects execution only
through project Assets and these control files; it never calls the OpenCode API.

## Execution Identity

Each project has one execution ID:

```text
basename(realpath(prj-home)) + "." + sha256(realpath(dirname(prj-home)))[:16]
```

The project directory therefore determines identity. `plan.name`, Lab names, execution variants,
workspace inheritance, archives, and backup levels are not part of this model.

## Plan

Artifact ownership is inferred from names. An Artifact ending in `.<role>` is Agent-owned and must
have a `goal`; all others are Host-owned. A name containing `.sess.<role>` represents knowledge tied
to that role Session.

```toml
[roles.builder]
read = ["shared/"]
write = ["scratch/builder/"]
commands = ["telora --help", "telora -C *"]

[artifacts.tool]
assets = ["bin/tool"]

[artifacts."learn.sess.builder"]
goal = "goals/learn.md"
requires = ["tool"]

[artifacts."result.builder"]
goal = "goals/build.md"
requires = ["learn.sess.builder", "feedback?"]
inputs = ["docs/"]
assets = ["src/"]
check = ["src/result.txt"]
commands = ["project-tool verify *"]

[artifacts.feedback]
assets = ["feedback.md"]
```

Fields have distinct meanings:

- `requires` defines the Artifact DAG, task triggering, and freshness. A trailing `?` makes a
  dependency optional.
- `assets` describes the current Artifact's output range and grants write access to its owner.
- `inputs` is the task's read list. When omitted, it is the deduplicated union of the direct
  `requires` Artifacts' `assets`; an explicit value, including `[]`, replaces that default.
- `check` lists paths that must exist before the Artifact can settle.
- `goal` is the task document used for a role-owned Artifact.
- `commands` grants Artifact-scoped shell command patterns. Executable files in `inputs` also grant
  `<path> *` and `./<path> *` automatically.

An optional `[roles.<role>]` table grants stable `read`, `write`, and `commands` permissions. Role
permissions are appended after the selected Artifact's permissions, and role `write` paths are also
readable. A configured role must exist as an owner inferred from an Artifact suffix.

The Supervisor maintains one long-lived Session per role. It creates a task record in
`states.sqlite`, prompts an idle role only when one of its Artifacts is runnable, validates `assets`
and `check` after the turn, and refreshes the Artifact marker on success. Each task description lists
its resolved `inputs`; `updated` means that the file or directory tree changed after that role's
previous task ended. Input Asset changes never trigger a task by themselves.

Generated role files contain only the stable role identity and contain no task instructions. On Plan
activation, Supervisor writes immutable permission snapshots for every role-owned Artifact under
`.labflow-exec/roles/<dag-hash>/`, plus a role-only idle snapshot for each role. Immediately before each
dispatch, it atomically replaces `.labflow-exec/ws/.opencode/agents/<role>.md` with a hard link to the
selected snapshot: resolved `inputs` plus `assets` are readable, only `assets` are writable, and
Artifact commands and executable inputs provide that task's command allowlist. Stable role permissions
are appended to every snapshot. A settled task links the role back to its role-only identity.
Supervisor supplies the selected Artifact's complete `goal`, dependencies, and resolved input list in
the task prompt.

The generation hash covers both the normalized Artifact DAG and role permissions. Changing either
creates a new snapshot generation and supersedes active tasks before they are dispatched again.

## Development

```bash
uv run python -m unittest discover -s tests -v
uv run python -m py_compile src/labflow/*.py
```

## Run With uv

```bash
uvx --from 'git+https://github.com/hh9527/labflow.git@<commit>' labflow --help
```

Pin a commit or immutable release tag for experiments.
