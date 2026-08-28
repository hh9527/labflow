# Labflow

Labflow runs one artifact-driven Plan in its own project directory. OpenCode and the Supervisor are
ordinary external processes owned by the operator. The Supervisor prepares and maintains the hidden
execution control plane; Host control is entirely file based.

## Run

Put `labflow-plan.toml` in the project root and generate its local launcher:

```bash
cd /path/to/project
labflow init --port 4199
./.labflow-exec/bin/serve
```

`init` creates `.labflow-exec/bin/serve` and `.labflow-exec/bin/attach`. The `serve` script first asks
the Supervisor command to prepare `.labflow-exec`, starts OpenCode from `prj-home` with the generated
configuration, and then waits for `artifacts/_supervisor`. It invokes a real Supervisor generation only
while that marker exists. Leave it running in its own terminal. To attach the TUI from another
terminal, run:

```bash
./.labflow-exec/bin/attach
```

The equivalent manual OpenCode command is:

```bash
OPENCODE_CONFIG="$PWD/.labflow-exec/ws/opencode.json" \
OPENCODE_CONFIG_DIR="$PWD/.labflow-exec/ws/.opencode" \
opencode serve --hostname 127.0.0.1 --port 4199 --pure
```

Host commands only read Supervisor file projections:

```bash
labflow host status
labflow query 'SELECT type, COUNT(*) FROM timeline GROUP BY type'
labflow host pull
labflow attach
```

Optional ontology-backed queries are enabled only when both `TELORA_BIN` and
`OM_LABFLOW_PATH` are set:

```bash
TELORA_BIN=/path/to/telora \
OM_LABFLOW_PATH=/path/to/om-labflow \
labflow query-om request.json
```

`query-om` invokes the OM-Labflow `query` entry with the JSON file as its named
`input` source, validates the resulting `{sql, bindings}` object, and executes it through
the same strictly read-only SQLite boundary as `labflow query`. Paths may be relative to the
calling directory. Use `labflow query-om -` to read the request JSON from standard input.

Host control operations are ordinary project file operations: copy or atomically replace Assets,
touch `.labflow-exec/artifacts/<artifact>`, and update the reserved markers in that directory. Host never
starts, stops, or calls OpenCode. Initialization does not create either control marker; publish
initial Host Artifacts and explicitly start the Supervisor and reconciliation with:

```bash
touch .labflow-exec/artifacts/<artifact>
touch .labflow-exec/artifacts/_supervisor
touch .labflow-exec/artifacts/_active
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
    report-cursor
    states.sqlite
    events.sqlite
    artifacts/
      _active
      _supervisor
      <artifact>
    ws/
      opencode.json
      .opencode/
        agents/
          lab-ob.md
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

`artifacts/_active` is also the Plan activation boundary. While it is absent, Supervisor observes
existing Sessions but creates no Session, settlement, or prompt effects. Creating it or changing its mtime
causes Supervisor to reread `labflow-plan.toml`, regenerate `runtime.json` and OpenCode Agent files,
and atomically switch to the new DAG. Editing the Plan without touching `artifacts/_active` has no effect.
Deleting the marker stops new scheduling without discarding the last valid Plan. An invalid Plan
leaves scheduling stopped and appears as `plan_error` in `supervisor-status.json` until the Plan is
fixed and `artifacts/_active` is touched again.

One `labflow supervisor` invocation runs one generation: it records the mtime of
`artifacts/_supervisor`, then exits when that marker is touched or deleted. If the marker is absent, the
command only prepares the execution and returns. A process-independent shell loop can keep it
available:

```bash
# The first call prepares .labflow-exec and returns when no marker exists.
labflow supervisor --port 4199
while :; do
  while [ ! -f .labflow-exec/artifacts/_supervisor ]; do sleep 0.25; done
  labflow supervisor --port 4199
done
```

The launcher knows nothing about Supervisor state or OpenCode; it only waits for a regular file and
starts a command. Touching `artifacts/_supervisor` therefore restarts Supervisor through the next loop
iteration, while deleting it stops Supervisor until Host recreates it. Host affects execution only
through project Assets and these control files; it never calls the OpenCode API.

`artifacts/_system-blocked` is written by a Supervisor effect when an Agent explicitly cannot
complete a Task, or when the same Task fails protocol or mechanical validation three times within
two minutes. While it exists, the reducer emits no scheduling effects. Host resolves the underlying
problem and deletes this marker to resume reconciliation.

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
read = ["goals/", "bin/tool", "docs/", "shared/"]
write = ["src/", "scratch/builder/"]
commands = ["telora --help", "telora -C *", "project-tool verify *"]

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

[artifacts.feedback]
assets = ["feedback.md"]
```

Fields have distinct meanings:

- `requires` defines the Artifact DAG, task triggering, and freshness. A trailing `?` makes a
  dependency optional.
- `assets` describes the current Artifact's output range.
- `inputs` is the task's read list. When omitted, it is the deduplicated union of the direct
  `requires` Artifacts' `assets`; an explicit value, including `[]`, replaces that default.
- `check` lists paths that must exist before the Artifact can settle.
- `goal` is the task document used for a role-owned Artifact.

Every inferred role must have a `[roles.<role>]` table that explicitly defines `read`, `write`, and
`commands`, using `[]` for an empty permission set. These stable settings are the role's only
permissions; Labflow never expands them from tasks. When loading the Plan, Labflow verifies that
each owned Artifact's `goal`, resolved `inputs`, and `assets` are readable and its `assets` are
writable by the role. A directory permission ending in `/` covers paths below it, and a `write` path
is also readable. Commands come only from the role configuration and are not inferred or checked
against individual tasks. A configured role must exist as an owner inferred from an Artifact suffix.

The Supervisor maintains one long-lived Session per role. It creates a task record in
`states.sqlite`, prompts an idle role only when one of its Artifacts is runnable, validates `assets`
and `check` after the turn, and refreshes the Artifact marker on success. Each task description lists
its resolved `inputs`; `updated` means that the file or directory tree changed after that role's
previous task ended. Input Asset changes never trigger a task by themselves.

The root Session uses the `lab-ob` observer agent. It knows the execution database schemas and may use
`labflow host status` or the read-only `labflow query '<SQL>'` interface to answer ad hoc status,
statistics, and analysis questions. The query exposes `events.sqlite` as the main database and
`states.sqlite` under the `states` schema, with a two-second execution limit and a 1000-row result
limit. It grants no project or control-plane writes. Supervisor separately prints coarse
`task_started`, `task_completed`, and Host-waiting Timeline batches to its standard output, so the
`serve` terminal remains the continuous progress display. Batches use a five-second quiet debounce
and a fifteen-second maximum debounce. The committed SQLite row cursor is stored in
`.labflow-exec/report-cursor` so Supervisor restarts neither duplicate nor miss accepted reports.

Generated role files contain only the stable role identity and contain no task instructions. On Plan
activation, Supervisor writes one stable `.labflow-exec/ws/.opencode/agents/<role>.md` per role. Its
permissions come only from the role table: `read` controls `read`, `glob`, `grep`, and directory
listing; `write` controls edits and is also readable; `commands` defines the command allowlist. Plan
validation ensures those permissions cover the role's complete DAG responsibility. Task dispatch
never replaces a role file. OpenCode must be restarted after a
role permission change so it reloads the agent definition. Supervisor supplies a uniform task prompt
that references the selected Artifact's `goal` path and lists dependency and input freshness; it does
not embed the goal document's contents. Directory inputs ending in `/` are recursively expanded into
their concrete files in the prompt, with freshness computed for each file.

The DAG revision covers both the normalized Artifact DAG and role permissions. Changing either
supersedes active tasks before they are dispatched again.

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
