# Labflow

Labflow runs reproducible, artifact-driven agent laboratories. Its workflow model has three
resources:

```text
Artifact  expresses completion
Asset     carries workspace content
Session   carries agent context
```

Labflow currently provides an OpenCode runtime adapter. Workflow plans and the Artifact/Asset
protocol are independent of the application, language, and domain under test.

## Execution Modes

Labflow has two distinct execution modes:

```text
dag-mode      A Supervisor maintains role Sessions and delivers Artifact work through prompts.
benchmark-mode  A Questioner and Answerer run a fixed, answer-free problem suite.
```

`dag-mode` requires a Workflow and derives role permissions from Artifact Assets. `benchmark-mode`
has no Workflow or coordinator. It gives the Answerer read-only inputs and writable outputs while
keeping each problem's optional hidden knowledge private to the Questioner. Each fresh Q/A pair
handles a bounded batch of problems without Session forks.

## Commands

Labflow has one executable, four command groups, and a direct TUI entry:

```text
labflow lab    run laboratory servers
labflow attach connect a TUI and select a session
labflow host   start, observe, and control sessions
labflow agent  operate inside an Agent workspace
labflow supervisor  maintain Sessions and the laboratory Timeline
```

Typical use from a project Git worktree:

```bash
labflow lab run local --port 4199
labflow host test-connect local
labflow supervisor local
labflow host start local sample-plan
labflow host status local sample-plan@1
labflow attach local
# Stop the server, then reclaim the laboratory:
labflow lab remove local
```

`labflow lab run` creates `.labs/<lab-name>` as a symbolic link to the temporary Lab root. The Lab
root's `config.json` is the configuration source of truth; it records the Lab name, port, and Host
workspace. Agent workspaces live at `ws/<plan-id>[.<variant>]@<generation>/`, while Host-only state
and archives live under `control/` and `archive/`. `lab run` replaces its own process with the
OpenCode server, so a running laboratory does not retain Labflow's runtime. After stopping the
server, `lab remove` removes the symbolic link and reclaims the Lab root.

Run exactly one `labflow supervisor <lab-name>` as a separate foreground process for each Lab. It
can start before any execution exists and does not currently own or stop `labflow lab run`. A
non-blocking Lab-level ownership lock rejects a second Supervisor. Host start publishes an execution
maintenance directory; the Supervisor then observes its Sessions and Timeline. An execution with an
`artifacts/` directory additionally enables Artifact-DAG scheduling.

```text
<lab-root>/
  supervisor/
    <plan-id>[.<variant>]@<generation>/
      artifacts/
        <artifact>
  timeline.sqlite3
```

The Supervisor directory is desired state. For DAG executions its Artifact directory is canonical;
the Agent workspace `control/artifacts` path is a generated link to it. `timeline.sqlite3` is one
laboratory-wide append-only observation database. It stores closed Session and Turn activity plus
Task, Artifact, Host-request, and DAG-revision facts. Host observation and statistics read it.
Reducer scheduling state is kept in memory and never recovered from Timeline data.

Every Timeline item can carry `session`, `turn`, `task_kind`, `task_id`, `artifact`, and
`dag_revision` dimensions. A Task is either an Artifact task (`task_kind = artifact`, with the
Artifact name as `task_id`) or a Benchmark problem (`task_kind = problem`, with the problem ID as
`task_id`). One Task may span multiple Turns. A Turn belongs to at most one Task, while management
Turns and workflow facts may omit either dimension. The DAG revision is the SHA-256 digest of the
normalized Workflow, so it remains stable across Supervisor restarts and changes whenever the DAG
definition changes.

Scheduling is a reconciliation loop over desired and observed state:

```text
desired execution/Artifact state + observed Workflow/Session state
    -> mutable reducer state + idempotent CreateSession/PromptSession effects
    -> observed backend state
```

The Workflow snapshot is taken under the same lock used by Agent Artifact operations. A missing role
Session is created, a busy Session is only observed, and an idle Session is prompted only while it
owns runnable Artifact pressure. After a prompt, a newly completed assistant turn or an observed
busy-to-idle transition makes the Session eligible for reconciliation again. Correctness does not
depend on sleeping for a grace period or guessing that an asynchronous operation has completed.
Removing `artifacts/` clears scheduling pressure while preserving Timeline observation; removing the
execution maintenance directory forgets its scheduling state without deleting Timeline history.

The latest observation is written atomically to `<lab-root>/supervisor-status.json`, including all
observed Session identities and states, required and optional Host requests, and role-level runtime
errors. Host event queries merge the persisted Timeline with operational Host-action records.

## Artifact Workflow

An Artifact is a timestamped workflow fact. A name ending in `.<role>` is owned by that role; every
other Artifact is owned by the Host. The workflow does not contain a separate `owner` field.

```json
{
  "schema": "labflow.workflow/v1",
  "roles": ["a1", "a2"],
  "artifacts": {
    "input": {
      "desc": "Initial material",
      "assets": [
        { "path": "bin/tool", "level": 0 },
        { "path": "guide/", "level": 0 }
      ]
    },
    "output.a1": {
      "desc": "First role output",
      "input": ["input", "notes?"],
      "assets": [{ "path": "src/", "level": 2 }],
      "instruction": "Produce the first output"
    },
    "notes": {
      "desc": "Optional Host input",
      "assets": [{ "path": "NOTES.md", "level": 1 }]
    },
    "result": {
      "desc": "Completed workflow",
      "input": ["output.a1"]
    }
  }
}
```

An Asset path ending in `/` denotes a directory; every other path denotes a file. `level` defaults
to `2` and controls retention only:

```text
0  environment or scaffold Asset; never backed up
1  process Asset; backed up only when explicitly requested
2  result Asset; backed up by default
```

Refreshing an Artifact validates that its declared Assets exist with the declared file/directory
kind. Updating an Asset does not refresh an Artifact or trigger the DAG.

Every Artifact independently creates completion pressure. A ready role-owned Artifact is offered to
that Agent; a ready Host-owned Artifact is offered to the Host. Inputs only control readiness and
invalidation. Labflow does not designate workflow start or finish nodes, and the Host decides when
to stop a session.

Role file permissions are derived from the same graph. The Assets of Artifacts owned by a role are
read/write; the Assets of their direct inputs are read-only. A write grant wins when a path occurs
in both sets. Plans therefore do not maintain separate role `read` or `write` lists.

## Task Delivery

An Agent does not poll for work. When a role Session is idle and owns a runnable Artifact, the
Supervisor atomically assigns one Task and sends a text prompt containing the target, its
instruction, the complete direct input state, changed Assets, and the exact submit command. The
underlying Task record has this structure:

```json
{
  "target": { "name": "output.a1", "instruction": "Produce the first output" },
  "inputs": [
    { "name": "input", "fresh": true },
    { "name": "notes", "fresh": null }
  ],
  "assets": [
    { "path": "bin/tool", "updated": true },
    { "path": "guide/", "updated": true }
  ]
}
```

`fresh` is `true` when the input was refreshed after the target, `false` when it was not, and `null`
when an optional input does not yet exist. `updated` follows the same Artifact publication boundary
for Assets. Labflow does not hash Asset contents to infer workflow changes.

The Agent completes exactly the delivered Task, runs `labflow agent submit <role> <artifact>`, and
ends its Turn. It does not claim or wait for another Task. A later runnable Artifact causes another
Supervisor prompt. A busy Session is never interrupted. Assignment and stale-Task replacement use
the same lock as Artifact evaluation; failed prompt delivery leaves the active Task available for
deterministic redelivery.

Host pull distinguishes blocking `requests` from `opt_requests`. A Host Artifact used only through
optional inputs appears in `opt_requests`; it remains visible but does not wake a waiting Host pull.

## Benchmark Mode

Benchmark plans declare inputs, outputs, and questions without expected answers:

```json
{
  "kind": "benchmark-mode",
  "questioner": "q",
  "answerer": "a",
  "batchSize": 5,
  "input": [{"path": "knowledge/"}],
  "output": [{"path": "ch/out/", "level": 2}],
  "problems": [
    {"q": "problems/0000.md", "maxTurns": 2},
    {"q": "problems/0001.md", "k": "problems/0001-info.md", "maxTurns": 3}
  ]
}
```

At start, Labflow copies the complete suite into the plan workspace as
`problem/<id>/q.md` and optional `problem/<id>/k.md`, then triggers each batch once. The Questioner
opens each problem with `labflow agent start-problem <id>`, which copies exact Q/K and generated metadata
into `ch/`. It passes `ch/q.md` verbatim through dialogue, maintains the required nonempty
`ch/out/report.md`, and closes with `labflow agent end-problem ok|error|cancel`. The Answerer may leave `ch/out/ok-*` success
evidence or `ch/out/err-*` failure evidence; both sets may be absent and they cannot coexist. Labflow
does not interpret evidence file formats. The Answerer cannot write `report.md`.

Labflow partitions the suite into groups of `batchSize`. Each group gets a fresh Questioner Session;
the Questioner creates one fresh Answerer child and reuses that pair for every problem in the group.
There is no preflight or Session fork. Reuse amortizes language, documentation, and tool learning
while different groups remain isolated.

Within one batch the Questioner reads all prepared Q/K files in order, asks through its one Answerer
child, and supplies only requested clarification. Recording copies `report.md` and optional evidence
to `result/<id>/`, clears the channel, and establishes the next problem's timing boundary. `ok`
retains only `ok-*`, `error` only `err-*`, and `cancel` neither evidence family. After the
batch returns, Labflow writes one complete record per problem to `result/stats.jsonl`. Correctness
remains a Host judgment; the Host does not participate in per-problem scheduling.

## Run With uv

No installation is required:

```bash
uvx --from 'git+https://github.com/hh9527/labflow.git@<commit>' labflow --help
```

Pin a commit or immutable release tag for real experiments.

## Development

Labflow requires Python 3.11 or newer and has no runtime dependencies:

```bash
python3 -m unittest discover -s tests -t .
python3 -m py_compile src/labflow/*.py
```
