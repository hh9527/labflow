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

## Commands

Labflow has one executable and three command groups:

```text
labflow lab    run and inspect laboratory servers
labflow host   start, observe, and control sessions
labflow agent  pull and submit Artifact work
```

Typical use from a project Git worktree:

```bash
labflow lab run local --port 4199
labflow host test-connect local
labflow host start local sample-plan
labflow host status local sample-plan/1
labflow lab ls local
labflow lab attach local sample-plan/1
```

`labflow lab run` writes `{port, root}` to `target/labs/<lab-name>/config.json`. Session state and
workspaces live under that temporary lab root. Stopping the foreground process terminates the
server and reclaims the root.

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

## Agent Loop

An Agent repeatedly runs:

```bash
labflow agent pull a1
labflow agent submit a1 output.a1
```

A successful pull returns the complete direct input and input Asset sets:

```json
{
  "target": { "name": "output.a1" },
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

When no work becomes available before the 60-second timeout, pull returns JSON `null`. A persistent
Agent immediately pulls again.

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
