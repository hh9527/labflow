from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from labflow.client import OpenCodeNotFound
from labflow.cli_host import pull, status
from labflow.cli_init import generate as generate_launcher
from labflow.cli_lab import remove as remove_lab
from labflow.config import ControlError
from labflow.project import (
    activate_plan, execution_id, load_execution, load_plan, prepare_execution,
)
from labflow.runtime_opencode import dag_hash, resume_prompt
from labflow.supervisor import Supervisor, main as supervisor_main
from labflow.task_cli import (
    assign_task, evaluate, load_workflow, refresh_artifact, submit,
    task_records,
)
from labflow.state import atomic_json


PLAN = '''
[artifacts.tool]
assets = ["bin/tool"]

[artifacts."learn.sess.a1"]
goal = "goals/learn.md"
requires = ["tool"]

[artifacts."work.a1"]
goal = "goals/work.md"
requires = ["learn.sess.a1", "feedback?"]
inputs = ["docs/"]
assets = ["src/"]
check = ["src/result.txt"]

[artifacts.feedback]
assets = ["feedback.md"]

[artifacts.done]
requires = ["work.a1"]
assets = ["src/result.txt"]
check = ["src/result.txt"]

[roles.a1]
read = ["goals/", "bin/tool", "docs/"]
write = ["src/"]
commands = []
'''


class ProjectPlanTest(unittest.TestCase):
    def project(self, parent: Path) -> Path:
        root = parent / "demo"
        (root / "goals").mkdir(parents=True)
        (root / "bin").mkdir()
        (root / "docs").mkdir()
        (root / "src").mkdir()
        (root / "labflow-plan.toml").write_text(PLAN, encoding="utf-8")
        (root / "goals" / "learn.md").write_text("# Learn\n\nLearn it.\n", encoding="utf-8")
        (root / "goals" / "work.md").write_text("# Work\n\nBuild it.\n", encoding="utf-8")
        tool = root / "bin" / "tool"
        tool.write_text("tool", encoding="utf-8")
        tool.chmod(0o755)
        return root

    def test_task_prompt_uses_uniform_file_reference_template(self):
        prompt = resume_prompt("a1", {}, {
            "target": {
                "name": "work.a1", "goal": "goals/work.md", "goal_updated": True,
            },
            "requires": [
                {"name": "fresh", "fresh": True},
                {"name": "same", "fresh": False},
                {"name": "missing", "fresh": None},
            ],
            "inputs": [
                {"path": "docs/A.md", "updated": True},
                {"path": "docs/B.md", "updated": False},
            ],
        })
        self.assertEqual(prompt, '''# 任务：`work.a1`

## 目标

按照 `goals/work.md` 的要求完成任务，并简单回复：

- 如果完成，则回复必须以“已完成任务。”开头
- 如果无法完成任务，则回复必须以“无法完成任务。”开头

## 前序任务输出

- `fresh`（已刷新）
- `same`（未改变）
- `missing`（尚不存在）

## 你需要的详细文件清单

- `goals/work.md`（已更新）
- `docs/A.md`（已更新）
- `docs/B.md`（未改变）
''')

    def test_plan_identity_and_workflow_are_derived_from_project(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.project(Path(temporary))
            digest = hashlib.sha256(os.fsencode(str(root.parent.resolve()))).hexdigest()[:16]
            self.assertEqual(execution_id(root), f"demo.{digest}")

            manifest = load_plan(root / "labflow-plan.toml")

            self.assertEqual(list(manifest.roles), ["a1"])
            work = manifest.workflow["artifacts"]["work.a1"]
            self.assertEqual(
                work["requires"],
                [{"id": "learn.sess.a1", "optional": False},
                 {"id": "feedback", "optional": True}],
            )
            self.assertEqual([item["path"] for item in work["inputs"]], ["docs/"])
            self.assertEqual(work["goal"], "goals/work.md")
            self.assertEqual(manifest.roles["a1"]["read"], [
                "goals/", "bin/tool", "docs/",
            ])

    def test_plan_rejects_task_paths_outside_explicit_role_permissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.project(Path(temporary))
            plan = PLAN.replace(
                'read = ["goals/", "bin/tool", "docs/"]',
                'read = ["goals/", "bin/tool"]',
            )
            (root / "labflow-plan.toml").write_text(plan, encoding="utf-8")

            with self.assertRaisesRegex(
                ControlError,
                r"role a1 lacks permissions required by artifact work\.a1: read 'docs/'",
            ):
                load_plan(root / "labflow-plan.toml")

    def test_plan_requires_complete_explicit_role_permissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.project(Path(temporary))
            plan = PLAN.replace("commands = []\n", "")
            (root / "labflow-plan.toml").write_text(plan, encoding="utf-8")

            with self.assertRaisesRegex(ControlError, "must explicitly define: commands"):
                load_plan(root / "labflow-plan.toml")

    def test_init_generates_executable_project_control_scripts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.project(Path(temporary))
            serve, attach = generate_launcher(root, 4199)
            serve_content = serve.read_text(encoding="utf-8")
            attach_content = attach.read_text(encoding="utf-8")

            self.assertEqual(serve, root / ".labflow-exec/bin/serve")
            self.assertEqual(attach, root / ".labflow-exec/bin/attach")
            self.assertEqual(serve.stat().st_mode & 0o777, 0o755)
            self.assertEqual(attach.stat().st_mode & 0o777, 0o755)
            self.assertIn("supervisor --port 4199 --prepare-only", serve_content)
            self.assertIn(os.path.abspath(sys.executable), serve_content)
            self.assertIn(
                '"$@" serve --hostname 127.0.0.1 --port 4199 --pure',
                serve_content,
            )
            self.assertIn("[ ! -f .labflow-exec/ctrl/supervisor ]", serve_content)
            self.assertIn("labflow.cli attach", attach_content)
            subprocess.run(["sh", "-n", str(serve)], check=True)
            subprocess.run(["sh", "-n", str(attach)], check=True)

            serve.write_text("local change\n", encoding="utf-8")
            regenerated, _ = generate_launcher(root, 4199)
            self.assertIn(
                "supervisor --port 4199 --prepare-only",
                regenerated.read_text(encoding="utf-8"),
            )

    def test_init_rejects_an_invalid_port(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.project(Path(temporary))
            with self.assertRaisesRegex(ControlError, "port must be"):
                generate_launcher(root, 0)

    def test_load_execution_rebuilds_missing_state_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, _, _ = prepare_execution(root, lab, 4199)
            (home / "states.sqlite").unlink()

            loaded_home, _, _ = load_execution(root)

            self.assertEqual(loaded_home, home)
            with sqlite3.connect(home / "states.sqlite") as connection:
                tables = {
                    row[0] for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                states = dict(connection.execute("SELECT key, value FROM state"))
            self.assertIn("state", tables)
            self.assertIn("task_records", tables)
            self.assertIsNone(json.loads(states["root_session_id"]))
            self.assertEqual(json.loads(states["active_control"]), {
                "applied_mtime_ns": None,
                "error": None,
                "observed_mtime_ns": None,
            })

    def test_supervisor_prepare_only_ignores_existing_control_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, _, _ = prepare_execution(root, lab, 4199)
            (home / "ctrl/supervisor").touch()
            previous = Path.cwd()
            try:
                os.chdir(root)
                with patch("labflow.supervisor.Client", side_effect=AssertionError):
                    result = supervisor_main(["--port", "4199", "--prepare-only"])
            finally:
                os.chdir(previous)
            self.assertEqual(result, 0)

    def test_role_permissions_are_stable_across_all_owned_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            (root / "shared").mkdir()
            (root / "scratch").mkdir()
            plan = PLAN.replace(
                'read = ["goals/", "bin/tool", "docs/"]\n'
                'write = ["src/"]\ncommands = []',
                'read = ["goals/", "bin/tool", "docs/", "shared/"]\n'
                'write = ["src/", "scratch/"]\n'
                'commands = ["telora --help", "telora -C *"]',
            )
            (root / "labflow-plan.toml").write_text(plan, encoding="utf-8")
            manifest = load_plan(root / "labflow-plan.toml")
            self.assertEqual(manifest.roles["a1"]["read"], [
                "goals/", "bin/tool", "docs/", "shared/",
            ])
            self.assertEqual(manifest.roles["a1"]["write"], ["src/", "scratch/"])

            lab = parent / "lab"
            lab.mkdir()
            home, _, _ = prepare_execution(root, lab, 4199)
            role = (home / "ws/.opencode/agents/a1.md").read_text(encoding="utf-8")
            observer = (home / "ws/.opencode/agents/lab-ob.md").read_text(encoding="utf-8")
            runtime_config = json.loads((home / "ws/opencode.json").read_text())

            self.assertIn('"shared/**":"allow"', role)
            self.assertIn('"scratch/**":"allow"', role)
            self.assertIn('"telora --help":"allow"', role)
            self.assertIn('"telora -C *":"allow"', role)
            self.assertNotIn('"bin/tool *":"allow"', role)
            self.assertEqual(runtime_config["default_agent"], "lab-ob")
            self.assertIn("只读数据观察员", observer)
            self.assertIn("query *", observer)
            self.assertIn("query-om *", observer)
            self.assertIn("`timeline`", observer)
            self.assertIn("`states.task_records", observer)
            self.assertFalse((home / "ws/.opencode/agents/coordinator.md").exists())
            self.assertFalse((home / "ws/.opencode/commands/ob.md").exists())
            self.assertLess(
                role.index('"src/**":"allow"'),
                role.index('"scratch/**":"allow"'),
            )
            self.assertFalse((home / "roles").exists())

            (root / "labflow-plan.toml").write_text(
                plan + "\n[roles.unknown]\nread = []\nwrite = []\ncommands = ['true']\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(Exception, "unknown role"):
                load_plan(root / "labflow-plan.toml")

    def test_optional_feedback_edge_can_close_an_iteration_loop(self):
        plan = '''
[artifacts."draft.a1"]
goal = "goals/work.md"
requires = ["feedback?"]
assets = ["src/"]

[artifacts.review]
requires = ["draft.a1"]
assets = ["feedback.md"]

[artifacts.feedback]
requires = ["review"]
assets = ["feedback.md"]

[roles.a1]
read = ["goals/work.md", "feedback.md"]
write = ["src/"]
commands = []
'''
        with tempfile.TemporaryDirectory() as temporary:
            root = self.project(Path(temporary))
            (root / "labflow-plan.toml").write_text(plan, encoding="utf-8")
            manifest = load_plan(root / "labflow-plan.toml")
            self.assertIn("draft.a1", manifest.workflow["artifacts"])
            workflow = manifest.workflow
            task = assign_task(root, workflow, "a1", "draft.a1")
            self.assertIsNotNone(task)
            submit(root, workflow, "a1", ["draft.a1"])
            (root / "feedback.md").write_text("revise", encoding="utf-8")
            refresh_artifact(root, workflow, "review")
            refresh_artifact(root, workflow, "feedback")
            draft = evaluate(root, workflow)["artifacts"]["draft.a1"]
            self.assertFalse(draft["current"])
            self.assertTrue(draft["runnable"])

    def test_plan_paths_cannot_alias_the_execution_control_plane(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.project(Path(temporary))
            (root / ".labflow-exec").mkdir()
            (root / "alias").symlink_to(root / ".labflow-exec", target_is_directory=True)
            plan = PLAN.replace('inputs = ["docs/"]', 'inputs = ["alias/"]')
            (root / "labflow-plan.toml").write_text(plan, encoding="utf-8")
            with self.assertRaisesRegex(Exception, "unsafe artifact work.a1 path"):
                load_plan(root / "labflow-plan.toml")

    def test_execution_uses_project_assets_and_hidden_artifact_markers(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, manifest, config = prepare_execution(root, lab, 4199)
            workflow = manifest.workflow

            self.assertEqual(home, root / ".labflow-exec")
            self.assertEqual(config["project_home"], str(root.resolve()))
            refresh_artifact(root, workflow, "tool")
            self.assertTrue((home / "artifacts" / "tool").is_file())
            self.assertFalse((root / "artifacts").exists())
            role = (home / "ws/.opencode/agents/a1.md").read_text(encoding="utf-8")
            self.assertIn('".labflow-exec/**":"deny"', role)
            self.assertIn("你是 Labflow 角色 a1。", role)
            self.assertNotIn("唯一任务", role)
            self.assertIn('"bin/tool":"allow"', role)
            self.assertIn('"docs/**":"allow"', role)
            self.assertIn('"src/**":"allow"', role)
            role_path = home / "ws/.opencode/agents/a1.md"
            inode = role_path.stat().st_ino
            activate_plan(home, manifest)
            self.assertEqual(inode, role_path.stat().st_ino)
            self.assertFalse((home / "roles").exists())
            self.assertFalse((root / "opencode.json").exists())
            self.assertFalse((root / ".opencode").exists())

            assign_task(root, workflow, "a1", "learn.sess.a1")
            submit(root, workflow, "a1", ["learn.sess.a1"])
            assign_task(root, workflow, "a1", "work.a1")
            self.assertFalse(evaluate(root, workflow)["artifacts"]["work.a1"]["current"])
            with self.assertRaisesRegex(Exception, "assets are incomplete"):
                submit(root, workflow, "a1", ["work.a1"])
            (root / "src" / "result.txt").write_text("done", encoding="utf-8")
            submit(root, workflow, "a1", ["work.a1"])
            self.assertTrue(evaluate(root, workflow)["artifacts"]["work.a1"]["current"])
            self.assertFalse((home / "tasks").exists())
            with sqlite3.connect(home / "states.sqlite") as connection:
                count = connection.execute("SELECT count(*) FROM task_records").fetchone()[0]
            self.assertGreaterEqual(count, 2)

    def test_prepare_locally_ignores_execution_control_in_git(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            lab = parent / "lab"
            lab.mkdir()
            prepare_execution(root, lab, 4199)
            result = subprocess.run(
                ["git", "check-ignore", "-q", ".labflow-exec/config.json"], cwd=root,
            )
            self.assertEqual(result.returncode, 0)

    def test_asset_copy_after_rejected_marker_does_not_publish_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, manifest, _ = prepare_execution(root, lab, 4199)
            (root / "bin/tool").unlink()
            marker = home / "artifacts/tool"
            marker.touch()
            supervisor = Supervisor(home, 4199)
            try:
                supervisor.step()
            finally:
                supervisor.close()
            self.assertFalse(marker.exists())

            (root / "bin/tool").write_text("tool", encoding="utf-8")
            supervisor = Supervisor(home, 4199)
            try:
                supervisor.step()
            finally:
                supervisor.close()
            self.assertFalse(evaluate(root, manifest.workflow)["artifacts"]["tool"]["current"])

            marker.touch()
            supervisor = Supervisor(home, 4199)
            try:
                supervisor.step()
            finally:
                supervisor.close()
            self.assertTrue(evaluate(root, manifest.workflow)["artifacts"]["tool"]["current"])

    def test_supervisor_start_prepares_control_plane_without_activating_tasks(self):
        class HealthyBackend:
            def __init__(self, *args, **kwargs):
                pass

            def health(self):
                return {"healthy": True}

        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            previous = Path.cwd()
            try:
                os.chdir(root)
                with patch("labflow.supervisor.Client", HealthyBackend):
                    self.assertEqual(supervisor_main(["--port", "4199", "--once"]), 0)
                home = root / ".labflow-exec"
                config = json.loads((home / "config.json").read_text(encoding="utf-8"))
                self.assertEqual(config["project_home"], str(root))
                self.assertEqual({
                    "OPENCODE_CONFIG": str(root / ".labflow-exec/ws/opencode.json"),
                    "OPENCODE_CONFIG_DIR": str(root / ".labflow-exec/ws/.opencode"),
                }, {
                    "OPENCODE_CONFIG": str(home / "ws/opencode.json"),
                    "OPENCODE_CONFIG_DIR": str(home / "ws/.opencode"),
                })
                self.assertTrue((home / "ctrl").is_dir())
                self.assertFalse((home / "ctrl/active").exists())
                self.assertFalse((home / "ctrl/supervisor").exists())
                self.assertFalse((home / "supervisor-status.json").exists())
                (home / "ctrl/supervisor").touch()
                with patch("labflow.supervisor.Client", HealthyBackend):
                    self.assertEqual(supervisor_main(["--once"]), 0)
                self.assertEqual(status()["executions"][0]["title"], execution_id(root))
                self.assertEqual(pull(0)["tasks"], ["tool"])
            finally:
                os.chdir(previous)
            with patch("labflow.cli_lab.socket.create_connection", side_effect=OSError):
                remove_lab(Path(config["lab_root"]))

    def test_supervisor_generation_exits_when_marker_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, _, _ = prepare_execution(root, lab, 4199)
            marker = home / "ctrl/supervisor"

            for action in ("touch", "delete"):
                with self.subTest(action=action):
                    marker.touch()
                    generation = marker.stat().st_mtime_ns
                    supervisor = Supervisor(home, 4199)
                    calls = []

                    def step():
                        calls.append(action)
                        if action == "touch":
                            os.utime(marker, ns=(generation + 1, generation + 1))
                        else:
                            marker.unlink()

                    try:
                        with patch.object(supervisor, "step", side_effect=step):
                            supervisor.run(
                                control_marker=marker, generation=generation,
                            )
                    finally:
                        supervisor.close()
                    self.assertEqual(calls, [action])

    def test_active_marker_is_the_plan_activation_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, _, _ = prepare_execution(root, lab, 4199)
            supervisor = Supervisor(home, 4199)
            plan = root / "labflow-plan.toml"
            active = home / "ctrl/active"
            try:
                plan.write_text(PLAN + '''

[artifacts.hot]
assets = ["hot.txt"]
''', encoding="utf-8")
                supervisor._sync_active()
                self.assertNotIn("hot", supervisor.manifest.workflow["artifacts"])

                active.touch()
                supervisor._sync_active()
                self.assertTrue(supervisor.active)
                self.assertIn("hot", supervisor.manifest.workflow["artifacts"])
                self.assertIn("hot", load_workflow(root)["artifacts"])

                plan.write_text(plan.read_text(encoding="utf-8") + '''

[artifacts.later]
assets = ["later.txt"]
''', encoding="utf-8")
                supervisor._sync_active()
                self.assertNotIn("later", supervisor.manifest.workflow["artifacts"])

                supervisor.close()
                supervisor = Supervisor(home, 4199)
                self.assertTrue(supervisor.active)
                self.assertNotIn("later", supervisor.manifest.workflow["artifacts"])

                generation = active.stat().st_mtime_ns
                os.utime(active, ns=(generation + 1, generation + 1))
                supervisor._sync_active()
                self.assertIn("later", supervisor.manifest.workflow["artifacts"])

                plan.write_text("invalid = true\n", encoding="utf-8")
                generation = active.stat().st_mtime_ns
                os.utime(active, ns=(generation + 1, generation + 1))
                supervisor._sync_active()
                self.assertFalse(supervisor.active)
                self.assertIsNotNone(supervisor.plan_error)
                self.assertIn("later", supervisor.manifest.workflow["artifacts"])

                supervisor.close()
                supervisor = Supervisor(home, 4199)
                self.assertFalse(supervisor.active)
                self.assertIsNotNone(supervisor.plan_error)
                self.assertIn("later", supervisor.manifest.workflow["artifacts"])

                active.unlink()
                supervisor._sync_active()
                self.assertFalse(supervisor.active)
                self.assertIsNone(supervisor.plan_error)
            finally:
                supervisor.close()

    def test_role_permission_change_supersedes_active_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, manifest, _ = prepare_execution(root, lab, 4199)
            refresh_artifact(root, manifest.workflow, "tool")
            assign_task(root, manifest.workflow, "a1", "learn.sess.a1")
            previous = dag_hash(manifest)

            plan = root / "labflow-plan.toml"
            plan.write_text(
                plan.read_text(encoding="utf-8").replace(
                    'commands = []', 'commands = ["telora *"]',
                ),
                encoding="utf-8",
            )
            (home / "ctrl/active").touch()
            supervisor = Supervisor(home, 4199)
            try:
                supervisor._sync_active()
                current = dag_hash(supervisor.manifest)
            finally:
                supervisor.close()

            self.assertNotEqual(previous, current)
            role = (home / "ws/.opencode/agents/a1.md").read_text(encoding="utf-8")
            self.assertIn('"telora *":"allow"', role)
            self.assertFalse((home / "roles").exists())
            self.assertEqual(task_records(root)["active"], [])
            self.assertEqual(task_records(root)["history"][0]["status"], "stale")

    def test_supervisor_uses_project_databases_and_recovers_root_session(self):
        class FailingWriter:
            def check(self):
                raise RuntimeError("event store unavailable")

            def close(self):
                pass

        class Backend:
            created = 0

            def __init__(self, *args, **kwargs):
                pass

            def create_session(self, *args, **kwargs):
                Backend.created += 1
                return {"id": f"session-{Backend.created}"}

            def health(self):
                return {"healthy": True}

            def sessions(self):
                return []

            def children(self, session_id):
                return []

            def statuses(self):
                return {}

            def session_messages(self, session_id):
                return []

        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, _, _ = prepare_execution(root, lab, 4199)
            (home / "ctrl/active").touch()
            with patch("labflow.supervisor.Client", Backend):
                supervisor = Supervisor(home, 4199)
                try:
                    assert supervisor.writer is not None
                    supervisor.writer.close()
                    supervisor.writer = FailingWriter()
                    supervisor.step()
                finally:
                    supervisor.close()
            self.assertTrue((home / "events.sqlite").is_file())
            self.assertTrue((home / "host-tasks.json").is_file())
            with sqlite3.connect(home / "states.sqlite") as connection:
                value = connection.execute(
                    "SELECT value FROM state WHERE key = 'root_session_id'"
                ).fetchone()[0]
            self.assertEqual(json.loads(value), "session-1")
            self.assertEqual(Backend.created, 2)
            projection = json.loads((home / "supervisor-status.json").read_text())
            self.assertIn("event store unavailable", projection["event_error"])

    def test_disappearing_child_session_does_not_stop_supervisor(self):
        class Backend:
            sessions_by_id = {}
            created = 0
            disappear_id = None

            def __init__(self, *args, **kwargs):
                pass

            def health(self):
                return {"healthy": True}

            def sessions(self):
                return list(Backend.sessions_by_id.values())

            def create_session(self, title, parent_id=None, agent=None):
                Backend.created += 1
                identity = f"session-{Backend.created}"
                value = {"id": identity, "title": title, "agent": agent}
                if parent_id is not None:
                    value["parentID"] = parent_id
                Backend.sessions_by_id[identity] = value
                return value

            def children(self, session_id):
                return [item for item in Backend.sessions_by_id.values()
                        if item.get("parentID") == session_id]

            def statuses(self):
                return {identity: {"type": "idle"} for identity in Backend.sessions_by_id}

            def session_messages(self, session_id):
                if session_id == Backend.disappear_id:
                    Backend.sessions_by_id.pop(session_id)
                    Backend.disappear_id = None
                    raise OpenCodeNotFound("session disappeared", 69)
                return []

            def prompt_session(self, session_id, text, agent=None):
                return None

        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, manifest, _ = prepare_execution(root, lab, 4199)
            refresh_artifact(root, manifest.workflow, "tool")
            (home / "ctrl/active").touch()

            with patch("labflow.supervisor.Client", Backend):
                supervisor = Supervisor(home, 4199)
                try:
                    supervisor.step()
                    old_role = next(
                        identity for identity, session in Backend.sessions_by_id.items()
                        if session.get("agent") == "a1"
                    )
                    Backend.disappear_id = old_role
                    supervisor.step()
                    for _ in range(3):
                        supervisor.step()
                finally:
                    supervisor.close()

            role_sessions = [
                identity for identity, session in Backend.sessions_by_id.items()
                if session.get("agent") == "a1"
            ]
            self.assertEqual(len(role_sessions), 1)
            self.assertNotEqual(role_sessions[0], old_role)

    def test_pause_and_restart_do_not_duplicate_sessions_or_prompts(self):
        class Backend:
            sessions_by_id = {}
            prompts = []
            completed = False
            message_id = "message-1"

            def __init__(self, *args, **kwargs):
                pass

            def health(self):
                return {"healthy": True}

            def sessions(self):
                return list(Backend.sessions_by_id.values())

            def create_session(self, title, parent_id=None, agent=None):
                identity = f"session-{len(Backend.sessions_by_id) + 1}"
                value = {"id": identity, "title": title, "agent": agent}
                if parent_id is not None:
                    value["parentID"] = parent_id
                Backend.sessions_by_id[identity] = value
                return value

            def children(self, session_id):
                return [item for item in Backend.sessions_by_id.values()
                        if item.get("parentID") == session_id]

            def statuses(self):
                return {identity: {"type": "idle"} for identity in Backend.sessions_by_id}

            def session_messages(self, session_id):
                session = Backend.sessions_by_id.get(session_id, {})
                if not Backend.completed or session.get("agent") != "a1":
                    return []
                completed = int(time.time() * 1000)
                return [{
                    "info": {
                        "id": Backend.message_id, "role": "assistant", "finish": "stop",
                        "time": {"created": completed - 1, "completed": completed},
                        "tokens": {"output": 1},
                    },
                    "parts": [{"type": "text", "text": "done"}],
                }]

            def prompt_session(self, session_id, text, agent=None):
                Backend.prompts.append((session_id, agent, text))

        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = self.project(parent)
            lab = parent / "lab"
            lab.mkdir()
            home, manifest, _ = prepare_execution(root, lab, 4199)
            refresh_artifact(root, manifest.workflow, "tool")
            with patch("labflow.supervisor.Client", Backend):
                paused = Supervisor(home, 4199)
                try:
                    paused.step()
                finally:
                    paused.close()
                self.assertEqual(Backend.sessions_by_id, {})

                (home / "ctrl/active").touch()
                first = Supervisor(home, 4199)
                try:
                    first.step()
                finally:
                    first.close()
                self.assertEqual(len(Backend.sessions_by_id), 2)
                self.assertEqual(len(Backend.prompts), 1)
                self.assertIn("# 任务：`learn.sess.a1`", Backend.prompts[0][2])
                self.assertIn("按照 `goals/learn.md` 的要求", Backend.prompts[0][2])
                self.assertIn("- `tool`（已刷新）", Backend.prompts[0][2])
                self.assertIn("- `goals/learn.md`（已更新）", Backend.prompts[0][2])
                self.assertIn("- `bin/tool`（已更新）", Backend.prompts[0][2])
                self.assertNotIn("Learn it.", Backend.prompts[0][2])
                scoped_role = (home / "ws/.opencode/agents/a1.md").read_text(
                    encoding="utf-8"
                )
                self.assertIn('"bin/tool":"allow"', scoped_role)
                self.assertIn('"docs/**":"allow"', scoped_role)
                self.assertIn('"src/**":"allow"', scoped_role)
                role_inode = (home / "ws/.opencode/agents/a1.md").stat().st_ino

                restarted = Supervisor(home, 4199)
                try:
                    restarted.step()
                finally:
                    restarted.close()
            self.assertEqual(len(Backend.sessions_by_id), 2)
            self.assertEqual(len(Backend.prompts), 1)
            Backend.completed = True
            with patch("labflow.supervisor.Client", Backend):
                settling = Supervisor(home, 4199)
                try:
                    settling.step()
                finally:
                    settling.close()
            self.assertTrue((home / "artifacts" / "learn.sess.a1").is_file())
            self.assertFalse((home / "working").exists())
            self.assertEqual(task_records(root)["active"][0]["artifacts"], ["work.a1"])
            work_role = (home / "ws/.opencode/agents/a1.md").read_text(encoding="utf-8")
            self.assertIn('"docs/**":"allow"', work_role)
            self.assertIn('"src/**":"allow"', work_role)
            self.assertIn('"bin/tool":"allow"', work_role)
            self.assertEqual(
                role_inode, (home / "ws/.opencode/agents/a1.md").stat().st_ino,
            )
            Backend.message_id = "message-2"
            with patch("labflow.supervisor.Client", Backend):
                incomplete = Supervisor(home, 4199)
                try:
                    incomplete.step()
                finally:
                    incomplete.close()
            self.assertFalse((home / "artifacts" / "work.a1").exists())
            (root / "src/result.txt").write_text("done", encoding="utf-8")
            Backend.message_id = "message-3"
            with patch("labflow.supervisor.Client", Backend):
                completed = Supervisor(home, 4199)
                try:
                    completed.step()
                finally:
                    completed.close()
            self.assertTrue((home / "artifacts" / "work.a1").is_file())
            idle_role = (home / "ws/.opencode/agents/a1.md").read_text(encoding="utf-8")
            self.assertIn('"docs/**":"allow"', idle_role)
            self.assertIn('"src/**":"allow"', idle_role)
            self.assertEqual(
                role_inode, (home / "ws/.opencode/agents/a1.md").stat().st_ino,
            )


if __name__ == "__main__":
    unittest.main()
