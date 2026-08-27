from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path

from labflow.task_cli import (
    TaskError, assign_task, clear_session_qualifications, evaluate, load_workflow,
    parser, refresh_artifact, remove_artifact, restore_artifacts,
    role_asset_permissions, submit, task_records, validate_workflow,
)


def artifact_workflow() -> dict:
    return validate_workflow({
        "schema": "labflow.workflow/v1",
        "roles": ["a1", "a2"],
        "artifacts": {
            "input-0": {
                "desc": "Initial input",
                "assets": [{"path": "guide/", "level": 0}],
            },
            "input-optional": {
                "desc": "Optional input",
                "assets": [{"path": "notes.txt", "level": 1}],
            },
            "output-1.a1": {
                "desc": "First output",
                "input": ["input-0", "input-optional?"],
                "assets": [{"path": "result-1.txt", "level": 2}],
                "instruction": "Create result-1.txt",
            },
            "output-2.a2": {
                "desc": "Second output", "input": ["output-1.a1"],
                "assets": ["result-2.txt"], "instruction": "Create result-2.txt",
            },
            "output-3": {
                "desc": "Final output", "input": ["output-1.a1", "output-2.a2"],
            },
        },
    })


class ArtifactWorkflowTest(unittest.TestCase):
    def prepare(self, root: Path) -> dict:
        (root / "guide").mkdir()
        (root / "guide" / "GOAL.md").write_text("goal", encoding="utf-8")
        value = artifact_workflow()
        raw = json.loads(json.dumps(value))
        for artifact in raw["artifacts"].values():
            artifact.pop("owner")
        (root / "experiment.json").write_text(json.dumps({"workflow": raw}), encoding="utf-8")
        return value

    def test_assignment_lists_complete_inputs_and_assets_with_change_flags(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = self.prepare(root)
            self.assertEqual(load_workflow(root), value)
            refresh_artifact(root, value, "input-0")

            first = assign_task(root, value, "a1", "output-1.a1")
            assert first is not None
            self.assertEqual(first["target"], {
                "name": "output-1.a1", "instruction": "Create result-1.txt",
            })
            self.assertEqual(first["inputs"], [
                {"name": "input-0", "fresh": True},
                {"name": "input-optional", "fresh": None},
            ])
            self.assertEqual(first["assets"], [{"path": "guide/", "updated": True}])

            (root / "result-1.txt").write_text("one", encoding="utf-8")
            submit(root, value, "a1", ["output-1.a1"])
            (root / "notes.txt").write_text("new input", encoding="utf-8")
            refresh_artifact(root, value, "input-optional")

            second = assign_task(root, value, "a1", "output-1.a1")
            assert second is not None
            self.assertEqual(second["inputs"], [
                {"name": "input-0", "fresh": False},
                {"name": "input-optional", "fresh": True},
            ])
            self.assertEqual(second["assets"], [
                {"path": "guide/", "updated": False},
                {"path": "notes.txt", "updated": True},
            ])

    def test_shared_asset_is_deduplicated(self):
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1", "roles": ["a1"],
            "artifacts": {
                "left": {"desc": "left", "assets": ["shared.txt"]},
                "right": {"desc": "right", "assets": ["shared.txt"]},
                "work.a1": {"desc": "work", "input": ["left", "right"],
                         "instruction": "work"},
                "done": {"desc": "done", "input": ["work.a1"]},
            },
        })
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "shared.txt").write_text("shared", encoding="utf-8")
            refresh_artifact(root, workflow, "left")
            refresh_artifact(root, workflow, "right")
            result = assign_task(root, workflow, "a1", "work.a1")
            assert result is not None
            self.assertEqual(result["assets"], [{"path": "shared.txt", "updated": True}])

    def test_role_permissions_are_derived_from_owned_and_input_assets(self):
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1", "roles": ["a1", "a2"],
            "artifacts": {
                "input.a2": {"desc": "input", "assets": ["model/"],
                             "instruction": "produce input"},
                "output.a1": {"desc": "output", "input": ["input.a2"],
                              "assets": ["result.json"], "instruction": "produce output"},
            },
        })
        self.assertEqual(role_asset_permissions(workflow, "a1"), {
            "read": ["result.json", "model/"],
            "write": ["result.json"],
        })
        self.assertEqual(role_asset_permissions(workflow, "a2"), {
            "read": ["model/"], "write": ["model/"],
        })

    def test_assignment_returns_none_when_target_is_not_runnable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = self.prepare(root)
            self.assertIsNone(assign_task(root, value, "a2", "output-2.a2"))

    def test_assignment_completes_one_target_at_a_time(self):
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1", "roles": ["a1"],
            "artifacts": {
                "input": {"desc": "input"},
                "first.a1": {"desc": "first", "input": ["input"],
                          "assets": ["first.txt"], "instruction": "first"},
                "second.a1": {"desc": "second", "input": ["input"],
                           "assets": ["second.txt"], "instruction": "second"},
                "finish": {"desc": "finish", "input": ["first.a1", "second.a1"]},
            },
        })
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh_artifact(root, workflow, "input")
            first = assign_task(root, workflow, "a1", "first.a1")
            assert first is not None
            self.assertEqual(first["target"], {"name": "first.a1", "instruction": "first"})
            (root / "first.txt").write_text("first", encoding="utf-8")
            submit(root, workflow, "a1", ["first.a1"])
            second = assign_task(root, workflow, "a1", "second.a1")
            assert second is not None
            self.assertEqual(second["target"], {
                "name": "second.a1", "instruction": "second",
            })

    def test_assignment_reuses_active_task_when_another_target_is_preferred(self):
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1", "roles": ["a1"],
            "artifacts": {
                "input": {"desc": "input"},
                "first.a1": {"desc": "first", "input": ["input"],
                             "instruction": "first"},
                "second.a1": {"desc": "second", "input": ["input"],
                              "instruction": "second"},
            },
        })
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh_artifact(root, workflow, "input")
            first = assign_task(root, workflow, "a1", "first.a1")
            assert first is not None
            task_id = task_records(root)["active"][0]["task_id"]

            reused = assign_task(root, workflow, "a1", "second.a1")

            assert reused is not None
            self.assertEqual(reused["target"], {
                "name": "first.a1", "instruction": "first",
            })
            self.assertEqual(task_records(root)["active"][0]["task_id"], task_id)

    def test_ownership_and_asset_types_are_enforced(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = self.prepare(root)
            with self.assertRaisesRegex(TaskError, "role-owned"):
                refresh_artifact(root, value, "output-1.a1")
            refresh_artifact(root, value, "input-0")
            assign_task(root, value, "a1", "output-1.a1")
            (root / "result-1.txt").mkdir()
            with self.assertRaisesRegex(TaskError, "assets are incomplete"):
                submit(root, value, "a1", ["output-1.a1"])

    def test_host_force_refresh_supersedes_active_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = self.prepare(root)
            refresh_artifact(root, value, "input-0")
            assign_task(root, value, "a1", "output-1.a1")
            (root / "result-1.txt").write_text("host supplied", encoding="utf-8")
            result = refresh_artifact(root, value, "output-1.a1", force=True)
            self.assertTrue(result["host_forced"])
            self.assertEqual(task_records(root)["history"][0]["status"], "stale")

    def test_remove_and_restore_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = self.prepare(root)
            refresh_artifact(root, value, "input-0")
            self.assertTrue(remove_artifact(root, value, "input-0")["removed"])
            self.assertFalse(evaluate(root, value)["artifacts"]["input-0"]["current"])
            (root / "result-1.txt").write_text("inherited", encoding="utf-8")
            restored = restore_artifacts(root, value, ["input-0", "output-1.a1"])
            self.assertEqual([item["artifact"] for item in restored], ["input-0", "output-1.a1"])
            self.assertEqual(task_records(root), {"active": [], "history": []})

    def test_session_qualification_gates_work_without_invalidating_output(self):
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1", "roles": ["a1"],
            "artifacts": {
                "language": {"desc": "language"},
                "learn.sess.a1": {
                    "desc": "learn", "input": ["language"], "instruction": "learn",
                },
                "work.a1": {
                    "desc": "work", "input": ["language", "learn.sess.a1"],
                    "assets": ["work.txt"], "instruction": "work",
                },
                "done": {"desc": "done", "input": ["work.a1"]},
            },
        })
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh_artifact(root, workflow, "language")
            status = evaluate(root, workflow)["artifacts"]
            self.assertTrue(status["learn.sess.a1"]["runnable"])
            self.assertFalse(status["work.a1"]["runnable"])
            self.assertEqual(status["work.a1"]["missing_qualifications"], [
                "learn.sess.a1",
            ])

            assign_task(root, workflow, "a1", "learn.sess.a1")
            submit(root, workflow, "a1", ["learn.sess.a1"])
            self.assertTrue(evaluate(root, workflow)["artifacts"]["work.a1"]["runnable"])
            (root / "work.txt").write_text("done", encoding="utf-8")
            assign_task(root, workflow, "a1", "work.a1")
            submit(root, workflow, "a1", ["work.a1"])

            self.assertEqual(clear_session_qualifications(root, workflow, "a1"), [
                "learn.sess.a1",
            ])
            status = evaluate(root, workflow)["artifacts"]
            self.assertFalse(status["learn.sess.a1"]["current"])
            self.assertTrue(status["work.a1"]["current"])
            self.assertTrue(status["done"]["submittable"])
            with self.assertRaisesRegex(TaskError, "cannot restore session qualifications"):
                restore_artifacts(root, workflow, ["learn.sess.a1"])

            remove_artifact(root, workflow, "work.a1", force=True)
            self.assertTrue(refresh_artifact(
                root, workflow, "work.a1", force=True,
            )["host_forced"])
            with self.assertRaisesRegex(TaskError, "cannot refresh a session qualification"):
                refresh_artifact(root, workflow, "learn.sess.a1", force=True)

    def test_changed_inputs_supersede_active_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = self.prepare(root)
            refresh_artifact(root, value, "input-0")
            assign_task(root, value, "a1", "output-1.a1")
            first = task_records(root)["active"][0]["task_id"]
            (root / "notes.txt").write_text("new", encoding="utf-8")
            refresh_artifact(root, value, "input-optional")
            assign_task(root, value, "a1", "output-1.a1")
            second = task_records(root)["active"][0]["task_id"]
            self.assertNotEqual(first, second)
            self.assertEqual(task_records(root)["history"][0]["status"], "stale")

    def test_validation_rejects_bad_owner_assets_and_graph(self):
        base = {
            "schema": "labflow.workflow/v1", "roles": ["a1"],
            "artifacts": {
                "start": {"desc": "start"},
                "work.a1": {"desc": "work", "input": ["missing?"],
                         "instruction": "work"},
                "finish": {"desc": "finish", "input": ["work.a1"]},
            },
        }
        with self.assertRaisesRegex(TaskError, "unknown input"):
            validate_workflow(base)
        base["artifacts"]["work.a1"]["input"] = ["finish"]
        with self.assertRaisesRegex(TaskError, "dependency cycle"):
            validate_workflow(base)
        base["artifacts"]["work.a1"]["input"] = ["start"]
        base["artifacts"]["work.a1"]["assets"] = [{"path": "out/", "level": 3}]
        with self.assertRaisesRegex(TaskError, "level must be 0, 1, or 2"):
            validate_workflow(base)
        base["artifacts"]["work.a1"]["assets"] = []
        base["artifacts"]["start"]["owner"] = "host"
        with self.assertRaisesRegex(TaskError, "unknown artifact start key.*owner"):
            validate_workflow(base)

    def test_validation_enforces_names_and_session_qualification_edges(self):
        validate_workflow({
            "schema": "labflow.workflow/v1", "roles": ["a1"],
            "artifacts": {"input-0": {"desc": "valid numeric second segment"}},
        })
        for name in ("Input", "input_0", "0-input", "input-", ".input", "input..value"):
            with self.subTest(name=name), self.assertRaisesRegex(TaskError, "invalid artifact id"):
                validate_workflow({
                    "schema": "labflow.workflow/v1", "roles": ["a1"],
                    "artifacts": {name: {"desc": "bad"}},
                })
        with self.assertRaisesRegex(TaskError, "invalid workflow roles"):
            validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["team.a1"],
                "artifacts": {"input": {"desc": "input"}},
            })
        with self.assertRaisesRegex(TaskError, "can only gate artifacts owned by a1"):
            validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["a1", "a2"],
                "artifacts": {
                    "learn.sess.a1": {"desc": "learn", "instruction": "learn"},
                    "work.a2": {"desc": "work", "input": ["learn.sess.a1"],
                                "instruction": "work"},
                },
            })
        with self.assertRaisesRegex(TaskError, "cannot be optional"):
            validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["a1"],
                "artifacts": {
                    "learn.sess.a1": {"desc": "learn", "instruction": "learn"},
                    "work.a1": {"desc": "work", "input": ["learn.sess.a1?"],
                                "instruction": "work"},
                },
            })

    def test_cli_exposes_status_and_benchmark_agent_commands(self):
        self.assertEqual(parser().parse_args(["status"]).command, "status")
        self.assertEqual(parser().parse_args(["start-problem", "0001"]).problem, "0001")
        self.assertEqual(parser().parse_args(["end-problem", "error"]).outcome, "error")
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parser().parse_args(["mark-done", "a1", "output-1"])
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parser().parse_args(["pull", "a1"])
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parser().parse_args(["submit", "a1", "output-1.a1"])


if __name__ == "__main__":
    unittest.main()
