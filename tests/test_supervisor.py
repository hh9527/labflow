from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from labflow.config import ControlError
from labflow.events import event_detail, project_events
from labflow.state import SCHEMA, bind_plan, save_state
from labflow.supervisor import (
    EffectState, LifecycleEvent, Supervisor, SupervisorState, reduce, supervisor_lock,
)
from labflow.task_cli import (
    assign_task, refresh_artifact, submit, task_records, validate_workflow,
    workflow_status,
)
from labflow.timeline_projection import closed_message_events
from labflow.timeline_store import TimelineWriter, read, statistics


class ReducerTest(unittest.TestCase):
    def execution(self) -> LifecycleEvent:
        return LifecycleEvent("execution_updated", "plan@1", {
            "workspace": "/tmp/ws",
            "root_session_id": "ses_root",
            "active": True,
            "dag": True,
            "roles": ["a1"],
        })

    def test_mutates_state_and_deduplicates_create_and_prompt_effects(self):
        state = SupervisorState()

        created = reduce(state, self.execution())
        self.assertEqual(created, [])
        reduce(state, LifecycleEvent("observed_sessions_updated", "plan@1", {
            "sessions": [],
        }))
        reduce(state, self.execution())
        created = reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        ))
        self.assertEqual([effect.kind for effect in created], ["create_session"])
        self.assertEqual(reduce(state, self.execution()), [])

        reduce(state, LifecycleEvent("workflow_observed", "plan@1", {
            "runnable": {"a1": [("output.a1", 11)]},
            "requests": ["approval"],
            "optional_requests": ["feedback"],
        }))
        prompted = reduce(state, LifecycleEvent("effect_succeeded", "plan@1", {
            "key": created[0].key,
            "effect_kind": "create_session",
            "role": "a1",
            "title": "a1",
            "backend_id": "ses_a1",
        }))

        self.assertEqual([effect.kind for effect in prompted], ["prompt_session"])
        self.assertEqual(state.executions["plan@1"].requests, ("approval",))
        self.assertEqual(state.executions["plan@1"].optional_requests, ("feedback",))

        reduce(state, LifecycleEvent("effect_succeeded", "plan@1", {
            "key": prompted[0].key,
            "effect_kind": "prompt_session",
            "role": "a1",
            "title": "a1",
            "backend_id": "ses_a1",
        }))
        # OpenCode can briefly remain idle after accepting prompt_async.
        repeated = reduce(state, LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "idle",
        }))
        self.assertEqual(repeated, [])

    def test_timeline_only_execution_never_creates_sessions(self):
        state = SupervisorState()
        event = self.execution()
        event = LifecycleEvent(event.kind, event.execution, {**event.data, "dag": False})
        self.assertEqual(reduce(state, event), [])

    def test_disabling_dag_clears_pressure_and_host_requests(self):
        state = SupervisorState()
        reduce(state, self.execution())
        reduce(state, LifecycleEvent("observed_sessions_updated", "plan@1", {
            "sessions": [],
        }))
        reduce(state, LifecycleEvent("workflow_observed", "plan@1", {
            "runnable": {"a1": [("output.a1", 11)]},
            "requests": ["approval"], "optional_requests": ["feedback"],
        }))
        disabled = self.execution()
        disabled = LifecycleEvent(disabled.kind, disabled.execution, {
            **disabled.data, "dag": False,
        })

        self.assertEqual(reduce(state, disabled), [])
        execution = state.executions["plan@1"]
        self.assertEqual(execution.runnable, {})
        self.assertEqual(execution.requests, ())
        self.assertEqual(execution.optional_requests, ())

    def test_deleting_execution_forgets_only_its_effects(self):
        state = SupervisorState()
        reduce(state, self.execution())
        reduce(state, LifecycleEvent("observed_sessions_updated", "plan@1", {
            "sessions": [],
        }))
        created = reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        ))[0]
        state.effects["other"] = EffectState("succeeded", "another@1")

        reduce(state, LifecycleEvent("execution_deleted", "plan@1"))

        self.assertNotIn("plan@1", state.executions)
        self.assertNotIn(created.key, state.effects)
        self.assertIn("other", state.effects)

    def test_failed_effect_marks_role_failed(self):
        state = SupervisorState()
        reduce(state, self.execution())
        reduce(state, LifecycleEvent("observed_sessions_updated", "plan@1", {
            "sessions": [],
        }))
        created = reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        ))[0]

        reduce(state, LifecycleEvent("effect_failed", "plan@1", {
            "key": created.key, "effect_kind": "create_session", "role": "a1",
            "error": "backend unavailable",
        }))

        self.assertEqual(state.effects[created.key].status, "failed")
        self.assertEqual(state.executions["plan@1"].sessions["a1"].status, "failed")

    def test_disappeared_session_gets_a_new_create_epoch(self):
        state = SupervisorState()
        reduce(state, self.execution())
        reduce(state, LifecycleEvent("observed_sessions_updated", "plan@1", {
            "sessions": [],
        }))
        first = reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        ))[0]
        reduce(state, LifecycleEvent("effect_succeeded", "plan@1", {
            "key": first.key, "effect_kind": "create_session", "role": "a1",
            "title": "a1", "backend_id": "ses_a1",
        }))
        reduce(state, LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1",
            "status": "idle",
        }))

        second = reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        ))

        self.assertEqual([effect.kind for effect in second], ["create_session"])
        self.assertNotEqual(first.key, second[0].key)

    def test_created_session_is_not_duplicated_before_backend_visibility(self):
        state = SupervisorState()
        reduce(state, self.execution())
        reduce(state, LifecycleEvent("observed_sessions_updated", "plan@1", {
            "sessions": [],
        }))
        created = reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        ))[0]
        reduce(state, LifecycleEvent("effect_succeeded", "plan@1", {
            "key": created.key, "effect_kind": "create_session", "role": "a1",
            "title": "a1", "backend_id": "ses_a1",
        }))

        self.assertEqual(reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        )), [])
        self.assertEqual(state.executions["plan@1"].sessions["a1"].backend_id,
                         "ses_a1")

    def test_completed_turn_recovers_when_busy_transition_was_missed(self):
        state = SupervisorState()
        reduce(state, self.execution())
        reduce(state, LifecycleEvent("observed_sessions_updated", "plan@1", {
            "sessions": [],
        }))
        created = reduce(state, LifecycleEvent(
            "session_missing", "plan@1", {"role": "a1"},
        ))[0]
        reduce(state, LifecycleEvent("workflow_observed", "plan@1", {
            "runnable": {"a1": [("output.a1", 11)]},
        }))
        prompted = reduce(state, LifecycleEvent("effect_succeeded", "plan@1", {
            "key": created.key, "effect_kind": "create_session", "role": "a1",
            "title": "a1", "backend_id": "ses_a1",
        }))[0]
        reduce(state, LifecycleEvent("effect_succeeded", "plan@1", {
            "key": prompted.key, "effect_kind": "prompt_session", "role": "a1",
            "title": "a1", "backend_id": "ses_a1",
        }))

        retried = reduce(state, LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1",
            "status": "idle", "completed_turn": True,
        }))

        self.assertEqual(retried, [])
        retried = reduce(state, LifecycleEvent("workflow_observed", "plan@1", {
            "runnable": {"a1": [("output.a1", 11)]},
        }))
        self.assertEqual([effect.kind for effect in retried], ["prompt_session"])
        self.assertNotEqual(prompted.key, retried[0].key)


class SupervisorOwnershipTest(unittest.TestCase):
    def test_only_one_supervisor_can_own_a_laboratory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with supervisor_lock(root):
                with self.assertRaisesRegex(ControlError, "already owns"):
                    with supervisor_lock(root):
                        self.fail("second Supervisor unexpectedly acquired the Lab")

            with supervisor_lock(root):
                pass


class TimelineProjectionTest(unittest.TestCase):
    def message(self) -> dict:
        return {
            "info": {
                "id": "msg_1", "role": "assistant", "finish": "stop",
                "time": {"created": 1000, "completed": 2000},
                "tokens": {
                    "input": 7, "output": 3, "reasoning": 5,
                    "cache": {"read": 2, "write": 1},
                },
            },
            "parts": [
                {"type": "reasoning", "text": "must not be stored"},
                {"id": "tool_1", "type": "tool", "tool": "bash", "state": {
                    "status": "completed", "input": {"command": "just validate --all"},
                    "metadata": {"exit": 0}, "time": {"start": 1200, "end": 1400},
                }},
                {"type": "text", "text": "Completed."},
            ],
        }

    def test_projects_only_closed_compact_events(self):
        events = closed_message_events("plan@1", "a1", "a1", self.message())

        self.assertEqual([event["type"] for event in events],
                         ["turn_started", "action", "thinking", "thinking",
                          "reply", "turn_ended"])
        action = next(event for event in events if event["type"] == "action")
        self.assertEqual((action["action"], action["success"], action["command"]),
                         ("shell", True, "just validate --all"))
        self.assertEqual((action["at"], action["duration"]), (1200, 200))
        reply = next(event for event in events if event["type"] == "reply")
        self.assertEqual((reply["tokens"], reply["reasoning_tokens"]), (3, 5))
        self.assertNotIn("must not be stored", repr(events))

    def test_structured_write_reports_paths_and_failure(self):
        message = self.message()
        message["parts"][1] = {
            "id": "tool_2", "type": "tool", "tool": "write", "state": {
                "status": "error", "input": {"filePath": "result/output.json"},
                "output": "permission denied", "time": {"start": 1200, "end": 1250},
            },
        }
        action = next(
            event for event in closed_message_events("plan@1", "a1", "a1", message)
            if event["type"] == "action"
        )
        self.assertEqual(action["paths"], ["result/output.json"])
        self.assertFalse(action["success"])

    def test_incomplete_message_and_action_are_not_projected(self):
        message = self.message()
        message["info"]["time"].pop("completed")
        message["parts"][1]["state"]["status"] = "running"
        message["parts"][1]["state"]["time"].pop("end")

        self.assertEqual(closed_message_events("plan@1", "a1", "a1", message), [])


class TimelineStoreTest(unittest.TestCase):
    def test_writer_appends_and_deduplicates_without_reader_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "db.sqlite3"
            record = {
                "id": "action:1", "execution": "plan@1", "session": "a1",
                "role": "a1", "type": "action", "at": 100, "duration": 20,
                "action": "write", "success": True, "paths": ["out.json"],
            }
            writer = TimelineWriter(path, batch_size=2, flush_seconds=.01)
            writer.submit([record, record])
            writer.close()

            values = read(path, "plan@1")
            self.assertEqual(len(values), 1)
            self.assertEqual(values[0]["paths"], ["out.json"])
            self.assertTrue(values[0]["success"])
            with sqlite3.connect(path) as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM action_paths"
                ).fetchone()[0], 1)

    def test_host_projection_reads_the_laboratory_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            writer = TimelineWriter(root / "db.sqlite3", flush_seconds=.01)
            writer.submit([{
                "id": "reply:plan@1:a1:msg:complete",
                "execution": "plan@1", "session": "a1", "role": "a1",
                "type": "reply", "at": 200, "duration": 0, "tokens": 3,
                "summary": "done",
            }])
            writer.close()
            workspace = root / "ws"
            workspace.mkdir()
            context = mock.Mock(root=root / "control", state={
                "lab_root": str(root), "title": "plan@1", "workspace": str(workspace),
            })

            events = project_events(context, 100)
            detail = event_detail(context, events[0]["id"])

            self.assertEqual(events[0]["session"], "a1")
            self.assertEqual(detail["detail"]["tokens"], 3)

    def test_host_projection_merges_timeline_and_operational_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "ws"
            workspace.mkdir()
            artifact_event = {
                "schema": "labflow.artifact-event/v1", "number": 1,
                "artifact": "input", "reason": "refresh", "mtime_ns": 150_000_000,
            }
            writer = TimelineWriter(root / "db.sqlite3", flush_seconds=.01)
            writer.submit([{
                "id": "reply:plan@1:a1:msg:complete",
                "execution": "plan@1", "session": "a1", "role": "a1",
                "type": "reply", "at": 200, "duration": 0, "summary": "done",
            }])
            writer.close()
            context = mock.Mock(
                root=root / "control",
                state={"lab_root": str(root), "title": "plan@1",
                       "workspace": str(workspace), "artifact_events": [artifact_event]},
            )

            events = project_events(context, 100)

            self.assertEqual([event["type"] for event in events], ["reply"])
            context.client.assert_not_called()

    def test_unicode_timeline_identity_can_be_read_by_event_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "ws"
            workspace.mkdir()
            event_id = "reply:plan@1:启动 a1:msg:complete"
            writer = TimelineWriter(root / "db.sqlite3", flush_seconds=.01)
            writer.submit([{
                "id": event_id, "execution": "plan@1", "session": "启动 a1",
                "role": "a1", "type": "reply", "at": 200, "duration": 0,
            }])
            writer.close()
            context = mock.Mock(
                root=root / "control",
                state={"lab_root": str(root), "title": "plan@1",
                       "workspace": str(workspace)},
            )

            detail = event_detail(context, event_id)

            self.assertEqual(detail["detail"]["session"], "启动 a1")

    def test_statistics_aggregate_timing_tokens_commands_and_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "db.sqlite3"
            writer = TimelineWriter(path, flush_seconds=.01)
            writer.submit([
                {"id": "t1", "execution": "plan@1", "session": "a1", "role": "a1",
                 "type": "thinking", "at": 10, "duration": 30, "tokens": 5},
                {"id": "a1", "execution": "plan@1", "session": "a1", "role": "a1",
                 "type": "action", "at": 40, "duration": 20, "action": "shell",
                 "success": False, "command": "just check", "exit_code": 1},
                {"id": "r1", "execution": "plan@1", "session": "a1", "role": "a1",
                 "type": "reply", "at": 60, "duration": 0, "tokens": 3,
                 "input_tokens": 7, "output_tokens": 3, "reasoning_tokens": 5},
            ])
            writer.close()

            value = statistics(path, "plan@1")

            self.assertEqual(value["sessions"][0]["longest_thinking_ms"], 30)
            self.assertEqual(value["sessions"][0]["action_failures"], 1)
            self.assertEqual(value["commands"][0], {
                "command": "just check", "count": 1, "duration": 20, "failures": 1,
            })


class SupervisorRuntimeTest(unittest.TestCase):
    def _state(self, root: Path, title: str, workspace: Path,
               workflow: dict | None, execution: dict) -> None:
        control = bind_plan(root, "demo", title)
        (control / "active").touch()
        workspace.mkdir(parents=True, exist_ok=True)
        if workflow is not None:
            (control / "artifacts").mkdir(exist_ok=True)
        save_state(control, {
            "schema": SCHEMA, "plan_id": "demo", "title": title,
            "phase": "active", "workspace": str(workspace),
            "session_id": "ses_root", "workflow": workflow,
            "execution": execution,
        })

    def _dag(self, root: Path, workspace: Path) -> dict:
        workflow = validate_workflow({
            "schema": "labflow.workflow/v1", "roles": ["a1"],
            "artifacts": {
                "input": {"desc": "input"},
                "output.a1": {"desc": "output", "input": ["input"],
                              "instruction": "build"},
            },
        })
        self._state(root, "demo@1", workspace, workflow, {"kind": "dag-mode"})
        refresh_artifact(workspace, workflow, "input")
        return workflow

    def test_observe_only_goal_collects_existing_sessions_without_effects(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "bench@1" / "ws"
            workspace.mkdir(parents=True)
            self._state(root, "bench@1", workspace, None, {
                "kind": "benchmark-mode", "questioner": "q",
            })
            message = {
                "info": {"id": "msg", "role": "assistant",
                         "time": {"created": 10, "completed": 20},
                         "tokens": {"output": 1}},
                "parts": [{"type": "text", "text": "done"}],
            }
            client = mock.Mock()
            client.statuses.return_value = {"ses_root": {"type": "idle"}}
            client.children.return_value = []
            client.sessions.return_value = [{
                "id": "ses_root", "title": "bench@1", "agent": "q",
            }]
            client.session_messages.return_value = [message]
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step(); supervisor.step()
            finally:
                supervisor.close()

            self.assertEqual(len(read(root / "db.sqlite3", "bench@1")), 5)
            client.create_session.assert_not_called()
            status = json.loads((root / "supervisor-status.json").read_text())
            self.assertEqual(status["executions"][0]["title"], "bench@1")

    def test_materializes_host_tasks_from_workflow_projection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            desired = root / "exec" / "demo@1" / "artifacts"
            desired.mkdir(parents=True)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["a1"],
                "artifacts": {
                    "input": {"desc": "input"},
                    "approval": {"desc": "approval", "input": ["input"]},
                    "feedback": {"desc": "feedback"},
                    "output.a1": {
                        "desc": "output", "input": ["feedback?"],
                        "instruction": "build output",
                    },
                },
            })
            self._state(root, "demo@1", workspace, workflow, {"kind": "dag-mode"})
            refresh_artifact(workspace, workflow, "input")
            client = mock.Mock()
            client.statuses.return_value = {
                "ses_root": {"type": "idle"}, "ses_a1": {"type": "idle"},
            }
            client.children.return_value = [{
                "id": "ses_a1", "title": "a1", "agent": "a1",
            }]
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    path = root / "host-task.json"
                    modified = path.stat().st_mtime_ns
                    supervisor.step()
                    self.assertEqual(path.stat().st_mtime_ns, modified)
            finally:
                supervisor.close()

            tasks = json.loads(path.read_text())
            self.assertEqual(tasks["demo@1"], {
                "tasks": ["approval"], "optional_tasks": ["feedback"],
            })

    def test_benchmark_detached_sessions_are_observed_and_collected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "bench@1" / "ws"
            workspace.mkdir(parents=True)
            control = bind_plan(root, "demo", "bench@1")
            (control / "active").touch()
            save_state(control, {
                "schema": SCHEMA, "plan_id": "demo", "title": "bench@1",
                "phase": "idle", "workspace": str(workspace),
                "session_id": "ses_root", "workflow": None,
                "execution": {
                    "kind": "benchmark-mode", "questioner": "q", "answerer": "a",
                },
                "benchmark": {
                    "sessions": [{"id": "ses_q", "agent": "q"}],
                    "problems": [{
                        "questioner_session_id": "ses_q",
                        "answerer_session_id": "ses_a",
                    }],
                },
            })
            message = {
                "info": {"id": "msg", "role": "assistant",
                         "time": {"created": 10, "completed": 20},
                         "tokens": {"output": 1}},
                "parts": [{"type": "text", "text": "done"}],
            }
            client = mock.Mock()
            client.statuses.return_value = {}
            client.children.return_value = []
            client.sessions.return_value = [
                {"id": "ses_root", "title": "bench@1", "agent": "q"},
                {"id": "ses_q", "title": "bench@1.batch-1.q", "agent": "q"},
                {"id": "ses_a", "title": "bench@1.batch-1.a", "agent": "a"},
            ]
            client.session_messages.return_value = [message]
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
            finally:
                supervisor.close()

            events = read(root / "db.sqlite3", "bench@1")
            self.assertEqual({event["session"] for event in events}, {
                "bench@1", "bench@1.batch-1.q", "bench@1.batch-1.a",
            })
            status = json.loads((root / "supervisor-status.json").read_text())
            self.assertEqual(len(status["executions"][0]["sessions"]), 3)

    def test_timeline_only_duplicate_roles_do_not_fail_or_create_effects(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            self._state(root, "demo@1", workspace, {
                "schema": "labflow.workflow/v1", "roles": ["a1"], "artifacts": {},
            }, {"kind": "dag-mode"})
            (root / "exec" / "demo@1" / "artifacts").rmdir()
            client = mock.Mock()
            client.statuses.return_value = {}
            client.children.side_effect = lambda session_id: ([
                {"id": "ses_a1_1", "title": "a1-one", "agent": "a1"},
                {"id": "ses_a1_2", "title": "a1-two", "agent": "a1"},
            ] if session_id == "ses_root" else [])
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
            finally:
                supervisor.close()

            self.assertEqual(supervisor.state.effects, {})
            self.assertEqual(supervisor.state.executions["demo@1"].sessions, {})
            status = json.loads((root / "supervisor-status.json").read_text())
            self.assertEqual({item["status"] for item in
                              status["executions"][0]["sessions"]}, {"idle"})

    def test_restarting_once_does_not_duplicate_timeline_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "bench@1" / "ws"
            workspace.mkdir(parents=True)
            self._state(root, "bench@1", workspace, None, {
                "kind": "benchmark-mode", "questioner": "q",
            })
            message = {
                "info": {"id": "msg", "role": "assistant",
                         "time": {"created": 10, "completed": 20},
                         "tokens": {"output": 1}},
                "parts": [{"type": "text", "text": "done"}],
            }
            client = mock.Mock()
            client.statuses.return_value = {}
            client.children.return_value = []
            client.sessions.return_value = []
            client.session_messages.return_value = [message]
            with mock.patch("labflow.supervisor.Client", return_value=client):
                for _ in range(2):
                    supervisor = Supervisor(root, 4199)
                    try:
                        supervisor.step()
                    finally:
                        supervisor.close()

            self.assertEqual(len(read(root / "db.sqlite3", "bench@1")), 5)

    def test_dag_goal_creates_and_prompts_one_missing_runnable_role(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            self._dag(root, workspace)
            client = mock.Mock()
            client.statuses.return_value = {"ses_root": {"type": "idle"}}
            client.children.return_value = []
            client.session_messages.return_value = []
            client.create_session.return_value = {"id": "ses_a1"}
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
            finally:
                supervisor.close()

            client.create_session.assert_called_once_with(
                "a1", parent_id="ses_root", agent="a1",
            )
            client.prompt_session.assert_called_once()
            prompt = client.prompt_session.call_args.args[1]
            self.assertIn("`output.a1`", prompt)
            self.assertIn("Supervisor 负责校验资产、结算 Artifact", prompt)
            self.assertTrue((root / "exec" / "demo@1" / "working" / "a1").is_file())
            self.assertIn("input", supervisor.state.executions["demo@1"].artifacts)

    def test_completed_turn_is_settled_by_supervisor(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            workflow = self._dag(root, workspace)
            client = mock.Mock()
            client.statuses.return_value = {"ses_a1": {"type": "idle"}}
            client.children.side_effect = lambda session_id: ([{
                "id": "ses_a1", "title": "a1", "agent": "a1",
            }] if session_id == "ses_root" else [])
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            working = root / "exec" / "demo@1" / "working" / "a1"
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    completed = working.stat().st_mtime_ns // 1_000_000 + 1
                    client.session_messages.return_value = [{
                        "info": {
                            "id": "msg_done", "role": "assistant", "finish": "stop",
                            "time": {"created": completed - 1, "completed": completed},
                            "tokens": {"output": 1},
                        },
                        "parts": [{"type": "text", "text": "done"}],
                    }]
                    supervisor.step()
            finally:
                supervisor.close()

            self.assertFalse(working.exists())
            self.assertTrue((root / "exec" / "demo@1" / "artifacts/output.a1").is_file())
            self.assertEqual(task_records(workspace)["history"][0]["status"], "submitted")
            self.assertFalse(workflow_status(workspace, workflow)["artifacts"]
                             ["output.a1"]["runnable"])
            client.prompt_session.assert_called_once()
            types = [event["type"] for event in read(root / "db.sqlite3", "demo@1")]
            self.assertIn("task_started", types)
            self.assertIn("task_completed", types)
            self.assertIn("artifact_refreshed", types)

    def test_replacement_session_clears_qualification_before_dispatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["a1"],
                "artifacts": {
                    "input": {"desc": "input"},
                    "learn.sess.a1": {
                        "desc": "learn", "input": ["input"], "instruction": "learn",
                    },
                    "output.a1": {
                        "desc": "output", "input": ["input", "learn.sess.a1"],
                        "instruction": "build",
                    },
                },
            })
            self._state(root, "demo@1", workspace, workflow, {"kind": "dag-mode"})
            refresh_artifact(workspace, workflow, "input")
            assign_task(workspace, workflow, "a1", "learn.sess.a1")
            submit(workspace, workflow, "a1", ["learn.sess.a1"])
            assign_task(workspace, workflow, "a1", "output.a1")
            client = mock.Mock()
            client.statuses.return_value = {"ses_root": {"type": "idle"}}
            client.children.return_value = []
            client.session_messages.return_value = []
            client.create_session.return_value = {"id": "ses_a1"}
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    client.children.side_effect = lambda session_id: ([{
                        "id": "ses_a1", "title": "a1", "agent": "a1",
                    }] if session_id == "ses_root" else [])
                    client.statuses.return_value = {
                        "ses_root": {"type": "idle"}, "ses_a1": {"type": "idle"},
                    }
                    supervisor.step()
            finally:
                supervisor.close()

            self.assertFalse((root / "exec" / "demo@1" / "artifacts"
                              / "learn.sess.a1").exists())
            stale = next(item for item in task_records(workspace)["history"]
                         if item["status"] == "stale")
            self.assertEqual(stale["reason"], "role Session was replaced")
            self.assertIn("`learn.sess.a1`", client.prompt_session.call_args.args[1])

    def test_incomplete_assets_are_reprompted_without_submitting_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["a1"],
                "artifacts": {
                    "input": {"desc": "input"},
                    "output.a1": {
                        "desc": "output", "input": ["input"],
                        "assets": ["result.txt"], "instruction": "build",
                    },
                },
            })
            self._state(root, "demo@1", workspace, workflow, {"kind": "dag-mode"})
            refresh_artifact(workspace, workflow, "input")
            client = mock.Mock()
            client.statuses.return_value = {"ses_a1": {"type": "idle"}}
            client.children.side_effect = lambda session_id: ([{
                "id": "ses_a1", "title": "a1", "agent": "a1",
            }] if session_id == "ses_root" else [])
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            working = root / "exec" / "demo@1" / "working" / "a1"
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    completed = working.stat().st_mtime_ns // 1_000_000 + 1
                    client.session_messages.return_value = [{
                        "info": {
                            "id": "msg_done", "role": "assistant", "finish": "stop",
                            "time": {"created": completed - 1, "completed": completed},
                        },
                        "parts": [{"type": "text", "text": "done"}],
                    }]
                    supervisor.step()
            finally:
                supervisor.close()

            self.assertTrue(working.is_file())
            self.assertFalse((root / "exec" / "demo@1" / "artifacts/output.a1").exists())
            self.assertEqual(len(client.prompt_session.call_args_list), 2)
            self.assertIn("artifact assets are incomplete", client.prompt_session.call_args.args[1])

    def test_dag_startup_reuses_existing_role_without_duplicate_create_or_prompt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            self._dag(root, workspace)
            client = mock.Mock()
            client.statuses.return_value = {"ses_a1": {"type": "idle"}}
            client.children.side_effect = lambda session_id: ([{
                "id": "ses_a1", "title": "a1", "agent": "a1",
            }] if session_id == "ses_root" else [])
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step(); supervisor.step()
            finally:
                supervisor.close()

            client.create_session.assert_not_called()
            client.prompt_session.assert_called_once()

    def test_active_marker_controls_scheduling_without_hiding_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            self._dag(root, workspace)
            active = root / "exec" / "demo@1" / "active"
            active.unlink()
            client = mock.Mock()
            client.statuses.return_value = {"ses_root": {"type": "idle"}}
            client.children.return_value = []
            client.session_messages.return_value = []
            client.create_session.return_value = {"id": "ses_a1"}
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    self.assertIn("demo@1", supervisor.state.executions)
                    client.create_session.assert_not_called()
                    active.touch()
                    supervisor.step()
            finally:
                supervisor.close()

            client.create_session.assert_called_once_with(
                "a1", parent_id="ses_root", agent="a1",
            )
            client.prompt_session.assert_called_once()

    def test_dag_refresh_waits_for_busy_role_then_prompts_when_idle(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            workflow = self._dag(root, workspace)
            refresh_artifact(workspace, workflow, "output.a1", force=True)
            client = mock.Mock()
            client.statuses.side_effect = [
                {"ses_a1": {"type": "idle"}},
                {"ses_a1": {"type": "busy"}},
                {"ses_a1": {"type": "idle"}},
            ]
            client.children.side_effect = lambda session_id: ([{
                "id": "ses_a1", "title": "a1", "agent": "a1",
            }] if session_id == "ses_root" else [])
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    refresh_artifact(workspace, workflow, "input")
                    supervisor.step()
                    client.prompt_session.assert_not_called()
                    supervisor.step()
            finally:
                supervisor.close()

            client.create_session.assert_not_called()
            client.prompt_session.assert_called_once()

    def test_dag_disappeared_role_is_recreated(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            workflow = self._dag(root, workspace)
            refresh_artifact(workspace, workflow, "output.a1", force=True)
            roots = iter([[{"id": "ses_a1", "title": "a1", "agent": "a1"}], []])
            client = mock.Mock()
            client.statuses.return_value = {"ses_a1": {"type": "idle"}}

            def children(session_id):
                return next(roots) if session_id == "ses_root" else []

            client.children.side_effect = children
            client.session_messages.return_value = []
            client.create_session.return_value = {"id": "ses_a1_new"}
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step(); supervisor.step()
            finally:
                supervisor.close()

            client.create_session.assert_called_once_with(
                "a1", parent_id="ses_root", agent="a1",
            )
            client.prompt_session.assert_not_called()

    def test_dag_duplicate_role_is_failed_without_prompt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            self._dag(root, workspace)
            client = mock.Mock()
            client.statuses.return_value = {}
            client.children.side_effect = lambda session_id: ([
                {"id": "ses_a1_1", "title": "a1-one", "agent": "a1"},
                {"id": "ses_a1_2", "title": "a1-two", "agent": "a1"},
            ] if session_id == "ses_root" else [])
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
            finally:
                supervisor.close()

            session = supervisor.state.executions["demo@1"].sessions["a1"]
            self.assertEqual(session.status, "failed")
            self.assertIn("duplicate Session", str(session.error))
            client.create_session.assert_not_called()
            client.prompt_session.assert_not_called()
            status = json.loads((root / "supervisor-status.json").read_text())
            self.assertIn("duplicate Session", status["executions"][0]["errors"][0]["error"])

    def test_removing_artifact_mode_stops_scheduling_and_clears_pressure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "demo@1" / "ws"
            workspace.mkdir(parents=True)
            self._dag(root, workspace)
            client = mock.Mock()
            client.statuses.return_value = {"ses_a1": {"type": "idle"}}
            client.children.side_effect = lambda session_id: ([{
                "id": "ses_a1", "title": "a1", "agent": "a1",
            }] if session_id == "ses_root" else [])
            client.session_messages.return_value = []
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    artifacts = root / "exec" / "demo@1" / "artifacts"
                    for path in artifacts.iterdir():
                        path.unlink()
                    artifacts.rmdir()
                    supervisor.step()
            finally:
                supervisor.close()

            execution = supervisor.state.executions["demo@1"]
            self.assertFalse(execution.dag)
            self.assertEqual(execution.runnable, {})
            self.assertEqual(execution.requests, ())
            client.prompt_session.assert_called_once()

    def test_removing_execution_goal_keeps_timeline_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "exec" / "bench@1" / "ws"
            workspace.mkdir(parents=True)
            desired = root / "exec" / "bench@1" / ".labflow-plan"
            self._state(root, "bench@1", workspace, None, {
                "kind": "benchmark-mode", "questioner": "q",
            })
            message = {
                "info": {"id": "msg", "role": "assistant",
                         "time": {"created": 10, "completed": 20},
                         "tokens": {"output": 1}},
                "parts": [{"type": "text", "text": "done"}],
            }
            client = mock.Mock()
            client.statuses.return_value = {}
            client.children.return_value = []
            client.sessions.return_value = []
            client.session_messages.return_value = [message]
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step()
                    desired.unlink()
                    supervisor.step()
            finally:
                supervisor.close()

            self.assertNotIn("bench@1", supervisor.state.executions)
            self.assertEqual(len(read(root / "db.sqlite3", "bench@1")), 5)


if __name__ == "__main__":
    unittest.main()
