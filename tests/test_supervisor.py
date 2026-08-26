from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from labflow.events import event_detail, project_events
from labflow.state import SCHEMA, bind_plan, save_state
from labflow.supervisor import LifecycleEvent, Supervisor, SupervisorState, reduce
from labflow.task_cli import refresh_artifact, validate_workflow
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
                         ["action", "thinking", "thinking", "reply"])
        action = events[0]
        self.assertEqual((action["action"], action["success"], action["command"]),
                         ("shell", True, "just validate --all"))
        self.assertEqual((action["at"], action["duration"]), (1200, 200))
        reply = events[-1]
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
        action = closed_message_events("plan@1", "a1", "a1", message)[0]
        self.assertEqual(action["paths"], ["result/output.json"])
        self.assertFalse(action["success"])


class TimelineStoreTest(unittest.TestCase):
    def test_writer_appends_and_deduplicates_without_reader_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timeline.sqlite3"
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
            writer = TimelineWriter(root / "timeline.sqlite3", flush_seconds=.01)
            writer.submit([{
                "id": "reply:plan@1:a1:msg:complete",
                "execution": "plan@1", "session": "a1", "role": "a1",
                "type": "reply", "at": 200, "duration": 0, "tokens": 3,
                "summary": "done",
            }])
            writer.close()
            context = mock.Mock(state={
                "lab_root": str(root), "title": "plan@1", "workspace": str(root / "ws"),
            })

            events = project_events(context, 100)
            detail = event_detail(context, events[0]["id"])

            self.assertEqual(events[0]["session"], "a1")
            self.assertEqual(detail["detail"]["tokens"], 3)

    def test_statistics_aggregate_timing_tokens_commands_and_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timeline.sqlite3"
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
        save_state(control, {
            "schema": SCHEMA, "plan_id": "demo", "title": title,
            "phase": "active", "workspace": str(workspace),
            "session_id": "ses_root", "workflow": workflow,
            "execution": execution,
        })

    def test_observe_only_goal_collects_existing_sessions_without_effects(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "ws" / "bench@1"
            workspace.mkdir(parents=True)
            (root / "supervisor" / "bench@1").mkdir(parents=True)
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
            client.session_messages.return_value = [message]
            supervisor = Supervisor(root, 4199)
            try:
                with mock.patch("labflow.supervisor.Client", return_value=client):
                    supervisor.step(); supervisor.step()
            finally:
                supervisor.close()

            self.assertEqual(len(read(root / "timeline.sqlite3", "bench@1")), 2)
            client.create_session.assert_not_called()
            status = json.loads((root / "supervisor-status.json").read_text())
            self.assertEqual(status["executions"][0]["title"], "bench@1")

    def test_dag_goal_creates_and_prompts_one_missing_runnable_role(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "ws" / "demo@1"
            workspace.mkdir(parents=True)
            desired = root / "supervisor" / "demo@1" / "artifacts"
            desired.mkdir(parents=True)
            control = workspace / "control"
            control.mkdir()
            (control / "artifacts").symlink_to(desired, target_is_directory=True)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["a1"],
                "artifacts": {
                    "input": {"desc": "input"},
                    "output.a1": {"desc": "output", "input": ["input"],
                                  "instruction": "build"},
                },
            })
            refresh_artifact(workspace, workflow, "input")
            self._state(root, "demo@1", workspace, workflow, {"kind": "dag-mode"})
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
            self.assertIn("input", supervisor.state.executions["demo@1"].artifacts)


if __name__ == "__main__":
    unittest.main()
