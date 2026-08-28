from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from labflow.config import ControlError
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


if __name__ == "__main__":
    unittest.main()
