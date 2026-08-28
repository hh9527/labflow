from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from labflow.config import ControlError
from labflow.supervisor import (
    EffectState, LifecycleEvent, Supervisor, SupervisorState, _state_dump, _state_load,
    reduce, supervisor_lock,
)
from labflow.task_cli import (
    assign_task, refresh_artifact, submit, task_records, validate_workflow,
    workflow_status,
)
from labflow.timeline_projection import closed_message_events
from labflow.timeline_report import TimelineReporter
from labflow.timeline_store import TimelineWriter, read, statistics, task_statistics


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
            "task": {
                "task_id": "a1-1000000000", "artifact": "output.a1",
                "started_at_ns": 1000000000,
            },
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

    def _active_task_state(self) -> SupervisorState:
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
            "task": {
                "task_id": "a1-1000000000", "artifact": "output.a1",
                "started_at_ns": 1000000000,
            },
        }))
        return state

    def test_turn_then_idle_and_idle_then_turn_converge_to_settlement(self):
        turn = LifecycleEvent("turn_completed", "plan@1", {
            "role": "a1", "title": "a1", "message_id": "msg_done",
            "completed_at": 2000, "finish": "stop", "reply": "已完成任务。done",
        })
        busy = LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "busy",
        })
        idle = LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "idle",
        })

        turn_first = self._active_task_state()
        reduce(turn_first, busy)
        self.assertEqual(reduce(turn_first, turn), [])
        first = reduce(turn_first, idle)

        idle_first = self._active_task_state()
        reduce(idle_first, busy)
        self.assertEqual(reduce(idle_first, idle), [])
        second = reduce(idle_first, turn)

        self.assertEqual([effect.kind for effect in first], ["settle_task"])
        self.assertEqual([effect.kind for effect in second], ["settle_task"])
        self.assertEqual(first[0].data, second[0].data)

    def test_duplicate_completed_turn_does_not_duplicate_settlement(self):
        state = self._active_task_state()
        reduce(state, LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "idle",
        }))
        turn = LifecycleEvent("turn_completed", "plan@1", {
            "role": "a1", "title": "a1", "message_id": "msg_done",
            "completed_at": 2000, "finish": "stop", "reply": "已完成任务。done",
        })

        self.assertEqual([effect.kind for effect in reduce(state, turn)], ["settle_task"])
        self.assertEqual(reduce(state, turn), [])

    def test_tool_call_turn_is_an_intermediate_message(self):
        state = self._active_task_state()
        execution = state.executions["plan@1"]

        effects = reduce(state, LifecycleEvent("turn_completed", "plan@1", {
            "role": "a1", "title": "a1", "message_id": "msg_tool",
            "completed_at": 2000, "finish": "tool-calls", "reply": "",
        }))

        self.assertEqual(effects, [])
        self.assertIsNone(execution.blocked_reason)
        self.assertNotIn("a1", execution.failures)
        self.assertIn("a1", execution.active_tasks)

    def test_idle_session_with_non_stop_last_message_is_aborted(self):
        state = self._active_task_state()
        execution = state.executions["plan@1"]

        effects = reduce(state, LifecycleEvent("turn_aborted", "plan@1", {
            "role": "a1", "title": "a1", "message_id": "msg_tool",
            "completed_at": 2000, "finish": "tool-calls",
        }))

        self.assertEqual(effects, [])
        self.assertEqual(execution.failures["a1"].kind, "turn_aborted")
        self.assertEqual(execution.sessions["a1"].error_kind, "turn_aborted")
        self.assertEqual(reduce(state, LifecycleEvent("turn_aborted", "plan@1", {
            "role": "a1", "title": "a1", "message_id": "msg_tool",
            "completed_at": 2000, "finish": "tool-calls",
        })), [])
        self.assertEqual(execution.failures["a1"].count, 1)

    def test_validation_failure_event_requests_one_repair_prompt(self):
        state = self._active_task_state()
        reduce(state, LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "idle",
        }))
        event = LifecycleEvent("task_validation_failed", "plan@1", {
            "role": "a1", "task_id": "a1-1000000000",
            "turn_id": "msg_done", "error": "artifact assets are incomplete", "at": 2000,
        })

        effects = reduce(state, event)

        self.assertEqual([effect.kind for effect in effects], ["prompt_session"])
        self.assertEqual(effects[0].data["error"], "artifact assets are incomplete")
        self.assertTrue(effects[0].data["include_checks"])

    def test_unclassified_reply_is_retried_without_mechanical_checks(self):
        state = self._active_task_state()
        reduce(state, LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "busy",
        }))
        effects = reduce(state, LifecycleEvent("turn_completed", "plan@1", {
            "role": "a1", "title": "a1", "message_id": "msg_bad",
            "completed_at": 2000, "finish": "stop", "reply": "任务做完了",
        }))
        self.assertEqual(effects, [])

        effects = reduce(state, LifecycleEvent("session_observed", "plan@1", {
            "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "idle",
        }))
        self.assertEqual([effect.kind for effect in effects], ["prompt_session"])
        self.assertIn("reply must start", effects[0].data["error"])
        self.assertFalse(effects[0].data["include_checks"])

    def test_three_recent_validation_failures_block_execution_until_host_clears_it(self):
        state = self._active_task_state()
        effects = []
        for index, at in enumerate((2000, 3000, 4000), 1):
            reduce(state, LifecycleEvent("session_observed", "plan@1", {
                "role": "a1", "title": "a1", "backend_id": "ses_a1", "status": "idle",
            }))
            effects = reduce(state, LifecycleEvent("task_validation_failed", "plan@1", {
                "role": "a1", "task_id": "a1-1000000000",
                "turn_id": f"msg-{index}", "error": "artifact assets are incomplete",
                "at": at,
            }))
            if index < 3:
                self.assertEqual([effect.kind for effect in effects], ["prompt_session"])
                reduce(state, LifecycleEvent("effect_succeeded", "plan@1", {
                    "key": effects[0].key, "effect_kind": "prompt_session",
                    "role": "a1", "title": "a1", "backend_id": "ses_a1",
                }))

        self.assertEqual([effect.kind for effect in effects], ["write_system_blocked"])
        self.assertIsNotNone(state.executions["plan@1"].blocked_reason)
        resumed = reduce(state, LifecycleEvent("system_blocked_observed", "plan@1", {
            "present": False,
        }))
        self.assertIsNone(state.executions["plan@1"].blocked_reason)
        self.assertEqual([effect.kind for effect in resumed], ["prompt_session"])

    def test_failure_window_and_blocked_state_survive_restart(self):
        state = self._active_task_state()
        execution = state.executions["plan@1"]
        execution.blocked_reason = "artifact assets are incomplete"
        reduce(state, LifecycleEvent("task_validation_failed", "plan@1", {
            "role": "a1", "task_id": "a1-1000000000", "turn_id": "msg-1",
            "error": "artifact assets are incomplete", "at": 2000,
        }))

        restored = _state_load(_state_dump(state))

        restored_execution = restored.executions["plan@1"]
        self.assertEqual(restored_execution.blocked_reason,
                         "artifact assets are incomplete")
        self.assertEqual(restored_execution.failures["a1"].count, 1)

    def test_newer_active_marker_acknowledges_retained_system_block(self):
        state = self._active_task_state()
        execution = state.executions["plan@1"]
        execution.blocked_reason = "host intervention required"
        state.active_mtime = 2000

        reduce(state, LifecycleEvent("system_blocked_observed", "plan@1", {
            "present": True, "mtime_ns": 1000,
        }))

        self.assertIsNone(execution.blocked_reason)
        self.assertEqual(execution.failures, {})

        reduce(state, LifecycleEvent("system_blocked_observed", "plan@1", {
            "present": True, "mtime_ns": 3000,
        }))
        self.assertEqual(execution.blocked_reason, "system-blocked marker exists")


class SupervisorOwnershipTest(unittest.TestCase):
    def test_timeline_is_flushed_to_supervisor_stdout_before_commit(self):
        class Reporter:
            committed = False

            def poll(self):
                return "[26-08-28 22:16:02] query.a1 已完成"

            def commit(self):
                self.committed = True

        supervisor = object.__new__(Supervisor)
        supervisor.timeline_reporter = Reporter()
        supervisor.timeline_error = None
        with mock.patch("builtins.print") as output:
            supervisor._display_timeline()

        output.assert_called_once_with(
            "[26-08-28 22:16:02] query.a1 已完成", flush=True,
        )
        self.assertTrue(supervisor.timeline_reporter.committed)

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

    def test_reporter_debounces_coarse_events_and_persists_cursor(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            writer = TimelineWriter(home / "events.sqlite", flush_seconds=.01)
            writer.submit([
                {"id": "start", "execution": "plan@1", "session": "a1", "role": "a1",
                 "task_kind": "artifact", "task_id": "query.a1", "type": "task_started",
                 "at": 1000, "duration": 0, "payload": {"attempt_id": "attempt-1"}},
                {"id": "thinking", "execution": "plan@1", "session": "a1",
                 "role": "a1", "task_kind": "artifact", "task_id": "query.a1",
                 "type": "thinking", "at": 1100, "duration": 800},
                {"id": "reply", "execution": "plan@1", "session": "a1", "role": "a1",
                 "task_kind": "artifact", "task_id": "query.a1", "type": "reply",
                 "at": 2000, "duration": 0, "input_tokens": 100,
                 "output_tokens": 20, "reasoning_tokens": 5},
                {"id": "complete", "execution": "plan@1", "session": "a1",
                 "role": "a1", "task_kind": "artifact", "task_id": "query.a1",
                 "type": "task_completed", "at": 3000, "duration": 0,
                 "payload": {"attempt_id": "attempt-1", "status": "submitted"}},
                {"id": "host", "execution": "plan@1", "task_kind": "artifact",
                 "task_id": "query", "artifact": "query", "type": "host_request_opened",
                 "at": 3001, "duration": 0, "payload": {"optional": False}},
                {"id": "approved-1", "execution": "plan@1", "task_kind": "artifact",
                 "task_id": "query", "artifact": "query", "type": "artifact_refreshed",
                 "at": 3100, "duration": 0},
                {"id": "approved-2", "execution": "plan@1", "task_kind": "artifact",
                 "task_id": "query", "artifact": "query", "type": "artifact_refreshed",
                 "at": 3200, "duration": 0},
            ])
            writer.close()
            task_values = task_statistics(
                home / "events.sqlite", "plan@1", ["query.a1"], ["query"], 4000,
            )
            now = [0.0]
            reporter = TimelineReporter(home, "plan@1", clock=lambda: now[0])

            self.assertIsNone(reporter.poll())
            now[0] = 5.0
            message = reporter.poll()

            self.assertIn("query.a1 已开始", message)
            self.assertIn("query.a1 已完成（耗时 2.0s，Token 125", message)
            self.assertIn("最长思考 0.8s", message)
            self.assertIn("query 等待 Host 处理", message)
            self.assertRegex(message, r"\[70-01-01 \d\d:\d\d:\d\d\] query\.a1 已开始")
            role = task_values["role_tasks"]["query.a1"]
            self.assertEqual(role["rounds"], 1)
            self.assertEqual(role["total"], {
                "duration_ms": 2000, "tokens": 125, "longest_thinking_ms": 800,
            })
            self.assertEqual(role["latest"]["status"], "submitted")
            self.assertEqual(task_values["host_tasks"]["query"], {
                "approvals": 2, "last_approved_at": 3200,
            })
            reporter.commit()
            self.assertGreater(int((home / "report-cursor").read_text()), 0)
            restarted = TimelineReporter(home, "plan@1", clock=lambda: now[0])
            self.assertIsNone(restarted.poll())


if __name__ == "__main__":
    unittest.main()
