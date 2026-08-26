from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from labflow.config import ControlError, Manifest, load_manifest
from labflow.metrics import summarize_thread_metrics
from labflow.runtime_opencode import generate
from labflow.state import SCHEMA, atomic_write, load_state, save_state
from labflow.thread_service import (
    approve_baseline,
    close_thread,
    comment_thread,
    install_bundle,
    open_thread,
)


def assistant(message_id: str) -> dict:
    return {
        "info": {
            "id": message_id,
            "role": "assistant",
            "finish": "stop",
            "time": {"created": 1000, "completed": 2000},
        },
        "parts": [{"type": "text", "text": "done"}],
    }


class ThreadServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.root = self.base / "execution"
        self.workspace = self.base / "workspace"
        self.bundle = self.base / "bundle"
        self.workspace.mkdir()
        self.bundle.mkdir()
        (self.bundle / "engine.txt").write_text("engine\n", encoding="utf-8")
        (self.base / "start.md").write_text("qualify\n", encoding="utf-8")
        (self.base / "a5.md").write_text("serve requests\n", encoding="utf-8")
        self.manifest = Manifest(
            "service", self.base, (), {"a5": {
                "description": "production worker", "instructions": "a5.md",
                "commands": [], "preflight": [],
            }}, (), (), (),
            execution={
                "kind": "thread-service", "role": "a5", "start": "start.md",
                "baseline": {"checks": ["answers/0000.json"],
                             "command": ["validate", "0000"]},
                "bundle": {"paths": ["engine.txt"]},
            },
        )
        self.root.mkdir()
        atomic_write(self.root / "plan", b"service\n")
        self.state = {
            "schema": SCHEMA, "plan_id": "service", "session_name": "service/1",
            "phase": "idle", "workspace": str(self.workspace),
            "session_id": "ses_base", "active_round": None,
            "session_base": "worker-service",
            "lab_root": str(self.base),
            "execution": self.manifest.execution,
        }
        save_state(self.root, self.state)
        self.state = install_bundle(
            self.root, self.state, self.manifest, str(self.bundle)
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def context(self, client: mock.Mock) -> mock.Mock:
        context = mock.Mock()
        context.root = self.root
        context.state = load_state(self.root)
        context.manifest = self.manifest
        context.client.return_value = client
        context.rounds.return_value = []
        return context

    def test_bundle_is_copied_and_fingerprinted(self):
        self.assertEqual((self.workspace / "engine.txt").read_text(), "engine\n")
        self.assertEqual(len(self.state["bundle"]["digest"]), 64)
        self.assertEqual(self.state["thread_service"], {"baseline": None, "active": None})

    def test_baseline_approval_validates_answer_and_freezes_message(self):
        answer = self.workspace / "answers" / "0000.json"
        answer.parent.mkdir()
        answer.write_text("{}\n", encoding="utf-8")
        client = mock.Mock()
        client.statuses.return_value = {"ses_base": {"type": "idle"}}
        client.messages.return_value = [assistant("msg_base")]
        client.session_messages.return_value = [assistant("msg_base")]
        context = self.context(client)
        completed = subprocess.CompletedProcess([], 0, "valid\n", "")
        with mock.patch(
            "labflow.thread_service.resolve_command",
            return_value=["validate", "0000"],
        ), mock.patch(
            "labflow.thread_service.subprocess.run", return_value=completed
        ) as run:
            record = approve_baseline(context, "a5")
        self.assertEqual(record["message_id"], "msg_base")
        self.assertEqual(load_state(self.root)["thread_service"]["baseline"], record)
        run.assert_called_once()

    def test_thread_forks_baseline_reuses_context_for_comments_and_archives(self):
        state = load_state(self.root)
        state["thread_service"]["baseline"] = {
            "role": "a5", "session_id": "ses_base", "message_id": "msg_base",
            "bundle_digest": state["bundle"]["digest"], "approved_at": "now",
        }
        save_state(self.root, state)
        problem = self.base / "problem.md"
        comment = self.base / "comment.md"
        problem.write_text("# 0001\nquestion\n", encoding="utf-8")
        comment.write_text("clarification\n", encoding="utf-8")
        client = mock.Mock()
        client.statuses.return_value = {
            "ses_base": {"type": "idle"}, "ses_thread": {"type": "idle"}
        }
        client.session_messages.side_effect = lambda session: [
            assistant("msg_base" if session == "ses_base" else "msg_thread")
        ]
        client.fork_session.return_value = {"id": "ses_thread"}
        client.sessions.return_value = []
        context = self.context(client)

        opened = open_thread(context, "a5", "0001", str(problem))
        self.assertEqual(opened["session_id"], "ses_thread")
        client.fork_session.assert_called_once_with("ses_base")
        client.prompt_session.assert_called_with("ses_thread", problem.read_text(), agent="a5")
        with self.assertRaisesRegex(ControlError, "already has an active thread"):
            open_thread(context, "a5", "0002", str(problem))

        commented = comment_thread(context, "a5", "0001", str(comment))
        self.assertEqual(len(commented["inputs"]), 2)
        client.prompt_session.assert_called_with("ses_thread", comment.read_text(), agent="a5")

        closed = close_thread(context, "a5")
        self.assertEqual(closed["status"], "closed")
        self.assertIsNone(load_state(self.root)["thread_service"]["active"])
        self.assertEqual(client.update_session.call_args_list[0].args,
                         ("ses_thread", {"title": "worker-service.0001/1"}))
        self.assertEqual(client.update_session.call_count, 2)

    def test_query_service_plan_generates_primary_a5_without_coordinator(self):
        workspace = self.base / "adapter"
        workspace.mkdir()
        generate(self.manifest, workspace)
        self.assertFalse((workspace / ".opencode/agents/coordinator.md").exists())
        self.assertTrue((workspace / ".opencode/agents/a5.md").is_file())
        self.assertIn('"default_agent": "a5"',
                      (workspace / "opencode.json").read_text(encoding="utf-8"))

    def test_thread_metrics_are_split_by_host_round(self):
        messages = [
            {"info": {"role": "user", "time": {"created": 1000}}, "parts": []},
            {"info": {"role": "assistant", "time": {"created": 1100, "completed": 2000},
                      "tokens": {"input": 3, "output": 2}}, "parts": [{
                          "type": "tool", "tool": "bash", "state": {
                              "input": {"command": "just a5 make-query 0001"},
                              "time": {"start": 1200, "end": 1500},
                          },
                      }]},
            {"info": {"role": "user", "time": {"created": 3000}}, "parts": []},
            {"info": {"role": "assistant", "time": {"created": 3100, "completed": 4000},
                      "tokens": {"reasoning": 7}}, "parts": []},
        ]
        value = summarize_thread_metrics(messages, {"query": ["just a5 *"]}, 5000)
        self.assertEqual(len(value["rounds"]), 2)
        self.assertEqual(value["tokens"]["fresh"], 12)
        self.assertEqual(value["command_count"], 1)
        self.assertEqual(value["rounds"][0]["elapsed_ms"], 1000)


if __name__ == "__main__":
    unittest.main()
