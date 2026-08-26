from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from labflow.benchmark_mode import run
from labflow.bundle import install_bundle
from labflow.config import Manifest, sha256
from labflow.runtime_opencode import generate
from labflow.state import SCHEMA, atomic_write, load_state, save_state


class FakeClient:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.values: dict[str, list[dict]] = {"ses_base": []}
        self.children_by_root: list[dict] = []
        self.forks: list[tuple[str, str | None]] = []
        self.counter = 0

    def health(self): return {"healthy": True}
    def status(self): return {"type": "idle"}
    def statuses(self): return {name: {"type": "idle"} for name in self.values}
    def messages(self): return self.values["ses_base"]
    def session_messages(self, session): return self.values[session]
    def sessions(self): return []
    def children(self, _session=None): return list(self.children_by_root)

    def create_session(self, title, parent_id=None, agent=None):
        self.counter += 1
        session = f"ses_q_{self.counter}"
        self.values[session] = []
        self.children_by_root.append({"id": session, "title": title, "agent": agent})
        return {"id": session}

    def fork_session(self, session, message_id=None):
        self.counter += 1
        target = f"ses_a_{self.counter}"
        source = self.values[session]
        if message_id is None:
            copied = []
        else:
            index = next(i for i, value in enumerate(source)
                         if value.get("info", {}).get("id") == message_id)
            copied = [dict(value) for value in source[:index + 1]]
        self.values[target] = copied
        self.children_by_root.append({"id": target, "title": "", "agent": "a"})
        self.forks.append((session, message_id))
        return {"id": target}

    def update_session(self, session, payload):
        next(item for item in self.children_by_root if item["id"] == session).update(payload)

    def prompt_session(self, session, text, agent=None):
        now = int(time.time() * 1000)
        values = self.values[session]
        values.append({"info": {"id": f"u-{session}-{len(values)}", "role": "user",
                                 "time": {"created": now}},
                       "parts": [{"type": "text", "text": text}]})
        if agent == "q":
            reply = ({"action": "reply", "text": "EMEA"}
                     if "Which region?" in text else {"action": "done"})
            output = json.dumps(reply)
        elif text.strip() == "measured question":
            output = "Which region?"
        elif text.strip() == "EMEA":
            output = "final EMEA"
        else:
            output = "warm final"
        values.append({"info": {"id": f"a-{session}-{len(values)}", "role": "assistant",
                                 "agent": agent, "finish": "stop",
                                 "time": {"created": now, "completed": now + 1}},
                       "parts": [{"type": "text", "text": output}]})
        if agent == "a":
            answer = self.workspace / "answers/result.txt"
            answer.parent.mkdir(parents=True, exist_ok=True)
            answer.write_text(output + "\n", encoding="utf-8")


class BenchmarkModeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.root = self.base / "executions/bench/1"
        self.workspace = self.base / "workspace"
        self.bundle = self.base / "bundle"
        self.workspace.mkdir()
        self.bundle.mkdir()
        (self.bundle / "knowledge.txt").write_text("knowledge\n", encoding="utf-8")
        (self.base / "q.md").write_text("question user\n", encoding="utf-8")
        (self.base / "a.md").write_text("answer user\n", encoding="utf-8")
        (self.base / "0000.md").write_text("warm question\n", encoding="utf-8")
        (self.base / "0001.md").write_text("measured question\n", encoding="utf-8")
        (self.base / "0001-info.md").write_text("Region is EMEA.\n", encoding="utf-8")
        (self.base / "experiment.json").write_text("{}\n", encoding="utf-8")
        execution = {
            "kind": "benchmark-mode", "questioner": "q", "answerer": "a",
            "preflight": 1,
            "input": [{"path": "knowledge.txt", "level": 0}],
            "output": [{"path": "answers/", "level": 2}],
            "problems": [
                {"id": "0000", "q": "0000.md", "k": None, "maxTurns": 1},
                {"id": "0001", "q": "0001.md", "k": "0001-info.md", "maxTurns": 2},
            ],
            "bundle": {"paths": ["knowledge.txt"]},
        }
        self.manifest = Manifest(
            "bench", self.base, (), {
                "q": {"description": "questioner", "instructions": "q.md",
                      "commands": [], "preflight": []},
                "a": {"description": "answerer", "instructions": "a.md",
                      "commands": [], "preflight": []},
            }, (), (), (), execution=execution,
        )
        self.root.mkdir(parents=True)
        atomic_write(self.root / "plan", b"bench\n")
        state = {
            "schema": SCHEMA, "plan_id": "bench", "session_name": "bench/1",
            "phase": "idle", "workspace": str(self.workspace),
            "session_id": "ses_base", "active_round": None, "next_round": 0,
            "session_base": "bench", "session_title": "bench/1",
            "lab_root": str(self.base), "execution": execution,
            "adapter_hashes": {}, "asset_hashes": {}, "metrics": {"roles": {}},
            "input_hashes": {"experiment.json": sha256(self.base / "experiment.json")},
        }
        save_state(self.root, state)
        state = install_bundle(self.root, state, self.manifest, str(self.bundle))
        self.client = FakeClient(self.workspace)
        self.context = mock.Mock()
        self.context.root = self.root
        self.context.state = state
        self.context.manifest = self.manifest
        self.context.client.return_value = self.client

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_adapter_isolated_questioner_and_answerer_permissions(self):
        adapter = self.base / "adapter"
        adapter.mkdir()
        generate(self.manifest, adapter)
        q_text = (adapter / ".opencode/agents/q.md").read_text(encoding="utf-8")
        a_text = (adapter / ".opencode/agents/a.md").read_text(encoding="utf-8")
        q_permission = json.loads(next(line for line in q_text.splitlines()
                                       if line.startswith("permission: "))[12:])
        a_permission = json.loads(next(line for line in a_text.splitlines()
                                       if line.startswith("permission: "))[12:])
        self.assertEqual(q_permission["read"], {"*": "deny", "experiment.json": "deny",
                                                "**/experiment.json": "deny"})
        self.assertEqual(a_permission["read"]["knowledge.txt"], "allow")
        self.assertEqual(a_permission["edit"]["answers/**"], "allow")

    def test_preflight_warms_one_baseline_and_measured_problem_forks_it(self):
        response = run(self.context, since=0)
        self.assertEqual(len(response["result"]["preflight"]), 1)
        measured = response["result"]["problems"][0]
        self.assertEqual((measured["status"], measured["turns"]), ("completed", 2))
        self.assertEqual([item["text"].strip() for item in measured["transcript"]],
                         ["measured question", "Which region?", "EMEA", "final EMEA"])
        self.assertEqual(measured["metrics"]["answerer"]["assistant_messages"], 2)
        self.assertEqual(measured["metrics"]["questioner"]["assistant_messages"], 2)
        self.assertEqual(measured["metrics"]["answerer"]["rounds"][0]["round"], 1)
        baseline_reply = self.client.values["ses_base"][-1]["info"]["id"]
        self.assertEqual(self.client.forks, [("ses_base", baseline_reply)])
        archived = self.root / "benchmark/problems/0001/outputs/answers/result.txt"
        self.assertEqual(archived.read_text(), "final EMEA\n")
        self.assertFalse((self.workspace / "answers/result.txt").exists())
        self.assertEqual(load_state(self.root)["benchmark"]["status"], "completed")

    def test_completed_run_is_idempotent(self):
        first = run(self.context, since=0)
        second = run(self.context, since=0)
        self.assertEqual(second, first)


if __name__ == "__main__":
    unittest.main()
