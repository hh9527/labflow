from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from labflow.benchmark_mode import end_problem, run, start_problem
from labflow.bundle import install_bundle
from labflow.config import ControlError, Manifest, sha256
from labflow.runtime_opencode import generate
from labflow.state import SCHEMA, atomic_write, load_state, save_state


class FakeClient:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.evidence = "ok"
        self.values: dict[str, list[dict]] = {"ses_base": []}
        self.children_by_parent: dict[str, list[dict]] = {"ses_base": []}
        self.counter = 0
        self.host_prompts = 0

    def health(self): return {"healthy": True}
    def status(self): return {"type": "idle"}
    def statuses(self): return {name: {"type": "idle"} for name in self.values}
    def messages(self): return self.values["ses_base"]
    def session_messages(self, session): return self.values[session]
    def sessions(self): return []
    def children(self, session=None): return list(self.children_by_parent.get(session, []))

    def create_session(self, title, parent_id=None, agent=None):
        self.counter += 1
        session = f"ses_q_{self.counter}"
        self.values[session] = []
        self.children_by_parent.setdefault(parent_id, []).append(
            {"id": session, "title": title, "agent": agent}
        )
        self.children_by_parent[session] = []
        return {"id": session}

    def update_session(self, session, payload):
        next(item for children in self.children_by_parent.values() for item in children
             if item["id"] == session).update(payload)

    def prompt_session(self, session, text, agent=None):
        self.host_prompts += 1
        now = int(time.time() * 1000)
        q_messages = self.values[session]
        q_messages.append({"info": {"id": f"u-{session}", "role": "user",
                                     "time": {"created": now}},
                           "parts": [{"type": "text", "text": text}]})
        if agent != "q":
            raise AssertionError("Host only prompts Questioner Sessions")
        self.counter += 1
        answerer = f"ses_a_{self.counter}"
        self.values[answerer] = []
        self.children_by_parent[session].append(
            {"id": answerer, "title": "answerer", "agent": "a"}
        )
        self.children_by_parent[answerer] = []
        execution = json.loads((self.workspace / "experiment.json").read_text())["execution"]
        for index, problem in enumerate(execution["problems"]):
            start_problem(self.workspace, problem["id"])
            created = int(time.time() * 1000)
            a_messages = self.values[answerer]
            a_messages.extend([
                {"info": {"id": f"u-{answerer}-{index}", "role": "user",
                           "time": {"created": created}},
                 "parts": [{"type": "text", "text": f"question {problem['id']}"}]},
                {"info": {"id": f"a-{answerer}-{index}", "role": "assistant", "agent": "a",
                           "time": {"created": created, "completed": created + 1}},
                 "parts": [{"type": "text", "text": f"answer {problem['id']}"}]},
            ])
            channel = self.workspace / "ch/out"
            channel.mkdir(parents=True, exist_ok=True)
            (channel / "report.md").write_text(f"report {problem['id']}\n", encoding="utf-8")
            if self.evidence == "ok":
                (channel / "ok-answer.json").write_text(
                    json.dumps({"answer": problem["id"]}) + "\n", encoding="utf-8"
                )
            elif self.evidence == "err":
                (channel / "err-diagnostic.txt").write_text("diagnostic\n", encoding="utf-8")
            end_problem(self.workspace, {"ok": "ok", "err": "error"}.get(
                self.evidence, "cancel"
            ))
            time.sleep(.002)
        completed = int(time.time() * 1000)
        q_messages.append({"info": {"id": f"a-{session}", "role": "assistant", "agent": agent,
                                     "finish": "stop",
                                     "time": {"created": now, "completed": completed}},
                           "parts": [{"type": "text", "text": "batch completed"}]})


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
        for name, content in (("q.md", "questioner"), ("a.md", "answerer"),
                              ("0000.md", "question zero"), ("0001.md", "question one"),
                              ("0001-info.md", "hidden fact")):
            (self.base / name).write_text(content + "\n", encoding="utf-8")
        execution = {
            "kind": "benchmark-mode", "questioner": "q", "answerer": "a", "batchSize": 2,
            "input": [{"path": "knowledge.txt", "level": 0}],
            "output": [{"path": "ch/out/", "level": 2}],
            "problems": [
                {"id": "0000", "q": "0000.md", "k": None, "maxTurns": 1},
                {"id": "0001", "q": "0001.md", "k": "0001-info.md", "maxTurns": 2},
            ],
            "bundle": {"paths": ["knowledge.txt"]},
        }
        self.manifest = Manifest(
            "bench", self.base, (), {
                "q": {"description": "questioner", "instructions": "q.md",
                      "commands": ["labflow problem start *", "labflow problem end *"],
                      "preflight": ["labflow problem start sample",
                                    "labflow problem end cancel"]},
                "a": {"description": "answerer", "instructions": "a.md",
                      "commands": [], "preflight": []},
            }, (), (), (), execution=execution,
        )
        self.root.mkdir(parents=True)
        atomic_write(self.root / "plan", b"bench\n")
        atomic_write(self.workspace / "experiment.json", json.dumps({
            "schema": "labflow.experiment-runtime/v1", "plan_id": "bench",
            "workflow": None, "execution": execution,
        }).encode())
        state = {
            "schema": SCHEMA, "plan_id": "bench", "session_name": "bench/1",
            "phase": "idle", "workspace": str(self.workspace), "session_id": "ses_base",
            "active_round": None, "next_round": 0, "session_base": "bench",
            "session_title": "bench/1", "lab_root": str(self.base), "execution": execution,
            "adapter_hashes": {}, "asset_hashes": {}, "metrics": {"roles": {}},
            "input_hashes": {"experiment.json": sha256(self.workspace / "experiment.json")},
        }
        save_state(self.root, state)
        state = install_bundle(self.root, state, self.manifest, str(self.bundle))
        self.client = FakeClient(self.workspace)
        self.context = mock.Mock(root=self.root, state=state, manifest=self.manifest)
        self.context.client.return_value = self.client

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_adapter_enforces_q_a_channel_ownership(self):
        adapter = self.base / "adapter"
        adapter.mkdir()
        generate(self.manifest, adapter)
        q_text = (adapter / ".opencode/agents/q.md").read_text(encoding="utf-8")
        a_text = (adapter / ".opencode/agents/a.md").read_text(encoding="utf-8")
        q_permission = json.loads(next(line for line in q_text.splitlines()
                                       if line.startswith("permission: "))[12:])
        a_permission = json.loads(next(line for line in a_text.splitlines()
                                       if line.startswith("permission: "))[12:])
        self.assertEqual(q_permission["task"], {"*": "deny", "a": "allow"})
        self.assertEqual(a_permission["task"], "deny")
        self.assertNotIn("knowledge.txt", q_permission["read"])
        self.assertEqual(a_permission["read"]["knowledge.txt"], "allow")
        self.assertEqual(q_permission["edit"]["ch/out/report.md"], "allow")
        self.assertNotIn("ch/q.md", q_permission["edit"])
        self.assertEqual(a_permission["edit"]["ch/out/**"], "allow")
        self.assertEqual(a_permission["edit"]["ch/out/report.md"], "deny")
        self.assertIn("Host 会一次性准备并触发整批题目", q_text)
        self.assertIn("原文逐字发送", q_text)
        self.assertIn("`report.md` 由 Questioner 编写", a_text)

    def test_one_host_trigger_runs_and_archives_the_whole_batch(self):
        response = run(self.context, since=0)
        self.assertEqual(self.client.host_prompts, 1)
        self.assertEqual([record["problem"] for record in response["result"]["problems"]],
                         ["0000", "0001"])
        questioners = self.client.children("ses_base")
        self.assertEqual(len(questioners), 1)
        self.assertEqual(len(self.client.children(questioners[0]["id"])), 1)
        for problem_id in ("0000", "0001"):
            root = self.workspace / "result" / problem_id
            self.assertEqual((root / "report.md").read_text(), f"report {problem_id}\n")
            self.assertEqual(json.loads((root / "ok-answer.json").read_text()),
                             {"answer": problem_id})
        stats = (self.workspace / "result/stats.jsonl").read_text().splitlines()
        self.assertEqual(len(stats), 2)
        self.assertEqual(load_state(self.root)["benchmark"]["status"], "completed")

    def test_report_only_and_error_evidence_are_valid(self):
        self.client.evidence = "none"
        response = run(self.context, since=0)
        self.assertEqual(response["result"]["problems"][0]["evidence"], [])
        self.assertEqual(list((self.workspace / "result/0000").iterdir()),
                         [self.workspace / "result/0000/report.md"])

    def test_end_requires_report_and_outcome_selects_evidence(self):
        (self.workspace / "problem/0000").mkdir(parents=True)
        (self.workspace / "problem/0000/q.md").write_text("exact question\n")
        start_problem(self.workspace, "0000")
        with self.assertRaisesRegex(ControlError, "report.md"):
            end_problem(self.workspace, "ok")
        channel = self.workspace / "ch/out"
        (channel / "report.md").write_text("report\n")
        (channel / "ok-note.txt").write_text("opaque success\n")
        (channel / "err-diagnostic.txt").write_text("error\n")
        end_problem(self.workspace, "ok")
        self.assertTrue((self.workspace / "result/0000/ok-note.txt").is_file())
        self.assertFalse((self.workspace / "result/0000/err-diagnostic.txt").exists())

    def test_start_copies_exact_problem_and_generated_metadata(self):
        source = self.workspace / "problem/0000"
        source.mkdir(parents=True)
        (source / "q.md").write_bytes("exact question\n".encode())
        (source / "k.md").write_bytes("hidden knowledge\n".encode())
        result = start_problem(self.workspace, "0000")
        self.assertEqual(result["maxTurns"], 1)
        self.assertEqual((self.workspace / "ch/q.md").read_bytes(),
                         (source / "q.md").read_bytes())
        self.assertEqual(json.loads((self.workspace / "ch/metadata.json").read_text()),
                         {"id": "0000", "maxTurns": 1})


if __name__ == "__main__":
    unittest.main()
