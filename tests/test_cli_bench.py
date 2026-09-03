from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from labflow.benchmark import load_bundle, stage_path
from labflow.cli_bench import (
    Context, _batch_finish, _batch_start, _clarify, _finish, _next,
)


class FakeClient:
    sessions: dict[str, list[dict]] = {}
    created: list[dict] = []
    deleted: list[str] = []

    def __init__(self, url: str, workspace: str, session_id: str | None = None, **kwargs):
        self.url = url
        self.workspace = workspace
        self.session_id = session_id

    def create_session(self, title: str, parent_id=None, agent=None):
        identity = f"resolver-{len(self.created) + 1}"
        self.created.append({
            "id": identity, "title": title, "parent_id": parent_id, "agent": agent,
        })
        self.sessions[identity] = []
        return {"id": identity}

    def messages(self):
        return list(self.sessions[self.session_id])

    def prompt_session(self, session_id: str, text: str, agent=None):
        index = len(self.sessions[session_id]) + 1
        self.sessions[session_id].append({
            "info": {
                "id": f"message-{index}", "role": "assistant", "finish": "stop",
                "time": {"created": index * 100, "completed": index * 100 + 10},
                "tokens": {"input": index, "output": 2, "reasoning": 3},
            },
            "parts": [{"type": "text", "text": f"answer {index}: {text}"}],
        })

    def status(self):
        return {"type": "idle"}

    def abort_session(self, session_id: str):
        return None

    def delete_session(self, session_id: str):
        self.deleted.append(session_id)
        self.sessions.pop(session_id, None)


class BenchmarkCliTest(unittest.TestCase):
    def setUp(self):
        FakeClient.sessions = {}
        FakeClient.created = []
        FakeClient.deleted = []

    def context(self, root: Path) -> Context:
        source = root / "input"
        (source / "public").mkdir(parents=True)
        (source / "questions.jsonl").write_text(
            '\n'.join(json.dumps(item) for item in (
                {"id": "q1", "Q": "question one", "K": "knowledge one"},
                {"id": "q2", "Q": "question two", "K": "knowledge two"},
            )) + '\n', encoding="utf-8",
        )
        (source / "selected.jsonl").write_text('"q1"\n"q2"\n', encoding="utf-8")
        home = root / ".labflow-exec"
        return Context(
            root=root, home=home, role="bench-icm", task_id="bench-icm-1",
            artifact={"benchmark": True}, artifact_name="report.bench-icm",
            input_path="input/", asset_path="report.sqlite", bundle=load_bundle(source),
            stage=stage_path(home, "report.bench-icm"),
            client=FakeClient("http://127.0.0.1:4199", str(root)),
        )

    def test_independent_resolver_batch_is_exported_deleted_and_finished(self):
        with tempfile.TemporaryDirectory() as temporary, patch(
            "labflow.cli_bench.Client", FakeClient,
        ):
            context = self.context(Path(temporary))
            batch = _batch_start(context, 10)
            self.assertEqual(batch["session_id"], "resolver-1")
            self.assertEqual(FakeClient.created[0]["agent"], "priv-resolver")
            self.assertIsNone(FakeClient.created[0]["parent_id"])

            first = _next(context, 1)
            self.assertEqual(first["case_id"], "q1")
            self.assertEqual(first["private_knowledge"], "knowledge one")
            clarified = _clarify(context, "clarification", 1)
            self.assertEqual(clarified["clarification_round"], 1)
            self.assertEqual(_next(context, 1)["case_id"], "q2")
            self.assertTrue(_next(context, 1)["batch_complete"])

            _batch_finish(context)
            self.assertEqual(FakeClient.deleted, ["resolver-1"])
            result = _finish(context)
            self.assertEqual(result["selected_count"], 2)
            self.assertEqual(result["outcome"], "completed")

            with sqlite3.connect(context.root / "report.sqlite") as connection:
                cases = connection.execute(
                    "SELECT case_id, status FROM cases WHERE iter = 0 ORDER BY ordinal"
                ).fetchall()
                rounds = connection.execute(
                    "SELECT case_id, round FROM clarifications WHERE iter = 0 "
                    "ORDER BY case_id, round"
                ).fetchall()
                batches = connection.execute(
                    "SELECT session_id, resolver_deleted FROM batches WHERE iter = 0"
                ).fetchall()
            self.assertEqual(cases, [("q1", "answered"), ("q2", "answered")])
            self.assertEqual(rounds, [("q1", 1)])
            self.assertEqual(batches, [("resolver-1", 1)])


if __name__ == "__main__":
    unittest.main()
