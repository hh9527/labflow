from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from labflow.benchmark import (
    begin_batch, commit_batch, finish_stage, initialize_stage, load_bundle,
    mark_resolver_deleted, status, validate_artifact,
)
from labflow.config import ControlError


class BenchmarkStageTest(unittest.TestCase):
    def bundle(self, root: Path):
        source = root / "input"
        source.mkdir(parents=True)
        (source / "questions.jsonl").write_text(
            "\n".join(json.dumps(value, ensure_ascii=False) for value in (
                {"id": "q1", "Q": "one", "K": "private one"},
                {"id": "q2", "Q": "two", "K": "private two"},
                {"id": "q3", "Q": "three", "K": "private three"},
            )) + "\n",
            encoding="utf-8",
        )
        (source / "selected.jsonl").write_text('"q1"\n{"id":"q2"}\n"q3"\n')
        return load_bundle(source)

    def test_batches_commit_before_resolver_deletion_and_finish_to_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = self.bundle(root)
            stage = root / "stage.sqlite"
            artifact = root / "report.sqlite"
            initialize_stage(
                stage, run_id="task-1", artifact="report.bench-icm",
                input_path="input/", bundle=bundle, now=100,
            )
            begin_batch(
                stage, batch_id="batch-1", generation=1,
                session_id="session-1", case_ids=["q1", "q2"], now=110,
            )
            commit_batch(stage, batch_id="batch-1", now=120, results=[
                {
                    "id": "q1", "status": "answered", "answer": {"query": "ok"},
                    "measurements": {"duration_ms": 10, "input_tokens": 2},
                },
                {
                    "id": "q2", "status": "clarification_exhausted",
                    "clarifications": [{"question": "which?", "answer": "known"}],
                    "error": "round limit",
                },
            ])
            with self.assertRaisesRegex(ControlError, "live resolver"):
                finish_stage(stage, artifact)
            mark_resolver_deleted(stage, batch_id="batch-1", now=130)

            finish_stage(stage, artifact, now=140)

            self.assertEqual(validate_artifact(artifact), {
                "schema": "labflow.benchmark-artifact/v1",
                "run_id": "task-1",
                "outcome": "completed",
                "selected_count": 3,
            })
            with sqlite3.connect(artifact) as connection:
                cases = connection.execute(
                    "SELECT case_id, status FROM cases WHERE iter = 0 ORDER BY ordinal"
                ).fetchall()
                private = connection.execute(
                    "SELECT sql FROM sqlite_master WHERE sql LIKE '%private one%'"
                ).fetchall()
            self.assertEqual(cases, [
                ("q1", "answered"),
                ("q2", "clarification_exhausted"),
                ("q3", "not_attempted"),
            ])
            self.assertEqual(private, [])

    def test_stage_operations_are_idempotent(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = self.bundle(root)
            stage = root / "stage.sqlite"
            initialize_stage(
                stage, run_id="task-1", artifact="report.bench-icm",
                input_path="input/", bundle=bundle,
            )
            initialize_stage(
                stage, run_id="task-1", artifact="report.bench-icm",
                input_path="input/", bundle=bundle,
            )
            begin_batch(
                stage, batch_id="batch-1", generation=1,
                session_id="session-1", case_ids=["q1"],
            )
            begin_batch(
                stage, batch_id="batch-1", generation=1,
                session_id="session-1", case_ids=["q1"],
            )
            result = [{"id": "q1", "status": "failed", "error": "bad"}]
            commit_batch(stage, batch_id="batch-1", results=result)
            commit_batch(stage, batch_id="batch-1", results=result)
            mark_resolver_deleted(stage, batch_id="batch-1")
            mark_resolver_deleted(stage, batch_id="batch-1")
            self.assertEqual(status(stage)["case_statuses"], {
                "failed": 1, "pending": 2,
            })

    def test_bundle_rejects_unknown_and_duplicate_selection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = self.bundle(root)
            (bundle.path / "selected.jsonl").write_text('"q1"\n"missing"\n')
            with self.assertRaisesRegex(ControlError, "unknown id"):
                load_bundle(bundle.path)
            (bundle.path / "selected.jsonl").write_text('"q1"\n"q1"\n')
            with self.assertRaisesRegex(ControlError, "duplicate id"):
                load_bundle(bundle.path)
            (bundle.path / "selected.jsonl").write_text('"q1"\n', encoding="utf-8")
            self.assertEqual(load_bundle(bundle.path).selected, ("q1",))

    def test_unfinalized_stage_is_not_a_valid_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = self.bundle(root)
            stage = root / "stage.sqlite"
            initialize_stage(
                stage, run_id="task-1", artifact="report.bench-icm",
                input_path="input/", bundle=bundle,
            )
            with self.assertRaisesRegex(ControlError, "not finalized"):
                validate_artifact(stage)

    def test_persistent_stage_discards_unsealed_tail_and_retains_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = self.bundle(root)
            stage = root / "report.bench-icm.sqlite"
            artifact = root / "report.sqlite"

            initialize_stage(
                stage, run_id="task-1", artifact="report.bench-icm",
                input_path="input/", bundle=bundle, now=100,
            )
            finish_stage(stage, artifact, run_id="task-1", now=110)
            initialize_stage(
                stage, run_id="task-2", artifact="report.bench-icm",
                input_path="input/", bundle=bundle, now=120,
            )
            begin_batch(
                stage, batch_id="batch-1", generation=1,
                session_id="resolver-2", case_ids=["q1"], now=130,
            )
            initialize_stage(
                stage, run_id="task-3", artifact="report.bench-icm",
                input_path="input/", bundle=bundle, now=140,
            )

            with sqlite3.connect(stage) as connection:
                self.assertEqual(connection.execute(
                    "SELECT value FROM metadata WHERE key = 'iter_end'"
                ).fetchone()[0], "1")
                self.assertEqual(connection.execute(
                    "SELECT iter, run_id FROM iterations ORDER BY iter"
                ).fetchall(), [(0, "task-1"), (1, "task-3")])
                self.assertEqual(connection.execute(
                    "SELECT DISTINCT iter FROM cases ORDER BY iter"
                ).fetchall(), [(0,), (1,)])
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM batches WHERE iter >= 1"
                ).fetchone()[0], 0)

            finish_stage(stage, artifact, run_id="task-3", now=150)
            finish_stage(stage, artifact, run_id="task-3", now=160)
            with sqlite3.connect(stage) as connection:
                self.assertEqual(connection.execute(
                    "SELECT value FROM metadata WHERE key = 'iter_end'"
                ).fetchone()[0], "2")
            self.assertEqual(validate_artifact(artifact)["run_id"], "task-3")


if __name__ == "__main__":
    unittest.main()
