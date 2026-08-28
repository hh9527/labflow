from __future__ import annotations

import os
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from labflow.cli_host import parser as host_parser
from labflow.cli_init import parser as init_parser
from labflow.cli_lab import attach_parser, parser as lab_parser, remove
from labflow.cli_query import (
    lower_om, om_parser, parser as query_parser, query, query_om,
)
from labflow.config import ControlError
from labflow.project import LAB_SCHEMA
from labflow.state import atomic_json
from labflow.supervisor import parser as supervisor_parser


class CommandSurfaceTest(unittest.TestCase):
    def test_init_requires_only_a_port(self):
        args = init_parser().parse_args(["--port", "4199"])
        self.assertEqual(args.port, 4199)

    def test_lab_has_no_name_and_attach_uses_current_project(self):
        remove_args = lab_parser().parse_args(["remove", "/tmp/labflow-example"])
        self.assertEqual(remove_args.lab_root, Path("/tmp/labflow-example"))
        self.assertEqual(attach_parser().parse_args([]).__dict__, {})
        with self.assertRaises(SystemExit):
            attach_parser().parse_args(["named-lab"])
        supervisor = supervisor_parser().parse_args(["--port", "4199", "--once"])
        self.assertEqual((supervisor.port, supervisor.once), (4199, True))

    def test_host_only_exposes_file_control_commands(self):
        self.assertEqual(host_parser().parse_args(["status"]).command, "status")
        self.assertEqual(query_parser().parse_args(["SELECT 1"]).sql, "SELECT 1")
        self.assertEqual(om_parser().parse_args(["request.json"]).input, Path("request.json"))
        self.assertEqual(om_parser().parse_args(["-"]).input, Path("-"))
        self.assertEqual(host_parser().parse_args(["pull", "--timeout", "1"]).timeout, 1)
        with self.assertRaises(SystemExit):
            host_parser().parse_args(["start", "old-lab"])

    def test_query_reads_both_databases_and_rejects_writes(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            with sqlite3.connect(home / "events.sqlite") as connection:
                connection.execute("CREATE TABLE timeline(id TEXT, type TEXT)")
                connection.execute("INSERT INTO timeline VALUES ('event-1', 'task_started')")
            with sqlite3.connect(home / "states.sqlite") as connection:
                connection.execute("CREATE TABLE state(key TEXT, value TEXT)")
                connection.execute("INSERT INTO state VALUES ('active', 'true')")

            value = query(home, "SELECT id, value FROM timeline, states.state")

            self.assertEqual(value["columns"], ["id", "value"])
            self.assertEqual(value["rows"], [["event-1", "true"]])
            self.assertFalse(value["truncated"])
            with self.assertRaisesRegex(ControlError, "read-only query failed"):
                query(home, "DELETE FROM timeline")

    def test_query_om_requires_explicit_environment(self):
        with self.assertRaisesRegex(ControlError, "TELORA_BIN, OM_LABFLOW_PATH"):
            lower_om(Path("request.json"), environ={})

    def test_query_om_lowers_parameterized_query_and_executes_read_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            home = root / "exec"
            home.mkdir()
            with sqlite3.connect(home / "events.sqlite") as connection:
                connection.execute("CREATE TABLE timeline(id TEXT, type TEXT)")
                connection.execute("INSERT INTO timeline VALUES ('event-1', 'task_started')")
                connection.execute("INSERT INTO timeline VALUES ('event-2', 'reply')")
            with sqlite3.connect(home / "states.sqlite") as connection:
                connection.execute("CREATE TABLE state(key TEXT, value TEXT)")
            telora = root / "telora"
            telora.write_text("executable", encoding="utf-8")
            telora.chmod(0o755)
            ontology = root / "om-labflow"
            ontology.mkdir()
            source = root / "request.json"
            source.write_text("{}", encoding="utf-8")
            completed = subprocess.CompletedProcess(
                [], 0,
                stdout='{"sql":"SELECT id FROM timeline WHERE type = ?","bindings":["task_started"]}',
                stderr="",
            )
            environment = {
                "TELORA_BIN": str(telora), "OM_LABFLOW_PATH": str(ontology),
            }

            with patch("labflow.cli_query.subprocess.run", return_value=completed) as run:
                value = query_om(home, source, environ=environment)

            self.assertEqual(value["rows"], [["event-1"]])
            command = run.call_args.args[0]
            self.assertEqual(command[:5], [
                str(telora), "-C", str(ontology), "run", "query",
            ])
            self.assertEqual(command[-2:], ["--source", f"input={source}"])
            self.assertNotIn("stderr", run.call_args.kwargs)

    def test_query_om_preserves_relative_paths_and_inherits_stdin(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "bin").mkdir()
            telora = root / "bin/telora"
            telora.write_text("executable", encoding="utf-8")
            telora.chmod(0o755)
            (root / "om-labflow").mkdir()
            completed = subprocess.CompletedProcess(
                [], 0, stdout='{"sql":"SELECT 1","bindings":[]}', stderr="",
            )
            previous = Path.cwd()
            try:
                os.chdir(root)
                with patch("labflow.cli_query.subprocess.run", return_value=completed) as run:
                    sql, bindings = lower_om(Path("-"), environ={
                        "TELORA_BIN": "bin/telora", "OM_LABFLOW_PATH": "om-labflow",
                    })
            finally:
                os.chdir(previous)

            self.assertEqual((sql, bindings), ("SELECT 1", []))
            self.assertEqual(run.call_args.args[0], [
                "bin/telora", "-C", "om-labflow", "run", "query",
                "--source", "input=stdin+json://",
            ])
            self.assertNotIn("stdin", run.call_args.kwargs)
            self.assertNotIn("stderr", run.call_args.kwargs)

    def test_query_om_rejects_non_query_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            telora = root / "telora"
            telora.write_text("executable", encoding="utf-8")
            telora.chmod(0o755)
            ontology = root / "om-labflow"
            ontology.mkdir()
            source = root / "request.json"
            source.write_text("{}", encoding="utf-8")
            completed = subprocess.CompletedProcess(
                [], 0, stdout='{"sql":"SELECT 1","bindings":[],"extra":true}', stderr="",
            )
            with patch("labflow.cli_query.subprocess.run", return_value=completed):
                with self.assertRaisesRegex(ControlError, "only sql and bindings"):
                    lower_om(source, environ={
                        "TELORA_BIN": str(telora), "OM_LABFLOW_PATH": str(ontology),
                    })

    def test_lab_remove_rejects_non_labflow_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            atomic_json(root / "config.json", {
                "schema": LAB_SCHEMA, "port": 4199, "root": str(root), "pid": os.getpid(),
            })
            with self.assertRaisesRegex(ControlError, "unexpected Lab root"):
                remove(root)


if __name__ == "__main__":
    unittest.main()
