from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from labflow.cli_host import parser as host_parser
from labflow.cli_lab import attach_parser, parser as lab_parser, remove
from labflow.config import ControlError
from labflow.project import LAB_SCHEMA
from labflow.state import atomic_json
from labflow.supervisor import parser as supervisor_parser


class CommandSurfaceTest(unittest.TestCase):
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
        self.assertEqual(host_parser().parse_args(["pull", "--timeout", "1"]).timeout, 1)
        with self.assertRaises(SystemExit):
            host_parser().parse_args(["start", "old-lab"])

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
