from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from unittest import mock

from labflow.client import Client
from labflow.config import ControlError, Manifest, load_manifest, safe_relative, validate_identifier
from labflow.observe import failures, latest_assistant, normalized, summarize
from labflow.query import select_engine
from labflow.external import probe_direct, probe_mise, resolve_capabilities, resolve_cli, resolve_command
from labflow.state import (
    SCHEMA,
    archive_root,
    atomic_json,
    bind_plan,
    create_lab_config,
    execution_root,
    load_connect_test,
    load_lab_config,
    load_state,
    record_connect_test,
    save_state,
    validate_title,
    workspace_root,
)
from labflow.lifecycle import (
    _inherit_execution,
    _inheritance_compatible,
    copy_archive,
    export_session,
    lab_sessions,
    opencode_environment,
    next_session_title,
    prepare,
    probe_opencode_connection,
    run_validation,
)
from labflow.runtime_opencode import ENVIRONMENT, MODEL, generate
from labflow.metrics import collect_metrics
from labflow.context import Context, resolve as resolve_context
from labflow.events import event_detail, project_events
from labflow.permissions import preflight_permissions
from labflow.reporting import submit_report
from labflow.task_cli import (
    assign_task, evaluate, load_workflow, refresh_artifact, submit, task_records,
    validate_workflow,
    workflow_status,
)
from labflow.watch import WatchWindow, acp_events, message_events, watch_progress
from labflow.cli_host import (
    _abort_sessions,
    _configure_start,
    _host_pull,
    _submit,
    _resume,
    _role_output_owners,
    _status,
    _test_connect,
    _update,
    main as control_main,
    parser as control_parser,
)
from labflow.cli_lab import (
    attach_main, attach_parser, main as lab_main, parser as lab_parser,
)
from labflow.cli import main as labflow_main, parser as labflow_parser


class Handler(BaseHTTPRequestHandler):
    messages: list[dict] = []
    last_payload: dict = {}

    def log_message(self, *_args): pass

    def response(self, value, code=200):
        body = json.dumps(value).encode(); self.send_response(code); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/global/health"): self.response({"healthy": True})
        elif self.path.startswith("/session/status"): self.response({"ses_test": {"type": "idle"}})
        elif self.path.startswith("/session?"): self.response([
            {"id": "ses_test", "title": "test@1", "directory": "/tmp/ws"}
        ])
        elif "/message?" in self.path: self.response(self.messages)
        elif self.path.startswith("/broken"): self.send_response(200); self.end_headers(); self.wfile.write(b"not-json")
        else: self.response({}, 404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0)); payload = json.loads(self.rfile.read(length) or b"{}")
        self.__class__.last_payload = payload
        if self.path.startswith("/session?"): self.response({"id": "ses_test"})
        elif "/fork?" in self.path: self.response({"id": "ses_fork"})
        elif "/prompt_async?" in self.path:
            text = payload["parts"][0]["text"]
            self.messages.append({"info": {"id": f"usr_{len(self.messages)}", "role": "user", "time": {"created": 1}}, "parts": [{"type": "text", "text": text}]})
            self.response(None)
        else: self.response({}, 404)

    def do_PATCH(self):
        length = int(self.headers.get("Content-Length", 0))
        payload = json.loads(self.rfile.read(length) or b"{}")
        self.__class__.last_payload = payload
        if self.path.startswith("/session/"):
            self.response({"id": "ses_test", **payload})
        else:
            self.response({}, 404)


class ServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Handler.messages = []; cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler); cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True); cls.thread.start()
        cls.client = Client(f"http://127.0.0.1:{cls.server.server_port}", "/tmp/ws", "ses_test")

    @classmethod
    def tearDownClass(cls): cls.server.shutdown(); cls.server.server_close(); cls.thread.join()

    def test_contract(self):
        self.assertTrue(self.client.health()["healthy"]); self.assertEqual(self.client.status()["type"], "idle")
        self.assertEqual(self.client.sessions()[0]["title"], "test@1")
        self.assertEqual(Client(self.client.url, "/tmp/ws").create_session("test")["id"], "ses_test")
        Client(self.client.url, "/tmp/ws").create_session("child", "ses_parent")
        self.assertEqual(Handler.last_payload, {"title": "child", "parentID": "ses_parent"})
        self.client.prompt("hello"); self.assertEqual(self.client.messages()[-1]["parts"][0]["text"], "hello")
        self.client.prompt_session("ses_test", "continue"); self.assertEqual(self.client.messages()[-1]["parts"][0]["text"], "continue")
        self.assertEqual(self.client.fork_session("ses_test")["id"], "ses_fork")
        self.client.prompt_session("ses_test", "role", agent="a5")
        self.assertEqual(Handler.last_payload["agent"], "a5")
        self.client.update_session("ses_test", {"time": {"archived": 123}})
        self.assertEqual(Handler.last_payload, {"time": {"archived": 123}})

    def test_loopback_only(self):
        with self.assertRaises(ControlError): Client("http://example.com:12", "/tmp/ws")

    def test_unavailable(self):
        with self.assertRaises(ControlError): Client("http://127.0.0.1:1", "/tmp/ws", timeout=.01).health()


class ConfigStateTest(unittest.TestCase):
    @staticmethod
    def write_plan(plan: Path) -> None:
        plan.mkdir(parents=True)
        (plan / "roles").mkdir()
        (plan / "host").mkdir()
        (plan / "roles" / "a1.md").write_text("Complete delivered work.\n", encoding="utf-8")
        (plan / "host" / "secret.md").write_text("hidden\n", encoding="utf-8")
        (plan / "seed.txt").write_text("seed\n", encoding="utf-8")
        (plan / "experiment.json").write_text(json.dumps({
            "schema": "labflow.experiment-plan/v1",
            "workspace": ["seed.txt"],
            "roles": {"a1": {
                "description": "worker", "instructions": "roles/a1.md",
                "commands": ["labflow agent submit a1 *"],
                "preflight": ["labflow agent submit a1 *"],
            }},
            "assets": [{"source": "tool", "path": "bin/tool", "mode": "0555"}],
            "validation": [], "observe": ["bin"],
            "workflow": {
                "schema": "labflow.workflow/v1",
                "roles": ["a1"],
                "artifacts": {
                    "input": {"desc": "input", "assets": [
                        {"path": "seed.txt", "level": 0},
                        {"path": "bin/tool", "level": 0}
                    ]},
                    "output.a1": {"desc": "output", "input": ["input"],
                                  "assets": ["output.txt"], "instruction": "produce output"}
                }
            }
        }))

    @staticmethod
    def commit_repo(repo: Path) -> None:
        subprocess.run(["git", "init", "--quiet"], cwd=repo, check=True)
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(["git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
                        "commit", "--quiet", "-m", "plan"], cwd=repo, check=True)

    def test_identifiers_and_paths(self):
        self.assertEqual(validate_identifier("a2-001", "exec"), "a2-001")
        for value in ("../x", "/x", ".", "a/../b"):
            with self.assertRaises(ControlError): safe_relative(value)
        for value in ("A", "a/b", ".hidden", "a b"):
            with self.assertRaises(ControlError): validate_identifier(value, "id")

    def test_manifest_preserves_project_specific_preflight_commands(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            plan = repo / "experiment-plans" / "demo"
            self.write_plan(plan)
            path = plan / "experiment.json"
            manifest = json.loads(path.read_text(encoding="utf-8"))
            manifest["roles"]["a1"]["preflight"] = ["./bin/compiler types --limit 20"]
            path.write_text(json.dumps(manifest), encoding="utf-8")
            self.assertEqual(
                load_manifest(repo, "demo").permission_preflight["a1"],
                ("./bin/compiler types --limit 20",),
            )

    def test_artifact_publication_command_is_available(self):
        args = control_parser().parse_args(["submit", "t1", "demo@1", "output-1", "result"])
        self.assertEqual((args.command, args.lab_name, args.title, args.artifacts),
                         ("submit", "t1", "demo@1", ["output-1", "result"]))
        forced = control_parser().parse_args(["submit", "t1", "demo@1", "output-1", "--force"])
        self.assertTrue(forced.force)
        forced_update = control_parser().parse_args(
            ["update", "t1", "demo@1", "output.txt=input.txt", "--force"]
        )
        self.assertTrue(forced_update.force)
        forced_resume = control_parser().parse_args(["resume", "t1", "demo@1", "a1", "--force"])
        self.assertTrue(forced_resume.force)

    def test_stat_command_is_available(self):
        args = control_parser().parse_args(["stat", "t1", "demo@1"])
        self.assertEqual((args.command, args.lab_name, args.title),
                         ("stat", "t1", "demo@1"))

    def test_control_surface_includes_connection_preflight(self):
        self.assertEqual(set(control_parser()._subparsers._group_actions[0].choices),
                         {"test-connect", "start", "stat", "status", "pull", "event",
                          "update", "submit", "resume", "abort-sessions"})
        args = control_parser().parse_args(["pull", "t1", "demo@1", "123", "--timeout", "5"])
        self.assertEqual((args.title, args.since, args.timeout), ("demo@1", 123, 5.0))
        event = control_parser().parse_args(["event", "t1", "demo@1", "task:a1-1"])
        self.assertEqual((event.title, event.event_id), ("demo@1", "task:a1-1"))
        abort = control_parser().parse_args(["abort-sessions", "t1", "demo@1"])
        self.assertEqual((abort.command, abort.title), ("abort-sessions", "demo@1"))
        connect = control_parser().parse_args(["test-connect", "t1"])
        self.assertEqual(connect.lab_name, "t1")

    def test_connection_preflight_can_reuse_an_existing_lab(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = repo / "lab"; lab_root.mkdir()
            create_lab_config(repo, "old-lab", 4205, lab_root)
            with mock.patch(
                "labflow.cli_host.probe_opencode_connection",
                return_value={"health": True, "session_id": "ses_probe", "title": "connect@1"},
            ) as probe:
                result = _test_connect(repo, "old-lab")
            self.assertEqual(result["lab_name"], "old-lab")
            probe.assert_called_once_with(
                "old-lab", 4205, lab_root / "connection"
            )

    def test_resume_targets_an_existing_inactive_role(self):
        client = mock.Mock()
        client.children.return_value = [{"id": "ses_a5", "agent": "a5"}]
        client.statuses.side_effect = [
            {"ses_a5": {"type": "idle"}},
            {"ses_a5": {"type": "busy"}},
        ]
        client.create_session.return_value = {"id": "ses_a5_new"}
        context = mock.Mock()
        context.state = {"title": "demo@1", "workflow": {"roles": ["a5"]},
                         "session_id": "ses_coordinator", "execution_base": "demo",
                         "lab_root": "/tmp/lab"}
        client.sessions.return_value = []
        context.client.return_value = client

        result = _resume(context, "a5")
        self.assertEqual(result["session_id"], "ses_a5")
        self.assertEqual((result["action"], result["managed_by"]),
                         ("retained", "supervisor"))
        client.prompt_session.assert_not_called()

    def test_resume_is_idempotent_for_a_busy_role(self):
        client = mock.Mock()
        client.children.return_value = [{"id": "ses_a5", "agent": "a5"}]
        client.statuses.return_value = {"ses_a5": {"type": "busy"}}
        context = mock.Mock()
        context.state = {"title": "demo@1", "workflow": {"roles": ["a5"]}}
        context.client.return_value = client

        self.assertEqual(_resume(context, "a5")["action"], "already_running")
        client.prompt_session.assert_not_called()

    def test_force_resume_aborts_and_restarts_a_busy_role(self):
        client = mock.Mock()
        old = {"id": "ses_a5_old", "agent": "a5"}
        new = {"id": "ses_a5_new", "agent": "a5"}
        client.children.side_effect = [[old], [old, new]]
        client.statuses.side_effect = [
            {"ses_a5_old": {"type": "busy"}},
            {"ses_a5_old": {"type": "idle"}},
            {"ses_a5_old": {"type": "idle"}, "ses_a5_new": {"type": "busy"}},
        ]
        client.create_session.return_value = {"id": "ses_a5_new"}
        context = mock.Mock()
        context.state = {"title": "demo@1", "workflow": {"roles": ["a5"]},
                         "session_id": "ses_coordinator", "execution_base": "demo",
                         "lab_root": "/tmp/lab"}
        client.sessions.return_value = []
        context.client.return_value = client

        result = _resume(context, "a5", force=True)

        self.assertEqual((result["action"], result["session_id"]),
                         ("recreated", "ses_a5_new"))
        client.abort_session.assert_called_once_with("ses_a5_old")
        client.prompt_session.assert_not_called()
        client.create_session.assert_called_once_with(
            "demo.a5@1", parent_id="ses_coordinator", agent="a5"
        )

    def test_resume_rejects_unknown_role_and_recreates_missing_session(self):
        context = mock.Mock()
        context.state = {"title": "demo@1", "workflow": {"roles": ["a5"]}}

        with self.assertRaisesRegex(ControlError, "unknown workflow role"):
            _resume(context, "a4")

        context.state["session_id"] = "ses_coordinator"
        context.state["execution_base"] = "demo"
        context.state["lab_root"] = "/tmp/lab"
        client = mock.Mock()
        client.children.side_effect = [[], [{"id": "ses_a5_new", "agent": "a5"}]]
        client.statuses.side_effect = [{}, {"ses_a5_new": {"type": "busy"}}]
        client.create_session.return_value = {"id": "ses_a5_new"}
        client.sessions.return_value = []
        context.client.return_value = client
        result = _resume(context, "a5")
        self.assertEqual((result["action"], result["session_id"]), ("recreated", "ses_a5_new"))
        client.prompt_session.assert_not_called()
        client.create_session.assert_called_once_with(
            "demo.a5@1", parent_id="ses_coordinator", agent="a5"
        )

    def test_resume_retains_an_existing_idle_supervisor_managed_session(self):
        client = mock.Mock()
        old = {"id": "ses_a5_old", "agent": "a5"}
        new = {"id": "ses_a5_new", "agent": "a5"}
        client.children.side_effect = [[old], [old], [old, new]]
        client.statuses.side_effect = [
            {"ses_a5_old": {"type": "idle"}},
            {"ses_a5_old": {"type": "idle"}},
            {"ses_a5_old": {"type": "idle"}, "ses_a5_new": {"type": "busy"}},
        ]
        client.create_session.return_value = {"id": "ses_a5_new"}
        context = mock.Mock()
        context.state = {"title": "demo@1", "workflow": {"roles": ["a5"]},
                         "session_id": "ses_coordinator", "execution_base": "demo",
                         "lab_root": "/tmp/lab"}
        client.sessions.return_value = []
        context.client.return_value = client
        result = _resume(context, "a5")
        self.assertEqual((result["action"], result["session_id"]),
                         ("retained", "ses_a5_old"))
        client.prompt_session.assert_not_called()
        client.create_session.assert_not_called()

    def test_start_requires_lab_and_plan_identity(self):
        args = control_parser().parse_args(["start", "t1", "sample-plan"])
        self.assertEqual(
            (args.command, args.lab_name, args.plan_id, args.variant, args.from_title),
            ("start", "t1", "sample-plan", None, None),
        )
        inherited = control_parser().parse_args(
            ["start", "t1", "sample-plan", "--variant", "trial",
             "--from", "sample-plan@1"]
        )
        self.assertEqual(inherited.variant, "trial")
        self.assertEqual(inherited.from_title, "sample-plan@1")

    def test_abort_sessions_stops_busy_execution_tree_without_deleting_history(self):
        client = mock.Mock()
        client.children.side_effect = lambda session_id=None: {
            "ses_root": [{"id": "ses_a5"}, {"id": "ses_idle"}],
            "ses_a5": [{"id": "ses_nested"}],
        }.get(session_id, [])
        client.statuses.side_effect = [
            {
                "ses_root": {"type": "idle"},
                "ses_a5": {"type": "busy"},
                "ses_idle": {"type": "idle"},
                "ses_nested": {"type": "busy"},
            },
            {
                "ses_root": {"type": "idle"},
                "ses_a5": {"type": "idle"},
                "ses_idle": {"type": "idle"},
                "ses_nested": {"type": "idle"},
            },
        ]
        context = mock.Mock()
        context.state = {"title": "demo@1", "session_id": "ses_root"}
        context.client.return_value = client

        result = _abort_sessions(context)

        self.assertEqual(set(result["sessions"]),
                         {"ses_root", "ses_a5", "ses_idle", "ses_nested"})
        self.assertEqual(set(result["aborted"]), {"ses_a5", "ses_nested"})
        self.assertEqual(set(result["already_idle"]), {"ses_root", "ses_idle"})
        self.assertEqual(
            {call.args[0] for call in client.abort_session.call_args_list},
            {"ses_a5", "ses_nested"},
        )

    def test_lab_run_accepts_name_and_optional_port(self):
        args = lab_parser().parse_args(["run", "t1", "--port", "4199"])
        self.assertEqual((args.command, args.lab_name, args.port), ("run", "t1", 4199))
        remove = lab_parser().parse_args(["remove", "t1"])
        self.assertEqual((remove.command, remove.lab_name), ("remove", "t1"))
        self.assertEqual(attach_parser().parse_args(["t1"]).lab_name, "t1")

    def test_labflow_groups_runtime_commands(self):
        self.assertEqual(
            set(labflow_parser()._subparsers._group_actions[0].choices),
            {"lab", "attach", "host", "agent", "supervisor"},
        )
        cases = (
            ("lab", "labflow.cli.cli_lab.main", ["run", "t1"]),
            ("attach", "labflow.cli.cli_lab.attach_main", ["t1"]),
            ("host", "labflow.cli.cli_host.main", ["status", "t1", "demo@1"]),
            ("agent", "labflow.cli.task_cli.main", ["status"]),
            ("supervisor", "labflow.cli.supervisor.main", ["t1", "--once"]),
        )
        for group, target, arguments in cases:
            with self.subTest(group=group), mock.patch(target, return_value=0) as delegated:
                self.assertEqual(labflow_main([group, *arguments]), 0)
                delegated.assert_called_once_with(arguments, prog=f"labflow {group}")

    def test_run_reports_an_occupied_port_before_waiting_for_host(self):
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen(1)
            port = occupied.getsockname()[1]
            stderr = StringIO()
            with mock.patch(
                "labflow.cli_lab.resolve_cli", return_value=("opencode",)
            ), redirect_stderr(stderr):
                result = lab_main(["run", "t1", "--port", str(port)])
        self.assertEqual(result, 69)
        self.assertIn(f"cannot reserve lab port {port}", stderr.getvalue())

    def test_run_only_hosts_the_headless_daemon(self):
        with tempfile.TemporaryDirectory() as temporary:
            stdout = StringIO()
            with mock.patch(
                "labflow.cli_lab.repository_root",
                return_value=Path(temporary),
            ), mock.patch(
                "labflow.cli_lab.resolve_cli", return_value=("opencode",)
            ), mock.patch(
                "labflow.cli_lab.os.chdir"
            ) as chdir, mock.patch(
                "labflow.cli_lab.os.dup2"
            ), mock.patch(
                "labflow.cli_lab.os.execvpe"
            ) as execvpe, redirect_stdout(stdout):
                result = lab_main(["run", "t1", "--port", "4199"])
            self.assertEqual(result, 0)
            self.assertTrue((Path(temporary) / ".labs/t1").is_symlink())
            chdir.assert_called_once()
            self.assertIn("serve", execvpe.call_args.args[1])
            self.assertIn("Lab t1 is starting", stdout.getvalue())
            shutil.rmtree(chdir.call_args.args[0])

    def test_remove_reclaims_a_stopped_lab(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = Path(tempfile.mkdtemp(prefix="labflow-t1-", dir="/tmp"))
            create_lab_config(repo, "t1", 4199, lab_root)
            with mock.patch(
                "labflow.cli_lab.repository_root", return_value=repo
            ), mock.patch(
                "labflow.cli_lab.socket.create_connection", side_effect=ConnectionRefusedError
            ):
                self.assertEqual(lab_main(["remove", "t1"]), 0)
            self.assertFalse((repo / ".labs/t1").is_symlink())
            self.assertFalse(lab_root.exists())

    def test_remove_rejects_a_running_lab(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = Path(tempfile.mkdtemp(prefix="labflow-t1-", dir="/tmp"))
            create_lab_config(repo, "t1", 4199, lab_root)
            connection = mock.MagicMock()
            stderr = StringIO()
            with mock.patch(
                "labflow.cli_lab.repository_root", return_value=repo
            ), mock.patch(
                "labflow.cli_lab.socket.create_connection", return_value=connection
            ), redirect_stderr(stderr):
                self.assertEqual(lab_main(["remove", "t1"]), 75)
            self.assertTrue((repo / ".labs/t1").is_symlink())
            self.assertTrue(lab_root.exists())
            shutil.rmtree(lab_root)
            (repo / ".labs/t1").unlink()

    def test_attach_leaves_session_selection_to_the_tui(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = repo / "lab"; lab_root.mkdir()
            create_lab_config(repo, "t1", 4199, lab_root)
            completed = mock.Mock(returncode=0)
            with mock.patch(
                "labflow.cli_lab.repository_root", return_value=repo
            ), mock.patch(
                "labflow.cli_lab.resolve_cli", return_value=("opencode",)
            ), mock.patch(
                "labflow.cli_lab.opencode_environment", return_value=ENVIRONMENT
            ), mock.patch(
                "labflow.cli_lab.subprocess.run", return_value=completed
            ) as run:
                result = attach_main(["t1"])
        self.assertEqual(result, 0)
        run.assert_called_once_with(
            ["opencode", "attach", "http://127.0.0.1:4199"],
            cwd=str(lab_root), env=ENVIRONMENT,
        )

    def test_control_start_prepares_and_creates_the_session(self):
        repo = Path("/repo")
        root = Path("/tmp/lab/control/sample-plan@2")
        prepared = {"phase": "preparing"}
        context = mock.Mock()
        client = mock.Mock()
        client.sessions.return_value = [{"title": "sample-plan@1"}]
        output = StringIO()
        with mock.patch(
            "labflow.cli_host._controller_repo", return_value=repo
        ), mock.patch(
            "labflow.cli_host._configure_start",
            return_value={"port": 4199, "root": "/tmp/lab", "lab_name": "t1",
                          "bundle": None},
        ), mock.patch("labflow.cli_host.Client", return_value=client), mock.patch(
            "labflow.cli_host.prepare",
            return_value=(root, prepared, True),
        ) as prepare_call, mock.patch(
            "labflow.cli_host.create_execution_session",
            return_value={"phase": "ready"},
        ) as create_session, mock.patch(
            "labflow.cli_host.resolve", return_value=context
        ), mock.patch(
            "labflow.cli_host._start", return_value={"kind": "initial"}
        ), redirect_stdout(output):
            result = control_main(["start", "t1", "sample-plan"])
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output.getvalue())["title"], "sample-plan@2")
        prepare_call.assert_called_once_with(
            "sample-plan", "sample-plan@2", 4199, from_title=None,
            lab_name="t1", lab_root="/tmp/lab"
        )
        create_session.assert_called_once_with(root, prepared, "sample-plan@2")

    def test_host_configures_the_explicit_plan_and_runner_port(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            plan = repo / "experiment-plans" / "demo"
            self.write_plan(plan)
            lab_root = repo / "lab"; lab_root.mkdir()
            create_lab_config(repo, "t1", 43123, lab_root)
            record_connect_test("t1", lab_root, {
                "health": True, "session_id": "ses_probe", "title": "connect@1"
            })
            value = _configure_start(repo, "t1", "demo")
            self.assertEqual(value["lab_name"], "t1")
            self.assertEqual(value["port"], 43123)

    def test_inheritance_copies_only_current_unchanged_artifact_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = repo / "lab"; lab_root.mkdir()
            plan = repo / "experiment-plans" / "demo"
            plan.mkdir(parents=True)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1",
                "roles": ["a1", "a5"],
                "artifacts": {
                    "input-0": {"desc": "input",
                                "assets": [{"path": "GOAL.md", "level": 0}]},
                    "input-1": {"desc": "optional input",
                                "assets": [{"path": "NOTES.md", "level": 1}]},
                    "output-1.a1": {"desc": "output",
                                 "input": ["input-0", "input-1?"],
                                 "assets": ["output.txt"], "instruction": "build"},
                    "output-2": {"desc": "host output", "input": ["output-1.a1"]},
                    "output-3.a5": {"desc": "role output",
                                 "input": ["output-2"], "instruction": "work"},
                    "output-4": {"desc": "final output", "input": ["output-3.a5"]},
                },
            })
            source_root = bind_plan(lab_root, "demo", "demo@1")
            source_workspace = workspace_root(lab_root, "demo@1")
            source_workspace.mkdir(parents=True)
            (source_workspace / "GOAL.md").write_text("old language", encoding="utf-8")
            refresh_artifact(source_workspace, workflow, "input-0")
            (source_workspace / "NOTES.md").write_text("process notes", encoding="utf-8")
            refresh_artifact(source_workspace, workflow, "input-1")
            (source_workspace / "output.txt").write_text("result", encoding="utf-8")
            assign_task(source_workspace, workflow, "a1", "output-1.a1")
            submit(source_workspace, workflow, "a1", ["output-1.a1"])
            refresh_artifact(source_workspace, workflow, "output-2")
            save_state(source_root, {
                "schema": SCHEMA, "plan_id": "demo", "title": "demo@1", "phase": "idle",
                "workspace": str(source_workspace), "workflow": workflow,
            })

            target = lab_root / "target-workspace"
            target.mkdir()
            (target / "GOAL.md").write_text("old language", encoding="utf-8")
            result = _inherit_execution(lab_root, "demo@1", "demo", target, workflow)
            self.assertEqual(result["artifacts"], ["input-0", "input-1", "output-1.a1", "output-2"])
            self.assertEqual((target / "output.txt").read_text(), "result")
            self.assertEqual((target / "NOTES.md").read_text(), "process notes")
            self.assertEqual((target / "GOAL.md").read_text(), "old language")
            status = evaluate(target, workflow)["artifacts"]
            self.assertTrue(status["output-2"]["current"])
            self.assertTrue(status["output-3.a5"]["runnable"])
            self.assertFalse((target / ".labflow" / "active").exists())
            self.assertFalse((target / ".labflow" / "history").exists())

    def test_inheritance_requires_an_identical_artifact_definition(self):
        old = {
            "id": "output-2", "desc": "output", "owner": "host",
            "input": [{"id": "output-1", "optional": False}],
            "assets": [], "instruction": None,
        }
        new = dict(old)
        self.assertTrue(_inheritance_compatible(old, new, {}, {}))
        new["input"] = [*old["input"], {"id": "output-extra", "optional": False}]
        self.assertFalse(_inheritance_compatible(old, new, {}, {}))
        new["desc"] = "changed semantics"
        self.assertFalse(_inheritance_compatible(old, new, {}, {}))

    def test_lab_configuration_records_port_and_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = repo / "lab"; lab_root.mkdir()
            value = create_lab_config(repo, "t1", 4199, lab_root)
            self.assertEqual(load_lab_config(repo, "t1"), value)
            self.assertEqual(value["port"], 4199)
            self.assertEqual(value["root"], str(lab_root))
            self.assertEqual((repo / ".labs/t1").resolve(), lab_root)
            self.assertEqual(json.loads((lab_root / "config.json").read_text()), {
                "schema": "labflow.lab/v1", "name": "t1", "port": 4199,
                "host_workspace": str(repo),
            })
            with self.assertRaisesRegex(ControlError, "configured differently"):
                create_lab_config(repo, "t1", 4200, lab_root)

    def test_context_resolves_a_session_inside_its_named_lab(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = repo / "lab"; lab_root.mkdir()
            create_lab_config(repo, "t1", 4199, lab_root)
            root = bind_plan(lab_root, "demo", "demo@1")
            save_state(root, {
                "schema": SCHEMA, "plan_id": "demo", "title": "demo@1",
                "lab_name": "t1", "lab_root": str(lab_root), "phase": "idle",
                "workspace": str(workspace_root(lab_root, "demo@1")),
            })
            manifest = mock.Mock()
            with mock.patch(
                "labflow.context.repository_root", return_value=repo
            ), mock.patch(
                "labflow.context.load_manifest", return_value=manifest
            ):
                context = resolve_context("t1", "demo@1")
            self.assertEqual((context.root, context.manifest), (root, manifest))

            state = load_state(root); state["lab_name"] = "other"; save_state(root, state)
            with mock.patch(
                "labflow.context.repository_root", return_value=repo
            ), mock.patch(
                "labflow.context.load_manifest", return_value=manifest
            ), self.assertRaisesRegex(ControlError, "lab identity mismatch"):
                resolve_context("t1", "demo@1")

    def test_start_requires_a_connection_test_before_freezing_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            plan = repo / "experiment-plans" / "demo"
            self.write_plan(plan)
            lab_root = repo / "lab"; lab_root.mkdir()
            create_lab_config(repo, "t1", 43123, lab_root)
            with self.assertRaisesRegex(ControlError, "test-connect"):
                _configure_start(repo, "t1", "demo")

    def test_start_requires_an_active_lab(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            plan = repo / "experiment-plans" / "demo"
            self.write_plan(plan)
            with self.assertRaisesRegex(ControlError, "labflow lab run t1"):
                _configure_start(repo, "t1", "demo")

    def test_connect_records_a_receipt_inside_the_lab(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            lab_root = repo / "lab"; lab_root.mkdir()
            create_lab_config(repo, "t1", 4199, lab_root)
            with mock.patch(
                "labflow.cli_host.probe_opencode_connection",
                return_value={"health": True, "session_id": "ses_probe", "title": "connect@1"},
            ) as probe:
                receipt = _test_connect(repo, "t1")
            self.assertEqual(load_connect_test("t1", lab_root), receipt)
            probe.assert_called_once_with("t1", 4199, lab_root / "connection")

    def test_connection_probe_exercises_the_runner_health_and_session(self):
        client = mock.Mock()
        client.health.return_value = {"healthy": True}
        client.create_session.return_value = {"id": "ses_probe"}
        client.sessions.return_value = [{"title": "connect@1"}]
        workspace = Path("/tmp/lab/connection")
        with mock.patch("labflow.lifecycle.Client", return_value=client) as factory:
            result = probe_opencode_connection("t1", 4199, workspace)
        self.assertEqual(result, {
            "health": True, "session_id": "ses_probe", "title": "connect@2"
        })
        factory.assert_called_once_with("http://127.0.0.1:4199", str(workspace), timeout=0.5)
        client.health.assert_called_once()
        client.create_session.assert_called_once_with("connect@2")

    def test_lab_sessions_merge_workspaces_and_allocate_global_generation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            connection = root / "connection"; connection.mkdir()
            workspace = root / "ws" / "demo@1"
            workspace.mkdir(parents=True)
            primary = mock.Mock(workspace=str(connection), url="http://127.0.0.1:4199",
                                timeout=5)
            primary.sessions.return_value = [{
                "id": "ses_connect", "title": "connect@1",
                "directory": str(connection),
            }]
            execution_client = mock.Mock()
            execution_client.sessions.return_value = [{
                "id": "ses_demo", "title": "demo@1", "directory": str(workspace),
            }]
            with mock.patch("labflow.lifecycle.Client",
                            return_value=execution_client):
                records = lab_sessions(primary, root)
                title = next_session_title(primary, "demo", root)
            self.assertEqual({item["id"] for item in records}, {"ses_connect", "ses_demo"})
            self.assertEqual(title, "demo@2")

    def test_update_copies_and_removes_workspace_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            host = root / "host"
            workspace = root / "workspace"
            host.mkdir()
            workspace.mkdir()
            (host / "notes.md").write_text("revise", encoding="utf-8")
            context = mock.Mock(state={"phase": "idle", "workspace": str(workspace)})
            with mock.patch("labflow.cli_host.Path.cwd", return_value=host):
                _update(context, ["docs/NOTES.md=notes.md"])
                self.assertEqual((workspace / "docs/NOTES.md").read_text(), "revise")
                _update(context, ["docs/NOTES.md=!"])
                self.assertFalse((workspace / "docs/NOTES.md").exists())

    def test_update_accepts_absolute_host_source_outside_repository(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            source = root / "host-input.md"
            workspace.mkdir()
            source.write_text("external input", encoding="utf-8")
            context = mock.Mock(state={"phase": "idle", "workspace": str(workspace)})

            result = _update(context, [f"docs/INPUT.md={source}"])

            self.assertEqual((workspace / "docs/INPUT.md").read_text(), "external input")
            self.assertEqual(result[0]["source"], str(source))

    def test_update_preserves_source_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            source = root / "tool"
            workspace.mkdir()
            source.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            source.chmod(0o775)
            context = mock.Mock(state={"phase": "idle", "workspace": str(workspace)})

            result = _update(context, [f"bin/tool={source}"])

            destination = workspace / "bin/tool"
            self.assertEqual(destination.stat().st_mode & 0o7777, 0o775)
            self.assertEqual(result[0]["mode"], "0775")

    def test_update_still_rejects_unsafe_destination(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            source = root / "input.md"
            workspace.mkdir()
            source.write_text("input", encoding="utf-8")
            context = mock.Mock(state={"phase": "idle", "workspace": str(workspace)})

            with self.assertRaisesRegex(ControlError, "unsafe destination"):
                _update(context, [f"../escaped.md={source}"])
            self.assertFalse((root / "escaped.md").exists())

    def test_update_replaces_and_removes_directory_assets(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            source = root / "source"
            workspace.mkdir()
            source.mkdir()
            (source / "new.txt").write_text("new", encoding="utf-8")
            (workspace / "tree").mkdir()
            (workspace / "tree" / "old.txt").write_text("old", encoding="utf-8")
            context = mock.Mock(state={"phase": "idle", "workspace": str(workspace)})

            result = _update(context, [f"tree/={source}"])
            self.assertEqual(result[0]["path"], "tree/")
            self.assertTrue((workspace / "tree" / "new.txt").is_file())
            self.assertFalse((workspace / "tree" / "old.txt").exists())
            removed = _update(context, ["tree/=!"])
            self.assertTrue(removed[0]["removed"])
            self.assertFalse((workspace / "tree").exists())

    def test_role_output_directory_asset_contains_nested_paths(self):
        workflow = {
            "artifacts": {
                "output-1": {
                    "owner": "a3",
                    "assets": [{"path": "model/", "level": 2}],
                },
            },
        }
        self.assertEqual(_role_output_owners(workflow, "model/root.code"), ["a3"])
        self.assertEqual(_role_output_owners(workflow, "model/src/main.code"), ["a3"])

    def test_force_update_and_submit_cross_role_ownership_and_record_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            workspace.mkdir()
            source = root / "replacement.txt"
            source.write_text("replacement", encoding="utf-8")
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1",
                "roles": ["a1"],
                "artifacts": {
                    "input-0": {"desc": "input"},
                    "output-1.a1": {"desc": "output", "input": ["input-0"],
                                 "assets": ["output.txt"], "instruction": "build"},
                    "output-2": {"desc": "final", "input": ["output-1.a1"]},
                },
            })
            context = mock.Mock()
            context.root = root / "execution"
            context.state = {"title": "demo@1", "phase": "idle",
                             "workspace": str(workspace), "workflow": workflow}
            with self.assertRaisesRegex(ControlError, "requires --force"):
                _update(context, [f"output.txt={source}"])
            updated = _update(context, [f"output.txt={source}"], force=True)
            self.assertTrue(updated[0]["host_forced"])
            removed = _submit(context, ["output-1.a1=!"], force=True)
            self.assertTrue(removed[0]["host_forced"])
            events = list((context.root / "host-interventions").glob("*.json"))
            self.assertEqual(len(events), 2)
            self.assertEqual(
                len(list((workspace / "control/host-interventions").glob("*.json"))), 2
            )

    def test_atomic_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root / "plan").write_text("plan\n")
            state = {"schema": SCHEMA, "plan_id": "plan", "title": "plan@1", "phase": "ready"}; save_state(root, state)
            self.assertEqual(load_state(root), state)

    def test_opencode_environment_is_adapter_owned(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary); plan = repo / "experiment-plans" / "demo"
            self.write_plan(plan)
            manifest = load_manifest(repo, "demo")
            self.assertNotIn("environment", json.loads((plan / "experiment.json").read_text()))
            self.assertEqual(opencode_environment({})["OPENCODE_EXPERIMENTAL_OUTPUT_TOKEN_MAX"],
                             ENVIRONMENT["OPENCODE_EXPERIMENTAL_OUTPUT_TOKEN_MAX"])

    def test_manifest_validates_metrics(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary); plan = repo / "experiment-plans" / "demo"
            self.write_plan(plan)
            path = plan / "experiment.json"
            data = json.loads(path.read_text())
            data["metrics"] = {"roles": {"a1": {
                "learning_phases": ["language_learning"],
                "work_phase": "implementation",
                "work_files": ["output/src/main.code"],
                "artifacts": {
                    "code": {"core": ["output/src/*.code"]},
                    "documents": {"docs": ["output/NOTES.md"]},
                },
            }}}
            path.write_text(json.dumps(data))
            metrics = load_manifest(repo, "demo").metrics
            self.assertEqual(metrics["roles"]["a1"]["work_phase"], "implementation")
            self.assertEqual(metrics["roles"]["a1"]["artifacts"]["code"]["core"], ["output/src/*.code"])

    def test_prepare_copies_tracked_plan_and_generates_runtime_adapter(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary); plan = repo / "experiment-plans" / "demo"; self.write_plan(plan)
            lab_root = repo / "lab"; lab_root.mkdir()
            artifact = repo / "tool"; artifact.write_text("tool")
            self.commit_repo(repo)
            git = ("git",)
            with mock.patch("labflow.lifecycle.repository_root", return_value=repo), \
                 mock.patch("labflow.lifecycle.git_metadata", return_value=("rev", False)), \
                 mock.patch("labflow.lifecycle.resolve_cli", return_value=git), \
                 mock.patch("labflow.lifecycle.subprocess.run", wraps=subprocess.run):
                _root, state, created = prepare(
                    "demo", "demo@1", 4567, lab_name="t1", lab_root=str(lab_root)
                )
            self.assertTrue(created)
            workspace = Path(state["workspace"])
            self.assertTrue((workspace / "experiment.json").is_file())
            self.assertNotIn(
                "owner",
                json.loads((workspace / "experiment.json").read_text())["workflow"]
                ["artifacts"]["input"],
            )
            self.assertEqual(load_workflow(workspace), state["workflow"])
            self.assertTrue((workspace / ".opencode/agents/a1.md").is_file())
            role_text = (workspace / ".opencode/agents/a1.md").read_text()
            coordinator_text = (workspace / ".opencode/agents/coordinator.md").read_text()
            self.assertNotIn("labflow agent pull", role_text)
            self.assertIn("Supervisor 会主动投递", role_text)
            self.assertIn('"task":"deny"', coordinator_text)
            self.assertIn("不要启动 sub-agent", coordinator_text)
            self.assertEqual((workspace / "bin/tool").read_text(), "tool")
            self.assertEqual((workspace / "seed.txt").read_text(), "seed\n")
            self.assertFalse((workspace / "host/secret.md").exists())
            self.assertEqual(state["opencode_environment"], ENVIRONMENT)
            self.assertEqual(set(state["permission_preflight"]), {"a1"})
            self.assertEqual(state["reporting"], {"sinks": []})
            self.assertEqual(state["metrics"], {"roles": {}})
            self.assertEqual(workspace, workspace_root(lab_root, "demo@1"))
            self.assertEqual(_root, execution_root(lab_root, "demo@1"))
            self.assertEqual(Path(state["archive"]), archive_root(lab_root, "demo@1"))
            artifact_link = workspace / "control" / "artifacts"
            artifact_root = lab_root / "supervisor" / "demo@1" / "artifacts"
            self.assertTrue(artifact_link.is_symlink())
            self.assertEqual(artifact_link.resolve(), artifact_root.resolve())

    def test_prepare_reuses_a_named_session_workspace(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary); plan = repo / "experiment-plans" / "demo"; self.write_plan(plan)
            lab_root = repo / "lab"; lab_root.mkdir()
            artifact = repo / "tool"; artifact.write_text("tool")
            self.commit_repo(repo)
            with mock.patch("labflow.lifecycle.repository_root", return_value=repo), \
                 mock.patch("labflow.lifecycle.git_metadata", return_value=("repo-rev", False)):
                root, prepared, created = prepare(
                    "demo", "demo@1", 4567, lab_name="t1", lab_root=str(lab_root)
                )
                same_root, same_state, created_again = prepare(
                    "demo", "demo@1", 4567, lab_name="t1", lab_root=str(lab_root)
                )
            self.assertTrue(created)
            self.assertFalse(created_again)
            self.assertEqual((same_root, same_state["workspace"]),
                             (root, prepared["workspace"]))

    def test_prepare_rejects_dirty_plan(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary); plan = repo / "experiment-plans" / "demo"; self.write_plan(plan)
            lab_root = repo / "lab"; lab_root.mkdir()
            artifact = repo / "tool"; artifact.write_text("tool")
            self.commit_repo(repo)
            (plan / "dirty").write_text("dirty")
            with mock.patch("labflow.lifecycle.repository_root", return_value=repo), \
                 mock.patch("labflow.lifecycle.git_metadata", return_value=("rev", False)):
                with self.assertRaisesRegex(ControlError, "clean and committed"):
                    prepare("demo", "demo@1", 4567, lab_name="t1", lab_root=str(lab_root))


class ObserveQueryTest(unittest.TestCase):
    def setUp(self):
        self.messages = [
            {"info": {"id": "u", "role": "user", "time": {"created": 10}}, "parts": [{"type": "text", "text": "go"}]},
            {"info": {"id": "a", "role": "assistant", "finish": "stop", "time": {"created": 11, "completed": 20}, "tokens": {"reasoning": 3}}, "parts": [{"type": "tool", "tool": "bash", "state": {"status": "error", "input": {}, "metadata": {"exit": 1}, "output": "bad"}}, {"type": "text", "text": "done"}]},
        ]

    def test_summary_and_failures(self):
        summary = summarize(self.messages); self.assertEqual(summary["duration_ms"], 10); self.assertEqual(summary["tool_failures"], 1)
        self.assertEqual(failures(self.messages)[0]["count"], 1); self.assertEqual(latest_assistant(self.messages)["info"]["id"], "a")

    def test_query_selection(self):
        with mock.patch.dict(os.environ, {"OC_QUERY_ENGINE": "jq"}), mock.patch("labflow.query.probe_direct", return_value=("jq",)):
            self.assertEqual(select_engine(), ("jq", ["jq"]))
        with mock.patch.dict(os.environ, {"OC_QUERY_ENGINE": "bad"}):
            with self.assertRaises(ControlError): select_engine()

    def test_cli_mise_fallback(self):
        resolve_cli.cache_clear(); probe_direct.cache_clear(); probe_mise.cache_clear()

    def test_manifest_command_uses_mise_prefix(self):
        with mock.patch("labflow.external.resolve_cli", return_value=("mise", "x", "--", "cargo")):
            self.assertEqual(
                resolve_command(["cargo", "build", "-p", "demo"]),
                ["mise", "x", "--", "cargo", "build", "-p", "demo"],
            )
        failed = subprocess.CompletedProcess([], 127, "", "missing")
        passed = subprocess.CompletedProcess([], 0, "1.0", "")
        with mock.patch("subprocess.run", side_effect=[failed, passed]):
            self.assertEqual(resolve_cli("opencode"), ("mise", "x", "--", "opencode"))
        resolve_cli.cache_clear(); probe_direct.cache_clear(); probe_mise.cache_clear()

    def test_validation_resolves_relative_executable_from_validation_cwd(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            validation_cwd = workspace / "crate"
            validation_cwd.mkdir(parents=True)
            executable = workspace / "bin" / "tool"
            executable.parent.mkdir()
            executable.write_text("#!/bin/sh\necho validated\n")
            executable.chmod(0o755)
            manifest = Manifest(
                "demo", root, (), {},
                ({"name": "crate", "cwd": "crate", "command": ["../bin/tool"], "required": True},),
                (), (),
            )
            context = Context(root, root / "execution", {
                "workspace": str(workspace), "archive": str(root / "archive/demo@1")
            }, manifest)

            results = run_validation(context)

            self.assertEqual(results[0]["exit"], 0)
            self.assertEqual(results[0]["stdout"], "validated\n")

    def test_query_prefers_direct_jq_over_mise_jaq(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch("labflow.external.probe_direct", side_effect=lambda cli: (cli,) if cli == "jq" else None), mock.patch("labflow.external.probe_mise", return_value=None):
            self.assertEqual(select_engine(), ("jq", ["jq"]))


class MetricsTest(unittest.TestCase):
    def test_collects_phases_tokens_waiting_and_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            source = workspace / "output" / "src" / "main.code"
            source.parent.mkdir(parents=True)
            source.write_text("let x = 1;\nexport { x };\n")
            notes = workspace / "output" / "NOTES.md"
            notes.write_text("# Notes\n\nDone.\n")
            messages = [
                {"info": {"role": "user", "time": {"created": 0}}, "parts": []},
                {"info": {"role": "assistant", "time": {"created": 1, "completed": 5},
                          "tokens": {"input": 10, "output": 2, "reasoning": 3, "cache": {"read": 7}}}, "parts": []},
                {"info": {"role": "user", "time": {"created": 10}}, "parts": []},
                {"info": {"role": "assistant", "time": {"created": 11, "completed": 15},
                          "tokens": {"input": 20, "output": 3, "reasoning": 4}}, "parts": []},
                {"info": {"role": "assistant", "time": {"created": 16, "completed": 20},
                          "tokens": {"input": 30, "output": 5, "reasoning": 6}},
                 "parts": [{"type": "tool", "tool": "write", "state": {"input": {"filePath": str(source)}}}]},
                {"info": {"role": "assistant", "time": {"created": 21, "completed": 25},
                          "tokens": {"input": 40, "output": 7, "reasoning": 8}}, "parts": []},
                {"info": {"role": "assistant", "time": {"created": 26, "completed": 126},
                          "tokens": {}},
                 "parts": [{"type": "tool", "tool": "bash", "state": {
                     "status": "completed",
                     "input": {"command": "labflow agent pull worker"},
                     "time": {"start": 125, "end": 126},
                 }}]},
            ]
            definition = {"roles": {"worker": {
                "learning_phases": ["language_learning", "api_learning"],
                "work_phase": "implementation",
                "work_files": ["output/src/main.code"],
                "artifacts": {
                    "code": {"core": ["output/src/*.code"]},
                    "documents": {"docs": ["output/NOTES.md"]},
                },
            }}}
            children = [{"id": "ses_worker", "agent": "worker", "title": "Worker",
                         "model": {"providerID": "provider", "id": "model", "variant": "v"}}]
            result = collect_metrics("demo@1", "idle", workspace, children,
                                     lambda _session: messages, definition)
            role = result["roles"][0]
            self.assertEqual(result["title"], "demo@1")
            self.assertEqual([phase["name"] for phase in role["phases"]],
                             ["language_learning", "api_learning", "implementation"])
            self.assertEqual(role["tokens"]["fresh"], 138)
            self.assertEqual(role["time"], {"first_created": 1, "last_completed": 126,
                                             "active_ms": 16, "span_ms": 125, "waiting_ms": 109})
            self.assertEqual(role["artifacts"]["code"]["total"], {"files": 1, "lines": 2, "bytes": 25})
            self.assertEqual(role["artifacts"]["documents"]["total"]["lines"], 3)
            self.assertEqual(role["productivity"]["code_lines_per_1k_work_fresh_tokens"], 20.833)
            self.assertEqual(result["aggregate"]["phases"]["learning"]["tokens"]["fresh"], 42)
            self.assertEqual(result["aggregate"]["phases"]["work"]["tokens"]["fresh"], 96)
            self.assertEqual(result["aggregate"]["time"]["span_ms"], 125)

    def test_unconfigured_role_is_not_mislabeled_as_learning(self):
        messages = [{"info": {"role": "assistant", "time": {"created": 1, "completed": 2},
                              "tokens": {"input": 3}}, "parts": []}]
        children = [{"id": "ses_worker", "agent": "worker"}]
        result = collect_metrics("run", "idle", Path("/tmp"), children, lambda _session: messages,
                                 {"roles": {}})
        role = result["roles"][0]
        self.assertEqual(role["classification"], {"configured": False, "work_boundary_observed": None})
        self.assertEqual([(phase["name"], phase["kind"]) for phase in role["phases"]],
                         [("unclassified", "unclassified")])
        self.assertEqual(result["aggregate"]["phases"]["unclassified"]["tokens"]["fresh"], 3)

    def test_metrics_warn_when_configured_files_and_work_boundary_are_missing(self):
        messages = [{
            "info": {"role": "assistant", "time": {"created": 1000, "completed": 2000},
                     "tokens": {"input": 1}},
            "parts": [],
        }]
        definition = {"roles": {"worker": {
            "learning_phases": ["learning"],
            "work_phase": "implementation",
            "work_files": ["missing/src/*.code"],
            "artifacts": {"code": {"core": ["missing/src/*.code"]}},
        }}}
        records = {"active": [], "history": [{
            "task_id": "worker-1", "role": "worker", "artifacts": ["build.worker"],
            "status": "submitted", "started_at_ns": 900_000_000,
            "submitted_at_ns": 2_100_000_000,
        }]}
        result = collect_metrics(
            "run", "active", Path("/tmp"), [{"id": "ses_worker", "agent": "worker"}],
            lambda _session: messages, definition, records, now_ms=3000,
        )
        self.assertEqual(
            [warning["kind"] for warning in result["roles"][0]["warnings"]],
            ["artifact_pattern_no_match", "work_boundary_not_observed"],
        )

    def test_task_metrics_cover_tokens_thinking_and_declared_commands(self):
        messages = [{
            "info": {"role": "assistant", "time": {"created": 1000, "completed": 2000},
                     "tokens": {"input": 10, "output": 3}},
            "parts": [{"type": "tool", "tool": "bash", "state": {
                "input": {"command": "./bin/compiler run main -C demo"},
                "time": {"start": 1300, "end": 1400},
            }}],
        }]
        records = {"active": [], "history": [{
            "task_id": "a1-1", "role": "a1", "artifacts": ["demo.a1"],
            "status": "submitted", "started_at_ns": 900_000_000,
            "submitted_at_ns": 2_100_000_000,
        }]}
        result = collect_metrics(
            "run", "idle", Path("/tmp"), [{"id": "ses_a1", "agent": "a1"}],
            lambda _session: messages, {"roles": {"a1": {
                "commands": {"compiler": ["./bin/compiler *"]},
            }}}, records, now_ms=3000,
        )
        task = result["tasks"][0]
        self.assertEqual(task["tokens"]["fresh"], 13)
        self.assertEqual(task["elapsed_ms"], 1200)
        self.assertEqual(task["longest_thinking_ms"], 600)
        self.assertEqual(task["commands"], {"compiler": {"count": 1, "elapsed_ms": 100}})
        self.assertEqual(task["command_count"], 1)
        self.assertEqual(task["command_elapsed_ms"], 100)

    def test_declared_wrapper_command_is_counted_without_tool_special_case(self):
        messages = [{
            "info": {"role": "assistant", "time": {"created": 1000, "completed": 2000}},
            "parts": [{"type": "tool", "tool": "bash", "state": {
                "input": {"command": "just make-query"},
                "time": {"start": 1200, "end": 1500},
            }}],
        }]
        records = {"active": [], "history": [{
            "task_id": "a5-1", "role": "a5", "artifacts": ["answer.a5"],
            "status": "submitted", "started_at_ns": 900_000_000,
            "submitted_at_ns": 2_100_000_000,
        }]}
        result = collect_metrics(
            "run", "idle", Path("/tmp"), [{"id": "ses_a5", "agent": "a5"}],
            lambda _session: messages, {"roles": {"a5": {
                "commands": {"query": ["just make-query"]},
            }}}, records, now_ms=3000,
        )
        self.assertEqual(result["tasks"][0]["commands"], {
            "query": {"count": 1, "elapsed_ms": 300},
        })

    def test_collects_multiple_work_phases_at_first_matching_writes(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            messages = [
                {"info": {"role": "assistant", "time": {"created": 1, "completed": 2},
                          "tokens": {"input": 1}}, "parts": []},
                {"info": {"role": "assistant", "time": {"created": 3, "completed": 4},
                          "tokens": {"input": 2}},
                 "parts": [{"type": "tool", "tool": "write", "state": {
                     "input": {"filePath": str(workspace / "model" / "src.code")}
                 }}]},
                {"info": {"role": "assistant", "time": {"created": 5, "completed": 6},
                          "tokens": {"input": 3}}, "parts": []},
                {"info": {"role": "assistant", "time": {"created": 7, "completed": 8},
                          "tokens": {"input": 4}},
                 "parts": [{"type": "tool", "tool": "edit", "state": {
                     "input": {"filePath": str(workspace / "public" / "query.code")}
                 }}]},
                {"info": {"role": "assistant", "time": {"created": 9, "completed": 10},
                          "tokens": {"input": 5}}, "parts": []},
            ]
            definition = {"roles": {"worker": {
                "learning_phases": ["learning"],
                "work_phases": [
                    {"name": "modeling", "files": ["model/**"]},
                    {"name": "public_surface", "files": ["public/**"]},
                ],
                "artifacts": {},
            }}}
            children = [{"id": "ses_worker", "agent": "worker"}]
            result = collect_metrics(
                "run", "idle", workspace, children, lambda _session: messages, definition
            )
            phases = result["roles"][0]["phases"]
            self.assertEqual(
                [(phase["name"], phase["tokens"]["fresh"]) for phase in phases],
                [("learning", 1), ("modeling", 5), ("public_surface", 9)],
            )

    def test_stat_reads_live_child_messages(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_dir = root / "result"
            child_dir = result_dir / "children"
            workspace = result_dir / "workspace"
            child_dir.mkdir(parents=True)
            workspace.mkdir()
            session_id = "ses_worker"
            messages = [
                {"info": {"role": "assistant", "time": {"created": 1, "completed": 2},
                          "tokens": {"input": 7}}, "parts": []},
            ]
            children = [{"id": session_id, "agent": "worker", "title": "Worker"}]
            context = Context(Path(temporary), root, {
                "title": "demo@1", "phase": "idle", "workspace": str(workspace),
                "metrics": {"roles": {}},
            }, mock.Mock(metrics={"roles": {}}))
            output = StringIO()
            with mock.patch("labflow.cli_host.resolve", return_value=context), \
                 mock.patch("labflow.cli_host._live_children",
                            return_value=(children, {session_id: messages}, {})), redirect_stdout(output):
                self.assertEqual(control_main(["stat", "t1", "demo@1"]), 0)
            document = json.loads(output.getvalue())
            self.assertEqual(document["execution_phase"], "idle")
            self.assertEqual(document["roles"][0]["tokens"]["fresh"], 7)


    def test_replacement_sessions_are_aggregated_as_one_role(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            messages = {
                "old": [{"info": {"role": "assistant", "time": {"created": 1, "completed": 2},
                                    "tokens": {"input": 3}}, "parts": []}],
                "new": [{"info": {"role": "assistant", "time": {"created": 3, "completed": 4},
                                    "tokens": {"input": 5}}, "parts": []}],
            }
            result = collect_metrics(
                "run", "idle", workspace,
                [{"id": "old", "agent": "a5"}, {"id": "new", "agent": "a5"}],
                messages.__getitem__, {"roles": {}},
            )
            self.assertEqual(len(result["roles"]), 1)
            self.assertEqual(result["roles"][0]["session_ids"], ["old", "new"])
            self.assertEqual(result["roles"][0]["tokens"]["fresh"], 8)


class ArchiveExportTest(unittest.TestCase):
    def test_archive_is_repeatable_allows_internal_file_links_and_rejects_escape(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary); root = repo / "role"; workspace = repo / "workspace"
            (workspace / "output").mkdir(parents=True); (workspace / "output" / "x").write_text("x")
            (workspace / "process.txt").write_text("process")
            (workspace / "tool").write_text("tool")
            manifest = Manifest("demo", repo, (), {}, (), (), ())
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["worker"],
                "artifacts": {"result": {
                    "desc": "result",
                    "assets": [
                        {"path": "output/", "level": 2},
                        {"path": "process.txt", "level": 1},
                        {"path": "tool", "level": 0},
                    ],
                }},
            })
            refresh_artifact(workspace, workflow, "result")
            context = Context(repo, root, {"workspace": str(workspace), "workflow": workflow}, manifest)
            destination = root / "result" / "workspace"
            copy_archive(context, destination); self.assertTrue((destination / "output/x").is_file())
            self.assertFalse((destination / "process.txt").exists())
            self.assertFalse((destination / "tool").exists())
            copy_archive(context, destination, include_process=True)
            self.assertTrue((destination / "process.txt").is_file())
            self.assertFalse((destination / "tool").exists())
            copy_archive(context, destination); self.assertTrue((destination / "output/x").is_file())
            os.symlink("x", workspace / "output" / "internal")
            copy_archive(context, destination)
            self.assertEqual((destination / "output/internal").read_text(), "x")
            os.symlink("/tmp", workspace / "output" / "escape")
            with self.assertRaises(ControlError): copy_archive(context, destination)

    def test_export_retries_truncated_json(self):
        context = mock.Mock()
        context.state = {"workspace": "/tmp/ws"}
        payloads = iter((b'{"messages":["', b'{"messages":[]}'))
        def run(*_args, **kwargs):
            kwargs["stdout"].write(next(payloads))
            return subprocess.CompletedProcess([], 0, b"", b"")
        with mock.patch("labflow.lifecycle.resolve_cli", return_value=("opencode",)), \
             mock.patch("labflow.lifecycle.subprocess.run", side_effect=run) as run_mock, \
             mock.patch("labflow.lifecycle.time.sleep"):
            self.assertEqual(export_session(context, "ses_test"), {"messages": []})
            self.assertEqual(run_mock.call_count, 2)

    def test_export_reports_failure_after_three_attempts(self):
        context = mock.Mock()
        context.state = {"workspace": "/tmp/ws"}
        failed = subprocess.CompletedProcess([], 1, b"", b"bad export")
        with mock.patch("labflow.lifecycle.resolve_cli", return_value=("opencode",)), \
             mock.patch("labflow.lifecycle.subprocess.run", return_value=failed) as run, \
             mock.patch("labflow.lifecycle.time.sleep"):
            with self.assertRaisesRegex(ControlError, "bad export"):
                export_session(context, "ses_test")
            self.assertEqual(run.call_count, 3)


class PermissionPreflightTest(unittest.TestCase):
    def manifest(self, root: Path, commands: tuple[str, ...]) -> Manifest:
        return Manifest("demo", root, (), {"worker": {"preflight": list(commands)}},
                        (), (), ())

    def workspace(self, root: Path, permission: object) -> Path:
        workspace = root / "ws"; agents = workspace / ".opencode" / "agents"
        agents.mkdir(parents=True)
        (workspace / "opencode.json").write_text(json.dumps({"permission": "deny"}))
        (agents / "worker.md").write_text(
            f"---\npermission: {json.dumps(permission, separators=(',', ':'))}\n---\nWorker.\n"
        )
        return workspace

    def test_accepts_declared_best_effort_command(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = self.workspace(root, {"bash": {
                "*": "deny", "./bin/compiler run * -C model *": "allow",
            }})
            command = "./bin/compiler run invalid -C model --best-effort"
            result = preflight_permissions(self.manifest(root, (command,)), workspace)
            self.assertEqual(result["worker"], [{"command": command, "decision": "allow"}])

    def test_rejects_deny_and_ask(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = self.workspace(root, {"bash": {"*": "deny"}})
            with self.assertRaisesRegex(ControlError, "rejected worker command"):
                preflight_permissions(self.manifest(root, ("./bin/compiler run main",)), workspace)
            workspace = self.workspace(root / "ask", {"bash": {"*": "ask"}})
            with self.assertRaisesRegex(ControlError, "interactive permission"):
                preflight_permissions(self.manifest(root, ()), workspace)

    def test_rejects_allowed_command_family_missing_from_preflight(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = self.workspace(root, {"bash": {
                "*": "deny",
                "./bin/compiler query *": "allow",
                "./bin/compiler types *": "allow",
            }})
            manifest = self.manifest(
                root, ("./bin/compiler query exports @bin/main -C demo",)
            )
            with self.assertRaisesRegex(ControlError, "unexercised worker command family"):
                preflight_permissions(manifest, workspace)


class StatusSummaryTest(unittest.TestCase):
    def test_status_surfaces_host_gate_without_verbose_graph(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1",
                "roles": ["a1"],
                "artifacts": {
                    "input-0": {"desc": "input"},
                    "output-1.a1": {
                        "desc": "output", "input": ["input-0"],
                        "instruction": "build",
                    },
                    "output-2": {"desc": "final", "input": ["output-1.a1"]},
                },
            })
            refresh_artifact(workspace, workflow, "input-0")
            assign_task(workspace, workflow, "a1", "output-1.a1")
            submit(workspace, workflow, "a1", ["output-1.a1"])
            context = mock.Mock(state={
                "title": "demo@1", "lab_name": "t1",
                "phase": "active", "workspace": str(workspace),
                "workflow": workflow,
            })
            context.root = workspace / "execution"
            metrics = {"aggregate": {"tokens": {"fresh": 10}}}
            detail = {"agents": [{"role": "a1", "state": "idle"}],
                      "records": {"active": [], "history": []}}
            with mock.patch("labflow.cli_host._metrics",
                            return_value=(metrics, detail)):
                summary = _status(context)
                verbose = _status(context, True)
            self.assertNotIn("artifacts", summary)
            self.assertEqual(summary["artifact_summary"]["submittable"], ["output-2"])
            self.assertEqual(summary["next_host_actions"], [{
                "action": "submit_artifact",
                "artifact": "output-2",
                "command": "labflow host submit t1 demo@1 output-2",
            }])
            self.assertIn("artifacts", verbose)

    def test_status_includes_the_supervisor_execution_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); workspace = root / "ws"
            workspace.mkdir()
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1", "roles": ["a1"],
                "artifacts": {"output.a1": {
                    "desc": "output", "instruction": "build",
                }},
            })
            atomic_json(root / "supervisor-status.json", {
                "schema": "labflow.supervisor-status/v1", "updated_at": 123,
                "executions": [{
                    "title": "demo@1", "dag": True,
                    "requests": ["approval"], "optional_requests": [],
                    "errors": [{"role": "a1", "error": "duplicate Session"}],
                    "sessions": [{
                        "backend_id": "ses_a1", "title": "a1",
                        "role": "a1", "status": "idle",
                    }],
                }],
            })
            context = mock.Mock(state={
                "title": "demo@1", "lab_name": "t1", "lab_root": str(root),
                "phase": "active", "workspace": str(workspace),
                "workflow": workflow,
            })
            context.root = root / "control"
            metrics = {"aggregate": {"tokens": {"fresh": 0}}}
            detail = {"agents": [], "records": {"active": [], "history": []}}

            with mock.patch("labflow.cli_host._metrics", return_value=(metrics, detail)):
                status = _status(context)

            self.assertEqual(status["supervision"]["updated_at"], 123)
            self.assertEqual(status["supervision"]["errors"][0]["role"], "a1")

    def test_host_pull_returns_immediately_for_submitable_gate_and_summarizes_window(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1",
                "roles": ["a1"],
                "artifacts": {
                    "input-0": {"desc": "input"},
                    "output-1.a1": {"desc": "output", "input": ["input-0"],
                                 "instruction": "build"},
                    "output-2": {"desc": "final", "input": ["output-1.a1"],
                                 "assets": ["completion.txt"]},
                },
            })
            refresh_artifact(workspace, workflow, "input-0")
            assign_task(workspace, workflow, "a1", "output-1.a1")
            started_at_ns = task_records(workspace)["active"][0]["started_at_ns"]
            submit(workspace, workflow, "a1", ["output-1.a1"])
            context = mock.Mock(state={
                "title": "demo@1", "phase": "active", "workspace": str(workspace),
                "workflow": workflow,
            })
            context.root = workspace / "execution"
            context.client.return_value.children.return_value = []
            result = _host_pull(context, started_at_ns // 1_000_000 - 1, timeout=60)
            self.assertLess(result["timeline"]["waited_ms"], 1000)
            self.assertEqual(result["result"], {
                "requests": ["output-2"], "opt_requests": [],
            })
            self.assertFalse(workflow_status(workspace, workflow)["artifacts"]["output-2"]["submittable"])
            self.assertEqual([event["status"] for event in result["timeline"]["events"]
                              if event["type"] == "task"], ["submitted"])
            repeated = _host_pull(context, result["timeline"]["next_since"], timeout=0)
            self.assertEqual([event["id"] for event in repeated["timeline"]["events"]],
                             [event["id"] for event in result["timeline"]["events"]
                              if event["at"] == result["timeline"]["next_since"]])
            self.assertEqual(repeated["result"], {
                "requests": ["output-2"], "opt_requests": [],
            })

    def test_host_pull_reports_optional_host_artifacts_without_waking(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            workflow = validate_workflow({
                "schema": "labflow.workflow/v1",
                "roles": ["a1"],
                "artifacts": {
                    "input-0": {"desc": "input"},
                    "input-optional": {"desc": "optional input"},
                    "output-1.a1": {"desc": "output",
                                 "input": ["input-0", "input-optional?"],
                                 "instruction": "build"},
                    "output-2": {"desc": "final", "input": ["output-1.a1"]},
                },
            })
            refresh_artifact(workspace, workflow, "input-0")
            context = mock.Mock(state={
                "title": "demo@1", "phase": "active", "workspace": str(workspace),
                "workflow": workflow,
            })
            context.root = workspace / "execution"
            context.client.return_value.children.return_value = []

            result = _host_pull(context, None, timeout=.02)

            self.assertGreaterEqual(result["timeline"]["waited_ms"], 10)
            self.assertEqual(result["result"], {
                "requests": [], "opt_requests": ["input-optional"],
            })

    def test_host_pull_rejects_waits_longer_than_one_minute(self):
        context = mock.Mock(state={"workflow": {"roles": []}})
        with self.assertRaisesRegex(ControlError, "between 0 and 60"):
            _host_pull(context, None, timeout=61)


class EventProjectionTest(unittest.TestCase):
    def test_projects_compact_lifecycle_events_and_reads_sanitized_detail(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            context = mock.Mock(state={"workspace": str(workspace)})
            context.root = workspace / "execution"
            message = {
                "info": {"id": "msg_1", "role": "assistant", "finish": "stop",
                         "time": {"created": 1000, "completed": 3600},
                         "tokens": {"input": 2, "output": 3, "reasoning": 5}},
                "parts": [
                    {"type": "reasoning", "text": "private reasoning"},
                    {"id": "part_1", "type": "tool", "tool": "bash", "state": {
                        "status": "completed", "input": {"command": "just make-query"},
                        "output": "ok", "metadata": {"exit": 0},
                        "time": {"start": 2200, "end": 2400},
                    }},
                    {"type": "text", "text": "Query completed."},
                ],
            }
            client = context.client.return_value
            client.children.return_value = [{"id": "ses_1", "agent": "a5"}]
            client.session_messages.return_value = [message]
            events = project_events(context, 999)
            self.assertEqual([event["type"] for event in events],
                             ["thinking", "action", "thinking", "reply"])
            self.assertEqual(events[1]["summary"], "just make-query")
            thinking = event_detail(context, "thinking:ses_1:msg_1:0")
            self.assertNotIn("parts", thinking["detail"])
            self.assertNotIn("private reasoning", json.dumps(thinking))
            self.assertEqual(thinking["detail"]["event"]["end_at"], 2200)
            action = event_detail(context, "action:ses_1:msg_1:part_1")
            self.assertEqual(action["detail"]["state"]["output"], "ok")


class WatchTest(unittest.TestCase):
    def test_window_debounce_timeout_and_finish(self):
        empty = WatchWindow(10, 30, 300)
        self.assertIsNone(empty.reason(309))
        self.assertEqual(empty.reason(310), "timeout")
        active = WatchWindow(10, 30, 300); active.add("one", {"kind": "file_start"}, 20)
        self.assertIsNone(active.reason(49))
        self.assertEqual(active.reason(50), "debounced")
        self.assertEqual(active.reason(21, finished=True), "experiment_finished")

    def test_reasoning_is_ignored_and_tool_states_are_distinct(self):
        messages = [{"info": {"id": "msg", "role": "assistant"}, "parts": [
            {"type": "reasoning", "text": "secret"},
            {"id": "tool", "type": "tool", "tool": "bash",
             "state": {"status": "running", "input": {"command": "echo ok"}}},
        ]}]
        started = message_events("ses", "a2", messages)
        self.assertEqual([event[1]["kind"] for event in started], ["command_start"])
        messages[0]["parts"][1]["state"] = {
            "status": "completed", "input": {"command": "echo ok"}, "metadata": {"exit": 0},
        }
        completed = message_events("ses", "a2", messages)
        self.assertEqual([event[1]["kind"] for event in completed], ["command_result"])
        self.assertNotEqual(started[0][0], completed[0][0])

    def test_permission_event_is_infrastructure_error(self):
        events = acp_events({"type": "permission.asked", "properties": {"sessionID": "ses"}}, {})
        self.assertEqual(events[0][1]["kind"], "infrastructure_permission_error")

    def test_persisted_cursor_deduplicates_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root / "plan").write_text("demo\n")
            save_state(root, {"schema": SCHEMA, "plan_id": "demo", "title": "demo@1",
                               "phase": "finished", "workspace": "/tmp/ws",
                               "server_url": "http://127.0.0.1:1", "session_id": "ses"})
            manifest = Manifest("demo", root, (), {}, (), (), ())
            context = Context(root, root, load_state(root), manifest)
            client = mock.Mock()
            client.messages.return_value = [{"info": {"id": "msg", "role": "assistant"}, "parts": [
                {"id": "tool", "type": "tool", "tool": "read",
                 "state": {"status": "completed", "input": {"filePath": "GOAL.md"}}},
            ]}]
            client.children.return_value = []
            with mock.patch.object(Context, "client", return_value=client):
                first = watch_progress(context, 30, 300)
                second = watch_progress(context, 30, 300)
            self.assertEqual(len(first["events"]), 1)
            self.assertEqual(second["events"], [])
            self.assertEqual(first["next_cursor"], "1")
            self.assertEqual(second["next_cursor"], "2")


class ReportingTest(unittest.TestCase):
    def context(self, root: Path, sinks: list[dict]) -> Context:
        manifest = Manifest("demo", root, (), {}, (), (), ())
        state = {"title": "demo@1", "reporting": {"sinks": sinks}}
        return Context(root, root / "execution", state, manifest)

    def test_without_sink_only_persists_locally(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); body = root / "body.md"; body.write_text("Working.")
            with mock.patch("labflow.reporting.resolve_cli") as resolve:
                record = submit_report(self.context(root, []), body)
            resolve.assert_not_called()
            self.assertEqual(record["status"], "ok")
            stored = root / "execution" / "reports" / "000.md"
            self.assertIn("未经 Host 验收", stored.read_text())

    def test_github_sink_uses_body_file_and_records_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); body = root / "body.md"; body.write_text("Working.")
            sink = {"kind": "github_issue_comment", "repository": "owner/repo", "issue": 7}
            failed = subprocess.CompletedProcess([], 1, "", "offline")
            with mock.patch("labflow.reporting.resolve_cli", return_value=("gh",)), \
                 mock.patch("labflow.reporting.subprocess.run", return_value=failed) as run:
                record = submit_report(self.context(root, [sink]), body)
            self.assertEqual(record["status"], "error")
            command = run.call_args.args[0]
            self.assertIn("--body-file", command)
            self.assertNotIn("--body", command)
            self.assertEqual(record["sinks"][0]["error"], "offline")


if __name__ == "__main__": unittest.main()
