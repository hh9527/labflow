from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any

from .config import ControlError
from .context import Context
from .events import project_events
from .metrics import summarize_thread_metrics
from .observe import text_parts
from .state import atomic_json, atomic_write, load_state, locked, save_state


PROBLEM_ROOT = Path("problem")
CHANNEL_ROOT = Path("ch")
CHANNEL_OUTPUT = CHANNEL_ROOT / "out"
RESULT_ROOT = Path("result")
RECORD_ROOT = Path(".labflow") / "benchmark-records"


def _completed_after(messages: list[dict[str, Any]], preceding: str | None) -> dict[str, Any] | None:
    start = 0
    if preceding is not None:
        indexes = [index for index, message in enumerate(messages)
                   if message.get("info", {}).get("id") == preceding]
        start = indexes[-1] + 1 if indexes else 0
    values = [message for message in messages[start:]
              if message.get("info", {}).get("role") == "assistant"
              and message.get("info", {}).get("time", {}).get("completed") is not None]
    return values[-1] if values else None


def _prompt(client: Any, session_id: str, prompt: str, role: str,
            timeout: float = 3600.0) -> dict[str, Any]:
    if client.statuses().get(session_id, {"type": "idle"}).get("type") == "busy":
        raise ControlError(f"Benchmark Session is busy: {session_id}", 75)
    messages = client.session_messages(session_id)
    preceding = messages[-1].get("info", {}).get("id") if messages else None
    client.prompt_session(session_id, prompt, agent=role)
    deadline = time.monotonic() + timeout
    while True:
        messages = client.session_messages(session_id)
        completed = _completed_after(messages, preceding)
        status = client.statuses().get(session_id, {"type": "idle"}).get("type")
        if completed is not None and status != "busy":
            return completed
        if time.monotonic() >= deadline:
            raise ControlError(f"timed out waiting for Benchmark role {role}", 75)
        time.sleep(.1)


def _new_session(client: Any, title: str, parent: str | None, role: str) -> str:
    response = client.create_session(title, parent_id=parent, agent=role)
    session_id = response.get("id") if isinstance(response, dict) else None
    if not isinstance(session_id, str) or not session_id.startswith("ses_"):
        raise ControlError("OpenCode returned an invalid Benchmark Session identity", 69)
    return session_id


def _transcript(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for message in messages:
        info = message.get("info", {})
        if info.get("role") not in ("user", "assistant"):
            continue
        result.append({
            "role": info.get("role"), "message_id": info.get("id"),
            "created_at": info.get("time", {}).get("created"),
            "completed_at": info.get("time", {}).get("completed"),
            "text": "\n".join(text_parts(message)),
        })
    return result


def _read_runtime(workspace: Path) -> dict[str, Any]:
    try:
        runtime = json.loads((workspace / "experiment.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"cannot read Benchmark runtime: {exc}", 66) from None
    execution = runtime.get("execution")
    if not isinstance(execution, dict) or execution.get("kind") != "benchmark-mode":
        raise ControlError("operation requires benchmark-mode", 64)
    return execution


def _safe_problem_id(value: str) -> str:
    if (not value or value in (".", "..") or "/" in value or "\\" in value
            or not all(character.isalnum() or character in "._-" for character in value)):
        raise ControlError(f"invalid Benchmark problem id: {value!r}", 64)
    return value


def start_problem(workspace: Path, problem_id: str) -> dict[str, Any]:
    execution = _read_runtime(workspace)
    problem_id = _safe_problem_id(problem_id)
    problems = {problem["id"]: problem for problem in execution["problems"]}
    if problem_id not in problems:
        raise ControlError(f"unknown Benchmark problem: {problem_id}", 64)
    channel = workspace / CHANNEL_ROOT
    metadata = channel / "metadata.json"
    if metadata.exists():
        raise ControlError("a Benchmark problem is already active", 75)
    source = workspace / PROBLEM_ROOT / problem_id
    if not (source / "q.md").is_file():
        raise ControlError(f"missing prepared Benchmark problem: {problem_id}", 66)
    output = channel / "out"
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ControlError("Benchmark output channel is not clean", 75)
    shutil.copy2(source / "q.md", channel / "q.md")
    if (source / "k.md").is_file():
        shutil.copy2(source / "k.md", channel / "k.md")
    atomic_json(metadata, {"id": problem_id, "maxTurns": problems[problem_id]["maxTurns"]})
    return {"schema": "labflow.problem-start/v1", "id": problem_id,
            "maxTurns": problems[problem_id]["maxTurns"]}


def end_problem(workspace: Path, outcome: str) -> dict[str, Any]:
    _read_runtime(workspace)
    if outcome not in ("ok", "error", "cancel"):
        raise ControlError(f"invalid Benchmark outcome: {outcome!r}", 64)
    metadata_path = workspace / CHANNEL_ROOT / "metadata.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ControlError("no Benchmark problem is active", 75) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"invalid Benchmark channel metadata: {exc}", 75) from None
    problem_id = _safe_problem_id(metadata.get("id"))
    started_at_ns = metadata_path.stat().st_mtime_ns
    channel = workspace / CHANNEL_OUTPUT
    report = channel / "report.md"
    if not report.is_file() or report.is_symlink() or not report.read_text(encoding="utf-8").strip():
        raise ControlError("ch/out/report.md must be a nonempty regular file", 75)
    evidence = [path for path in sorted(channel.iterdir())
                if path.is_file() and not path.is_symlink()
                and (path.name.startswith("ok-") or path.name.startswith("err-"))]
    prefix = {"ok": "ok-", "error": "err-", "cancel": None}[outcome]
    retained = [path for path in evidence if prefix is not None and path.name.startswith(prefix)]

    destination = workspace / RESULT_ROOT / problem_id
    if destination.exists():
        raise ControlError(f"Benchmark problem was already recorded: {problem_id}", 75)
    destination.mkdir(parents=True)
    shutil.copy2(report, destination / "report.md")
    for path in retained:
        shutil.copy2(path, destination / path.name)
    checkpoint = {
        "schema": "labflow.benchmark-record/v1", "problem": problem_id,
        "outcome": outcome, "started_at_ns": started_at_ns,
        "recorded_at_ns": time.time_ns(), "evidence": [path.name for path in retained],
    }
    atomic_json(workspace / RECORD_ROOT / f"{problem_id}.json", checkpoint)
    channel_root = workspace / CHANNEL_ROOT
    for path in list(channel_root.iterdir()):
        if path.is_file() or path.is_symlink():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    (channel_root / "out").mkdir()
    return checkpoint


def _prepare_workspace(context: Context) -> None:
    workspace = Path(context.state["workspace"])
    problem_root = workspace / PROBLEM_ROOT
    for path in (problem_root, workspace / CHANNEL_OUTPUT, workspace / RESULT_ROOT,
                 workspace / RECORD_ROOT):
        path.mkdir(parents=True, exist_ok=True)
    if any(problem_root.iterdir()) or any((workspace / RESULT_ROOT).iterdir()):
        raise ControlError("Benchmark workspace is not clean", 75)
    for problem in context.manifest.execution["problems"]:
        destination = problem_root / problem["id"]
        destination.mkdir()
        shutil.copy2(context.manifest.root / problem["q"], destination / "q.md")
        if problem["k"] is not None:
            shutil.copy2(context.manifest.root / problem["k"], destination / "k.md")


def _batch_prompt(problems: list[dict[str, Any]]) -> str:
    limits = {problem["id"]: problem["maxTurns"] for problem in problems}
    return (
        "完成这一批 Benchmark：" + ", ".join(limits) + "。题面和可选隐藏知识已经一次性放入 "
        "`problem/<id>/{q,k}.md`。严格按编号顺序逐题执行 `labflow problem start <id>`，然后"
        "读取 `ch/q.md` 并把原文逐字发送给 Answerer，不得转述或改写；可选 K 位于 `ch/k.md`，"
        "约束位于 `ch/metadata.json`。本批只创建一个 Answerer 子会话并持续复用。每题完成后"
        "由你写 `ch/out/report.md`，然后根据结果执行 `labflow problem end ok|error|cancel`；归档"
        "成功后再开始下一题。每题最多进行的 Answerer 轮数为："
        + json.dumps(limits, ensure_ascii=False, separators=(",", ":"))
        + "。全部归档后再结束回复。"
    )


def _messages_in_window(messages: list[dict[str, Any]], start_ns: int,
                        end_ns: int) -> list[dict[str, Any]]:
    start_ms, end_ms = start_ns // 1_000_000, end_ns // 1_000_000
    return [message for message in messages
            if start_ms <= int(message.get("info", {}).get("time", {}).get("created", 0)) <= end_ms]


def _finalize_batch(context: Context, problems: list[dict[str, Any]], batch: int,
                    questioner_session: str, started_ns: int) -> list[dict[str, Any]]:
    workspace = Path(context.state["workspace"])
    execution = context.manifest.execution
    client = context.client()
    children = [item for item in client.children(questioner_session)
                if item.get("agent") == execution["answerer"]]
    if len(children) != 1 or not isinstance(children[0].get("id"), str):
        raise ControlError("Questioner must create exactly one Answerer per batch", 65)
    answerer_session = children[0]["id"]
    client.update_session(answerer_session,
                          {"title": f"{context.state['session_name']}.batch-{batch}.a"})
    q_messages = client.session_messages(questioner_session)
    a_messages = client.session_messages(answerer_session)
    metric_roles = context.state.get("metrics", context.manifest.metrics).get("roles", {})
    records = []
    boundary = started_ns
    for problem in problems:
        record_path = workspace / RECORD_ROOT / f"{problem['id']}.json"
        if not record_path.is_file():
            raise ControlError(f"Questioner did not record Benchmark problem {problem['id']}", 65)
        checkpoint = json.loads(record_path.read_text(encoding="utf-8"))
        end_ns = int(checkpoint["recorded_at_ns"])
        q_window = _messages_in_window(q_messages, boundary, end_ns)
        a_window = _messages_in_window(a_messages, boundary, end_ns)
        turns = sum(message.get("info", {}).get("role") == "assistant" for message in a_window)
        record = {
            **checkpoint, "batch": batch,
            "status": "completed" if turns <= problem["maxTurns"] else "protocol_error",
            "turns": turns, "started_at_ns": int(checkpoint.get("started_at_ns", boundary)),
            "end_at_ns": end_ns,
            "elapsed_ms": (end_ns - int(checkpoint.get("started_at_ns", boundary))) // 1_000_000,
            "questioner_session_id": questioner_session,
            "answerer_session_id": answerer_session,
            "transcript": {"questioner": _transcript(q_window),
                           "answerer": _transcript(a_window)},
            "metrics": {
                "questioner": summarize_thread_metrics(
                    q_window, metric_roles.get(execution["questioner"], {}).get("commands", {}),
                    now_ms=end_ns // 1_000_000),
                "answerer": summarize_thread_metrics(
                    a_window, metric_roles.get(execution["answerer"], {}).get("commands", {}),
                    now_ms=end_ns // 1_000_000),
            },
        }
        if turns > problem["maxTurns"]:
            record["turn_error"] = f"Answerer used {turns} turns; maximum is {problem['maxTurns']}"
        records.append(record)
        boundary = end_ns + 1
    return records


def _write_stats(workspace: Path, records: list[dict[str, Any]]) -> None:
    content = "".join(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                      for record in records)
    atomic_write(workspace / RESULT_ROOT / "stats.jsonl", content.encode())


def run(context: Context, since: int | None = None) -> dict[str, Any]:
    execution = context.manifest.execution
    if execution.get("kind") != "benchmark-mode":
        raise ControlError("operation requires benchmark-mode", 64)
    existing = context.state.get("benchmark")
    if isinstance(existing, dict) and existing.get("status") == "completed":
        return existing["response"]
    if isinstance(existing, dict) and existing.get("status") == "running":
        raise ControlError("Benchmark is already running", 75)
    started = int(time.time() * 1000)
    since = started if since is None else since
    _prepare_workspace(context)
    with locked(context.root):
        state = load_state(context.root)
        state["phase"] = "active"
        state["benchmark"] = {"status": "running", "started_at": started,
                              "problems": [], "sessions": []}
        save_state(context.root, state)
        context.state = state

    client = context.client()
    records: list[dict[str, Any]] = []
    try:
        batch_size = execution["batchSize"]
        for offset in range(0, len(execution["problems"]), batch_size):
            batch = offset // batch_size + 1
            problems = execution["problems"][offset:offset + batch_size]
            q_session = _new_session(client, f"{context.state['session_name']}.batch-{batch}.q",
                                     None, execution["questioner"])
            with locked(context.root):
                state = load_state(context.root)
                state["benchmark"]["sessions"].append({
                    "id": q_session, "agent": execution["questioner"], "batch": batch,
                })
                save_state(context.root, state)
                context.state = state
            batch_started_ns = time.time_ns()
            _prompt(client, q_session, _batch_prompt(problems), execution["questioner"])
            records.extend(_finalize_batch(context, problems, batch, q_session, batch_started_ns))
            _write_stats(Path(context.state["workspace"]), records)
            with locked(context.root):
                state = load_state(context.root)
                state["benchmark"]["problems"] = records
                save_state(context.root, state)
                context.state = state
    except Exception:
        with locked(context.root):
            state = load_state(context.root)
            state["phase"] = "failed"
            state["benchmark"]["status"] = "failed"
            state["benchmark"]["problems"] = records
            save_state(context.root, state)
            context.state = state
        raise

    events = project_events(context, since)
    observed = int(time.time() * 1000)
    response = {
        "schema": "labflow.host-observation/v1", "session_name": context.state["session_name"],
        "timeline": {"clock": "unix_ms", "since": since,
                     "next_since": max([since, *(event["at"] for event in events)]),
                     "observed_at": observed, "waited_ms": observed - started, "events": events},
        "result": {"problems": records},
    }
    with locked(context.root):
        state = load_state(context.root)
        state["phase"] = "idle"
        state["benchmark"] = {"status": "completed", "started_at": started,
                              "completed_at": observed, "problems": records,
                              "sessions": state.get("benchmark", {}).get("sessions", []),
                              "response": response}
        save_state(context.root, state)
        context.state = state
    return response
