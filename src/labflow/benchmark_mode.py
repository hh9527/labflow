from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any

from .config import ControlError, sha256
from .context import Context
from .events import project_events
from .metrics import summarize_thread_metrics
from .observe import latest_assistant, text_parts
from .state import load_state, locked, now, save_state


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


def _prompt(client: Any, session_id: str, text: str, role: str,
            timeout: float = 600.0) -> dict[str, Any]:
    statuses = client.statuses()
    if statuses.get(session_id, {"type": "idle"}).get("type") == "busy":
        raise ControlError(f"Benchmark Session is busy: {session_id}", 75)
    messages = client.session_messages(session_id)
    preceding = messages[-1].get("info", {}).get("id") if messages else None
    client.prompt_session(session_id, text, agent=role)
    deadline = time.monotonic() + timeout
    while True:
        messages = client.session_messages(session_id)
        completed = _completed_after(messages, preceding)
        if completed is not None:
            return completed
        if time.monotonic() >= deadline:
            raise ControlError(f"timed out waiting for Benchmark role {role}", 75)
        time.sleep(.1)


def _questioner_prompt(question: str, knowledge: str | None,
                       answer: str, first: bool) -> str:
    context = "题面之外没有可补充的信息。" if knowledge is None else knowledge
    prefix = (
        "你是 Benchmark 的 Questioner，只扮演提出业务问题的用户，不判断答案是否正确。\n"
        "判断 Answerer 的最新回复是否在请求澄清。若是，只依据原始题面和隐含知识作最窄的"
        "事实回答；不得主动提示解法或泄漏未被询问的信息。若不是澄清请求，则结束对话。\n"
        "只输出一行 JSON：{\"action\":\"reply\",\"text\":\"...\"} 或 "
        "{\"action\":\"done\"}。\n\n"
        f"原始题面：\n{question}\n\n隐含知识：\n{context}\n\n"
    ) if first else ""
    return f"{prefix}Answerer 的最新回复：\n{answer}"


def _questioner_action(message: dict[str, Any]) -> dict[str, str]:
    raw = "\n".join(text_parts(message)).strip()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ControlError(f"Questioner returned invalid JSON: {exc}", 65) from None
    if not isinstance(value, dict) or value.get("action") not in ("reply", "done"):
        raise ControlError("Questioner returned an invalid action", 65)
    if value["action"] == "done" and set(value) == {"action"}:
        return {"action": "done"}
    if (value["action"] == "reply" and set(value) == {"action", "text"}
            and isinstance(value.get("text"), str) and value["text"].strip()):
        return {"action": "reply", "text": value["text"]}
    raise ControlError("Questioner returned an invalid action payload", 65)


def _reset_outputs(workspace: Path, outputs: list[dict[str, Any]]) -> None:
    for asset in outputs:
        directory = asset["path"].endswith("/")
        path = workspace / asset["path"].rstrip("/")
        if path.is_symlink():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
        if directory:
            path.mkdir(parents=True)


def _archive_outputs(context: Context, problem_id: str,
                     outputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    workspace = Path(context.state["workspace"])
    root = context.root / "benchmark" / "problems" / problem_id / "outputs"
    result = []
    for asset in outputs:
        relative = Path(asset["path"].rstrip("/"))
        source = workspace / relative
        record: dict[str, Any] = {"path": asset["path"], "level": asset["level"],
                                  "present": source.exists()}
        if source.is_dir():
            files = []
            for child in sorted(source.rglob("*")):
                if child.is_file() and not child.is_symlink():
                    files.append({"path": child.relative_to(source).as_posix(),
                                  "sha256": sha256(child), "bytes": child.stat().st_size})
            record["files"] = files
            if asset["level"] > 0:
                shutil.copytree(source, root / relative, dirs_exist_ok=True)
        elif source.is_file() and not source.is_symlink():
            record.update({"sha256": sha256(source), "bytes": source.stat().st_size})
            if asset["level"] > 0:
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        result.append(record)
    return result


def _new_session(client: Any, title: str, parent: str, role: str) -> str:
    response = client.create_session(title, parent_id=parent, agent=role)
    session_id = response.get("id") if isinstance(response, dict) else None
    if not isinstance(session_id, str) or not session_id.startswith("ses_"):
        raise ControlError("OpenCode returned an invalid Benchmark Session identity", 69)
    return session_id


def _fork_answerer(client: Any, baseline: str, message_id: str | None,
                   title: str) -> str:
    response = client.fork_session(baseline, message_id)
    session_id = response.get("id") if isinstance(response, dict) else None
    if not isinstance(session_id, str) or not session_id.startswith("ses_"):
        raise ControlError("OpenCode returned an invalid Answerer fork identity", 69)
    client.update_session(session_id, {"title": title})
    return session_id


def _run_problem(context: Context, problem: dict[str, Any], answerer_session: str,
                 preflight: bool) -> dict[str, Any]:
    execution = context.manifest.execution
    client = context.client()
    question = (context.manifest.root / problem["q"]).read_text(encoding="utf-8")
    knowledge = ((context.manifest.root / problem["k"]).read_text(encoding="utf-8")
                 if problem["k"] is not None else None)
    questioner_session = _new_session(
        client, f"{context.state['session_name']}.{problem['id']}.q",
        context.state["session_id"], execution["questioner"],
    )
    answerer_start = len(client.session_messages(answerer_session))
    transcript = [{"role": "q", "text": question}]
    started = int(time.time() * 1000)
    status = "turn_limit"
    for turn in range(1, problem["maxTurns"] + 1):
        answer_message = _prompt(
            client, answerer_session, transcript[-1]["text"], execution["answerer"]
        )
        answer = "\n".join(text_parts(answer_message))
        transcript.append({"role": "a", "text": answer,
                           "message_id": answer_message.get("info", {}).get("id")})
        q_message = _prompt(
            client, questioner_session,
            _questioner_prompt(question, knowledge, answer, first=turn == 1),
            execution["questioner"],
        )
        action = _questioner_action(q_message)
        if action["action"] == "done":
            status = "completed"
            break
        if turn == problem["maxTurns"]:
            break
        transcript.append({"role": "q", "text": action["text"],
                           "message_id": q_message.get("info", {}).get("id")})
    ended = int(time.time() * 1000)
    metric_roles = context.state.get("metrics", context.manifest.metrics).get("roles", {})
    answerer_messages = client.session_messages(answerer_session)[answerer_start:]
    questioner_messages = client.session_messages(questioner_session)
    return {
        "id": problem["id"], "preflight": preflight, "status": status,
        "turns": sum(1 for item in transcript if item["role"] == "a"),
        "started_at": started, "end_at": ended, "elapsed_ms": ended - started,
        "answerer_session_id": answerer_session,
        "questioner_session_id": questioner_session,
        "transcript": transcript,
        "metrics": {
            "answerer": summarize_thread_metrics(
                answerer_messages,
                metric_roles.get(execution["answerer"], {}).get("commands", {}),
                now_ms=ended,
            ),
            "questioner": summarize_thread_metrics(
                questioner_messages,
                metric_roles.get(execution["questioner"], {}).get("commands", {}),
                now_ms=ended,
            ),
        },
        "outputs": _archive_outputs(context, problem["id"], execution["output"]),
    }


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
    with locked(context.root):
        state = load_state(context.root)
        state["phase"] = "active"
        state["benchmark"] = {"status": "running", "started_at": started,
                              "problems": []}
        save_state(context.root, state)
        context.state = state

    client = context.client()
    workspace = Path(context.state["workspace"])
    baseline_session = context.state["session_id"]
    records = []
    try:
        for index, problem in enumerate(execution["problems"]):
            is_preflight = index < execution["preflight"]
            _reset_outputs(workspace, execution["output"])
            if is_preflight:
                answerer_session = baseline_session
            else:
                baseline_message = latest_assistant(
                    client.session_messages(baseline_session)
                )
                baseline_message_id = (baseline_message.get("info", {}).get("id")
                                       if baseline_message else None)
                answerer_session = _fork_answerer(
                    client, baseline_session, baseline_message_id,
                    f"{context.state['session_name']}.{problem['id']}.a",
                )
            record = _run_problem(context, problem, answerer_session, is_preflight)
            records.append(record)
            with locked(context.root):
                state = load_state(context.root)
                state["benchmark"]["problems"] = records
                save_state(context.root, state)
        _reset_outputs(workspace, execution["output"])
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
    result = {
        "preflight": [record for record in records if record["preflight"]],
        "problems": [record for record in records if not record["preflight"]],
    }
    response = {
        "schema": "labflow.host-observation/v1",
        "session_name": context.state["session_name"],
        "timeline": {
            "clock": "unix_ms", "since": since,
            "next_since": max([since, *(event["at"] for event in events)]),
            "observed_at": observed, "waited_ms": observed - started,
            "events": events,
        },
        "result": result,
    }
    with locked(context.root):
        state = load_state(context.root)
        state["phase"] = "idle"
        state["benchmark"] = {"status": "completed", "started_at": started,
                              "completed_at": observed, "problems": records,
                              "response": response}
        save_state(context.root, state)
        context.state = state
    return response
