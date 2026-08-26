from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
import time
from typing import Any, Iterator

from .config import ControlError


class Client:
    def __init__(self, server_url: str, workspace: str, session_id: str | None = None, timeout: float = 5):
        parsed = urllib.parse.urlsplit(server_url)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.path or parsed.query:
            raise ControlError(f"unsafe opencode server URL: {server_url}")
        self.url = server_url.rstrip("/"); self.workspace = workspace; self.session_id = session_id; self.timeout = timeout

    def _request(self, path: str, method: str = "GET", payload: Any = None,
                 timeout: float | None = None) -> Any:
        separator = "&" if "?" in path else "?"
        path += separator + urllib.parse.urlencode({"directory": self.workspace})
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(self.url + path, data=body, method=method,
                                         headers={"Content-Type": "application/json"} if body else {})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        error: Exception | None = None
        for attempt in range(4):
            try:
                with opener.open(request, timeout=timeout or self.timeout) as response:
                    raw = response.read()
                break
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                error = exc
                if attempt < 3: time.sleep(.15 * (attempt + 1))
        else:
            raise ControlError(f"opencode daemon unavailable after retries: {error}", 69) from None
        if not raw: return None
        try: return json.loads(raw)
        except json.JSONDecodeError as exc: raise ControlError(f"malformed opencode response: {exc}") from None

    def health(self) -> Any: return self._request("/global/health")
    def sessions(self) -> list[dict[str, Any]]:
        value = self._request("/session")
        if not isinstance(value, list):
            raise ControlError("opencode returned an invalid session list", 69)
        return value
    def statuses(self) -> dict[str, Any]: return self._request("/session/status")
    def status(self) -> dict[str, Any]: return self.statuses().get(self.session_id, {"type": "idle"})
    def messages(self) -> list[dict[str, Any]]: return self._request(f"/session/{self.session_id}/message")
    def children(self, session_id: str | None = None) -> list[dict[str, Any]]:
        return self._request(f"/session/{session_id or self.session_id}/children")
    def session_messages(self, session_id: str) -> list[dict[str, Any]]:
        return self._request(f"/session/{session_id}/message")
    def create_session(self, title: str, parent_id: str | None = None,
                       agent: str | None = None) -> dict[str, Any]:
        payload = {"title": title}
        if parent_id is not None: payload["parentID"] = parent_id
        if agent is not None: payload["agent"] = agent
        return self._request("/session", "POST", payload)
    def fork_session(self, session_id: str, message_id: str | None = None) -> dict[str, Any]:
        payload = {} if message_id is None else {"messageID": message_id}
        return self._request(f"/session/{session_id}/fork", "POST", payload)
    def update_session(self, session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request(f"/session/{session_id}", "PATCH", payload)
    def prompt(self, text: str, agent: str | None = None) -> Any:
        return self.prompt_session(str(self.session_id), text, agent)
    def prompt_session(self, session_id: str, text: str, agent: str | None = None) -> Any:
        payload = {"parts": [{"type": "text", "text": text}]}
        if agent is not None: payload["agent"] = agent
        return self._request(f"/session/{session_id}/prompt_async", "POST", payload)
    def abort_session(self, session_id: str) -> Any:
        return self._request(f"/session/{session_id}/abort", "POST", {})

    def events(self, timeout: float | None = None) -> Iterator[dict[str, Any]]:
        query = urllib.parse.urlencode({"directory": self.workspace})
        request = urllib.request.Request(f"{self.url}/event?{query}")
        try:
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=timeout) as response:
                for raw in response:
                    if raw.startswith(b"data: "):
                        try: yield json.loads(raw[6:])
                        except json.JSONDecodeError: continue
        except TimeoutError:
            return
        except (urllib.error.URLError, OSError) as exc:
            raise ControlError(f"event stream unavailable: {exc}", 69) from None
