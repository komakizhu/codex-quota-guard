#!/usr/bin/env python3
"""Local, event-driven Codex quota guard.

The helper talks to the local Codex app-server.  Reading quota state does not
start a model turn.  It only starts a turn when the configured resume message
is needed after a reset.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import pathlib
import queue
import shutil
import socket
import struct
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo


DEFAULT_CODEX_EXECUTABLE = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/"
    "CodexCLI.app/Contents/MacOS/codex"
)
DEFAULT_RESUME_MESSAGE = "额度已重置，请从刚才被中断的位置继续执行。"
ACTIVE_THREAD_STATUS = "active"
ACTIVE_THREAD_STATUSES = {"active", "running", "working"}
IN_PROGRESS_TURN_STATUS = "inProgress"
TERMINAL_THREAD_STATUSES = {"idle", "completed", "archived"}
RESUME_TERMINAL_STATUSES = {"completed", "archived"}
UNKNOWN_THREAD_STATUSES = {"notLoaded", "loading"}
SHARED_SUPPORT_DIR = pathlib.Path.home() / "Library/Application Support/CodexQuotaGuard"


def default_state() -> dict[str, Any]:
    return {
        "paused_threads": {},
        "pause_episode_primary_resets_at": None,
        "resume_attempted_primary_resets_at": None,
        "next_primary_reset_at": None,
        "next_fixed_refresh_at": None,
        "fixed_refresh_schedule_signature": None,
        "pending_scheduled_refresh": None,
        "last_scheduled_refresh_at": None,
        "scheduled_refresh_missed_count": 0,
        "stop_attempts": {},
        "reset_attempted_for_credit_id": None,
        "reset_idempotency_key": None,
        "reset_pending_verification": None,
        "last_primary_remaining": None,
        "last_secondary_remaining": None,
        "last_reset_credit_warning_reset_at": None,
        "notification_keys": {},
        "connection_failure_notified": False,
        "connection_backoff_seconds": 1,
        "config_revision": None,
        "config_loaded_at": None,
        "updated_at": None,
    }


def remaining_percent(window: dict[str, Any] | None) -> float | None:
    if not window or window.get("usedPercent") is None:
        return None
    return 100.0 - float(window["usedPercent"])


@dataclass(frozen=True)
class Limits:
    primary_remaining: float | None
    secondary_remaining: float | None
    primary_resets_at: int | None
    secondary_resets_at: int | None
    reset_credit_count: int
    reset_credits: list[dict[str, Any]]
    raw: dict[str, Any]


def _codex_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    by_id = payload.get("rateLimitsByLimitId") or {}
    return by_id.get("codex") or payload.get("rateLimits") or {}


def parse_limits(payload: dict[str, Any]) -> Limits:
    snapshot = _codex_snapshot(payload)
    primary = snapshot.get("primary") or {}
    secondary = snapshot.get("secondary") or {}
    credits = payload.get("rateLimitResetCredits") or {}
    detailed = credits.get("credits")
    return Limits(
        primary_remaining=remaining_percent(primary),
        secondary_remaining=remaining_percent(secondary),
        primary_resets_at=primary.get("resetsAt"),
        secondary_resets_at=secondary.get("resetsAt"),
        reset_credit_count=int(credits.get("availableCount") or 0),
        reset_credits=list(detailed or []),
        raw=payload,
    )


def reset_due(now: float, reset_at: int | None, delay: int) -> bool:
    return reset_at is not None and now >= float(reset_at) + max(0, int(delay))


def next_scheduled_refresh(
    now: float,
    hours: list[int],
    offset_seconds: int,
    timezone_name: str,
) -> float:
    """Return the next optional fixed-clock refresh in the configured timezone."""
    timezone = ZoneInfo(timezone_name)
    current = datetime.fromtimestamp(now, timezone)
    candidates: list[datetime] = []
    for day_offset in range(3):
        day = current.date() + timedelta(days=day_offset)
        for raw_hour in hours:
            hour = 0 if raw_hour == 24 else raw_hour
            target_day = day + timedelta(days=1) if raw_hour == 24 else day
            candidate = datetime.combine(
                target_day, datetime_time(hour=hour, minute=0), tzinfo=timezone
            ) + timedelta(seconds=max(0, int(offset_seconds)))
            if candidate.timestamp() > now:
                candidates.append(candidate)
    if not candidates:
        raise ValueError("scheduled_refresh_hours 没有可用时刻")
    return min(candidates).timestamp()


def schedule_signature(config: dict[str, Any]) -> str:
    return json.dumps(
        {
            "hours": list(config["scheduled_refresh_hours"]),
            "offset": int(config["scheduled_refresh_offset_seconds"]),
            "timezone": str(config["timezone"]),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def retry_delay(attempt: int) -> int:
    return min(60, 2 ** max(0, min(int(attempt) - 1, 6)))


def is_candidate_thread(thread: dict[str, Any]) -> bool:
    return (thread.get("status") or {}).get("type") in ACTIVE_THREAD_STATUSES


def in_progress_turn_id(thread: dict[str, Any]) -> str | None:
    for turn in reversed(thread.get("turns") or []):
        if turn.get("status") == IN_PROGRESS_TURN_STATUS:
            return turn.get("id")
    return None


def load_json(path: pathlib.Path, fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else dict(fallback)
    except FileNotFoundError:
        return dict(fallback)


def save_json(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    temporary.replace(path)


def merged_config(value: dict[str, Any]) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "primary_warning_percent": 3.0,
        "secondary_warning_percent": 2.0,
        "post_reset_delay_seconds": 60,
        "fallback_poll_seconds": 0,
        "force_stop_active_turns": True,
        "resume_paused_turns": True,
        "auto_consume_reset_credit": False,
        "notify_desktop": True,
        "dry_run": False,
        "scheduled_refresh_enabled": True,
        "scheduled_refresh_hours": [7, 12, 17, 22],
        "scheduled_refresh_hour_shift": 0,
        "scheduled_refresh_offset_seconds": 60,
        "timezone": "Asia/Shanghai",
        "app_server_socket": str(
            pathlib.Path.home() / ".codex/app-server-control/app-server-control.sock"
        ),
        "exclude_thread_ids": [],
        "resume_messages": {},
        "codex_executable": DEFAULT_CODEX_EXECUTABLE,
        "state_file": str(SHARED_SUPPORT_DIR / "state.json"),
        "event_log_file": str(SHARED_SUPPORT_DIR / "events.jsonl"),
    }
    defaults.update(value)
    defaults["scheduled_refresh_enabled"] = True
    defaults["force_stop_active_turns"] = True
    for key in ("primary_warning_percent", "secondary_warning_percent"):
        number = float(defaults[key])
        if not 0 <= number <= 100:
            raise ValueError(f"{key} must be between 0 and 100")
        defaults[key] = number
    for key in ("post_reset_delay_seconds", "fallback_poll_seconds"):
        defaults[key] = max(0, int(defaults[key]))
    defaults["scheduled_refresh_offset_seconds"] = max(
        0, int(defaults["scheduled_refresh_offset_seconds"])
    )
    raw_shift = value.get("scheduled_refresh_hour_shift")
    if raw_shift is not None:
        try:
            shift = int(raw_shift)
        except (TypeError, ValueError) as error:
            raise ValueError("scheduled_refresh_hour_shift 必须是整数") from error
        if not -12 <= shift <= 12:
            raise ValueError("scheduled_refresh_hour_shift 必须在 -12 到 12 之间")
        defaults["scheduled_refresh_hour_shift"] = shift
        defaults["scheduled_refresh_hours"] = sorted(
            {((hour + shift) % 24) for hour in (7, 12, 17, 22)}
        )
    hours = defaults["scheduled_refresh_hours"]
    if isinstance(hours, str):
        hours = [part.strip() for part in hours.split(",") if part.strip()]
    try:
        parsed_hours = sorted({int(hour) for hour in hours})
    except (TypeError, ValueError) as error:
        raise ValueError("scheduled_refresh_hours 必须是 0–24 的整数列表") from error
    if any(hour < 0 or hour > 24 for hour in parsed_hours):
        raise ValueError("scheduled_refresh_hours 必须是 0–24 的整数列表")
    if not parsed_hours:
        raise ValueError("scheduled_refresh_hours 不能是空列表")
    defaults["scheduled_refresh_hours"] = parsed_hours
    ZoneInfo(str(defaults["timezone"]))
    defaults["exclude_thread_ids"] = [str(item) for item in defaults["exclude_thread_ids"]]
    defaults["resume_messages"] = dict(defaults["resume_messages"] or {})
    return defaults


def discover_executable(configured: str) -> str:
    if configured and pathlib.Path(configured).exists():
        return configured
    discovered = shutil.which("codex")
    if discovered:
        return discovered
    if pathlib.Path(DEFAULT_CODEX_EXECUTABLE).exists():
        return DEFAULT_CODEX_EXECUTABLE
    raise FileNotFoundError("找不到 Codex CLI；请在 config.json 设置 codex_executable")


class AppServerError(RuntimeError):
    pass


class AppServerClient:
    """JSON-RPC client for Codex's managed app-server WebSocket socket."""

    def __init__(
        self,
        executable: str,
        socket_path: str,
        request_timeout: float = 30.0,
    ):
        self.executable = executable
        self.socket_path = str(pathlib.Path(socket_path).expanduser())
        self.request_timeout = request_timeout
        self.socket: socket.socket | None = None
        self._reader_thread: threading.Thread | None = None
        self._condition = threading.Condition()
        self._write_lock = threading.Lock()
        self._next_id = 1
        self._responses: dict[int, dict[str, Any]] = {}
        self._notifications: queue.Queue[dict[str, Any]] = queue.Queue()
        self._connection_error: Exception | None = None
        self._closing = False

    def start(self) -> None:
        if self.socket is not None:
            return
        self._ensure_managed_daemon()
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(self.socket_path)
                self._websocket_handshake(sock)
                self.socket = sock
                self._closing = False
                self._connection_error = None
                self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
                self._reader_thread.start()
                self.call(
                    "initialize",
                    {
                        "clientInfo": {
                            "name": "codex-quota-guard",
                            "title": "Codex Quota Guard",
                            "version": "1.1.0",
                        }
                    },
                )
                self._send_notification("initialized", {})
                return
            except (OSError, AppServerError) as error:
                last_error = error
                self.close()
                if attempt == 0:
                    self._start_managed_daemon()
                    time.sleep(0.5)
        raise AppServerError(f"无法连接 Codex managed app-server: {last_error}")

    def _ensure_managed_daemon(self) -> None:
        if not pathlib.Path(self.socket_path).exists():
            self._start_managed_daemon()

    def _start_managed_daemon(self) -> None:
        subprocess.run(
            [self.executable, "app-server", "daemon", "start"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
        )

    def _websocket_handshake(self, sock: socket.socket) -> None:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            "GET / HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        sock.sendall(request)
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                raise AppServerError("Codex app-server 在 WebSocket 握手时断开")
            response += chunk
        if not response.startswith(b"HTTP/1.1 101"):
            raise AppServerError(f"Codex app-server WebSocket 握手失败: {response[:200]!r}")

    def _read_exact(self, size: int) -> bytes:
        if self.socket is None:
            raise AppServerError("Codex app-server socket 未连接")
        data = b""
        while len(data) < size:
            chunk = self.socket.recv(size - len(data))
            if not chunk:
                raise AppServerError("Codex app-server socket 已断开")
            data += chunk
        return data

    def _read_frame(self) -> tuple[int, bytes]:
        header = self._read_exact(2)
        first, second = header
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(8))[0]
        mask = self._read_exact(4) if second & 0x80 else None
        payload = self._read_exact(length)
        if mask:
            payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        return first & 0x0F, payload

    def _send_frame(self, payload: bytes, opcode: int = 1) -> None:
        if self.socket is None:
            raise AppServerError("Codex app-server socket 未连接")
        mask = os.urandom(4)
        length = len(payload)
        if length < 126:
            header = bytes([0x80 | opcode, 0x80 | length])
        elif length < 65536:
            header = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack("!H", length)
        else:
            header = bytes([0x80 | opcode, 0x80 | 127]) + struct.pack("!Q", length)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        with self._write_lock:
            self.socket.sendall(header + mask + masked)

    def _read_loop(self) -> None:
        try:
            while not self._closing:
                opcode, payload = self._read_frame()
                if opcode == 8:
                    raise AppServerError("Codex app-server WebSocket 已关闭")
                if opcode == 9:
                    self._send_frame(payload, opcode=10)
                    continue
                if opcode not in (1, 2):
                    continue
                message = json.loads(payload)
                with self._condition:
                    if isinstance(message.get("id"), int):
                        self._responses[message["id"]] = message
                        self._condition.notify_all()
                    elif message.get("method"):
                        self._notifications.put(message)
                        self._condition.notify_all()
        except Exception as error:
            if not self._closing:
                with self._condition:
                    self._connection_error = error
                    self._condition.notify_all()

    def _send_notification(self, method: str, params: dict[str, Any]) -> None:
        self._write({"jsonrpc": "2.0", "method": method, "params": params})

    def _write(self, value: dict[str, Any]) -> None:
        self._send_frame(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self.start_if_needed()
        with self._condition:
            request_id = self._next_id
            self._next_id += 1
            self._write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            deadline = time.monotonic() + self.request_timeout
            while request_id not in self._responses:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AppServerError(f"等待 {method} 超时")
                if self._connection_error:
                    raise AppServerError(f"调用 {method} 时连接断开: {self._connection_error}")
                self._condition.wait(timeout=remaining)
            response = self._responses.pop(request_id)
        if "error" in response:
            raise AppServerError(f"{method} 失败: {response['error']}")
        return response.get("result") or {}

    def start_if_needed(self) -> None:
        if self.socket is None:
            self.start()

    def next_notification(self, timeout: float | None) -> dict[str, Any] | None:
        self.start_if_needed()
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        with self._condition:
            while True:
                try:
                    return self._notifications.get_nowait()
                except queue.Empty:
                    pass
                if self._connection_error:
                    raise AppServerError(
                        f"等待 app-server 通知时连接断开: {self._connection_error}"
                    )
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._condition.wait(timeout=remaining)

    def close(self) -> None:
        self._closing = True
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.socket.close()
        self.socket = None


class StateStore:
    def __init__(
        self,
        state_file: pathlib.Path,
        event_log_file: pathlib.Path,
        persist: bool = True,
    ):
        self.state_file = state_file
        self.event_log_file = event_log_file
        self.persist = persist
        self.events: list[dict[str, Any]] = []
        self.state = load_json(state_file, default_state()) if persist else default_state()
        base = default_state()
        base.update(self.state)
        self.state = base

    def save(self) -> None:
        self.state["updated_at"] = int(time.time())
        if self.persist:
            save_json(self.state_file, self.state)

    def event(self, event: str, **details: Any) -> None:
        record = {"at": int(time.time()), "event": event, **details}
        self.events.append(record)
        if len(self.events) > 1000:
            del self.events[:-500]
        if self.persist:
            self.event_log_file.parent.mkdir(parents=True, exist_ok=True)
            with self.event_log_file.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(json.dumps(record, ensure_ascii=False), flush=True)


class QuotaGuard:
    def __init__(
        self,
        client: AppServerClient,
        config: dict[str, Any],
        store: StateStore,
        now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.client = client
        self.config = config
        self.store = store
        self.now = now
        self.sleep = sleep
        signature = schedule_signature(config)
        persisted_signature = self.store.state.get("fixed_refresh_schedule_signature")
        persisted_next = self.store.state.get("next_fixed_refresh_at")
        if persisted_next is not None and (
            not persisted_signature or persisted_signature == signature
        ):
            self.next_fixed_refresh_at = float(persisted_next)
        else:
            self.next_fixed_refresh_at = next_scheduled_refresh(
                self.now(), config["scheduled_refresh_hours"],
                config["scheduled_refresh_offset_seconds"], config["timezone"],
            )
        self.store.state["next_fixed_refresh_at"] = self.next_fixed_refresh_at
        self.store.state["fixed_refresh_schedule_signature"] = signature
        self.store.state["config_revision"] = config.get("config_revision")
        self.store.state["config_loaded_at"] = int(time.time())
        self.store.save()

    def read_limits(self) -> Limits:
        return parse_limits(self.client.call("account/rateLimits/read", {}))

    def list_threads(self) -> list[dict[str, Any]]:
        data: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "archived": False,
                "limit": 100,
                "useStateDbOnly": False,
            }
            if cursor:
                params["cursor"] = cursor
            result = self.client.call("thread/list", params)
            data.extend(result.get("data") or [])
            cursor = result.get("nextCursor")
            if not cursor:
                return data

    def read_thread(self, thread_id: str) -> dict[str, Any]:
        result = self.client.call(
            "thread/read", {"threadId": thread_id, "includeTurns": True}
        )
        return result.get("thread") or {}

    def notify(self, title: str, message: str) -> bool:
        self.store.event("notification", title=title, message=message)
        if self._dry_run():
            self.store.event("would_notify", title=title, message=message)
            return True
        if not self.config.get("notify_desktop", True):
            return True
        script = f'display notification {json.dumps(message, ensure_ascii=False)} with title {json.dumps(title, ensure_ascii=False)}'
        try:
            subprocess.run(
                ["osascript", "-e", script],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            return True
        except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError) as error:
            self.store.event("notification_delivery_failed", error=str(error))
            return False

    def _notify_once(self, key: str, title: str, message: str) -> bool:
        keys = self.store.state.setdefault("notification_keys", {})
        if key in keys:
            return False
        delivered = self.notify(title, message)
        if delivered:
            keys[key] = int(self.now())
            self.store.save()
        return delivered

    def _dry_run(self) -> bool:
        return bool(self.config.get("dry_run"))

    def _verify_interrupted(self, thread_id: str, turn_id: str) -> bool:
        deadline = self.now() + 10
        while self.now() <= deadline:
            thread = self.read_thread(thread_id)
            if not isinstance(thread, dict) or not thread:
                return False
            status = (thread.get("status") or {}).get("type")
            active_turn_id = in_progress_turn_id(thread)
            if active_turn_id is None and status in TERMINAL_THREAD_STATUSES:
                return True
            if active_turn_id is not None and active_turn_id != turn_id:
                return False
            self.sleep(0.25)
        return False

    def _active_turn(self, thread: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        if not is_candidate_thread(thread):
            return thread, None
        hydrated = self.read_thread(thread["id"])
        return hydrated, in_progress_turn_id(hydrated)

    def _stop_attempt_key(self, reset_at: int, thread_id: str, turn_id: str) -> str:
        return f"{int(reset_at)}:{thread_id}:{turn_id}"

    def _record_stop_failure(self, key: str, error: str) -> None:
        attempts = self.store.state.setdefault("stop_attempts", {})
        previous = attempts.get(key) or {}
        count = int(previous.get("attempts") or 0) + 1
        attempts[key] = {
            "status": "failed",
            "attempts": count,
            "last_attempt_at": int(self.now()),
            "next_retry_at": int(self.now()) + retry_delay(count),
            "error": error,
        }
        if len(attempts) > 500:
            ordered = sorted(
                attempts.items(), key=lambda item: int(item[1].get("last_attempt_at") or 0)
            )
            self.store.state["stop_attempts"] = dict(ordered[-250:])

    def _goal_evidence(self, thread: dict[str, Any]) -> bool:
        for key in ("goal", "goalMode", "goal_mode", "activeGoal", "active_goal"):
            value = thread.get(key)
            if value is True:
                return True
            if isinstance(value, dict):
                if value.get("active") is True or value.get("status") in {"active", "running", "working"}:
                    return True
                if value.get("id") or value.get("objective"):
                    return True
            if isinstance(value, str) and value.lower() in {"active", "running", "working"}:
                return True
        return False

    def force_stop_active_threads(self, reset_at: int) -> int:
        stopped_count = 0
        excluded = set(self.config.get("exclude_thread_ids") or [])
        for listed in self.list_threads():
            thread_id = listed.get("id")
            if not thread_id or thread_id in excluded or not is_candidate_thread(listed):
                continue
            try:
                thread, turn_id = self._active_turn(listed)
                if not turn_id:
                    self.store.event(
                        "active_thread_without_verified_turn", thread_id=thread_id,
                        title=thread.get("name") if isinstance(thread, dict) else None,
                    )
                    continue
                key = self._stop_attempt_key(reset_at, thread_id, turn_id)
                attempt = (self.store.state.setdefault("stop_attempts", {})).get(key) or {}
                if attempt.get("status") == "verified":
                    continue
                if int(attempt.get("next_retry_at") or 0) > int(self.now()):
                    continue
                if self._dry_run():
                    self.store.event("would_interrupt", thread_id=thread_id, turn_id=turn_id)
                    continue
                self.client.call(
                    "turn/interrupt", {"threadId": thread_id, "turnId": turn_id}
                )
                if not self._verify_interrupted(thread_id, turn_id):
                    self._record_stop_failure(key, "中断后仍无法确认任务已停止")
                    self.store.save()
                    self.store.event(
                        "interrupt_verification_failed", thread_id=thread_id, turn_id=turn_id
                    )
                    continue
                self.store.state.setdefault("stop_attempts", {})[key] = {
                    "status": "verified",
                    "attempts": int(attempt.get("attempts") or 0) + 1,
                    "last_attempt_at": int(self.now()),
                }
                self.store.state["paused_threads"][thread_id] = {
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "title": thread.get("name") or thread.get("preview") or thread_id,
                    "cwd": thread.get("cwd"),
                    "reset_at": reset_at,
                }
                self.store.save()
                self.store.event(
                    "forced_stop_verified", thread_id=thread_id, turn_id=turn_id, reset_at=reset_at
                )
                stopped_count += 1
                self.notify("Codex 额度保护", f"已强制停止：{thread.get('name') or thread_id}")
            except AppServerError as error:
                self._record_stop_failure(
                    self._stop_attempt_key(reset_at, thread_id, turn_id or "unknown"), str(error)
                )
                self.store.save()
                self.store.event(
                    "forced_stop_unavailable_or_failed", thread_id=thread_id, error=str(error)
                )
                self._notify_once(
                    f"forced-stop-failed:{reset_at}:{thread_id}",
                    "Codex 额度保护失败",
                    f"任务 {thread_id} 无法完成原生强制停止",
                )
        return stopped_count

    def _resume_message(self, thread_id: str) -> str:
        messages = self.config.get("resume_messages") or {}
        return str(messages.get(thread_id) or DEFAULT_RESUME_MESSAGE)

    def _verify_started(self, thread_id: str, expected_turn_id: str | None = None) -> tuple[bool, bool]:
        deadline = self.now() + 10
        while self.now() <= deadline:
            thread = self.read_thread(thread_id)
            if isinstance(thread, dict) and thread:
                status = (thread.get("status") or {}).get("type")
                turn_id = in_progress_turn_id(thread)
                if status in ACTIVE_THREAD_STATUSES and turn_id and (
                    expected_turn_id is None or turn_id == expected_turn_id
                ):
                    return True, self._goal_evidence(thread)
            self.sleep(0.25)
        return False, False

    def _schedule_resume_retry(self, record: dict[str, Any], error: str) -> None:
        attempts = int(record.get("resume_attempts") or 0) + 1
        record["resume_attempts"] = attempts
        record["next_resume_retry_at"] = int(self.now()) + retry_delay(attempts)
        record["last_resume_error"] = error
        self.store.event(
            "resume_retry_scheduled",
            thread_id=record.get("thread_id"),
            attempts=attempts,
            retry_at=record["next_resume_retry_at"],
            error=error,
        )

    def _resume_failure(self, record: dict[str, Any], error: str) -> None:
        self._schedule_resume_retry(record, error)
        self._notify_once(
            f"resume-failed:{record.get('thread_id')}:{record.get('reset_at')}",
            "Codex 额度恢复失败",
            f"任务 {record.get('title') or record.get('thread_id')} 暂时无法恢复：{error}",
        )

    def _started_turn_id(self, result: dict[str, Any]) -> str | None:
        turn = result.get("turn")
        if isinstance(turn, dict) and turn.get("id"):
            return str(turn["id"])
        value = result.get("turnId") or result.get("turn_id")
        return str(value) if value else None

    def resume_paused_threads(self, limits: Limits | int) -> None:
        paused = dict(self.store.state.get("paused_threads") or {})
        if not paused or not self.config.get("resume_paused_turns", True):
            return
        if isinstance(limits, int):
            primary_remaining = 101.0
            compatibility_reset_at = limits
        else:
            primary_remaining = limits.primary_remaining
            compatibility_reset_at = None
        threshold = float(self.config["primary_warning_percent"])
        if primary_remaining is None or primary_remaining <= threshold:
            return
        for thread_id, record in paused.items():
            reset_at = int(record.get("reset_at") or compatibility_reset_at or 0)
            if not reset_at or not reset_due(
                self.now(), reset_at, int(self.config["post_reset_delay_seconds"])
            ):
                continue
            if int(record.get("next_resume_retry_at") or 0) > int(self.now()):
                continue
            try:
                thread = self.read_thread(thread_id)
                if not isinstance(thread, dict) or not thread:
                    self._resume_failure(record, "任务读取结果为空")
                    continue
                status = (thread.get("status") or {}).get("type")
                if status in RESUME_TERMINAL_STATUSES:
                    self.store.event(
                        "resume_skipped_terminal", thread_id=thread_id, status=status
                    )
                    self.store.state["paused_threads"].pop(thread_id, None)
                    continue
                if status == "notLoaded":
                    if self._dry_run():
                        self.store.event("would_resume_thread", thread_id=thread_id)
                        continue
                    else:
                        resumed = self.client.call(
                            "thread/resume", {"threadId": thread_id, "excludeTurns": False}
                        )
                        thread = resumed.get("thread") or thread
                        status = (thread.get("status") or {}).get("type")
                    if status == "notLoaded":
                        self._resume_failure(record, "任务仍处于 notLoaded")
                        continue
                if in_progress_turn_id(thread):
                    self.store.event("resume_skipped_already_active", thread_id=thread_id)
                    self.store.state["paused_threads"].pop(thread_id, None)
                    continue
                if status != "idle":
                    self._resume_failure(record, f"任务状态不明确：{status or 'unknown'}")
                    continue
                if self._dry_run():
                    self.store.event("would_resume", thread_id=thread_id, reset_at=reset_at)
                    continue
                result = self.client.call(
                    "turn/start",
                    {
                        "threadId": thread_id,
                        "input": [{"type": "text", "text": self._resume_message(thread_id)}],
                    },
                )
                started_turn_id = self._started_turn_id(result)
                verified, goal_confirmed = self._verify_started(thread_id, started_turn_id)
                if not verified:
                    self._resume_failure(record, "启动后未确认新的活动 turn")
                    self.store.event("resume_verification_failed", thread_id=thread_id)
                    continue
                self.store.state["paused_threads"].pop(thread_id, None)
                record["resume_attempts"] = 0
                record.pop("next_resume_retry_at", None)
                self.store.save()
                goal_requested = "create_goal" in self._resume_message(thread_id) or "goal 模式" in self._resume_message(thread_id)
                if goal_requested and not goal_confirmed:
                    event_name = "resume_started_goal_unconfirmed"
                    message = f"额度已重置，已启动恢复 turn，但 goal 模式尚未确认：{record.get('title') or thread_id}"
                elif goal_requested:
                    event_name = "resume_verified_goal"
                    message = f"额度已重置，已恢复 goal：{record.get('title') or thread_id}"
                else:
                    event_name = "resume_verified"
                    message = f"额度已重置，已恢复：{record.get('title') or thread_id}"
                self.store.event(
                    event_name,
                    thread_id=thread_id,
                    reset_at=reset_at,
                    goal_confirmed=goal_confirmed,
                )
                self.notify("Codex 额度保护", message)
            except AppServerError as error:
                self._resume_failure(record, str(error))
                self.store.event("resume_failed", thread_id=thread_id, error=str(error))
        if not self.store.state["paused_threads"]:
            self.store.state["pause_episode_primary_resets_at"] = None
        self.store.save()

    def consume_reset_credit(self, limits: Limits) -> None:
        if not self.config.get("auto_consume_reset_credit", False):
            return
        if limits.secondary_remaining is None:
            return
        if limits.secondary_remaining > float(self.config["secondary_warning_percent"]):
            return
        available = [credit for credit in limits.reset_credits if credit.get("status") == "available"]
        if limits.reset_credit_count <= 0 or not available:
            if self.store.state.get("last_reset_credit_warning_reset_at") == limits.secondary_resets_at:
                return
            self.store.state["last_reset_credit_warning_reset_at"] = limits.secondary_resets_at
            self.store.save()
            self.store.event(
                "reset_credit_unavailable",
                available_count=limits.reset_credit_count,
                reason="没有可识别的可用 credit id",
            )
            self.notify("Codex bankreset 不可用", "总额度已低于预警值，但没有可识别的可用重置卡")
            return
        credit_id = available[0].get("id")
        if not credit_id:
            self.store.event("reset_credit_unavailable", reason="可用重置卡缺少 credit id")
            self.notify("Codex bankreset 不可用", "可用重置卡缺少明确 credit id，未执行重置")
            return
        if self.store.state.get("reset_attempted_for_credit_id") == credit_id:
            return
        if self._dry_run():
            self.store.event("would_consume_reset_credit", credit_id=credit_id)
            return
        params: dict[str, Any] = {"idempotencyKey": str(uuid.uuid4())}
        params["creditId"] = credit_id
        self.store.state["reset_attempted_for_credit_id"] = credit_id
        self.store.state["reset_idempotency_key"] = params["idempotencyKey"]
        self.store.state["reset_pending_verification"] = {
            "credit_id": credit_id,
            "attempted_at": int(self.now()),
            "before_secondary_remaining": limits.secondary_remaining,
            "before_secondary_resets_at": limits.secondary_resets_at,
        }
        self.store.save()
        try:
            result = self.client.call("account/rateLimitResetCredit/consume", params)
            verified = self.read_limits()
            matching_credit = [
                credit for credit in verified.reset_credits if credit.get("id") == credit_id
            ]
            credit_consumed = verified.reset_credit_count < limits.reset_credit_count or bool(
                matching_credit and matching_credit[0].get("status") != "available"
            )
            quota_changed = (
                verified.secondary_remaining is not None
                and limits.secondary_remaining is not None
                and verified.secondary_remaining > limits.secondary_remaining
            ) or verified.secondary_resets_at != limits.secondary_resets_at
            if credit_consumed and quota_changed:
                self.store.state["reset_pending_verification"] = None
                self.store.save()
                self.store.event(
                    "reset_credit_consumed_and_verified",
                    credit_id=credit_id,
                    result=result,
                    secondary_remaining=verified.secondary_remaining,
                )
                self.notify("Codex bankreset", "已使用一张可用重置卡并确认额度已变化")
            else:
                self.store.event(
                    "reset_credit_pending_verification",
                    credit_id=credit_id,
                    result=result,
                    credit_consumed=credit_consumed,
                    quota_changed=quota_changed,
                    secondary_remaining=verified.secondary_remaining,
                )
                self.notify("Codex bankreset 待确认", "接口已返回，但重置卡消耗或额度变化尚未得到完整确认")
        except AppServerError as error:
            self.store.event("reset_credit_failed", credit_id=credit_id, error=str(error))
            self.notify("Codex bankreset 失败", str(error))

    def handle_limits(self, limits: Limits) -> None:
        state = self.store.state
        threshold = float(self.config["primary_warning_percent"])
        if limits.primary_remaining is not None and limits.primary_remaining <= threshold:
            reset_at = limits.primary_resets_at
            if reset_at is None:
                self._notify_once(
                    "low-quota-without-reset",
                    "Codex 额度保护失败",
                    "5 小时余量低于阈值，但接口没有返回重置时间，未操作任何任务",
                )
                self.store.event("low_quota_without_reset_timestamp")
            else:
                if state.get("pause_episode_primary_resets_at") != reset_at:
                    state["pause_episode_primary_resets_at"] = reset_at
                    state["resume_attempted_primary_resets_at"] = None
                stopped_count = 0
                if self.config.get("force_stop_active_turns", True):
                    stopped_count = self.force_stop_active_threads(int(reset_at)) or 0
                state["pause_episode_primary_resets_at"] = reset_at
                self.store.save()
                if stopped_count:
                    self.notify(
                        "Codex 额度保护",
                        f"5 小时余量 {limits.primary_remaining:.1f}%，已强制停止 {stopped_count} 个任务",
                    )
                elif not state.get("paused_threads") and state.get("notification_keys", {}).get(f"no-stop:{reset_at}") is None:
                    self.store.event(
                        "pause_cycle_no_verified_stops",
                        primary_remaining=limits.primary_remaining,
                        reset_at=reset_at,
                    )
                    self._notify_once(
                        f"no-stop:{reset_at}",
                        "Codex 额度保护",
                        f"5 小时余量 {limits.primary_remaining:.1f}%，未发现可验证的活动 turn；未暂停任何任务",
                    )

        elif limits.primary_remaining is not None and limits.primary_remaining > threshold:
            self.resume_paused_threads(limits)

        self.consume_reset_credit(limits)
        state["next_primary_reset_at"] = limits.primary_resets_at
        state["last_primary_remaining"] = limits.primary_remaining
        state["last_secondary_remaining"] = limits.secondary_remaining
        self.store.save()

    def next_wait_seconds(self) -> float | None:
        deadlines: list[float] = []
        if self.config.get("resume_paused_turns", True):
            for record in (self.store.state.get("paused_threads") or {}).values():
                reset_at = record.get("reset_at")
                if reset_at is None:
                    continue
                due = float(reset_at) + int(self.config["post_reset_delay_seconds"])
                retry_at = record.get("next_resume_retry_at")
                if retry_at is not None:
                    due = max(due, float(retry_at))
                deadlines.append(due)
        pending = self.store.state.get("pending_scheduled_refresh") or {}
        if pending.get("next_retry_at") is not None:
            deadlines.append(float(pending["next_retry_at"]))
        fallback = int(self.config.get("fallback_poll_seconds") or 0)
        if fallback > 0:
            deadlines.append(self.now() + fallback)
        if self.config.get("scheduled_refresh_enabled", True):
            deadlines.append(self.next_fixed_refresh_at)
        if not deadlines:
            return 3600.0
        return max(0.0, min(deadlines) - self.now())

    def _pending_refresh_message(self, pending: dict[str, Any]) -> str:
        primary = pending.get("primary_remaining")
        secondary = pending.get("secondary_remaining")
        primary_text = "未知" if primary is None else f"{float(primary):.1f}%"
        secondary_text = "未知" if secondary is None else f"{float(secondary):.1f}%"
        missed = int(pending.get("missed_count") or 0)
        prefix = f"已合并补发，错过 {missed} 个定时点；" if missed else ""
        return f"{prefix}已刷新：5 小时剩余 {primary_text}，总额度剩余 {secondary_text}"

    def _deliver_pending_refresh(self) -> None:
        pending = self.store.state.get("pending_scheduled_refresh")
        if not pending or int(pending.get("next_retry_at") or 0) > int(self.now()):
            return
        message = self._pending_refresh_message(pending)
        if self.notify("Codex 定时额度提醒", message):
            self.store.event(
                "scheduled_refresh_notification_delivered",
                scheduled_at=pending.get("scheduled_at"),
                missed_count=pending.get("missed_count", 0),
            )
            self.store.state["pending_scheduled_refresh"] = None
            self.store.save()
            return
        attempts = int(pending.get("notification_attempts") or 0) + 1
        pending["notification_attempts"] = attempts
        pending["next_retry_at"] = int(self.now()) + retry_delay(attempts)
        self.store.event(
            "scheduled_refresh_notification_retry_scheduled",
            scheduled_at=pending.get("scheduled_at"),
            retry_at=pending["next_retry_at"],
            attempts=attempts,
        )
        self.store.save()

    def _process_fixed_refresh(self, limits: Limits) -> None:
        if not self.config.get("scheduled_refresh_enabled", True):
            return
        now = self.now()
        if self.next_fixed_refresh_at > now:
            self._deliver_pending_refresh()
            return
        due_times: list[float] = []
        next_at = self.next_fixed_refresh_at
        while next_at <= now:
            due_times.append(next_at)
            next_at = next_scheduled_refresh(
                next_at + 1,
                self.config["scheduled_refresh_hours"],
                self.config["scheduled_refresh_offset_seconds"],
                self.config["timezone"],
            )
            if len(due_times) >= 100:
                break
        self.next_fixed_refresh_at = next_at
        self.store.state["next_fixed_refresh_at"] = next_at
        self.store.state["last_scheduled_refresh_at"] = due_times[-1]
        self.store.state["scheduled_refresh_missed_count"] = max(0, len(due_times) - 1)
        pending = self.store.state.get("pending_scheduled_refresh") or {}
        missed_count = int(pending.get("missed_count") or 0) + max(0, len(due_times) - 1)
        self.store.state["pending_scheduled_refresh"] = {
            "scheduled_at": due_times[-1],
            "missed_count": missed_count,
            "primary_remaining": limits.primary_remaining,
            "secondary_remaining": limits.secondary_remaining,
            "notification_attempts": 0,
            "next_retry_at": int(now),
        }
        self.store.event(
            "scheduled_refresh_completed",
            scheduled_at=due_times[-1],
            missed_count=missed_count,
            primary_remaining=limits.primary_remaining,
            secondary_remaining=limits.secondary_remaining,
            notification_delivered=False,
        )
        self._deliver_pending_refresh()

    def run_once(self) -> Limits:
        limits = self.read_limits()
        self.handle_limits(limits)
        self._process_fixed_refresh(limits)
        self.store.save()
        return limits

    def run_forever(self) -> None:
        watched = {"account/rateLimits/updated", "turn/started", "thread/status/changed"}
        while True:
            try:
                self.run_once()
                self.store.state["connection_backoff_seconds"] = 1
                if self.store.state.get("connection_failure_notified"):
                    self.store.state["connection_failure_notified"] = False
                    self.store.event("connection_recovered")
                    self.store.save()
                while True:
                    notification = self.client.next_notification(self.next_wait_seconds())
                    if notification is None or notification.get("method") in watched:
                        break
            except (AppServerError, OSError) as error:
                self.client.close()
                self.store.event("connection_error", error=str(error))
                if not self.store.state.get("connection_failure_notified"):
                    self.store.state["connection_failure_notified"] = True
                    self.store.save()
                    self.notify(
                        "Codex 额度守护连接失败",
                        f"暂时无法读取 Codex 额度，将自动重连：{error}",
                    )
                backoff = int(self.store.state.get("connection_backoff_seconds") or 1)
                self.sleep(backoff)
                self.store.state["connection_backoff_seconds"] = min(60, backoff * 2)
                self.store.save()


def print_status(config: dict[str, Any]) -> None:
    state = load_json(pathlib.Path(config["state_file"]).expanduser(), default_state())
    print(json.dumps(state, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="事件驱动的 Codex 本地额度保护器")
    parser.add_argument("--config", required=True, help="JSON 配置文件")
    parser.add_argument("--once", action="store_true", help="只读取并执行一次策略")
    parser.add_argument("--status", action="store_true", help="打印持久化状态后退出")
    parser.add_argument("--dry-run", action="store_true", help="只记录动作，不中断、恢复或消耗重置卡")
    args = parser.parse_args(argv)
    config = merged_config(load_json(pathlib.Path(args.config), {}))
    if args.dry_run:
        config["dry_run"] = True
    if args.status:
        print_status(config)
        return 0

    executable = discover_executable(str(config["codex_executable"]))
    store = StateStore(
        pathlib.Path(config["state_file"]).expanduser(),
        pathlib.Path(config["event_log_file"]).expanduser(),
        persist=not bool(config.get("dry_run")),
    )
    client = AppServerClient(executable, str(config["app_server_socket"]))
    guard = QuotaGuard(client, config, store)
    try:
        if args.once:
            limits = guard.run_once()
            print(
                json.dumps(
                    {
                        "primary_remaining": limits.primary_remaining,
                        "secondary_remaining": limits.secondary_remaining,
                        "primary_resets_at": limits.primary_resets_at,
                    },
                    ensure_ascii=False,
                )
            )
        else:
            guard.run_forever()
    except KeyboardInterrupt:
        return 0
    except (AppServerError, OSError, ValueError) as error:
        store.event("fatal_error", error=str(error))
        return 1
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
