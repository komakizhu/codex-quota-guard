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
import math
import os
import pathlib
import queue
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone as datetime_timezone
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
REFRESH_BRIDGE_CAPABILITY_VERSION = 1


def default_state() -> dict[str, Any]:
    return {
        "paused_threads": {},
        "pause_episode_primary_resets_at": None,
        "resume_attempted_primary_resets_at": None,
        "next_primary_reset_at": None,
        "next_fixed_refresh_at": None,
        "fixed_refresh_schedule_signature": None,
        "pending_scheduled_refresh": None,
        "pending_scheduled_refresh_events": [],
        "scheduled_refresh_bridge_thread_id": None,
        "scheduled_refresh_bridge_created_at": None,
        "scheduled_refresh_bridge_capability_version": None,
        "last_consumed_quota_sequence": 0,
        "last_observed_limits": None,
        "action_instance_id": None,
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
        "quota_sequence": 0,
        "quota_read_started_at": None,
        "quota_read_finished_at": None,
        "next_quota_check_at": None,
        "quota_check_interval_seconds": None,
        "quota_check_mode": None,
        "last_primary_success_at": None,
        "last_secondary_success_at": None,
        "primary_data_expires_at": None,
        "secondary_data_expires_at": None,
        "primary_data_status": "unknown",
        "secondary_data_status": "unknown",
        "primary_last_error": None,
        "secondary_last_error": None,
        "quota_failure_notified": {"primary": False, "secondary": False},
        "config_revision": None,
        "config_loaded_at": None,
        "updated_at": None,
    }


def remaining_percent(window: dict[str, Any] | None) -> float | None:
    if not isinstance(window, dict):
        return None
    value = window.get("usedPercent")
    if isinstance(value, bool):
        return None
    try:
        used = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(used) or not 0.0 <= used <= 100.0:
        return None
    return 100.0 - used


def valid_reset_timestamp(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return int(number)


def timestamp_value(value: Any) -> float | None:
    """Parse numeric or ISO-8601 timestamps without treating bad data as evidence."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = None
    if number is not None:
        return number if math.isfinite(number) else None
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime_timezone.utc)
    result = parsed.timestamp()
    return result if math.isfinite(result) else None


def effective_window_status(
    state: dict[str, Any],
    label: str,
    now: float,
) -> str:
    """Expose expired data as expired even before the next read attempt."""
    status = str(state.get(f"{label}_data_status") or "unknown")
    expiry = timestamp_value(state.get(f"{label}_data_expires_at"))
    if status == "current" and expiry is not None and now > expiry:
        return "expired"
    return status


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
    if isinstance(by_id, dict):
        selected = by_id.get("codex")
        if isinstance(selected, dict):
            return selected
    fallback = payload.get("rateLimits") or {}
    return fallback if isinstance(fallback, dict) else {}


def parse_limits(payload: dict[str, Any]) -> Limits:
    if not isinstance(payload, dict):
        payload = {}
    snapshot = _codex_snapshot(payload)
    primary = snapshot.get("primary") if isinstance(snapshot, dict) else {}
    secondary = snapshot.get("secondary") if isinstance(snapshot, dict) else {}
    primary = primary if isinstance(primary, dict) else {}
    secondary = secondary if isinstance(secondary, dict) else {}
    credits = payload.get("rateLimitResetCredits") or {}
    credits = credits if isinstance(credits, dict) else {}
    detailed = credits.get("credits")
    try:
        count = int(credits.get("availableCount") or 0)
    except (TypeError, ValueError):
        count = 0
    return Limits(
        primary_remaining=remaining_percent(primary),
        secondary_remaining=remaining_percent(secondary),
        primary_resets_at=valid_reset_timestamp(primary.get("resetsAt")),
        secondary_resets_at=valid_reset_timestamp(secondary.get("resetsAt")),
        reset_credit_count=max(0, count),
        reset_credits=[item for item in (detailed or []) if isinstance(item, dict)],
        raw=payload,
    )


def reset_due(now: float, reset_at: int | None, delay: int) -> bool:
    return reset_at is not None and now >= float(reset_at) + max(0, int(delay))


def next_scheduled_refresh(
    now: float,
    hours: list[int],
    offset_seconds: int,
    timezone_name: str,
    hour_shift: int = 0,
) -> float:
    """Return the next fixed refresh, accumulating the offset per schedule slot."""
    timezone = ZoneInfo(timezone_name)
    current = datetime.fromtimestamp(now, timezone)
    candidates: list[datetime] = []
    # The hour shift moves the four base slots as a group, while the offset is
    # deliberately applied by slot: 7:00, 12:01, 17:02, 22:03 at 60 seconds.
    # Persisted hours are kept in slot order so wrapped schedules remain
    # unambiguous (for example, 13, 18, 23, 4 after a +6 hour shift).
    slot_hours = list(hours)
    if int(hour_shift) and slot_hours == [7, 12, 17, 22]:
        slot_hours = [((hour + int(hour_shift)) % 24) for hour in slot_hours]
    slot_offset_seconds = max(0, int(offset_seconds))
    for day_offset in range(-1, 4):
        day = current.date() + timedelta(days=day_offset)
        for slot_index, raw_hour in enumerate(slot_hours):
            total_seconds = (
                int(raw_hour) * 3600
                + slot_index * slot_offset_seconds
            )
            day_carry, seconds_of_day = divmod(total_seconds, 24 * 3600)
            hour, remainder = divmod(seconds_of_day, 3600)
            minute, second = divmod(remainder, 60)
            candidate = datetime.combine(
                day + timedelta(days=day_carry),
                datetime_time(hour=hour, minute=minute, second=second),
                tzinfo=timezone,
            )
            if candidate.timestamp() > now:
                candidates.append(candidate)
    if not candidates:
        raise ValueError("scheduled_refresh_hours 没有可用时刻")
    return min(candidates).timestamp()


def schedule_signature(config: dict[str, Any]) -> str:
    return json.dumps(
        {
            "algorithm": "per-slot-offset-v1",
            "hours": list(config["scheduled_refresh_hours"]),
            "hour_shift": int(config.get("scheduled_refresh_hour_shift", 0)),
            "offset": int(config["scheduled_refresh_offset_seconds"]),
            "timezone": str(config["timezone"]),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def retry_delay(attempt: int) -> int:
    return min(60, 2 ** max(0, min(int(attempt) - 1, 6)))


def _interpolate_interval(
    remaining: float,
    lower_percent: float,
    upper_percent: float,
    lower_seconds: int,
    upper_seconds: int,
) -> int:
    if upper_percent <= lower_percent:
        return max(1, int(lower_seconds))
    ratio = (remaining - lower_percent) / (upper_percent - lower_percent)
    ratio = min(1.0, max(0.0, ratio))
    value = lower_seconds + ratio * (upper_seconds - lower_seconds)
    return max(1, int(round(value)))


def quota_gradient_interval(
    remaining: float | None,
    config: dict[str, Any],
) -> int:
    """Return a monotonic local-read interval for the most urgent quota.

    The default curve has these anchor points:
    100% -> 600s, 90% -> 500s, 20% -> 200s, 19% -> 90s,
    10% -> 10s, and below 10% -> 5s.  The high and low anchors remain
    configurable through the existing ordinary/critical interval settings;
    the intermediate points scale with the ordinary interval while retaining
    the same shape.
    """
    floor = max(1, int(config.get("critical_check_interval_seconds", 5)))
    ordinary = max(floor, int(config.get("ordinary_check_interval_seconds", 600)))
    if remaining is None:
        return floor
    if isinstance(remaining, bool):
        return floor
    try:
        value = float(remaining)
    except (TypeError, ValueError):
        return floor
    if not math.isfinite(value):
        return floor
    value = min(100.0, max(0.0, value))
    if value < 10.0:
        return floor

    # Keep the curve monotonic even if a user chooses unusual but valid
    # interval settings in the preferences window.
    at_ten = max(10, floor)
    at_nineteen = max(at_ten, int(round(ordinary * 0.15)))
    at_boundary = max(at_nineteen, int(round(ordinary / 3)))
    at_ninety = max(at_boundary, int(round(ordinary * 5 / 6)))
    boundary = min(89.0, max(11.0, float(config.get("critical_boundary_percent", 20))))
    just_below_boundary = max(10.0, boundary - 1.0)

    if value < just_below_boundary:
        return _interpolate_interval(value, 10.0, just_below_boundary, at_ten, at_nineteen)
    if value < boundary:
        return _interpolate_interval(value, just_below_boundary, boundary, at_nineteen, at_boundary)
    if value < 90.0:
        return _interpolate_interval(value, boundary, 90.0, at_boundary, at_ninety)
    return _interpolate_interval(value, 90.0, 100.0, at_ninety, ordinary)


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
    raw_value = dict(value or {})
    defaults: dict[str, Any] = {
        "primary_warning_percent": 3.0,
        "secondary_warning_percent": 1.0,
        "post_reset_delay_seconds": 60,
        "fallback_poll_seconds": 0,
        "ordinary_check_interval_seconds": 600,
        "critical_check_interval_seconds": 5,
        "critical_boundary_percent": 20,
        "quota_read_timeout_seconds": 5,
        # The managed desktop app-server's rate-limit RPC is materially
        # slower than ordinary control calls on this host. Keep the generic
        # action budget at 5s, but give the isolated reader a bounded budget
        # large enough to accept a real current quota response.
        "reader_quota_read_timeout_seconds": 30,
        "scheduled_refresh_read_timeout_seconds": 30,
        "health_check_interval_seconds": 5,
        "heartbeat_interval_seconds": 5,
        "watchdog_restart_window_seconds": 600,
        "watchdog_max_restarts": 3,
        "watchdog_restart_cooldown_seconds": 600,
        "force_stop_active_turns": True,
        "resume_paused_turns": True,
        "auto_consume_reset_credit": False,
        "notify_desktop": True,
        "scheduled_refresh_thread_id": "",
        "dry_run": False,
        "scheduled_refresh_enabled": True,
        "scheduled_refresh_delivery": "local_app_server_bridge",
        # launchd starts the service without the repository as its cwd (often
        # "/").  Use the guard's own directory unless the user configured a
        # dedicated read-only workspace explicitly.
        "scheduled_refresh_cwd": str(pathlib.Path(__file__).resolve().parent),
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
        "health_file": None,
        "watchdog_state_file": None,
        "reader_state_file": str(SHARED_SUPPORT_DIR / "reader-state.json"),
        "reader_health_file": str(SHARED_SUPPORT_DIR / "reader-health.json"),
        "actions_health_file": str(SHARED_SUPPORT_DIR / "actions-health.json"),
        "quota_result_file": str(SHARED_SUPPORT_DIR / "quota-results.jsonl"),
    }
    defaults.update(raw_value)
    defaults["scheduled_refresh_enabled"] = True
    delivery_value = value.get("scheduled_refresh_delivery")
    if delivery_value is None and value.get("scheduled_refresh_thread_id"):
        # Preserve the pre-bridge meaning of an explicitly configured target
        # thread.  New configurations without a target use the guard-owned
        # local bridge automatically.
        delivery_value = "local_app_server"
    delivery = str(delivery_value or defaults["scheduled_refresh_delivery"])
    if delivery not in {"local_app_server", "local_app_server_bridge", "codex_automation"}:
        raise ValueError(
            "scheduled_refresh_delivery 必须是 local_app_server、local_app_server_bridge 或 codex_automation"
        )
    # Older installations used a Codex-native automation as the sender.  The
    # standalone guard cannot invoke that MCP surface, so transparently move
    # the delivery responsibility to a persistent local app-server thread.
    if delivery == "codex_automation":
        defaults["scheduled_refresh_delivery_migrated_from"] = delivery
        delivery = "local_app_server_bridge"
    defaults["scheduled_refresh_delivery"] = delivery
    defaults["force_stop_active_turns"] = True
    for key in ("primary_warning_percent", "secondary_warning_percent"):
        number = float(defaults[key])
        if not 0 <= number <= 100:
            raise ValueError(f"{key} must be between 0 and 100")
        defaults[key] = number
    for key in ("post_reset_delay_seconds", "fallback_poll_seconds"):
        defaults[key] = max(0, int(defaults[key]))
    if "ordinary_check_interval_seconds" not in raw_value:
        legacy_fallback = int(defaults.get("fallback_poll_seconds") or 0)
        defaults["ordinary_check_interval_seconds"] = (
            max(60, min(3600, legacy_fallback)) if legacy_fallback > 0 else 600
        )
    interval_limits = {
        "ordinary_check_interval_seconds": (60, 3600),
        "critical_check_interval_seconds": (1, 60),
        "quota_read_timeout_seconds": (1, 30),
        "reader_quota_read_timeout_seconds": (5, 30),
        "scheduled_refresh_read_timeout_seconds": (5, 30),
        "health_check_interval_seconds": (1, 30),
        "heartbeat_interval_seconds": (1, 10),
        "watchdog_restart_window_seconds": (60, 3600),
        "watchdog_max_restarts": (1, 10),
        "watchdog_restart_cooldown_seconds": (60, 3600),
    }
    for key, (lower, upper) in interval_limits.items():
        try:
            number = int(defaults[key])
        except (TypeError, ValueError) as error:
            raise ValueError(f"{key} 必须是整数") from error
        if not lower <= number <= upper:
            raise ValueError(f"{key} 必须在 {lower} 到 {upper} 之间")
        defaults[key] = number
    try:
        boundary = float(defaults["critical_boundary_percent"])
    except (TypeError, ValueError) as error:
        raise ValueError("critical_boundary_percent 必须是数字") from error
    if not math.isfinite(boundary) or not 1 <= boundary <= 100:
        raise ValueError("critical_boundary_percent 必须在 1 到 100 之间")
    defaults["critical_boundary_percent"] = boundary
    if defaults["critical_check_interval_seconds"] > defaults["ordinary_check_interval_seconds"]:
        raise ValueError("critical_check_interval_seconds 不能大于 ordinary_check_interval_seconds")
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
        defaults["scheduled_refresh_hours"] = [
            (hour + shift) % 24 for hour in (7, 12, 17, 22)
        ]
    hours = defaults["scheduled_refresh_hours"]
    if isinstance(hours, str):
        hours = [part.strip() for part in hours.split(",") if part.strip()]
    try:
        parsed_hours = list(dict.fromkeys(int(hour) for hour in hours))
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
    state_path = pathlib.Path(str(defaults["state_file"])).expanduser()
    defaults["reader_state_file"] = str(
        pathlib.Path(str(defaults.get("reader_state_file") or state_path.with_name("reader-state.json"))).expanduser()
    )
    reader_health = defaults.get("reader_health_file")
    if "reader_health_file" not in raw_value and defaults.get("health_file"):
        reader_health = defaults["health_file"]
    defaults["reader_health_file"] = str(
        pathlib.Path(str(reader_health or state_path.with_name("reader-health.json"))).expanduser()
    )
    defaults["actions_health_file"] = str(
        pathlib.Path(str(defaults.get("actions_health_file") or state_path.with_name("actions-health.json"))).expanduser()
    )
    defaults["quota_result_file"] = str(
        pathlib.Path(str(defaults.get("quota_result_file") or state_path.with_name("quota-results.jsonl"))).expanduser()
    )
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


class AppServerMethodError(AppServerError):
    """A JSON-RPC method returned an application-level error."""

    def __init__(self, method: str, error: Any):
        self.method = method
        self.error = error if isinstance(error, dict) else {"message": str(error)}
        self.code = self.error.get("code")
        self.message = str(self.error.get("message") or self.error)
        super().__init__(f"{method} 失败: {self.error}")

    @property
    def is_active_writer(self) -> bool:
        return self.code == -32600 and "active writer" in self.message.lower()


class ResultPublicationError(OSError):
    """The quota read completed but its immutable result could not be published."""


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
        self._turn_completions: dict[tuple[str, str], dict[str, Any]] = {}
        self.dynamic_tool_handler: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None
        self._connection_error: Exception | None = None
        self._closing = False
        self._connection_generation = 0

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AppServerError("Codex app-server 请求总期限已到")
        return remaining

    def start(self, deadline: float | None = None) -> None:
        if self.socket is not None:
            return
        deadline = deadline or time.monotonic() + self.request_timeout
        self._ensure_managed_daemon(deadline)
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                remaining = self._remaining(deadline)
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(remaining)
                sock.connect(self.socket_path)
                self._websocket_handshake(sock, deadline)
                self.socket = sock
                self._closing = False
                self._connection_error = None
                self._connection_generation += 1
                generation = self._connection_generation
                self._reader_thread = threading.Thread(
                    target=self._read_loop,
                    args=(sock, generation),
                    daemon=True,
                )
                self._reader_thread.start()
                self.call(
                    "initialize",
                    {
                        "clientInfo": {
                            "name": "codex-quota-guard",
                            "title": "Codex Quota Guard",
                            "version": "1.1.0",
                        },
                        # Dynamic tools are an experimental app-server
                        # capability.  Without this opt-in, thread/start
                        # rejects the tool list before a refresh turn exists.
                        "capabilities": {"experimentalApi": True},
                    },
                    deadline=deadline,
                    _skip_start=True,
                )
                self._send_notification("initialized", {}, deadline=deadline)
                return
            except (OSError, AppServerError, subprocess.SubprocessError) as error:
                last_error = error
                self.close()
                if attempt == 0:
                    self._start_managed_daemon(self._remaining(deadline))
                    time.sleep(min(0.5, self._remaining(deadline)))
        raise AppServerError(f"无法连接 Codex managed app-server: {last_error}")

    def _ensure_managed_daemon(self, deadline: float | None = None) -> None:
        if not pathlib.Path(self.socket_path).exists():
            self._start_managed_daemon(
                self._remaining(deadline) if deadline is not None else None
            )

    def _start_managed_daemon(self, timeout: float | None = None) -> None:
        subprocess.run(
            [self.executable, "app-server", "daemon", "start"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout or self.request_timeout,
        )

    def _websocket_handshake(self, sock: socket.socket, deadline: float) -> None:
        sock.settimeout(self._remaining(deadline))
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            "GET / HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        sock.settimeout(self._remaining(deadline))
        sock.sendall(request)
        response = b""
        while b"\r\n\r\n" not in response:
            sock.settimeout(self._remaining(deadline))
            chunk = sock.recv(4096)
            if not chunk:
                raise AppServerError("Codex app-server 在 WebSocket 握手时断开")
            response += chunk
        if not response.startswith(b"HTTP/1.1 101"):
            raise AppServerError(f"Codex app-server WebSocket 握手失败: {response[:200]!r}")
        sock.settimeout(None)

    def _read_exact(self, sock: socket.socket, size: int) -> bytes:
        if sock is None:
            raise AppServerError("Codex app-server socket 未连接")
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise AppServerError("Codex app-server socket 已断开")
            data += chunk
        return data

    def _read_frame(self, sock: socket.socket) -> tuple[int, bytes]:
        header = self._read_exact(sock, 2)
        first, second = header
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exact(sock, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(sock, 8))[0]
        mask = self._read_exact(sock, 4) if second & 0x80 else None
        payload = self._read_exact(sock, length)
        if mask:
            payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        return first & 0x0F, payload

    def _send_frame(
        self,
        payload: bytes,
        opcode: int = 1,
        sock: socket.socket | None = None,
        deadline: float | None = None,
    ) -> None:
        sock = sock or self.socket
        if sock is None:
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
            if deadline is not None:
                sock.settimeout(self._remaining(deadline))
            sock.sendall(header + mask + masked)
            if deadline is not None:
                sock.settimeout(None)

    def _read_loop(self, sock: socket.socket, generation: int) -> None:
        try:
            while not self._closing:
                opcode, payload = self._read_frame(sock)
                if opcode == 8:
                    raise AppServerError("Codex app-server WebSocket 已关闭")
                if opcode == 9:
                    self._send_frame(payload, opcode=10, sock=sock)
                    continue
                if opcode not in (1, 2):
                    continue
                message = json.loads(payload)
                # A socket from a previous connection may still deliver a
                # late frame after close/reconnect.  It must never satisfy a
                # request belonging to the new connection generation.
                if self.socket is not sock or self._connection_generation != generation:
                    continue
                with self._condition:
                    if message.get("id") is not None and message.get("method"):
                        handler = self.dynamic_tool_handler
                        threading.Thread(
                            target=self._handle_server_request,
                            args=(message, sock, generation, handler),
                            daemon=True,
                        ).start()
                    elif isinstance(message.get("id"), int):
                        self._responses[message["id"]] = message
                        self._condition.notify_all()
                    elif message.get("method"):
                        if message.get("method") == "turn/completed":
                            params = message.get("params") or {}
                            turn = params.get("turn") or {}
                            thread_id = params.get("threadId")
                            turn_id = turn.get("id") if isinstance(turn, dict) else None
                            if thread_id and turn_id:
                                self._turn_completions[(str(thread_id), str(turn_id))] = turn
                        self._notifications.put(message)
                        self._condition.notify_all()
        except Exception as error:
            if not self._closing and self.socket is sock and self._connection_generation == generation:
                with self._condition:
                    self._connection_error = error
                    self._condition.notify_all()

    def _handle_server_request(
        self,
        message: dict[str, Any],
        sock: socket.socket,
        generation: int,
        handler: Callable[[str, dict[str, Any]], dict[str, Any]] | None,
    ) -> None:
        """Answer app-server requests without blocking the WebSocket reader.

        Dynamic tools are server-to-client JSON-RPC requests.  The handler may
        need to issue a nested app-server request (for example the quota read),
        so it must run on a separate thread rather than inside ``_read_loop``.
        """
        request_id = message.get("id")
        method = str(message.get("method") or "")
        params = message.get("params")
        params = params if isinstance(params, dict) else {}
        try:
            if handler is None:
                raise AppServerError("当前客户端未注册动态工具处理器")
            result = handler(method, params)
            response: dict[str, Any] = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result,
            }
        except Exception as error:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": str(error),
                },
            }
        try:
            if self.socket is sock and self._connection_generation == generation:
                self._send_frame(
                    json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode(),
                    sock=sock,
                )
        except (OSError, AppServerError):
            pass

    def _send_notification(
        self,
        method: str,
        params: dict[str, Any],
        deadline: float | None = None,
    ) -> None:
        self._write({"jsonrpc": "2.0", "method": method, "params": params}, deadline=deadline)

    def _write(self, value: dict[str, Any], deadline: float | None = None) -> None:
        self._send_frame(
            json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(),
            deadline=deadline,
        )

    def call(
        self,
        method: str,
        params: dict[str, Any],
        timeout: float | None = None,
        deadline: float | None = None,
        _skip_start: bool = False,
    ) -> dict[str, Any]:
        deadline = deadline or time.monotonic() + (timeout or self.request_timeout)
        if not _skip_start:
            self.start_if_needed(deadline=deadline)
        with self._condition:
            request_id = self._next_id
            self._next_id += 1
            self._write(
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
                deadline=deadline,
            )
            while request_id not in self._responses:
                remaining = self._remaining(deadline)
                if self._connection_error:
                    raise AppServerError(f"调用 {method} 时连接断开: {self._connection_error}")
                self._condition.wait(timeout=remaining)
            response = self._responses.pop(request_id)
        if "error" in response:
            raise AppServerMethodError(method, response["error"])
        return response.get("result") or {}

    def start_if_needed(self, deadline: float | None = None) -> None:
        if self.socket is None:
            self.start(deadline=deadline)

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

    def wait_for_turn_completion(
        self,
        thread_id: str,
        turn_id: str,
        timeout: float,
    ) -> dict[str, Any] | None:
        """Return a completion notification without requiring list_turns support."""
        deadline = time.monotonic() + max(0.0, float(timeout))
        key = (str(thread_id), str(turn_id))
        with self._condition:
            while True:
                completed = self._turn_completions.pop(key, None)
                if completed is not None:
                    return completed
                if self._connection_error:
                    raise AppServerError(
                        f"等待 turn/{turn_id} 完成时连接断开: {self._connection_error}"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._condition.wait(timeout=remaining)

    def close(self) -> None:
        self._closing = True
        socket_to_close = self.socket
        self.socket = None
        self._connection_generation += 1
        with self._condition:
            self._responses.clear()
            self._turn_completions.clear()
            self._condition.notify_all()
        if socket_to_close is not None:
            try:
                socket_to_close.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            socket_to_close.close()


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
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.client = client
        self.config = config
        self.store = store
        self.now = now
        self.sleep = sleep
        self.monotonic = monotonic
        self.dynamic_tool_client: AppServerClient | None = None
        self.dynamic_tool_limits: Limits | None = None
        self.dynamic_tool_limits_at: float | None = None
        self.process_started_at = time.time()
        self.health_file = pathlib.Path(
            str(config.get("health_file") or self.store.state_file.with_name("health.json"))
        ).expanduser()
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
                int(config.get("scheduled_refresh_hour_shift", 0)),
            )
        self.store.state["next_fixed_refresh_at"] = self.next_fixed_refresh_at
        self.store.state["fixed_refresh_schedule_signature"] = signature
        self.store.state["config_revision"] = config.get("config_revision")
        self.store.state["config_loaded_at"] = int(time.time())
        persisted_quota_deadline = self.store.state.get("next_quota_check_at")
        if persisted_quota_deadline is None:
            self.next_quota_check_at = self.now()
        else:
            self.next_quota_check_at = max(self.now(), float(persisted_quota_deadline))
        self.store.state["next_quota_check_at"] = self.next_quota_check_at
        pending_events = self.store.state.get("pending_scheduled_refresh_events")
        if not isinstance(pending_events, list):
            pending_events = []
            self.store.state["pending_scheduled_refresh_events"] = pending_events
        legacy_pending = self.store.state.get("pending_scheduled_refresh")
        if isinstance(legacy_pending, dict) and legacy_pending.get("status") not in {"confirmed", "failed"}:
            legacy_event_id = legacy_pending.get("event_id")
            if not legacy_event_id:
                legacy_event_id = f"{config.get('scheduled_refresh_thread_id') or 'notification'}:{int(legacy_pending.get('scheduled_at') or self.now())}"
                legacy_pending["event_id"] = legacy_event_id
            if not any(item.get("event_id") == legacy_event_id for item in pending_events if isinstance(item, dict)):
                pending_events.append(legacy_pending)
        self._write_health("starting")
        self.store.save()

    def read_limits(
        self,
        client: Any | None = None,
        deadline: float | None = None,
    ) -> Limits:
        started_at = self.now()
        request_id = str(uuid.uuid4())
        quota_client = client if client is not None else self.client
        request_deadline = deadline or (
            self.monotonic() + float(self.config["quota_read_timeout_seconds"])
        )
        timeout_budget = max(0.0, request_deadline - self.monotonic())
        self.store.state["quota_read_started_at"] = started_at
        self.store.state["request_in_flight"] = True
        self.store.state["request_id"] = request_id
        self.store.state["request_deadline_at"] = started_at + timeout_budget
        self._write_health(
            "reading",
            request_id=request_id,
            request_in_flight=True,
            request_deadline_at=self.store.state["request_deadline_at"],
        )
        try:
            try:
                payload = quota_client.call(
                    "account/rateLimits/read", {}, deadline=request_deadline
                )
            except TypeError as error:
                if "deadline" not in str(error):
                    raise
                payload = quota_client.call("account/rateLimits/read", {})
            limits = parse_limits(payload)
            finished_at = self.now()
            self.store.state["quota_read_finished_at"] = finished_at
            self.store.state["request_in_flight"] = False
            self.store.state["request_finished_at"] = finished_at
            self._write_health(
                "read_complete",
                request_id=request_id,
                request_in_flight=False,
                request_finished_at=finished_at,
            )
            return limits
        except Exception as error:
            finished_at = self.now()
            self.store.state["quota_read_finished_at"] = finished_at
            self.store.state["request_in_flight"] = False
            self.store.state["request_finished_at"] = finished_at
            self._write_health(
                "read_failed",
                request_id=request_id,
                request_in_flight=False,
                request_finished_at=finished_at,
                error=str(error),
            )
            raise

    def _write_health(self, status: str, **details: Any) -> None:
        """Publish worker health separately from action state.

        The supervisor reads this file while the worker is blocked in a socket
        request.  It is deliberately updated only at event-loop boundaries;
        a stale heartbeat therefore represents a stuck worker, not merely an
        idle one waiting for a notification.
        """
        if not self.store.persist:
            return
        payload = {
            "pid": os.getpid(),
            "instance_id": self.config.get("_process_instance_id"),
            "process_started_at": self.process_started_at,
            "heartbeat_at": time.time(),
            "heartbeat_interval_seconds": self.config.get("heartbeat_interval_seconds", 5),
            "status": status,
            "config_revision": self.config.get("config_revision"),
            "quota_sequence": self.store.state.get("quota_sequence", 0),
            "primary_last_success_at": self.store.state.get("last_primary_success_at"),
            "secondary_last_success_at": self.store.state.get("last_secondary_success_at"),
            "primary_data_status": effective_window_status(self.store.state, "primary", self.now()),
            "secondary_data_status": effective_window_status(self.store.state, "secondary", self.now()),
            "primary_data_expires_at": self.store.state.get("primary_data_expires_at"),
            "secondary_data_expires_at": self.store.state.get("secondary_data_expires_at"),
            "primary_remaining": self.store.state.get("last_primary_remaining"),
            "secondary_remaining": self.store.state.get("last_secondary_remaining"),
            "next_quota_check_at": self.store.state.get("next_quota_check_at"),
            "request_started_at": self.store.state.get("quota_read_started_at"),
            "request_finished_at": self.store.state.get("quota_read_finished_at"),
            "request_in_flight": self.store.state.get("request_in_flight", False),
            "request_id": self.store.state.get("request_id"),
            "request_deadline_at": self.store.state.get("request_deadline_at"),
        }
        payload.update(details)
        try:
            save_json(self.health_file, payload)
        except OSError:
            # A health write must not take down the quota worker. The next
            # boundary will retry and the supervisor will report the failure.
            pass

    def _quota_check_interval(self, limits: Limits) -> tuple[int, str]:
        values = (limits.primary_remaining, limits.secondary_remaining)
        # Both windows are required for a normal gradient decision. If one is
        # missing or invalid, use the safety floor instead of treating the
        # other window as proof that the account is healthy.
        if any(value is None for value in values):
            return quota_gradient_interval(None, self.config), "critical"

        remaining = min(float(values[0]), float(values[1]))
        interval = quota_gradient_interval(remaining, self.config)
        if remaining < 10.0:
            return interval, "critical"
        return interval, "gradient"

    def _record_window_result(
        self,
        name: str,
        remaining: float | None,
        now: float,
    ) -> None:
        label = "primary" if name == "primary" else "secondary"
        status_key = f"{label}_data_status"
        error_key = f"{label}_last_error"
        success_key = f"last_{label}_success_at"
        failure_flags = self.store.state.setdefault(
            "quota_failure_notified", {"primary": False, "secondary": False}
        )
        previous_status = self.store.state.get(status_key, "unknown")
        if remaining is None:
            self.store.state[status_key] = "invalid"
            self.store.state[error_key] = "接口未返回有效的 0–100 用量百分比"
            self.store.state[f"{label}_data_expires_at"] = now
            if not failure_flags.get(label, False):
                failure_flags[label] = True
                self.store.event(
                    "quota_window_invalid",
                    window=label,
                    reason=self.store.state[error_key],
                )
                self.notify(
                    "Codex 额度检测失败",
                    f"{label} 额度接口返回缺失或非法数据，已停止把旧值视为当前正常值",
                )
            return
        self.store.state[status_key] = "current"
        self.store.state[error_key] = None
        self.store.state[success_key] = now
        if previous_status in {"invalid", "expired"} and failure_flags.get(label, False):
            self.store.event("quota_window_recovered", window=label)
        failure_flags[label] = False

    def _record_limits(self, limits: Limits) -> None:
        now = self.now()
        self.store.state["quota_sequence"] = int(self.store.state.get("quota_sequence") or 0) + 1
        self._record_window_result("primary", limits.primary_remaining, now)
        self._record_window_result("secondary", limits.secondary_remaining, now)
        if limits.primary_remaining is not None:
            self.store.state["last_primary_remaining"] = limits.primary_remaining
        if limits.secondary_remaining is not None:
            self.store.state["last_secondary_remaining"] = limits.secondary_remaining
        interval, mode = self._quota_check_interval(limits)
        self.store.state["quota_check_interval_seconds"] = interval
        self.store.state["quota_check_mode"] = mode
        self.next_quota_check_at = now + interval
        self.store.state["next_quota_check_at"] = self.next_quota_check_at
        expiry = now + interval + int(self.config["quota_read_timeout_seconds"])
        for label in ("primary", "secondary"):
            if self.store.state.get(f"{label}_data_status") == "current":
                self.store.state[f"{label}_data_expires_at"] = expiry
        self._write_health("healthy" if all(
            self.store.state.get(f"{label}_data_status") == "current"
            for label in ("primary", "secondary")
        ) else "quota_degraded")

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

    def read_thread(self, thread_id: str, include_turns: bool = True) -> dict[str, Any]:
        params: dict[str, Any] = {"threadId": thread_id}
        if include_turns:
            params["includeTurns"] = True
        try:
            result = self.client.call("thread/read", params)
        except AppServerMethodError as error:
            # Current managed desktop app-server builds expose thread state
            # but may not implement the paginated list_turns backend.  The
            # caller can still use turn/completed notifications for delivery
            # confirmation; task-control callers must treat missing turns as
            # unverified and therefore must not interrupt or resume blindly.
            if include_turns and "list_turns is not supported" in error.message.lower():
                result = self.client.call("thread/read", {"threadId": thread_id})
                thread = result.get("thread") or {}
                if isinstance(thread, dict):
                    thread["_turns_unavailable"] = True
                return thread
            raise
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

    def _goal_control_capable(self, thread: dict[str, Any]) -> bool:
        """Return whether this client can make native goal requests.

        Production uses ``AppServerClient``.  The explicit test-double hook
        keeps unit tests honest without forcing ordinary fake clients to
        implement an experimental protocol method they are not testing.
        """
        return isinstance(self.client, AppServerClient) or bool(
            getattr(self.client, "supports_goal_control", False)
        )

    def _read_goal(self, thread_id: str, thread: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
        """Read native goal state, returning (goal, protocol_supported)."""
        if not self._goal_control_capable(thread) and not self._goal_evidence(thread):
            return None, False
        try:
            result = self.client.call("thread/goal/get", {"threadId": thread_id})
        except AppServerMethodError as error:
            if error.code == -32601 or "method not found" in error.message.lower():
                return None, False
            raise
        goal = result.get("goal") if isinstance(result, dict) else None
        return (goal if isinstance(goal, dict) else None), True

    @staticmethod
    def _goal_status(goal: dict[str, Any] | None) -> str | None:
        if not isinstance(goal, dict):
            return None
        value = goal.get("status")
        return str(value).lower() if value is not None else None

    def _verify_goal_status(self, thread_id: str, expected: str) -> bool:
        deadline = self.now() + 10
        while self.now() <= deadline:
            goal, supported = self._read_goal(thread_id, {})
            if supported and self._goal_status(goal) == expected:
                return True
            self.sleep(0.25)
        return False

    def force_stop_active_threads(self, reset_at: int) -> int:
        stopped_count = 0
        excluded = set(self.config.get("exclude_thread_ids") or [])
        for listed in self.list_threads():
            thread_id = listed.get("id")
            if not thread_id or thread_id in excluded or not is_candidate_thread(listed):
                continue
            turn_id: str | None = None
            try:
                thread, turn_id = self._active_turn(listed)
                goal, goal_api_supported = self._read_goal(thread_id, thread)
                goal_status = self._goal_status(goal)
                goal_is_active = goal_status == "active" or (
                    not goal_api_supported and self._goal_evidence(thread)
                )
                if goal_status == "paused":
                    # A goal paused by the user or another controller is not
                    # ours to resume later.  Do not interrupt a turn under it
                    # and accidentally claim ownership of the pause episode.
                    self.store.event("goal_already_paused_not_owned", thread_id=thread_id)
                    continue
                if goal_is_active and not goal_api_supported:
                    self.store.event(
                        "goal_control_unavailable",
                        thread_id=thread_id,
                        title=thread.get("name") if isinstance(thread, dict) else None,
                    )
                    self._notify_once(
                        f"goal-control-unavailable:{reset_at}:{thread_id}",
                        "Codex 额度保护受限",
                        f"任务 {thread_id} 检测到 Goal，但当前接口没有可验证的原生 Goal 暂停控制",
                    )
                    continue
                if not turn_id:
                    if goal_is_active:
                        self.store.event(
                            "active_goal_without_verified_turn",
                            thread_id=thread_id,
                            title=thread.get("name") if isinstance(thread, dict) else None,
                        )
                    else:
                        self.store.event(
                            "active_thread_without_verified_turn", thread_id=thread_id,
                            title=thread.get("name") if isinstance(thread, dict) else None,
                        )
                    if goal_status == "paused":
                        self.store.event("goal_already_paused_not_owned", thread_id=thread_id)
                    if not goal_is_active:
                        continue
                    key = self._stop_attempt_key(reset_at, thread_id, "goal")
                    attempt = (self.store.state.setdefault("stop_attempts", {})).get(key) or {}
                    if attempt.get("status") == "verified" or int(attempt.get("next_retry_at") or 0) > int(self.now()):
                        continue
                    if self._dry_run():
                        self.store.event("would_pause_goal", thread_id=thread_id)
                        continue
                    self.client.call(
                        "thread/goal/set", {"threadId": thread_id, "status": "paused"}
                    )
                    if not self._verify_goal_status(thread_id, "paused"):
                        self._record_stop_failure(key, "设置 Goal 暂停后无法确认状态")
                        self.store.save()
                        self.store.event("goal_pause_verification_failed", thread_id=thread_id)
                        continue
                    self.store.state.setdefault("stop_attempts", {})[key] = {
                        "status": "verified",
                        "attempts": int(attempt.get("attempts") or 0) + 1,
                        "last_attempt_at": int(self.now()),
                    }
                    self.store.state["paused_threads"][thread_id] = {
                        "thread_id": thread_id,
                        "turn_id": None,
                        "title": thread.get("name") or thread.get("preview") or thread_id,
                        "cwd": thread.get("cwd"),
                        "reset_at": reset_at,
                        "goal_paused_verified": True,
                        "goal_objective": goal.get("objective") if isinstance(goal, dict) else None,
                    }
                    self.store.save()
                    self.store.event(
                        "goal_pause_verified", thread_id=thread_id, reset_at=reset_at
                    )
                    stopped_count += 1
                    self.notify("Codex 额度保护", f"已暂停 Goal：{thread.get('name') or thread_id}")
                    self.store.event(
                        "forced_stop_verified", thread_id=thread_id, turn_id=None, reset_at=reset_at
                    )
                    continue
                key = self._stop_attempt_key(reset_at, thread_id, turn_id)
                attempt = (self.store.state.setdefault("stop_attempts", {})).get(key) or {}
                if attempt.get("status") == "verified":
                    continue
                if int(attempt.get("next_retry_at") or 0) > int(self.now()):
                    continue
                if self._dry_run():
                    if goal_is_active:
                        self.store.event("would_pause_goal", thread_id=thread_id)
                    self.store.event("would_interrupt", thread_id=thread_id, turn_id=turn_id)
                    continue
                if goal_is_active:
                    self.client.call(
                        "thread/goal/set", {"threadId": thread_id, "status": "paused"}
                    )
                    if not self._verify_goal_status(thread_id, "paused"):
                        self._record_stop_failure(key, "设置 Goal 暂停后无法确认状态")
                        self.store.save()
                        self.store.event(
                            "goal_pause_verification_failed", thread_id=thread_id, turn_id=turn_id
                        )
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
                if goal_is_active and not self._verify_goal_status(thread_id, "paused"):
                    self._record_stop_failure(key, "中断后 Goal 状态未保持 paused")
                    self.store.save()
                    self.store.event(
                        "goal_pause_verification_failed", thread_id=thread_id, turn_id=turn_id
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
                    "goal_paused_verified": bool(goal_is_active),
                    "goal_objective": goal.get("objective") if isinstance(goal, dict) else None,
                }
                self.store.save()
                self.store.event(
                    "goal_pause_and_turn_stop_verified" if goal_is_active else "forced_stop_verified",
                    thread_id=thread_id, turn_id=turn_id, reset_at=reset_at,
                )
                stopped_count += 1
                self.notify(
                    "Codex 额度保护",
                    f"已暂停 Goal 并强制停止当前 turn：{thread.get('name') or thread_id}"
                    if goal_is_active else f"已强制停止：{thread.get('name') or thread_id}",
                )
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

    def _verify_started(
        self,
        thread_id: str,
        expected_turn_id: str | None = None,
        timeout: float = 10,
    ) -> tuple[bool, bool]:
        deadline = self.now() + max(0.1, float(timeout))
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
                if record.get("goal_paused_verified"):
                    goal, goal_api_supported = self._read_goal(thread_id, thread)
                    goal_status = self._goal_status(goal)
                    if not goal_api_supported:
                        self._resume_failure(record, "原生 Goal 恢复接口不可用")
                        continue
                    if goal_status == "active":
                        self.store.event("resume_skipped_goal_already_active", thread_id=thread_id)
                        self.store.state["paused_threads"].pop(thread_id, None)
                        continue
                    if goal_status in {"complete", "blocked", "usageLimited", "budgetLimited"} or goal is None:
                        self.store.event(
                            "resume_skipped_goal_terminal_or_missing",
                            thread_id=thread_id,
                            status=goal_status,
                        )
                        self.store.state["paused_threads"].pop(thread_id, None)
                        continue
                    if goal_status != "paused":
                        self._resume_failure(record, f"Goal 状态不明确：{goal_status or 'unknown'}")
                        continue
                    if self._dry_run():
                        self.store.event("would_resume_goal", thread_id=thread_id, reset_at=reset_at)
                        continue
                    self.client.call(
                        "thread/goal/set", {"threadId": thread_id, "status": "active"}
                    )
                    if not self._verify_goal_status(thread_id, "active"):
                        self._resume_failure(record, "恢复 Goal 后无法确认 active 状态")
                        self.store.event("goal_resume_verification_failed", thread_id=thread_id)
                        continue
                    turn_verified, _ = self._verify_started(thread_id, timeout=5)
                    self.store.state["paused_threads"].pop(thread_id, None)
                    record["resume_attempts"] = 0
                    record.pop("next_resume_retry_at", None)
                    self.store.save()
                    self.store.event(
                        "resume_verified_goal_native",
                        thread_id=thread_id,
                        reset_at=reset_at,
                        turn_active=turn_verified,
                    )
                    self.notify(
                        "Codex 额度保护",
                        (
                            f"额度已重置，已恢复 Goal 并确认自动续跑：{record.get('title') or thread_id}"
                            if turn_verified
                            else f"额度已重置，Goal 状态已恢复，但自动续跑尚未确认：{record.get('title') or thread_id}"
                        ),
                    )
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

    def handle_limits(self, limits: Limits, record: bool = True) -> None:
        if record:
            self._record_limits(limits)
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
        if limits.primary_remaining is not None:
            state["last_primary_remaining"] = limits.primary_remaining
        if limits.secondary_remaining is not None:
            state["last_secondary_remaining"] = limits.secondary_remaining
        self.store.save()

    def next_wait_seconds(self) -> float | None:
        deadlines: list[float] = []
        quota_deadline = self.store.state.get("next_quota_check_at")
        if quota_deadline is not None:
            deadlines.append(float(quota_deadline))
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
        if self.config.get("scheduled_refresh_enabled", True):
            deadlines.append(self.next_fixed_refresh_at)
        if not deadlines:
            return 3600.0
        return max(0.0, min(deadlines) - self.now())

    def close(self) -> None:
        if self.dynamic_tool_client is not None:
            self.dynamic_tool_client.close()
            self.dynamic_tool_client = None

    def _pending_refresh_message(self, pending: dict[str, Any]) -> str:
        primary = pending.get("primary_remaining")
        secondary = pending.get("secondary_remaining")
        primary_text = "未知" if primary is None else f"{float(primary):.1f}%"
        secondary_text = "未知" if secondary is None else f"{float(secondary):.1f}%"
        missed = int(pending.get("missed_count") or 0)
        prefix = f"已合并补发，错过 {missed} 个定时点；" if missed else ""
        return f"{prefix}已刷新：5 小时剩余 {primary_text}，总额度剩余 {secondary_text}"

    def _refresh_target(self, pending: dict[str, Any] | None = None) -> str:
        if pending:
            target = pending.get("target_thread_id")
            if target:
                return str(target)
        if self.config.get("scheduled_refresh_delivery") == "local_app_server_bridge":
            bridge_id = self.store.state.get("scheduled_refresh_bridge_thread_id")
            if bridge_id:
                return str(bridge_id)
        return str(self.config.get("scheduled_refresh_thread_id") or "")

    def _handle_refresh_dynamic_tool(
        self,
        method: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Expose the local quota read as a real tool inside the bridge turn."""
        if method != "item/tool/call":
            raise AppServerError(f"不支持的 app-server 请求：{method}")
        tool_name = str(params.get("tool") or "")
        if tool_name != "get_usage_limits":
            raise AppServerError(f"不支持的额度守护动态工具：{tool_name}")
        # A dynamic-tool request is delivered while the app-server is waiting
        # for this WebSocket response.  Some managed app-server builds do not
        # service a nested request on that same connection until the tool
        # returns, which would make the tool time out even though a normal
        # quota read works.  Use a short-lived second connection for the local
        # read in production; lightweight test doubles keep the original path.
        limits = self.dynamic_tool_limits
        if limits is None or self.dynamic_tool_limits_at is None:
            raise AppServerError("额度刷新专用会话尚未准备好有效额度快照")
        snapshot_age = self.monotonic() - self.dynamic_tool_limits_at
        if snapshot_age > 60:
            raise AppServerError("额度刷新专用会话的额度快照已过期")
        snapshot = {
            "primary_remaining_percent": limits.primary_remaining,
            "secondary_remaining_percent": limits.secondary_remaining,
            "primary_resets_at": limits.primary_resets_at,
            "secondary_resets_at": limits.secondary_resets_at,
            "reset_credit_count": limits.reset_credit_count,
        }
        return {
            "success": True,
            "contentItems": [{
                "type": "inputText",
                "text": json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
            }],
        }

    def _ensure_refresh_tool_client(self, timeout: float | None = None) -> AppServerClient:
        """Return a pre-warmed connection reserved for dynamic tool reads."""
        budget = max(
            5.0,
            float(
                timeout
                if timeout is not None
                else self.config.get("scheduled_refresh_read_timeout_seconds", 30)
            ),
        )
        candidate = self.dynamic_tool_client
        if candidate is not None and (
            candidate.socket is not None and candidate._connection_error is None
        ):
            return candidate
        if candidate is not None:
            candidate.close()
        if not isinstance(self.client, AppServerClient):
            raise AppServerError("当前额度刷新客户端不支持动态工具连接")
        candidate = AppServerClient(
            self.client.executable,
            self.client.socket_path,
            request_timeout=budget,
        )
        candidate.start(deadline=self.monotonic() + budget)
        self.dynamic_tool_client = candidate
        return candidate

    def _prepare_refresh_tool_snapshot(self) -> Limits:
        """Read a fresh snapshot before starting the model turn.

        The dynamic-tool callback has a short server-side response window.
        Fetching the quota before ``turn/start`` lets the callback answer from
        a verified current snapshot even when the upstream usage endpoint is
        slow.  This remains a local app-server read and does not create a
        model turn.
        """
        timeout = max(
            5.0,
            float(self.config.get("scheduled_refresh_read_timeout_seconds", 30)),
        )
        if isinstance(self.client, AppServerClient):
            tool_client = self._ensure_refresh_tool_client(timeout)
            limits = self.read_limits(
                client=tool_client,
                deadline=self.monotonic() + timeout,
            )
        else:
            limits = self.read_limits()
        if (
            limits.primary_remaining is None
            or limits.secondary_remaining is None
            or limits.primary_resets_at is None
        ):
            raise AppServerError("额度刷新接口没有返回完整的当前快照")
        self.dynamic_tool_limits = limits
        self.dynamic_tool_limits_at = self.monotonic()
        return limits

    @staticmethod
    def _refresh_dynamic_tools() -> list[dict[str, Any]]:
        return [{
            "type": "function",
            "name": "get_usage_limits",
            "description": (
                "读取当前 Codex 五小时和总额度窗口。必须使用返回的百分比和 Unix "
                "重置时间回答，不执行 bank reset、任务控制或设置修改。"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        }]

    @staticmethod
    def _thread_id_from_start_result(result: dict[str, Any]) -> str | None:
        thread = result.get("thread") if isinstance(result, dict) else None
        if isinstance(thread, dict) and thread.get("id"):
            return str(thread["id"])
        for key in ("threadId", "thread_id", "id"):
            value = result.get(key) if isinstance(result, dict) else None
            if value:
                return str(value)
        return None

    def _ensure_refresh_bridge_thread(self, pending: dict[str, Any]) -> str | None:
        """Create or reuse the guard-owned desktop Codex conversation.

        The previous ``codex_automation`` mode only recorded a delegation; it
        never submitted a message from this process.  This bridge owns one
        persistent local app-server thread instead.  Keeping the client open
        for the lifetime of the action process lets the submitted turn finish
        and be queried before the scheduled event is acknowledged.
        """
        bridge_id = self.store.state.get("scheduled_refresh_bridge_thread_id")
        upgrade_required = bool(
            bridge_id
            and self.store.state.get("scheduled_refresh_bridge_capability_version")
            != REFRESH_BRIDGE_CAPABILITY_VERSION
        )
        if bridge_id:
            try:
                thread = self.read_thread(str(bridge_id))
            except AppServerMethodError as error:
                message = error.message.lower()
                if "not found" not in message and "not loaded" not in message:
                    self._queue_refresh_retry(pending, "refresh_bridge_thread_read_failed", str(error))
                    return None
                self.store.state["scheduled_refresh_bridge_thread_id"] = None
                self.store.state["scheduled_refresh_bridge_created_at"] = None
                self.store.state["scheduled_refresh_bridge_capability_version"] = None
                self.store.event(
                    "refresh_bridge_thread_unavailable",
                    thread_id=bridge_id,
                    error=str(error),
                )
                self.store.save()
            except AppServerError as error:
                self._queue_refresh_retry(pending, "refresh_bridge_thread_read_failed", str(error))
                return None
            else:
                status = (thread.get("status") or {}).get("type")
                if status in ACTIVE_THREAD_STATUSES:
                    pending["next_retry_at"] = int(self.now()) + 60
                    self.store.event(
                        "refresh_bridge_thread_busy",
                        thread_id=bridge_id,
                        status=status,
                        retry_at=pending["next_retry_at"],
                    )
                    self.store.save()
                    return None
                if status in {"idle", "notLoaded"}:
                    if not upgrade_required:
                        return str(bridge_id)
                    old_bridge_id = bridge_id
                    self.store.state["scheduled_refresh_bridge_thread_id"] = None
                    self.store.state["scheduled_refresh_bridge_created_at"] = None
                    self.store.state["scheduled_refresh_bridge_capability_version"] = None
                    self.store.event(
                        "refresh_bridge_thread_upgrade_required",
                        thread_id=old_bridge_id,
                        capability_version=REFRESH_BRIDGE_CAPABILITY_VERSION,
                    )
                    self.store.save()
                    bridge_id = None
                else:
                    self.store.state["scheduled_refresh_bridge_thread_id"] = None
                    self.store.state["scheduled_refresh_bridge_created_at"] = None
                    self.store.state["scheduled_refresh_bridge_capability_version"] = None
                    self.store.event(
                        "refresh_bridge_thread_replaced",
                        thread_id=bridge_id,
                        status=status,
                    )
                    self.store.save()

        if self._dry_run():
            self.store.event("would_create_refresh_bridge_thread")
            return "dry-run-refresh-bridge"

        cwd = pathlib.Path(
            str(
                self.config.get("scheduled_refresh_cwd")
                or pathlib.Path(__file__).resolve().parent
            )
        ).expanduser()
        params = {
            "cwd": str(cwd),
            "ephemeral": False,
            "sandbox": "read-only",
            "approvalPolicy": "never",
            "dynamicTools": self._refresh_dynamic_tools(),
        }
        try:
            result = self.client.call("thread/start", params)
        except (AppServerError, OSError) as error:
            self._queue_refresh_retry(pending, "refresh_bridge_thread_create_failed", str(error))
            self._notify_once(
                "refresh-bridge-create-failed",
                "Codex 定时会话不可用",
                f"无法创建额度刷新专用会话，将按退避重试：{error}",
            )
            return None
        thread_id = self._thread_id_from_start_result(result)
        if not thread_id:
            error = "thread/start 返回中没有 thread id"
            self._queue_refresh_retry(pending, "refresh_bridge_thread_create_failed", error)
            self._notify_once(
                "refresh-bridge-create-failed",
                "Codex 定时会话不可用",
                error,
            )
            return None
        self.store.state["scheduled_refresh_bridge_thread_id"] = thread_id
        self.store.state["scheduled_refresh_bridge_created_at"] = int(self.now())
        self.store.state["scheduled_refresh_bridge_capability_version"] = REFRESH_BRIDGE_CAPABILITY_VERSION
        self.store.event(
            "refresh_bridge_thread_created",
            thread_id=thread_id,
            cwd=str(cwd),
        )
        self.store.save()
        return thread_id

    def _retarget_pending_refresh(self, pending: dict[str, Any]) -> str:
        """Bind an unsubmitted event to the currently configured refresh session."""
        target = self._refresh_target()
        if not target:
            return self._refresh_target(pending)
        previous = pending.get("target_thread_id")
        if previous == target:
            return target
        if pending.get("conversation_submitted") or pending.get("conversation_attempted"):
            # A method-level rejection is definitive evidence that no turn was
            # accepted by the old target.  It is safe to migrate that event to
            # a newly configured native app-server thread; an uncertain
            # timeout without such evidence must remain bound to its original
            # target so that we never duplicate a possibly accepted turn.
            error_text = str(pending.get("last_conversation_error") or "").lower()
            rejected_by_target = (
                "active writer" in error_text
                or "thread not found" in error_text
            )
            if not (
                pending.get("status") == "uncertain"
                and not pending.get("conversation_turn_id")
                and rejected_by_target
            ):
                return self._refresh_target(pending)
            old_event_id = pending.get("event_id")
            pending["conversation_attempted"] = False
            pending["conversation_submitted"] = False
            pending.pop("attempted_at", None)
            pending.pop("conversation_turn_id", None)
            pending.pop("confirmation_retry_attempts", None)
            pending["status"] = "pending"
            pending["next_retry_at"] = int(self.now())
            self.store.event(
                "scheduled_refresh_submission_reset_after_target_rejection",
                previous_thread_id=previous,
                thread_id=target,
                old_event_id=old_event_id,
            )
        old_event_id = pending.get("event_id")
        pending["target_thread_id"] = target
        scheduled_at = int(pending.get("scheduled_at") or self.now())
        pending["event_id"] = f"{target}:{scheduled_at}"
        for event in self.store.state.setdefault("pending_scheduled_refresh_events", []):
            if event.get("event_id") == old_event_id:
                event.update(pending)
        self.store.event(
            "scheduled_refresh_target_selected",
            previous_thread_id=previous,
            thread_id=target,
            scheduled_at=scheduled_at,
        )
        self.store.save()
        return target

    def _schedule_refresh_confirmation_retry(self, pending: dict[str, Any]) -> None:
        attempts = int(pending.get("confirmation_retry_attempts") or 0) + 1
        pending["confirmation_retry_attempts"] = attempts
        pending["next_retry_at"] = int(self.now()) + retry_delay(attempts)
        self.store.event(
            "scheduled_refresh_waiting_for_confirmation",
            event_id=pending.get("event_id"),
            retry_at=pending["next_retry_at"],
            attempts=attempts,
        )
        self.store.save()

    def _queue_refresh_retry(
        self,
        pending: dict[str, Any],
        event_name: str,
        error: str,
        reset_submission: bool = False,
    ) -> None:
        attempts = int(pending.get("conversation_retry_attempts") or 0) + 1
        pending["conversation_retry_attempts"] = attempts
        pending["status"] = "retry"
        pending["last_conversation_error"] = error
        pending["next_retry_at"] = int(self.now()) + retry_delay(attempts)
        if reset_submission:
            pending["conversation_attempted"] = False
            pending["conversation_submitted"] = False
            pending.pop("attempted_at", None)
            pending.pop("conversation_turn_id", None)
        self.store.event(
            event_name,
            event_id=pending.get("event_id"),
            thread_id=self._refresh_target(pending),
            retry_at=pending["next_retry_at"],
            attempts=attempts,
            error=error,
        )
        self.store.save()

    def _confirm_uncertain_refresh(self, pending: dict[str, Any]) -> bool:
        target = self._refresh_target(pending)
        if not target:
            return False
        try:
            thread = self.read_thread(target)
        except AppServerError as error:
            self.store.event(
                "scheduled_refresh_confirmation_query_failed",
                event_id=pending.get("event_id"),
                error=str(error),
            )
            return False
        turns = thread.get("turns") if isinstance(thread, dict) else None
        turns = turns if isinstance(turns, list) else []
        known_id = pending.get("conversation_turn_id")
        if known_id:
            matched = next((turn for turn in turns if str(turn.get("id")) == str(known_id)), None)
            if matched is None:
                return False
        else:
            attempted_at = timestamp_value(pending.get("attempted_at")) or 0.0
            candidates = []
            for turn in turns:
                if not isinstance(turn, dict):
                    continue
                created_at = timestamp_value(turn.get("createdAt"))
                if created_at is None:
                    created_at = timestamp_value(turn.get("created_at"))
                if created_at is not None and created_at >= attempted_at:
                    candidates.append(turn)
            if len(candidates) != 1:
                self.store.event(
                    "scheduled_refresh_confirmation_ambiguous",
                    event_id=pending.get("event_id"),
                    candidate_count=len(candidates),
                )
                return False
            pending["conversation_turn_id"] = candidates[0].get("id")
        pending["conversation_submitted"] = True
        pending["status"] = "accepted"
        pending["confirmation_observed_at"] = int(self.now())
        self.store.event(
            "scheduled_refresh_conversation_confirmed",
            event_id=pending.get("event_id"),
            turn_id=pending.get("conversation_turn_id"),
        )
        self.store.save()
        return True

    @staticmethod
    def _turn_status(turn: dict[str, Any]) -> str | None:
        value = turn.get("status")
        if isinstance(value, dict):
            value = value.get("type") or value.get("status")
        return str(value).lower() if value is not None else None

    @staticmethod
    def _refresh_tool_evidence(turn: dict[str, Any]) -> str:
        """Return success, failed, or missing evidence for the quota tool."""
        items = turn.get("items") if isinstance(turn, dict) else None
        if not isinstance(items, list):
            return "missing"
        observed = False
        for item in items:
            if not isinstance(item, dict):
                continue
            kind = str(item.get("type") or "").replace("_", "").lower()
            tool = str(item.get("tool") or item.get("name") or "")
            if kind != "dynamictoolcall" or tool != "get_usage_limits":
                continue
            observed = True
            if item.get("success") is True and str(item.get("status") or "").lower() in {
                "completed", "complete", "success", "succeeded"
            }:
                return "success"
        return "failed" if observed else "missing"

    def _confirm_submitted_refresh(self, pending: dict[str, Any]) -> bool:
        """Confirm execution evidence for an already accepted refresh turn."""
        target = self._refresh_target(pending)
        turn_id = pending.get("conversation_turn_id")
        if not target or not turn_id:
            return False
        try:
            thread = self.read_thread(target)
        except AppServerError as error:
            self.store.event(
                "scheduled_refresh_execution_query_failed",
                event_id=pending.get("event_id"),
                turn_id=turn_id,
                error=str(error),
            )
            return False
        turns = thread.get("turns") if isinstance(thread, dict) else None
        turns = turns if isinstance(turns, list) else []
        matched = next((turn for turn in turns if isinstance(turn, dict) and str(turn.get("id")) == str(turn_id)), None)
        if matched is None:
            wait_for_completion = getattr(self.client, "wait_for_turn_completion", None)
            if callable(wait_for_completion):
                completion_timeout = min(
                    5.0,
                    float(self.config.get("quota_read_timeout_seconds") or 5),
                )
                matched = wait_for_completion(target, str(turn_id), completion_timeout)
        if (
            matched is None
            and isinstance(thread, dict)
            and thread.get("_turns_unavailable")
            and (thread.get("status") or {}).get("type") == "idle"
        ):
            # Some managed desktop builds expose no turn history at all.  A
            # turn/start response with an in-progress turn followed by the
            # same dedicated thread becoming idle is the strongest available
            # completion evidence on that protocol version.  It is kept
            # distinct from turn-level evidence in the audit record.
            matched = {
                "id": turn_id,
                "status": "completed",
                "_completion_evidence": "thread_idle_without_turn_history",
            }
        if matched is None:
            return False
        status = self._turn_status(matched)
        terminal_statuses = {
            "completed", "complete", "succeeded", "success", "failed",
            "error", "errored", "interrupted", "cancelled", "canceled",
        }
        has_execution_result = any(
            matched.get(key) is not None
            for key in ("result", "output", "completedAt", "completed_at", "error")
        )
        if status not in terminal_statuses and not has_execution_result:
            return False
        if self.config.get("scheduled_refresh_delivery") == "local_app_server_bridge":
            tool_evidence = self._refresh_tool_evidence(matched)
            if tool_evidence != "success":
                event_name = (
                    "scheduled_refresh_execution_failed"
                    if tool_evidence == "failed"
                    else "scheduled_refresh_execution_missing_tool_evidence"
                )
                self.store.event(
                    event_name,
                    event_id=pending.get("event_id"),
                    turn_id=turn_id,
                    turn_status=status,
                )
                self._notify_once(
                    f"refresh-execution:{pending.get('event_id')}:{tool_evidence}",
                    "Codex 定时会话未完成额度读取",
                    "会话 turn 已结束，但没有成功的 get_usage_limits 工具证据；保持待确认，不标记刷新成功",
                )
                return False
        pending["status"] = "confirmed"
        pending["execution_confirmed_at"] = int(self.now())
        pending["turn_status"] = status
        self.store.event(
            "scheduled_refresh_execution_confirmed",
            event_id=pending.get("event_id"),
            turn_id=turn_id,
            turn_status=status,
            completion_evidence=matched.get("_completion_evidence", "turn"),
        )
        self.store.save()
        return True

    def _is_pending_refresh_event(self, candidate: dict[str, Any]) -> bool:
        status = candidate.get("status")
        if status in {"pending", "retry"}:
            return True
        return bool(
            self.config.get("scheduled_refresh_delivery") == "local_app_server_bridge"
            and status == "delegated"
            and not candidate.get("conversation_submitted")
        )

    def _select_pending_refresh_event(self) -> dict[str, Any] | None:
        """Select one durable refresh event without replaying legacy spam.

        Older builds could leave several unsubmitted ``delegated`` records in
        the state file.  Once delivery is moved to the local bridge, those
        records represent missed schedule points, not four independent model
        turns.  Keep the newest event as the durable identity and fold the
        older points into its missed-count summary.
        """
        events = self.store.state.setdefault("pending_scheduled_refresh_events", [])
        if self.config.get("scheduled_refresh_delivery") == "local_app_server_bridge":
            legacy = [
                candidate
                for candidate in events
                if candidate.get("status") == "delegated"
                and not candidate.get("conversation_submitted")
            ]
            if legacy:
                selected = max(
                    legacy,
                    key=lambda candidate: float(candidate.get("scheduled_at") or 0),
                )
                if len(legacy) > 1:
                    merged_ids = [
                        str(candidate.get("event_id"))
                        for candidate in legacy
                        if candidate is not selected
                    ]
                    missed_count = sum(
                        int(candidate.get("missed_count") or 0)
                        for candidate in legacy
                    ) + len(legacy) - 1
                    selected["missed_count"] = missed_count
                    selected["legacy_merged_event_ids"] = merged_ids
                    selected["next_retry_at"] = int(self.now())
                    for candidate in legacy:
                        if candidate is selected:
                            continue
                        candidate["status"] = "merged"
                        candidate["merged_into"] = selected.get("event_id")
                    self.store.event(
                        "scheduled_refresh_legacy_events_merged",
                        selected_event_id=selected.get("event_id"),
                        merged_event_ids=merged_ids,
                        missed_count=missed_count,
                    )
                    self.store.save()
                return selected
        return next(
            (candidate for candidate in events if self._is_pending_refresh_event(candidate)),
            None,
        )

    def _deliver_pending_refresh(self) -> None:
        pending = self.store.state.get("pending_scheduled_refresh")
        if not pending:
            pending = self._select_pending_refresh_event()
            if pending:
                self.store.state["pending_scheduled_refresh"] = pending
        if not pending or int(pending.get("next_retry_at") or 0) > int(self.now()):
            return
        if self.config.get("scheduled_refresh_delivery") == "local_app_server_bridge":
            # A legacy delegated event was never sent.  Re-open it as a real
            # local conversation event; this is safe because no turn id was
            # recorded as accepted by the old automation path.
            if pending.get("status") == "delegated" and not pending.get("conversation_submitted"):
                pending["status"] = "pending"
                pending["conversation_delivery"] = "local_app_server_bridge"
                pending["next_retry_at"] = int(self.now())
                self.store.event(
                    "scheduled_refresh_legacy_delivery_migrated",
                    event_id=pending.get("event_id"),
                )
                self.store.save()
            # Do not retarget an uncertain submission: it may already have
            # been accepted by the old target.  Query that original target
            # first, just as the normal exactly-once path requires.
            if not pending.get("conversation_attempted") and not pending.get("conversation_submitted"):
                if isinstance(self.client, AppServerClient) and not self._dry_run():
                    try:
                        # Warm the second connection before turn/start.  The
                        # app-server gives a dynamic tool only a short window
                        # to answer, so opening this socket from inside the
                        # tool callback is too late on a cold launch.
                        self._prepare_refresh_tool_snapshot()
                    except (AppServerError, OSError, subprocess.SubprocessError) as error:
                        self._queue_refresh_retry(
                            pending,
                            "refresh_tool_connection_failed",
                            str(error),
                        )
                        self._notify_once(
                            "refresh-tool-connection-failed",
                            "Codex 定时会话额度工具不可用",
                            f"额度工具连接尚未就绪，将按退避重试：{error}",
                        )
                        return
                if self._ensure_refresh_bridge_thread(pending) is None:
                    return
        target = self._retarget_pending_refresh(pending)
        pending.setdefault("status", "pending")
        if pending.get("status") == "submitting":
            # A crash between turn/start and persisting its response leaves a
            # durable in-progress marker.  Reclassify it as uncertain and
            # query the conversation before considering any retry.
            pending["status"] = "uncertain"
            self.store.event(
                "scheduled_refresh_submission_recovered_as_uncertain",
                event_id=pending.get("event_id"),
            )
            self.store.save()
        if pending.get("status") == "uncertain":
            if not self._confirm_uncertain_refresh(pending):
                self._schedule_refresh_confirmation_retry(pending)
                return
        if pending.get("conversation_submitted") and pending.get("status") != "confirmed":
            if not self._confirm_submitted_refresh(pending):
                self._schedule_refresh_confirmation_retry(pending)
                return
        message = self._pending_refresh_message(pending)
        if target and not pending.get("conversation_submitted"):
            if self._dry_run():
                self.store.event("would_send_refresh_message", thread_id=target)
            else:
                if pending.get("conversation_attempted"):
                    # A timeout can mean the request was accepted. Never blindly
                    # start a second model turn for the same scheduled event.
                    return
                try:
                    thread = self.read_thread(target)
                except AppServerMethodError as error:
                    # A daemon-created persistent thread may exist but not be
                    # loaded into this app-server worker yet.  This is not a
                    # missing target: turn/start is the operation that loads
                    # it.  Only this explicit protocol error bypasses the
                    # read-before-submit gate; other method errors remain a
                    # retryable delivery failure.
                    if "thread not loaded" in error.message.lower():
                        self.store.event(
                            "refresh_message_target_not_loaded",
                            thread_id=target,
                            error=str(error),
                        )
                        thread = {"status": {"type": "notLoaded"}}
                    else:
                        pending["next_retry_at"] = int(self.now()) + 60
                        self.store.event(
                            "refresh_message_target_read_failed",
                            thread_id=target,
                            error=str(error),
                        )
                        self.store.save()
                        return
                except AppServerError as error:
                    pending["next_retry_at"] = int(self.now()) + 60
                    self.store.event(
                        "refresh_message_target_read_failed",
                        thread_id=target,
                        error=str(error),
                    )
                    self.store.save()
                    return
                status = (thread.get("status") or {}).get("type")
                if status in ACTIVE_THREAD_STATUSES:
                    pending["next_retry_at"] = int(self.now()) + 60
                    self.store.save()
                    return
                if status == "notLoaded":
                    # The managed app-server can report a thread as notLoaded
                    # while rejecting thread/resume with -32600 "already has
                    # an active writer".  turn/start is the actual operation
                    # we need and can load the dedicated thread itself; do not
                    # make resume a prerequisite for delivery.
                    self.store.event(
                        "refresh_message_target_not_loaded",
                        thread_id=target,
                    )
                elif status != "idle":
                    pending["next_retry_at"] = int(self.now()) + 60
                    self.store.event("refresh_message_target_unavailable", thread_id=target, status=status)
                    self.store.save()
                    return
                pending["conversation_attempted"] = True
                pending["attempted_at"] = int(self.now())
                pending["status"] = "submitting"
                self.store.save()
                prompt = (
                    "【额度守护定时会话】本专用会话已提供名为 get_usage_limits 的额度工具。"
                    "请先实际调用这个工具一次，读取返回的五小时和总额度，"
                    "简短回复五小时剩余百分比及接口返回的重置时间（北京时间）。"
                    "这是用户授权的定时请求。不要调用 bank reset，不要创建或恢复 goal，"
                    "不要修改项目或设置。"
                )
                try:
                    result = self.client.call("turn/start", {
                        "threadId": target,
                        "input": [{"type": "text", "text": prompt}],
                    })
                    turn_id = self._started_turn_id(result)
                    if not turn_id:
                        raise AppServerError("会话请求返回但没有 turn id，提交状态待确认")
                    pending["conversation_submitted"] = True
                    pending["conversation_turn_id"] = turn_id
                    pending["status"] = "accepted"
                    self.store.event("scheduled_refresh_conversation_submitted", thread_id=target, turn_id=turn_id, scheduled_at=pending.get("scheduled_at"))
                    self.store.save()
                except AppServerMethodError as error:
                    if error.is_active_writer:
                        self._queue_refresh_retry(
                            pending,
                            "refresh_message_target_writer_busy",
                            str(error),
                            reset_submission=True,
                        )
                        self._notify_once(
                            f"refresh-writer-busy:{target}",
                            "Codex 定时会话被占用",
                            "专用刷新会话当前有其他 writer，占用解除后将自动重试；额度读取不受影响",
                        )
                        return
                    pending["status"] = "uncertain"
                    self.store.event("scheduled_refresh_conversation_uncertain", thread_id=target, error=str(error))
                    self.notify("Codex 定时会话提交待确认", str(error))
                    pending.pop("next_retry_at", None)
                    self.store.save()
                    return
                if not self._confirm_submitted_refresh(pending):
                    return
        if self.notify("Codex 定时额度提醒", message):
            pending["status"] = "confirmed"
            pending["notification_delivered"] = True
            self.store.event(
                "scheduled_refresh_notification_delivered",
                scheduled_at=pending.get("scheduled_at"),
                missed_count=pending.get("missed_count", 0),
            )
            events = self.store.state.setdefault("pending_scheduled_refresh_events", [])
            for event in events:
                if event.get("event_id") == pending.get("event_id"):
                    event.update(pending)
            self.store.state["pending_scheduled_refresh"] = None
            self.store.state["pending_scheduled_refresh"] = self._select_pending_refresh_event()
            self.store.save()
            return
        attempts = int(pending.get("notification_attempts") or 0) + 1
        pending["notification_attempts"] = attempts
        pending["status"] = "retry"
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
                int(self.config.get("scheduled_refresh_hour_shift", 0)),
            )
            if len(due_times) >= 100:
                break
        self.next_fixed_refresh_at = next_at
        self.store.state["next_fixed_refresh_at"] = next_at
        self.store.state["last_scheduled_refresh_at"] = due_times[-1]
        self.store.state["scheduled_refresh_missed_count"] = max(0, len(due_times) - 1)
        pending = self.store.state.get("pending_scheduled_refresh") or {}
        missed_count = max(0, len(due_times) - 1)
        event_id = f"{self.config.get('scheduled_refresh_thread_id') or 'notification'}:{int(due_times[-1])}"
        event = {
            "event_id": event_id,
            "target_thread_id": self.config.get("scheduled_refresh_thread_id") or None,
            "scheduled_at": due_times[-1],
            "missed_count": missed_count,
            "primary_remaining": limits.primary_remaining,
            "secondary_remaining": limits.secondary_remaining,
            "notification_attempts": 0,
            "next_retry_at": int(now),
            "status": "pending",
        }
        events = self.store.state.setdefault("pending_scheduled_refresh_events", [])
        existing = next((item for item in events if item.get("event_id") == event_id), None)
        if existing is None:
            events.append(event)
        else:
            event = existing
        active = self.store.state.get("pending_scheduled_refresh")
        if not active or active.get("status") in {"confirmed", "failed"}:
            self.store.state["pending_scheduled_refresh"] = event
        elif active.get("event_id") == event_id:
            active.update(event)
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
        self._write_health("cycle_start")
        limits = self.read_limits()
        self.handle_limits(limits)
        self._process_fixed_refresh(limits)
        self.store.save()
        self._write_health("cycle_complete")
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
                    wait_seconds = self.next_wait_seconds()
                    heartbeat_seconds = float(self.config["heartbeat_interval_seconds"])
                    notification = self.client.next_notification(
                        min(wait_seconds, heartbeat_seconds)
                    )
                    self._write_health("waiting")
                    if notification is not None and notification.get("method") in watched:
                        break
                    if notification is None and wait_seconds <= heartbeat_seconds:
                        break
            except (AppServerError, OSError) as error:
                self.client.close()
                self._write_health("connection_error", error=str(error))
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


def default_reader_state() -> dict[str, Any]:
    return {
        "quota_sequence": 0,
        "next_quota_check_at": None,
        "quota_check_interval_seconds": None,
        "quota_check_mode": None,
        "last_primary_remaining": None,
        "last_secondary_remaining": None,
        "primary_resets_at": None,
        "secondary_resets_at": None,
        "primary_data_status": "unknown",
        "secondary_data_status": "unknown",
        "primary_last_success_at": None,
        "secondary_last_success_at": None,
        "primary_data_expires_at": None,
        "secondary_data_expires_at": None,
        "last_error": None,
        "request_in_flight": False,
        "request_id": None,
        "request_started_at": None,
        "request_finished_at": None,
        "request_deadline_at": None,
        "updated_at": None,
    }


def append_json_line(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_json_lines(path: pathlib.Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    records: list[dict[str, Any]] = []
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


class QuotaReader:
    """Owns local quota reads and publishes immutable result records."""

    def __init__(
        self,
        client: AppServerClient,
        config: dict[str, Any],
        now: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.client = client
        self.config = dict(config)
        self.instance_id = str(uuid.uuid4())
        self.process_started_at = time.time()
        self.config["_process_instance_id"] = self.instance_id
        self.now = now
        self.monotonic = monotonic
        self.sleep = sleep
        self.persist = not bool(config.get("dry_run"))
        self.state_file = pathlib.Path(config["reader_state_file"]).expanduser()
        self.health_file = pathlib.Path(config["reader_health_file"]).expanduser()
        self.result_file = pathlib.Path(config["quota_result_file"]).expanduser()
        loaded = load_json(self.state_file, default_reader_state()) if self.persist else {}
        base = default_reader_state()
        base.update(loaded)
        self.state = base
        if self.persist:
            # Recover the monotonic cursor if a process died after appending a
            # result but before its state-file replace completed.
            prior_records = read_json_lines(self.result_file)
            prior_sequences: list[int] = []
            for item in prior_records:
                if not isinstance(item, dict):
                    continue
                try:
                    prior_sequences.append(int(item.get("sequence") or 0))
                except (TypeError, ValueError):
                    continue
            if prior_sequences:
                self.state["quota_sequence"] = max(
                    int(self.state.get("quota_sequence") or 0),
                    max(prior_sequences),
                )
        persisted_deadline = self.state.get("next_quota_check_at")
        self.next_quota_check_at = (
            self.now() if persisted_deadline is None else max(self.now(), float(persisted_deadline))
        )
        self.last_wall = self.now()
        self.last_monotonic = self.monotonic()
        self._write_health("starting")

    def _save(self) -> None:
        self.state["updated_at"] = int(self.now())
        if self.persist:
            save_json(self.state_file, self.state)

    def _write_health(self, status: str, **details: Any) -> None:
        payload = {
            "pid": os.getpid(),
            "instance_id": self.instance_id,
            "process_started_at": self.process_started_at,
            "heartbeat_at": time.time(),
            "heartbeat_interval_seconds": self.config.get("heartbeat_interval_seconds", 5),
            "status": status,
            "config_revision": self.config.get("config_revision"),
            "quota_sequence": self.state.get("quota_sequence", 0),
            "next_quota_check_at": self.next_quota_check_at,
            "primary_last_success_at": self.state.get("primary_last_success_at"),
            "secondary_last_success_at": self.state.get("secondary_last_success_at"),
            "primary_data_status": effective_window_status(self.state, "primary", self.now()),
            "secondary_data_status": effective_window_status(self.state, "secondary", self.now()),
            "primary_data_expires_at": self.state.get("primary_data_expires_at"),
            "secondary_data_expires_at": self.state.get("secondary_data_expires_at"),
            "primary_remaining": self.state.get("last_primary_remaining"),
            "secondary_remaining": self.state.get("last_secondary_remaining"),
            "request_in_flight": self.state.get("request_in_flight", False),
            "request_id": self.state.get("request_id"),
            "request_started_at": self.state.get("request_started_at"),
            "request_finished_at": self.state.get("request_finished_at"),
            "request_deadline_at": self.state.get("request_deadline_at"),
        }
        payload.update(details)
        try:
            if self.persist:
                save_json(self.health_file, payload)
        except OSError:
            self.state["last_error"] = "无法写入读取健康文件"

    def _record_limits(self, limits: Limits, error: str | None = None) -> None:
        now = self.now()
        self.state["quota_sequence"] = int(self.state.get("quota_sequence") or 0) + 1
        self.state["last_primary_remaining"] = limits.primary_remaining
        self.state["last_secondary_remaining"] = limits.secondary_remaining
        self.state["primary_resets_at"] = limits.primary_resets_at
        self.state["secondary_resets_at"] = limits.secondary_resets_at
        self.state["primary_data_status"] = "current" if limits.primary_remaining is not None else "invalid"
        self.state["secondary_data_status"] = "current" if limits.secondary_remaining is not None else "invalid"
        if limits.primary_remaining is not None:
            self.state["primary_last_success_at"] = now
        if limits.secondary_remaining is not None:
            self.state["secondary_last_success_at"] = now
        interval, mode = self._interval(limits)
        self.state["quota_check_interval_seconds"] = interval
        self.state["quota_check_mode"] = mode
        self.next_quota_check_at = now + interval
        self.state["next_quota_check_at"] = self.next_quota_check_at
        expiry = now + interval + int(self.config["quota_read_timeout_seconds"])
        self.state["primary_data_expires_at"] = expiry if limits.primary_remaining is not None else now
        self.state["secondary_data_expires_at"] = expiry if limits.secondary_remaining is not None else now
        self.state["last_error"] = error
        record = {
            "sequence": self.state["quota_sequence"],
            "recorded_at": now,
            "primary_remaining": limits.primary_remaining,
            "secondary_remaining": limits.secondary_remaining,
            "primary_resets_at": limits.primary_resets_at,
            "secondary_resets_at": limits.secondary_resets_at,
            "reset_credit_count": limits.reset_credit_count,
            "reset_credits": limits.reset_credits,
            "primary_data_status": self.state["primary_data_status"],
            "secondary_data_status": self.state["secondary_data_status"],
            "primary_data_expires_at": self.state["primary_data_expires_at"],
            "secondary_data_expires_at": self.state["secondary_data_expires_at"],
            "valid": limits.primary_remaining is not None and limits.secondary_remaining is not None and error is None,
            "error": error,
        }
        if self.persist:
            try:
                append_json_line(self.result_file, record)
            except OSError as error:
                self.state["last_error"] = f"额度结果发布失败：{error}"
                self._save()
                self._write_health("write_failed", error=str(error))
                raise ResultPublicationError(str(error)) from error
        self._save()
        self._write_health("read_complete" if error is None else "read_failed")

    def _interval(self, limits: Limits) -> tuple[int, str]:
        if limits.primary_remaining is None or limits.secondary_remaining is None:
            return quota_gradient_interval(None, self.config), "critical"
        remaining = min(limits.primary_remaining, limits.secondary_remaining)
        interval = quota_gradient_interval(remaining, self.config)
        return interval, "critical" if remaining < 10 else "gradient"

    def _read_timeout_seconds(self) -> float:
        """Return the bounded budget for the isolated real quota reader."""
        return float(
            self.config.get(
                "reader_quota_read_timeout_seconds",
                self.config["quota_read_timeout_seconds"],
            )
        )

    def read_once(self) -> Limits:
        started = self.now()
        request_id = str(uuid.uuid4())
        timeout = self._read_timeout_seconds()
        deadline = self.monotonic() + timeout
        self.state.update({
            "request_in_flight": True,
            "request_id": request_id,
            "request_started_at": started,
            "request_finished_at": None,
            "request_deadline_at": started + timeout,
        })
        self._save()
        self._write_health("reading")
        try:
            try:
                payload = self.client.call("account/rateLimits/read", {}, deadline=deadline)
            except TypeError as error:
                # Keep small test doubles and older local adapters compatible;
                # the production AppServerClient always accepts the deadline.
                if "deadline" not in str(error):
                    raise
                payload = self.client.call("account/rateLimits/read", {})
            limits = parse_limits(payload)
            self.state["request_in_flight"] = False
            self.state["request_finished_at"] = self.now()
            self._record_limits(limits)
            return limits
        except Exception as error:
            self.state["request_in_flight"] = False
            self.state["request_finished_at"] = self.now()
            if isinstance(error, ResultPublicationError):
                self._save()
                self._write_health("write_failed", error=str(error))
                raise
            limits = Limits(None, None, None, None, 0, [], {})
            self._record_limits(limits, str(error))
            raise

    def _interruptible_sleep(self, seconds: float) -> None:
        end = self.monotonic() + max(0.0, seconds)
        heartbeat = float(self.config["heartbeat_interval_seconds"])
        while self.monotonic() < end:
            remaining = end - self.monotonic()
            self._write_health("waiting_reconnect")
            self.sleep(min(heartbeat, remaining))

    def run_forever(self) -> None:
        watched = {
            "account/rateLimits/updated",
            "turn/started",
            "thread/status/changed",
        }
        backoff = 1
        while True:
            now = self.now()
            mono = self.monotonic()
            wall_delta = now - self.last_wall
            mono_delta = mono - self.last_monotonic
            self.last_wall = now
            self.last_monotonic = mono
            if wall_delta < -60 or abs(wall_delta - mono_delta) > 60:
                self.next_quota_check_at = now
                self._write_health("clock_changed")
            try:
                if self.next_quota_check_at <= now:
                    self.read_once()
                    backoff = 1
                    continue
                notification = self.client.next_notification(
                    min(self.next_quota_check_at - now, float(self.config["heartbeat_interval_seconds"]))
                )
                self._write_health("waiting")
                if notification is not None and notification.get("method") in watched:
                    self.next_quota_check_at = self.now()
            except (AppServerError, OSError, subprocess.SubprocessError) as error:
                self.client.close()
                self.state["last_error"] = str(error)
                self._save()
                self._write_health("connection_error", error=str(error))
                self._interruptible_sleep(backoff)
                backoff = min(60, backoff * 2)


class ActionProcess:
    """Consumes reader results and owns all configured actions."""

    def __init__(
        self,
        client: AppServerClient | None,
        config: dict[str, Any],
        now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.config = dict(config)
        self.config["health_file"] = self.config["actions_health_file"]
        self.config["_process_instance_id"] = str(uuid.uuid4())
        self.now = now
        self.sleep = sleep
        self.client = client
        self.store = StateStore(
            pathlib.Path(self.config["state_file"]).expanduser(),
            pathlib.Path(self.config["event_log_file"]).expanduser(),
            persist=not bool(self.config.get("dry_run")),
        )
        self.guard = QuotaGuard(client, self.config, self.store, now=now, sleep=sleep)
        self._restore_observed_limits()
        if self.client is not None:
            try:
                self.client.dynamic_tool_handler = self.guard._handle_refresh_dynamic_tool
            except (AttributeError, TypeError):
                # Lightweight test doubles and read-only client adapters may
                # intentionally not expose the app-server tool hook.
                pass

    def _record_validity(self, record: dict[str, Any]) -> str:
        """Return whether a published reader result is safe for actions."""
        if record.get("valid") is not True:
            return "invalid"
        recorded_at = timestamp_value(record.get("recorded_at"))
        if recorded_at is None:
            return "missing_timestamp"
        for label in ("primary", "secondary"):
            value = record.get(f"{label}_remaining")
            if isinstance(value, bool):
                return f"{label}_invalid"
            declared_status = record.get(f"{label}_data_status")
            if declared_status is not None and str(declared_status) != "current":
                return f"{label}_{declared_status}"
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return f"{label}_invalid"
            if not math.isfinite(numeric) or not 0.0 <= numeric <= 100.0:
                return f"{label}_invalid"
            expires_at = timestamp_value(record.get(f"{label}_data_expires_at"))
            if expires_at is None:
                return f"{label}_missing_expiry"
            if self.now() > expires_at:
                return f"{label}_expired"
        return "current"

    def _limits_from_record(self, record: dict[str, Any]) -> Limits | None:
        if self._record_validity(record) != "current":
            return None
        return Limits(
            record.get("primary_remaining"),
            record.get("secondary_remaining"),
            record.get("primary_resets_at"),
            record.get("secondary_resets_at"),
            int(record.get("reset_credit_count") or 0),
            [item for item in (record.get("reset_credits") or []) if isinstance(item, dict)],
            {},
        )

    def _mirror_reader_result(self, record: dict[str, Any], validity: str) -> None:
        """Reflect the reader's per-window evidence in the action health state."""
        now = self.now()
        sequence = int(record.get("sequence") or 0)
        self.store.state["quota_sequence"] = max(
            int(self.store.state.get("quota_sequence") or 0), sequence
        )
        self.store.state["last_error"] = record.get("error")
        for label in ("primary", "secondary"):
            value = record.get(f"{label}_remaining")
            expiry = timestamp_value(record.get(f"{label}_data_expires_at"))
            declared_status = record.get(f"{label}_data_status")
            if declared_status is None:
                value_is_valid = False
                if value is not None and not isinstance(value, bool):
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError):
                        numeric = None
                    value_is_valid = (
                        numeric is not None
                        and math.isfinite(numeric)
                        and 0 <= numeric <= 100
                    )
                record_status = (
                    "current"
                    if value_is_valid and expiry is not None and now <= expiry
                    else "expired"
                    if value_is_valid and expiry is not None
                    else "invalid"
                )
            else:
                record_status = str(declared_status)
            if record_status == "current" and expiry is not None:
                status = "current" if now <= expiry else "expired"
            elif record_status == "expired" or "expired" in validity:
                status = "expired"
            else:
                status = "invalid"
            self.store.state[f"{label}_data_status"] = status
            self.store.state[f"{label}_data_expires_at"] = expiry or now
            if value is not None and not isinstance(value, bool):
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    numeric = None
                if numeric is not None and math.isfinite(numeric) and 0 <= numeric <= 100:
                    self.store.state[f"last_{label}_remaining"] = numeric
            if status == "current":
                recorded_at = timestamp_value(record.get("recorded_at"))
                if recorded_at is not None:
                    self.store.state[f"last_{label}_success_at"] = recorded_at
        for label in ("primary", "secondary"):
            reset_at = record.get(f"{label}_resets_at")
            if reset_at is not None:
                self.store.state[f"{label}_resets_at"] = reset_at

    def _remember_reader_result(self, record: dict[str, Any]) -> None:
        """Persist the newest reader result, including invalid evidence."""
        self.store.state["last_observed_limits"] = {
            "primary_remaining": record.get("primary_remaining"),
            "secondary_remaining": record.get("secondary_remaining"),
            "primary_resets_at": record.get("primary_resets_at"),
            "secondary_resets_at": record.get("secondary_resets_at"),
            "recorded_at": record.get("recorded_at"),
            "primary_data_status": record.get("primary_data_status"),
            "secondary_data_status": record.get("secondary_data_status"),
            "primary_data_expires_at": record.get("primary_data_expires_at"),
            "secondary_data_expires_at": record.get("secondary_data_expires_at"),
            "valid": record.get("valid"),
            "error": record.get("error"),
        }

    def _restore_observed_limits(self) -> None:
        """Rehydrate action health from the latest immutable reader snapshot."""
        observed = self.store.state.get("last_observed_limits")
        if not isinstance(observed, dict):
            return
        record = dict(observed)
        record.setdefault(
            "sequence",
            int(self.store.state.get("last_consumed_quota_sequence") or 0),
        )
        record.setdefault("recorded_at", self.store.state.get("updated_at"))
        for label in ("primary", "secondary"):
            expiry = timestamp_value(record.get(f"{label}_data_expires_at"))
            if record.get(f"{label}_data_status") is None:
                value = record.get(f"{label}_remaining")
                value_is_valid = False
                if value is not None and not isinstance(value, bool):
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError):
                        numeric = None
                    value_is_valid = (
                        numeric is not None
                        and math.isfinite(numeric)
                        and 0 <= numeric <= 100
                    )
                record[f"{label}_data_status"] = (
                    "current"
                    if value_is_valid and expiry is not None and self.now() <= expiry
                    else "expired"
                    if value_is_valid and expiry is not None
                    else "invalid"
                )
        self._mirror_reader_result(record, "current")
        self.store.save()
        self.guard._write_health("actions_restored")

    def process_results(self) -> None:
        consumed = int(self.store.state.get("last_consumed_quota_sequence") or 0)
        records = sorted(
            read_json_lines(pathlib.Path(self.config["quota_result_file"]).expanduser()),
            key=lambda item: int(item.get("sequence") or 0),
        )
        for record in records:
            sequence = int(record.get("sequence") or 0)
            if sequence <= consumed:
                continue
            validity = self._record_validity(record)
            limits = self._limits_from_record(record) if validity == "current" else None
            if limits is None:
                self._mirror_reader_result(record, validity)
                self._remember_reader_result(record)
                consumed = sequence
                self.store.state["last_consumed_quota_sequence"] = sequence
                self.store.event(
                    "quota_result_expired" if "expired" in validity else "quota_result_invalid",
                    sequence=sequence,
                    reason=validity,
                    error=record.get("error"),
                )
                self.store.save()
                continue
            if self.client is None:
                self.store.event("actions_unavailable", sequence=sequence)
                self.store.save()
                return
            # Do not advance the consumer cursor until all action work for this
            # sequence has completed.  A crashed/unavailable action process can
            # therefore inspect the immutable result again after restart.
            self.guard.handle_limits(limits, record=False)
            self.guard._process_fixed_refresh(limits)
            self._mirror_reader_result(record, validity)
            consumed = sequence
            self.store.state["last_consumed_quota_sequence"] = sequence
            self._remember_reader_result(record)
            self.store.save()
        self.guard._write_health("actions_results_updated")

    def _latest_limits(self) -> Limits | None:
        value = self.store.state.get("last_observed_limits")
        if not isinstance(value, dict):
            return None
        if value.get("primary_remaining") is None or value.get("secondary_remaining") is None:
            return None
        for label in ("primary", "secondary"):
            expires_at = timestamp_value(value.get(f"{label}_data_expires_at"))
            if expires_at is None or self.now() > expires_at:
                return None
        return Limits(
            value["primary_remaining"],
            value["secondary_remaining"],
            value.get("primary_resets_at"),
            value.get("secondary_resets_at"),
            0,
            [],
            {},
        )

    def run_forever(self) -> None:
        backoff = 1
        while True:
            try:
                self.process_results()
                latest = self._latest_limits()
                if latest is not None:
                    self.guard._process_fixed_refresh(latest)
                self.guard._write_health("actions_waiting")
                if self.client is None:
                    self.sleep(1)
                else:
                    self.client.next_notification(1)
                backoff = 1
            except (AppServerError, OSError, subprocess.SubprocessError) as error:
                self.guard._write_health("actions_error", error=str(error))
                if self.client is not None:
                    self.client.close()
                self._interruptible_reconnect_sleep(min(60, backoff))
                backoff = min(60, backoff * 2)

    def _interruptible_reconnect_sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + max(0.0, float(seconds))
        heartbeat = max(1.0, float(self.config["heartbeat_interval_seconds"]))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            self.guard._write_health(
                "waiting_reconnect",
                reconnect_backoff_seconds=max(0.0, remaining),
            )
            self.sleep(min(heartbeat, remaining))

    def close(self) -> None:
        self.guard.close()


class QuotaSupervisor:
    """Small parent process that watches reader and actions independently.

    LaunchAgent's KeepAlive only notices an exited process. This supervisor
    additionally notices a reader whose event loop stopped publishing a
    heartbeat or whose quota request exceeded its deadline. It only starts and
    stops the reader process that it created; it never touches Codex tasks or
    automatically restarts the actions process.
    """

    def __init__(
        self,
        config: dict[str, Any],
        config_path: pathlib.Path,
        python_executable: str | None = None,
        script_path: pathlib.Path | None = None,
        now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        popen: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
    ):
        self.config = config
        self.config_path = config_path
        self.python_executable = python_executable or sys.executable
        self.script_path = script_path or pathlib.Path(__file__).resolve()
        self.now = now
        self.sleep = sleep
        self.popen = popen
        self.instance_id = str(uuid.uuid4())
        self.process_started_at = time.time()
        self.config["_process_instance_id"] = self.instance_id
        self.reader: subprocess.Popen[Any] | None = None
        self.actions: subprocess.Popen[Any] | None = None
        # Compatibility aliases for older tests and status readers.
        self.worker: subprocess.Popen[Any] | None = None
        self.reader_started_at: float | None = None
        self.actions_started_at: float | None = None
        self.worker_started_at: float | None = None
        self.reader_pid: int | None = None
        self.reader_instance_id: str | None = None
        self.actions_pid: int | None = None
        self.actions_instance_id: str | None = None
        self.restart_times: list[float] = []
        self.suppressed_until: float | None = None
        self.alerted_keys: set[str] = set()
        self.watchdog_file = pathlib.Path(
            str(config.get("watchdog_state_file") or (
                pathlib.Path(config.get("state_file", "state.json")).expanduser()
                .with_name("watchdog.json")
            ))
        ).expanduser()
        if not config.get("dry_run"):
            persisted = load_json(self.watchdog_file, {})
            persisted_times = persisted.get("restart_times")
            if isinstance(persisted_times, list):
                self.restart_times = [
                    timestamp_value(item) for item in persisted_times
                    if timestamp_value(item) is not None
                ]
            persisted_suppressed = timestamp_value(persisted.get("restart_suppressed_until"))
            self.suppressed_until = persisted_suppressed

    def _write_health(self, status: str, **details: Any) -> None:
        if self.config.get("dry_run"):
            return
        payload = {
            "pid": os.getpid(),
            "instance_id": self.instance_id,
            "process_started_at": self.process_started_at,
            "heartbeat_at": time.time(),
            "heartbeat_interval_seconds": self.config.get("heartbeat_interval_seconds", 5),
            "status": status,
            "reader_pid": getattr(self.reader, "pid", None) if self.reader else None,
            "actions_pid": getattr(self.actions, "pid", None) if self.actions else None,
            "restart_count_in_window": len(self.restart_times),
            "restart_times": list(self.restart_times),
            "restart_suppressed_until": self.suppressed_until,
            "config_revision": self.config.get("config_revision"),
        }
        payload.update(details)
        try:
            save_json(self.watchdog_file, payload)
        except OSError:
            pass

    def _worker_health_file(self) -> pathlib.Path:
        return pathlib.Path(
            str(self.config.get("reader_health_file") or self.config.get("health_file") or (
                pathlib.Path(self.config.get("state_file", "state.json")).expanduser()
                .with_name("reader-health.json")
            ))
        ).expanduser()

    def _reader_health_file(self) -> pathlib.Path:
        return self._worker_health_file()

    def _actions_health_file(self) -> pathlib.Path:
        return pathlib.Path(
            str(self.config.get("actions_health_file") or (
                pathlib.Path(self.config.get("state_file", "state.json")).expanduser()
                .with_name("actions-health.json")
            ))
        ).expanduser()

    def _alert_once(self, key: str, title: str, message: str) -> None:
        if key in self.alerted_keys or not self.config.get("notify_desktop", True):
            return
        script = f'display notification {json.dumps(message, ensure_ascii=False)} with title {json.dumps(title, ensure_ascii=False)}'
        try:
            subprocess.run(
                ["osascript", "-e", script],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            self.alerted_keys.add(key)
        except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
            pass

    def _start_process(self, mode: str, reason: str) -> subprocess.Popen[Any] | None:
        args = [
            self.python_executable,
            str(self.script_path),
            f"--{mode}",
            "--config",
            str(self.config_path),
        ]
        if self.config.get("dry_run"):
            args.append("--dry-run")
        try:
            return self.popen(args)
        except (OSError, subprocess.SubprocessError) as error:
            self._write_health(f"{mode}_start_failed", reason=reason, error=str(error))
            return None

    def _start_reader(self, reason: str) -> bool:
        self.reader = self._start_process("reader", reason)
        self.worker = self.reader
        self.reader_started_at = self.now() if self.reader else None
        self.worker_started_at = self.reader_started_at
        self.reader_pid = getattr(self.reader, "pid", None) if self.reader else None
        self.reader_instance_id = None
        self._write_health("reader_started" if self.reader else "reader_start_failed", reason=reason)
        return self.reader is not None

    def _start_actions(self, reason: str) -> bool:
        self.actions = self._start_process("actions", reason)
        self.actions_started_at = self.now() if self.actions else None
        self.actions_pid = getattr(self.actions, "pid", None) if self.actions else None
        self.actions_instance_id = None
        self._write_health("actions_started" if self.actions else "actions_start_failed", reason=reason)
        return self.actions is not None

    def _stop_process(self, process: subprocess.Popen[Any] | None, reason: str, mode: str) -> None:
        if process is None or process.poll() is not None:
            return
        try:
            process.terminate()
            process.wait(timeout=2)
        except (OSError, subprocess.TimeoutExpired):
            try:
                process.kill()
                process.wait(timeout=2)
            except (OSError, subprocess.TimeoutExpired):
                pass
        self._write_health(f"{mode}_stopped", reason=reason)

    def _stop_reader(self, reason: str) -> None:
        self._stop_process(self.reader, reason, "reader")
        self.reader = None
        self.worker = None
        self.reader_started_at = None
        self.worker_started_at = None
        self.reader_pid = None
        self.reader_instance_id = None

    def _stop_actions(self, reason: str) -> None:
        self._stop_process(self.actions, reason, "actions")
        self.actions = None
        self.actions_started_at = None
        self.actions_pid = None
        self.actions_instance_id = None

    def _start_worker(self, reason: str) -> bool:
        """Compatibility entry: a worker restart means reader restart."""
        return self._start_reader(reason)

    def _stop_worker(self, reason: str) -> None:
        self._stop_reader(reason)

    def _can_restart(self) -> bool:
        now = self.now()
        window = int(self.config["watchdog_restart_window_seconds"])
        self.restart_times = [at for at in self.restart_times if now - at < window]
        if self.suppressed_until is not None:
            if now < self.suppressed_until:
                return False
            self.suppressed_until = None
            self.restart_times = []
        if len(self.restart_times) >= int(self.config["watchdog_max_restarts"]):
            self.suppressed_until = now + int(self.config["watchdog_restart_cooldown_seconds"])
            self._write_health("restart_suppressed", reason="重启次数达到上限")
            return False
        self.restart_times.append(now)
        self._write_health("reader_restart_allowed")
        return True

    def _reader_is_stale(self) -> tuple[bool, str]:
        health = load_json(self._reader_health_file(), {})
        now = self.now()
        grace = 3 * int(self.config["heartbeat_interval_seconds"])
        started = self.reader_started_at or now
        expected_pid = self.reader_pid
        expected_instance = self.reader_instance_id
        if expected_pid is not None and health.get("pid") != expected_pid:
            if now - started > grace:
                return True, "reader 健康属于旧进程"
            return False, "reader 正在启动"
        health_started = timestamp_value(health.get("process_started_at"))
        if expected_pid is not None and health_started is not None and health_started + 1 < started:
            if now - started > grace:
                return True, "reader 健康属于旧进程"
            return False, "reader 正在启动"
        health_instance = health.get("instance_id")
        if expected_instance is not None and health_instance != expected_instance:
            if now - started > grace:
                return True, "reader 健康属于旧实例"
            return False, "reader 正在启动"
        if expected_instance is None and health_instance:
            self.reader_instance_id = str(health_instance)
        heartbeat = health.get("heartbeat_at")
        if heartbeat is None:
            if now - started > grace:
                return True, "reader 未发布心跳"
            return False, "reader 正在启动"
        heartbeat_value = timestamp_value(heartbeat)
        if heartbeat_value is None:
            if now - started > grace:
                return True, "reader 心跳无效"
            return False, "reader 正在启动"
        request_deadline = health.get("request_deadline_at")
        deadline_value = timestamp_value(request_deadline)
        if health.get("request_in_flight"):
            if deadline_value is not None and now > deadline_value:
                return True, "额度读取请求超过总期限"
            # A slow but still bounded request is healthy enough for the
            # supervisor to wait on.  Its heartbeat is intentionally not
            # refreshed from a blocked socket call; the deadline is the
            # authoritative liveness signal while it is in flight.
            return False, "reader 请求进行中"
        age = now - heartbeat_value
        if age > grace:
            return True, f"reader 心跳过期 {age:.1f}s"
        return False, "healthy"

    def _worker_is_stale(self) -> tuple[bool, str]:
        return self._reader_is_stale()

    def _actions_are_stale(self) -> tuple[bool, str]:
        health = load_json(self._actions_health_file(), {})
        now = self.now()
        grace = 3 * int(self.config["heartbeat_interval_seconds"])
        started = self.actions_started_at or now
        if self.actions is not None and health.get("pid") != self.actions_pid:
            if now - started > grace:
                return True, "actions 健康属于旧进程"
            return False, "actions 正在启动"
        health_started = timestamp_value(health.get("process_started_at"))
        if health_started is not None and health_started + 1 < started:
            if now - started > grace:
                return True, "actions 健康属于旧进程"
            return False, "actions 正在启动"
        health_instance = health.get("instance_id")
        if self.actions_instance_id is not None and health_instance != self.actions_instance_id:
            if now - started > grace:
                return True, "actions 健康属于旧实例"
            return False, "actions 正在启动"
        if self.actions_instance_id is None and health_instance:
            self.actions_instance_id = str(health_instance)
        heartbeat = health.get("heartbeat_at")
        if heartbeat is None:
            if now - started > grace:
                return True, "actions 未发布心跳"
            return False, "actions 正在启动"
        heartbeat_value = timestamp_value(heartbeat)
        if heartbeat_value is None:
            if now - started > grace:
                return True, "actions 心跳无效"
            return False, "actions 正在启动"
        age = now - heartbeat_value
        if age > grace:
            return True, f"actions 心跳过期 {age:.1f}s"
        return False, "healthy"

    def _expired_windows(self) -> list[str]:
        health = load_json(self._worker_health_file(), {})
        now = self.now()
        expired: list[str] = []
        for label in ("primary", "secondary"):
            expiry = timestamp_value(health.get(f"{label}_data_expires_at"))
            status = health.get(f"{label}_data_status")
            if status in {"invalid", "expired"} or (expiry is not None and now > expiry):
                expired.append(label)
        return expired

    def run_forever(self) -> None:
        self._start_reader("initial")
        self._start_actions("initial")
        while True:
            reader_reason: str | None = None
            if self.reader is None:
                reader_reason = "reader 未启动"
            elif self.reader.poll() is not None:
                reader_reason = f"reader 已退出，状态码 {self.reader.returncode}"
            else:
                stale, stale_reason = self._reader_is_stale()
                if stale:
                    reader_reason = stale_reason
            if reader_reason is not None:
                if self.reader is not None:
                    self._stop_reader(reader_reason)
                if self._can_restart():
                    self._start_reader(reader_reason)
                else:
                    self._write_health("restart_suppressed", reason=reader_reason)
                    self._alert_once(
                        "restart-suppressed",
                        "Codex 额度检测失效",
                        f"额度读取进程已失效，自动重启已暂缓：{reader_reason}",
                    )
            if reader_reason is None:
                self.alerted_keys.discard("reader-failed")
            else:
                self._alert_once(
                    "reader-failed",
                    "Codex 额度检测失效",
                    f"额度读取进程异常：{reader_reason}",
                )

            # Actions are deliberately checked even when reader recovery is in
            # progress.  A blocked actions process must not hide a reader or
            # watchdog failure, and a reader failure must not suppress the
            # actions alarm.
            actions_reason: str | None = None
            if self.actions is None:
                actions_reason = "actions 未启动"
            elif self.actions.poll() is not None:
                actions_reason = f"actions 已退出，状态码 {self.actions.returncode}"
            else:
                actions_stale, actions_stale_reason = self._actions_are_stale()
                if actions_stale:
                    actions_reason = actions_stale_reason
            if actions_reason is not None:
                self._alert_once(
                    "actions-failed",
                    "Codex 额度动作进程异常",
                    f"动作进程未运行，额度读取仍继续：{actions_reason}",
                )
                self._write_health("actions_failed", reason=actions_reason)
            else:
                self.alerted_keys.discard("actions-failed")

            expired = self._expired_windows()
            if expired:
                for label in ("primary", "secondary"):
                    if label in expired:
                        self._alert_once(
                            f"quota-expired:{label}",
                            "Codex 额度数据过期",
                            f"{label} 额度没有在期限内更新，当前旧值不再视为正常",
                        )
                    else:
                        self.alerted_keys.discard(f"quota-expired:{label}")
            else:
                for label in ("primary", "secondary"):
                    self.alerted_keys.discard(f"quota-expired:{label}")

            if reader_reason is not None or expired:
                self._write_health(
                    "degraded",
                    reader_error=reader_reason,
                    expired_windows=expired,
                    actions_error=actions_reason,
                )
            elif actions_reason is not None:
                self._write_health("actions_degraded", actions_error=actions_reason)
            else:
                self._write_health("healthy")
            self.sleep(float(self.config["health_check_interval_seconds"]))


def print_status(config: dict[str, Any]) -> None:
    state = load_json(pathlib.Path(config["state_file"]).expanduser(), default_state())
    print(json.dumps(state, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="事件驱动的 Codex 本地额度保护器")
    parser.add_argument("--config", required=True, help="JSON 配置文件")
    parser.add_argument("--once", action="store_true", help="只读取并执行一次策略")
    parser.add_argument("--status", action="store_true", help="打印持久化状态后退出")
    parser.add_argument("--dry-run", action="store_true", help="只记录动作，不中断、恢复或消耗重置卡")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--reader", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--actions", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--supervise", action="store_true", help="运行独立额度守护监督进程")
    args = parser.parse_args(argv)
    config_path = pathlib.Path(args.config).expanduser()
    config = merged_config(load_json(config_path, {}))
    if args.dry_run:
        config["dry_run"] = True
    if args.status:
        print_status(config)
        return 0

    if args.worker:
        args.supervise = True

    if args.supervise and not args.reader and not args.actions and not args.once:
        supervisor = QuotaSupervisor(config, config_path)
        try:
            supervisor.run_forever()
        except KeyboardInterrupt:
            supervisor._stop_actions("supervisor 退出")
            supervisor._stop_reader("supervisor 退出")
            return 0
        return 0

    executable = discover_executable(str(config["codex_executable"]))

    if args.reader:
        client = AppServerClient(
            executable,
            str(config["app_server_socket"]),
            request_timeout=float(
                config.get(
                    "reader_quota_read_timeout_seconds",
                    config["quota_read_timeout_seconds"],
                )
            ),
        )
        reader = QuotaReader(client, config)
        try:
            reader.run_forever()
        except KeyboardInterrupt:
            return 0
        finally:
            client.close()
        return 0

    if args.actions:
        client = AppServerClient(
            executable,
            str(config["app_server_socket"]),
            request_timeout=float(config["quota_read_timeout_seconds"]),
        )
        actions = ActionProcess(client, config)
        try:
            actions.run_forever()
        except KeyboardInterrupt:
            return 0
        finally:
            actions.close()
            client.close()
        return 0

    store = StateStore(
        pathlib.Path(config["state_file"]).expanduser(),
        pathlib.Path(config["event_log_file"]).expanduser(),
        persist=not bool(config.get("dry_run")),
    )
    client = AppServerClient(
        executable,
        str(config["app_server_socket"]),
        request_timeout=float(config["quota_read_timeout_seconds"]),
    )
    guard = QuotaGuard(client, config, store)
    client.dynamic_tool_handler = guard._handle_refresh_dynamic_tool
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
        guard.close()
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
