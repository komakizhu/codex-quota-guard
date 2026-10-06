import importlib.util
import json
import pathlib
import queue
import sys
import tempfile
import threading
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo


SOURCE = pathlib.Path(__file__).with_name("codex_quota_guard.py")
SPEC = importlib.util.spec_from_file_location("codex_quota_guard", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class QuotaPolicyTests(unittest.TestCase):
    def test_selects_codex_limit_and_computes_remaining(self):
        snapshot = {
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {"usedPercent": 97, "resetsAt": 2000},
                    "secondary": {"usedPercent": 98, "resetsAt": 3000},
                }
            }
        }
        limits = MODULE.parse_limits(snapshot)
        self.assertEqual(limits.primary_remaining, 3)
        self.assertEqual(limits.secondary_remaining, 2)
        self.assertEqual(limits.primary_resets_at, 2000)

    def test_due_after_reset_delay_uses_interface_timestamp(self):
        self.assertFalse(MODULE.reset_due(now=2059, reset_at=2000, delay=60))
        self.assertTrue(MODULE.reset_due(now=2060, reset_at=2000, delay=60))

    def test_active_thread_requires_in_progress_turn(self):
        active = {"status": {"type": "active"}}
        self.assertTrue(MODULE.is_candidate_thread(active))
        self.assertFalse(MODULE.is_candidate_thread({"status": {"type": "idle"}}))
        self.assertFalse(MODULE.is_candidate_thread({"status": {"type": "notLoaded"}}))
        self.assertEqual(
            MODULE.in_progress_turn_id({"turns": [{"id": "old", "status": "completed"}]}),
            None,
        )
        self.assertEqual(
            MODULE.in_progress_turn_id({"turns": [{"id": "new", "status": "inProgress"}]}),
            "new",
        )

    def test_state_round_trip_is_json_safe(self):
        state = MODULE.default_state()
        state["paused_threads"]["thread-1"] = {"turn_id": "turn-1", "reset_at": 2000}
        encoded = json.dumps(state)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["paused_threads"]["thread-1"]["turn_id"], "turn-1")

    def test_same_active_turn_is_deduplicated_but_new_turn_is_scanned(self):
        class FakeClient:
            def __init__(self):
                self.current_turn = "turn-1"
                self.running = True
                self.interrupts = []

            def call(self, method, params):
                if method == "thread/list":
                    return {"data": [{"id": "thread-1", "status": {"type": "active"}}]}
                if method == "thread/read":
                    return {"thread": self.read_thread(params["threadId"])}
                if method == "turn/interrupt":
                    self.interrupts.append(params["turnId"])
                    self.running = False
                    return {}
                raise AssertionError(method)

            def read_thread(self, thread_id):
                turns = [{"id": self.current_turn, "status": "inProgress"}] if self.running else []
                return {
                    "id": thread_id,
                    "name": "worker",
                    "status": {"type": "active" if self.running else "idle"},
                    "turns": turns,
                }

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config(
                {
                    "notify_desktop": False,
                    "state_file": str(root / "state.json"),
                    "event_log_file": str(root / "events.jsonl"),
                }
            )
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            client = FakeClient()
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: 1000)
            guard.consume_reset_credit = lambda limits: None
            limits = MODULE.Limits(3, 50, 2000, 3000, 0, [], {})

            guard.handle_limits(limits)
            guard.handle_limits(limits)
            client.running = True
            client.current_turn = "turn-2"
            guard.handle_limits(limits)

            self.assertEqual(client.interrupts, ["turn-1", "turn-2"])
            self.assertEqual(store.state["pause_episode_primary_resets_at"], 2000)

    def test_resume_uses_recorded_reset_time_not_new_window_timestamp(self):
        class FakeClient:
            def __init__(self):
                self.started = []
                self.active = False

            def call(self, method, params):
                if method == "thread/read":
                    return {"thread": self.read_thread(params["threadId"])}
                if method == "turn/start":
                    self.started.append(params)
                    self.active = True
                    return {"turn": {"id": "new-turn"}}
                raise AssertionError(method)

            def read_thread(self, thread_id):
                return {
                    "id": thread_id,
                    "status": {"type": "active" if self.active else "idle"},
                    "turns": [{"id": "new-turn", "status": "inProgress"}] if self.active else [],
                }

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            store.state["paused_threads"] = {
                "thread-1": {"thread_id": "thread-1", "title": "worker", "reset_at": 2000}
            }
            clock = [2030.0]
            client = FakeClient()
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: clock[0])
            high = MODULE.Limits(50, 50, 20000, 3000, 0, [], {})
            guard.handle_limits(high)
            self.assertEqual(client.started, [])
            clock[0] = 2061
            guard.handle_limits(high)
            self.assertEqual(len(client.started), 1)
            self.assertEqual(store.state["paused_threads"], {})

    def test_empty_thread_response_cannot_verify_forced_stop(self):
        class FakeClient:
            def __init__(self):
                self.interrupted = False

            def list_threads(self):
                return [{"id": "thread-1", "status": {"type": "active"}}]

            def read_thread(self, thread_id):
                if self.interrupted:
                    return {}
                return {
                    "id": thread_id,
                    "status": {"type": "active"},
                    "turns": [{"id": "turn-1", "status": "inProgress"}],
                }

            def call(self, method, params):
                if method == "thread/list":
                    return {"data": [{"id": "thread-1", "status": {"type": "active"}}]}
                if method == "thread/read":
                    return {"thread": self.read_thread(params["threadId"])}
                self.interrupted = True
                return {}

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            client = FakeClient()
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: 1000)
            guard.force_stop_active_threads(2000)
            self.assertEqual(store.state["paused_threads"], {})
            self.assertTrue(any(e["event"] == "interrupt_verification_failed" for e in store.events))

    def test_dry_run_does_not_write_state_or_consume_credit(self):
        class FakeClient:
            def __init__(self, payload):
                self.payload = payload
                self.calls = []

            def call(self, method, params):
                self.calls.append(method)
                return self.payload

        payload = {
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {"usedPercent": 10, "resetsAt": 2000},
                    "secondary": {"usedPercent": 99, "resetsAt": 3000},
                }
            },
            "rateLimitResetCredits": {
                "availableCount": 1,
                "credits": [{"id": "card-1", "status": "available"}],
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"dry_run": True, "auto_consume_reset_credit": True})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl", persist=False)
            client = FakeClient(payload)
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: 1000)
            guard.consume_reset_credit(MODULE.parse_limits(payload))
            self.assertIsNone(store.state["reset_attempted_for_credit_id"])
            self.assertEqual(client.calls, [])
            self.assertFalse((root / "state.json").exists())

    def test_reset_success_requires_credit_and_quota_change(self):
        before = {
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {"usedPercent": 10, "resetsAt": 2000},
                    "secondary": {"usedPercent": 99, "resetsAt": 3000},
                }
            },
            "rateLimitResetCredits": {
                "availableCount": 1,
                "credits": [{"id": "card-1", "status": "available"}],
            },
        }

        class FakeClient:
            def __init__(self):
                self.calls = []

            def call(self, method, params):
                self.calls.append((method, params))
                if method == "account/rateLimitResetCredit/consume":
                    return {}
                return before

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "notify_desktop": False,
                "auto_consume_reset_credit": True,
            })
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            client = FakeClient()
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: 1000)
            guard.consume_reset_credit(MODULE.parse_limits(before))
            events = [e["event"] for e in store.events]
            self.assertIn("reset_credit_pending_verification", events)
            self.assertNotIn("reset_credit_consumed_and_verified", events)

    def test_due_schedule_is_preserved_across_guard_restart(self):
        timezone = ZoneInfo("Asia/Shanghai")
        due = datetime(2026, 10, 6, 7, 1, tzinfo=timezone).timestamp()
        now = datetime(2026, 10, 6, 7, 2, tzinfo=timezone).timestamp()

        class FakeClient:
            def call(self, method, params):
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": int(now + 3600)},
                    "secondary": {"usedPercent": 30, "resetsAt": int(now + 86400)},
                }}}

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            store.state["next_fixed_refresh_at"] = due
            guard = MODULE.QuotaGuard(FakeClient(), config, store, now=lambda: now)
            self.assertEqual(guard.next_fixed_refresh_at, due)
            guard.run_once()
            self.assertTrue(any(e["event"] == "scheduled_refresh_completed" for e in store.events))

    def test_old_uniform_offset_signature_is_recomputed(self):
        timezone = ZoneInfo("Asia/Shanghai")
        now = datetime(2026, 10, 6, 6, 59, tzinfo=timezone).timestamp()
        old_signature = json.dumps(
            {
                "hours": [7, 12, 17, 22],
                "offset": 60,
                "timezone": "Asia/Shanghai",
            },
            sort_keys=True,
            separators=(",", ":"),
        )

        class FakeClient:
            def call(self, method, params):
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": int(now + 3600)},
                    "secondary": {"usedPercent": 30, "resetsAt": int(now + 86400)},
                }}}

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            store.state["next_fixed_refresh_at"] = datetime(
                2026, 10, 6, 7, 1, tzinfo=timezone
            ).timestamp()
            store.state["fixed_refresh_schedule_signature"] = old_signature
            guard = MODULE.QuotaGuard(FakeClient(), config, store, now=lambda: now)
            self.assertEqual(
                guard.next_fixed_refresh_at,
                datetime(2026, 10, 6, 7, 0, tzinfo=timezone).timestamp(),
            )

    def test_missed_fixed_slots_are_merged_into_one_notification(self):
        timezone = ZoneInfo("Asia/Shanghai")
        due = datetime(2026, 10, 6, 7, 1, tzinfo=timezone).timestamp()
        now = datetime(2026, 10, 6, 23, 0, tzinfo=timezone).timestamp()

        class FakeClient:
            def call(self, method, params):
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": int(now + 3600)},
                    "secondary": {"usedPercent": 30, "resetsAt": int(now + 86400)},
                }}}

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            store.state["next_fixed_refresh_at"] = due
            guard = MODULE.QuotaGuard(FakeClient(), config, store, now=lambda: now)
            guard.run_once()
            completed = [e for e in store.events if e["event"] == "scheduled_refresh_completed"]
            notifications = [e for e in store.events if e["event"] == "notification"]
            self.assertEqual(len(completed), 1)
            self.assertEqual(completed[0]["missed_count"], 3)
            self.assertEqual(len(notifications), 1)
            self.assertIn("合并补发", notifications[0]["message"])

    def test_failed_schedule_notification_remains_pending_for_retry(self):
        timezone = ZoneInfo("Asia/Shanghai")
        due = datetime(2026, 10, 6, 7, 1, tzinfo=timezone).timestamp()
        clock = [datetime(2026, 10, 6, 7, 2, tzinfo=timezone).timestamp()]

        class FakeClient:
            def call(self, method, params):
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": int(clock[0] + 3600)},
                    "secondary": {"usedPercent": 30, "resetsAt": int(clock[0] + 86400)},
                }}}

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            store.state["next_fixed_refresh_at"] = due
            guard = MODULE.QuotaGuard(FakeClient(), config, store, now=lambda: clock[0])
            guard.notify = lambda title, message: False
            guard.run_once()
            self.assertIsNotNone(store.state["pending_scheduled_refresh"])
            self.assertTrue(any(e["event"] == "scheduled_refresh_notification_retry_scheduled" for e in store.events))
            guard.notify = lambda title, message: True
            clock[0] += 2
            guard.run_once()
            self.assertIsNone(store.state["pending_scheduled_refresh"])
            self.assertTrue(any(e["event"] == "scheduled_refresh_notification_delivered" for e in store.events))

    def test_notification_wait_fails_fast_on_disconnected_client(self):
        client = MODULE.AppServerClient.__new__(MODULE.AppServerClient)
        client._notifications = queue.Queue()
        client._condition = threading.Condition()
        client._connection_error = RuntimeError("socket closed")
        client.start_if_needed = lambda: None
        with self.assertRaises(MODULE.AppServerError):
            client.next_notification(60)

    def test_run_forever_reconnects_with_backoff_after_disconnect(self):
        timezone = ZoneInfo("Asia/Shanghai")
        now = datetime(2026, 10, 6, 6, 0, tzinfo=timezone).timestamp()

        class Finished(Exception):
            pass

        class FakeClient:
            def __init__(self):
                self.reads = 0
                self.notifications = 0
                self.closed = 0

            def call(self, method, params):
                self.reads += 1
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": int(now + 3600)},
                    "secondary": {"usedPercent": 30, "resetsAt": int(now + 86400)},
                }}}

            def next_notification(self, timeout):
                self.notifications += 1
                if self.notifications == 1:
                    raise MODULE.AppServerError("socket closed")
                raise Finished()

            def close(self):
                self.closed += 1

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            client = FakeClient()
            sleeps = []
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: now, sleep=sleeps.append)
            with self.assertRaises(Finished):
                guard.run_forever()
            self.assertEqual(client.reads, 2)
            self.assertEqual(client.closed, 1)
            self.assertEqual(sleeps, [1])
            self.assertTrue(any(e["event"] == "connection_error" for e in store.events))

    def test_low_quota_without_verified_stop_is_not_reported_as_handled(self):
        class FakeClient:
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config(
                {
                    "notify_desktop": False,
                    "state_file": str(root / "state.json"),
                    "event_log_file": str(root / "events.jsonl"),
                }
            )
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(FakeClient(), config, store, now=lambda: 1000)
            guard.force_stop_active_threads = lambda reset_at: 0
            guard.consume_reset_credit = lambda limits: None

            guard.handle_limits(MODULE.Limits(3, 50, 2000, 3000, 0, [], {}))

            events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
            self.assertTrue(any(event["event"] == "pause_cycle_no_verified_stops" for event in events))
            self.assertFalse(any(event["event"] == "forced_stop_verified" for event in events))

    def test_thresholds_and_actions_are_runtime_configurable(self):
        config = MODULE.merged_config(
            {
                "primary_warning_percent": 8,
                "secondary_warning_percent": 1.5,
                "post_reset_delay_seconds": 90,
                "force_stop_active_turns": False,
                "auto_consume_reset_credit": False,
            }
        )
        self.assertEqual(config["primary_warning_percent"], 8.0)
        self.assertEqual(config["secondary_warning_percent"], 1.5)
        self.assertEqual(config["post_reset_delay_seconds"], 90)
        self.assertTrue(config["force_stop_active_turns"])
        self.assertFalse(config["auto_consume_reset_credit"])

    def test_default_fixed_refresh_hours_are_seven_twelve_seventeen_twentytwo(self):
        config = MODULE.merged_config({})
        self.assertEqual(config["scheduled_refresh_hours"], [7, 12, 17, 22])
        self.assertEqual(config["scheduled_refresh_hour_shift"], 0)
        shifted = MODULE.merged_config({"scheduled_refresh_hour_shift": 1})
        self.assertEqual(shifted["scheduled_refresh_hours"], [8, 13, 18, 23])
        wrapped = MODULE.merged_config({"scheduled_refresh_hour_shift": 6})
        self.assertEqual(wrapped["scheduled_refresh_hours"], [13, 18, 23, 4])

    def test_optional_fixed_refresh_schedule_is_timezone_aware(self):
        shanghai = ZoneInfo("Asia/Shanghai")
        now = datetime(2026, 10, 6, 5, 30, tzinfo=shanghai).timestamp()
        next_refresh = MODULE.next_scheduled_refresh(
            now, [7, 12, 17, 22], offset_seconds=60, timezone_name="Asia/Shanghai"
        )
        expected = datetime(2026, 10, 6, 7, 0, tzinfo=shanghai).timestamp()
        self.assertEqual(next_refresh, expected)

    def test_fixed_refresh_offset_accumulates_by_five_hour_slot(self):
        shanghai = ZoneInfo("Asia/Shanghai")
        cases = [
            (datetime(2026, 10, 6, 7, 0, 1), datetime(2026, 10, 6, 12, 1)),
            (datetime(2026, 10, 6, 12, 1, 1), datetime(2026, 10, 6, 17, 2)),
            (datetime(2026, 10, 6, 17, 2, 1), datetime(2026, 10, 6, 22, 3)),
            (datetime(2026, 10, 6, 22, 3, 1), datetime(2026, 10, 7, 7, 0)),
        ]
        for now, expected in cases:
            with self.subTest(now=now):
                actual = MODULE.next_scheduled_refresh(
                    now.replace(tzinfo=shanghai).timestamp(),
                    [7, 12, 17, 22],
                    offset_seconds=60,
                    timezone_name="Asia/Shanghai",
                )
                self.assertEqual(actual, expected.replace(tzinfo=shanghai).timestamp())

    def test_shifted_refresh_slots_keep_slot_offsets_across_midnight(self):
        shanghai = ZoneInfo("Asia/Shanghai")
        now = datetime(2026, 10, 6, 23, 2, 1, tzinfo=shanghai).timestamp()
        next_refresh = MODULE.next_scheduled_refresh(
            now,
            [13, 18, 23, 4],
            offset_seconds=60,
            timezone_name="Asia/Shanghai",
            hour_shift=6,
        )
        expected = datetime(2026, 10, 7, 4, 3, tzinfo=shanghai).timestamp()
        self.assertEqual(next_refresh, expected)

    def test_uses_managed_app_server_socket_by_default(self):
        config = MODULE.merged_config({})
        self.assertEqual(
            config["app_server_socket"],
            str(pathlib.Path.home() / ".codex/app-server-control/app-server-control.sock"),
        )

    def test_required_policies_and_bankreset_default(self):
        config = MODULE.merged_config({"scheduled_refresh_enabled": False, "force_stop_active_turns": False})
        self.assertTrue(config["scheduled_refresh_enabled"])
        self.assertTrue(config["force_stop_active_turns"])
        self.assertFalse(config["auto_consume_reset_credit"])

    def test_daily_schedule_all_slots_and_overnight_gap(self):
        timezone = ZoneInfo("Asia/Shanghai")
        for hour, minute, next_hour, next_minute, day in [
            (7, 0, 12, 1, 6),
            (12, 1, 17, 2, 6),
            (17, 2, 22, 3, 6),
            (22, 3, 7, 0, 7),
        ]:
            now = datetime(2026, 10, 6, hour, minute, 1, tzinfo=timezone).timestamp()
            expected = datetime(2026, 10, day, next_hour, next_minute, tzinfo=timezone).timestamp()
            self.assertEqual(MODULE.next_scheduled_refresh(now, [7, 12, 17, 22], 60, "Asia/Shanghai"), expected)

    def test_timer_loop_reads_and_notifies_once_even_with_stale_reset(self):
        timezone = ZoneInfo("Asia/Shanghai")
        start = datetime(2026, 10, 6, 6, 59, tzinfo=timezone).timestamp()
        clock = [start]
        calls = []
        waits = []

        class Finished(Exception):
            pass

        class FakeClient:
            def call(self, method, params):
                calls.append(method)
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": int(start - 120)},
                    "secondary": {"usedPercent": 30, "resetsAt": int(start + 86400)},
                }}}

            def next_notification(self, timeout):
                waits.append(timeout)
                if len(waits) == 1:
                    clock[0] += timeout
                    return None
                raise Finished()

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(FakeClient(), config, store, now=lambda: clock[0])
            with self.assertRaises(Finished):
                guard.run_forever()
            self.assertEqual(calls, ["account/rateLimits/read"] * 2)
            self.assertEqual(waits, [60, 5 * 3600 + 60])
            events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
            self.assertEqual(sum(e["event"] == "scheduled_refresh_completed" for e in events), 1)
            self.assertEqual(sum(e.get("title") == "Codex 定时额度提醒" for e in events), 1)
            guard.run_once()
            events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
            self.assertEqual(sum(e.get("title") == "Codex 定时额度提醒" for e in events), 1)


if __name__ == "__main__":
    unittest.main()
