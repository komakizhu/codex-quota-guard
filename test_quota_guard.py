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
    def test_scheduled_refresh_starts_real_conversation_once(self):
        class Client:
            def __init__(self):
                self.started = []

            def call(self, method, params):
                if method == "thread/read":
                    if self.started:
                        return {"thread": {"id": "target", "status": {"type": "idle"}, "turns": [
                            {"id": "refresh-turn", "status": "completed", "result": {"ok": True}}
                        ]}}
                    return {"thread": {"id": "target", "status": {"type": "idle"}}}
                if method == "turn/start":
                    self.started.append(params)
                    return {"turn": {"id": "refresh-turn"}}
                raise AssertionError(method)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            client = Client()
            config = MODULE.merged_config({"notify_desktop": False, "scheduled_refresh_thread_id": "target"})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: 1000)
            store.state["pending_scheduled_refresh"] = {"scheduled_at": 999, "next_retry_at": 1000}
            guard._deliver_pending_refresh()
            guard._deliver_pending_refresh()
            self.assertEqual(len(client.started), 1)
            self.assertEqual(client.started[0]["threadId"], "target")
            self.assertIn("get_usage_limits", client.started[0]["input"][0]["text"])
            self.assertIsNone(store.state["pending_scheduled_refresh"])

    def test_uncertain_refresh_queries_thread_before_confirmation(self):
        class Client:
            def __init__(self):
                self.reads = 0
                self.starts = 0

            def call(self, method, params):
                if method == "thread/read":
                    self.reads += 1
                    return {"thread": {"turns": [{"id": "turn-after-timeout", "createdAt": 1001, "status": "completed", "result": {"ok": True}}]}}
                if method == "turn/start":
                    self.starts += 1
                    raise MODULE.AppServerError("request timed out")
                raise AssertionError(method)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "notify_desktop": False,
                "scheduled_refresh_thread_id": "target",
                "state_file": str(root / "state.json"),
                "event_log_file": str(root / "events.jsonl"),
            })
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            client = Client()
            guard = MODULE.QuotaGuard(client, config, store, now=lambda: 1001)
            store.state["pending_scheduled_refresh"] = {
                "event_id": "target:1000",
                "scheduled_at": 1000,
                "attempted_at": 1000,
                "conversation_attempted": True,
                "status": "uncertain",
                "next_retry_at": 1001,
                "primary_remaining": 80,
                "secondary_remaining": 90,
            }
            guard._deliver_pending_refresh()
            self.assertEqual(client.starts, 0)
            self.assertEqual(client.reads, 2)
            self.assertIsNone(store.state["pending_scheduled_refresh"])
            self.assertTrue(any(
                event["event"] == "scheduled_refresh_conversation_confirmed"
                for event in store.events
            ))

    def test_uncertain_refresh_accepts_iso_timestamp_and_ignores_bad_candidates(self):
        class Client:
            def call(self, method, params):
                if method == "thread/read":
                    return {"thread": {"turns": [
                        {"id": "bad", "createdAt": "not-a-time"},
                        {"id": "turn-after-timeout", "createdAt": "2026-10-07T00:00:01Z"},
                    ]}}
                raise AssertionError(method)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "notify_desktop": False,
                "scheduled_refresh_thread_id": "target",
                "state_file": str(root / "state.json"),
                "event_log_file": str(root / "events.jsonl"),
            })
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(Client(), config, store, now=lambda: 1791331201)
            store.state["pending_scheduled_refresh"] = {
                "event_id": "target:1791331200",
                "scheduled_at": 1791331200,
                "attempted_at": 1791331200,
                "conversation_attempted": True,
                "status": "uncertain",
                "next_retry_at": 1791331201,
            }
            self.assertTrue(guard._confirm_uncertain_refresh(store.state["pending_scheduled_refresh"]))
            self.assertEqual(
                store.state["pending_scheduled_refresh"]["conversation_turn_id"],
                "turn-after-timeout",
            )

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

    def test_invalid_quota_values_are_not_current_data(self):
        invalid = {
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {"usedPercent": float("nan"), "resetsAt": 2000},
                    "secondary": {"usedPercent": True, "resetsAt": 3000},
                }
            }
        }
        limits = MODULE.parse_limits(invalid)
        self.assertIsNone(limits.primary_remaining)
        self.assertIsNone(limits.secondary_remaining)
        self.assertIsNone(MODULE.remaining_percent({"usedPercent": 101}))
        self.assertIsNone(MODULE.remaining_percent({"usedPercent": float("inf")}))

    def test_legacy_zero_fallback_uses_six_hundred_second_guard(self):
        config = MODULE.merged_config({"fallback_poll_seconds": 0})
        self.assertEqual(config["ordinary_check_interval_seconds"], 600)
        self.assertEqual(config["critical_check_interval_seconds"], 5)
        self.assertEqual(config["critical_boundary_percent"], 20)

    def test_legacy_positive_fallback_migrates_to_ordinary_interval(self):
        config = MODULE.merged_config({"fallback_poll_seconds": 3600})
        self.assertEqual(config["ordinary_check_interval_seconds"], 3600)

    def test_quota_gradient_uses_anchors_and_accelerates_monotonically(self):
        config = MODULE.merged_config({})
        expected = {
            100: 600,
            90: 500,
            20: 200,
            19: 90,
            10: 10,
            9.9: 5,
            0: 5,
        }
        for remaining, interval in expected.items():
            self.assertEqual(
                MODULE.quota_gradient_interval(remaining, config), interval
            )

        samples = [100, 90, 68, 20, 19, 15, 10, 9]
        intervals = [MODULE.quota_gradient_interval(value, config) for value in samples]
        self.assertTrue(all(
            previous >= current
            for previous, current in zip(intervals, intervals[1:])
        ))
        self.assertGreater(intervals[2], intervals[3])
        self.assertGreater(intervals[3], intervals[4])
        self.assertGreater(intervals[4], intervals[5])

    def test_quota_gradient_uses_the_lower_valid_window(self):
        class Client:
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"notify_desktop": False})
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(Client(), config, store, now=lambda: 1000)
            guard.handle_limits(MODULE.Limits(80, 20, 2000, 3000, 0, [], {}))
            self.assertEqual(store.state["quota_check_interval_seconds"], 200)
            self.assertEqual(store.state["quota_check_mode"], "gradient")

    def test_quota_deadline_is_persistent_and_not_recomputed_from_now(self):
        class Client:
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            clock = [1000.0]
            config = MODULE.merged_config({
                "notify_desktop": False,
                "scheduled_refresh_enabled": True,
            })
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(Client(), config, store, now=lambda: clock[0])
            guard.next_fixed_refresh_at = 100000
            guard.handle_limits(MODULE.Limits(80, 70, 2000, 3000, 0, [], {}))
            self.assertEqual(store.state["next_quota_check_at"], 1414)
            clock[0] = 1050
            self.assertEqual(guard.next_wait_seconds(), 364)
            guard.store.event("unrelated_event")
            self.assertEqual(guard.next_wait_seconds(), 364)

    def test_critical_window_uses_five_second_deadline_and_missing_data_is_degraded(self):
        class Client:
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            clock = [1000.0]
            config = MODULE.merged_config({
                "notify_desktop": False,
                "health_file": str(root / "health.json"),
            })
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(Client(), config, store, now=lambda: clock[0])
            guard.handle_limits(MODULE.Limits(None, 80, None, 3000, 0, [], {}))
            self.assertEqual(store.state["quota_check_mode"], "critical")
            self.assertEqual(store.state["quota_check_interval_seconds"], 5)
            self.assertEqual(store.state["next_quota_check_at"], 1005)
            self.assertEqual(store.state["primary_data_status"], "invalid")
            self.assertEqual(store.state["secondary_data_status"], "current")

    def test_health_file_records_window_times_and_status_separately(self):
        class Client:
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "notify_desktop": False,
                "health_file": str(root / "health.json"),
            })
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(Client(), config, store, now=lambda: 1000.0)
            guard.handle_limits(MODULE.Limits(80, None, 2000, None, 0, [], {}))
            health = json.loads((root / "health.json").read_text())
            self.assertEqual(health["primary_data_status"], "current")
            self.assertEqual(health["secondary_data_status"], "invalid")
            self.assertEqual(health["primary_last_success_at"], 1000.0)
            self.assertIsNone(health["secondary_last_success_at"])

    def test_supervisor_limits_restart_storms_and_detects_stale_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            clock = [1000.0]
            config = MODULE.merged_config({
                "state_file": str(root / "state.json"),
                "health_file": str(root / "health.json"),
                "watchdog_state_file": str(root / "watchdog.json"),
            })
            supervisor = MODULE.QuotaSupervisor(
                config,
                root / "config.json",
                now=lambda: clock[0],
            )
            self.assertTrue(supervisor._can_restart())
            self.assertTrue(supervisor._can_restart())
            self.assertTrue(supervisor._can_restart())
            self.assertFalse(supervisor._can_restart())
            self.assertIsNotNone(supervisor.suppressed_until)
            (root / "health.json").write_text(json.dumps({"heartbeat_at": 980.0}))
            stale, reason = supervisor._worker_is_stale()
            self.assertTrue(stale)
            self.assertIn("心跳", reason)

    def test_supervisor_does_not_timeout_completed_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            clock = [1000.0]
            config = MODULE.merged_config({
                "state_file": str(root / "state.json"),
                "health_file": str(root / "health.json"),
                "watchdog_state_file": str(root / "watchdog.json"),
            })
            supervisor = MODULE.QuotaSupervisor(
                config,
                root / "config.json",
                now=lambda: clock[0],
            )
            (root / "health.json").write_text(json.dumps({
                "pid": 123,
                "instance_id": "current",
                "heartbeat_at": 999.0,
                "request_in_flight": False,
                "request_started_at": 900.0,
                "request_finished_at": 901.0,
            }))
            supervisor.reader_pid = 123
            supervisor.reader_instance_id = "current"
            supervisor.reader_started_at = 900.0
            stale, reason = supervisor._reader_is_stale()
            self.assertFalse(stale, reason)

    def test_supervisor_rejects_stale_health_from_previous_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "state_file": str(root / "state.json"),
                "health_file": str(root / "health.json"),
            })
            supervisor = MODULE.QuotaSupervisor(config, root / "config.json", now=lambda: 1000.0)
            (root / "health.json").write_text(json.dumps({
                "pid": 12,
                "instance_id": "old",
                "heartbeat_at": 999.0,
                "request_in_flight": False,
            }))
            supervisor.reader_pid = 34
            supervisor.reader_instance_id = "new"
            supervisor.reader_started_at = 999.0
            stale, reason = supervisor._reader_is_stale()
            self.assertFalse(stale, reason)
            self.assertIn("启动", reason)

    def test_supervisor_starts_reader_and_actions_as_separate_processes(self):
        class Process:
            next_pid = 100

            def __init__(self, args):
                self.args = args
                self.pid = Process.next_pid
                Process.next_pid += 1
                self.returncode = None

            def poll(self):
                return self.returncode

            def terminate(self):
                self.returncode = 0

            def wait(self, timeout=None):
                return self.returncode

        processes = []

        def popen(args):
            process = Process(args)
            processes.append(process)
            return process

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({"state_file": str(root / "state.json")})
            supervisor = MODULE.QuotaSupervisor(
                config,
                root / "config.json",
                popen=popen,
            )
            self.assertTrue(supervisor._start_reader("test"))
            self.assertTrue(supervisor._start_actions("test"))
            self.assertEqual(processes[0].args[2], "--reader")
            self.assertEqual(processes[1].args[2], "--actions")
            self.assertNotEqual(processes[0].pid, processes[1].pid)

    def test_client_call_preserves_one_request_deadline(self):
        client = MODULE.AppServerClient("codex", "/tmp/unused.sock", request_timeout=5)
        captured = []
        client.start_if_needed = lambda deadline=None: captured.append(deadline)
        client._write = lambda value, deadline=None: captured.append(deadline)
        client._responses[1] = {"id": 1, "result": {"ok": True}}
        deadline = 1234.5
        result = client.call("test", {}, deadline=deadline)
        self.assertEqual(result, {"ok": True})
        self.assertEqual(captured, [deadline, deadline])

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

    def test_reader_publishes_quota_result_without_running_actions(self):
        class Client:
            def __init__(self):
                self.calls = []

            def call(self, method, params, **kwargs):
                self.calls.append(method)
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": 2000},
                    "secondary": {"usedPercent": 30, "resetsAt": 3000},
                }}}

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "state_file": str(root / "actions.json"),
                "reader_state_file": str(root / "reader.json"),
                "quota_result_file": str(root / "quota-results.jsonl"),
                "health_file": str(root / "reader-health.json"),
                "notify_desktop": False,
            })
            client = Client()
            reader = MODULE.QuotaReader(client, config, now=lambda: 1000.0, monotonic=lambda: 1000.0)
            limits = reader.read_once()
            self.assertEqual(limits.primary_remaining, 80)
            self.assertEqual(client.calls, ["account/rateLimits/read"])
            self.assertTrue((root / "quota-results.jsonl").exists())
            record = json.loads((root / "quota-results.jsonl").read_text().splitlines()[0])
            self.assertEqual(record["sequence"], 1)
            self.assertEqual(record["primary_remaining"], 80)

    def test_reader_invalid_window_publishes_safe_degraded_result(self):
        class Client:
            def call(self, method, params, **kwargs):
                return {"rateLimitsByLimitId": {"codex": {
                    "primary": {"usedPercent": 20, "resetsAt": 2000},
                    "secondary": {"usedPercent": float("nan"), "resetsAt": 3000},
                }}}

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "reader_state_file": str(root / "reader.json"),
                "reader_health_file": str(root / "reader-health.json"),
                "quota_result_file": str(root / "quota-results.jsonl"),
                "notify_desktop": False,
            })
            reader = MODULE.QuotaReader(Client(), config, now=lambda: 1000.0, monotonic=lambda: 1000.0)
            reader.read_once()
            self.assertEqual(reader.state["quota_check_interval_seconds"], 5)
            record = json.loads((root / "quota-results.jsonl").read_text().splitlines()[0])
            self.assertFalse(record["valid"])

    def test_action_process_consumes_reader_result_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            result_file = root / "quota-results.jsonl"
            result_file.write_text(json.dumps({
                "sequence": 1,
                "recorded_at": 1000,
                "primary_remaining": 3,
                "secondary_remaining": 90,
                "primary_resets_at": 2000,
                "secondary_resets_at": 3000,
                "valid": True,
            }) + "\n")
            config = MODULE.merged_config({
                "state_file": str(root / "actions.json"),
                "event_log_file": str(root / "events.jsonl"),
                "quota_result_file": str(result_file),
                "health_file": str(root / "actions-health.json"),
                "notify_desktop": False,
                "force_stop_active_turns": False,
            })
            guard = MODULE.ActionProcess(None, config, now=lambda: 1000.0)
            guard.process_results()
            self.assertEqual(guard.store.state["last_consumed_quota_sequence"], 0)
            self.assertTrue(any(event["event"] == "actions_unavailable" for event in guard.store.events))
            guard.process_results()
            self.assertEqual(guard.store.state["last_consumed_quota_sequence"], 0)

    def test_pending_refresh_events_are_not_overwritten(self):
        class Client:
            def call(self, method, params):
                if method == "thread/read":
                    return {"thread": {"status": {"type": "active"}}}
                raise AssertionError(method)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = MODULE.merged_config({
                "state_file": str(root / "state.json"),
                "event_log_file": str(root / "events.jsonl"),
                "notify_desktop": False,
                "scheduled_refresh_enabled": True,
                "scheduled_refresh_thread_id": "target",
            })
            store = MODULE.StateStore(root / "state.json", root / "events.jsonl")
            guard = MODULE.QuotaGuard(Client(), config, store, now=lambda: 1000000000.0)
            guard.next_fixed_refresh_at = 999999900.0
            guard._process_fixed_refresh(MODULE.Limits(80, 90, 2000, 3000, 0, [], {}))
            first = store.state["pending_scheduled_refresh"]["scheduled_at"]
            store.state["pending_scheduled_refresh"]["status"] = "uncertain"
            guard.next_fixed_refresh_at = 999999901.0
            guard._process_fixed_refresh(MODULE.Limits(70, 90, 2000, 3000, 0, [], {}))
            pending = store.state["pending_scheduled_refresh"]
            self.assertEqual(pending["scheduled_at"], first)
            self.assertEqual(pending["status"], "uncertain")
            self.assertEqual(len(store.state["pending_scheduled_refresh_events"]), 2)

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
                if len(waits) <= 13:
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
            self.assertEqual(waits, [5] * 14)
            events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
            self.assertEqual(sum(e["event"] == "scheduled_refresh_completed" for e in events), 1)
            self.assertEqual(sum(e.get("title") == "Codex 定时额度提醒" for e in events), 1)
            guard.run_once()
            events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
            self.assertEqual(sum(e.get("title") == "Codex 定时额度提醒" for e in events), 1)


if __name__ == "__main__":
    unittest.main()
