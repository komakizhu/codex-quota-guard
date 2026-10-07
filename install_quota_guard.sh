#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
USER_HOME="${HOME}"
LABEL="local.codex.quota-guard"
LAUNCH_AGENTS_DIR="$USER_HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS_DIR/$LABEL.plist"
SUPPORT_DIR="$USER_HOME/Library/Application Support/CodexQuotaGuard"
BACKUP_ROOT="$SUPPORT_DIR/backups/$(date +%Y%m%d%H%M%S)"
PYTHON_BIN="$(command -v python3)"
INSTALLED_APP="/Applications/CodexQuotaGuardSettings.app"
ROLLBACK_NEEDED=0
ROLLBACK_DONE=0
NEW_APP_INSTALLED=0

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "找不到可执行的 python3" >&2
    exit 1
fi

mkdir -p "$LAUNCH_AGENTS_DIR" "$SUPPORT_DIR" "$BACKUP_ROOT"

rollback_installation() {
    if [[ "$ROLLBACK_DONE" -eq 1 ]]; then
        return
    fi
    ROLLBACK_DONE=1
    set +e
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true

    if [[ -e "$BACKUP_ROOT/$LABEL.plist" ]]; then
        cp -pR "$BACKUP_ROOT/$LABEL.plist" "$PLIST_PATH"
        launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH" 2>/dev/null || true
        launchctl kickstart -k "gui/$(id -u)/$LABEL" 2>/dev/null || true
    else
        rm -f "$PLIST_PATH"
    fi

    if [[ "$NEW_APP_INSTALLED" -eq 1 && -d "$INSTALLED_APP" ]]; then
        mv "$INSTALLED_APP" "$BACKUP_ROOT/failed-CodexQuotaGuardSettings.app" 2>/dev/null || true
    fi
    if [[ -d "$BACKUP_ROOT/CodexQuotaGuardSettings.app" ]]; then
        mv "$BACKUP_ROOT/CodexQuotaGuardSettings.app" "$INSTALLED_APP" 2>/dev/null || true
    fi
}

on_exit() {
    local status=$?
    if [[ "$status" -ne 0 && "$ROLLBACK_NEEDED" -eq 1 && "$ROLLBACK_DONE" -eq 0 ]]; then
        echo "安装失败，正在恢复旧服务和安装 APP。备份：${BACKUP_ROOT}" >&2
        rollback_installation
    fi
    exit "$status"
}
trap on_exit EXIT

backup_if_present() {
    local source="$1"
    if [[ -e "$source" ]]; then
        cp -pR "$source" "$BACKUP_ROOT/"
    fi
}

backup_if_present "$PLIST_PATH"
backup_if_present "$SUPPORT_DIR/config.json"
backup_if_present "$SUPPORT_DIR/state.json"
backup_if_present "$SUPPORT_DIR/reader-state.json"
backup_if_present "$SUPPORT_DIR/events.jsonl"
backup_if_present "$SUPPORT_DIR/quota-results.jsonl"
backup_if_present "$USER_HOME/Applications/CodexQuotaGuardSettings.app"

ROLLBACK_NEEDED=1
if [[ -d "$INSTALLED_APP" ]]; then
    mv "$INSTALLED_APP" "$BACKUP_ROOT/CodexQuotaGuardSettings.app"
fi
cp -pR "$APP_DIR/CodexQuotaGuardSettings.app" "$INSTALLED_APP"
NEW_APP_INSTALLED=1

launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true

TEMP_PLIST="$BACKUP_ROOT/$LABEL.plist.new"
sed \
    -e "s|/usr/bin/python3|$PYTHON_BIN|g" \
    -e "s|APP_DIR|$APP_DIR|g" \
    -e "s|USER_HOME|$USER_HOME|g" \
    "$APP_DIR/LaunchAgent.template.plist" > "$TEMP_PLIST"
plutil -lint "$TEMP_PLIST"
cp "$TEMP_PLIST" "$PLIST_PATH"

launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

for attempt in {1..120}; do
    if "$PYTHON_BIN" - "$SUPPORT_DIR" "$SUPPORT_DIR/config.json" <<'PY'
import json
import pathlib
import sys
import time

support = pathlib.Path(sys.argv[1])
config_path = pathlib.Path(sys.argv[2])
try:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    revision = config.get("config_revision")
    now = time.time()
    for name in ("reader-health.json", "actions-health.json", "watchdog.json"):
        health = json.loads((support / name).read_text(encoding="utf-8"))
        if not isinstance(health.get("pid"), int) or health["pid"] <= 0:
            raise ValueError(name + ": pid")
        if not health.get("instance_id"):
            raise ValueError(name + ": instance_id")
        if revision is not None and health.get("config_revision") != revision:
            raise ValueError(name + ": config_revision")
        interval = float(health.get("heartbeat_interval_seconds", 5))
        if now - float(health["heartbeat_at"]) > max(15, interval * 3):
            raise ValueError(name + ": stale heartbeat")
except (OSError, ValueError, TypeError, json.JSONDecodeError, KeyError):
    raise SystemExit(1)
PY
    then
        echo "已启用 ${LABEL}；健康文件已确认。备份：${BACKUP_ROOT}"
        exit 0
    fi
    sleep 1
done

echo "监督服务已加载，但未能在 120 秒内确认三个健康文件；正在恢复旧服务和安装 APP。备份：${BACKUP_ROOT}" >&2
rollback_installation
exit 2
