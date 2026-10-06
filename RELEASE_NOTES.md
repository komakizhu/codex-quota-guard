# Codex Quota Guard 1.0 — Build 12

首次公开可下载版本，适用于 Apple Silicon、macOS 15 或更新版本。

- 本地事件驱动额度监控，默认五小时保护阈值 3%，自动 bank reset 默认关闭。
- 每日刷新时点按槽位递增延迟：7:00、12:01、17:02、22:03，支持整体小时偏移。
- 可通过 `scheduled_refresh_thread_id` 向指定 Codex 对话发送真实五小时额度刷新请求。
- 设置保存反馈、白底黑色 C 图标，以及状态栏和 Dock 图标显示设置。
- 持久化定时事件、错过时点合并提醒、连接断开自动恢复。

解压后将 `CodexQuotaGuardSettings.app` 拖入“应用程序”。后台监控需要 Python 3.10+、已安装并登录的 Codex，以及根据 README 配置的 LaunchAgent；仅打开设置 APP 不会自动安装后台服务。会话目标 ID 需自行配置，公开包不包含个人任务 ID。

APP 使用临时签名，未经过 Apple 公证，首次打开可能需要在系统“隐私与安全性”中允许。
