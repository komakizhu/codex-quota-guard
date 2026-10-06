# Codex Quota Guard

这是一个本地、低开销的 Codex 额度保护小程序。它连接本机 Codex 管理的 app-server 控制 socket（默认 `~/.codex/app-server-control/app-server-control.sock`），而不是另起一个隔离的 app-server；读取额度不会启动模型，也不会消耗 Codex 的模型额度。它默认事件驱动，收到额度变化通知时才读取，平时睡到接口返回的 `primary.resetsAt` 加一分钟，不做每 5 分钟轮询。

默认策略是：5 小时余量低于 `primary_warning_percent` 时，枚举未归档且确实处于 active 的任务，对每个当前进行中的 turn 调用原生 `turn/interrupt`，并读取线程确认已经停止；重置时间到达后再等待 `post_reset_delay_seconds`，向本轮成功停止的线程发送配置中的继续消息，并再次确认进入进行中状态。总额度低于 `secondary_warning_percent` 时，若有可用重置卡，调用 Codex 原生 `account/rateLimitResetCredit/consume`；没有可用卡或调用失败会写入事件日志并弹出通知，不会假装成功。

## 使用

1. 复制配置并按需要修改阈值、延迟和目标线程消息：

   ```bash
   cp config.example.json config.json
   ```

2. 先做一次实际读取验证。需要只观察、不执行停止/恢复/重置时，加 `--dry-run`：

   ```bash
   python3 codex_quota_guard.py --config config.json --once --dry-run
   ```

3. 常驻运行：

   ```bash
   python3 codex_quota_guard.py --config config.json
   ```

   退出后，状态在 `state_file`，事件在 `event_log_file`。阈值直接编辑 `config.json`，重启程序即可生效。

也可以直接打开同目录下的 `CodexQuotaGuardSettings.app`。这是原生 macOS 设置窗口，不是网页前端；配置统一保存在 `~/Library/Application Support/CodexQuotaGuard/config.json`，首次打开会从旧位置迁移合法配置。保存后会异步重启后台守护程序，并等待后台确认已经读取新配置。界面可以调整：5 小时强制停止阈值、重置后延迟、总额度 bank reset 阈值，以及自动恢复、自动 bank reset 两个开关。应用使用白底黑色 C 图标，并提供状态栏图标和 Dock 图标的独立显示开关；默认显示两个入口，保存按钮只有在有未保存修改时才可用，保存后会显示“已保存”。

“每日固定刷新时刻”默认固定开启。基础时刻为 `7:00、12:00、17:00、22:00`；设置界面只提供一个整体小时偏移量：每按一次 `+1`，四个时刻全部加一小时；每按一次 `−1`，四个时刻全部减一小时。另有“每个时点递增延迟”：默认 60 秒，实际时刻为 `7:00、12:01、17:02、22:03`，而不是四个时点统一延迟。它只做附加读取，不会把固定时刻当成真实重置时间，也不会覆盖接口返回的 `resetsAt`。电脑休眠或程序退出导致错过多个时点时，恢复后会合并补发一次提醒。

`fallback_poll_seconds` 默认是 `0`，表示没有固定轮询；额度变化依靠 app-server 的通知，重置检查依靠接口返回的时间戳。如果希望在某些版本的 app-server 不发通知时增加保险检查，可把它设成例如 `3600`，只会每小时本地读取一次，并不启动模型 turn。

## 后台启动

`scheduled_refresh_thread_id` 指定定时消息的目标 Codex 对话。配置后，到点会发送真实会话请求，让 Codex 调用 `get_usage_limits` 并回复五小时额度。正在执行的对话会等到空闲再发送；请求超时或返回不明确时记录待确认，避免重复启动。该会话会使用少量模型额度，桌面通知作为附加提醒。此设置不会调用 bank reset。

`LaunchAgent.template.plist` 是模板，先把其中的 `APP_DIR` 和 `USER_HOME` 替换为实际绝对路径，再复制到 `~/Library/LaunchAgents/` 并用 `launchctl bootstrap` 加载。这里不自动安装，避免未经确认改变用户的登录项。若只想暂时运行，直接使用上面的常驻命令即可。

程序不会归档、handoff、删除线程，也不会给没有进行中 turn 的 idle/notLoaded 线程发送停止指令；如果控制 socket 返回的线程视图没有可验证的活动 turn，程序会明确记录“未暂停任何任务”，不会把额度周期误报为已处理。所有自动操作均写入事件日志。若要保守验证配置，可先用 `--dry-run` 运行一轮。
