# Codex Quota Guard

这是一个本地、低开销的 Codex 额度保护小程序。它连接本机 Codex 管理的 app-server 控制 socket（默认 `~/.codex/app-server-control/app-server-control.sock`），而不是另起一个隔离的 app-server；读取额度不会启动模型，也不会消耗 Codex 的模型额度。

额度检测有三层护栏：额度事件到达时立即读取；事件丢失时，按两个窗口中较低的有效余量使用梯度间隔本地读取；独立监督进程每 5 秒检查读取进程、动作进程、心跳、读取请求期限和两个窗口的数据期限。默认曲线锚点为：100% 读一次后等待 600 秒、90% 等待 500 秒、20% 等待 200 秒、19% 等待 90 秒、10% 等待 10 秒，低于 10% 固定每 5 秒读取。一次读取超时默认 5 秒，因此事件丢失时的最大检测延迟约为：100% 时 605 秒、90% 时 505 秒、20% 时 205 秒、19% 时 95 秒、10% 时 15 秒、低于 10% 时 10 秒。任一窗口数据无效时使用 5 秒安全间隔。兜底读取只调用本地额度接口，不启动模型 turn。

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

旧配置中的 `fallback_poll_seconds` 仍可读取：旧值为 0 时迁移到梯度上限 600 秒，旧的正值作为梯度上限并限制在 60–3600 秒。新配置可调整 `ordinary_check_interval_seconds`（60–3600，额度充足时上限）、`critical_check_interval_seconds`（1–60，低于 10% 时安全间隔，且不大于上限）和 `critical_boundary_percent`（1–100，梯度加速起点；默认 20），读取总超时默认为 5 秒。中间梯度按默认锚点在这些上限/下限之间插值。缺失、空响应、NaN、无穷值或越界值不会刷新对应窗口的成功时间；旧值会保留用于诊断，但会标记为过期/无效。

## 后台启动

`scheduled_refresh_thread_id` 指定定时消息的目标 Codex 对话。配置后，到点会发送真实会话请求，让 Codex 调用 `get_usage_limits` 并回复五小时额度。正在执行的对话会等到空闲再发送；请求超时或返回不明确时记录待确认，避免重复启动。该会话会使用少量模型额度，桌面通知作为附加提醒。此设置不会调用 bank reset。

`LaunchAgent.template.plist` 是模板，先把其中的 `APP_DIR` 和 `USER_HOME` 替换为实际绝对路径，再复制到 `~/Library/LaunchAgents/` 并用 `launchctl bootstrap` 加载。模板启动的是 `--supervise`：监督进程只管理自己创建的读取进程和动作进程；读取进程 10 分钟内最多自动重启 3 次，随后暂停重启 10 分钟，避免重启风暴。动作进程异常只报警并保留未核对动作，不自动重放。这里不自动安装，避免未经确认改变用户的登录项。若只想暂时运行，可直接使用 `python3 codex_quota_guard.py --config config.json --worker`；`--worker` 只是兼容入口，会映射到完整监督结构。

监督入口会分别启动 `--reader`、`--actions` 两个子进程。读取进程写入 `reader-health.json`、`reader-state.json` 和只追加的 `quota-results.jsonl`；动作进程写入 `actions-health.json`、现有 `state.json`；监督进程写入 `watchdog.json`。设置界面分别显示读取、动作和监督状态，并显示两个额度窗口的最后成功时间与过期状态。动作失败或接口持续不可用不会被旧额度掩盖，也不会把额度检测成功宣称为任务暂停成功。

程序不会归档、handoff、删除线程，也不会给没有进行中 turn 的 idle/notLoaded 线程发送停止指令；如果控制 socket 返回的线程视图没有可验证的活动 turn，程序会明确记录“未暂停任何任务”，不会把额度周期误报为已处理。所有自动操作均写入事件日志。若要保守验证配置，可先用 `--dry-run` 运行一轮。
