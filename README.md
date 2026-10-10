# Codex Quota Guard

这是一个本地、低开销的 Codex 额度保护小程序。额度读取和定时刷新继续使用本机 Codex 管理的 app-server 控制 socket（默认 `~/.codex/app-server-control/app-server-control.sock`）；桌面任务状态和控制则通过独立的桌面 IPC（默认 `~/.codex/ipc/ipc.sock`）按 allow-list 读取。读取额度或订阅桌面状态不会启动模型，也不会消耗 Codex 的模型额度。

额度检测有三层护栏：额度事件到达时立即读取；事件丢失时，按两个窗口中较低的有效余量使用梯度间隔本地读取；独立监督进程每 5 秒检查读取进程、动作进程、心跳、读取请求期限和两个窗口的数据期限。默认曲线锚点为：100% 读一次后等待 600 秒、90% 等待 500 秒、20% 等待 200 秒、19% 等待 90 秒、10% 等待 10 秒，低于 10% 固定每 5 秒读取。独立读取进程的额度 RPC 总超时默认 30 秒，以覆盖真实 managed app-server 的慢响应；因此事件丢失时的最大检测延迟约为：100% 时 630 秒、90% 时 530 秒、20% 时 230 秒、19% 时 120 秒、10% 时 40 秒、低于 10% 时 35 秒。任一窗口数据无效时使用 5 秒安全间隔。兜底读取只调用本地额度接口，不启动模型 turn。

默认策略是：5 小时余量低于 `primary_warning_percent` 时，只有在 `desktop_control_mode=control` 且目标在线程 allow-list 中时，才依据桌面 IPC 的新鲜活动 turn执行原生控制。`desktop_control_task_mode=turn_only` 会用精确 `turnId` 停止当前普通 turn，并在额度恢复后通过 `thread-follower-start-turn` 启动一个新的普通 turn；两步都必须查询桌面快照验证，不能把新 turn 当作 Goal 恢复。`desktop_control_task_mode=goal` 仍要求独立后台可调用的原生 Goal 暂停/恢复入口；当前适配器明确不把普通 turn、普通消息、归档或 handoff 当作 Goal 恢复，因此入口缺失时保持恢复记录和受限状态。总额度低于 `secondary_warning_percent` 时，若有可用重置卡，调用 Codex 原生 `account/rateLimitResetCredit/consume`；没有可用卡或调用失败会写入事件日志并弹出通知，不会假装成功。

生产环境的 `desktop_control_mode` 默认是 `disabled`，`desktop_control_thread_ids` 默认为空，写操作还必须显式启用。`desktop_control_task_mode` 默认是 `turn_only`；只有显式改为 `goal` 才会走 Goal 控制。旧的 `desktop_control_verified` 只为兼容读取，不能作为能力证明。额度 app-server 能读到不等于它连接的是桌面 Codex 的同一任务视图；桌面适配器会先做 owner discovery，再订阅 canonical history，遇到空响应、版本断档、归属变化或过期快照就拒绝控制。可用 `python3 codex_quota_guard.py --config <config> --desktop-check --desktop-thread-id <thread-id>` 做只读检查；该命令不会发送 turn。安装脚本会按 `turn_only` 或 `goal` 模式分别核对所需能力。

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

旧配置中的 `fallback_poll_seconds` 仍可读取：旧值为 0 时迁移到梯度上限 600 秒，旧的正值作为梯度上限并限制在 60–3600 秒。新配置可调整 `ordinary_check_interval_seconds`（60–3600，额度充足时上限）、`critical_check_interval_seconds`（1–60，低于 10% 时安全间隔，且不大于上限）和 `critical_boundary_percent`（1–100，梯度加速起点；默认 20）。普通动作请求仍使用 5 秒预算；独立额度读取和定时预取使用 5–30 秒预算，默认 30 秒。中间梯度按默认锚点在这些上限/下限之间插值。缺失、空响应、NaN、无穷值或越界值不会刷新对应窗口的成功时间；旧值会保留用于诊断，但会标记为过期/无效。

## 后台启动

定时刷新默认使用 `local_app_server_bridge`：到点后守护程序通过本机 Codex managed app-server 创建或复用一个自己管理的持久会话，并在 `thread/start` 时注册本地动态 `get_usage_limits` 工具，再实际提交一条用户消息。会话会真实调用该工具，额度由守护程序通过 `account/rateLimits/read` 返回；它不依赖会话自身的 MCP，也不会把系统通知当作会话已发送。由于动态工具属于 app-server experimental capability，客户端会在 `initialize` 中显式声明 `experimentalApi`。`scheduled_refresh_cwd` 可指定新会话的工作目录，留空时使用守护程序脚本所在目录；`scheduled_refresh_thread_id` 只作为显式 `local_app_server` 模式的目标。旧配置的 `codex_automation` 会在内存中自动迁移到 bridge，不再只记录“已委托”而不发送消息，也不会调用独立 automation。

提交记录包含 thread ID、turn ID 和受理/完成状态。定时事件会先通过独立本地连接预取并验证当前额度快照，再让动态工具在 Codex 会话的短响应窗口内即时返回；额度快照或 `get_usage_limits` 成功证据缺失时，不能只凭 turn completed 标记刷新成功。当前 managed app-server 若不支持 `thread/read(includeTurns=true)`，守护程序会退回到不带 turns 的线程状态，并使用 `turn/completed` 通知继续查询；没有成功工具证据前不会标记定时会话成功。预取默认最多等待 30 秒，期间不启动模型 turn。该会话会使用少量模型额度，桌面通知只是附加提醒。此设置不会调用 bank reset。

`LaunchAgent.template.plist` 是模板；需要启用后台时运行 `./install_quota_guard.sh`。脚本会使用当前 `python3` 的绝对路径，备份旧 LaunchAgent、配置、状态和已安装 APP，再用 `--supervise` 加载并确认 `reader-health.json`、`actions-health.json`、`watchdog.json` 的 PID、实例、配置版本和新鲜心跳；若确认超时，会卸载本次新服务并恢复备份的 LaunchAgent 与 APP。安装前还会核对当前状态中的 owner discovery、状态订阅、turn 中止、Goal 暂停和 Goal 恢复证据；任何缺失都拒绝部署，不读取旧布尔开关冒充通过。模板启动的是 `--supervise`：监督进程只管理自己创建的读取进程和动作进程；读取进程 10 分钟内最多自动重启 3 次，随后暂停重启 10 分钟，避免重启风暴。动作进程异常只报警并保留未核对动作，不自动重放。若只想暂时运行，可直接使用 `python3 codex_quota_guard.py --config config.json --worker`；`--worker` 只是兼容入口，会映射到完整监督结构。

监督入口会分别启动 `--reader`、`--actions` 两个子进程。读取进程写入 `reader-health.json`、`reader-state.json` 和只追加的 `quota-results.jsonl`；动作进程写入 `actions-health.json`、现有 `state.json`；监督进程写入 `watchdog.json`。动作进程消费结果前会检查两个窗口的数据期限；过期或缺少期限的历史结果只记录并推进序号，不会触发暂停、恢复、定时会话或 bank reset。设置界面分别显示读取、动作和监督状态，并显示刷新时间、两个额度窗口的最后成功时间与过期状态；旧 `health.json` 只作为诊断提示，不能代表新版三层后台已运行。动作失败或接口持续不可用不会被旧额度掩盖，也不会把额度检测成功宣称为任务暂停成功。

程序不会归档、handoff、删除线程，也不会给没有进行中 turn 的 idle/notLoaded 线程发送停止指令；如果控制 socket 返回的线程视图没有可验证的活动 turn，程序会明确记录“未暂停任何任务”，不会把额度周期误报为已处理。所有自动操作均写入事件日志。若要保守验证配置，可先用 `--dry-run` 运行一轮。
