import AppKit
import Foundation
import SwiftUI

private let guardLabel = "local.codex.quota-guard"
private extension Notification.Name {
    static let quotaGuardSettingsChanged = Notification.Name("CodexQuotaGuardSettingsChanged")
}

private enum CLogoImage {
    static func make(size: CGFloat) -> NSImage {
        let image = NSImage(size: NSSize(width: size, height: size))
        image.lockFocus()

        let background = NSBezierPath(
            roundedRect: NSRect(x: 0, y: 0, width: size, height: size),
            xRadius: size * 0.18,
            yRadius: size * 0.18
        )
        NSColor.white.setFill()
        background.fill()

        let path = NSBezierPath()
        path.appendArc(
            withCenter: NSPoint(x: size * 0.5, y: size * 0.5),
            radius: size * 0.29,
            startAngle: 60,
            endAngle: 300,
            clockwise: false
        )
        path.lineWidth = max(2, size * 0.12)
        path.lineCapStyle = .round
        NSColor.black.setStroke()
        path.stroke()

        image.unlockFocus()
        image.isTemplate = false
        return image
    }
}

final class AppDelegate: NSObject, NSApplicationDelegate {
    private var statusItem: NSStatusItem?
    private var settingsObserver: NSObjectProtocol?

    func applicationDidFinishLaunching(_ notification: Notification) {
        settingsObserver = NotificationCenter.default.addObserver(
            forName: .quotaGuardSettingsChanged,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.applyVisibilitySettings()
        }
        applyVisibilitySettings()
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        false
    }

    deinit {
        if let settingsObserver {
            NotificationCenter.default.removeObserver(settingsObserver)
        }
    }

    private func applyVisibilitySettings() {
        let settings = visibilitySettings()
        NSApp.setActivationPolicy(settings.showDockIcon ? .regular : .accessory)
        NSApp.applicationIconImage = CLogoImage.make(size: 128)

        if settings.showStatusIcon {
            installStatusItemIfNeeded()
        } else if let statusItem {
            NSStatusBar.system.removeStatusItem(statusItem)
            self.statusItem = nil
        }
    }

    private func installStatusItemIfNeeded() {
        guard statusItem == nil else { return }
        let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        item.button?.image = CLogoImage.make(size: 18)
        item.button?.imagePosition = .imageOnly
        item.button?.setAccessibilityLabel("Codex 额度守护")
        item.isVisible = true

        let menu = NSMenu()
        let openItem = NSMenuItem(title: "打开设置", action: #selector(openSettings), keyEquivalent: ",")
        openItem.target = self
        menu.addItem(openItem)
        menu.addItem(.separator())
        let quitItem = NSMenuItem(title: "退出", action: #selector(quit), keyEquivalent: "q")
        quitItem.target = self
        menu.addItem(quitItem)
        item.menu = menu
        statusItem = item
    }

    @objc private func openSettings() {
        NSApp.setActivationPolicy(visibilitySettings().showDockIcon ? .regular : .accessory)
        NSApp.activate(ignoringOtherApps: true)
        NSApp.windows.first?.makeKeyAndOrderFront(nil)
    }

    @objc private func quit() {
        NSApp.terminate(nil)
    }

    private func visibilitySettings() -> (showStatusIcon: Bool, showDockIcon: Bool) {
        guard let data = try? Data(contentsOf: SettingsModel.defaultConfigURL()),
              let object = try? JSONSerialization.jsonObject(with: data),
              let config = object as? [String: Any] else {
            return (true, true)
        }
        return (
            config["show_status_bar_icon"] as? Bool ?? true,
            config["show_dock_icon"] as? Bool ?? true
        )
    }
}

final class SettingsModel: ObservableObject {
    private let baseRefreshHours = [7, 12, 17, 22]

    @Published var primaryWarning = "3"
    @Published var secondaryWarning = "1"
    @Published var resetDelay = "60"
    @Published var forceStop = true
    @Published var resumeTasks = true
    @Published var autoBankReset = false
    @Published var ordinaryCheckInterval = "600"
    @Published var criticalCheckInterval = "5"
    @Published var criticalBoundary = "20"
    @Published var showStatusBarIcon = true
    @Published var showDockIcon = true
    @Published var fixedRefreshEnabled = true
    @Published var fixedRefreshHourShift = 0
    @Published var fixedRefreshOffset = "60"
    @Published var statusMessage = ""
    @Published var healthMessage = "检测健康状态尚未读取"
    @Published private var savedFingerprint = ""
    @Published private var feedbackFingerprint: String?
    private var configLoadError: String?

    var displayedStatusMessage: String {
        if feedbackFingerprint == currentFingerprint { return statusMessage }
        return hasUnsavedChanges ? "有未保存的修改" : statusMessage
    }

    var hasUnsavedChanges: Bool {
        savedFingerprint != currentFingerprint
    }

    var shiftedRefreshHours: [Int] {
        let normalized = baseRefreshHours.map { hour in
            ((hour + fixedRefreshHourShift) % 24 + 24) % 24
        }
        return normalized
    }

    var shiftedRefreshHoursText: String {
        shiftedRefreshHours.map(String.init).joined(separator: ",")
    }

    var shiftedRefreshTimesText: String {
        let offset = max(0, Int(fixedRefreshOffset.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0)
        let daySeconds = 24 * 60 * 60
        return baseRefreshHours.enumerated().map { index, baseHour in
            let totalSeconds = baseHour * 60 * 60 + fixedRefreshHourShift * 60 * 60 + index * offset
            let normalized = ((totalSeconds % daySeconds) + daySeconds) % daySeconds
            let hour = normalized / 3600
            let minute = (normalized % 3600) / 60
            let second = normalized % 60
            if second == 0 {
                return String(format: "%d:%02d", hour, minute)
            }
            return String(format: "%d:%02d:%02d", hour, minute, second)
        }.joined(separator: "、")
    }

    let configURL: URL
    private let reloadGuard: (() throws -> Void)?

    init(configURL: URL = SettingsModel.defaultConfigURL(), reloadGuard: (() throws -> Void)? = nil) {
        self.configURL = configURL
        self.reloadGuard = reloadGuard
        load()
    }

    func load() {
        loadHealth()
        configLoadError = nil
        var data: Data
        var migrated = false
        do {
            if !FileManager.default.fileExists(atPath: configURL.path) {
                if configURL == Self.defaultConfigURL(),
                   FileManager.default.fileExists(atPath: Self.legacyConfigURL().path) {
                    data = try Data(contentsOf: Self.legacyConfigURL())
                    migrated = true
                } else {
                    savedFingerprint = currentFingerprint
                    feedbackFingerprint = currentFingerprint
                    statusMessage = "未找到配置文件，将使用默认值"
                    return
                }
            } else {
                data = try Data(contentsOf: configURL)
            }
        } catch {
            configLoadError = "配置文件无法读取：\(error.localizedDescription)"
            savedFingerprint = currentFingerprint
            feedbackFingerprint = currentFingerprint
            statusMessage = configLoadError!
            return
        }

        let config: [String: Any]
        do {
            guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                throw NSError(domain: "CodexQuotaGuard", code: 1, userInfo: [NSLocalizedDescriptionKey: "配置根节点不是 JSON 对象"])
            }
            config = object
        } catch {
            configLoadError = "配置文件损坏，已停止保存以避免覆盖原配置：\(error.localizedDescription)"
            savedFingerprint = currentFingerprint
            feedbackFingerprint = currentFingerprint
            statusMessage = configLoadError!
            return
        }

        if migrated {
            do {
                try FileManager.default.createDirectory(
                    at: Self.defaultSupportDirectory(),
                    withIntermediateDirectories: true
                )
                try data.write(to: configURL, options: .atomic)
                statusMessage = "已从旧位置迁移配置"
            } catch {
                configLoadError = "旧配置读取成功，但迁移到统一配置目录失败：\(error.localizedDescription)"
                savedFingerprint = currentFingerprint
                feedbackFingerprint = currentFingerprint
                statusMessage = configLoadError!
                return
            }
        }

        primaryWarning = Self.number(config["primary_warning_percent"], fallback: 3).displayString
        secondaryWarning = Self.number(config["secondary_warning_percent"], fallback: 1).displayString
        resetDelay = String(Int(Self.number(config["post_reset_delay_seconds"], fallback: 60)))
        ordinaryCheckInterval = String(Int(Self.number(config["ordinary_check_interval_seconds"], fallback: 600)))
        criticalCheckInterval = String(Int(Self.number(config["critical_check_interval_seconds"], fallback: 5)))
        criticalBoundary = Self.number(config["critical_boundary_percent"], fallback: 20).displayString
        forceStop = true
        resumeTasks = config["resume_paused_turns"] as? Bool ?? true
        autoBankReset = config["auto_consume_reset_credit"] as? Bool ?? false
        showStatusBarIcon = config["show_status_bar_icon"] as? Bool ?? true
        showDockIcon = config["show_dock_icon"] as? Bool ?? true
        fixedRefreshEnabled = true
        let savedShift = (config["scheduled_refresh_hour_shift"] as? NSNumber)?.intValue ?? 0
        fixedRefreshHourShift = min(12, max(-12, savedShift))
        fixedRefreshOffset = String(Int(Self.number(config["scheduled_refresh_offset_seconds"], fallback: 60)))
        savedFingerprint = currentFingerprint
        feedbackFingerprint = currentFingerprint
        if !migrated { statusMessage = "配置已读取" }
    }

    func save() {
        feedbackFingerprint = currentFingerprint
        if let configLoadError {
            statusMessage = configLoadError
            return
        }
        guard let primary = validatedPercent(primaryWarning),
              let secondary = validatedPercent(secondaryWarning),
              let delay = Int(resetDelay.trimmingCharacters(in: .whitespacesAndNewlines)), delay >= 0,
              let ordinary = Int(ordinaryCheckInterval.trimmingCharacters(in: .whitespacesAndNewlines)), ordinary >= 60, ordinary <= 3600,
              let critical = Int(criticalCheckInterval.trimmingCharacters(in: .whitespacesAndNewlines)), critical >= 1, critical <= 60, critical <= ordinary,
              let boundary = validatedPercent(criticalBoundary), boundary >= 1,
              let offset = Int(fixedRefreshOffset.trimmingCharacters(in: .whitespacesAndNewlines)), offset >= 0 else {
            statusMessage = "请输入有效数值：额度百分比 1–100；普通检测 60–3600 秒；临界检测 1–60 秒且不大于普通检测"
            return
        }

        var config: [String: Any] = [:]
        if FileManager.default.fileExists(atPath: configURL.path) {
            do {
                let data = try Data(contentsOf: configURL)
                guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                    throw NSError(domain: "CodexQuotaGuard", code: 2, userInfo: [NSLocalizedDescriptionKey: "配置根节点不是 JSON 对象"])
                }
                config = object
            } catch {
                statusMessage = "保存失败：现有配置无法解析，未覆盖原文件：\(error.localizedDescription)"
                return
            }
        }
        config["primary_warning_percent"] = primary
        config["secondary_warning_percent"] = secondary
        config["post_reset_delay_seconds"] = delay
        config["ordinary_check_interval_seconds"] = ordinary
        config["critical_check_interval_seconds"] = critical
        config["critical_boundary_percent"] = boundary
        config["force_stop_active_turns"] = forceStop
        config["resume_paused_turns"] = resumeTasks
        config["auto_consume_reset_credit"] = autoBankReset
        config["show_status_bar_icon"] = showStatusBarIcon
        config["show_dock_icon"] = showDockIcon
        config["scheduled_refresh_enabled"] = fixedRefreshEnabled
        config["scheduled_refresh_hours"] = shiftedRefreshHours
        config["scheduled_refresh_hour_shift"] = fixedRefreshHourShift
        config["scheduled_refresh_offset_seconds"] = offset
        config["timezone"] = config["timezone"] ?? "Asia/Shanghai"
        let revision = UUID().uuidString
        config["config_revision"] = revision

        do {
            try FileManager.default.createDirectory(
                at: configURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            let data = try JSONSerialization.data(withJSONObject: config, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: configURL, options: .atomic)
            savedFingerprint = currentFingerprint
            NotificationCenter.default.post(name: .quotaGuardSettingsChanged, object: nil)
            if let reloadGuard {
                do {
                    try reloadGuard()
                    statusMessage = "已保存，额度守护程序已重新加载"
                } catch {
                    statusMessage = "已保存，但守护程序重新加载失败：\(error.localizedDescription)"
                }
            } else {
                statusMessage = "已保存，正在等待额度守护程序重新加载…"
                restartGuard(revision: revision) { [weak self] error in
                    guard let self else { return }
                    if let error {
                        self.statusMessage = "已保存，但守护程序重新加载失败：\(error.localizedDescription)"
                    } else {
                        self.statusMessage = "已保存，额度守护程序已重新加载"
                    }
                }
            }
        } catch {
            statusMessage = "保存失败：\(error.localizedDescription)"
        }
    }

    private func validatedPercent(_ value: String) -> Double? {
        guard let number = Double(value.trimmingCharacters(in: .whitespacesAndNewlines)), number.isFinite, number >= 0, number <= 100 else { return nil }
        return number
    }

    private func restartGuard(revision: String, completion: @escaping (Error?) -> Void) {
        let stateURL = Self.defaultStateURL()
        DispatchQueue.global(qos: .utility).async {
            do {
                let process = Process()
                process.executableURL = URL(fileURLWithPath: "/bin/launchctl")
                process.arguments = ["kickstart", "-k", "gui/\(getuid())/\(guardLabel)"]
                let exitSignal = DispatchSemaphore(value: 0)
                process.terminationHandler = { _ in exitSignal.signal() }
                try process.run()
                guard exitSignal.wait(timeout: .now() + 5) == .success else {
                    process.terminate()
                    throw NSError(domain: "CodexQuotaGuard", code: 3, userInfo: [NSLocalizedDescriptionKey: "等待 launchctl 重载超时"])
                }
                guard process.terminationStatus == 0 else {
                    throw NSError(domain: "CodexQuotaGuard", code: Int(process.terminationStatus), userInfo: [NSLocalizedDescriptionKey: "launchctl 返回 \(process.terminationStatus)"])
                }
                let deadline = Date().addingTimeInterval(5)
                var confirmed = false
                while Date() < deadline {
                    if let data = try? Data(contentsOf: stateURL),
                       let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                       object["config_revision"] as? String == revision,
                       self.healthFileIsFresh(Self.defaultReaderHealthURL(), revision: revision),
                       self.healthFileIsFresh(Self.defaultActionsHealthURL(), revision: revision),
                       self.healthFileIsFresh(Self.defaultWatchdogURL(), revision: revision) {
                        confirmed = true
                        break
                    }
                    Thread.sleep(forTimeInterval: 0.1)
                }
                guard confirmed else {
                    throw NSError(domain: "CodexQuotaGuard", code: 4, userInfo: [NSLocalizedDescriptionKey: "后台未确认已读取新配置"])
                }
                DispatchQueue.main.async { completion(nil) }
            } catch {
                DispatchQueue.main.async { completion(error) }
            }
        }
    }

    private func healthFileIsFresh(_ url: URL, revision: String) -> Bool {
        guard let health = readHealth(url),
              health["config_revision"] as? String == revision,
              let pid = health["pid"] as? NSNumber,
              pid.intValue > 0,
              let instance = health["instance_id"] as? String,
              !instance.isEmpty,
              let heartbeat = health["heartbeat_at"] as? NSNumber else {
            return false
        }
        let configuredInterval = (health["heartbeat_interval_seconds"] as? NSNumber)?.doubleValue ?? 5
        return Date().timeIntervalSince1970 - heartbeat.doubleValue <= max(15, configuredInterval * 3)
    }

    static func defaultConfigURL() -> URL {
        defaultSupportDirectory().appendingPathComponent("config.json")
    }

    static func defaultSupportDirectory() -> URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("CodexQuotaGuard", isDirectory: true)
    }

    static func defaultStateURL() -> URL {
        defaultSupportDirectory().appendingPathComponent("state.json")
    }

    static func defaultHealthURL() -> URL {
        defaultSupportDirectory().appendingPathComponent("health.json")
    }

    static func defaultReaderHealthURL() -> URL {
        defaultSupportDirectory().appendingPathComponent("reader-health.json")
    }

    static func defaultActionsHealthURL() -> URL {
        defaultSupportDirectory().appendingPathComponent("actions-health.json")
    }

    static func defaultWatchdogURL() -> URL {
        defaultSupportDirectory().appendingPathComponent("watchdog.json")
    }

    static func legacyConfigURL() -> URL {
        Bundle.main.bundleURL
            .deletingLastPathComponent()
            .appendingPathComponent("config.json")
    }

    private var currentFingerprint: String {
        [
            primaryWarning,
            secondaryWarning,
            resetDelay,
            forceStop.description,
            resumeTasks.description,
            autoBankReset.description,
            ordinaryCheckInterval,
            criticalCheckInterval,
            criticalBoundary,
            showStatusBarIcon.description,
            showDockIcon.description,
            fixedRefreshEnabled.description,
            fixedRefreshHourShift.description,
            fixedRefreshOffset,
        ].joined(separator: "|")
    }

    private static func number(_ value: Any?, fallback: Double) -> Double {
        (value as? NSNumber)?.doubleValue ?? fallback
    }

    func loadHealth() {
        let reader = readHealth(Self.defaultReaderHealthURL())
        let legacy = readHealth(Self.defaultHealthURL())
        let actions = readHealth(Self.defaultActionsHealthURL())
        let watchdog = readHealth(Self.defaultWatchdogURL())
        let readerStatus = processHealthText(reader, label: "读取")
        let actionsStatus = processHealthText(actions, label: "动作")
        let watchdogStatus = processHealthText(watchdog, label: "监督")
        let primary = healthWindowText(reader, label: "5小时", key: "primary")
        let secondary = healthWindowText(reader, label: "总额度", key: "secondary")
        let legacyHint = reader == nil && legacy != nil ? "；旧健康文件仅供诊断" : ""
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "zh_CN")
        formatter.timeZone = TimeZone(identifier: "Asia/Shanghai")
        formatter.dateFormat = "HH:mm:ss"
        healthMessage = "刷新 \(formatter.string(from: Date()))；\(readerStatus)；\(primary)；\(secondary)；\(actionsStatus)；\(watchdogStatus)\(legacyHint)"
    }

    private func readHealth(_ url: URL) -> [String: Any]? {
        guard FileManager.default.fileExists(atPath: url.path) else {
            return nil
        }
        guard let data = try? Data(contentsOf: url),
              let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return ["_read_error": true]
        }
        return object
    }

    private func processHealthText(_ health: [String: Any]?, label: String) -> String {
        guard let health else { return "\(label)：未运行" }
        if health["_read_error"] as? Bool == true { return "\(label)：文件损坏" }
        guard let pid = health["pid"] as? NSNumber,
              pid.intValue > 0,
              let instance = health["instance_id"] as? String,
              !instance.isEmpty,
              let heartbeat = health["heartbeat_at"] as? NSNumber else {
            return "\(label)：状态无效"
        }
        let heartbeatInterval = (health["heartbeat_interval_seconds"] as? NSNumber)?.doubleValue ?? 5
        if Date().timeIntervalSince1970 - heartbeat.doubleValue > max(15, heartbeatInterval * 3) {
            return "\(label)：心跳过期"
        }
        let status = health["status"] as? String ?? "未知"
        if let error = health["error"] as? String, !error.isEmpty {
            return "\(label)：\(status)（\(error)）"
        }
        return "\(label)：\(status)"
    }

    private func healthWindowText(_ health: [String: Any]?, label: String, key: String) -> String {
        guard let health else { return "\(label)：未运行" }
        if health["_read_error"] as? Bool == true { return "\(label)：文件损坏" }
        var status = health["\(key)_data_status"] as? String ?? "未知"
        if let expiry = health["\(key)_data_expires_at"] as? NSNumber,
           expiry.doubleValue < Date().timeIntervalSince1970,
           status == "current" {
            status = "过期"
        }
        guard let timestamp = health["\(key)_last_success_at"] as? NSNumber else {
            return "\(label)：\(status)"
        }
        let date = Date(timeIntervalSince1970: timestamp.doubleValue)
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "zh_CN")
        formatter.timeZone = TimeZone(identifier: "Asia/Shanghai")
        formatter.dateFormat = "MM-dd HH:mm:ss"
        return "\(label)：\(status) \(formatter.string(from: date))"
    }
}

private extension Double {
    var displayString: String {
        truncatingRemainder(dividingBy: 1) == 0 ? String(Int(self)) : String(self)
    }
}

struct SettingsView: View {
    @ObservedObject var model: SettingsModel

    var body: some View {
        Form {
            Section("额度阈值") {
                settingRow(
                    title: "5 小时余量低于",
                    help: "触发当前进行中任务的原生强制停止",
                    text: $model.primaryWarning,
                    suffix: "%"
                )
                settingRow(
                    title: "总额度低于",
                    help: "触发 bank reset 重置卡",
                    text: $model.secondaryWarning,
                    suffix: "%"
                )
                settingRow(
                    title: "重置后延迟",
                    help: "接口返回重置时间后等待，避免刚重置时读取失败",
                    text: $model.resetDelay,
                    suffix: "秒"
                )
            }

            Section("动作") {
                Text("额度阈值保护：固定开启")
                Toggle("额度重置后自动恢复本轮任务", isOn: $model.resumeTasks)
                Toggle("总额度低于阈值时自动使用 bank reset", isOn: $model.autoBankReset)
            }

            Section("三层额度检测护栏") {
                settingRow(
                    title: "额度充足时上限间隔",
                    help: "梯度调度在额度充足时的最大间隔；默认 600 秒",
                    text: $model.ordinaryCheckInterval,
                    suffix: "秒"
                )
                settingRow(
                    title: "低于 10% 安全间隔",
                    help: "任一窗口低于 10% 后固定使用；默认 5 秒",
                    text: $model.criticalCheckInterval,
                    suffix: "秒"
                )
                settingRow(
                    title: "梯度加速起点",
                    help: "从此余量开始逐步缩短读取间隔；默认 20%",
                    text: $model.criticalBoundary,
                    suffix: "%"
                )
                Text("接口事件到达时立即读取；本地兜底读取不启动模型 turn。默认曲线：100%=600秒、90%=500秒、20%=200秒、19%=90秒、10%=10秒，低于10%固定5秒。")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                HStack {
                    Text("检测健康")
                    Spacer()
                    Button("刷新") { model.loadHealth() }
                    Text(model.healthMessage)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .multilineTextAlignment(.trailing)
                }
            }

            Section("图标显示") {
                Toggle("显示状态栏图标", isOn: $model.showStatusBarIcon)
                Toggle("显示 Dock 图标", isOn: $model.showDockIcon)
                Text("图标采用白底黑色 C。建议至少保留一个入口，两个都隐藏后仍可从 App 文件重新打开设置。")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Section("每日固定刷新与提醒") {
                Text("每日固定刷新与提醒：固定开启")
                HStack {
                    VStack(alignment: .leading, spacing: 3) {
                        Text("刷新时刻整体偏移")
                        Text("基础时刻为 7、12、17、22；每次只需按 +1 或 −1 小时")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    Stepper(value: $model.fixedRefreshHourShift, in: -12...12, step: 1) {
                        Text(model.fixedRefreshHourShift == 0
                             ? "不偏移"
                             : String(format: "%+d 小时", model.fixedRefreshHourShift))
                            .monospacedDigit()
                            .frame(minWidth: 72, alignment: .trailing)
                    }
                    .accessibilityLabel("整体调整刷新时刻")
                }
                .disabled(!model.fixedRefreshEnabled)
                Text("当前时刻：\(model.shiftedRefreshTimesText)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .disabled(!model.fixedRefreshEnabled)
                settingRow(
                    title: "每个时点递增延迟",
                    help: "第一个时点不延迟，之后每个 5 小时时点递增，默认 60 秒",
                    text: $model.fixedRefreshOffset,
                    suffix: "秒"
                )
                .disabled(!model.fixedRefreshEnabled)
                Text("每天到点读取额度并向配置的 Codex 对话发送刷新请求；每个时点递增延迟。")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Section {
                HStack {
                    Button("重新读取") { model.load() }
                    Spacer()
                    Button("保存设置") { model.save() }
                        .keyboardShortcut(.defaultAction)
                        .disabled(!model.hasUnsavedChanges)
                }
                Text(model.displayedStatusMessage)
                    .font(.footnote)
                    .foregroundStyle(model.hasUnsavedChanges ? .orange : .secondary)
            }
        }
        .formStyle(.grouped)
        .padding(18)
        .frame(width: 540, height: 540)
    }

    @ViewBuilder
    private func settingRow(title: String, help: String, text: Binding<String>, suffix: String) -> some View {
        HStack {
            VStack(alignment: .leading, spacing: 3) {
                Text(title)
                Text(help)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            TextField("", text: text)
                .multilineTextAlignment(.trailing)
                .frame(width: 70)
            Text(suffix)
                .frame(width: 28, alignment: .leading)
        }
    }
}

#if !SETTINGS_MODEL_TEST
@main
struct CodexQuotaGuardSettingsApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var model = SettingsModel()

    var body: some Scene {
        WindowGroup("Codex 额度守护设置") {
            SettingsView(model: model)
        }
        .windowResizability(.contentSize)
    }
}
#endif
