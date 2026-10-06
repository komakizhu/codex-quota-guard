import Foundation

@main
struct SettingsSaveTests {
    static func main() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("config.json")
        try Data("{\"primary_warning_percent\":3,\"custom_key\":\"preserve\"}".utf8).write(to: url)
        var reloadCount = 0
        let model = SettingsModel(configURL: url, reloadGuard: { reloadCount += 1 })
        precondition(!model.hasUnsavedChanges)
        precondition(model.shiftedRefreshTimesText == "7:00、12:01、17:02、22:03")
        model.fixedRefreshHourShift = 1
        precondition(model.shiftedRefreshTimesText == "8:00、13:01、18:02、23:03")
        model.fixedRefreshHourShift = 0
        model.primaryWarning = "invalid"
        model.save()
        precondition(model.hasUnsavedChanges)
        precondition(model.displayedStatusMessage.contains("请输入有效数值"))
        precondition(reloadCount == 0)
        model.primaryWarning = " 4 "
        precondition(model.displayedStatusMessage == "有未保存的修改")
        model.save()
        precondition(!model.hasUnsavedChanges)
        precondition(model.displayedStatusMessage.hasPrefix("已保存"))
        let stored = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
        precondition((stored["primary_warning_percent"] as? NSNumber)?.intValue == 4)
        precondition(stored["custom_key"] as? String == "preserve")
        precondition(reloadCount == 1)
        let reloaded = SettingsModel(configURL: url, reloadGuard: {})
        precondition(reloaded.primaryWarning == "4" && !reloaded.hasUnsavedChanges)
        let failure = SettingsModel(configURL: directory.appendingPathComponent("missing/config.json"), reloadGuard: {})
        failure.save()
        precondition(!failure.hasUnsavedChanges)
        precondition(failure.displayedStatusMessage.hasPrefix("已保存"))
        let corruptURL = directory.appendingPathComponent("corrupt.json")
        try Data("not-json".utf8).write(to: corruptURL)
        let corrupt = SettingsModel(configURL: corruptURL, reloadGuard: {})
        corrupt.primaryWarning = "6"
        corrupt.save()
        precondition(corrupt.hasUnsavedChanges)
        precondition(corrupt.displayedStatusMessage.contains("配置文件损坏") || corrupt.displayedStatusMessage.contains("无法解析"))
        let corruptContents = String(data: try Data(contentsOf: corruptURL), encoding: .utf8)
        precondition(corruptContents == "not-json")
        let reloadFailure = SettingsModel(configURL: url, reloadGuard: {
            throw NSError(domain: "test", code: 1)
        })
        reloadFailure.primaryWarning = "5"
        reloadFailure.save()
        precondition(!reloadFailure.hasUnsavedChanges)
        precondition(reloadFailure.displayedStatusMessage.contains("已保存，但"))
        print("PASS: invalid input, successful save, persisted values, reload, write failure, service failure")
    }
}
