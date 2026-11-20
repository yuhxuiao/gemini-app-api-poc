import AppKit
import Foundation

// dsh_cc_launcher.swift - Minimal macOS AppKit launcher for `一键启动 Claude Code.app`
// Double-click to open Claude Code inside the DSH Gemini Web Gateway project;
// drag-and-drop any folder onto the `.app` icon to open Claude Code in that folder.

func shellQuote(_ s: String) -> String {
    return "'" + s.replacingOccurrences(of: "'", with: "'\\''") + "'"
}

final class ClaudeLauncherDelegate: NSObject, NSApplicationDelegate {
    private var handledOpenURLs = false
    private let dryRun = ProcessInfo.processInfo.environment["DSH_LAUNCHER_DRY_RUN"] == "1"
        || CommandLine.arguments.contains("--dry-run")

    private func resolveProjectDir() -> String? {
        let fm = FileManager.default
        var candidates: [String] = []
        if let plistDir = Bundle.main.object(forInfoDictionaryKey: "DSHProjectDir") as? String, !plistDir.isEmpty {
            candidates.append(plistDir)
        }
        candidates.append(Bundle.main.bundleURL.deletingLastPathComponent().path)
        for cand in candidates {
            let script = (cand as NSString).appendingPathComponent("一键启动-ClaudeCode.command")
            if fm.isExecutableFile(atPath: script) {
                return cand
            }
        }
        return nil
    }

    private func resolveTerminalURL() -> URL? {
        if let u = NSWorkspace.shared.urlForApplication(withBundleIdentifier: "com.apple.Terminal") {
            return u
        }
        for path in ["/System/Applications/Utilities/Terminal.app", "/Applications/Utilities/Terminal.app"] {
            if FileManager.default.fileExists(atPath: path) {
                return URL(fileURLWithPath: path)
            }
        }
        return nil
    }

    private func showErrorAlert(_ title: String, _ message: String) {
        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = title
        alert.informativeText = message
        alert.runModal()
    }

    private func launchInTerminal(droppedURLs: [URL]) {
        guard let projectDir = resolveProjectDir() else {
            if dryRun {
                fputs("{\"ok\":false,\"error\":\"project_dir_not_found\"}\n", stderr)
                exit(1)
            }
            showErrorAlert("未找到项目启动脚本", "请确认 `一键启动-ClaudeCode.command` 位于当前项目目录内。")
            NSApp.terminate(nil)
            return
        }

        let commandScript = (projectDir as NSString).appendingPathComponent("一键启动-ClaudeCode.command")
        var targetDir: String? = nil
        if let first = droppedURLs.first {
            var isDir: ObjCBool = false
            if FileManager.default.fileExists(atPath: first.path, isDirectory: &isDir) {
                targetDir = isDir.boolValue ? first.path : first.deletingLastPathComponent().path
            }
        }
        if targetDir == nil {
            for arg in CommandLine.arguments.dropFirst() where !arg.hasPrefix("-") {
                var isDir: ObjCBool = false
                if FileManager.default.fileExists(atPath: arg, isDirectory: &isDir) {
                    targetDir = isDir.boolValue ? arg : URL(fileURLWithPath: arg).deletingLastPathComponent().path
                    break
                }
            }
        }

        guard let termURL = resolveTerminalURL() else {
            if dryRun {
                fputs("{\"ok\":false,\"error\":\"terminal_app_not_found\"}\n", stderr)
                exit(1)
            }
            showErrorAlert("未找到 macOS Terminal.app", "无法打开终端窗口。")
            NSApp.terminate(nil)
            return
        }

        var openURL = URL(fileURLWithPath: commandScript)
        if let customDir = targetDir {
            let wrapperPath = (NSTemporaryDirectory() as NSString).appendingPathComponent("dsh_cc_\(UUID().uuidString.prefix(8)).command")
            let body = "#!/usr/bin/env bash\nrm -f -- \"$0\"\nexec \(shellQuote(commandScript)) \(shellQuote(customDir))\n"
            if !dryRun {
                do {
                    try body.write(toFile: wrapperPath, atomically: true, encoding: .utf8)
                    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: wrapperPath)
                    openURL = URL(fileURLWithPath: wrapperPath)
                } catch {
                    showErrorAlert("创建启动脚本失败", error.localizedDescription)
                    NSApp.terminate(nil)
                    return
                }
            } else {
                openURL = URL(fileURLWithPath: wrapperPath)
            }
        }

        if dryRun {
            let info: [String: Any] = [
                "ok": true,
                "project_dir": projectDir,
                "command_script": commandScript,
                "target_dir": targetDir ?? projectDir,
                "terminal_app": termURL.path,
                "open_url": openURL.path
            ]
            if let data = try? JSONSerialization.data(withJSONObject: info, options: [.sortedKeys]),
               let jsonStr = String(data: data, encoding: .utf8) {
                print(jsonStr)
            }
            exit(0)
        }

        let cfg = NSWorkspace.OpenConfiguration()
        cfg.activates = true
        NSWorkspace.shared.open([openURL], withApplicationAt: termURL, configuration: cfg) { _, err in
            DispatchQueue.main.async {
                if let err = err {
                    self.showErrorAlert("启动 Terminal 失败", err.localizedDescription)
                }
                NSApp.terminate(nil)
            }
        }
    }

    func application(_ application: NSApplication, open urls: [URL]) {
        guard !handledOpenURLs else { return }
        handledOpenURLs = true
        launchInTerminal(droppedURLs: urls)
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        let delay: TimeInterval = dryRun ? 0.01 : 0.12
        DispatchQueue.main.asyncAfter(deadline: .now() + delay) { [weak self] in
            guard let self = self, !self.handledOpenURLs else { return }
            self.handledOpenURLs = true
            self.launchInTerminal(droppedURLs: [])
        }
    }
}

let app = NSApplication.shared
let delegate = ClaudeLauncherDelegate()
app.delegate = delegate
app.run()
