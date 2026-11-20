import AppKit
import Foundation
import WebKit

// dsh_webkit_bridge.swift v4.6.0-clean-carbon
// Runs inside `DSHWebKitBridge.app` (`com.dsh.gemini.webkitbridge`) with:
// - Singleton `flock` + `.pid` lifecycle + `FD_CLOEXEC` + SIGTERM/SIGINT graceful socket cleanup
// - Dedicated serial `writeQueue` + 2 MB `SO_SNDBUF` + `poll(POLLOUT)` backpressure handling
// - Non-blocking `<1ms` cached `status`, `5.0s` `SO_RCVTIMEO` on incoming client reads, and `WKNavigationDelegate` failure recovery
// - Live `otAQ7b` model & `carbon_multi_ultra_ultra_policy` discovery (`node[17]` + `otAQ7b[0][24]`) with deterministic `.sortedKeys` JSON
// - Deep-Think 9-minute semantic watchdog (`firstWrbTimeoutMs` + `idleTimeoutMs` up to 540s) + native `batchexecute` (`GzXR5e`) auto-cleanup
// - Background drain-to-commit (`background_drain`), worker-slot visibility/spellcheck hardening, and `wizHydrated` readiness gating

let BRIDGE_VERSION = "4.6.0-clean-carbon"
let SAFARI_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15"
let GEMINI_APP_URL = "https://gemini.google.com/app"

@_silgen_name("flock")
private func posixFlock(_ fd: Int32, _ operation: Int32) -> Int32

func setCloseOnExec(_ fd: Int32) {
    guard fd >= 0 else { return }
    let flags = Darwin.fcntl(fd, F_GETFD, 0)
    if flags >= 0 {
        _ = Darwin.fcntl(fd, F_SETFD, flags | FD_CLOEXEC)
    }
}

func acquireSingletonLock(socketPath: String) -> Int32 {
    let lockPath = socketPath + ".lock"
    let fd = Darwin.open(lockPath, O_CREAT | O_RDWR | O_CLOEXEC, mode_t(0o600))
    guard fd >= 0 else {
        fputs("[dsh-webkit-bridge] Failed to open lock file \(lockPath): \(String(cString: strerror(errno)))\n", stderr)
        exit(1)
    }
    setCloseOnExec(fd)
    if posixFlock(fd, LOCK_EX | LOCK_NB) != 0 {
        let err = errno
        fputs("[dsh-webkit-bridge] Another instance holds \(lockPath) (errno=\(err)); exiting without touching socket.\n", stderr)
        Darwin.close(fd)
        exit(0)
    }
    return fd
}

func writePidFile(socketPath: String) {
    let pidPath = socketPath + ".pid"
    let pidStr = "\(Darwin.getpid())\n"
    let pidFd = Darwin.open(pidPath, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, mode_t(0o600))
    if pidFd >= 0 {
        setCloseOnExec(pidFd)
        let bytes = Array(pidStr.utf8)
        _ = bytes.withUnsafeBytes { ptr in
            if let base = ptr.baseAddress {
                return Darwin.write(pidFd, base, ptr.count)
            }
            return 0
        }
        Darwin.close(pidFd)
    } else {
        try? pidStr.write(toFile: pidPath, atomically: true, encoding: .utf8)
    }
}

final class SocketClientSession {
    let fd: Int32
    let reqId: String
    private let lock = NSLock()
    private let writeQueue: DispatchQueue
    private var logicallyClosed = false
    private var fdClosed = false
    private var hasActiveReadSource = false
    private var eofSource: DispatchSourceRead?
    private var onRemoteDisconnect: (() -> Void)?

    init(fd: Int32, reqId: String) {
        self.fd = fd
        self.reqId = reqId
        self.writeQueue = DispatchQueue(label: "com.dsh.webkit.sock.\(reqId)", qos: .userInitiated)

        var sndBuf: Int32 = 2 * 1024 * 1024
        _ = Darwin.setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndBuf, socklen_t(MemoryLayout<Int32>.size))
        var one: Int32 = 1
        _ = Darwin.setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
    }

    func startDisconnectWatch(onDisconnect: @escaping () -> Void) {
        lock.lock()
        if logicallyClosed || fdClosed || eofSource != nil {
            lock.unlock()
            return
        }
        self.onRemoteDisconnect = onDisconnect
        let flags = Darwin.fcntl(fd, F_GETFL, 0)
        if flags >= 0 {
            _ = Darwin.fcntl(fd, F_SETFL, flags | O_NONBLOCK)
        }
        let src = DispatchSource.makeReadSource(fileDescriptor: fd, queue: DispatchQueue.global(qos: .userInitiated))
        self.eofSource = src
        self.hasActiveReadSource = true
        lock.unlock()

        src.setEventHandler { [weak self] in
            guard let self = self else { return }
            var probe: UInt8 = 0
            let n = Darwin.recv(self.fd, &probe, 1, MSG_PEEK)
            let err = errno
            let disconnected: Bool
            if n == 0 {
                disconnected = true
            } else if n < 0 && err != EAGAIN && err != EWOULDBLOCK && err != EINTR {
                disconnected = true
            } else {
                disconnected = false
            }
            if disconnected {
                var cb: (() -> Void)?
                var srcToCancel: DispatchSourceRead?
                self.lock.lock()
                if !self.logicallyClosed {
                    self.logicallyClosed = true
                    cb = self.onRemoteDisconnect
                    self.onRemoteDisconnect = nil
                }
                srcToCancel = self.eofSource
                self.eofSource = nil
                self.lock.unlock()
                (srcToCancel ?? src).cancel()
                if let cb = cb {
                    DispatchQueue.main.async(execute: cb)
                }
            }
        }

        // Strong `self` capture keeps session alive until cancel handler finishes closing fd.
        src.setCancelHandler {
            self.writeQueue.async {
                self.lock.lock()
                self.hasActiveReadSource = false
                if !self.fdClosed {
                    self.fdClosed = true
                    _ = Darwin.shutdown(self.fd, SHUT_WR)
                    Darwin.close(self.fd)
                }
                self.lock.unlock()
            }
        }

        src.resume()
    }

    func sendJSON(_ dict: [String: Any]) {
        lock.lock()
        let closedNow = logicallyClosed || fdClosed
        lock.unlock()
        if closedNow { return }

        guard let data = try? JSONSerialization.data(withJSONObject: dict, options: []) else { return }
        var line = data
        line.append(0x0A)

        writeQueue.async {
            self.lock.lock()
            let skip = self.logicallyClosed || self.fdClosed
            self.lock.unlock()
            if skip { return }

            line.withUnsafeBytes { rawBuf in
                guard let base = rawBuf.baseAddress else { return }
                var sent = 0
                while sent < rawBuf.count {
                    let n = Darwin.write(self.fd, base.advanced(by: sent), rawBuf.count - sent)
                    if n > 0 {
                        sent += n
                        continue
                    }
                    let err = errno
                    if n < 0 && err == EINTR {
                        continue
                    }
                    if n < 0 && (err == EAGAIN || err == EWOULDBLOCK) {
                        var pfd = Darwin.pollfd(fd: self.fd, events: Int16(POLLOUT), revents: 0)
                        let pr = Darwin.poll(&pfd, 1, 30000)
                        if pr > 0 && (pfd.revents & Int16(POLLERR | POLLHUP | POLLNVAL)) == 0 {
                            continue
                        }
                    }
                    // Genuine peer disconnect or 30s stall
                    var cb: (() -> Void)?
                    var srcToCancel: DispatchSourceRead?
                    self.lock.lock()
                    if !self.logicallyClosed {
                        self.logicallyClosed = true
                        cb = self.onRemoteDisconnect
                        self.onRemoteDisconnect = nil
                    }
                    srcToCancel = self.eofSource
                    self.eofSource = nil
                    if !self.hasActiveReadSource && !self.fdClosed {
                        self.fdClosed = true
                        Darwin.close(self.fd)
                    }
                    self.lock.unlock()
                    srcToCancel?.cancel()
                    if let cb = cb {
                        DispatchQueue.main.async(execute: cb)
                    }
                    break
                }
            }
        }
    }

    func close(completion: (() -> Void)? = nil) {
        writeQueue.async {
            self.lock.lock()
            self.onRemoteDisconnect = nil
            self.logicallyClosed = true
            let src = self.eofSource
            self.eofSource = nil
            if !self.hasActiveReadSource && !self.fdClosed {
                self.fdClosed = true
                _ = Darwin.shutdown(self.fd, SHUT_WR)
                Darwin.close(self.fd)
            }
            self.lock.unlock()
            src?.cancel()
            completion?()
        }
    }

    deinit {
        lock.lock()
        let needClose = !fdClosed
        let fdToClose = fd
        fdClosed = true
        lock.unlock()
        if needClose && fdToClose >= 0 {
            Darwin.close(fdToClose)
        }
    }
}

private let RESIDENT_BRIDGE_USER_SCRIPT = """
(function() {
  if (window.__dshResidentInstalled) return;
  window.__dshResidentInstalled = true;
  window.__dshAborts = window.__dshAborts || {};
  window.__dshAbortReasons = window.__dshAbortReasons || {};
  window.__dshSniffedHeaders = window.__dshSniffedHeaders || {};
  window.__dshSniffedBl = "";
  window.__dshModelsDiscoveredAt = 0;

  // Keep resident offscreen worker WKWebView slots active (prevent Boq/DOM background timer throttling)
  try {
    Object.defineProperty(document, "visibilityState", { get: function() { return "visible"; }, configurable: true });
    Object.defineProperty(document, "hidden", { get: function() { return false; }, configurable: true });
    Object.defineProperty(document, "webkitHidden", { get: function() { return false; }, configurable: true });
    window.addEventListener("visibilitychange", function(ev) {
      try { ev.stopImmediatePropagation(); } catch (_) {}
    }, true);
  } catch (_) {}

  // Prevent Gemini composer auto-focus from triggering macOS NSSpellServer / hiservices XPC stalls
  function dshQuietEditableElements(root) {
    try {
      var scope = (root && root.querySelectorAll) ? root : document;
      var els = scope.querySelectorAll('[contenteditable], textarea, input');
      for (var i = 0; i < els.length; i++) {
        var el = els[i];
        el.setAttribute("spellcheck", "false");
        el.setAttribute("autocorrect", "off");
        el.setAttribute("autocapitalize", "off");
        if (document.activeElement === el) {
          try { el.blur(); } catch (_) {}
        }
      }
    } catch (_) {}
  }
  document.addEventListener("focusin", function(ev) {
    try {
      var t = ev && ev.target;
      if (t && t.setAttribute) {
        t.setAttribute("spellcheck", "false");
        t.setAttribute("autocorrect", "off");
        if (t.isContentEditable || t.tagName === "TEXTAREA") {
          try { t.blur(); } catch (_) {}
        }
      }
    } catch (_) {}
  }, true);
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function() { dshQuietEditableElements(document); }, { once: true });
  } else {
    dshQuietEditableElements(document);
  }
  window.addEventListener("load", function() { setTimeout(function() { dshQuietEditableElements(document); }, 120); }, { once: true });

  function dshPost(msg) {
    try {
      if (window.webkit && window.webkit.messageHandlers && window.webkit.messageHandlers.dshBridge) {
        window.webkit.messageHandlers.dshBridge.postMessage(msg);
      }
    } catch (_) {}
  }

  function detectAuthUserPrefix() {
    var m = (location.pathname || "").match(/^\\/u\\/(\\d+)(?:\\/|$)/);
    return {
      prefix: m ? ("/u/" + m[1]) : "",
      authUser: m ? String(m[1]) : ""
    };
  }

  function computeSafeDelta(prev, curr) {
    if (!curr || curr === prev) return {delta: "", nextPrev: prev || ""};
    if (!prev) return {delta: curr, nextPrev: curr};
    if (curr.startsWith(prev)) {
      return {delta: curr.slice(prev.length), nextPrev: curr};
    }
    if (curr.length <= prev.length) {
      return {delta: "", nextPrev: prev};
    }
    var anchorLen = Math.min(24, prev.length);
    if (anchorLen >= 4) {
      var tailAnchor = prev.slice(prev.length - anchorLen);
      var pos = curr.lastIndexOf(tailAnchor);
      if (pos !== -1 && pos + anchorLen <= curr.length) {
        return {delta: curr.slice(pos + anchorLen), nextPrev: curr};
      }
    }
    return {delta: curr.slice(prev.length), nextPrev: curr};
  }

  function extractThoughtFromCand(cand) {
    if (!Array.isArray(cand) || cand.length <= 37 || !Array.isArray(cand[37])) return "";
    var c37 = cand[37];
    if (c37.length > 0) {
      if (Array.isArray(c37[0]) && typeof c37[0][0] === "string" && c37[0][0]) {
        return c37[0][0];
      }
      if (typeof c37[0] === "string" && c37[0]) {
        return c37[0];
      }
    }
    if (c37.length > 1 && Array.isArray(c37[1])) {
      var chunks = [];
      for (var i = 0; i < c37[1].length; i++) {
        var step = c37[1][i];
        if (Array.isArray(step) && Array.isArray(step[0]) && typeof step[0][0] === "string" && step[0][0]) {
          chunks.push(step[0][0]);
        } else if (Array.isArray(step) && typeof step[0] === "string" && step[0]) {
          chunks.push(step[0]);
        }
      }
      if (chunks.length > 0) return chunks.join("\\n\\n");
    }
    return "";
  }

  function stripCtrl94AndExtractThoughts(rawText, allowTailHold) {
    var CTRL94 = "<ctrl94>thought";
    var CTRL95 = "<ctrl95>";
    var thoughts = [];
    var clean = rawText || "";
    while (true) {
      var sIdx = clean.indexOf(CTRL94);
      if (sIdx === -1) break;
      var pre = clean.slice(0, sIdx);
      var after = clean.slice(sIdx + CTRL94.length);
      var eIdx = after.indexOf(CTRL95);
      if (eIdx === -1) {
        var openThk = after.replace(/^\\s+/, "");
        if (allowTailHold && openThk) {
          var ltThk = openThk.lastIndexOf("<");
          if (ltThk !== -1 && (openThk.length - ltThk) < CTRL95.length && CTRL95.indexOf(openThk.slice(ltThk)) === 0) {
            openThk = openThk.slice(0, ltThk);
          }
        }
        if (openThk) thoughts.push(openThk);
        clean = pre;
        break;
      }
      var closedThk = after.slice(0, eIdx).replace(/^\\s+/, "");
      if (closedThk) thoughts.push(closedThk);
      clean = pre + after.slice(eIdx + CTRL95.length);
    }
    if (allowTailHold && clean) {
      var ltIdx = clean.lastIndexOf("<");
      if (ltIdx !== -1 && (clean.length - ltIdx) < CTRL94.length && CTRL94.indexOf(clean.slice(ltIdx)) === 0) {
        clean = clean.slice(0, ltIdx);
      }
    }
    return {cleanText: clean, inlineThought: thoughts.join("\\n\\n")};
  }

  function createWrbDecoder(reqId, ctrl) {
    var buf = "";
    var xssiStripped = false;
    var prevThinking = "";
    var prevText = "";
    var lastThoughts = "";
    var lastRawText = "";
    var lastCandThought = "";
    var routedModel = "";
    var cid = "", rid = "", rcid = "";
    var sawWrb = false;
    var sawContent = false;
    var fatalError = null;
    var canvasMap = {};
    var chipUrlToId = {};
    var lenRe = /^\\s*\\d+\\s*$/;
    var nextFrameBoundaryRe = /\\n\\s*\\d+\\s*\\n\\s*\\[\\[/;
    var chipRe = /https?:\\/\\/googleusercontent\\.com\\/immersive_entry_chip\\/(\\d+)/g;
    var unexpandedChipRe = /https?:\\/\\/googleusercontent\\.com\\/immersive_entry_chip\\/\\d+/;
    var HTTPS_CHIP_PFX = "https://googleusercontent.com/immersive_entry_chip/";
    var HTTP_CHIP_PFX = "http://googleusercontent.com/immersive_entry_chip/";

    function mapRpcError(rawCode, detailObj) {
      var codeStr = (rawCode !== undefined && rawCode !== null) ? String(rawCode) : "UNKNOWN";
      var codeNum = (typeof rawCode === "number") ? rawCode : parseInt(codeStr, 10);
      var detailStr = "";
      try { detailStr = (typeof detailObj === "string") ? detailObj : JSON.stringify(detailObj || ""); } catch (_) {}
      var lowDetail = detailStr.toLowerCase();
      var mapped = "RPC_ERR_" + codeStr;
      if (
        codeNum === 429 || codeNum === 8 || codeNum === 1037 || codeNum === 1052 || codeNum === 1060 ||
        /resource_exhausted|rate_limit|quota|too many requests|429/.test(lowDetail)
      ) {
        mapped = "HTTP_429";
      } else if (
        codeNum === 401 || codeNum === 403 || codeNum === 16 || codeNum === 7 || codeNum === 1016 ||
        /unauthenticated|permission_denied|auth_required|login|sign.?in/.test(lowDetail)
      ) {
        mapped = "AUTH_REQUIRED";
      }
      return {
        code: mapped,
        message: "StreamGenerate RPC error (" + codeStr + "): " + detailStr.slice(0, 400),
        explicitRpcError: true,
        reload: (mapped === "AUTH_REQUIRED")
      };
    }

    function updateCanvasCache(cand) {
      var updated = false;
      try {
        if (cand.length > 12 && Array.isArray(cand[12]) && cand[12].length > 8 && cand[12][8] && typeof cand[12][8] === "object") {
          var m57 = cand[12][8]["57"];
          if (Array.isArray(m57)) {
            for (var k = 0; k < m57.length; k++) {
              var cm = m57[k];
              if (Array.isArray(cm) && Array.isArray(cm[0]) && typeof cm[0][0] === "string" && typeof cm[1] === "string") {
                chipUrlToId[cm[0][0]] = cm[1];
                updated = true;
              }
            }
          }
        }
        if (cand.length > 30 && Array.isArray(cand[30]) && cand[30].length > 0) {
          var entries = Array.isArray(cand[30][0]) ? cand[30] : [cand[30]];
          for (var idx = 0; idx < entries.length; idx++) {
            var entry = entries[idx];
            if (!Array.isArray(entry) || entry.length <= 4) continue;
            var content = entry[4];
            if (typeof content === "string" && content.length > 0) {
              var kindNum = entry.length > 10 ? entry[10] : 1;
              var isCode = (kindNum === 2);
              var fileName = (entry.length > 9 && typeof entry[9] === "string") ? entry[9] : "";
              var ext = fileName.indexOf(".") !== -1 ? fileName.split(".").pop().toLowerCase() : "";
              var lang = "";
              if (entry.length > 14 && Array.isArray(entry[14])) {
                lang = isCode ? (entry[14][1] || "") : (entry[14][4] || "");
              }
              if (!lang) lang = ext || "html";
              canvasMap[idx] = {
                id: typeof entry[0] === "string" ? entry[0] : "",
                title: (entry.length > 2 && typeof entry[2] === "string" ? entry[2] : fileName),
                fileName: fileName,
                kind: isCode ? "code" : "document",
                lang: String(lang),
                content: content
              };
              updated = true;
            }
          }
        }
      } catch (_) {}
      return updated;
    }

    function expandCanvasChips(rawText) {
      if (!rawText || rawText.indexOf("immersive_entry_chip/") === -1) return rawText;
      return rawText.replace(chipRe, function(fullUrl, idxStr) {
        var idx = parseInt(idxStr, 10);
        var item = canvasMap[idx];
        if (!item && chipUrlToId[fullUrl]) {
          var targetId = chipUrlToId[fullUrl];
          for (var key in canvasMap) {
            if (canvasMap[key] && canvasMap[key].id === targetId) {
              item = canvasMap[key];
              break;
            }
          }
        }
        if (!item) return fullUrl;
        var body = item.content.trim();
        if (item.kind === "code" && body.indexOf("```") !== 0) {
          return "```" + (item.lang || "html") + "\\n" + body + "\\n```";
        }
        return body;
      });
    }

    function holdUnexpandedChips(text, allowHold) {
      if (!allowHold || !text) return text;
      var m = unexpandedChipRe.exec(text);
      var out = (m && m.index >= 0) ? text.slice(0, m.index) : text;
      var httpIdx = out.lastIndexOf("http");
      if (httpIdx !== -1 && (out.length - httpIdx) <= HTTPS_CHIP_PFX.length) {
        var tail = out.slice(httpIdx);
        if (tail.length >= 9 && (HTTPS_CHIP_PFX.indexOf(tail) === 0 || HTTP_CHIP_PFX.indexOf(tail) === 0)) {
          out = out.slice(0, httpIdx);
        }
      }
      return out;
    }

    function emitThoughtAndText(candThought, rawText, allowHold) {
      if (candThought) lastCandThought = candThought;
      if (rawText) lastRawText = rawText;

      var effectiveRaw = rawText || lastRawText;
      var stripped = stripCtrl94AndExtractThoughts(effectiveRaw, allowHold);
      var thoughtStr = candThought || lastCandThought || stripped.inlineThought;

      if (thoughtStr) {
        lastThoughts = thoughtStr;
        var rThk = computeSafeDelta(prevThinking, lastThoughts);
        prevThinking = rThk.nextPrev;
        if (rThk.delta) {
          sawContent = true;
          dshPost({reqId: reqId, event: "direct_delta", kind: "thinking", data: rThk.delta, conversation_id: cid, response_id: rid, choice_id: rcid});
        }
      }

      if (stripped.cleanText) {
        var expandedText = expandCanvasChips(stripped.cleanText);
        var emitCandidate = holdUnexpandedChips(expandedText, allowHold);
        if (emitCandidate) {
          var rTxt = computeSafeDelta(prevText, emitCandidate);
          prevText = rTxt.nextPrev;
          if (rTxt.delta) {
            sawContent = true;
            dshPost({reqId: reqId, event: "direct_delta", kind: "text", data: rTxt.delta, conversation_id: cid, response_id: rid, choice_id: rcid});
          }
        }
      }
    }

    function processOuter(outer) {
      if (!Array.isArray(outer)) return;
      for (var i = 0; i < outer.length; i++) {
        if (ctrl && ctrl.signal && ctrl.signal.aborted) return;
        var item = outer[i];
        if (!Array.isArray(item) || item.length < 2) continue;

        if (item[0] === "e") {
          var isNormalEos = (
            item.length >= 5 &&
            item[2] === null &&
            item[3] === null &&
            typeof item[4] === "number" &&
            (sawWrb || (typeof item[1] === "number" && item[1] >= 1 && item[1] <= 4))
          );
          if (!isNormalEos && item[1] !== null && item[1] !== undefined) {
            if (!fatalError) {
              fatalError = mapRpcError(item[1], item.slice(2));
            }
            return;
          }
          continue;
        }

        if (item[0] !== "wrb.fr") continue;

        if ((item[2] === null || item[2] === undefined) && item.length > 5 && Array.isArray(item[5])) {
          if (!fatalError) {
            var errCode = item[5].length > 0 ? item[5][0] : "UNKNOWN";
            fatalError = mapRpcError(errCode, item[5]);
          }
          return;
        }

        if (item.length < 3 || typeof item[2] !== "string") continue;
        sawWrb = true;
        var part = null;
        try { part = JSON.parse(item[2]); } catch (_) { continue; }
        if (!Array.isArray(part)) continue;

        // Detect upstream routed model display name BEFORE emitting any text/thinking delta
        var candLabels = [part.length > 42 ? part[42] : null, part.length >= 2 ? part[part.length - 2] : null];
        for (var li = 0; li < candLabels.length; li++) {
          var lbl = candLabels[li];
          if (typeof lbl === "string" && /(Argon|Pro|Flash|Hydrogen|Carbon|Gemini)/i.test(lbl) && lbl.length < 64) {
            if (lbl !== routedModel) {
              routedModel = lbl;
              dshPost({reqId: reqId, event: "meta", routed_model: routedModel});
            }
            break;
          }
        }
        if (ctrl && ctrl.signal && ctrl.signal.aborted) return;

        if (part.length > 1 && Array.isArray(part[1])) {
          if (typeof part[1][0] === "string" && part[1][0]) cid = part[1][0];
          if (typeof part[1][1] === "string" && part[1][1]) rid = part[1][1];
        }
        if (part.length <= 4 || !Array.isArray(part[4]) || !part[4].length) continue;
        var cand = part[4][0];
        if (!Array.isArray(cand) || !cand.length) continue;
        if (typeof cand[0] === "string" && cand[0]) rcid = cand[0];

        var canvasUpdated = updateCanvasCache(cand);
        var thoughtStr = extractThoughtFromCand(cand);
        var rawText = (cand.length > 1 && Array.isArray(cand[1]) && typeof cand[1][0] === "string") ? cand[1][0] : "";

        if (thoughtStr || rawText || (canvasUpdated && lastRawText)) {
          emitThoughtAndText(thoughtStr, rawText, true);
        }
      }
    }

    return {
      feed: function(chunk) {
        if (!chunk || fatalError) return;
        buf += chunk;
        if (!xssiStripped) {
          var trimmed = buf.trimStart();
          if (trimmed.startsWith(")]}'")) {
            var nl0 = buf.indexOf("\\n");
            if (nl0 === -1) return;
            buf = buf.slice(nl0 + 1);
            xssiStripped = true;
          } else if (trimmed.startsWith("<")) {
            fatalError = {code: "AUTH_REQUIRED", message: "StreamGenerate returned HTML/login redirect page", reload: true};
            buf = "";
            return;
          } else if (trimmed.length >= 4) {
            xssiStripped = true;
          }
        }
        while (buf.length > 0 && !fatalError) {
          if (ctrl && ctrl.signal && ctrl.signal.aborted) {
            buf = "";
            return;
          }
          var nl = buf.indexOf("\\n");
          if (nl === -1) {
            var c0 = buf.trim();
            if (c0.startsWith("[") && c0.endsWith("]")) {
              try {
                var p0 = JSON.parse(c0);
                buf = "";
                processOuter(p0);
              } catch (_) {}
            }
            break;
          }
          var line = buf.slice(0, nl).trim();
          if (!line || lenRe.test(line)) {
            buf = buf.slice(nl + 1);
            continue;
          }
          if (line.startsWith("[")) {
            try {
              var p1 = JSON.parse(line);
              buf = buf.slice(nl + 1);
              processOuter(p1);
              continue;
            } catch (_) {}
            var accEnd = nl;
            var matchedMulti = false;
            while (true) {
              var nxtNl = buf.indexOf("\\n", accEnd + 1);
              var probeEnd = (nxtNl === -1) ? buf.length : nxtNl;
              var candStr = buf.slice(0, probeEnd).trim();
              if (candStr.endsWith("]")) {
                try {
                  var pm = JSON.parse(candStr);
                  buf = (nxtNl === -1) ? "" : buf.slice(nxtNl + 1);
                  processOuter(pm);
                  matchedMulti = true;
                  break;
                } catch (_) {}
              }
              if (nxtNl === -1) break;
              accEnd = nxtNl;
            }
            if (!matchedMulti) {
              // If a subsequent length-prefixed frame `\\n<digits>\\n[[` has already arrived, skip broken frame cleanly!
              var bMatch = nextFrameBoundaryRe.exec(buf.slice(1));
              if (bMatch && bMatch.index >= 0) {
                buf = buf.slice(1 + bMatch.index + 1);
                continue;
              }
              if (buf.length > 4 * 1024 * 1024) buf = buf.slice(nl + 1);
              break;
            }
          } else {
            buf = buf.slice(nl + 1);
          }
        }
      },
      hasFatalError: function() { return Boolean(fatalError); },
      getFatalError: function() { return fatalError; },
      hasSawWrb: function() { return sawWrb; },
      hasEmittedContent: function() { return sawContent; },
      flushCursorOnlyIfNeeded: function() {
        if (!fatalError && buf.trim().length > 0) {
          var rem = buf.trim();
          var firstBracket = rem.indexOf("[");
          var lastBracket = rem.lastIndexOf("]");
          if (firstBracket !== -1 && lastBracket > firstBracket) {
            try {
              var pTail = JSON.parse(rem.slice(firstBracket, lastBracket + 1));
              processOuter(pTail);
            } catch (_) {}
          }
          buf = "";
        }
        if (!fatalError && (lastRawText || lastCandThought)) {
          emitThoughtAndText(lastCandThought, lastRawText, false);
        }
        if (!fatalError && sawWrb && (cid || rid || rcid)) {
          dshPost({reqId: reqId, event: "direct_delta", kind: "text", data: "", conversation_id: cid, response_id: rid, choice_id: rcid});
        }
        return sawWrb;
      }
    };
  }

  function recordSniffedProto(urlStr, extHdrs) {
    if (!urlStr || (urlStr.indexOf("StreamGenerate") === -1 && urlStr.indexOf("batchexecute") === -1)) return;
    var blMatch = urlStr.match(/[?&]bl=([^&]+)/);
    var capturedBl = blMatch ? decodeURIComponent(blMatch[1]) : "";
    if (!capturedBl && (!extHdrs || Object.keys(extHdrs).length === 0)) return;
    if (capturedBl) window.__dshSniffedBl = capturedBl;
    if (extHdrs) {
      for (var ek in extHdrs) window.__dshSniffedHeaders[ek] = extHdrs[ek];
    }
    var uInfo = detectAuthUserPrefix();
    dshPost({event: "proto_profile", bl: capturedBl, auth_user: uInfo.authUser, headers: extHdrs || {}});
  }

  var origFetch = window.fetch;
  if (typeof origFetch === "function") {
    window.fetch = function(input, init) {
      try {
        var isInternal = init && init.__dshInternal === true;
        var urlStr = (typeof input === "string") ? input : (input && input.url ? String(input.url) : "");
        if (!isInternal && (urlStr.indexOf("StreamGenerate") !== -1 || urlStr.indexOf("batchexecute") !== -1)) {
          var extHdrs = {};
          var rawH = (init && init.headers) || (input && input.headers) || null;
          if (rawH) {
            if (typeof rawH.forEach === "function") {
              rawH.forEach(function(v, k) {
                if (String(k).toLowerCase().indexOf("x-goog-ext-") === 0) extHdrs[String(k).toLowerCase()] = String(v);
              });
            } else if (typeof rawH === "object") {
              for (var hk in rawH) {
                if (Object.prototype.hasOwnProperty.call(rawH, hk) && hk.toLowerCase().indexOf("x-goog-ext-") === 0) {
                  extHdrs[hk.toLowerCase()] = String(rawH[hk]);
                }
              }
            }
          }
          recordSniffedProto(urlStr, extHdrs);
        }
      } catch (_) {}
      return origFetch.apply(this, arguments);
    };
  }
  if (typeof XMLHttpRequest !== "undefined") {
    var origXhrOpen = XMLHttpRequest.prototype.open;
    var origXhrSetH = XMLHttpRequest.prototype.setRequestHeader;
    var origXhrSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.open = function(m, u) {
      this.__dshUrl = String(u || "");
      this.__dshHdrs = {};
      return origXhrOpen.apply(this, arguments);
    };
    XMLHttpRequest.prototype.setRequestHeader = function(k, v) {
      if (this.__dshHdrs && String(k).toLowerCase().indexOf("x-goog-ext-") === 0) {
        this.__dshHdrs[String(k).toLowerCase()] = String(v);
      }
      return origXhrSetH.apply(this, arguments);
    };
    XMLHttpRequest.prototype.send = function() {
      try {
        if (this.__dshUrl) {
          recordSniffedProto(this.__dshUrl, this.__dshHdrs || {});
        }
      } catch (_) {}
      return origXhrSend.apply(this, arguments);
    };
  }

  // Live otAQ7b dynamic model, policy & tier capacity discovery
  window.__dshDiscoverAccountModels = async function(forceRun) {
    try {
      var nowMs = Date.now();
      if (!forceRun && window.__dshModelsDiscoveredAt > 0 && (nowMs - window.__dshModelsDiscoveredAt) < 1800000) {
        return;
      }
      var w = window.WIZ_global_data || {};
      var at = String(w.SNlM0e || "");
      var bl = String(w.cfb2h || window.__dshSniffedBl || "");
      var sid = String(w.FdrFJe || "");
      if (!at || !bl || location.hostname !== "gemini.google.com") return;
      window.__dshModelsDiscoveredAt = nowMs;

      var uInfo = detectAuthUserPrefix();
      var srcPath = (uInfo.prefix || "") + "/app";
      var reqNum = Math.floor(100000 + Math.random() * 800000);
      var qs = "rpcids=otAQ7b&source-path=" + encodeURIComponent(srcPath)
             + "&bl=" + encodeURIComponent(bl)
             + (sid ? "&f.sid=" + encodeURIComponent(sid) : "")
             + "&hl=en&_reqid=" + String(reqNum) + "&rt=c";
      var url = "https://gemini.google.com" + uInfo.prefix + "/_/BardChatUi/data/batchexecute?" + qs;
      var sniffed525 = (window.__dshSniffedHeaders && window.__dshSniffedHeaders["x-goog-ext-525001261-jspb"])
                     || "[1,null,null,null,null,null,null,null,[4]]";
      var hdrs = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "X-Same-Domain": "1",
        "x-goog-ext-525001261-jspb": sniffed525,
        "x-goog-ext-73010989-jspb": "[0]"
      };
      if (uInfo.authUser) {
        hdrs["X-Goog-AuthUser"] = uInfo.authUser;
      }
      var fReq = JSON.stringify([[["otAQ7b", "[]", null, "generic"]]]);
      var body = "f.req=" + encodeURIComponent(fReq) + "&at=" + encodeURIComponent(at) + "&";
      var resp = await origFetch.call(window, url, {
        method: "POST",
        credentials: "include",
        headers: hdrs,
        body: body,
        __dshInternal: true
      });
      if (!resp.ok) return;
      var raw = await resp.text();
      var lines = raw.split("\\n");
      var capacity = [];
      var models = {};
      var policyModels = [];
      var hexRe = /^[0-9a-f]{16}$/i;

      var KNOWN_MODEL_BY_HEX = {
        "95a4c0abcba24364": {label: "Carbon", mode: 8},
        "1bc6b5d98741cd3d": {label: "4 Argon", mode: 8},
        "e6fa609c3fa255c0": {label: "3.1 Pro", mode: 3},
        "56fdd199312815e2": {label: "4 Hydrogen", mode: 1},
        "1d44b34bcaa1c04d": {label: "Flash-Lite", mode: 6}
      };

      function walkNode(node) {
        if (!Array.isArray(node)) return;
        if (node.length > 0 && node.length <= 64) {
          var allInts = true;
          var has4 = false;
          for (var j = 0; j < node.length; j++) {
            if (typeof node[j] !== "number" || !Number.isInteger(node[j])) { allInts = false; break; }
            if (node[j] === 4) has4 = true;
          }
          if (allInts && has4 && node.length > capacity.length) {
            capacity = node.slice();
          }
        }
        if (node.length >= 2 && typeof node[0] === "string") {
          var id0 = node[0];
          var idLow = id0.toLowerCase();
          if (hexRe.test(id0) || id0 === "argon" || id0 === "Argon4p0" || id0 === "carbon") {
            var labelStr = "";
            var altHex = "";
            var isSynthetic = false;
            var explicitMode = (node.length > 17 && typeof node[17] === "number" && Number.isInteger(node[17])) ? node[17] : 0;
            for (var si = 1; si < Math.min(node.length, 8); si++) {
              var v = node[si];
              if (typeof v === "string") {
                if (hexRe.test(v)) {
                  if (!altHex) altHex = v;
                } else if (!labelStr && v.length >= 2 && v.length <= 48) {
                  labelStr = v;
                }
              }
            }
            var knownMeta = KNOWN_MODEL_BY_HEX[idLow] || null;
            if (!labelStr && knownMeta) {
              labelStr = knownMeta.label;
            } else if (!labelStr && altHex) {
              labelStr = "Model_" + id0.slice(0, 6);
              isSynthetic = true;
            }
            if (labelStr) {
              var low = labelStr.toLowerCase();
              var mode = explicitMode || (knownMeta ? knownMeta.mode : 0);
              if (mode === 0) {
                if (low.indexOf("carbon") !== -1 || low.indexOf("argon") !== -1 || low.indexOf("ultra") !== -1) mode = 8;
                else if (low.indexOf("pro") !== -1 || low.indexOf("thinking") !== -1) mode = 3;
                else if (low.indexOf("lite") !== -1) mode = 6;
                else if (low.indexOf("hydrogen") !== -1 || low.indexOf("flash") !== -1) mode = 1;
                else if (!isSynthetic) mode = 1;
              }
              models[labelStr] = {hex: id0, alt_hex: altHex, label: labelStr, mode: mode, synthetic: isSynthetic};
            }
          }
        }
        for (var c = 0; c < node.length; c++) {
          if (Array.isArray(node[c])) walkNode(node[c]);
        }
      }

      function parsePolicyBlock(p24) {
        if (!Array.isArray(p24) || p24.length < 2 || typeof p24[0] !== "string" || !Array.isArray(p24[1])) return;
        if (p24[0] !== "carbon_multi_ultra_ultra_policy" && p24[0].indexOf("policy") === -1) return;
        var items = p24[1];
        for (var pi = 0; pi < items.length; pi++) {
          var entry = items[pi];
          if (!Array.isArray(entry) || entry.length < 2) continue;
          var hdr = entry[0];
          var levels = entry[1];
          if (!Array.isArray(hdr) || hdr.length <= 7 || !Array.isArray(levels)) continue;
          policyModels.push({
            enabled: Boolean(hdr[7]),
            mode: hdr[1],
            slug: (hdr[3] !== undefined && hdr[3] !== null) ? hdr[3] : null,
            label: hdr[5] || hdr[4] || "",
            levels: levels.map(function(l) { return Array.isArray(l) ? l[0] : l; })
          });
        }
      }

      for (var li = 0; li < lines.length; li++) {
        var ln = lines[li].trim();
        if (ln.indexOf("otAQ7b") === -1 || !ln.startsWith("[")) continue;
        try {
          var frames = JSON.parse(ln);
          if (!Array.isArray(frames)) continue;
          for (var fi = 0; fi < frames.length; fi++) {
            var fr = frames[fi];
            if (Array.isArray(fr) && fr[0] === "wrb.fr" && fr[1] === "otAQ7b" && typeof fr[2] !== "undefined" && fr[2] !== null) {
              var inner = (typeof fr[2] === "string") ? JSON.parse(fr[2]) : fr[2];
              walkNode(inner);
              if (Array.isArray(inner)) {
                if (inner.length > 24 && Array.isArray(inner[24])) {
                  parsePolicyBlock(inner[24]);
                } else if (Array.isArray(inner[0]) && inner[0].length > 24 && Array.isArray(inner[0][24])) {
                  parsePolicyBlock(inner[0][24]);
                }
              }
            }
          }
        } catch (_) {}
      }

      dshPost({
        event: "proto_profile",
        bl: bl,
        auth_user: uInfo.authUser,
        capacity: capacity.length > 0 ? capacity : null,
        discovered_models: models,
        policy_models: policyModels.length > 0 ? policyModels : null,
        headers: {}
      });
    } catch (_) {}
  };

  window.__dshDeleteConversations = async function(rawCids) {
    try {
      var list = Array.isArray(rawCids) ? rawCids : [];
      var clean = [];
      var seen = {};
      for (var i = 0; i < list.length; i++) {
        var cid = String(list[i] || "").trim();
        if (/^c_[0-9a-fA-F]{8,}$/.test(cid) && !seen[cid]) {
          seen[cid] = true;
          clean.push(cid);
        }
      }
      if (!clean.length) return JSON.stringify({ok: true, deleted: [], skipped: 0});
      var w = window.WIZ_global_data || {};
      var at = String(w.SNlM0e || "");
      var bl = String(w.cfb2h || window.__dshSniffedBl || "");
      var sid = String(w.FdrFJe || "");
      if (!at || !bl || location.hostname !== "gemini.google.com") {
        return JSON.stringify({ok: false, error: "WIZ_NOT_READY", deleted: []});
      }
      var uInfo = detectAuthUserPrefix();
      var srcPath = (uInfo.prefix || "") + "/app";
      var sniffed525 = (window.__dshSniffedHeaders && window.__dshSniffedHeaders["x-goog-ext-525001261-jspb"])
                     || "[1,null,null,null,null,null,null,null,[4]]";
      var hdrs = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "X-Same-Domain": "1",
        "x-goog-ext-525001261-jspb": sniffed525,
        "x-goog-ext-73010989-jspb": "[0]"
      };
      if (uInfo.authUser) hdrs["X-Goog-AuthUser"] = uInfo.authUser;

      var deleted = [];
      var errors = [];
      var cursorIdx = 0;
      async function deleteOne(cid) {
        var reqNum = Math.floor(100000 + Math.random() * 800000);
        var qs = "rpcids=GzXR5e&source-path=" + encodeURIComponent(srcPath)
               + "&bl=" + encodeURIComponent(bl)
               + (sid ? "&f.sid=" + encodeURIComponent(sid) : "")
               + "&hl=zh-CN&_reqid=" + String(reqNum) + "&rt=c";
        var url = "https://gemini.google.com" + uInfo.prefix + "/_/BardChatUi/data/batchexecute?" + qs;
        var fReq = JSON.stringify([[["GzXR5e", JSON.stringify([cid]), null, "generic"]]]);
        var body = "f.req=" + encodeURIComponent(fReq) + "&at=" + encodeURIComponent(at) + "&";
        try {
          var resp = await origFetch.call(window, url, {
            method: "POST",
            credentials: "include",
            headers: hdrs,
            body: body,
            __dshInternal: true
          });
          var txt = await resp.text();
          if (resp.ok && txt.indexOf("GzXR5e") !== -1) {
            deleted.push(cid);
          } else {
            errors.push(cid + ":HTTP_" + String(resp.status));
          }
        } catch (oneErr) {
          errors.push(cid + ":" + String(oneErr || "fetch_err"));
        }
      }
      var workers = [];
      var workerCount = Math.min(4, clean.length);
      for (var wi = 0; wi < workerCount; wi++) {
        workers.push((async function() {
          while (cursorIdx < clean.length) {
            var myIdx = cursorIdx++;
            await deleteOne(clean[myIdx]);
          }
        })());
      }
      await Promise.all(workers);
      return JSON.stringify({ok: errors.length === 0 || deleted.length > 0, deleted: deleted, requested: clean.length, errors: errors});
    } catch (e) {
      return JSON.stringify({ok: false, error: String(e || "unknown"), deleted: []});
    }
  };

  window.__dshListRecentConversations = async function(limitCount) {
    try {
      var lim = (typeof limitCount === "number" && limitCount > 0) ? Math.min(100, Math.round(limitCount)) : 30;
      var w = window.WIZ_global_data || {};
      var at = String(w.SNlM0e || "");
      var bl = String(w.cfb2h || window.__dshSniffedBl || "");
      var sid = String(w.FdrFJe || "");
      if (!at || !bl || location.hostname !== "gemini.google.com") {
        return JSON.stringify({ok: false, error: "WIZ_NOT_READY", cids: []});
      }
      var uInfo = detectAuthUserPrefix();
      var srcPath = (uInfo.prefix || "") + "/app";
      var reqNum = Math.floor(100000 + Math.random() * 800000);
      var qs = "rpcids=MaZiqc&source-path=" + encodeURIComponent(srcPath)
             + "&bl=" + encodeURIComponent(bl)
             + (sid ? "&f.sid=" + encodeURIComponent(sid) : "")
             + "&hl=zh-CN&_reqid=" + String(reqNum) + "&rt=c";
      var url = "https://gemini.google.com" + uInfo.prefix + "/_/BardChatUi/data/batchexecute?" + qs;
      var hdrs = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "X-Same-Domain": "1",
        "x-goog-ext-73010989-jspb": "[0]"
      };
      if (uInfo.authUser) hdrs["X-Goog-AuthUser"] = uInfo.authUser;
      var fReq = JSON.stringify([[["MaZiqc", JSON.stringify([lim, null, [0, null, 1]]), null, "generic"]]]);
      var body = "f.req=" + encodeURIComponent(fReq) + "&at=" + encodeURIComponent(at) + "&";
      var resp = await origFetch.call(window, url, {
        method: "POST",
        credentials: "include",
        headers: hdrs,
        body: body,
        __dshInternal: true
      });
      var raw = await resp.text();
      var matches = raw.match(/c_[0-9a-fA-F]{12,}/g) || [];
      var uniq = [];
      var seen = {};
      for (var i = 0; i < matches.length; i++) {
        if (!seen[matches[i]]) { seen[matches[i]] = true; uniq.push(matches[i]); }
      }
      return JSON.stringify({ok: resp.ok, status: resp.status, cids: uniq});
    } catch (e) {
      return JSON.stringify({ok: false, error: String(e || "unknown"), cids: []});
    }
  };

  window.__dshRunStream = async function(cfg) {
    var ctrl = new AbortController();
    window.__dshAborts[cfg.reqId] = ctrl;
    try {
      for (var waitIdx = 0; waitIdx < 40; waitIdx++) {
        var wProbe = window.WIZ_global_data || {};
        if (String(wProbe.SNlM0e || "").length > 6) break;
        if (location.hostname.indexOf("accounts.google.com") !== -1) break;
        var signLinkNow = document.querySelector('a[href*="ServiceLogin"], a[href*="/signin"]');
        if (document.readyState === "complete" && signLinkNow) break;
        await new Promise(function(r) { setTimeout(r, 150); });
      }

      var w = window.WIZ_global_data || {};
      var at = String(w.SNlM0e || "");
      var acct = String(w.oPEP7c || "");
      var bl = String(w.cfb2h || cfg.fallbackBl || window.__dshSniffedBl || "boq_gemini-web-uiserver_20261115.13_p0");
      var sid = String(w.FdrFJe || "");
      var guestSignIn = document.querySelector('a[href*="ServiceLogin"], a[href*="/signin"]');

      if (location.hostname.indexOf("accounts.google.com") !== -1 || (!at && document.readyState === "complete" && guestSignIn)) {
        dshPost({reqId: cfg.reqId, event: "error", code: "AUTH_REQUIRED", message: "Please sign in to gemini.google.com first"});
        return;
      }
      if (!at || location.hostname !== "gemini.google.com") {
        dshPost({reqId: cfg.reqId, event: "error", code: "BRIDGE_WARMING_UP", message: "WKWebView WIZ_global_data.SNlM0e is still hydrating"});
        return;
      }
      if (guestSignIn && acct.indexOf("@") === -1 && !document.querySelector('a[href*="SignOutOptions"]') && !document.querySelector('[aria-label*="@"]')) {
        dshPost({reqId: cfg.reqId, event: "error", code: "AUTH_REQUIRED", message: "Guest session detected; please sign in"});
        return;
      }

      var uInfo = detectAuthUserPrefix();
      if (w.cfb2h) {
        dshPost({event: "proto_profile", bl: String(w.cfb2h), auth_user: uInfo.authUser, headers: window.__dshSniffedHeaders || {}});
      }
      setTimeout(function() { if (window.__dshDiscoverAccountModels) window.__dshDiscoverAccountModels(false); }, 50);

      var reqNum = Math.floor(100000 + Math.random() * 800000);
      var qs = "bl=" + encodeURIComponent(bl)
             + (sid ? "&f.sid=" + encodeURIComponent(sid) : "")
             + "&hl=" + encodeURIComponent(cfg.hl || "zh-CN")
             + "&_reqid=" + String(reqNum)
             + "&rt=c";
      var url = "https://gemini.google.com" + uInfo.prefix + "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?" + qs;
      var hdrs = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "X-Same-Domain": "1"
      };
      if (uInfo.authUser) {
        hdrs["X-Goog-AuthUser"] = uInfo.authUser;
      }
      if (cfg.extraHeaders) {
        for (var k in cfg.extraHeaders) {
          if (Object.prototype.hasOwnProperty.call(cfg.extraHeaders, k)) {
            hdrs[k] = cfg.extraHeaders[k];
          }
        }
      }
      var body = "f.req=" + encodeURIComponent(cfg.fReq) + "&at=" + encodeURIComponent(at) + "&";
      var headersTimeoutMs = (typeof cfg.headersTimeoutMs === "number" && cfg.headersTimeoutMs >= 10000)
        ? Math.min(540000, Math.round(cfg.headersTimeoutMs))
        : 90000;
      var firstWrbTimeoutMs = (typeof cfg.firstWrbTimeoutMs === "number" && cfg.firstWrbTimeoutMs >= 10000)
        ? Math.min(540000, Math.round(cfg.firstWrbTimeoutMs))
        : headersTimeoutMs;
      var idleTimeoutMs = (typeof cfg.idleTimeoutMs === "number" && cfg.idleTimeoutMs >= 10000)
        ? Math.min(540000, Math.round(cfg.idleTimeoutMs))
        : 240000;
      var headersTimer = setTimeout(function() {
        window.__dshAbortReasons[cfg.reqId] = "FETCH_TIMEOUT";
        try { ctrl.abort(); } catch (_) {}
      }, headersTimeoutMs);
      var resp;
      try {
        resp = await origFetch.call(window, url, {
          method: "POST",
          credentials: "include",
          headers: hdrs,
          body: body,
          signal: ctrl.signal,
          __dshInternal: true
        });
      } finally {
        clearTimeout(headersTimer);
      }
      dshPost({reqId: cfg.reqId, event: "http_status", status: resp.status});
      if (!resp.ok || !resp.body) {
        var errTxt = "";
        try { errTxt = await resp.text(); } catch (_) {}
        dshPost({reqId: cfg.reqId, event: "error", code: "HTTP_" + String(resp.status), message: errTxt.slice(0, 800)});
        return;
      }
      var reader = resp.body.getReader();
      var dec = new TextDecoder("utf-8");
      var wrbDec = createWrbDecoder(cfg.reqId, ctrl);
      var streamOpenedAt = Date.now();
      var lastSemanticAt = streamOpenedAt;
      var sawRawWrb = false;
      while (true) {
        var nowMs = Date.now();
        var hasSemanticNow = cfg.emitRawChunks ? sawRawWrb : wrbDec.hasSawWrb();
        var stepBudgetMs = hasSemanticNow
          ? (idleTimeoutMs - (nowMs - lastSemanticAt))
          : Math.min(idleTimeoutMs, firstWrbTimeoutMs - (nowMs - streamOpenedAt));
        if (stepBudgetMs <= 0) {
          var stallReason = hasSemanticNow ? "STREAM_IDLE_TIMEOUT" : "FIRST_TOKEN_TIMEOUT";
          window.__dshAbortReasons[cfg.reqId] = stallReason;
          try { ctrl.abort(); } catch (_) {}
          break;
        }
        var readTimer = null;
        var step = await Promise.race([
          reader.read().then(function(r) {
            if (readTimer !== null) clearTimeout(readTimer);
            return r;
          }, function(e) {
            if (readTimer !== null) clearTimeout(readTimer);
            throw e;
          }),
          new Promise(function(_, reject) {
            var reasonOnFire = hasSemanticNow ? "STREAM_IDLE_TIMEOUT" : "FIRST_TOKEN_TIMEOUT";
            readTimer = setTimeout(function() {
              window.__dshAbortReasons[cfg.reqId] = reasonOnFire;
              try { ctrl.abort(); } catch (_) {}
              reject(new Error("StreamGenerate " + reasonOnFire + " after " + Math.round(stepBudgetMs / 1000) + "s"));
            }, Math.max(250, stepBudgetMs));
          })
        ]);
        if (step.done) break;
        if (ctrl.signal.aborted) break;
        var txt = dec.decode(step.value, {stream: true});
        if (txt) {
          if (cfg.emitRawChunks) {
            if (txt.indexOf("wrb.fr") !== -1) {
              sawRawWrb = true;
              lastSemanticAt = Date.now();
            }
            dshPost({reqId: cfg.reqId, event: "raw_chunk", data: txt});
          } else {
            var prevContentFlag = wrbDec.hasEmittedContent();
            var prevWrbFlag = wrbDec.hasSawWrb();
            wrbDec.feed(txt);
            if (wrbDec.hasSawWrb() && (!prevWrbFlag || wrbDec.hasEmittedContent() !== prevContentFlag || txt.indexOf("wrb.fr") !== -1)) {
              lastSemanticAt = Date.now();
              dshPost({reqId: cfg.reqId, event: "wrb_heartbeat"});
            }
            if (wrbDec.hasFatalError()) {
              try { await reader.cancel(); } catch (_) {}
              break;
            }
          }
        }
      }
      if (ctrl.signal.aborted) {
        var abortCode = window.__dshAbortReasons[cfg.reqId] || "CLIENT_ABORTED";
        var abortMsg = abortCode === "FETCH_TIMEOUT"
          ? ("StreamGenerate headers timed out after " + Math.round(headersTimeoutMs / 1000) + "s")
          : (abortCode === "FIRST_TOKEN_TIMEOUT"
            ? ("StreamGenerate produced zero wrb.fr frames within " + Math.round(firstWrbTimeoutMs / 1000) + "s (BOQ heartbeat stall)")
            : (abortCode === "STREAM_IDLE_TIMEOUT" ? ("StreamGenerate semantic progress stalled for " + Math.round(idleTimeoutMs / 1000) + "s") : "Stream aborted"));
        dshPost({reqId: cfg.reqId, event: "error", code: abortCode, message: abortMsg});
        return;
      }
      var tail = dec.decode();
      if (tail && !(!cfg.emitRawChunks && wrbDec.hasFatalError())) {
        if (cfg.emitRawChunks) {
          dshPost({reqId: cfg.reqId, event: "raw_chunk", data: tail});
        } else {
          wrbDec.feed(tail);
        }
      }
      var sawWrb = cfg.emitRawChunks ? true : wrbDec.flushCursorOnlyIfNeeded();
      var fatal = cfg.emitRawChunks ? null : wrbDec.getFatalError();
      if (fatal && (!sawWrb || !wrbDec.hasEmittedContent() || fatal.explicitRpcError || fatal.reload)) {
        dshPost({reqId: cfg.reqId, event: "error", code: fatal.code, message: fatal.message, reload: Boolean(fatal.reload)});
        return;
      }
      if (!sawWrb) {
        var stillHasAt = Boolean(window.WIZ_global_data && window.WIZ_global_data.SNlM0e);
        var emptyCode = !stillHasAt ? "AUTH_REQUIRED" : (cfg.hasCursor ? "STALE_CURSOR_OR_EMPTY" : "TRANSIENT_EMPTY_STREAM");
        dshPost({reqId: cfg.reqId, event: "error", code: emptyCode, message: "StreamGenerate returned no wrb.fr frames"});
        return;
      }
      dshPost({reqId: cfg.reqId, event: "done"});
    } catch (err) {
      var errStr = String(err || "");
      var reason = window.__dshAbortReasons[cfg.reqId];
      var code;
      if (reason === "FETCH_TIMEOUT" || reason === "FIRST_TOKEN_TIMEOUT" || reason === "STREAM_IDLE_TIMEOUT" || reason === "CLIENT_ABORTED") {
        code = reason;
      } else {
        code = (err && err.name === "AbortError") || errStr.indexOf("AbortError") !== -1 ? "CLIENT_ABORTED" : "FETCH_ERR";
      }
      dshPost({reqId: cfg.reqId, event: "error", code: code, message: errStr});
    } finally {
      delete window.__dshAborts[cfg.reqId];
      delete window.__dshAbortReasons[cfg.reqId];
    }
  };
})();
"""

final class WebSlot: NSObject, WKNavigationDelegate, WKUIDelegate, WKScriptMessageHandler {
    let slotIndex: Int
    let webView: WKWebView
    let hiddenWindow: NSWindow
    var pageLoaded: Bool = false
    var wizHydrated: Bool = false
    var cachedSNlM0e: String = ""
    var cachedBl: String = ""
    var cachedSid: String = ""
    var cachedUser: String = ""
    var cachedAuthUserIdx: String = ""
    var activeStreams: Int = 0
    var activeReqIds: Set<String> = []
    var drainingReqIds: Set<String> = []
    private var reqWatchdogs: [String: DispatchWorkItem] = [:]
    private var reqIdleTimeoutSec: [String: TimeInterval] = [:]
    private var drainWorkItems: [String: DispatchWorkItem] = [:]
    var lastActiveAt: Date = Date()
    var lastPageLoadAt: Date = Date()
    private var crashCount: Int = 0
    private var navFailureCount: Int = 0
    var consecutiveFetchFailures: Int = 0
    private var pendingRetryWorkItem: DispatchWorkItem?
    var needsReloadWhenIdle: Bool = false
    private var isDestroyed: Bool = false
    weak var bridge: DSHWebKitBridge?

    var effectiveForegroundStreams: Int {
        return activeReqIds.subtracting(drainingReqIds).count
    }

    init(slotIndex: Int, dataStore: WKWebsiteDataStore, bridge: DSHWebKitBridge) {
        self.slotIndex = slotIndex
        self.bridge = bridge

        let cfg = WKWebViewConfiguration()
        cfg.websiteDataStore = dataStore
        let ucc = WKUserContentController()
        let userScript = WKUserScript(
            source: RESIDENT_BRIDGE_USER_SCRIPT,
            injectionTime: .atDocumentStart,
            forMainFrameOnly: true
        )
        ucc.addUserScript(userScript)
        cfg.userContentController = ucc

        let rect = NSRect(x: -16000, y: -16000, width: 1100, height: 800)
        self.webView = WKWebView(frame: rect, configuration: cfg)
        self.webView.customUserAgent = SAFARI_USER_AGENT

        self.hiddenWindow = NSWindow(
            contentRect: rect,
            styleMask: [.borderless],
            backing: .buffered,
            defer: true
        )
        self.hiddenWindow.isReleasedWhenClosed = false
        self.hiddenWindow.contentView = self.webView

        super.init()
        ucc.add(self, name: "dshBridge")
        self.webView.navigationDelegate = self
        self.webView.uiDelegate = self
    }

    func destroySlot() {
        isDestroyed = true
        pendingRetryWorkItem?.cancel()
        pendingRetryWorkItem = nil
        for (_, item) in reqWatchdogs { item.cancel() }
        reqWatchdogs.removeAll()
        reqIdleTimeoutSec.removeAll()
        for (_, item) in drainWorkItems { item.cancel() }
        drainWorkItems.removeAll()
        drainingReqIds.removeAll()
        webView.stopLoading()
        webView.configuration.userContentController.removeScriptMessageHandler(forName: "dshBridge")
        webView.navigationDelegate = nil
        webView.uiDelegate = nil
        hiddenWindow.contentView = nil
        hiddenWindow.close()
    }

    func loadGeminiApp() {
        guard !isDestroyed else { return }
        navFailureCount = 0
        needsReloadWhenIdle = false
        performLoadGeminiApp()
    }

    private func performLoadGeminiApp() {
        guard !isDestroyed else { return }
        pendingRetryWorkItem?.cancel()
        pendingRetryWorkItem = nil
        pageLoaded = false
        wizHydrated = false
        lastPageLoadAt = Date()
        if let url = URL(string: GEMINI_APP_URL) {
            webView.load(URLRequest(url: url))
        }
    }

    func clearCachedAuth() {
        wizHydrated = false
        cachedSNlM0e = ""
        cachedUser = ""
    }

    private func cancelReqTimers(reqId: String) {
        reqWatchdogs.removeValue(forKey: reqId)?.cancel()
        reqIdleTimeoutSec.removeValue(forKey: reqId)
        drainWorkItems.removeValue(forKey: reqId)?.cancel()
        drainingReqIds.remove(reqId)
    }

    private func armReqWatchdog(reqId: String, timeoutSec: TimeInterval) {
        guard !isDestroyed, activeReqIds.contains(reqId) else { return }
        reqWatchdogs.removeValue(forKey: reqId)?.cancel()
        let item = DispatchWorkItem { [weak self] in
            guard let self = self, !self.isDestroyed, self.activeReqIds.contains(reqId) else { return }
            self.cancelReqTimers(reqId: reqId)
            self.activeReqIds.remove(reqId)
            self.activeStreams = self.activeReqIds.count
            self.consecutiveFetchFailures += 1
            self.needsReloadWhenIdle = true
            let abortJS = "window.__dshAbortReasons = window.__dshAbortReasons || {}; window.__dshAbortReasons[rid] = 'STREAM_IDLE_TIMEOUT'; if (window.__dshAborts && window.__dshAborts[rid]) { try { window.__dshAborts[rid].abort(); } catch (_) {} delete window.__dshAborts[rid]; }"
            self.webView.callAsyncJavaScript(abortJS, arguments: ["rid": reqId], in: nil, in: .page, completionHandler: nil)
            self.bridge?.deliverStreamEvent(reqId: reqId, dict: [
                "reqId": reqId,
                "event": "error",
                "code": "STREAM_IDLE_TIMEOUT",
                "message": "Native WebSlot watchdog fired after \(Int(timeoutSec))s stall"
            ])
            if self.needsReloadWhenIdle && self.activeStreams == 0 {
                self.needsReloadWhenIdle = false
                self.loadGeminiApp()
            }
        }
        reqWatchdogs[reqId] = item
        DispatchQueue.main.asyncAfter(deadline: .now() + max(5.0, timeoutSec), execute: item)
    }

    func scheduleBackgroundDrain(reqId: String, maxDrainMs: Int) {
        guard !isDestroyed, activeReqIds.contains(reqId) else { return }
        drainingReqIds.insert(reqId)
        drainWorkItems.removeValue(forKey: reqId)?.cancel()
        let delaySec = min(15.0, max(0.2, Double(maxDrainMs) / 1000.0))
        let item = DispatchWorkItem { [weak self] in
            guard let self = self, !self.isDestroyed else { return }
            self.drainWorkItems.removeValue(forKey: reqId)
            if self.activeReqIds.contains(reqId) {
                self.abortStream(reqId: reqId)
            }
        }
        drainWorkItems[reqId] = item
        DispatchQueue.main.asyncAfter(deadline: .now() + delaySec, execute: item)
    }

    func abortStream(reqId: String) {
        guard !isDestroyed else { return }
        cancelReqTimers(reqId: reqId)
        if activeReqIds.remove(reqId) != nil {
            activeStreams = activeReqIds.count
            lastActiveAt = Date()
        }
        let body = "window.__dshAbortReasons = window.__dshAbortReasons || {}; window.__dshAbortReasons[rid] = 'CLIENT_ABORTED'; if (window.__dshAborts && window.__dshAborts[rid]) { try { window.__dshAborts[rid].abort(); } catch (_) {} delete window.__dshAborts[rid]; }"
        webView.callAsyncJavaScript(body, arguments: ["rid": reqId], in: nil, in: .page, completionHandler: nil)
        if needsReloadWhenIdle && activeStreams == 0 {
            needsReloadWhenIdle = false
            loadGeminiApp()
        }
    }

    private func failActiveRequests(code: String, message: String) {
        let orphaned = Array(activeReqIds)
        for rid in orphaned {
            cancelReqTimers(reqId: rid)
        }
        activeReqIds.removeAll()
        activeStreams = 0
        for rid in orphaned {
            bridge?.deliverStreamEvent(reqId: rid, dict: [
                "reqId": rid,
                "event": "error",
                "code": code,
                "message": message
            ])
        }
    }

    private func scheduleReloadBackoff(delay: TimeInterval) {
        pendingRetryWorkItem?.cancel()
        let item = DispatchWorkItem { [weak self] in
            guard let self = self, !self.isDestroyed else { return }
            self.pendingRetryWorkItem = nil
            self.performLoadGeminiApp()
        }
        pendingRetryWorkItem = item
        DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: item)
    }

    private func handleNavigationFailure(context: String, error: Error) {
        guard !isDestroyed else { return }
        let nsErr = error as NSError
        if (nsErr.domain == NSURLErrorDomain && nsErr.code == NSURLErrorCancelled)
            || (nsErr.domain == "WebKitErrorDomain" && nsErr.code == 102) {
            return
        }
        pageLoaded = false
        failActiveRequests(
            code: "NAVIGATION_FAILED",
            message: "WKWebView \(context) failed: \(error.localizedDescription)"
        )
        guard navFailureCount < 6 else {
            fputs("[dsh-webkit-bridge] WebSlot #\(slotIndex) \(context) failed (\(error.localizedDescription)); reached max 6 retries\n", stderr)
            return
        }
        navFailureCount += 1
        let backoff = min(30.0, pow(2.0, Double(navFailureCount - 1)) * 0.8)
        fputs("[dsh-webkit-bridge] WebSlot #\(slotIndex) \(context) failed (\(error.localizedDescription)); retry \(navFailureCount)/6 in \(String(format: "%.1f", backoff))s\n", stderr)
        scheduleReloadBackoff(delay: backoff)
    }

    func webView(_ webView: WKWebView, createWebViewWith configuration: WKWebViewConfiguration, for navigationAction: WKNavigationAction, windowFeatures: WKWindowFeatures) -> WKWebView? {
        if navigationAction.targetFrame == nil {
            webView.load(navigationAction.request)
        }
        return nil
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        guard !isDestroyed else { return }
        pendingRetryWorkItem?.cancel()
        pendingRetryWorkItem = nil
        pageLoaded = true
        crashCount = 0
        navFailureCount = 0
        consecutiveFetchFailures = 0
        needsReloadWhenIdle = false
        refreshWizTokens { [weak self] authed in
            guard let self = self, !self.isDestroyed else { return }
            if authed {
                let discoverJS = "if (window.__dshDiscoverAccountModels) { window.__dshDiscoverAccountModels(true); }"
                self.webView.evaluateJavaScript(discoverJS, completionHandler: nil)
            } else {
                // Allow async WIZ_global_data hydration to settle shortly after initial DOM finish.
                DispatchQueue.main.asyncAfter(deadline: .now() + 1.2) { [weak self] in
                    guard let self = self, !self.isDestroyed, self.pageLoaded, self.cachedSNlM0e.isEmpty else { return }
                    self.refreshWizTokens { [weak self] authedRetry in
                        guard let self = self, authedRetry, !self.isDestroyed else { return }
                        let discoverJS = "if (window.__dshDiscoverAccountModels) { window.__dshDiscoverAccountModels(true); }"
                        self.webView.evaluateJavaScript(discoverJS, completionHandler: nil)
                    }
                }
            }
        }
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        handleNavigationFailure(context: "navigation", error: error)
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        handleNavigationFailure(context: "provisional navigation", error: error)
    }

    func webViewWebContentProcessDidTerminate(_ webView: WKWebView) {
        guard !isDestroyed else { return }
        pageLoaded = false
        clearCachedAuth()
        failActiveRequests(
            code: "WEBCONTENT_TERMINATED",
            message: "WKWebView WebContent process terminated; slot auto-reloading"
        )
        crashCount = min(crashCount + 1, 6)
        let backoff = min(15.0, pow(1.8, Double(crashCount - 1)) * 0.6)
        fputs("[dsh-webkit-bridge] WebSlot #\(slotIndex) WebContent terminated; auto-healing in \(String(format: "%.1f", backoff))s\n", stderr)
        scheduleReloadBackoff(delay: backoff)
    }

    func refreshWizTokens(completion: ((Bool) -> Void)?) {
        guard !isDestroyed else {
            completion?(false)
            return
        }
        let js = """
        (function() {
          try {
            if (location.hostname !== "gemini.google.com") {
              return JSON.stringify({signedIn: false, hydrating: document.readyState !== "complete", at: "", bl: "", sid: "", acct: "", authUser: ""});
            }
            var w = window.WIZ_global_data || {};
            var at = String(w.SNlM0e || "");
            var acct = String(w.oPEP7c || "");
            if (acct.indexOf("@") === -1) {
              var badgeEl = document.querySelector('[aria-label*="@"]');
              if (badgeEl) {
                var lbl = badgeEl.getAttribute('aria-label') || '';
                var m = lbl.match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}/);
                if (m) { acct = m[0]; }
              }
            }
            var um = (location.pathname || "").match(/^\\/u\\/(\\d+)(?:\\/|$)/);
            var authUser = um ? String(um[1]) : "";
            var hasSignInLink = Boolean(document.querySelector('a[href*="ServiceLogin"], a[href*="/signin"]'));
            var hasAccountBadge = Boolean(
              acct.indexOf("@") !== -1 ||
              document.querySelector('a[href*="SignOutOptions"]') ||
              document.querySelector('[aria-label*="@"]') ||
              (!hasSignInLink && at.length > 10)
            );
            return JSON.stringify({
              signedIn: Boolean(at && hasAccountBadge),
              hydrating: Boolean(!at && document.readyState !== "complete" && !hasSignInLink),
              at: at,
              bl: String(w.cfb2h || ""),
              sid: String(w.FdrFJe || ""),
              acct: acct,
              authUser: authUser
            });
          } catch (e) {
            return JSON.stringify({signedIn: false, hydrating: false, at: "", err: String(e)});
          }
        })();
        """
        webView.evaluateJavaScript(js) { [weak self] res, _ in
            guard let self = self, !self.isDestroyed else {
                completion?(false)
                return
            }
            if let s = res as? String,
               let data = s.data(using: .utf8),
               let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] {
                let signedIn = (obj["signedIn"] as? Bool) ?? false
                let hydrating = (obj["hydrating"] as? Bool) ?? false
                let at = (obj["at"] as? String) ?? ""
                let bl = (obj["bl"] as? String) ?? ""
                let sid = (obj["sid"] as? String) ?? ""
                let acct = (obj["acct"] as? String) ?? ""
                let authUser = (obj["authUser"] as? String) ?? ""
                if !bl.isEmpty || !authUser.isEmpty {
                    self.bridge?.persistProtoProfile(bl: bl, authUser: authUser, capacity: nil, discoveredModels: nil, policyModels: nil, headers: [:])
                }
                if signedIn && !at.isEmpty {
                    self.wizHydrated = true
                    self.cachedSNlM0e = at
                    self.cachedBl = bl
                    self.cachedSid = sid
                    self.cachedAuthUserIdx = authUser
                    if !acct.isEmpty {
                        self.cachedUser = acct
                    }
                    completion?(true)
                    return
                }
                if hydrating || !self.pageLoaded {
                    completion?(self.wizHydrated && !self.cachedSNlM0e.isEmpty)
                    return
                }
            }
            if !self.pageLoaded {
                completion?(self.wizHydrated && !self.cachedSNlM0e.isEmpty)
                return
            }
            self.clearCachedAuth()
            completion?(false)
        }
    }

    func startStreamGenerate(
        reqId: String,
        fReqJson: String,
        extraHeaders: [String: String],
        hl: String,
        fallbackBl: String,
        hasCursor: Bool,
        emitRawChunks: Bool = false,
        headersTimeoutMs: Int = 90000,
        firstWrbTimeoutMs: Int = 90000,
        idleTimeoutMs: Int = 120000
    ) {
        guard !isDestroyed else {
            bridge?.deliverStreamEvent(reqId: reqId, dict: [
                "reqId": reqId,
                "event": "error",
                "code": "SLOT_DESTROYED",
                "message": "WKWebView slot was destroyed before stream launch"
            ])
            return
        }
        activeReqIds.insert(reqId)
        activeStreams = activeReqIds.count
        lastActiveAt = Date()
        let idleSec = max(15.0, Double(idleTimeoutMs) / 1000.0)
        let initSec = max(Double(headersTimeoutMs), Double(firstWrbTimeoutMs)) / 1000.0 + 20.0
        reqIdleTimeoutSec[reqId] = idleSec + 20.0
        armReqWatchdog(reqId: reqId, timeoutSec: max(initSec, idleSec + 20.0))

        let cfgDict: [String: Any] = [
            "reqId": reqId,
            "fReq": fReqJson,
            "extraHeaders": extraHeaders,
            "hl": hl,
            "fallbackBl": fallbackBl,
            "hasCursor": hasCursor,
            "emitRawChunks": emitRawChunks,
            "headersTimeoutMs": headersTimeoutMs,
            "firstWrbTimeoutMs": firstWrbTimeoutMs,
            "idleTimeoutMs": idleTimeoutMs
        ]

        let asyncBody = """
        if (typeof window.__dshRunStream !== "function") {
          throw new Error("DSH resident stream helper not initialized on page");
        }
        await window.__dshRunStream(cfg);
        """

        webView.callAsyncJavaScript(
            asyncBody,
            arguments: ["cfg": cfgDict],
            in: nil,
            in: .page
        ) { [weak self] result in
            guard let self = self else { return }
            switch result {
            case .failure(let err):
                self.cancelReqTimers(reqId: reqId)
                if self.activeReqIds.remove(reqId) != nil {
                    self.activeStreams = self.activeReqIds.count
                    self.lastActiveAt = Date()
                }
                self.consecutiveFetchFailures += 1
                self.needsReloadWhenIdle = true
                self.bridge?.deliverStreamEvent(reqId: reqId, dict: [
                    "reqId": reqId,
                    "event": "error",
                    "code": "JS_CALL_ERR",
                    "message": err.localizedDescription
                ])
                if self.activeStreams == 0 {
                    self.needsReloadWhenIdle = false
                    self.loadGeminiApp()
                }
            case .success:
                DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
                    guard let self = self else { return }
                    self.cancelReqTimers(reqId: reqId)
                    if self.activeReqIds.remove(reqId) != nil {
                        self.activeStreams = self.activeReqIds.count
                        self.lastActiveAt = Date()
                    }
                    if self.needsReloadWhenIdle && self.activeStreams == 0 {
                        self.needsReloadWhenIdle = false
                        self.loadGeminiApp()
                    }
                }
            }
        }
    }

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        guard let dict = message.body as? [String: Any],
              let ev = dict["event"] as? String else { return }

        if ev == "proto_profile" {
            let bl = (dict["bl"] as? String) ?? ""
            let authUser = (dict["auth_user"] as? String) ?? ""
            let cap = dict["capacity"] as? [Int]
            let models = dict["discovered_models"] as? [String: Any]
            let policyModels = dict["policy_models"] as? [[String: Any]]
            let hdrs = (dict["headers"] as? [String: String]) ?? [:]
            if !bl.isEmpty {
                cachedBl = bl
            }
            if !authUser.isEmpty {
                cachedAuthUserIdx = authUser
            }
            bridge?.persistProtoProfile(bl: bl, authUser: authUser, capacity: cap, discoveredModels: models, policyModels: policyModels, headers: hdrs)
            return
        }

        guard let reqId = dict["reqId"] as? String else { return }
        if ev == "http_status" {
            consecutiveFetchFailures = 0
        }
        if ev == "wrb_heartbeat" {
            if let nextIdle = reqIdleTimeoutSec[reqId] {
                armReqWatchdog(reqId: reqId, timeoutSec: nextIdle)
            }
            return
        }
        if ev == "meta" || ev == "direct_delta" || ev == "raw_chunk" {
            if let nextIdle = reqIdleTimeoutSec[reqId] {
                armReqWatchdog(reqId: reqId, timeoutSec: nextIdle)
            }
        }
        var shouldReloadAfterErr = false
        if ev == "done" || ev == "error" {
            cancelReqTimers(reqId: reqId)
            if activeReqIds.remove(reqId) != nil {
                activeStreams = activeReqIds.count
            }
            lastActiveAt = Date()
            if ev == "done" {
                consecutiveFetchFailures = 0
            } else if let code = dict["code"] as? String {
                if code == "AUTH_REQUIRED" || code == "HTTP_401" || code == "HTTP_403" {
                    clearCachedAuth()
                    if code == "AUTH_REQUIRED" || ((dict["reload"] as? Bool) ?? false) {
                        needsReloadWhenIdle = true
                    }
                } else if code == "FETCH_ERR" || code == "FETCH_TIMEOUT" || code == "FIRST_TOKEN_TIMEOUT" || code == "STREAM_IDLE_TIMEOUT" || code == "TRANSIENT_EMPTY_STREAM" {
                    consecutiveFetchFailures += 1
                    needsReloadWhenIdle = true
                }
            }
            if needsReloadWhenIdle && activeStreams == 0 {
                needsReloadWhenIdle = false
                shouldReloadAfterErr = true
            }
        }
        bridge?.deliverStreamEvent(reqId: reqId, dict: dict)
        if shouldReloadAfterErr {
            loadGeminiApp()
        }
    }
}

final class DSHWebKitBridge: NSObject, NSApplicationDelegate, NSWindowDelegate, WKNavigationDelegate, WKUIDelegate {
    let socketPath: String
    let autoPopupLogin: Bool
    let protoProfilePath: String
    let lockFd: Int32
    let minSlots: Int = 1
    let maxSlots: Int = 4
    let idleSlotEvictSec: TimeInterval = 300.0
    private var nextSlotId: Int = 0
    private var activityToken: NSObjectProtocol?
    private var dataStore: WKWebsiteDataStore!
    private var slots: [WebSlot] = []
    private var loginWindow: NSWindow?
    private var loginWebView: WKWebView?
    private var loginPollTimer: Timer?
    private var isHidingLoginWindow: Bool = false
    private var serverFd: Int32 = -1
    private var sigtermSource: DispatchSourceSignal?
    private var sigintSource: DispatchSourceSignal?
    private var wakeObserver: NSObjectProtocol?
    private var isShuttingDown: Bool = false
    private var didCleanupSocket: Bool = false
    private let sessionsLock = NSLock()
    private var sessions: [String: (SocketClientSession, WebSlot)] = [:]
    private let protoWriteQueue = DispatchQueue(label: "com.dsh.webkit.protoprofile", qos: .utility)
    private var refreshTimer: Timer?
    private var lastSavedBl: String = ""
    private var lastAuthUser: String = ""
    private var lastCapacity: [Int] = []
    private var discoveredModelsCount: Int = 0
    private var lastDiscoveredModels: [String: Any] = [:]
    private var lastPolicyModels: [[String: Any]] = []
    private var protoRevision: Int = 0
    private var loggedDirectWriteFailure: Bool = false
    private var sniffedHeaders: [String: String] = [:]

    init(socketPath: String, autoPopupLogin: Bool, protoProfilePath: String, lockFd: Int32) {
        self.socketPath = socketPath
        self.autoPopupLogin = autoPopupLogin
        self.protoProfilePath = protoProfilePath
        self.lockFd = lockFd
        super.init()
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        return false
    }

    private func setupMainMenu() {
        let mainMenu = NSMenu()

        let appMenuItem = NSMenuItem()
        mainMenu.addItem(appMenuItem)
        let appMenu = NSMenu()
        appMenu.addItem(withTitle: "隐藏窗口", action: #selector(NSApplication.hide(_:)), keyEquivalent: "h")
        appMenu.addItem(NSMenuItem.separator())
        appMenu.addItem(withTitle: "关闭窗口", action: #selector(NSWindow.performClose(_:)), keyEquivalent: "w")
        appMenuItem.submenu = appMenu

        let editMenuItem = NSMenuItem()
        mainMenu.addItem(editMenuItem)
        let editMenu = NSMenu(title: "编辑")
        editMenu.addItem(withTitle: "撤销", action: Selector(("undo:")), keyEquivalent: "z")
        editMenu.addItem(withTitle: "重做", action: Selector(("redo:")), keyEquivalent: "Z")
        editMenu.addItem(NSMenuItem.separator())
        editMenu.addItem(withTitle: "剪切", action: #selector(NSText.cut(_:)), keyEquivalent: "x")
        editMenu.addItem(withTitle: "拷贝", action: #selector(NSText.copy(_:)), keyEquivalent: "c")
        editMenu.addItem(withTitle: "粘贴", action: #selector(NSText.paste(_:)), keyEquivalent: "v")
        editMenu.addItem(withTitle: "全选", action: #selector(NSText.selectAll(_:)), keyEquivalent: "a")
        editMenuItem.submenu = editMenu

        NSApplication.shared.mainMenu = mainMenu
    }

    @discardableResult
    private func spawnSlot(inheritCachedAuth: Bool = true) -> WebSlot {
        let idx = nextSlotId
        nextSlotId += 1
        let slot = WebSlot(slotIndex: idx, dataStore: dataStore, bridge: self)
        if inheritCachedAuth, let authed = slots.first(where: { !$0.cachedSNlM0e.isEmpty }) {
            slot.cachedSNlM0e = authed.cachedSNlM0e
            slot.cachedBl = authed.cachedBl
            slot.cachedSid = authed.cachedSid
            slot.cachedUser = authed.cachedUser
            slot.cachedAuthUserIdx = authed.cachedAuthUserIdx
        }
        slots.append(slot)
        slot.loadGeminiApp()
        return slot
    }

    private func reapIdleExtraSlots() {
        guard slots.count > minSlots else { return }
        let now = Date()
        var kept: [WebSlot] = []
        for (i, slot) in slots.enumerated() {
            if i < minSlots || slot.activeStreams > 0 || now.timeIntervalSince(slot.lastActiveAt) < idleSlotEvictSec {
                kept.append(slot)
            } else {
                slot.destroySlot()
            }
        }
        slots = kept
    }

    func persistProtoProfile(
        bl: String,
        authUser: String,
        capacity: [Int]?,
        discoveredModels: [String: Any]?,
        policyModels: [[String: Any]]? = nil,
        headers: [String: String]
    ) {
        var changed = false
        if !bl.isEmpty && bl != lastSavedBl {
            lastSavedBl = bl
            changed = true
        }
        if !authUser.isEmpty && authUser != lastAuthUser {
            lastAuthUser = authUser
            changed = true
        }
        if let cap = capacity, cap.count > lastCapacity.count {
            lastCapacity = cap
            changed = true
        }
        if let dm = discoveredModels, !dm.isEmpty {
            discoveredModelsCount = dm.count
            lastDiscoveredModels = dm
            changed = true
        }
        if let pm = policyModels, !pm.isEmpty {
            lastPolicyModels = pm
            changed = true
        }
        for (k, v) in headers where k.lowercased().hasPrefix("x-goog-ext-") && !v.isEmpty {
            if sniffedHeaders[k.lowercased()] != v {
                sniffedHeaders[k.lowercased()] = v
                changed = true
            }
        }
        guard changed else { return }
        protoRevision += 1
        guard !protoProfilePath.isEmpty else { return }
        let profilePath = protoProfilePath
        let snapBl = lastSavedBl
        let snapAuthUser = lastAuthUser
        let snapCap = lastCapacity
        let snapModels = discoveredModels
        let snapPolicyModels = policyModels
        let snapHdrs = sniffedHeaders
        protoWriteQueue.async { [weak self] in
            let dir = (profilePath as NSString).deletingLastPathComponent
            try? FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)

            let lockPath = profilePath + ".lock"
            let lfd = Darwin.open(lockPath, O_CREAT | O_RDWR | O_CLOEXEC, mode_t(0o600))
            if lfd >= 0 {
                setCloseOnExec(lfd)
                while posixFlock(lfd, LOCK_EX) != 0 {
                    if errno != EINTR { break }
                }
            }
            defer {
                if lfd >= 0 {
                    _ = posixFlock(lfd, LOCK_UN)
                    Darwin.close(lfd)
                }
            }

            var merged: [String: Any] = [:]
            if let oldData = try? Data(contentsOf: URL(fileURLWithPath: profilePath)),
               let oldObj = (try? JSONSerialization.jsonObject(with: oldData)) as? [String: Any] {
                merged = oldObj
            }
            if !snapBl.isEmpty {
                merged["bl"] = snapBl
            }
            if !snapAuthUser.isEmpty {
                merged["auth_user"] = snapAuthUser
            }
            if !snapCap.isEmpty {
                let prevCap = (merged["capacity"] as? [Int]) ?? []
                if snapCap.count >= prevCap.count {
                    merged["capacity"] = snapCap
                }
            }
            if let dm = snapModels, !dm.isEmpty {
                merged["discovered_models"] = dm
            }
            if let pm = snapPolicyModels, !pm.isEmpty {
                merged["policy_models"] = pm
            }
            if !snapHdrs.isEmpty {
                var curH = (merged["extra_headers"] as? [String: String]) ?? [:]
                for (k, v) in snapHdrs { curH[k] = v }
                merged["extra_headers"] = curH
            }
            merged["updated_at"] = Int(Date().timeIntervalSince1970)
            do {
                let outData = try JSONSerialization.data(withJSONObject: merged, options: [.prettyPrinted, .sortedKeys])
                let tmpPath = profilePath + ".tmp.\(ProcessInfo.processInfo.processIdentifier).\(UUID().uuidString.prefix(8))"
                do {
                    try outData.write(to: URL(fileURLWithPath: tmpPath))
                    if Darwin.rename(tmpPath, profilePath) != 0 {
                        let renErr = errno
                        _ = Darwin.unlink(tmpPath)
                        if let bridge = self, !bridge.loggedDirectWriteFailure {
                            bridge.loggedDirectWriteFailure = true
                            fputs("[dsh-webkit-bridge] rename(\(profilePath)) failed errno=\(renErr); serving via socket RPC proto_profile\n", stderr)
                        }
                    }
                } catch {
                    _ = Darwin.unlink(tmpPath)
                    if let bridge = self, !bridge.loggedDirectWriteFailure {
                        bridge.loggedDirectWriteFailure = true
                        fputs("[dsh-webkit-bridge] Direct write to \(profilePath) denied (\(error.localizedDescription)); serving via socket RPC proto_profile\n", stderr)
                    }
                }
            } catch {
                if let bridge = self, !bridge.loggedDirectWriteFailure {
                    bridge.loggedDirectWriteFailure = true
                    fputs("[dsh-webkit-bridge] JSONSerialization failed for proto_profile: \(error.localizedDescription)\n", stderr)
                }
            }
        }
    }

    private func cleanupSocketFiles() {
        guard !didCleanupSocket else { return }
        didCleanupSocket = true
        if serverFd >= 0 {
            let sfd = serverFd
            serverFd = -1
            Darwin.close(sfd)
        }
        Darwin.unlink(socketPath)
        Darwin.unlink(socketPath + ".pid")
    }

    private func initiateShutdown() {
        guard !isShuttingDown else { return }
        isShuttingDown = true
        cleanupSocketFiles()
        NSApplication.shared.terminate(nil)
    }

    private func setupSignalHandlers() {
        Darwin.signal(SIGTERM, SIG_IGN)
        Darwin.signal(SIGINT, SIG_IGN)

        let termSrc = DispatchSource.makeSignalSource(signal: SIGTERM, queue: .main)
        termSrc.setEventHandler { [weak self] in
            fputs("[dsh-webkit-bridge] Received SIGTERM; shutting down cleanly\n", stderr)
            self?.initiateShutdown()
        }
        termSrc.resume()
        sigtermSource = termSrc

        let intSrc = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
        intSrc.setEventHandler { [weak self] in
            fputs("[dsh-webkit-bridge] Received SIGINT; shutting down cleanly\n", stderr)
            self?.initiateShutdown()
        }
        intSrc.resume()
        sigintSource = intSrc
    }

    private func handleSystemWake() {
        fputs("[dsh-webkit-bridge] System wake observed; reloading idle slots to refresh HTTP/2 connections\n", stderr)
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
            guard let self = self, !self.isShuttingDown else { return }
            for slot in self.slots {
                if slot.activeStreams == 0 {
                    slot.loadGeminiApp()
                }
            }
        }
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        Darwin.signal(SIGPIPE, SIG_IGN)
        setupSignalHandlers()
        setupMainMenu()
        NSApplication.shared.setActivationPolicy(.accessory)

        activityToken = ProcessInfo.processInfo.beginActivity(
            options: [.userInitiatedAllowingIdleSystemSleep],
            reason: "DSH Pure-Web WKWebView Resident Plane"
        )

        dataStore = WKWebsiteDataStore.default()
        for _ in 0..<minSlots {
            spawnSlot(inheritCachedAuth: false)
        }

        startUnixSocketServer()

        wakeObserver = NSWorkspace.shared.notificationCenter.addObserver(
            forName: NSWorkspace.didWakeNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.handleSystemWake()
        }

        refreshTimer = Timer.scheduledTimer(withTimeInterval: 45.0, repeats: true) { [weak self] _ in
            guard let self = self else { return }
            self.reapIdleExtraSlots()
            let now = Date()
            for s in self.slots {
                if s.activeStreams == 0
                    && now.timeIntervalSince(s.lastActiveAt) >= 900.0
                    && now.timeIntervalSince(s.lastPageLoadAt) >= 900.0 {
                    s.loadGeminiApp()
                } else {
                    s.refreshWizTokens(completion: nil)
                }
            }
        }

        if autoPopupLogin {
            DispatchQueue.main.asyncAfter(deadline: .now() + 2.5) { [weak self] in
                guard let self = self else { return }
                if !self.isAnySlotAuthenticated() {
                    self.presentLoginWindow()
                }
            }
        }
    }

    func applicationWillTerminate(_ notification: Notification) {
        isShuttingDown = true
        cleanupSocketFiles()
        sigtermSource?.cancel()
        sigtermSource = nil
        sigintSource?.cancel()
        sigintSource = nil
        refreshTimer?.invalidate()
        refreshTimer = nil
        loginPollTimer?.invalidate()
        loginPollTimer = nil
        if let obs = wakeObserver {
            NSWorkspace.shared.notificationCenter.removeObserver(obs)
            wakeObserver = nil
        }
        if let tok = activityToken {
            ProcessInfo.processInfo.endActivity(tok)
            activityToken = nil
        }
    }

    func isAnySlotAuthenticated() -> Bool {
        return slots.contains { !$0.cachedSNlM0e.isEmpty }
    }

    func isAnySlotReady() -> Bool {
        return slots.contains { $0.pageLoaded && $0.wizHydrated && !$0.cachedSNlM0e.isEmpty }
    }

    private func cleanupLoginWindow(fromWindowWillClose: Bool = false) {
        loginPollTimer?.invalidate()
        loginPollTimer = nil
        if let win = loginWindow {
            win.delegate = nil
            if !fromWindowWillClose {
                win.orderOut(nil)
            }
            win.contentView = nil
            if !fromWindowWillClose {
                win.close()
            }
        }
        if let lwv = loginWebView {
            lwv.stopLoading()
            lwv.navigationDelegate = nil
            lwv.uiDelegate = nil
            if let blankURL = URL(string: "about:blank") {
                lwv.load(URLRequest(url: blankURL))
            }
        }
        loginWebView = nil
        loginWindow = nil
        isHidingLoginWindow = false
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
            NSApplication.shared.setActivationPolicy(.accessory)
        }
    }

    func windowWillClose(_ notification: Notification) {
        cleanupLoginWindow(fromWindowWillClose: true)
    }

    func webView(_ webView: WKWebView, createWebViewWith configuration: WKWebViewConfiguration, for navigationAction: WKNavigationAction, windowFeatures: WKWindowFeatures) -> WKWebView? {
        if navigationAction.targetFrame == nil {
            webView.load(navigationAction.request)
        }
        return nil
    }

    func presentLoginWindow() {
        isHidingLoginWindow = false
        NSApplication.shared.setActivationPolicy(.regular)

        if let win = loginWindow, let lwv = loginWebView {
            win.title = "Gemini 纯 Web 额度引擎 — 请点击右上角「登录」完成首次登录 (成功后自动收起)"
            win.level = .floating
            win.makeKeyAndOrderFront(nil)
            win.orderFrontRegardless()
            if #available(macOS 14.0, *) {
                NSApplication.shared.activate()
            } else {
                NSApplication.shared.activate(ignoringOtherApps: true)
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.6) { win.level = .normal }
            if lwv.url == nil, let u = URL(string: GEMINI_APP_URL) {
                lwv.load(URLRequest(url: u))
            } else if lwv.url?.host == "gemini.google.com" {
                let autoClickSignInJS = """
                (function() {
                  var w = window.WIZ_global_data || {};
                  if (String(w.oPEP7c || "").indexOf("@") !== -1) return "already_authed";
                  var links = Array.from(document.querySelectorAll('a[href]'));
                  for (var i = 0; i < links.length; i++) {
                    var h = links[i].href || "";
                    if (h.indexOf("ServiceLogin") !== -1 || h.indexOf("/signin") !== -1) {
                      location.href = h;
                      return "navigated";
                    }
                  }
                  return "stay";
                })();
                """
                lwv.evaluateJavaScript(autoClickSignInJS, completionHandler: nil)
            }
            startLoginCompletionPolling()
            return
        }

        let cfg = WKWebViewConfiguration()
        cfg.websiteDataStore = dataStore
        let rect = NSRect(x: 0, y: 0, width: 540, height: 740)
        let wv = WKWebView(frame: rect, configuration: cfg)
        wv.customUserAgent = SAFARI_USER_AGENT
        wv.navigationDelegate = self
        wv.uiDelegate = self

        let win = NSWindow(
            contentRect: rect,
            styleMask: [.titled, .closable, .resizable, .miniaturizable],
            backing: .buffered,
            defer: false
        )
        win.title = "Gemini 纯 Web 额度引擎 — 请完成 Google 账号登录 (登录成功后自动收起)"
        win.center()
        win.isReleasedWhenClosed = false
        win.delegate = self
        win.contentView = wv
        self.loginWindow = win
        self.loginWebView = wv

        win.level = .floating
        win.makeKeyAndOrderFront(nil)
        win.orderFrontRegardless()
        if #available(macOS 14.0, *) {
            NSApplication.shared.activate()
        } else {
            NSApplication.shared.activate(ignoringOtherApps: true)
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.6) { win.level = .normal }

        if let url = URL(string: GEMINI_APP_URL) {
            wv.load(URLRequest(url: url))
        }
        startLoginCompletionPolling()
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        guard webView === loginWebView, !isHidingLoginWindow else { return }
        if webView.url?.host == "gemini.google.com" {
            let autoSignInJS = """
            (function() {
              var w = window.WIZ_global_data || {};
              if (String(w.oPEP7c || "").indexOf("@") !== -1 || document.querySelector('a[href*="SignOutOptions"]')) {
                return "signed_in";
              }
              var links = Array.from(document.querySelectorAll('a[href]'));
              for (var i = 0; i < links.length; i++) {
                var h = links[i].href || "";
                if (h.indexOf("ServiceLogin") !== -1 || h.indexOf("/signin") !== -1) {
                  location.href = h;
                  return "redirecting_signin";
                }
              }
              return "guest_page";
            })();
            """
            webView.evaluateJavaScript(autoSignInJS, completionHandler: nil)
        }
    }

    private func startLoginCompletionPolling() {
        loginPollTimer?.invalidate()
        loginPollTimer = Timer.scheduledTimer(withTimeInterval: 1.5, repeats: true) { [weak self] _ in
            guard let self = self,
                  !self.isHidingLoginWindow,
                  let win = self.loginWindow,
                  win.isVisible,
                  let lwv = self.loginWebView,
                  lwv.url?.host == "gemini.google.com" else {
                return
            }
            let js = """
            (function() {
              var w = window.WIZ_global_data || {};
              var at = String(w.SNlM0e || "");
              var acct = String(w.oPEP7c || "");
              var hasAccountBadge = Boolean(
                acct.indexOf("@") !== -1 ||
                document.querySelector('a[href*="SignOutOptions"]') ||
                document.querySelector('[aria-label*="@"][role="button"]')
              );
              return JSON.stringify({
                signedIn: Boolean(at && hasAccountBadge),
                at: at,
                acct: acct
              });
            })();
            """
            lwv.evaluateJavaScript(js) { [weak self] res, _ in
                guard let self = self, !self.isHidingLoginWindow else { return }
                guard let s = res as? String,
                      let data = s.data(using: .utf8),
                      let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return }
                let signedIn = (obj["signedIn"] as? Bool) ?? false
                let at = (obj["at"] as? String) ?? ""
                let acct = (obj["acct"] as? String) ?? ""
                if signedIn && !at.isEmpty {
                    self.isHidingLoginWindow = true
                    self.loginPollTimer?.invalidate()
                    self.loginPollTimer = nil
                    for slot in self.slots {
                        slot.cachedSNlM0e = at
                        slot.cachedUser = acct
                        slot.loadGeminiApp()
                    }
                    let label = acct.isEmpty ? "已验证账号" : acct
                    win.title = "✅ 登录成功 (\(label))！正在转入后台常驻免值守模式..."
                    DispatchQueue.main.asyncAfter(deadline: .now() + 1.4) { [weak self] in
                        self?.cleanupLoginWindow()
                    }
                }
            }
        }
    }

    private func pickOrScaleSlot() -> WebSlot {
        if slots.isEmpty {
            return spawnSlot(inheritCachedAuth: false)
        }
        if let idleHealthy = slots.first(where: {
            $0.effectiveForegroundStreams == 0 && $0.pageLoaded && $0.wizHydrated && !$0.cachedSNlM0e.isEmpty && !$0.needsReloadWhenIdle && $0.consecutiveFetchFailures == 0
        }) {
            return idleHealthy
        }
        if let idleReady = slots.first(where: {
            $0.effectiveForegroundStreams == 0 && $0.pageLoaded && $0.wizHydrated && !$0.cachedSNlM0e.isEmpty && !$0.needsReloadWhenIdle
        }) {
            return idleReady
        }
        if let idleLoaded = slots.first(where: {
            $0.effectiveForegroundStreams == 0 && $0.pageLoaded && !$0.needsReloadWhenIdle && $0.consecutiveFetchFailures == 0
        }) {
            return idleLoaded
        }
        if slots.count < maxSlots {
            return spawnSlot(inheritCachedAuth: true)
        }
        if let leastLoadedReady = slots.filter({ $0.pageLoaded && $0.wizHydrated && !$0.cachedSNlM0e.isEmpty }).min(by: { $0.effectiveForegroundStreams < $1.effectiveForegroundStreams }) {
            return leastLoadedReady
        }
        if let leastLoaded = slots.filter({ $0.pageLoaded }).min(by: { $0.effectiveForegroundStreams < $1.effectiveForegroundStreams }) {
            return leastLoaded
        }
        return slots.min(by: { $0.effectiveForegroundStreams < $1.effectiveForegroundStreams }) ?? slots[0]
    }

    private func isSessionActive(reqId: String) -> Bool {
        sessionsLock.lock()
        let active = (sessions[reqId] != nil)
        sessionsLock.unlock()
        return active
    }

    private func launchStreamWhenReady(
        slot: WebSlot,
        reqId: String,
        fReq: String,
        extraHeaders: [String: String],
        hl: String,
        fallbackBl: String,
        hasCursor: Bool,
        emitRaw: Bool,
        headersTimeoutMs: Int = 90000,
        firstWrbTimeoutMs: Int = 90000,
        idleTimeoutMs: Int = 120000,
        attempt: Int = 0
    ) {
        guard isSessionActive(reqId: reqId) else { return }

        if slot.pageLoaded && slot.wizHydrated && !slot.cachedSNlM0e.isEmpty {
            slot.startStreamGenerate(
                reqId: reqId,
                fReqJson: fReq,
                extraHeaders: extraHeaders,
                hl: hl,
                fallbackBl: fallbackBl,
                hasCursor: hasCursor,
                emitRawChunks: emitRaw,
                headersTimeoutMs: headersTimeoutMs,
                firstWrbTimeoutMs: firstWrbTimeoutMs,
                idleTimeoutMs: idleTimeoutMs
            )
            return
        }

        let maxPolls = 100
        let scheduleNext = { [weak self] in
            guard let self = self else { return }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
                self.launchStreamWhenReady(
                    slot: slot,
                    reqId: reqId,
                    fReq: fReq,
                    extraHeaders: extraHeaders,
                    hl: hl,
                    fallbackBl: fallbackBl,
                    hasCursor: hasCursor,
                    emitRaw: emitRaw,
                    headersTimeoutMs: headersTimeoutMs,
                    firstWrbTimeoutMs: firstWrbTimeoutMs,
                    idleTimeoutMs: idleTimeoutMs,
                    attempt: attempt + 1
                )
            }
        }

        if slot.pageLoaded {
            slot.refreshWizTokens { [weak self] authed in
                guard let self = self, self.isSessionActive(reqId: reqId) else { return }
                if authed || attempt >= maxPolls {
                    slot.startStreamGenerate(
                        reqId: reqId,
                        fReqJson: fReq,
                        extraHeaders: extraHeaders,
                        hl: hl,
                        fallbackBl: fallbackBl,
                        hasCursor: hasCursor,
                        emitRawChunks: emitRaw,
                        headersTimeoutMs: headersTimeoutMs,
                        firstWrbTimeoutMs: firstWrbTimeoutMs,
                        idleTimeoutMs: idleTimeoutMs
                    )
                } else {
                    scheduleNext()
                }
            }
        } else if attempt >= maxPolls {
            slot.startStreamGenerate(
                reqId: reqId,
                fReqJson: fReq,
                extraHeaders: extraHeaders,
                hl: hl,
                fallbackBl: fallbackBl,
                hasCursor: hasCursor,
                emitRawChunks: emitRaw,
                headersTimeoutMs: headersTimeoutMs,
                firstWrbTimeoutMs: firstWrbTimeoutMs,
                idleTimeoutMs: idleTimeoutMs
            )
        } else {
            scheduleNext()
        }
    }

    func detachClientForBackgroundDrain(reqId: String, maxDrainMs: Int) -> Bool {
        sessionsLock.lock()
        let entry = sessions.removeValue(forKey: reqId)
        sessionsLock.unlock()
        guard let (client, slot) = entry else { return false }
        slot.scheduleBackgroundDrain(reqId: reqId, maxDrainMs: maxDrainMs)
        client.close()
        return true
    }

    func cancelClientStream(reqId: String) {
        sessionsLock.lock()
        let entry = sessions.removeValue(forKey: reqId)
        sessionsLock.unlock()
        if let (client, slot) = entry {
            slot.abortStream(reqId: reqId)
            client.close()
        }
    }

    func deliverStreamEvent(reqId: String, dict: [String: Any]) {
        sessionsLock.lock()
        let entry = sessions[reqId]
        let ev = (dict["event"] as? String) ?? ""
        if ev == "done" || ev == "error" {
            sessions.removeValue(forKey: reqId)
        }
        sessionsLock.unlock()

        guard let (client, _) = entry else { return }
        client.sendJSON(dict)
        if ev == "done" || ev == "error" {
            client.close()
        }
    }

    private func startUnixSocketServer() {
        guard socketPath.utf8.count < 104 else {
            fputs("[dsh-webkit-bridge] socketPath exceeds 103 UTF-8 bytes: \(socketPath)\n", stderr)
            exit(1)
        }
        Darwin.unlink(socketPath)
        serverFd = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard serverFd >= 0 else {
            fputs("[dsh-webkit-bridge] socket() failed\n", stderr)
            exit(1)
        }
        setCloseOnExec(serverFd)

        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let pathBytes = socketPath.utf8CString
        withUnsafeMutablePointer(to: &addr.sun_path) { ptr in
            let rawPtr = UnsafeMutableRawPointer(ptr).assumingMemoryBound(to: CChar.self)
            for i in 0..<min(pathBytes.count, 104) {
                rawPtr[i] = pathBytes[i]
            }
        }

        let bindRes = withUnsafePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { saPtr in
                Darwin.bind(serverFd, saPtr, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard bindRes == 0 else {
            fputs("[dsh-webkit-bridge] bind(\(socketPath)) failed\n", stderr)
            exit(1)
        }
        Darwin.chmod(socketPath, mode_t(0o600))
        Darwin.listen(serverFd, 32)
        fputs("[dsh-webkit-bridge \(BRIDGE_VERSION)] Listening on \(socketPath)\n", stderr)

        DispatchQueue.global(qos: .userInteractive).async { [weak self] in
            guard let self = self else { return }
            while true {
                let listenFd = self.serverFd
                if listenFd < 0 { break }
                let cfd = Darwin.accept(listenFd, nil, nil)
                if cfd < 0 {
                    let err = errno
                    if err == EINTR { continue }
                    if self.serverFd < 0 || err == EBADF || err == EINVAL { break }
                    continue
                }
                setCloseOnExec(cfd)
                var one: Int32 = 1
                _ = Darwin.setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
                DispatchQueue.global(qos: .userInitiated).async {
                    self.handleClientConnection(fd: cfd)
                }
            }
        }
    }

    private func handleClientConnection(fd: Int32) {
        var rcvTv = timeval(tv_sec: 5, tv_usec: 0)
        _ = Darwin.setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rcvTv, socklen_t(MemoryLayout<timeval>.size))

        let deadline = CFAbsoluteTimeGetCurrent() + 5.0
        var data = Data()
        var buf = [UInt8](repeating: 0, count: 16384)
        while true {
            if CFAbsoluteTimeGetCurrent() >= deadline {
                Darwin.close(fd)
                return
            }
            let n = Darwin.read(fd, &buf, buf.count)
            if n < 0 {
                if errno == EINTR { continue }
                Darwin.close(fd)
                return
            }
            if n == 0 { break }
            data.append(buf, count: n)
            if data.contains(0x0A) { break }
            if data.count > 8 * 1024 * 1024 {
                Darwin.close(fd)
                return
            }
        }
        guard let nlIdx = data.firstIndex(of: 0x0A) else {
            Darwin.close(fd)
            return
        }
        var zeroTv = timeval(tv_sec: 0, tv_usec: 0)
        _ = Darwin.setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &zeroTv, socklen_t(MemoryLayout<timeval>.size))

        let lineData = data.subdata(in: 0..<nlIdx)
        guard let req = (try? JSONSerialization.jsonObject(with: lineData)) as? [String: Any] else {
            Darwin.close(fd)
            return
        }

        let op = (req["op"] as? String) ?? "status"
        let reqId = (req["reqId"] as? String) ?? UUID().uuidString
        let session = SocketClientSession(fd: fd, reqId: reqId)

        DispatchQueue.main.async { [weak self] in
            guard let self = self else {
                session.close()
                return
            }
            if op == "status" {
                let authed = self.isAnySlotAuthenticated()
                let ready = self.isAnySlotReady()
                let inflight = self.slots.reduce(0) { $0 + $1.activeStreams }
                let bl = self.slots.first(where: { !$0.cachedBl.isEmpty })?.cachedBl ?? self.lastSavedBl
                let acct = self.slots.first(where: { !$0.cachedUser.isEmpty })?.cachedUser ?? ""
                let authUserIdx = self.slots.first(where: { !$0.cachedAuthUserIdx.isEmpty })?.cachedAuthUserIdx ?? self.lastAuthUser
                session.sendJSON([
                    "ok": true,
                    "bridge_version": BRIDGE_VERSION,
                    "plane": "webkit_native",
                    "logged_in": authed,
                    "ready": ready,
                    "account": acct,
                    "auth_user": authUserIdx,
                    "capacity": self.lastCapacity,
                    "discovered_models_count": self.discoveredModelsCount,
                    "proto_rev": self.protoRevision,
                    "page_loaded": self.slots.contains { $0.pageLoaded },
                    "slots": self.slots.count,
                    "max_slots": self.maxSlots,
                    "inflight": inflight,
                    "bl": bl,
                    "direct_delta": true,
                    "abort_propagation": true
                ])
                session.close()
                return
            }

            if op == "proto_profile" {
                let bl = self.slots.first(where: { !$0.cachedBl.isEmpty })?.cachedBl ?? self.lastSavedBl
                let authUserIdx = self.slots.first(where: { !$0.cachedAuthUserIdx.isEmpty })?.cachedAuthUserIdx ?? self.lastAuthUser
                var payload: [String: Any] = [
                    "ok": true,
                    "proto_rev": self.protoRevision,
                    "bl": bl,
                    "auth_user": authUserIdx,
                    "capacity": self.lastCapacity,
                    "discovered_models": self.lastDiscoveredModels,
                    "extra_headers": self.sniffedHeaders
                ]
                if !self.lastPolicyModels.isEmpty {
                    payload["policy_models"] = self.lastPolicyModels
                }
                session.sendJSON(payload)
                session.close()
                return
            }

            if op == "abort" {
                let targetId = (req["targetReqId"] as? String) ?? reqId
                self.cancelClientStream(reqId: targetId)
                session.sendJSON(["ok": true, "aborted": targetId])
                session.close()
                return
            }

            if op == "background_drain" {
                let targetId = (req["targetReqId"] as? String) ?? reqId
                let maxDrainMs = (req["maxDrainMs"] as? Int) ?? (req["maxDrainMs"] as? Double).map { Int($0) } ?? 6000
                let detached = self.detachClientForBackgroundDrain(reqId: targetId, maxDrainMs: maxDrainMs)
                session.sendJSON(["ok": true, "draining": detached, "targetReqId": targetId])
                session.close()
                return
            }

            if op == "delete_conversations" {
                let cids = (req["cids"] as? [String]) ?? []
                guard let slot = self.slots.first(where: { $0.pageLoaded && $0.wizHydrated && !$0.cachedSNlM0e.isEmpty }) ?? self.slots.first(where: { $0.pageLoaded }) else {
                    session.sendJSON(["ok": false, "error": "NO_READY_SLOT", "deleted": []])
                    session.close()
                    return
                }
                let jsBody = """
                if (typeof window.__dshDeleteConversations !== "function") {
                  return JSON.stringify({ok: false, error: "HELPER_MISSING", deleted: []});
                }
                return await window.__dshDeleteConversations(cids);
                """
                slot.webView.callAsyncJavaScript(jsBody, arguments: ["cids": cids], in: nil, in: .page) { result in
                    switch result {
                    case .failure(let err):
                        session.sendJSON(["ok": false, "error": err.localizedDescription, "deleted": []])
                    case .success(let res):
                        if let s = res as? String,
                           let d = s.data(using: .utf8),
                           let obj = (try? JSONSerialization.jsonObject(with: d)) as? [String: Any] {
                            session.sendJSON(obj)
                        } else {
                            session.sendJSON(["ok": false, "error": "INVALID_JS_RESULT", "deleted": []])
                        }
                    }
                    session.close()
                }
                return
            }

            if op == "list_conversations" {
                let limit = (req["limit"] as? Int) ?? 30
                guard let slot = self.slots.first(where: { $0.pageLoaded && $0.wizHydrated && !$0.cachedSNlM0e.isEmpty }) ?? self.slots.first(where: { $0.pageLoaded }) else {
                    session.sendJSON(["ok": false, "error": "NO_READY_SLOT", "cids": []])
                    session.close()
                    return
                }
                let jsBody = """
                if (typeof window.__dshListRecentConversations !== "function") {
                  return JSON.stringify({ok: false, error: "HELPER_MISSING", cids: []});
                }
                return await window.__dshListRecentConversations(lim);
                """
                slot.webView.callAsyncJavaScript(jsBody, arguments: ["lim": limit], in: nil, in: .page) { result in
                    switch result {
                    case .failure(let err):
                        session.sendJSON(["ok": false, "error": err.localizedDescription, "cids": []])
                    case .success(let res):
                        if let s = res as? String,
                           let d = s.data(using: .utf8),
                           let obj = (try? JSONSerialization.jsonObject(with: d)) as? [String: Any] {
                            session.sendJSON(obj)
                        } else {
                            session.sendJSON(["ok": false, "error": "INVALID_JS_RESULT", "cids": []])
                        }
                    }
                    session.close()
                }
                return
            }

            if op == "open_login" {
                self.presentLoginWindow()
                session.sendJSON(["ok": true, "login_window": "opened", "bridge_version": BRIDGE_VERSION])
                session.close()
                return
            }

            if op == "switch_account" {
                for s in self.slots { s.clearCachedAuth() }
                let allTypes = WKWebsiteDataStore.allWebsiteDataTypes()
                self.dataStore.removeData(ofTypes: allTypes, modifiedSince: Date(timeIntervalSince1970: 0)) { [weak self] in
                    guard let self = self else { return }
                    for s in self.slots { s.loadGeminiApp() }
                    if let lwv = self.loginWebView, let u = URL(string: GEMINI_APP_URL) {
                        lwv.load(URLRequest(url: u))
                    }
                    self.presentLoginWindow()
                }
                session.sendJSON(["ok": true, "switched": true, "login_window": "opened", "bridge_version": BRIDGE_VERSION])
                session.close()
                return
            }

            if op == "auth_invalidate" {
                for s in self.slots { s.clearCachedAuth() }
                session.sendJSON(["ok": true, "invalidated": true])
                session.close()
                return
            }

            if op == "reload" {
                for s in self.slots { s.loadGeminiApp() }
                session.sendJSON(["ok": true, "reloaded": true])
                session.close()
                return
            }

            if op == "shutdown" {
                self.isShuttingDown = true
                self.cleanupSocketFiles()
                session.sendJSON(["ok": true, "shutting_down": true])
                session.close {
                    DispatchQueue.main.async {
                        NSApplication.shared.terminate(nil)
                    }
                }
                return
            }

            if op == "stream_generate" {
                let fReq = (req["fReq"] as? String) ?? "[]"
                let hl = (req["hl"] as? String) ?? "zh-CN"
                let fallbackBl = (req["fallbackBl"] as? String) ?? self.lastSavedBl
                let extraHeaders = (req["extraHeaders"] as? [String: String]) ?? [:]
                let hasCursor = (req["hasCursor"] as? Bool) ?? false
                let emitRaw = (req["emitRawChunks"] as? Bool) ?? false
                let headersTimeoutMs = (req["headersTimeoutMs"] as? Int) ?? (req["headersTimeoutMs"] as? Double).map { Int($0) } ?? 90000
                let firstWrbTimeoutMs = (req["firstWrbTimeoutMs"] as? Int) ?? (req["firstWrbTimeoutMs"] as? Double).map { Int($0) } ?? headersTimeoutMs
                let idleTimeoutMs = (req["idleTimeoutMs"] as? Int) ?? (req["idleTimeoutMs"] as? Double).map { Int($0) } ?? 120000
                let slot = self.pickOrScaleSlot()
                self.sessionsLock.lock()
                self.sessions[reqId] = (session, slot)
                self.sessionsLock.unlock()

                session.startDisconnectWatch { [weak self] in
                    self?.cancelClientStream(reqId: reqId)
                }

                self.launchStreamWhenReady(
                    slot: slot,
                    reqId: reqId,
                    fReq: fReq,
                    extraHeaders: extraHeaders,
                    hl: hl,
                    fallbackBl: fallbackBl,
                    hasCursor: hasCursor,
                    emitRaw: emitRaw,
                    headersTimeoutMs: headersTimeoutMs,
                    firstWrbTimeoutMs: firstWrbTimeoutMs,
                    idleTimeoutMs: idleTimeoutMs,
                    attempt: 0
                )
                return
            }

            session.sendJSON(["ok": false, "error": "Unknown op \(op)"])
            session.close()
        }
    }
}

var sockPath = "/tmp/dsh_webkit_18896.sock"
var autoPopup = false
var protoProfile = ""
let argv = CommandLine.arguments
var idx = 1
while idx < argv.count {
    if argv[idx] == "--socket", idx + 1 < argv.count {
        sockPath = argv[idx + 1]
        idx += 2
    } else if argv[idx] == "--proto-profile", idx + 1 < argv.count {
        protoProfile = argv[idx + 1]
        idx += 2
    } else if argv[idx] == "--auto-popup-login" {
        autoPopup = true
        idx += 1
    } else {
        idx += 1
    }
}

if sockPath.utf8.count >= 104 {
    fputs("[dsh-webkit-bridge] socketPath exceeds 103 UTF-8 bytes: \(sockPath)\n", stderr)
    exit(1)
}

let singletonLockFd = acquireSingletonLock(socketPath: sockPath)
writePidFile(socketPath: sockPath)

let app = NSApplication.shared
let delegate = DSHWebKitBridge(socketPath: sockPath, autoPopupLogin: autoPopup, protoProfilePath: protoProfile, lockFd: singletonLockFd)
app.delegate = delegate
app.run()
