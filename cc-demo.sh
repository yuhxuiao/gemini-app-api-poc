#!/usr/bin/env bash
set -euo pipefail

export NO_PROXY="127.0.0.1,localhost,::1"
export no_proxy="127.0.0.1,localhost,::1"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${DSH_GATEWAY_PORT:-18896}"
UPSTREAM="${DSH_UPSTREAM:-dual_plane_web}"
WATERMARK="${DSH_WATERMARK_TOKENS:-192000}"
IPC_DIR="/tmp/dsh_gemini_ipc_${PORT}"
WEBKIT_SOCK="/tmp/dsh_webkit_${PORT}.sock"
PID_FILE="/tmp/dsh_gateway_${PORT}.pid"
LOG_FILE="/tmp/dsh_gateway_${PORT}.log"
RUNTIME_SETTINGS="/tmp/dsh_cc_settings_${PORT}.json"
CMD="${1:-launch}"

sync_worker_cli_to_tmp() {
  cp -f "${DIR}/dsh_worker_cli.py" /tmp/dsh_worker_cli.py 2>/dev/null || true
  chmod +x /tmp/dsh_worker_cli.py "${DIR}/dsh_worker_cli.py" "${DIR}/dsh_mcp_stdio.py" "${DIR}/dsh_local_tools_mcp.py" 2>/dev/null || true
}

rotate_log_if_large() {
  if [[ -f "${LOG_FILE}" ]]; then
    local sz
    sz="$(wc -c < "${LOG_FILE}" 2>/dev/null || echo 0)"
    if [[ "${sz}" -gt 5242880 ]]; then
      tail -n 2000 "${LOG_FILE}" > "${LOG_FILE}.tmp" 2>/dev/null && mv -f "${LOG_FILE}.tmp" "${LOG_FILE}" || true
    fi
  fi
}

build_runtime_settings() {
  /usr/bin/python3 - "${DIR}/claude_gateway_settings.json" "${RUNTIME_SETTINGS}" "${PORT}" <<'PY'
import json, os, sys
src, dst, port = sys.argv[1], sys.argv[2], sys.argv[3]
with open(src, "r", encoding="utf-8") as fp:
    cfg = json.load(fp)
primary = os.environ.get("DSH_PRIMARY_MODEL", cfg.get("model") or "gemini-4-neon")
subagent = os.environ.get("DSH_SUBAGENT_MODEL", primary)
haiku = os.environ.get("DSH_HAIKU_MODEL", "gemini-4-hydrogen")
cfg["model"] = primary
cfg["alwaysThinkingEnabled"] = True
cfg["showThinkingSummaries"] = True
cfg["effortLevel"] = os.environ.get("DSH_DEFAULT_EFFORT", cfg.get("effortLevel") or "xhigh")
env_blk = cfg.setdefault("env", {})
env_blk["NO_PROXY"] = "127.0.0.1,localhost,::1"
env_blk["no_proxy"] = "127.0.0.1,localhost,::1"
env_blk["ANTHROPIC_BASE_URL"] = f"http://127.0.0.1:{port}"
env_blk["ANTHROPIC_ + AUTH_TOKEN".replace(" + ", "")] = "dsh-local-gateway"
env_blk["ANTHROPIC_MODEL"] = primary
env_blk["ANTHROPIC_DEFAULT_MODEL"] = primary
env_blk["ANTHROPIC_DEFAULT_OPUS_MODEL"] = primary
env_blk["ANTHROPIC_DEFAULT_OPUS_MODEL_NAME"] = primary
env_blk["ANTHROPIC_DEFAULT_SONNET_MODEL"] = primary
env_blk["ANTHROPIC_DEFAULT_SONNET_MODEL_NAME"] = primary
env_blk["ANTHROPIC_DEFAULT_FABLE_MODEL"] = primary
env_blk["ANTHROPIC_DEFAULT_FABLE_MODEL_NAME"] = primary
env_blk["ANTHROPIC_DEFAULT_HAIKU_MODEL"] = haiku
env_blk["ANTHROPIC_DEFAULT_HAIKU_MODEL_NAME"] = haiku
env_blk["CLAUDE_CODE_SUBAGENT_MODEL"] = subagent
env_blk["CLAUDE_CODE_AUTO_COMPACT_WINDOW"] = "256000"
env_blk["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] = "64000"
env_blk["BASH_MAX_OUTPUT_LENGTH"] = "150000"
env_blk["CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY"] = "0"
env_blk["CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"] = "1"
env_blk["CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS"] = "1"
env_blk["CLAUDE_CODE_ATTRIBUTION_HEADER"] = "0"
env_blk["CLAUDE_CODE_ALWAYS_ENABLE_EFFORT"] = "1"
env_blk.pop("CLAUDE_CODE_EFFORT_LEVEL", None)
env_blk["CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR"] = "1"
env_blk["API_TIMEOUT_MS"] = "1200000"
env_blk["API_FORCE_IDLE_TIMEOUT"] = "0"
env_blk["CLAUDE_STREAM_IDLE_TIMEOUT_MS"] = "1200000"
env_blk["CLAUDE_ASYNC_AGENT_STALL_TIMEOUT_MS"] = "1200000"
with open(dst, "w", encoding="utf-8") as fp:
    json.dump(cfg, fp, indent=2, ensure_ascii=False)
PY
}

port_has_listener() {
  if curl --noproxy '*' -fsS -m 1 "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    return 0
  fi
  if [[ -n "$(/usr/sbin/lsof -ti "tcp:${PORT}" -sTCP:LISTEN 2>/dev/null || true)" ]]; then
    return 0
  fi
  return 1
}

stop_webkit_bridge() {
  if [[ -S "${WEBKIT_SOCK}" ]]; then
    /usr/bin/python3 - "${WEBKIT_SOCK}" <<'PY' 2>/dev/null || true
import socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(1.0)
s.connect(sys.argv[1])
s.sendall(b'{"op":"shutdown"}\n')
s.close()
PY
    sleep 0.25
  fi
}

stop_gateway() {
  local admin_hdr=()
  if [[ -n "${DSH_ADMIN_TOKEN:-}" ]]; then
    admin_hdr=(-H "X-Admin-Token: ${DSH_ADMIN_TOKEN}")
  fi
  curl --noproxy '*' -fsS -m 1 -X POST ${admin_hdr[@]+"${admin_hdr[@]}"} "http://127.0.0.1:${PORT}/_admin/shutdown" >/dev/null 2>&1 || true
  stop_webkit_bridge
  sleep 0.25
  if [[ -f "${PID_FILE}" ]]; then
    local pid
    pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
      for _ in {1..15}; do
        kill -0 "${pid}" 2>/dev/null || break
        sleep 0.1
      done
    fi
    rm -f "${PID_FILE}"
  fi
  local lpid
  lpid="$(/usr/sbin/lsof -ti "tcp:${PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${lpid}" ]]; then
    kill -TERM ${lpid} 2>/dev/null || true
    sleep 0.3
    lpid="$(/usr/sbin/lsof -ti "tcp:${PORT}" -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${lpid}" ]]; then
      kill -9 ${lpid} 2>/dev/null || true
      sleep 0.15
    fi
  fi
}

gateway_matches_runtime() {
  /usr/bin/python3 - "${PORT}" "${DIR}" "${UPSTREAM}" <<'PY' 2>/dev/null
import hashlib, json, os, sys, urllib.request

port, root_dir, expected_up = sys.argv[1], sys.argv[2], sys.argv[3]
runtime_relpaths = (
    "gateway_poc.py",
    "webkit_plane_client.py",
    "streaming_envelope_parser.py",
    "embedded_local_mcp.py",
    "bin/DSHWebKitBridge.app/Contents/MacOS/dsh-webkit-bridge",
)

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        while True:
            chunk = fp.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open(f"http://127.0.0.1:{port}/health", timeout=1.2) as r:
    d = json.loads(r.read().decode("utf-8"))

if d.get("version") != "5.0.0-pure-api":
    sys.exit(1)
if str(d.get("upstream", "")) != expected_up:
    sys.exit(1)

gw_py = os.path.join(root_dir, "gateway_poc.py")
if int(d.get("script_mtime", -1)) != int(os.path.getmtime(gw_py)):
    sys.exit(1)

rt_files = d.get("runtime_files")
if not isinstance(rt_files, dict):
    sys.exit(1)

for rel in runtime_relpaths:
    full = os.path.join(root_dir, rel)
    rec = rt_files.get(rel)
    if not isinstance(rec, dict):
        sys.exit(1)
    try:
        disk_mtime = int(os.path.getmtime(full))
        disk_hash = sha256_file(full)
    except OSError:
        disk_mtime = -1
        disk_hash = ""
    if int(rec.get("mtime", -2)) != disk_mtime or str(rec.get("sha256", "")) != disk_hash:
        sys.exit(1)

sys.exit(0)
PY
}

wait_plane_ready_briefly() {
  /usr/bin/python3 - "${PORT}" <<'PY' 2>/dev/null || true
import json, sys, time, urllib.request
port = sys.argv[1]
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
for _ in range(18):
    try:
        with opener.open(f"http://127.0.0.1:{port}/readyz", timeout=1.0) as r:
            d = json.loads(r.read().decode("utf-8"))
        if d.get("ready") or d.get("plane1_page_loaded"):
            break
    except Exception:
        pass
    time.sleep(0.2)
PY
}

start_gateway() {
  sync_worker_cli_to_tmp
  rotate_log_if_large
  build_runtime_settings
  if port_has_listener; then
    if gateway_matches_runtime; then
      return 0
    fi
    echo "[cc-demo] 检测到旧版网关或文件变更占用 ${PORT}，正在热切换为 Dual-Plane v5.0 Pure-API 网关..."
    stop_gateway
  fi
  local disable_p1="0"
  if [[ "${UPSTREAM}" == "mcp_task" ]]; then
    disable_p1="1"
  fi
  DSH_DISABLE_WEBKIT_AUTOSPAWN="${disable_p1}" DSH_IPC_DIR="${IPC_DIR}" DSH_WEBKIT_SOCK="${WEBKIT_SOCK}" DSH_GATEWAY_PORT="${PORT}" DSH_CLAIM_TIMEOUT_SEC="3.5" \
    nohup /usr/bin/python3 -u "${DIR}/gateway_poc.py" \
      --port "${PORT}" \
      --upstream "${UPSTREAM}" \
      --watermark "${WATERMARK}" >>"${LOG_FILE}" 2>&1 < /dev/null &
  echo $! > "${PID_FILE}"
  for _ in {1..50}; do
    if gateway_matches_runtime; then
      if [[ "${disable_p1}" == "0" ]]; then
        wait_plane_ready_briefly
      fi
      return 0
    fi
    sleep 0.12
  done
  echo "[cc-demo] 网关启动超时，请检查日志: ${LOG_FILE}" >&2
  tail -n 25 "${LOG_FILE}" >&2 || true
  return 1
}

show_status() {
  if ! curl --noproxy '*' -fsS -m 2 "http://127.0.0.1:${PORT}/health" | /usr/bin/python3 -m json.tool; then
    echo "[cc-demo] 端口 ${PORT} 上的本地网关当前未运行。" >&2
    return 1
  fi
}

find_claude_bin() {
  for cand in \
    "$(command -v claude 2>/dev/null || true)" \
    "/opt/homebrew/bin/claude" \
    "/usr/local/bin/claude" \
    "${HOME}/.local/bin/claude" \
    "${HOME}/.npm-global/bin/claude"; do
    if [[ -n "${cand}" && -x "${cand}" ]]; then
      printf '%s\n' "${cand}"
      return 0
    fi
  done
  return 1
}

case "${CMD}" in
  stop)
    stop_gateway
    echo "[cc-demo] 已停止端口 ${PORT} 上的本地网关。"
    exit 0
    ;;
  start|daemon)
    start_gateway
    echo "[cc-demo] Dual-Plane v5.0 Pure-API 网关后台已就绪: http://127.0.0.1:${PORT}"
    show_status
    exit 0
    ;;
  restart)
    stop_gateway
    start_gateway
    echo "[cc-demo] Dual-Plane v5.0 Pure-API 网关已重启: http://127.0.0.1:${PORT}"
    show_status
    exit 0
    ;;
  probe)
    shift || true
    start_gateway
    exec /usr/bin/python3 -u "${DIR}/probe_live_models_and_task.py" "$@"
    ;;
  app-mode|task-mode)
    shift || true
    UPSTREAM="mcp_task"
    TASK_CMD="请在当前 Gemini.app Task 或 Antigravity 会话中执行常驻守护值守命令（RequiresNetworkAccess=false）：/usr/bin/python3 /tmp/dsh_worker_cli.py daemon --ipc-dir ${IPC_DIR} --model gemini-4-neon --max-idle 1800"
    printf '%s' "${TASK_CMD}" | pbcopy 2>/dev/null || true
    echo "=================================================================="
    echo " [Plane 2 接管] 已自动复制 Gemini.app 常驻守护口令到剪贴板！"
    echo "  • 粘贴到 Gemini.app 新建的 1 个 Task：走消费级 Gemini Web 额度（仅占 1 条 Task 记录）。"
    echo "  • 粘贴到 Gemini.app 右上角 Antigravity (CodeMode) 面板：走内置 agy（100% 零 Web 记录）。"
    echo "=================================================================="
    ;;
  clean-chats)
    start_gateway
    ADMIN_HDR=()
    [[ -n "${DSH_ADMIN_TOKEN:-}" ]] && ADMIN_HDR=(-H "X-Admin-Token: ${DSH_ADMIN_TOKEN}")
    curl --noproxy '*' -fsS -m 25 -X POST ${ADMIN_HDR[@]+"${ADMIN_HDR[@]}"} "http://127.0.0.1:${PORT}/_admin/clean_chats" -d '{}' | /usr/bin/python3 -m json.tool
    exit 0
    ;;
  login)
    start_gateway
    ADMIN_HDR=()
    [[ -n "${DSH_ADMIN_TOKEN:-}" ]] && ADMIN_HDR=(-H "X-Admin-Token: ${DSH_ADMIN_TOKEN}")
    curl --noproxy '*' -fsS -m 5 -X POST ${ADMIN_HDR[@]+"${ADMIN_HDR[@]}"} "http://127.0.0.1:${PORT}/_admin/open_login" -d '{}' | /usr/bin/python3 -m json.tool
    echo "[cc-demo] 已打开登录窗口：请登录与 Gemini.app 相同的 Google 邮箱（独立令牌链互不顶线、共享同一份额度）。"
    exit 0
    ;;
  switch-account)
    start_gateway
    ADMIN_HDR=()
    [[ -n "${DSH_ADMIN_TOKEN:-}" ]] && ADMIN_HDR=(-H "X-Admin-Token: ${DSH_ADMIN_TOKEN}")
    curl --noproxy '*' -fsS -m 5 -X POST ${ADMIN_HDR[@]+"${ADMIN_HDR[@]}"} "http://127.0.0.1:${PORT}/_admin/switch_account" -d '{}' | /usr/bin/python3 -m json.tool
    echo "[cc-demo] 已清空旧网页会话并唤起登录窗，请登录与 Gemini.app 一致的 Google 邮箱。"
    exit 0
    ;;
  pin-account)
    shift || true
    TARGET_EMAIL="${1:-}"
    start_gateway
    /usr/bin/python3 - "${PORT}" "${TARGET_EMAIL}" <<'PY'
import json, os, sys, urllib.request
port, email = sys.argv[1], sys.argv[2]
headers = {"Content-Type": "application/json"}
tok = os.environ.get("DSH_ADMIN_TOKEN", "").strip()
if tok:
    headers["X-Admin-Token"] = tok
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/_admin/pin_account",
    data=json.dumps({"account": email}).encode("utf-8"),
    headers=headers,
    method="POST",
)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open(req, timeout=3.0) as r:
    print(r.read().decode("utf-8"))
PY
    exit 0
    ;;
  workflow)
    shift || true
    exec /usr/bin/python3 -u "${DIR}/workflow_runner.py" "$@"
    ;;
  status)
    show_status
    exit 0
    ;;
  logs)
    tail -n 80 -f "${LOG_FILE}"
    exit 0
    ;;
  launch|--*|-*)
    [[ "${CMD}" == "launch" ]] && shift || true
    ;;
esac

start_gateway

CLAUDE_BIN="$(find_claude_bin || true)"
if [[ -z "${CLAUDE_BIN}" ]]; then
  echo "[cc-demo] 未找到 claude 可执行文件，请确认已安装 Claude Code CLI。" >&2
  exit 1
fi

STATUS_LINE="$(/usr/bin/python3 - "${PORT}" <<'PY' 2>/dev/null || echo "0|0|1||"
import json, sys, urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open(f"http://127.0.0.1:{sys.argv[1]}/health", timeout=1.5) as r:
    d = json.loads(r.read().decode("utf-8"))
p1_obj = d.get("webkit_plane") or {}
p1 = 1 if p1_obj.get("logged_in") else 0
p2 = max(int(d.get("idle_task_workers", 0)), int(d.get("recent_web_workers", 0)))
mcp_cnt = int((d.get("embedded_local_mcp") or {}).get("server_count", 0))
acct = str(p1_obj.get("account") or "").replace("|", "")
pinned = str((d.get("account_alignment") or {}).get("pinned_account") or "").replace("|", "")
print(f"{p1}|{p2}|{mcp_cnt}|{acct}|{pinned}")
PY
)"
IFS='|' read -r P1_READY P2_WORKERS MCP_COUNT P1_ACCT PINNED_ACCT <<< "${STATUS_LINE}"

echo "=================================================================="
echo " Gemini.app x Claude Code 已启动 (Dual-Plane v5.0 Pure-API · 主模型 ${DSH_PRIMARY_MODEL:-gemini-4-neon} · 开放 /effort 调节)"
echo "  • 本地网关地址  : http://127.0.0.1:${PORT} (免公网双轨 MCP: ${MCP_COUNT} 个)"
if [[ "${UPSTREAM}" == "mcp_task" || "${P2_WORKERS}" -gt 0 ]]; then
  echo "  • 当前会话通道  : Gemini.app 前台单会话守护接管 (Plane 2 在线: ${P2_WORKERS} · Task 复用单条)"
elif [[ "${P1_READY}" == "1" ]]; then
  echo "  • 当前会话通道  : Plane 1 纯 API 严格锁定 · 账号: ${P1_ACCT:-已验证} (零自动路由 + 自动删旧会话)"
  if [[ -n "${PINNED_ACCT}" ]]; then
    echo "  • 账号防串号锁  : 已锁定对齐 ${PINNED_ACCT}"
  fi
else
  echo "  • 当前会话通道  : 未强行弹窗 (运行 ./cc-demo.sh task-mode 接管 App，或 ./cc-demo.sh login 绑定网页)"
fi
echo "  • 实时监控面板  : http://127.0.0.1:${PORT}/dashboard (支持一键清理残留网页会话)"
echo "  • 全模型深度探测: 运行 ./cc-demo.sh probe --smoke 探测当前账号全部模型与思考档位"
echo "=================================================================="

export ANTHROPIC_BASE_URL="http://127.0.0.1:${PORT}"
AUTH_ENV_NAME="ANTHROPIC_""AUTH_TOKEN"
if [[ -z "${!AUTH_ENV_NAME:-}" ]]; then
  export "${AUTH_ENV_NAME}=dsh-local-gateway"
fi
unset CLAUDE_CODE_EFFORT_LEVEL CLAUDE_CODE_CHILD_SESSION 2>/dev/null || true

exec "${CLAUDE_BIN}" --settings "${RUNTIME_SETTINGS}" --autocompact 256k "$@"
