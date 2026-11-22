#!/usr/bin/env bash
set -euo pipefail

# Clean PATH so double-clicking in Finder still exposes Homebrew/Claude/Node/Git.
for _p in "/opt/homebrew/bin" "/usr/local/bin" "${HOME}/.local/bin" "${HOME}/.npm-global/bin"; do
  if [[ -d "${_p}" && ":${PATH}:" != *":${_p}:"* ]]; then
    PATH="${_p}:${PATH}"
  fi
done
export PATH
export NO_PROXY="127.0.0.1,localhost,::1"
export no_proxy="127.0.0.1,localhost,::1"
unset CLAUDECODE CLAUDE_CODE_ENTRYPOINT CLAUDE_CODE_CHILD_SESSION CLAUDE_CODE_EFFORT_LEVEL 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${DSH_GATEWAY_PORT:-18896}"
PAUSE_ON_ERR=0
if [[ -t 0 && -t 1 && "${TERM_PROGRAM:-}" == "Apple_Terminal" ]]; then
  PAUSE_ON_ERR=1
fi

# Determine target working directory:
# 1) Explicit `-C <dir>` or first positional directory arg
# 2) Current directory if launched inside a project from terminal
# 3) Fallback to launcher project directory when double-clicked from Finder ($HOME)
TARGET_DIR=""
if [[ $# -ge 2 && "$1" == "-C" && -d "$2" ]]; then
  TARGET_DIR="$(cd "$2" && pwd)"
  shift 2
elif [[ $# -ge 1 && -d "$1" ]]; then
  TARGET_DIR="$(cd "$1" && pwd)"
  shift
elif [[ -n "${DSH_LAUNCHER_WORKDIR:-}" && -d "${DSH_LAUNCHER_WORKDIR}" ]]; then
  TARGET_DIR="$(cd "${DSH_LAUNCHER_WORKDIR}" && pwd)"
elif [[ "${PWD}" != "${HOME}" && "${PWD}" != "/" ]]; then
  TARGET_DIR="${PWD}"
else
  TARGET_DIR="${SCRIPT_DIR}"
fi

# Management subcommands pass straight through to cc-demo.sh without interactive launch banner.
case "${1:-}" in
  start|daemon|stop|restart|status|logs|login|switch-account|pin-account|workflow|clean-chats|app-mode|task-mode|probe)
    cd "${SCRIPT_DIR}"
    exec "${SCRIPT_DIR}/cc-demo.sh" "$@"
    ;;
esac

# Check if non-interactive `-p` / `--print` / `--help` / `--version` is requested.
IS_INTERACTIVE=1
for _arg in "$@"; do
  case "${_arg}" in
    -p|--print|-h|--help|-v|--version)
      IS_INTERACTIVE=0
      break
      ;;
  esac
done

echo "[一键启动] 工作目录: ${TARGET_DIR}"
echo "[一键启动] 正在检查 Dual-Plane v5.0 Pure-API 本地网关 (主模型 gemini-4-neon)..."

# Ensure gateway + WebKit bridge are running and up-to-date.
"${SCRIPT_DIR}/cc-demo.sh" start >/dev/null

# Wait briefly for WebKit bridge account hydration, and guide login only if truly unauthenticated.
AUTH_STATE="$(/usr/bin/python3 - "${PORT}" "${IS_INTERACTIVE}" <<'PY'
import json, sys, time, urllib.request

port = sys.argv[1]
interactive = sys.argv[2] == "1"
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
url = f"http://127.0.0.1:{port}/health"

def fetch_health():
    with opener.open(url, timeout=1.5) as r:
        return json.loads(r.read().decode("utf-8"))

d = {}
for _ in range(16):
    try:
        d = fetch_health()
        p1 = d.get("webkit_plane") or {}
        if p1.get("logged_in") or int(d.get("idle_task_workers", 0)) > 0:
            print("READY|" + str(p1.get("account") or ""))
            sys.exit(0)
        if p1.get("page_loaded") and not p1.get("logged_in"):
            break
    except Exception:
        pass
    time.sleep(0.25)

if not interactive:
    print("UNAUTH|")
    sys.exit(0)

print("NEED_LOGIN|")
PY
)"

AUTH_STATUS="${AUTH_STATE%%|*}"
if [[ "${AUTH_STATUS}" == "NEED_LOGIN" ]]; then
  echo "[一键启动] 检测到 WebKit 桥尚未登录 Google 账号，正在自动弹出登录窗口..."
  "${SCRIPT_DIR}/cc-demo.sh" login >/dev/null || true
  echo "[一键启动] 请在登录窗口完成 Google 登录（完成后将自动启动 Claude Code，按 Enter 可跳过）..."
  while true; do
    READY_NOW="$(/usr/bin/python3 - "${PORT}" <<'PY' 2>/dev/null || true
import json, sys, urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open(f"http://127.0.0.1:{sys.argv[1]}/health", timeout=1.2) as r:
    d = json.loads(r.read().decode("utf-8"))
p1 = d.get("webkit_plane") or {}
if p1.get("logged_in") or int(d.get("idle_task_workers", 0)) > 0:
    print("1")
PY
)"
    if [[ "${READY_NOW}" == "1" ]]; then
      echo "[一键启动] Google 登录态已就绪！"
      break
    fi
    if read -r -t 1 _ </dev/tty 2>/dev/null; then
      break
    fi
  done
fi

cd "${TARGET_DIR}"

if [[ "${IS_INTERACTIVE}" == "0" ]]; then
  exec "${SCRIPT_DIR}/cc-demo.sh" "$@"
fi

set +e
"${SCRIPT_DIR}/cc-demo.sh" "$@"
EXIT_CODE=$?
set -e

if [[ ${EXIT_CODE} -ne 0 && ${PAUSE_ON_ERR} -eq 1 ]]; then
  echo ""
  echo "[一键启动] Claude Code 异常退出 (code=${EXIT_CODE})，按回车键关闭窗口..."
  read -r _ || true
fi
exit ${EXIT_CODE}
