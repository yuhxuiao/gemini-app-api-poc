#!/usr/bin/env python3
"""
embedded_local_mcp.py - Gateway-Embedded Zero-Public-IP Stdio MCP Host & Dual-Layer Router
Architecture:
  - Layer A (Client Host Tools): Tools declared in Claude Code `/v1/messages` `tools[]`
    (`Bash`, `Read`, `Edit`, `Write`, `Agent`, `mcp__*` already loaded by client) ->
    returned to Claude Code as standard Anthropic SSE `tool_use` blocks.
  - Layer B (Embedded Local `stdio` MCP Pool): Local MCP servers configured in
    `gemini_local_mcp_config.json` or `<cwd>/.mcp.json` (excluding the reverse `dsh-bridge`
    worker stub and any server already mirrored in Layer A) -> spawned directly by the
    local gateway over `stdio` (`bufsize=0`, non-blocking `stdin` + `DefaultSelector`,
    background `stdout`/`stderr` drain threads, process-group cleanup via `os.killpg`).
    Zero public IP, zero ngrok/FRP tunnel, 100% compatible with pure Web inference!
"""

import collections
import errno
import json
import os
import queue
import re
import selectors
import signal
import stat
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

MAX_LINE_BYTES = 16 * 1024 * 1024
REVERSE_BRIDGE_NAMES = {"dsh-bridge", "dsh_bridge"}
STASH_MAX_ENTRIES = 256
STASH_TTL_SEC = 3600.0
_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env_vars(val: str) -> str:
    def _repl(m: re.Match) -> str:
        var = m.group(1)
        default = m.group(2)
        if var in os.environ:
            return os.environ[var]
        return default if default is not None else ""

    return _ENV_VAR_RE.sub(_repl, val)


def _expand_config_token(val: str, expand_tilde: bool = True) -> str:
    s = _expand_env_vars(val)
    if expand_tilde and s.startswith("~"):
        s = os.path.expanduser(s)
    return s


class StdioMcpClient:
    """Thread-safe non-blocking JSON-RPC 2.0 `stdio` MCP client for a single local MCP server."""

    def __init__(self, server_name: str, command: str, args: List[str], env: Optional[Dict[str, str]] = None, cwd: Optional[str] = None):
        self.server_name = server_name
        self.command = command
        self.args = list(args or [])
        self.extra_env = dict(env or {})
        self.cwd = cwd or os.getcwd()

        self._lock = threading.RLock()
        self._init_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._pending: Dict[Any, "queue.Queue[dict]"] = {}
        self._next_id = 1
        self._tools_cache: List[dict] = []
        self._stderr_ring: collections.deque = collections.deque(maxlen=200)
        self._initialized_ok = False
        self._closed = False

    def is_alive(self) -> bool:
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    def ensure_started(self, timeout_sec: float = 8.0) -> bool:
        with self._lock:
            if self._closed:
                return False
            if self._proc is not None and self._proc.poll() is None and self._initialized_ok:
                return True

        with self._init_lock:
            with self._lock:
                if self._closed:
                    return False
                if self._proc is not None and self._proc.poll() is None and self._initialized_ok:
                    return True
                self._cleanup_proc_locked()

                merged_env = os.environ.copy()
                merged_env.update(self.extra_env)
                merged_env["PYTHONUNBUFFERED"] = "1"

                try:
                    proc = subprocess.Popen(
                        [self.command] + self.args,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        bufsize=0,
                        cwd=self.cwd if os.path.isdir(self.cwd) else "/tmp",
                        env=merged_env,
                        start_new_session=True,
                        close_fds=True,
                    )
                    if proc.stdin is not None:
                        os.set_blocking(proc.stdin.fileno(), False)
                except Exception as exc:
                    self._stderr_ring.append(f"spawn failed: {exc}"[:500])
                    return False

                self._proc = proc
                self._initialized_ok = False
                threading.Thread(target=self._stdout_drain_loop, args=(proc,), daemon=True).start()
                threading.Thread(target=self._stderr_drain_loop, args=(proc,), daemon=True).start()

            # Perform MCP initialize handshake under _init_lock (outside _lock)
            init_res = self.request(
                "initialize",
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "dsh-dual-plane-gateway", "version": "4.0.0"},
                },
                timeout_sec=timeout_sec,
            )
            if "error" in init_res:
                with self._lock:
                    if self._proc is proc:
                        self._cleanup_proc_locked()
                return False

            self.notify("notifications/initialized", {})
            tools_res = self.request("tools/list", {}, timeout_sec=timeout_sec)
            if "error" in tools_res:
                err_obj = tools_res.get("error") or {}
                err_code = err_obj.get("code") if isinstance(err_obj, dict) else None
                if err_code != -32601:
                    with self._lock:
                        if self._proc is proc:
                            self._cleanup_proc_locked()
                    return False
                raw_tools = []
            else:
                raw_tools = ((tools_res.get("result") or {}).get("tools")) or []

            with self._lock:
                if self._closed or self._proc is not proc or proc.poll() is not None:
                    if self._proc is proc:
                        self._cleanup_proc_locked()
                    return False
                self._tools_cache = [t for t in raw_tools if isinstance(t, dict) and t.get("name")]
                self._initialized_ok = True
            return True

    def _send_rpc_reply(self, payload: dict) -> None:
        try:
            self._write_frame(payload, timeout_sec=2.0)
        except Exception:
            pass

    def _stdout_drain_loop(self, proc: subprocess.Popen) -> None:
        out = proc.stdout
        if out is None:
            return
        buf = bytearray()
        discarding_long_line = False
        try:
            while True:
                try:
                    chunk = out.read(16384)
                except (OSError, ValueError):
                    break
                if not chunk:
                    break
                if discarding_long_line:
                    nl_idx = chunk.find(b"\n")
                    if nl_idx == -1:
                        continue
                    discarding_long_line = False
                    buf.extend(chunk[nl_idx + 1 :])
                else:
                    buf.extend(chunk)

                while True:
                    nl = buf.find(b"\n")
                    if nl == -1:
                        if len(buf) > MAX_LINE_BYTES:
                            buf.clear()
                            discarding_long_line = True
                        break
                    if nl > MAX_LINE_BYTES:
                        del buf[: nl + 1]
                        continue
                    line_bytes = bytes(buf[:nl]).strip()
                    del buf[: nl + 1]
                    if not line_bytes:
                        continue
                    try:
                        msg = json.loads(line_bytes.decode("utf-8", errors="replace"))
                    except Exception:
                        continue
                    if not isinstance(msg, dict):
                        continue
                    if "method" in msg:
                        req_id = msg.get("id")
                        if req_id is not None:
                            method = msg.get("method")
                            if method == "ping":
                                reply = {"jsonrpc": "2.0", "id": req_id, "result": {}}
                            else:
                                reply = {
                                    "jsonrpc": "2.0",
                                    "id": req_id,
                                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                                }
                            threading.Thread(target=self._send_rpc_reply, args=(reply,), daemon=True).start()
                        continue
                    req_id = msg.get("id")
                    if req_id is not None:
                        with self._lock:
                            waiter = self._pending.pop(req_id, None)
                        if waiter is not None:
                            try:
                                waiter.put_nowait(msg)
                            except queue.Full:
                                pass
        finally:
            with self._lock:
                if self._proc is proc:
                    self._initialized_ok = False
                    for _, q in list(self._pending.items()):
                        try:
                            q.put_nowait({"error": {"code": -32000, "message": f"MCP server {self.server_name} exited"}})
                        except queue.Full:
                            pass
                    self._pending.clear()

    def _stderr_drain_loop(self, proc: subprocess.Popen) -> None:
        err = proc.stderr
        if err is None:
            return
        try:
            while True:
                try:
                    chunk = err.read(4096)
                except (OSError, ValueError):
                    break
                if not chunk:
                    break
                for ln in chunk.decode("utf-8", errors="replace").splitlines():
                    self._stderr_ring.append(ln[:500])
        except Exception:
            pass

    def _write_frame(self, payload: dict, timeout_sec: float = 5.0) -> None:
        raw = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        with self._lock:
            proc = self._proc
        if proc is None or proc.stdin is None or proc.poll() is not None:
            raise RuntimeError(f"MCP server {self.server_name} stdin unavailable")

        fd = proc.stdin.fileno()
        deadline = time.time() + timeout_sec
        with self._write_lock:
            sel = selectors.DefaultSelector()
            try:
                sel.register(fd, selectors.EVENT_WRITE)
                sent = 0
                total = len(raw)
                while sent < total:
                    rem = deadline - time.time()
                    if rem <= 0:
                        raise TimeoutError(f"Timed out writing to MCP server {self.server_name}")
                    if not sel.select(timeout=min(0.5, rem)):
                        continue
                    try:
                        n = os.write(fd, raw[sent : sent + 16384])
                        if n > 0:
                            sent += n
                    except OSError as exc:
                        if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                            continue
                        raise
            finally:
                try:
                    sel.close()
                except Exception:
                    pass

    def notify(self, method: str, params: dict) -> None:
        try:
            self._write_frame({"jsonrpc": "2.0", "method": method, "params": params})
        except Exception:
            pass

    def request(self, method: str, params: dict, timeout_sec: float = 30.0) -> dict:
        with self._lock:
            req_id = self._next_id
            self._next_id += 1
            q: "queue.Queue[dict]" = queue.Queue(maxsize=1)
            self._pending[req_id] = q
        try:
            self._write_frame({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}, timeout_sec=min(8.0, timeout_sec))
            return q.get(timeout=timeout_sec)
        except Exception as exc:
            with self._lock:
                self._pending.pop(req_id, None)
            return {"error": {"code": -32001, "message": str(exc)}}

    def list_tools(self) -> List[dict]:
        if not self.ensure_started():
            return []
        with self._lock:
            return list(self._tools_cache)

    def call_tool(self, tool_name: str, arguments: dict, timeout_sec: float = 45.0) -> dict:
        if not self.ensure_started():
            return {
                "content": [{"type": "text", "text": f"Failed to start local stdio MCP server {self.server_name!r}"}],
                "isError": True,
            }
        resp = self.request("tools/call", {"name": tool_name, "arguments": arguments or {}}, timeout_sec=timeout_sec)
        if "error" in resp:
            return {
                "content": [{"type": "text", "text": f"[Local MCP Error ({self.server_name})]: {resp['error']}"}],
                "isError": True,
            }
        return resp.get("result") or {"content": [{"type": "text", "text": ""}], "isError": False}

    def _cleanup_proc_locked(self) -> None:
        self._initialized_ok = False
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        for _, q in list(self._pending.items()):
            try:
                q.put_nowait({"error": {"code": -32000, "message": f"MCP server {self.server_name} closed"}})
            except queue.Full:
                pass
        self._pending.clear()
        for pipe in (proc.stdin, proc.stdout, proc.stderr):
            if pipe is not None:
                try:
                    pipe.close()
                except Exception:
                    pass
        if proc.poll() is None:
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                proc.wait(timeout=1.0)
            except Exception:
                try:
                    pgid = os.getpgid(proc.pid)
                    os.killpg(pgid, signal.SIGKILL)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._cleanup_proc_locked()


ClientKey = Tuple[str, str, Tuple[str, ...], str]


class EmbeddedLocalMcpHost:
    """
    Manages zero-public-IP local `stdio` MCP servers and resolves Layer A vs Layer B routing,
    including mixed-batch (`Layer A + Layer B`) result stashing & stitching.
    """

    def __init__(self, base_config_paths: Optional[List[str]] = None):
        self._lock = threading.RLock()
        self._base_config_paths = list(base_config_paths or [])
        self._clients: Dict[ClientKey, StdioMcpClient] = {}
        self._scope_keys: Dict[str, Set[ClientKey]] = {}
        self._loaded_files_mtime: Dict[str, float] = {}
        self._config_cache: Dict[str, Tuple[Tuple[int, int], Dict[str, dict]]] = {}
        # Stash for mixed Layer A + Layer B batches keyed by anchor Layer-A tool_use_id
        self._stashed_layer_b_results: "collections.OrderedDict[str, Tuple[float, List[dict]]]" = collections.OrderedDict()

    @staticmethod
    def _normalize_cwd(cwd: Optional[str]) -> str:
        if not cwd:
            return ""
        expanded = _expand_config_token(str(cwd).strip(), expand_tilde=True)
        return os.path.abspath(expanded) if expanded else ""

    def _read_config_cached(self, path: str) -> Optional[Dict[str, dict]]:
        if not path:
            return None
        abs_p = os.path.abspath(_expand_config_token(path, expand_tilde=True))
        try:
            st = os.stat(abs_p)
        except OSError:
            with self._lock:
                self._loaded_files_mtime.pop(abs_p, None)
                self._config_cache.pop(abs_p, None)
            return None
        if not stat.S_ISREG(st.st_mode):
            return None
        sig = (st.st_mtime_ns, st.st_size)
        with self._lock:
            cached = self._config_cache.get(abs_p)
            if cached is not None and cached[0] == sig:
                return cached[1]
        try:
            with open(abs_p, "r", encoding="utf-8") as fp:
                cfg = json.load(fp)
        except Exception:
            return None
        servers = cfg.get("mcpServers") if isinstance(cfg, dict) else None
        raw_servers = servers if isinstance(servers, dict) else {}
        with self._lock:
            self._loaded_files_mtime[abs_p] = st.st_mtime
            self._config_cache[abs_p] = (sig, raw_servers)
        return raw_servers

    def _discover_configs(self, cwd: Optional[str] = None) -> Dict[ClientKey, dict]:
        norm_cwd = self._normalize_cwd(cwd)
        paths: List[str] = []
        seen_paths: Set[str] = set()
        for p in self._base_config_paths:
            if not p:
                continue
            ap = os.path.abspath(_expand_config_token(p, expand_tilde=True))
            if ap not in seen_paths:
                seen_paths.add(ap)
                paths.append(ap)
        if norm_cwd and os.path.isdir(norm_cwd):
            proj_mcp = os.path.abspath(os.path.join(norm_cwd, ".mcp.json"))
            if proj_mcp not in seen_paths:
                seen_paths.add(proj_mcp)
                paths.append(proj_mcp)

        merged_by_name: Dict[str, dict] = {}
        for p in paths:
            servers = self._read_config_cached(p)
            if not servers:
                continue
            for sname, spec in servers.items():
                if not isinstance(spec, dict):
                    continue
                cmd = _expand_config_token(str(spec.get("command", "")).strip(), expand_tilde=True)
                if not cmd:
                    continue
                # Skip the reverse Plane-2 worker bridge (`dsh-bridge` -> `dsh-mcp-stdio`)
                if sname in REVERSE_BRIDGE_NAMES or "dsh-mcp-stdio" in cmd or "dsh_mcp_stdio.py" in cmd:
                    continue
                args = [_expand_config_token(str(a), expand_tilde=True) for a in (spec.get("args") or [])]
                env_raw = spec.get("env")
                env_dict = (
                    {str(k): _expand_env_vars(str(v)) for k, v in env_raw.items()}
                    if isinstance(env_raw, dict)
                    else {}
                )
                spec_cwd_raw = str(spec.get("cwd", "")).strip()
                eff_cwd = self._normalize_cwd(spec_cwd_raw) if spec_cwd_raw else (norm_cwd or os.path.dirname(p))
                merged_by_name[sname] = {
                    "server_name": sname,
                    "command": cmd,
                    "args": args,
                    "env": env_dict,
                    "cwd": eff_cwd,
                }

        keyed: Dict[ClientKey, dict] = {}
        for sname, spec in merged_by_name.items():
            ckey: ClientKey = (sname, spec["command"], tuple(spec["args"]), spec["cwd"])
            keyed[ckey] = spec
        return keyed

    def _sync_servers_scope(self, cwd: Optional[str] = None) -> List[ClientKey]:
        scope_id = self._normalize_cwd(cwd)
        specs = self._discover_configs(cwd=cwd)
        desired_keys = list(specs.keys())
        to_close: List[StdioMcpClient] = []

        with self._lock:
            for ckey, spec in specs.items():
                if ckey not in self._clients:
                    self._clients[ckey] = StdioMcpClient(
                        server_name=spec["server_name"],
                        command=spec["command"],
                        args=spec["args"],
                        env=spec["env"],
                        cwd=spec["cwd"],
                    )
            self._scope_keys[scope_id] = set(desired_keys)
            referenced: Set[ClientKey] = set()
            for ks in self._scope_keys.values():
                referenced.update(ks)
            for stale_key in list(self._clients.keys()):
                if stale_key not in referenced:
                    cli = self._clients.pop(stale_key, None)
                    if cli is not None:
                        to_close.append(cli)

        for cli in to_close:
            cli.close()
        return desired_keys

    def sync_servers(self, cwd: Optional[str] = None) -> None:
        self._sync_servers_scope(cwd=cwd)

    def get_active_layer_b_tools(self, client_layer_a_names: Set[str], cwd: Optional[str] = None) -> List[dict]:
        """
        Returns Layer B tools not already covered by `Claude Code`'s Layer A `tools[]`.
        Supports both bare `<tool_name>` and namespaced `mcp__<server>__<tool_name>`.
        On tool-name collisions across Layer B servers, prefers `namespaced_name`.
        """
        active_keys = self._sync_servers_scope(cwd=cwd)
        with self._lock:
            clients = [self._clients[k] for k in active_keys if k in self._clients]

        layer_a_lower = {n.lower() for n in client_layer_a_names}
        candidates: List[dict] = []
        raw_counts: Dict[str, int] = {}

        for cli in clients:
            sname = cli.server_name
            safe_srv = sname.replace("-", "_")
            for t in cli.list_tools():
                raw_tname = str(t.get("name", "")).strip()
                if not raw_tname:
                    continue
                ns_name = f"mcp__{safe_srv}__{raw_tname}"
                # Deduplicate if client already registered this tool/MCP server in Layer A
                if (
                    raw_tname in client_layer_a_names
                    or ns_name in client_layer_a_names
                    or raw_tname.lower() in layer_a_lower
                    or ns_name.lower() in layer_a_lower
                ):
                    continue
                raw_lower = raw_tname.lower()
                raw_counts[raw_lower] = raw_counts.get(raw_lower, 0) + 1
                candidates.append({
                    "name": raw_tname,
                    "namespaced_name": ns_name,
                    "server_name": sname,
                    "description": str(t.get("description", f"Local MCP tool {raw_tname} ({sname})")),
                    "parameters": t.get("inputSchema") or {"type": "object", "properties": {}},
                    "is_embedded_mcp": True,
                })

        for entry in candidates:
            if raw_counts.get(entry["name"].lower(), 0) > 1:
                entry["raw_name"] = entry["name"]
                entry["name"] = entry["namespaced_name"]
                entry["collision"] = True
        return candidates

    def resolve_layer_b_call(self, call_name: str, client_layer_a_names: Set[str], cwd: Optional[str] = None) -> Optional[Tuple[StdioMcpClient, str]]:
        """If `call_name` belongs to an active Layer B `stdio` MCP server (and not Layer A), returns `(client, raw_tool_name)`."""
        layer_a_lower = {n.lower() for n in client_layer_a_names}
        if call_name in client_layer_a_names or call_name.lower() in layer_a_lower:
            return None
        active_keys = self._sync_servers_scope(cwd=cwd)
        with self._lock:
            clients = [self._clients[k] for k in active_keys if k in self._clients]

        call_lower = call_name.lower()
        ns_ci_match: Optional[Tuple[StdioMcpClient, str]] = None
        raw_exact_match: Optional[Tuple[StdioMcpClient, str]] = None
        raw_ci_match: Optional[Tuple[StdioMcpClient, str]] = None

        for cli in clients:
            safe_srv = cli.server_name.replace("-", "_")
            prefix = f"mcp__{safe_srv}__"
            for t in cli.list_tools():
                raw_tname = str(t.get("name", "")).strip()
                if not raw_tname:
                    continue
                ns_name = prefix + raw_tname
                if call_name == ns_name:
                    return cli, raw_tname
                if call_name == raw_tname and raw_exact_match is None:
                    raw_exact_match = (cli, raw_tname)
                if call_lower == ns_name.lower() and ns_ci_match is None:
                    ns_ci_match = (cli, raw_tname)
                if call_lower == raw_tname.lower() and raw_ci_match is None:
                    raw_ci_match = (cli, raw_tname)

        return raw_exact_match or ns_ci_match or raw_ci_match

    @staticmethod
    def _format_tool_result_text(res: dict) -> str:
        blocks = res.get("content") or []
        parts: List[str] = []
        has_text = False
        for b in blocks:
            if not isinstance(b, dict):
                continue
            btype = str(b.get("type", "")).strip()
            if btype == "text":
                has_text = True
                parts.append(str(b.get("text", "")))
            elif btype in ("image", "audio"):
                mime = str(b.get("mimeType") or "application/octet-stream")
                data_s = str(b.get("data") or "")
                parts.append(f"[{btype}: mimeType={mime}, base64_len={len(data_s)}]")
            elif btype == "resource":
                r = b.get("resource") if isinstance(b.get("resource"), dict) else {}
                uri = str(r.get("uri") or b.get("uri") or "")
                if "text" in r and r["text"] is not None:
                    parts.append(f"[resource {uri}]:\n{r['text']}")
                elif "blob" in r and r["blob"] is not None:
                    mime = str(r.get("mimeType") or "application/octet-stream")
                    parts.append(f"[resource {uri}: mimeType={mime}, blob_base64_len={len(str(r['blob']))}]")
                else:
                    parts.append(f"[resource]: {json.dumps(b, ensure_ascii=False)}")
            else:
                parts.append(json.dumps(b, ensure_ascii=False))

        structured = res.get("structuredContent")
        if structured is not None:
            s_json = json.dumps(structured, ensure_ascii=False)
            if not parts:
                parts.append(s_json)
            elif not (len(parts) == 1 and has_text and parts[0].strip() == s_json):
                parts.append(f"[structuredContent]: {s_json[:4000]}")

        return "\n".join(parts) if parts else json.dumps(res, ensure_ascii=False)

    def execute_layer_b_tool(self, call_name: str, arguments: dict, client_layer_a_names: Set[str], cwd: Optional[str] = None) -> Optional[dict]:
        resolved = self.resolve_layer_b_call(call_name, client_layer_a_names, cwd=cwd)
        if resolved is None:
            return None
        cli, raw_tname = resolved
        res = cli.call_tool(raw_tname, arguments or {})
        blocks = res.get("content") or []
        text_out = self._format_tool_result_text(res)
        out = {
            "server": cli.server_name,
            "tool": raw_tname,
            "text": text_out,
            "is_error": bool(res.get("isError", False)),
            "content": blocks,
        }
        if "structuredContent" in res:
            out["structured_content"] = res["structuredContent"]
        return out

    def _prune_stashed_locked(self, now: Optional[float] = None) -> None:
        cur = time.monotonic() if now is None else now
        expired = [k for k, (ts, _) in self._stashed_layer_b_results.items() if cur - ts > STASH_TTL_SEC]
        for k in expired:
            self._stashed_layer_b_results.pop(k, None)
        while len(self._stashed_layer_b_results) > STASH_MAX_ENTRIES:
            self._stashed_layer_b_results.popitem(last=False)

    def stash_mixed_results(self, anchor_layer_a_id: str, results: List[dict]) -> None:
        if not anchor_layer_a_id or not results:
            return
        with self._lock:
            now = time.monotonic()
            self._prune_stashed_locked(now)
            self._stashed_layer_b_results[anchor_layer_a_id] = (now, list(results))
            self._stashed_layer_b_results.move_to_end(anchor_layer_a_id)
            while len(self._stashed_layer_b_results) > STASH_MAX_ENTRIES:
                self._stashed_layer_b_results.popitem(last=False)

    def pop_stashed_results(self, returned_layer_a_ids: List[str]) -> List[dict]:
        collected: List[dict] = []
        with self._lock:
            self._prune_stashed_locked()
            for tid in returned_layer_a_ids:
                entry = self._stashed_layer_b_results.pop(tid, None)
                if entry:
                    _, items = entry
                    if items:
                        collected.extend(items)
        return collected

    def status_summary(self) -> dict:
        with self._lock:
            self._prune_stashed_locked()
            servers = []
            for _, cli in self._clients.items():
                servers.append({
                    "name": cli.server_name,
                    "cwd": cli.cwd,
                    "alive": cli.is_alive(),
                    "tools": [t.get("name") for t in cli._tools_cache],
                })
            return {
                "server_count": len(servers),
                "servers": servers,
                "stashed_batches": len(self._stashed_layer_b_results),
            }

    def shutdown_all(self) -> None:
        with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
            self._scope_keys.clear()
        for cli in clients:
            cli.close()
