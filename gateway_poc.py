#!/usr/bin/env python3
"""
gateway_poc.py v4.5.0-resilient - Dual-Plane Pure Gemini Web Quota & API Gateway
Architecture:
  1. 100% Pure Gemini Web Quota (`acp_spawned: False`, ZERO external CLI daemons spawned):
     - Plane 1 (Default Resident Zero-Touch Engine): Native macOS `bin/dsh-webkit-bridge`
       (`WKWebView` isolated data store, `LSUIElement` accessory mode, App-Nap & background-timer immune,
       policy-aware `otAQ7b` routing + two-phase semantic TTFT/idle watchdogs + background drain-to-commit).
     - Plane 2 (Hot-Plug Override): Non-destructive POSIX FIFO bus (`/tmp/dsh_gemini_ipc_{port}`)
       allowing live foreground `Gemini.app` Web Workers to attach/detach at any time.
  2. Dual-Layer Tool Router & Clean Pure-API Mode:
     - Layer A (Client Host / API Tools): Anthropic `tools[]` / OpenAI `tools[]` returned via
       standard SSE/JSON `tool_use` / `tool_calls` with real-time `input_json_delta` streaming.
     - Layer B (`EmbeddedLocalMcpHost`): Local `stdio` MCP servers activated only when explicitly
       enabled (`X-Enable-Local-MCP: 1`, project `.mcp.json`, or Claude Code agent context).
     - Pure-Chat Mode: Zero tool/CWD/DSH-TP boilerplate injection when no tools are active.
  3. `DSH-TP v1.1` Zero-Drop Streaming Envelope Parser + Committed-Cursor Checkpoint Rewind +
     `LosslessPageStore` (`48,000`-char Web single-turn pagination + `192,000`-token empirical rollover).
"""

import argparse
import errno
import glob
import hashlib
import hmac
import http.server
import inspect
import json
import math
import os
import queue
import re
import select
import signal
import socket
import socketserver
import stat
import sys
import threading
import time
import urllib.parse
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from embedded_local_mcp import EmbeddedLocalMcpHost
from streaming_envelope_parser import (
    build_dsh_tp_system_instructions,
    generate_turn_nonce,
    lexical_repair_json,
)
from webkit_plane_client import WebKitPlaneClient

GATEWAY_VERSION = "4.6.0-clean-carbon"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HTTP_HOST = os.environ.get("DSH_GATEWAY_HOST", "127.0.0.1")
HTTP_PORT = int(os.environ.get("DSH_GATEWAY_PORT", "18896"))
IPC_DIR = os.environ.get("DSH_IPC_DIR", f"/tmp/dsh_gemini_ipc_{HTTP_PORT}")
WEBKIT_SOCK = os.environ.get("DSH_WEBKIT_SOCK", f"/tmp/dsh_webkit_{HTTP_PORT}.sock")
WEBKIT_BIN = os.environ.get("DSH_WEBKIT_BIN", os.path.join(SCRIPT_DIR, "bin", "dsh-webkit-bridge"))
LOCAL_MCP_CONFIG = os.environ.get("DSH_LOCAL_MCP_CONFIG", os.path.join(SCRIPT_DIR, "gemini_local_mcp_config.json"))
ACCOUNT_LOCK_FILE = os.environ.get("DSH_ACCOUNT_LOCK_FILE", os.path.join(SCRIPT_DIR, "runtime", "account_lock.json"))
SESSIONS_CACHE_FILE = os.environ.get("DSH_SESSIONS_CACHE_FILE", os.path.join(SCRIPT_DIR, "runtime", "sessions_cache.json"))
SESSION_CACHE_TTL_SEC = float(os.environ.get("DSH_SESSION_CACHE_TTL_SEC", "43200"))
SESSION_IDLE_CLEAN_SEC = float(os.environ.get("DSH_SESSION_IDLE_CLEAN_SEC", "900"))

BATCH_WINDOW_SEC = float(os.environ.get("DSH_BATCH_WINDOW_SEC", "0.08"))
RELAY_INTERVAL_SEC = float(os.environ.get("DSH_RELAY_SEC", "50.0"))
WORKER_CLAIM_TIMEOUT_SEC = float(os.environ.get("DSH_CLAIM_TIMEOUT_SEC", "3.5"))
UPSTREAM_MODE = os.environ.get("DSH_UPSTREAM", "dual_plane_web")
SOFT_WATERMARK_TOKENS = int(os.environ.get("DSH_WATERMARK_TOKENS", "192000"))
HARD_CEILING_TOKENS = int(os.environ.get("DSH_HARD_WATERMARK_TOKENS", "240000"))
MAX_TOOL_RESULT_CHARS = int(os.environ.get("DSH_MAX_TOOL_CHARS", "240000"))
WEB_RPC_PAGE_CHARS = int(os.environ.get("DSH_WEB_RPC_PAGE_CHARS", "48000"))
MAX_INTERNAL_MCP_HOPS = int(os.environ.get("DSH_MAX_MCP_HOPS", "12"))
MAX_REQUEST_BODY_BYTES = 32 * 1024 * 1024
MAX_TOOL_PAGES_PER_SESSION = 32
UNSUPPORTED_MULTIMODAL_TYPES = {"image", "image_url", "input_image", "document", "input_file", "file"}

RUNTIME_MONITORED_RELPATHS = (
    "gateway_poc.py",
    "webkit_plane_client.py",
    "streaming_envelope_parser.py",
    "embedded_local_mcp.py",
    "bin/DSHWebKitBridge.app/Contents/MacOS/dsh-webkit-bridge",
)


def _file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        while True:
            chunk = fp.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _capture_runtime_fingerprints() -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for rel in RUNTIME_MONITORED_RELPATHS:
        full = os.path.join(SCRIPT_DIR, rel)
        try:
            st = os.stat(full)
            out[rel] = {"mtime": int(st.st_mtime), "sha256": _file_sha256(full)}
        except OSError:
            out[rel] = {"mtime": -1, "sha256": ""}
    return out


STARTUP_RUNTIME_FILES = _capture_runtime_fingerprints()

QUIET_POLL_PATHS = {"/health", "/healthz", "/v1/health", "/readyz", "/api/hello", "/v1/models", "/_admin/state"}
DISCONNECT_ERRNOS = {
    errno.EPIPE,
    errno.ECONNRESET,
    getattr(errno, "EPROTOTYPE", 41),
    errno.ENOTCONN,
}

SUPPORTED_MODELS = [
    {
        "id": "gemini-app-carbon",
        "alias": "gemini-4-carbon",
        "label": "Gemini Carbon (Web 旗舰 · Carbon / 4 Hydrogen High 显式思考)",
        "family": "gemini-web",
        "context_window": 1048576,
        "default_tier": "high",
    },
    {
        "id": "gemini-app-thinking",
        "alias": "gemini-4-argon",
        "label": "Gemini Thinking (4 Hydrogen High · 深度显式思考)",
        "family": "gemini-web",
        "context_window": 1048576,
        "default_tier": "high",
    },
    {
        "id": "gemini-app-fast",
        "alias": "gemini-3.8-flash",
        "label": "Gemini Fast (4 Hydrogen Low · 极速响应)",
        "family": "gemini-web",
        "context_window": 1048576,
        "default_tier": "low",
    },
]

MODEL_BY_ID: Dict[str, dict] = {}
for _m in SUPPORTED_MODELS:
    MODEL_BY_ID[_m["id"]] = _m
    MODEL_BY_ID[_m["alias"]] = _m

TIER_SUFFIX_RE = re.compile(r"(?i)[-_](low|medium|high|xhigh)$")
MCP_NAME_CLEAN_RE = re.compile(r"[^a-zA-Z0-9_-]")
CLAUDE_WEB_ONLY_TOOLS = {"WebSearch", "WebFetch", "NotebookEdit"}
SUBAGENT_TOOLS = {"Task", "Agent", "dispatch_agent"}

AWAIT_TOOL_NAME = "dsh_await_result"
CALL_TOOL_NAME = "dsh_call_tool"
PAGE_TOOL_NAME = "dsh_read_tool_page"

BILLING_HEADER_RE = re.compile(r"(?im)^x-anthropic-billing-header:[^\r\n]*$")
SYS_REMINDER_OPEN_RE = re.compile(r"(?i)<\s*system-reminder\s*>")
SYS_REMINDER_CLOSE_RE = re.compile(r"(?i)<\s*/\s*system-reminder\s*>")
ANY_SYS_REMINDER_RE = re.compile(r"(?is)<system-reminder>.*?</system-reminder>")
LANDMARK_LINE_RE = re.compile(r"^\s*(?:class |def |async def |func |struct |enum |interface |export |# |## |=== )")
CWD_PATTERNS = [
    re.compile(r"(?i)Primary working directory:\s*([^\r\n\"\\<]+)"),
    re.compile(r"(?i)Working directory:\s*([^\r\n\"\\<]+)"),
    re.compile(r"(?i)Current working directory:\s*([^\r\n\"\\<]+)"),
]
USER_ID_SESSION_UUID_RE = re.compile(r"(?i)session_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")
RETRY_NOTICE_RE = re.compile(
    r"(?i)^\s*⚠\s*API error \(attempt \d+\)|your previous response was cut off because it exceeded the output token limit"
)


class GatewayHTTPError(Exception):
    """Structured API error mapped cleanly to HTTP status + Anthropic/OpenAI error payload."""

    def __init__(self, status_code: int, error_type: str, message: str, code: Optional[str] = None, param: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type
        self.message = message
        self.code = code
        self.param = param


class PayloadTooLargeError(ValueError):
    """Raised when an HTTP request body exceeds MAX_REQUEST_BODY_BYTES."""


class RequestCancelledError(RuntimeError):
    """Raised when the downstream client disconnects or cancels an active turn."""


def random_hex(n_bytes: int = 8) -> str:
    return uuid.uuid4().hex[: n_bytes * 2]


def short_sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def mcp_tool_name(host_name: str) -> str:
    clean = MCP_NAME_CLEAN_RE.sub("_", host_name)[:40]
    return f"dsh_{clean}_{short_sha256(host_name)[:12]}"


def est_tokens_str(s: str) -> int:
    return max(1, int(math.ceil(len((s or "").encode("utf-8")) / 3.95)))


def _safe_int(val: Any, default: int = 1) -> int:
    if val is None or isinstance(val, bool):
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        try:
            return int(float(str(val).strip()))
        except (TypeError, ValueError):
            return default


def _filter_supported_kwargs(fn: Any, **kwargs: Any) -> dict:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return kwargs
    params = sig.parameters
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return kwargs
    return {k: v for k, v in kwargs.items() if k in params}


def _extract_content_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                btype = str(item.get("type", "")).lower()
                if btype in ("text", "input_text", "output_text", ""):
                    txt = item.get("text")
                    if txt is not None:
                        parts.append(str(txt))
        return "\n".join(p for p in parts if p)
    return str(content)


def _dehydrate_text(text: str, max_chars: int = 2000, head_chars: int = 800, tail_chars: int = 400) -> str:
    if len(text) <= max_chars:
        return text
    return (
        f"[DSH_TOMBSTONE original_chars={len(text)}]: "
        + text[:head_chars]
        + " ... [dehydrated during 256K rollover] ... "
        + text[-tail_chars:]
    )


def sanitize_outbound_text(s: str) -> str:
    """Preserves exact user/tool text without rewriting model/brand nouns."""
    if not s:
        return ""
    return BILLING_HEADER_RE.sub("", s)


def prune_client_system_prompt(raw_sys: str) -> str:
    """Strips client billing headers, Claude Code identity boilerplate, and `<system-reminder>` blocks from `system_text`."""
    if not raw_sys:
        return ""
    s = BILLING_HEADER_RE.sub("", raw_sys).replace("\r\n", "\n")
    s = ANY_SYS_REMINDER_RE.sub("", s)
    s = SYS_REMINDER_OPEN_RE.sub("", s)
    s = SYS_REMINDER_CLOSE_RE.sub("", s)
    kept = []
    for line in s.split("\n"):
        stripped = line.strip()
        if stripped.startswith("You are Claude Code") or stripped.startswith("You are a Claude agent"):
            continue
        kept.append(line)
    return "\n".join(kept).strip()


def detect_cwd_from_context(sys_text: str, messages: list) -> Tuple[str, bool]:
    sample = sys_text + "\n" + json.dumps((messages or [])[:3], ensure_ascii=False)
    for pat in CWD_PATTERNS:
        m = pat.search(sample)
        if m:
            cand = m.group(1).strip()
            if os.path.isdir(cand):
                return cand, True
    return "/tmp", False


def extract_cwd_from_context(sys_text: str, messages: list) -> str:
    cwd, _ = detect_cwd_from_context(sys_text, messages)
    return cwd


def fold_mid_stream_system_messages(sys_text: str, raw_messages: list) -> Tuple[str, list]:
    """Folds any `role: 'system'|'developer'` entries inside `messages` cleanly into user/system context."""
    if not raw_messages:
        return sys_text, []
    extra_sys: List[str] = []
    cleaned_msgs: List[dict] = []
    pending_note: List[str] = []

    for m in raw_messages:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role", "user")).lower()
        if role in ("system", "developer"):
            txt = _extract_content_text(m.get("content", "")).strip()
            if txt:
                is_invariant_env = txt.startswith("# Environment") or ("You have been invoked in the following environment" in txt)
                if not cleaned_msgs or is_invariant_env:
                    if txt not in extra_sys:
                        extra_sys.append(txt)
                else:
                    pending_note.append(f"[System Note]: {txt}")
            continue

        msg_copy = dict(m)
        if pending_note and role == "user":
            note_str = "\n".join(pending_note)
            pending_note.clear()
            mc = msg_copy.get("content", "")
            if isinstance(mc, str):
                msg_copy["content"] = note_str + "\n\n" + mc
            elif isinstance(mc, list):
                msg_copy["content"] = [{"type": "text", "text": note_str}] + list(mc)
        cleaned_msgs.append(msg_copy)

    if pending_note:
        if cleaned_msgs and str(cleaned_msgs[-1].get("role", "")).lower() == "user":
            # Avoid inflating `len(messages)` / changing `first_user_fingerprint` when a client
            # (such as Claude Code CLI) appends a trailing `role: "system"` environment block after
            # the user prompt on Turn 1 and moves it before `tool_result` on Turn 2.
            extra_sys.extend(n[len("[System Note]: "):] if n.startswith("[System Note]: ") else n for n in pending_note)
        else:
            cleaned_msgs.append({"role": "user", "content": "\n".join(pending_note)})
    merged_sys = "\n\n".join([p for p in ([sys_text] if sys_text else []) + extra_sys if p])
    return merged_sys, cleaned_msgs


def sanitize_schema_node(node: Any) -> Any:
    if isinstance(node, list):
        return [sanitize_schema_node(x) for x in node]
    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, val in node.items():
            if k in ("pattern", "prefixItems", "$schema", "additionalProperties", "propertyNames", "unevaluatedProperties", "const", "default", "examples", "title"):
                continue
            if k == "$ref":
                out["type"] = "object"
                continue
            if k in ("anyOf", "oneOf", "allOf") and isinstance(val, list) and val:
                picked = next((sub for sub in val if isinstance(sub, dict) and sub.get("type") != "null"), val[0])
                pm = sanitize_schema_node(picked)
                if isinstance(pm, dict):
                    out.update(pm)
                continue
            if k == "type" and isinstance(val, list):
                out["type"] = next((t for t in val if isinstance(t, str) and t != "null"), "string")
                continue
            out[k] = sanitize_schema_node(val)
        if out.get("type") == "array" and "items" not in out:
            out["items"] = {"type": "string"}
        if isinstance(out.get("description"), str):
            out["description"] = out["description"].strip()[:200]
        return out
    return node


def extract_referenced_deferred_tools(messages: list) -> Set[str]:
    unlocked: Set[str] = set()
    for m in messages or []:
        mc = m.get("content")
        if isinstance(mc, list):
            for blk in mc:
                if not isinstance(blk, dict):
                    continue
                if blk.get("type") == "tool_use" and blk.get("name"):
                    unlocked.add(str(blk["name"]))
                elif blk.get("type") == "tool_reference" and blk.get("tool_name"):
                    unlocked.add(str(blk["tool_name"]))
    return unlocked


def sanitize_tools_catalog(raw_tools: list, messages: Optional[list] = None) -> List[dict]:
    if not raw_tools:
        return []
    unlocked_deferred = extract_referenced_deferred_tools(messages or [])
    seen = set()
    catalog: List[dict] = []
    ALWAYS_UNLOCKED_CORE_TOOLS = {"Bash", "Read", "Write", "Edit", "MultiEdit", "Glob", "Grep", "LS", "ToolSearch", "Agent", "Task"}
    for item in raw_tools:
        if not isinstance(item, dict):
            continue
        raw_item_name = str(item.get("name", "")).strip()
        if item.get("defer_loading") is True and raw_item_name not in unlocked_deferred and raw_item_name not in ALWAYS_UNLOCKED_CORE_TOOLS:
            continue
        if "function" in item and isinstance(item["function"], dict):
            fn = item["function"]
            name = str(fn.get("name", "")).strip()
            raw_desc = str(fn.get("description", ""))
            raw_schema = fn.get("parameters")
        else:
            name = str(item.get("name", "")).strip()
            raw_desc = str(item.get("description", ""))
            raw_schema = item.get("input_schema") or item.get("parameters")

        if not name or name in seen or name in CLAUDE_WEB_ONLY_TOOLS:
            continue
        seen.add(name)

        san = sanitize_schema_node(raw_schema or {"type": "object", "properties": {}})
        if not isinstance(san, dict):
            san = {"type": "object", "properties": {}}
        props = san.get("properties") if isinstance(san.get("properties"), dict) else {}
        props.pop("toolSummary", None)
        req_list = [r for r in (san.get("required") or []) if isinstance(r, str) and r != "toolSummary" and r in props]
        clean_schema: Dict[str, Any] = {"type": "object", "properties": props}
        if req_list:
            clean_schema["required"] = req_list

        desc = raw_desc.strip().split("\n", 1)[0][:240] or f"Execute host tool {name}"
        catalog.append({
            "name": name,
            "mcp_name": mcp_tool_name(name),
            "description": desc,
            "parameters": clean_schema,
        })

    if catalog:
        if PAGE_TOOL_NAME not in seen:
            catalog.append({
                "name": PAGE_TOOL_NAME,
                "mcp_name": mcp_tool_name(PAGE_TOOL_NAME),
                "description": "Read a specific page or grep pattern from a lossless paginated tool output (`DSH_AUTO_PAGINATED`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "page_token": {"type": "string", "description": "Page token from DSH_AUTO_PAGINATED header."},
                        "page": {"type": "integer", "description": "1-indexed page number to read."},
                        "grep": {"type": "string", "description": "Optional substring/regex filter across all stored pages."},
                    },
                    "required": ["page_token"],
                },
            })
        if AWAIT_TOOL_NAME not in seen:
            catalog.append({
                "name": AWAIT_TOOL_NAME,
                "mcp_name": mcp_tool_name(AWAIT_TOOL_NAME),
                "description": "Wait for the result of a harness tool call that reported it is still running.",
                "parameters": {
                    "type": "object",
                    "properties": {"call_id": {"type": "string", "description": "The call id from the still-running notice."}},
                    "required": ["call_id"],
                },
            })
        if CALL_TOOL_NAME not in seen:
            catalog.append({
                "name": CALL_TOOL_NAME,
                "mcp_name": mcp_tool_name(CALL_TOOL_NAME),
                "description": "Invoke any active harness tool by exact name and arguments.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Exact host tool name."},
                        "arguments": {"type": "object", "description": "Arguments object."},
                    },
                    "required": ["name"],
                },
            })
    return catalog


def resolve_model_and_tier(requested: Any, body: dict) -> Tuple[dict, str]:
    if requested is not None and not isinstance(requested, str):
        raise ValueError("'model' must be a string")
    if not isinstance(body, dict):
        raise ValueError("Request body must be a JSON object")

    s = (requested or "").strip().lower()
    if s.startswith("models/"):
        s = s[7:]
    if s.endswith("[1m]"):
        s = s[:-4]
    tier = ""
    if s not in MODEL_BY_ID:
        m = TIER_SUFFIX_RE.search(s)
        if m:
            tier = m.group(1).lower()
            s = TIER_SUFFIX_RE.sub("", s)

    reasoning_obj = body.get("reasoning")
    if reasoning_obj is not None and not isinstance(reasoning_obj, dict):
        raise ValueError("'reasoning' must be an object")
    out_cfg_obj = body.get("output_config")
    if out_cfg_obj is not None and not isinstance(out_cfg_obj, dict):
        raise ValueError("'output_config' must be an object")

    effort_raw = (
        body.get("reasoning_effort")
        or (reasoning_obj or {}).get("effort", "")
        or (out_cfg_obj or {}).get("effort", "")
    )
    if effort_raw and not isinstance(effort_raw, str):
        raise ValueError("'reasoning_effort'/'effort' must be a string")
    effort = str(effort_raw or "").strip().lower()
    if effort in ("low", "medium", "high"):
        tier = effort
    elif effort in ("xhigh", "max"):
        tier = "high"
    elif effort == "minimal":
        tier = "low"
    elif effort:
        raise ValueError(f"Unsupported reasoning effort: {effort_raw!r}")

    thk_raw = body.get("thinking")
    if thk_raw is not None:
        if not isinstance(thk_raw, dict):
            raise ValueError("'thinking' must be an object")
        thk_type = str(thk_raw.get("type", "")).strip().lower()
        if thk_type and thk_type not in ("enabled", "adaptive", "disabled"):
            raise ValueError(f"Unsupported thinking.type: {thk_type!r}")
        if thk_type == "disabled":
            # Explicitly override any `-high` suffix or effort tier when thinking is disabled.
            tier = "low"
        elif thk_type in ("enabled", "adaptive"):
            raw_budget = thk_raw.get("budget_tokens")
            if raw_budget is not None:
                if isinstance(raw_budget, bool):
                    raise ValueError("'thinking.budget_tokens' must be an integer")
                try:
                    b_tok = int(raw_budget)
                except (TypeError, ValueError) as exc:
                    raise ValueError("'thinking.budget_tokens' must be an integer") from exc
                if b_tok < 0:
                    raise ValueError("'thinking.budget_tokens' must be >= 0")
                if b_tok == 0:
                    tier = "low"
                elif not tier:
                    tier = "high" if b_tok >= 8000 else ("medium" if b_tok >= 2000 else "low")
            elif not tier:
                tier = "high"

    info = MODEL_BY_ID.get(s)
    if info is None:
        if "fast" in s or "flash" in s or "haiku" in s:
            info = MODEL_BY_ID["gemini-app-fast"]
        elif "thinking" in s or "sonnet" in s:
            info = MODEL_BY_ID["gemini-app-thinking"]
        else:
            info = MODEL_BY_ID["gemini-app-carbon"]

    if not tier:
        tier = info.get("default_tier", "high")
    return info, tier


def should_emit_thinking(body: dict, model_info: dict, tier: str, is_openai: bool = False) -> bool:
    thk_cfg = body.get("thinking")
    if isinstance(thk_cfg, dict):
        t_type = str(thk_cfg.get("type", "")).strip().lower()
        if t_type == "disabled":
            return False
        if t_type in ("enabled", "adaptive"):
            raw_b = thk_cfg.get("budget_tokens")
            if raw_b is not None and _safe_int(raw_b, 16000) <= 0:
                return False
            return True

    has_oai_reasoning = bool(
        body.get("reasoning_effort")
        or (isinstance(body.get("reasoning"), dict) and body["reasoning"].get("effort"))
    )
    if has_oai_reasoning:
        effort = str(
            body.get("reasoning_effort") or (body.get("reasoning") or {}).get("effort") or ""
        ).strip().lower()
        return effort not in ("none", "off", "disabled")

    if model_info.get("id") == "gemini-app-fast" or is_openai:
        return False
    return tier != "low" or model_info.get("id") in ("gemini-app-carbon", "gemini-app-thinking")


def extract_stop_sequences(body: dict) -> List[str]:
    if "stop_sequences" in body and body["stop_sequences"] is not None:
        raw = body["stop_sequences"]
        if not isinstance(raw, list) or any(not isinstance(x, str) for x in raw):
            raise ValueError("'stop_sequences' must be an array of strings")
        return [x for x in raw if x]
    if "stop" in body and body["stop"] is not None:
        raw = body["stop"]
        if isinstance(raw, str):
            return [raw] if raw else []
        if isinstance(raw, list) and all(isinstance(x, str) for x in raw):
            return [x for x in raw if x]
        raise ValueError("'stop' must be a string or an array of strings")
    return []


def parse_tool_choice(body: dict, tools: list) -> Tuple[str, str]:
    tc = body.get("tool_choice")
    if tc is None:
        return "auto", ""

    avail_names: Set[str] = set()
    for item in tools or []:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("function"), dict):
            nm = str(item["function"].get("name", "")).strip()
        else:
            nm = str(item.get("name", "")).strip()
        if nm:
            avail_names.add(nm)

    if isinstance(tc, str):
        low = tc.strip().lower()
        if low in ("auto", "none"):
            return low, ""
        if low in ("any", "required"):
            if not avail_names:
                raise ValueError(f"tool_choice={low!r} requires at least one declared tool")
            return "any", ""
        raise ValueError(f"Unsupported tool_choice: {tc!r}")

    if isinstance(tc, dict):
        t_type = str(tc.get("type", "")).strip().lower()
        if t_type in ("auto", "none"):
            return t_type, ""
        if t_type in ("any", "required"):
            if not avail_names:
                raise ValueError(f"tool_choice.type={t_type!r} requires at least one declared tool")
            return "any", ""
        if t_type in ("tool", "function"):
            fn_obj = tc.get("function") if isinstance(tc.get("function"), dict) else {}
            target = str(tc.get("name") or fn_obj.get("name") or "").strip()
            if not target:
                raise ValueError("tool_choice requires a non-empty tool/function name")
            if avail_names and target not in avail_names:
                raise ValueError(f"tool_choice references undeclared tool {target!r}")
            if not avail_names:
                raise ValueError(f"tool_choice={target!r} requires declared tools")
            return "tool", target
        raise ValueError(f"Unsupported tool_choice object: {tc!r}")

    raise ValueError("'tool_choice' must be a string or object")


def find_unsupported_multimodal_block(messages: list, raw_input: Any = None) -> Optional[str]:
    def _scan_blocks(val: Any) -> Optional[str]:
        if isinstance(val, list):
            for blk in val:
                if not isinstance(blk, dict):
                    continue
                btype = str(blk.get("type", "")).strip().lower()
                if btype in UNSUPPORTED_MULTIMODAL_TYPES:
                    return btype
                if "image_url" in blk or "image" in blk:
                    return "image"
                nested = _scan_blocks(blk.get("content"))
                if nested:
                    return nested
        return None

    for m in messages or []:
        if not isinstance(m, dict):
            continue
        hit = _scan_blocks(m.get("content"))
        if hit:
            return hit
    if isinstance(raw_input, list):
        for it in raw_input:
            if isinstance(it, dict):
                btype = str(it.get("type", "")).strip().lower()
                if btype in UNSUPPORTED_MULTIMODAL_TYPES:
                    return btype
                hit = _scan_blocks(it.get("content"))
                if hit:
                    return hit
    return None


def estimate_output_tokens(outcome: dict, include_thinking: bool = True) -> int:
    text_s = str(outcome.get("text") or "")
    think_s = str(outcome.get("thinking") or "") if include_thinking else ""
    tcs = outcome.get("tool_calls") or []
    tools_s = json.dumps(tcs, ensure_ascii=False) if tcs else ""
    total_chars = len(text_s) + len(think_s) + len(tools_s)
    if total_chars <= 0:
        return 1
    return max(1, int(math.ceil(total_chars / 3.95)))


def apply_stop_sequences_to_text(text: str, stop_seqs: List[str]) -> Tuple[str, Optional[str]]:
    if not text or not stop_seqs:
        return text, None
    best_idx = -1
    best_seq: Optional[str] = None
    for seq in stop_seqs:
        idx = text.find(seq)
        if idx != -1 and (best_idx == -1 or idx < best_idx):
            best_idx = idx
            best_seq = seq
    if best_seq is not None:
        return text[:best_idx], best_seq
    return text, None


class StopSequenceFilter:
    """Streaming holdback filter that truncates output at the earliest configured stop sequence."""

    def __init__(self, stop_seqs: Optional[List[str]] = None):
        self.stop_seqs = [s for s in (stop_seqs or []) if s]
        self.max_len = max((len(s) for s in self.stop_seqs), default=0)
        self.buf = ""
        self.matched: Optional[str] = None

    def feed(self, chunk: str) -> Tuple[str, Optional[str]]:
        if not chunk or self.matched is not None:
            return "", self.matched
        if not self.stop_seqs:
            return chunk, None
        self.buf += chunk
        truncated, hit = apply_stop_sequences_to_text(self.buf, self.stop_seqs)
        if hit is not None:
            self.matched = hit
            self.buf = ""
            return truncated, hit
        if self.max_len <= 1:
            emit = self.buf
            self.buf = ""
            return emit, None
        hold = self.max_len - 1
        if len(self.buf) > hold:
            emit = self.buf[:-hold]
            self.buf = self.buf[-hold:]
            return emit, None
        return "", None

    def flush(self) -> Tuple[str, Optional[str]]:
        if self.matched is not None or not self.buf:
            self.buf = ""
            return "", self.matched
        truncated, hit = apply_stop_sequences_to_text(self.buf, self.stop_seqs)
        self.buf = ""
        if hit is not None:
            self.matched = hit
        return truncated, self.matched


def classify_fast_utility_call(hdrs: dict, sys_text: str, tools: list, messages: list, max_tok: int) -> Optional[str]:
    """Zero-Web-quota local fast-path responder scoped strictly to Claude Code & keep-alive pings."""
    if not messages:
        return None
    last = messages[-1]
    if not isinstance(last, dict) or last.get("role") != "user":
        return None
    last_raw = json.dumps(last, ensure_ascii=False)
    if "Automated prompt-cache keep-alive ping" in last_raw:
        return "OK"

    lower_hdrs = {k.lower(): str(v) for k, v in (hdrs or {}).items()}
    is_cc_caller = (
        "claude-cli" in lower_hdrs.get("user-agent", "").lower()
        or any(k.startswith("x-claude-code-") for k in lower_hdrs)
    )
    if not is_cc_caller:
        return None

    if max_tok == 1:
        return "OK"

    content = last.get("content", "")
    tail_txt = ""
    has_tool_blocks = False
    for m in messages:
        mc = m.get("content")
        if isinstance(mc, list):
            for b in mc:
                if isinstance(b, dict) and b.get("type") in ("tool_use", "tool_result"):
                    has_tool_blocks = True
                    break
    if isinstance(content, str):
        tail_txt = content.strip()
    elif isinstance(content, list):
        for blk in reversed(content):
            if not isinstance(blk, dict):
                continue
            if blk.get("type") == "text":
                txt = blk.get("text", "").strip()
                if txt.startswith("<system-reminder>"):
                    continue
                tail_txt = txt
                break
            elif blk.get("type") == "tool_result":
                break

    if tail_txt.startswith("[SUGGESTION MODE:"):
        return ""
    if len(tail_txt) <= 4000:
        last_line = tail_txt.rsplit("\n", 1)[-1].strip()
        if tail_txt.startswith("Describe your most recent action in 3-5 words") or last_line.startswith("Describe your most recent action in 3-5 words"):
            return "Inspecting project files"

    req_cls = lower_hdrs.get("x-claude-code-request-class", "").lower()
    is_bg_toolless = (len(tools) == 0 and "Primary working directory" not in sys_text and not has_tool_blocks)
    combined = (sys_text[:2500] + "\n" + tail_txt[:2500])
    is_utility_prompt = (
        "isNewTopic" in combined
        or "<policy_spec>" in combined
        or "<filepaths>" in combined
        or "Extract any file paths" in combined
        or ("title" in combined.lower() and ("3-6" in combined or "concise" in combined.lower()))
    )

    if req_cls in ("utility", "background", "title", "suggestion") or (is_bg_toolless and is_utility_prompt):
        if "isNewTopic" in combined:
            return '{"isNewTopic": false, "title": null}'
        if "<filepaths>" in combined or "Extract any file paths" in combined:
            return "<filepaths>\n</filepaths>"
        if "<policy_spec>" in combined or "<prefix>" in combined or "command prefix" in combined.lower():
            return "none"
        if "title" in combined.lower() and ("3-6" in combined or "concise" in combined.lower() or req_cls == "title"):
            return "Project Inspection"
        return "OK"
    return None


def estimate_count_tokens(body: dict) -> int:
    chars = len(json.dumps(body.get("system", "") or "", ensure_ascii=False))
    chars += len(json.dumps(body.get("messages", []) or [], ensure_ascii=False))
    chars += len(json.dumps(body.get("tools", []) or [], ensure_ascii=False))
    return max(1, int(math.ceil(chars / 3.95)))


def build_landmark_preview(full_text: str, page_size: int, page_token: str) -> Tuple[List[str], str]:
    pages = [full_text[i : i + page_size] for i in range(0, len(full_text), page_size)]
    landmarks: List[str] = []
    for line_no, ln in enumerate(full_text.splitlines(), start=1):
        if LANDMARK_LINE_RE.match(ln):
            landmarks.append(f"L{line_no}: {ln.strip()[:96]}")
            if len(landmarks) >= 18:
                break
    toc_block = ""
    if landmarks:
        toc_block = "[Structural Landmarks]\n" + "\n".join(landmarks) + "\n\n"

    head_budget = max(4000, int(page_size * 0.75))
    tail_budget = max(1000, int(page_size * 0.16))
    head_part = full_text[:head_budget]
    tail_part = full_text[-tail_budget:] if len(full_text) > (head_budget + tail_budget) else ""
    if len(pages) > 1:
        p2_end = min(len(full_text), 2 * page_size)
        page_guide = (
            f"This preview omits middle characters. Call `{PAGE_TOOL_NAME}` with "
            f'{{"page_token": "{page_token}", "page": 1}} to read the full untruncated first {page_size} chars '
            f'(chars 1-{page_size}), or `"page": 2` for the second {page_size} chars '
            f'(chars {page_size + 1}-{p2_end}), or `"grep": "keyword"`.'
        )
    else:
        page_guide = (
            f"Call `{PAGE_TOOL_NAME}` with "
            f'{{"page_token": "{page_token}", "page": 1}} for the full untruncated {len(full_text)} chars (or `"grep": "keyword"`).'
        )
    header = (
        f"[DSH_AUTO_PAGINATED page=1/{len(pages)} total_chars={len(full_text)} page_token={page_token}]\n"
        f"{page_guide}\n\n"
    )
    body = header + toc_block + head_part
    if tail_part:
        body += f"\n\n...[omitted middle chars; use {PAGE_TOOL_NAME}(page_token={page_token!r}, page=1..{len(pages)}) to read]...\n\n[Tail Preview]\n" + tail_part
    return pages, body


def _format_tool_result_text(raw_text: str, max_result_chars: int, tombstone_dehydrate: bool, paginate_fn: Optional[Callable[[str], str]]) -> str:
    if tombstone_dehydrate:
        return _dehydrate_text(raw_text, max_chars=1200, head_chars=600, tail_chars=300)
    if paginate_fn is not None:
        return paginate_fn(raw_text)
    return raw_text[:max_result_chars]


def format_single_message_clean(
    m: dict,
    max_result_chars: int = MAX_TOOL_RESULT_CHARS,
    tombstone_dehydrate: bool = False,
    strip_reminders: bool = False,
    paginate_fn: Optional[Callable[[str], str]] = None,
) -> str:
    if not isinstance(m, dict):
        return ""
    role = str(m.get("role", "user")).lower()

    if role == "tool":
        tc_id = str(m.get("tool_call_id") or "")
        raw_c = _extract_content_text(m.get("content", ""))
        formatted_c = _format_tool_result_text(raw_c, max_result_chars, tombstone_dehydrate, paginate_fn)
        sep = "\n" if paginate_fn is not None else " "
        return f"[Tool Result ({tc_id})]:{sep}{formatted_c}"

    parts: List[str] = []
    content = m.get("content")
    if isinstance(content, str):
        txt = ANY_SYS_REMINDER_RE.sub("", content) if (strip_reminders and role == "user") else content
        if tombstone_dehydrate:
            txt = _dehydrate_text(txt, max_chars=2000, head_chars=800, tail_chars=400)
        if txt:
            parts.append(txt)
    elif isinstance(content, list):
        for blk in content:
            if not isinstance(blk, dict):
                continue
            btype = blk.get("type", "")
            if btype in ("thinking", "redacted_thinking", "image", "image_url", "input_image"):
                continue
            if btype in ("text", "input_text", "output_text"):
                raw_txt = str(blk.get("text", ""))
                txt = ANY_SYS_REMINDER_RE.sub("", raw_txt) if (strip_reminders and role == "user") else raw_txt
                if tombstone_dehydrate:
                    txt = _dehydrate_text(txt, max_chars=2000, head_chars=800, tail_chars=400)
                if txt:
                    parts.append(txt)
            elif btype == "tool_use":
                inp_json = json.dumps(blk.get("input", {}), ensure_ascii=False)
                if tombstone_dehydrate:
                    inp_json = _dehydrate_text(inp_json, max_chars=2000, head_chars=800, tail_chars=400)
                parts.append(f"[Tool Call ({blk.get('name')}, id={blk.get('id')})]: {inp_json}")
            elif btype == "tool_result":
                c = blk.get("content", "")
                if isinstance(c, list):
                    c = _extract_content_text(c)
                elif c is None:
                    c = ""
                else:
                    c = str(c)
                formatted_c = _format_tool_result_text(c, max_result_chars, tombstone_dehydrate, paginate_fn)
                sep = "\n" if (paginate_fn is not None and not tombstone_dehydrate) else " "
                parts.append(f"[Tool Result ({blk.get('tool_use_id')})]:{sep}{formatted_c}")
    elif content is not None:
        txt = str(content)
        if tombstone_dehydrate:
            txt = _dehydrate_text(txt, max_chars=2000, head_chars=800, tail_chars=400)
        if txt:
            parts.append(txt)

    if role == "assistant" and isinstance(m.get("tool_calls"), list):
        for tc in m["tool_calls"]:
            if not isinstance(tc, dict):
                continue
            tc_id = str(tc.get("id") or "")
            fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
            tc_name = str(fn.get("name") or tc.get("name") or "")
            raw_args = fn.get("arguments") if "arguments" in fn else tc.get("arguments", "{}")
            if isinstance(raw_args, str):
                args_str = raw_args
            else:
                args_str = json.dumps(raw_args or {}, ensure_ascii=False)
            if tombstone_dehydrate:
                args_str = _dehydrate_text(args_str, max_chars=2000, head_chars=800, tail_chars=400)
            parts.append(f"[Tool Call ({tc_name}, id={tc_id})]: {args_str}")

    return "\n".join(parts)


def compute_messages_prefix_hash(messages: list, strip_reminders: bool = True) -> str:
    if not messages:
        return ""
    parts = [
        f"{str(m.get('role', 'user')).lower()}:{format_single_message_clean(m, strip_reminders=strip_reminders).strip()}"
        for m in messages
        if isinstance(m, dict)
    ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]


def estimate_dehydrated_context_tokens(system_text: str, messages: list) -> int:
    clean_sys = prune_client_system_prompt(system_text)[:120000]
    total = est_tokens_str(clean_sys) if clean_sys else 0
    total_msgs = len(messages or [])
    for idx, m in enumerate(messages or []):
        dehydrate_this = idx < max(0, total_msgs - 4)
        total += est_tokens_str(
            format_single_message_clean(m, max_result_chars=WEB_RPC_PAGE_CHARS, tombstone_dehydrate=dehydrate_this)
        )
    return max(1, total)


def first_user_fingerprint(messages: list, strip_reminders: bool = True) -> str:
    if not messages:
        return ""
    limit = next((i for i, m in enumerate(messages) if isinstance(m, dict) and m.get("role") == "assistant"), min(2, len(messages)))
    parts = []
    has_nonempty = False
    for m in messages[:limit]:
        if not isinstance(m, dict):
            continue
        cleaned = format_single_message_clean(m, strip_reminders=strip_reminders).strip()
        if not cleaned and strip_reminders:
            cleaned = format_single_message_clean(m, strip_reminders=False).strip()
        if cleaned:
            has_nonempty = True
        parts.append(f"{m.get('role', 'user')}:{cleaned}")
    if not parts or not has_nonempty:
        return ""
    return short_sha256("||".join(parts))


def is_subagent_request(headers: dict, body: dict) -> bool:
    lower_hdrs = {k.lower(): v for k, v in (headers or {}).items()}
    if lower_hdrs.get("x-claude-code-agent-id", "").strip():
        return True
    meta = body.get("metadata") if isinstance(body, dict) and isinstance(body.get("metadata"), dict) else {}
    if meta.get("agent_id"):
        return True
    uid_str = str(meta.get("user_id") or "")
    return '"agent_id"' in uid_str


def derive_conv_key(headers: dict, body: dict, sys_text: str, messages: list) -> str:
    lower_hdrs = {k.lower(): v for k, v in (headers or {}).items()}
    explicit = ""
    for hk in ("x-claude-code-session-id", "x-session-id", "session-id", "thread-id", "x-conversation-id"):
        if lower_hdrs.get(hk, "").strip():
            explicit = lower_hdrs[hk].strip()
            break
    agent_id = lower_hdrs.get("x-claude-code-agent-id", "").strip()

    meta = body.get("metadata") if isinstance(body, dict) and isinstance(body.get("metadata"), dict) else {}
    if not explicit:
        explicit = str(meta.get("session_id") or meta.get("conversation_id") or "").strip()
    if not agent_id:
        agent_id = str(meta.get("agent_id") or "").strip()
    uid_str = str(meta.get("user_id") or "").strip()
    if uid_str:
        if uid_str.startswith("{"):
            try:
                uid_obj = json.loads(uid_str)
                if not explicit and uid_obj.get("session_id"):
                    explicit = str(uid_obj["session_id"]).strip()
                if not agent_id and uid_obj.get("agent_id"):
                    agent_id = str(uid_obj["agent_id"]).strip()
            except Exception:
                pass
        if not explicit:
            m = USER_ID_SESSION_UUID_RE.search(uid_str)
            if m:
                explicit = m.group(1)

    if explicit:
        req_cls = str(lower_hdrs.get("x-claude-code-request-class", "")).strip().lower()
        if req_cls and req_cls not in ("interactive", "main", "default", "chat", "agent", "primary"):
            return f"sid:{explicit}:cls:{req_cls}"
        if agent_id:
            return f"sid:{explicit}:ag:{agent_id}"
        return f"sid:{explicit}"

    first_fp = first_user_fingerprint(messages)[:8]
    if not first_fp:
        # Never collide concurrent empty/contentless requests on a shared `fp:` key.
        ephem = random_hex(8)
        return f"ephem:{ephem}:ag:{agent_id}" if agent_id else f"ephem:{ephem}"

    pruned_sys = prune_client_system_prompt(sys_text)
    sys_fp = short_sha256(pruned_sys[:4096])[:8]
    if agent_id:
        return f"fp:{sys_fp}:ag:{agent_id}:u:{first_fp}"
    return f"fp:{sys_fp}:u:{first_fp}"


class SafeSSEWriter:
    """Thread-safe SSE socket writer that swallows client disconnects cleanly without tracebacks."""

    def __init__(self, wfile: Any, on_close: Optional[Callable[[], None]] = None):
        self._wfile = wfile
        self._lock = threading.Lock()
        self._on_close = on_close
        self.closed = False

    def mark_closed(self) -> None:
        notify = False
        with self._lock:
            if not self.closed:
                self.closed = True
                notify = True
        if notify and self._on_close is not None:
            try:
                self._on_close()
            except Exception:
                pass

    def write_raw(self, raw_bytes: bytes) -> bool:
        failed = False
        with self._lock:
            if self.closed:
                return False
            try:
                self._wfile.write(raw_bytes)
                self._wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError):
                self.closed = True
                failed = True
            except OSError as exc:
                if exc.errno in DISCONNECT_ERRNOS:
                    self.closed = True
                    failed = True
                else:
                    self.closed = True
                    failed = True
        if failed and self._on_close is not None:
            try:
                self._on_close()
            except Exception:
                pass
        return False

    def write_event(self, ev: str, obj: dict) -> bool:
        payload = f"event: {ev}\ndata: {json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8")
        return self.write_raw(payload)

    def write_comment(self, comment: str) -> bool:
        return self.write_raw(f": {comment}\n\n".encode("utf-8"))


class ParkedCall:
    def __init__(self, call_id: str, name: str, args: dict):
        self.call_id = call_id
        self.name = name
        self.args = args
        self.result_q: "queue.Queue[dict]" = queue.Queue(maxsize=1)
        self.delivered = False
        self.parked_at = time.time()
        self.relay_count = 0


class ActiveTurn:
    def __init__(self, on_delta: Optional[Callable[[dict], None]] = None):
        self.lock = threading.RLock()
        self.tool_calls: List[ParkedCall] = []
        self.batch_timer: Optional[threading.Timer] = None
        self.resolved = False
        self.outcome_q: "queue.Queue[dict]" = queue.Queue(maxsize=1)
        self.created_at = time.time()
        self.first_delta_at: Optional[float] = None
        self.delta_chunks = 0
        self.streamed_text_parts: List[str] = []
        self.streamed_think_parts: List[str] = []
        self.on_delta = on_delta

    def push_delta(self, kind: str, content: str) -> None:
        if not content:
            return
        if kind == "text" and RETRY_NOTICE_RE.search(content):
            return
        with self.lock:
            if self.resolved:
                return
            if self.first_delta_at is None:
                self.first_delta_at = time.time()
            self.delta_chunks += 1
            if kind == "thinking":
                self.streamed_think_parts.append(content)
            else:
                self.streamed_text_parts.append(content)
            cb = self.on_delta
        if cb is not None:
            cb({"type": kind, "content": content})


class SessionState:
    def __init__(self, conv_key: str, session_token: str):
        self.lock = threading.RLock()
        self.turn_lock = threading.Lock()
        self.conv_key = conv_key
        self.session_token = session_token
        self.cwd: str = "/tmp"
        self.has_explicit_cwd: bool = False
        self.system_text: str = ""
        self.tools_catalog: List[dict] = []
        self.mcp_to_host: Dict[str, str] = {}
        self.allowed_tools: Dict[str, dict] = {}
        self.recent_tool_sigs: List[str] = []
        self.parked_calls: Dict[str, ParkedCall] = {}
        self.tool_pages: Dict[str, List[str]] = {}
        self.active_turn: Optional[ActiveTurn] = None
        self.next_delta_q: "queue.Queue[dict]" = queue.Queue()
        self.worker_waiting_next = False
        self.in_flight = False
        self.model_id: str = "gemini-app-carbon"
        self.tier: str = "high"
        self.account: str = ""
        self.synced_msg_count: int = 0
        self.first_fp: str = ""
        self.net_context_tokens: int = 0
        self.rollover_count: int = 0
        self.reuse_count: int = 0
        self.last_sys_sig: str = ""
        self.last_tools_sig: str = ""
        self.web_cursor: Tuple[str, str, str] = ("", "", "")
        self.web_turn_index: int = 0
        self.cursor_checkpoints: Dict[int, Tuple[Tuple[str, str, str], int, int, str]] = {}
        self.seen_web_cids: Set[str] = set()
        self.had_host_tools: bool = False
        self.is_ephemeral: bool = False
        self.created_at: float = time.time()
        self.last_used_at: float = time.time()

    def record_web_cursor_locked(self, cur: Tuple[str, str, str]) -> None:
        self.web_cursor = cur
        if cur and isinstance(cur[0], str) and cur[0].startswith("c_") and len(cur[0]) >= 10:
            self.seen_web_cids.add(cur[0])

    def pop_all_web_cids_locked(self) -> List[str]:
        out: List[str] = []
        for c in list(self.seen_web_cids):
            if c.startswith("c_") and len(c) >= 10 and c not in out:
                out.append(c)
        if self.web_cursor and self.web_cursor[0].startswith("c_") and len(self.web_cursor[0]) >= 10:
            if self.web_cursor[0] not in out:
                out.append(self.web_cursor[0])
        for cp_val in self.cursor_checkpoints.values():
            cp_cur = cp_val[0]
            if cp_cur and isinstance(cp_cur[0], str) and cp_cur[0].startswith("c_") and len(cp_cur[0]) >= 10:
                if cp_cur[0] not in out:
                    out.append(cp_cur[0])
        self.seen_web_cids.clear()
        return out

    def reset_cold_locked(self, retire_web_chat: bool = True) -> None:
        retired = self.pop_all_web_cids_locked() if retire_web_chat else []
        self.synced_msg_count = 0
        self.net_context_tokens = 0
        self.web_cursor = ("", "", "")
        self.web_turn_index = 0
        self.cursor_checkpoints.clear()
        self.recent_tool_sigs.clear()
        self.last_sys_sig = ""
        self.last_tools_sig = ""
        if retired:
            schedule_web_conversation_cleanup(retired)

    def reset_for_rollover_locked(self) -> None:
        self.reset_cold_locked(retire_web_chat=True)
        self.tool_pages.clear()
        self.rollover_count += 1

    def snapshot_state_locked(self) -> dict:
        return {
            "synced_msg_count": self.synced_msg_count,
            "first_fp": self.first_fp,
            "net_context_tokens": self.net_context_tokens,
            "rollover_count": self.rollover_count,
            "reuse_count": self.reuse_count,
            "last_sys_sig": self.last_sys_sig,
            "last_tools_sig": self.last_tools_sig,
            "web_cursor": self.web_cursor,
            "web_turn_index": self.web_turn_index,
            "cursor_checkpoints": dict(self.cursor_checkpoints),
            "seen_web_cids": set(self.seen_web_cids),
            "account": self.account,
        }

    def restore_state_snapshot_locked(self, snap: dict) -> None:
        self.synced_msg_count = int(snap["synced_msg_count"])
        self.first_fp = str(snap["first_fp"])
        self.net_context_tokens = int(snap["net_context_tokens"])
        self.rollover_count = int(snap["rollover_count"])
        self.reuse_count = int(snap["reuse_count"])
        self.last_sys_sig = str(snap["last_sys_sig"])
        self.last_tools_sig = str(snap["last_tools_sig"])
        self.web_cursor = snap["web_cursor"]
        self.web_turn_index = int(snap["web_turn_index"])
        self.cursor_checkpoints = dict(snap["cursor_checkpoints"])
        self.seen_web_cids = set(snap.get("seen_web_cids") or set())
        self.account = str(snap["account"])

    def paginate_tool_text(self, text: str, budget_chars: int = MAX_TOOL_RESULT_CHARS) -> str:
        if len(text) <= budget_chars:
            return text
        ptok = "pg_" + random_hex(6)
        pages, preview = build_landmark_preview(text, budget_chars, ptok)
        with self.lock:
            self.tool_pages[ptok] = pages
            while len(self.tool_pages) > MAX_TOOL_PAGES_PER_SESSION:
                oldest_tok = next(iter(self.tool_pages))
                self.tool_pages.pop(oldest_tok, None)
        return preview

    def read_stored_page(self, page_token: str, page_num: Any = 1, grep_pat: str = "") -> dict:
        with self.lock:
            pages = self.tool_pages.get(page_token)
        if not pages:
            return {"content": [{"type": "text", "text": f"Unknown page_token {page_token!r}."}], "isError": True}
        if grep_pat:
            full = "".join(pages)
            matches: List[str] = []
            try:
                rx = re.compile(grep_pat, re.IGNORECASE)
            except re.error:
                rx = re.compile(re.escape(grep_pat), re.IGNORECASE)
            for idx, ln in enumerate(full.splitlines(), start=1):
                if rx.search(ln):
                    matches.append(f"L{idx}: {ln[:240]}")
                    if len(matches) >= 120:
                        break
            body = "\n".join(matches) if matches else f"No lines matched {grep_pat!r} across {len(pages)} pages."
            return {"content": [{"type": "text", "text": f"[DSH_PAGE_GREP token={page_token} matches={len(matches)}]\n{body}"}], "isError": False}
        p_idx = max(1, _safe_int(page_num, 1))
        if p_idx > len(pages):
            return {"content": [{"type": "text", "text": f"Page {p_idx} out of range (1..{len(pages)})."}], "isError": True}
        return {
            "content": [{"type": "text", "text": f"[DSH_AUTO_PAGINATED page={p_idx}/{len(pages)} page_token={page_token}]\n" + pages[p_idx - 1]}],
            "isError": False,
        }

    def register_tools(self, raw_tools: list, cwd: str, sys_text: str, messages: Optional[list] = None, explicit_cwd: bool = False) -> None:
        cat = sanitize_tools_catalog(raw_tools, messages=messages)
        with self.lock:
            if explicit_cwd and cwd:
                self.cwd = cwd
                self.has_explicit_cwd = True
            elif not self.cwd:
                self.cwd = cwd or "/tmp"
            self.system_text = sys_text
            if cat:
                if len(cat) >= 2:
                    self.had_host_tools = True
                self.tools_catalog = cat
                self.allowed_tools = {t["name"]: {"name": t["name"], "description": t["description"], "schema": t["parameters"]} for t in cat}
                self.mcp_to_host = {}
                for t in cat:
                    self.mcp_to_host[t["mcp_name"]] = t["name"]
                    self.mcp_to_host["mcp_dsh-bridge_" + t["mcp_name"]] = t["name"]
                    self.mcp_to_host["dsh_" + t["name"]] = t["name"]
                    self.mcp_to_host[t["name"]] = t["name"]
            else:
                self.tools_catalog = []
                self.allowed_tools = {}
                self.mcp_to_host = {}

    def enqueue_parked_call(self, parked: ParkedCall) -> None:
        with self.lock:
            self.parked_calls[parked.call_id] = parked
            turn = self.active_turn
            if turn is None:
                return
            with turn.lock:
                if turn.resolved:
                    return
                if turn.first_delta_at is None:
                    turn.first_delta_at = time.time()
                turn.tool_calls.append(parked)
                if turn.batch_timer is not None:
                    turn.batch_timer.cancel()
                turn.batch_timer = threading.Timer(BATCH_WINDOW_SEC, self._flush_tool_batch, args=(turn,))
                turn.batch_timer.daemon = True
                turn.batch_timer.start()

    def _flush_tool_batch(self, turn: ActiveTurn) -> None:
        with turn.lock:
            if turn.resolved or not turn.tool_calls:
                return
            turn.resolved = True
            calls = list(turn.tool_calls)
            ttft_ms = int(((turn.first_delta_at or time.time()) - turn.created_at) * 1000)
            txt = "".join(turn.streamed_text_parts)
            thk = "".join(turn.streamed_think_parts)
            chunks = turn.delta_chunks
        with self.lock:
            for c in calls:
                c.delivered = True
        turn.outcome_q.put({
            "stop_reason": "tool_use",
            "tool_calls": [{"id": c.call_id, "name": c.name, "input": c.args} for c in calls],
            "text": txt,
            "thinking": thk,
            "ttft_ms": max(1, ttft_ms),
            "elapsed_ms": int((time.time() - turn.created_at) * 1000),
            "delta_chunks": chunks,
            "stream_mode": "plane2_fifo_tool_bridge",
        })

    def complete_reply(self, content: str, thinking: str = "", stream_mode: str = "plane2_fifo_stream") -> bool:
        with self.lock:
            turn = self.active_turn
            self.in_flight = False
            self.active_turn = None
        if turn is None:
            return False
        with turn.lock:
            if turn.batch_timer is not None:
                turn.batch_timer.cancel()
            if turn.resolved:
                return False
            already_thk = "".join(turn.streamed_think_parts)
            already_txt = "".join(turn.streamed_text_parts)
        if thinking and not already_thk:
            turn.push_delta("thinking", thinking)
        elif thinking and already_thk and thinking.startswith(already_thk) and len(thinking) > len(already_thk):
            turn.push_delta("thinking", thinking[len(already_thk) :])
        if content and not already_txt:
            turn.push_delta("text", content)
        elif content and already_txt:
            if content.startswith(already_txt):
                rem_txt = content[len(already_txt) :]
                if rem_txt:
                    turn.push_delta("text", rem_txt)
            else:
                turn.push_delta("text", content)
        with turn.lock:
            turn.resolved = True
            final_txt = "".join(turn.streamed_text_parts) or content
            final_thk = "".join(turn.streamed_think_parts) or thinking
            ttft_ms = int(((turn.first_delta_at or time.time()) - turn.created_at) * 1000)
            chunks = turn.delta_chunks
        turn.outcome_q.put({
            "stop_reason": "end_turn",
            "tool_calls": [],
            "text": final_txt,
            "thinking": final_thk,
            "ttft_ms": max(1, ttft_ms),
            "elapsed_ms": int((time.time() - turn.created_at) * 1000),
            "delta_chunks": chunks,
            "stream_mode": stream_mode if chunks <= 1 else "plane2_fifo_chunk_stream",
        })
        return True


class TaskWorkerPool:
    """Plane 2 Hot-Plug Gemini.app Foreground Web Worker Pool backed by non-blocking POSIX FIFOs."""

    def __init__(self):
        self.lock = threading.Lock()
        self.idle_workers: Dict[str, dict] = {}
        self.recent_seen: Dict[str, float] = {}
        self.total_registered = 0

    def idle_count(self) -> int:
        with self.lock:
            return len(self.idle_workers)

    def recent_worker_count(self, window_sec: float = 60.0) -> int:
        now = time.time()
        with self.lock:
            self.recent_seen = {k: ts for k, ts in self.recent_seen.items() if now - ts <= window_sec}
            return max(len(self.idle_workers), len(self.recent_seen))

    def idle_worker_models(self) -> List[str]:
        with self.lock:
            return [w.get("model", "any") for w in self.idle_workers.values()]

    def drain_for_restart(self) -> None:
        with self.lock:
            workers = list(self.idle_workers.values())
            self.idle_workers.clear()
        for w in workers:
            with w["lock"]:
                if w["retired"]:
                    continue
                w["retired"] = True
                try:
                    w["q"].put_nowait({"heartbeat": True})
                except queue.Full:
                    pass
        for sess in list(REGISTRY.by_token.values()):
            with sess.lock:
                if sess.worker_waiting_next:
                    try:
                        sess.next_delta_q.put_nowait({"heartbeat": True})
                    except queue.Full:
                        pass

    def _claim_queued_item(self, item: dict) -> bool:
        ilock = item.get("_lock")
        if ilock is not None:
            with ilock:
                if item.get("cancelled") or item.get("claimed_done"):
                    return False
                item["claimed_done"] = True
                ev = item.get("claimed")
                if ev is not None:
                    ev.set()
                return True
        if item.get("cancelled") or item.get("claimed_done"):
            return False
        item["claimed_done"] = True
        ev = item.get("claimed")
        if ev is not None:
            ev.set()
        return True

    def wait_for_prompt(self, worker_id: str, worker_model: str) -> dict:
        wid = worker_id or ("web_" + random_hex(4))
        q: "queue.Queue[dict]" = queue.Queue(maxsize=1)
        w_entry = {
            "id": wid,
            "model": (worker_model or "any").lower(),
            "q": q,
            "since": time.time(),
            "lock": threading.Lock(),
            "retired": False,
        }
        with self.lock:
            self.total_registered += 1
            self.recent_seen[wid] = time.time()
            while True:
                try:
                    queued_item = REGISTRY.prompt_dispatch_q.get_nowait()
                except queue.Empty:
                    break
                if self._claim_queued_item(queued_item):
                    return {"content": [{"type": "text", "text": queued_item["prompt"]}], "isError": False}
            self.idle_workers[wid] = w_entry
        try:
            item = q.get(timeout=15.0)
            with self.lock:
                self.recent_seen[wid] = time.time()
            if item.get("heartbeat"):
                return {"content": [{"type": "text", "text": f"[DSH_IDLE_HEARTBEAT worker_id={wid}] Restart/heartbeat drain."}], "isError": False}
            return {"content": [{"type": "text", "text": item["prompt"]}], "isError": False}
        except queue.Empty:
            with w_entry["lock"]:
                if not w_entry["retired"]:
                    w_entry["retired"] = True
                    item = None
                else:
                    try:
                        item = q.get_nowait()
                    except queue.Empty:
                        item = None
            with self.lock:
                self.recent_seen[wid] = time.time()
            if item is not None and not item.get("heartbeat"):
                return {"content": [{"type": "text", "text": item["prompt"]}], "isError": False}
            return {"content": [{"type": "text", "text": f"[DSH_IDLE_HEARTBEAT worker_id={wid}] No incoming request yet."}], "isError": False}
        finally:
            with w_entry["lock"]:
                w_entry["retired"] = True
            with self.lock:
                cur = self.idle_workers.get(wid)
                if cur is w_entry:
                    self.idle_workers.pop(wid, None)

    def try_dispatch(self, item: dict) -> bool:
        target_model = (item.get("model") or "").lower()
        while True:
            with self.lock:
                chosen_id = next(
                    (wid for wid, w in self.idle_workers.items() if w["model"] == target_model or target_model in w["model"]),
                    None,
                )
                if chosen_id is None and self.idle_workers:
                    chosen_id = next(iter(self.idle_workers.keys()))
                if chosen_id is None:
                    return False
                w = self.idle_workers.pop(chosen_id)
                self.recent_seen[chosen_id] = time.time()

            with w["lock"]:
                if w["retired"]:
                    continue
                ilock = item.get("_lock")
                if ilock is not None:
                    with ilock:
                        if item.get("cancelled") or item.get("claimed_done"):
                            # Put worker back? No, worker didn't receive anything; let it re-enter via loop/caller
                            # Wait: avoid dropping worker by re-inserting if still active!
                            reinsert = True
                        else:
                            reinsert = False
                            try:
                                w["q"].put_nowait(item)
                            except queue.Full:
                                w["retired"] = True
                                continue
                            w["retired"] = True
                            item["claimed_done"] = True
                            ev = item.get("claimed")
                            if ev is not None:
                                ev.set()
                            return True
                    if reinsert:
                        with self.lock:
                            if chosen_id not in self.idle_workers:
                                self.idle_workers[chosen_id] = w
                        return False
                else:
                    if item.get("cancelled") or item.get("claimed_done"):
                        with self.lock:
                            if chosen_id not in self.idle_workers:
                                self.idle_workers[chosen_id] = w
                        return False
                    try:
                        w["q"].put_nowait(item)
                    except queue.Full:
                        w["retired"] = True
                        continue
                    w["retired"] = True
                    item["claimed_done"] = True
                    ev = item.get("claimed")
                    if ev is not None:
                        ev.set()
                    return True


class SessionCursorStore:
    """Atomically persists Plane-1 `web_cursor` & `cursor_checkpoints` across gateway restarts with TTL eviction."""

    def __init__(self, path: str, ttl_sec: float = SESSION_CACHE_TTL_SEC, max_entries: int = 96):
        self.path = path
        self.ttl_sec = ttl_sec
        self.max_entries = max_entries
        self.lock = threading.Lock()
        self._entries: Dict[str, dict] = {}
        self._loaded = False

    def _ensure_loaded_locked(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.path or not os.path.isfile(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fp:
                raw = json.load(fp)
            if isinstance(raw, dict) and isinstance(raw.get("sessions"), dict):
                now = time.time()
                for k, v in raw["sessions"].items():
                    if isinstance(v, dict) and (now - float(v.get("updated_at", 0.0))) <= self.ttl_sec:
                        self._entries[k] = v
        except Exception:
            self._entries = {}

    def _flush_locked(self) -> None:
        if not self.path:
            return
        now = time.time()
        valid = {
            k: v for k, v in self._entries.items()
            if isinstance(v, dict) and (now - float(v.get("updated_at", 0.0))) <= self.ttl_sec
        }
        if len(valid) > self.max_entries:
            ordered = sorted(valid.items(), key=lambda kv: float(kv[1].get("updated_at", 0.0)), reverse=True)
            valid = dict(ordered[: self.max_entries])
        self._entries = valid
        try:
            parent = os.path.dirname(os.path.abspath(self.path))
            os.makedirs(parent, exist_ok=True)
            tmp_path = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp_path, "w", encoding="utf-8") as fp:
                json.dump({"version": 1, "sessions": valid}, fp, ensure_ascii=False)
            os.replace(tmp_path, self.path)
        except Exception:
            pass

    def restore_into(self, sess: SessionState, current_account: str = "") -> bool:
        with self.lock:
            self._ensure_loaded_locked()
            raw_rec = self._entries.get(sess.conv_key)
            if not raw_rec:
                return False
            if (time.time() - float(raw_rec.get("updated_at", 0.0))) > self.ttl_sec:
                self._entries.pop(sess.conv_key, None)
                self._flush_locked()
                return False
            rec_acct = str(raw_rec.get("account", "")).strip()
            if current_account and rec_acct and rec_acct.lower() != current_account.strip().lower():
                self._entries.pop(sess.conv_key, None)
                self._flush_locked()
                return False
            rec = json.loads(json.dumps(raw_rec, ensure_ascii=False))

        cur_raw = rec.get("web_cursor") or ["", "", ""]
        if not (isinstance(cur_raw, list) and len(cur_raw) == 3 and any(cur_raw)):
            return False
        cps_raw = rec.get("cursor_checkpoints") or {}
        restored_cps: Dict[int, tuple] = {}
        if isinstance(cps_raw, dict):
            for k_str, val in cps_raw.items():
                try:
                    k_int = int(k_str)
                    c_list = val[0]
                    if isinstance(c_list, list) and len(c_list) == 3:
                        p_hash = str(val[3]) if len(val) > 3 else ""
                        committed = bool(val[4]) if len(val) > 4 else True
                        restored_cps[k_int] = (
                            (str(c_list[0]), str(c_list[1]), str(c_list[2])),
                            int(val[1]),
                            int(val[2]),
                            p_hash,
                            committed,
                        )
                except Exception:
                    continue
        with sess.lock:
            sess.record_web_cursor_locked((str(cur_raw[0]), str(cur_raw[1]), str(cur_raw[2])))
            for s_cid in rec.get("seen_web_cids") or []:
                if isinstance(s_cid, str) and s_cid.startswith("c_") and len(s_cid) >= 10:
                    sess.seen_web_cids.add(s_cid)
            sess.web_turn_index = int(rec.get("web_turn_index", 0))
            sess.synced_msg_count = int(rec.get("synced_msg_count", 0))
            sess.net_context_tokens = int(rec.get("net_context_tokens", 0))
            sess.first_fp = str(rec.get("first_fp", ""))
            sess.model_id = str(rec.get("model_id", sess.model_id))
            sess.account = str(rec.get("account", ""))
            sess.last_sys_sig = str(rec.get("last_sys_sig", ""))
            sess.last_tools_sig = str(rec.get("last_tools_sig", ""))
            sess.cursor_checkpoints = restored_cps
        return True

    def save_from(self, sess: SessionState) -> None:
        with sess.lock:
            if sess.is_ephemeral:
                return
            conv_key = sess.conv_key
            cur = list(sess.web_cursor)
            if not any(cur):
                evict_only = True
                payload = None
            else:
                evict_only = False
                cps_ser = {
                    str(k): [
                        list(v[0]),
                        int(v[1]),
                        int(v[2]),
                        str(v[3]) if len(v) > 3 else "",
                        bool(v[4]) if len(v) > 4 else True,
                    ]
                    for k, v in sess.cursor_checkpoints.items()
                }
                payload = {
                    "web_cursor": cur,
                    "seen_web_cids": sorted(sess.seen_web_cids),
                    "web_turn_index": int(sess.web_turn_index),
                    "synced_msg_count": int(sess.synced_msg_count),
                    "net_context_tokens": int(sess.net_context_tokens),
                    "first_fp": str(sess.first_fp),
                    "model_id": str(sess.model_id),
                    "account": str(sess.account),
                    "last_sys_sig": str(sess.last_sys_sig),
                    "last_tools_sig": str(sess.last_tools_sig),
                    "cursor_checkpoints": cps_ser,
                    "updated_at": time.time(),
                }
        with self.lock:
            self._ensure_loaded_locked()
            if evict_only:
                if conv_key in self._entries:
                    self._entries.pop(conv_key, None)
                    self._flush_locked()
                return
            self._entries[conv_key] = payload
            self._flush_locked()

    def evict(self, conv_key: str) -> None:
        with self.lock:
            self._ensure_loaded_locked()
            if conv_key in self._entries:
                self._entries.pop(conv_key, None)
                self._flush_locked()

    @staticmethod
    def _extract_cids_from_record(rec: dict) -> List[str]:
        out: List[str] = []
        if not isinstance(rec, dict):
            return out
        cur = rec.get("web_cursor")
        if isinstance(cur, list) and cur and isinstance(cur[0], str) and cur[0].startswith("c_") and len(cur[0]) >= 10:
            out.append(cur[0])
        for sc in rec.get("seen_web_cids") or []:
            if isinstance(sc, str) and sc.startswith("c_") and len(sc) >= 10 and sc not in out:
                out.append(sc)
        cps = rec.get("cursor_checkpoints")
        if isinstance(cps, dict):
            for val in cps.values():
                if isinstance(val, list) and val and isinstance(val[0], list) and val[0]:
                    c0 = val[0][0]
                    if isinstance(c0, str) and c0.startswith("c_") and len(c0) >= 10 and c0 not in out:
                        out.append(c0)
        return out

    def reap_idle_entries(self, max_idle_sec: float, exclude_keys: Optional[Set[str]] = None) -> List[str]:
        if max_idle_sec <= 0:
            return []
        now = time.time()
        excl = exclude_keys or set()
        reaped_cids: List[str] = []
        changed = False
        with self.lock:
            self._ensure_loaded_locked()
            for k, rec in list(self._entries.items()):
                if k in excl:
                    continue
                if not isinstance(rec, dict) or (now - float(rec.get("updated_at", 0.0))) > max_idle_sec:
                    for c in self._extract_cids_from_record(rec):
                        if c not in reaped_cids:
                            reaped_cids.append(c)
                    self._entries.pop(k, None)
                    changed = True
            if changed:
                self._flush_locked()
        return reaped_cids

    def drain_all_cids_and_clear(self) -> List[str]:
        cids: List[str] = []
        with self.lock:
            self._ensure_loaded_locked()
            for rec in self._entries.values():
                for c in self._extract_cids_from_record(rec):
                    if c not in cids:
                        cids.append(c)
            self._entries.clear()
            self._flush_locked()
        return cids


class GatewayRegistry:
    def __init__(self, cursor_store: Optional[SessionCursorStore] = None, ttl_sec: float = SESSION_CACHE_TTL_SEC):
        self.lock = threading.Lock()
        self.by_conv: Dict[str, SessionState] = {}
        self.by_token: Dict[str, SessionState] = {}
        self.prompt_dispatch_q: "queue.Queue[dict]" = queue.Queue()
        self.telemetry: List[dict] = []
        self.cursor_store = cursor_store
        self.ttl_sec = ttl_sec
        self._last_gc_at: float = 0.0

    def has_active_tool_session(self, conv_key: str) -> bool:
        with self.lock:
            sess = self.by_conv.get(conv_key)
        if sess is None:
            return False
        with sess.lock:
            return bool(sess.had_host_tools and sess.synced_msg_count >= 2)

    def _evict_expired_locked(self, now: Optional[float] = None, idle_limit_sec: Optional[float] = None) -> List[str]:
        ts = now if now is not None else time.time()
        limit_sec = idle_limit_sec if idle_limit_sec is not None else min(self.ttl_sec, max(60.0, SESSION_IDLE_CLEAN_SEC))
        if idle_limit_sec is None and (ts - self._last_gc_at < 30.0) and len(self.by_conv) < 256:
            return []
        self._last_gc_at = ts
        reaped_cids: List[str] = []
        for conv_key, sess in list(self.by_conv.items()):
            if sess.turn_lock.locked():
                continue
            with sess.lock:
                expired = (not sess.in_flight) and (not sess.parked_calls) and ((ts - sess.last_used_at) > limit_sec)
                tok = sess.session_token
                cids = sess.pop_all_web_cids_locked() if expired else []
            if expired:
                self.by_conv.pop(conv_key, None)
                self.by_token.pop(tok, None)
                if self.cursor_store is not None:
                    self.cursor_store.evict(conv_key)
                for c in cids:
                    if c not in reaped_cids:
                        reaped_cids.append(c)
        return reaped_cids

    def reap_idle_sessions(self, max_idle_sec: float = SESSION_IDLE_CLEAN_SEC) -> List[str]:
        if max_idle_sec <= 0:
            return []
        with self.lock:
            reaped = self._evict_expired_locked(now=time.time(), idle_limit_sec=max_idle_sec)
            active_keys = set(self.by_conv.keys())
        if self.cursor_store is not None:
            disk_cids = self.cursor_store.reap_idle_entries(max_idle_sec=max_idle_sec, exclude_keys=active_keys)
            for c in disk_cids:
                if c not in reaped:
                    reaped.append(c)
        if reaped:
            schedule_web_conversation_cleanup(reaped)
        return reaped

    def clean_all_web_chats(self) -> dict:
        all_cids: List[str] = []
        with self.lock:
            sessions_list = list(self.by_conv.values())
            self.by_conv.clear()
            self.by_token.clear()
        for sess in sessions_list:
            with sess.lock:
                for c in sess.pop_all_web_cids_locked():
                    if c not in all_cids:
                        all_cids.append(c)
                sess.reset_cold_locked(retire_web_chat=False)
        if self.cursor_store is not None:
            for c in self.cursor_store.drain_all_cids_and_clear():
                if c not in all_cids:
                    all_cids.append(c)
        if not all_cids:
            return {"ok": True, "requested": 0, "deleted": [], "deleted_count": 0}
        res = get_webkit_plane().delete_conversations(all_cids, timeout_sec=20.0)
        deleted_list = res.get("deleted") if isinstance(res, dict) and isinstance(res.get("deleted"), list) else []
        return {
            "ok": bool(res.get("ok")) if isinstance(res, dict) else False,
            "requested": len(all_cids),
            "deleted": deleted_list,
            "deleted_count": len(deleted_list),
            "errors": res.get("errors", []) if isinstance(res, dict) else [],
        }

    def get_or_create(self, conv_key: str) -> SessionState:
        with self.lock:
            now = time.time()
            reaped = self._evict_expired_locked(now=now)
            if conv_key in self.by_conv:
                sess = self.by_conv[conv_key]
                with sess.lock:
                    sess.last_used_at = now
                if reaped:
                    schedule_web_conversation_cleanup(reaped)
                return sess
            tok = "tok_" + random_hex(10)
            sess = SessionState(conv_key, tok)
            if self.cursor_store is not None:
                self.cursor_store.restore_into(sess)
            self.by_conv[conv_key] = sess
            self.by_token[tok] = sess
        if reaped:
            schedule_web_conversation_cleanup(reaped)
        return sess

    def find_by_tool_result_ids(self, tool_call_ids: List[str]) -> Optional[SessionState]:
        with self.lock:
            for sess in self.by_token.values():
                with sess.lock:
                    for cid in tool_call_ids:
                        if cid in sess.parked_calls:
                            return sess
        return None

    def get_by_token(self, token: str) -> Optional[SessionState]:
        with self.lock:
            return self.by_token.get(token)

    def record_turn(self, entry: dict) -> None:
        with self.lock:
            entry["ts"] = time.strftime("%H:%M:%S")
            entry["think_chars"] = len(str(entry.get("thinking") or ""))
            self.telemetry.append(entry)
            if len(self.telemetry) > 200:
                self.telemetry = self.telemetry[-200:]


class AccountAlignmentStore:
    """Persists optional pinned Google email so Plane 1 never runs on an account mismatched with Gemini.app."""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()

    def _load_locked(self) -> Dict[str, str]:
        try:
            if os.path.isfile(self.path):
                with open(self.path, "r", encoding="utf-8") as fp:
                    d = json.load(fp)
                return {
                    "pinned_account": str(d.get("pinned_account", "")).strip(),
                    "last_seen_account": str(d.get("last_seen_account", "")).strip(),
                }
        except Exception:
            pass
        return {"pinned_account": "", "last_seen_account": ""}

    def _save_locked(self, obj: Dict[str, str]) -> None:
        parent = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(parent, exist_ok=True)
        tmp_path = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp_path, "w", encoding="utf-8") as fp:
            json.dump(obj, fp, indent=2, ensure_ascii=False)
        os.replace(tmp_path, self.path)

    def load(self) -> Dict[str, str]:
        with self.lock:
            return self._load_locked()

    def set_pinned(self, email: str) -> Dict[str, str]:
        with self.lock:
            cur = self._load_locked()
            cur["pinned_account"] = str(email or "").strip()
            self._save_locked(cur)
            return dict(cur)

    def note_seen(self, email: str) -> None:
        clean = str(email or "").strip()
        if not clean:
            return
        with self.lock:
            cur = self._load_locked()
            if cur.get("last_seen_account") != clean:
                cur["last_seen_account"] = clean
                self._save_locked(cur)

    def check_alignment(self, active_email: str) -> Tuple[bool, str, str]:
        active = str(active_email or "").strip()
        with self.lock:
            st = self._load_locked()
            if active and st.get("last_seen_account") != active:
                st["last_seen_account"] = active
                self._save_locked(st)
            pinned = st.get("pinned_account", "").strip()
        if not pinned:
            return True, pinned, active
        if not active:
            return False, pinned, ""
        return (pinned.lower() == active.lower()), pinned, active

    def _save(self, obj: Dict[str, str]) -> None:
        with self.lock:
            self._save_locked(obj)


WORKER_POOL = TaskWorkerPool()
CURSOR_STORE = SessionCursorStore(SESSIONS_CACHE_FILE, ttl_sec=SESSION_CACHE_TTL_SEC)
REGISTRY = GatewayRegistry(cursor_store=CURSOR_STORE, ttl_sec=SESSION_CACHE_TTL_SEC)
ACCOUNT_STORE = AccountAlignmentStore(ACCOUNT_LOCK_FILE)
EMBEDDED_MCP_HOST = EmbeddedLocalMcpHost(base_config_paths=[LOCAL_MCP_CONFIG])
WEBKIT_PLANE: Optional[WebKitPlaneClient] = None
_WEBKIT_PLANE_LOCK = threading.Lock()


def get_webkit_plane() -> WebKitPlaneClient:
    global WEBKIT_PLANE
    with _WEBKIT_PLANE_LOCK:
        if WEBKIT_PLANE is None or WEBKIT_PLANE.socket_path != WEBKIT_SOCK:
            auto_spawn = os.environ.get("DSH_DISABLE_WEBKIT_AUTOSPAWN") != "1"
            WEBKIT_PLANE = WebKitPlaneClient(socket_path=WEBKIT_SOCK, binary_path=WEBKIT_BIN, auto_spawn=auto_spawn)
        return WEBKIT_PLANE


def schedule_web_conversation_cleanup(cids: List[str]) -> None:
    if os.environ.get("DSH_DISABLE_AUTO_CHAT_CLEANUP") == "1":
        return
    clean_cids = [str(c).strip() for c in (cids or []) if isinstance(c, str) and c.strip().startswith("c_") and len(c.strip()) >= 10]
    if not clean_cids:
        return
    try:
        plane = get_webkit_plane()
        if hasattr(plane, "delete_conversations_async"):
            plane.delete_conversations_async(clean_cids)
    except Exception:
        pass


def normalize_host_tool_args(host_tool: str, args: dict, schema: dict, cwd: str = "") -> dict:
    out = dict(args or {})
    props = (schema or {}).get("properties") or {}
    if "file_path" in props and "file_path" not in out:
        for alt in ("path", "AbsolutePath", "TargetFile", "filepath"):
            if alt in out and out[alt]:
                out["file_path"] = out.pop(alt)
                break
    if "command" in props and "command" not in out:
        for alt in ("cmd", "CommandLine"):
            if alt in out and out[alt]:
                out["command"] = out.pop(alt)
                break
    if "file_path" in out and isinstance(out["file_path"], str):
        fp_s = out["file_path"].strip()
        if fp_s.startswith("~"):
            out["file_path"] = os.path.expanduser(fp_s)
        elif fp_s and cwd and not os.path.isabs(fp_s):
            out["file_path"] = os.path.normpath(os.path.join(cwd, fp_s))
    return out


def _resolve_effective_upstream(headers: dict) -> str:
    lower_hdrs = {k.lower(): str(v).strip() for k, v in (headers or {}).items()}
    override = lower_hdrs.get("x-upstream-mode", "").lower()
    if override:
        if override not in ("dual_plane_web", "mcp_task", "web"):
            raise ValueError(f"Unsupported X-Upstream-Mode: {override!r}")
        return override
    return UPSTREAM_MODE


def _should_enable_layer_b(headers: dict, raw_tools: list, cwd: str, explicit_cwd: bool, tool_choice_mode: str) -> bool:
    if tool_choice_mode == "none":
        return False
    lower_hdrs = {k.lower(): str(v) for k, v in (headers or {}).items()}
    if lower_hdrs.get("x-enable-local-mcp", "").strip() == "1":
        return True
    if explicit_cwd and cwd and os.path.isfile(os.path.join(cwd, ".mcp.json")):
        return True
    if lower_hdrs.get("x-claude-code-agent-id", "").strip() or ("claude-cli" in lower_hdrs.get("user-agent", "").lower()):
        return True
    if lower_hdrs.get("x-claude-code-session-id", "").strip() and bool(raw_tools):
        return True
    return False


def format_plane1_delta_prompt(
    sess: SessionState,
    system_text: str,
    messages_slice: list,
    is_delta: bool,
    tombstone_rollover: bool,
    nonce: str,
    layer_b_tools: List[dict],
    stitched_layer_b: Optional[List[dict]] = None,
    tool_choice: Optional[Any] = None,
    strip_reminders: bool = False,
    explicit_cwd_override: Optional[bool] = None,
    cur_sys_sig: Optional[str] = None,
    cur_tools_sig: Optional[str] = None,
    prev_sys_sig: str = "",
    prev_tools_sig: str = "",
) -> str:
    """Formats the pure-Web prompt payload for Plane 1 (`dsh-webkit-bridge` `StreamGenerate`)."""
    with sess.lock:
        layer_a_cat = [
            t for t in sess.tools_catalog
            if t["name"] not in (AWAIT_TOOL_NAME, CALL_TOOL_NAME)
        ]
        cwd = sess.cwd
        explicit_cwd = sess.has_explicit_cwd if explicit_cwd_override is None else explicit_cwd_override

    clean_sys = prune_client_system_prompt(system_text)[:120000]
    if cur_sys_sig is None:
        cur_sys_sig = hashlib.sha256(clean_sys.encode("utf-8")).hexdigest() if clean_sys else ""

    has_active_tools = bool(layer_a_cat or layer_b_tools)
    if cur_tools_sig is None:
        if has_active_tools:
            cur_tools_sig = hashlib.sha256(
                json.dumps(
                    {
                        "a": layer_a_cat,
                        "b": [{"name": t.get("name"), "parameters": t.get("parameters")} for t in layer_b_tools],
                        "tc": tool_choice,
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                ).encode("utf-8")
            ).hexdigest()
        else:
            cur_tools_sig = ""

    parts: List[str] = []
    tp_extra = _filter_supported_kwargs(build_dsh_tp_system_instructions, tool_choice=tool_choice)
    tool_guide = (
        build_dsh_tp_system_instructions(
            nonce,
            layer_a_cat,
            embedded_mcp_tools=layer_b_tools,
            cwd=(cwd if explicit_cwd else ""),
            **tp_extra,
        )
        if has_active_tools
        else ""
    )

    if not is_delta:
        if explicit_cwd:
            parts.append(f"[Working Directory: {cwd}]")
        if tool_guide:
            parts.append(tool_guide)
        if clean_sys:
            parts.append(f"[System Instructions]\n{clean_sys}")
    else:
        tools_changed = (cur_tools_sig != prev_tools_sig)
        sys_changed = (cur_sys_sig != prev_sys_sig)

        if tools_changed:
            if tool_guide:
                parts.append(tool_guide)
            elif prev_tools_sig:
                parts.append("[Tools Update]: All tools are inactive for this turn; respond directly in plain text.")
        elif tool_guide:
            write_dir_hint = cwd if explicit_cwd else "/abs/path"
            parts.append(
                f"[DSH-TP v1.1 TURN_NONCE={nonce}]: Emit `<dsh_call_{nonce}>...</dsh_call_{nonce}>` outside code fences "
                f"(MANDATORY `<dsh_call_{nonce} tool=\"Write\" mode=\"raw\"><dsh_arg_{nonce} name=\"file_path\">{write_dir_hint}/...</dsh_arg_{nonce}><dsh_arg_{nonce} name=\"content\">...</dsh_arg_{nonce}></dsh_call_{nonce}>` "
                f"for multi-line `Write`/`Edit`; NEVER open Canvas and NEVER paste code asking the user to save it manually)."
            )

        if sys_changed:
            if clean_sys:
                parts.append(f"[System Instructions]\n{clean_sys}")
            elif prev_sys_sig:
                parts.append("[System Instructions Cleared]")

    if stitched_layer_b:
        for item in stitched_layer_b:
            paged_b = sess.paginate_tool_text(item["text"], budget_chars=WEB_RPC_PAGE_CHARS)
            parts.append(f"[Local MCP Tool Result ({item['server']}/{item['tool']})]:\n{paged_b}")

    parts.append("[Conversation Update]" if is_delta else "[Conversation History]")
    total_msgs = len(messages_slice)
    for idx, m in enumerate(messages_slice):
        role_lbl = str(m.get("role", "user")).upper() if isinstance(m, dict) else "USER"
        dehydrate_this = tombstone_rollover and (idx < max(0, total_msgs - 4))
        paginate_cb = None if dehydrate_this else (lambda txt: sess.paginate_tool_text(txt, budget_chars=WEB_RPC_PAGE_CHARS))
        formatted_m = format_single_message_clean(
            m,
            max_result_chars=WEB_RPC_PAGE_CHARS,
            tombstone_dehydrate=dehydrate_this,
            strip_reminders=strip_reminders,
            paginate_fn=paginate_cb,
        )
        parts.append(f"{role_lbl}: {formatted_m}")

    return "\n\n".join(parts)


def execute_plane1_turn_with_local_mcp_loop(
    sess: SessionState,
    plane: WebKitPlaneClient,
    system_text: str,
    unseen_msgs: list,
    is_delta: bool,
    tombstone_rollover: bool,
    model_id: str,
    tier: str,
    priority: int,
    stitched_layer_b: List[dict],
    on_delta: Optional[Callable[[dict], None]] = None,
    on_sse_comment: Optional[Callable[[str], None]] = None,
    timeout_sec: float = 240.0,
    cancel_event: Optional[threading.Event] = None,
    allow_layer_b: bool = False,
    tool_choice: Optional[Any] = None,
    tool_choice_mode: str = "auto",
    forced_tool_name: str = "",
    strip_reminders: bool = False,
    explicit_cwd: bool = False,
    is_streaming: bool = False,
) -> dict:
    """
    Executes Plane 1 Web inference with internal Dual-Layer MCP short-circuiting AND
    automatic Gateway-Internal Self-Heal Retry Hop when a malformed envelope is detected.
    """
    with sess.lock:
        cwd = sess.cwd
        sess_explicit_cwd = explicit_cwd or sess.has_explicit_cwd
        layer_a_names: Set[str] = {
            k for k in sess.allowed_tools.keys()
            if k not in (AWAIT_TOOL_NAME, CALL_TOOL_NAME, PAGE_TOOL_NAME)
        }
        layer_a_cat = [
            t for t in sess.tools_catalog
            if t["name"] not in (AWAIT_TOOL_NAME, CALL_TOOL_NAME)
        ]
        allowed_schemas = dict(sess.allowed_tools)
        orig_cursor = sess.web_cursor
        orig_turn_idx = sess.web_turn_index
        orig_sys_sig = sess.last_sys_sig
        orig_tools_sig = sess.last_tools_sig

    norm_cwd = cwd if sess_explicit_cwd else ""
    layer_b_tools = EMBEDDED_MCP_HOST.get_active_layer_b_tools(layer_a_names, cwd=cwd) if allow_layer_b else []

    clean_sys_for_sig = prune_client_system_prompt(system_text)[:120000]
    target_sys_sig = hashlib.sha256(clean_sys_for_sig.encode("utf-8")).hexdigest() if clean_sys_for_sig else ""
    if layer_a_cat or layer_b_tools:
        target_tools_sig = hashlib.sha256(
            json.dumps(
                {
                    "a": layer_a_cat,
                    "b": [{"name": t.get("name"), "parameters": t.get("parameters")} for t in layer_b_tools],
                    "tc": tool_choice,
                },
                sort_keys=True,
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
    else:
        target_tools_sig = ""

    accumulated_text: List[str] = []
    accumulated_thinking: List[str] = []
    total_chunks = 0
    first_ttft: Optional[int] = None
    last_routed_model = ""
    self_healed_hops = 0
    tool_choice_retried = 0
    first_prompt_tokens = 0
    sent_prompt_tokens = 0
    last_cursor_committed = True
    t_start = time.time()

    cur_cursor = orig_cursor
    cur_turn_idx = orig_turn_idx
    cur_sys_sig = orig_sys_sig
    cur_tools_sig = orig_tools_sig

    next_slice = unseen_msgs
    cur_is_delta = is_delta
    cur_tombstone = tombstone_rollover
    cur_stitched = stitched_layer_b

    def _rollback_cursor() -> None:
        orphan_c = ""
        with sess.lock:
            if cur_cursor and cur_cursor[0] and cur_cursor[0] != orig_cursor[0]:
                orphan_c = cur_cursor[0]
                sess.seen_web_cids.discard(orphan_c)
            sess.web_cursor = orig_cursor
            sess.web_turn_index = orig_turn_idx
            sess.last_sys_sig = orig_sys_sig
            sess.last_tools_sig = orig_tools_sig
        if orphan_c:
            schedule_web_conversation_cleanup([orphan_c])

    try:
        for hop in range(MAX_INTERNAL_MCP_HOPS):
            if cancel_event is not None and cancel_event.is_set():
                raise RequestCancelledError("Client disconnected")

            sample_for_nonce = json.dumps(next_slice, ensure_ascii=False)[:32000]
            nonce = generate_turn_nonce(outbound_text=sample_for_nonce)
            prompt_str = format_plane1_delta_prompt(
                sess=sess,
                system_text=system_text,
                messages_slice=next_slice,
                is_delta=cur_is_delta,
                tombstone_rollover=cur_tombstone,
                nonce=nonce,
                layer_b_tools=layer_b_tools,
                stitched_layer_b=cur_stitched,
                tool_choice=tool_choice,
                strip_reminders=strip_reminders,
                explicit_cwd_override=sess_explicit_cwd,
                cur_sys_sig=target_sys_sig,
                cur_tools_sig=target_tools_sig,
                prev_sys_sig=cur_sys_sig,
                prev_tools_sig=cur_tools_sig,
            )
            hop_prompt_tokens = est_tokens_str(prompt_str)
            sent_prompt_tokens += hop_prompt_tokens
            if hop == 0:
                first_prompt_tokens = hop_prompt_tokens

            stream_extra = _filter_supported_kwargs(plane.stream_turn, cancel_event=cancel_event)
            step_res = plane.stream_turn(
                delta_prompt=prompt_str,
                nonce=nonce,
                cursor=cur_cursor,
                turn_index=cur_turn_idx,
                model_id=model_id,
                tier=tier,
                priority=priority,
                on_delta=on_delta,
                timeout_sec=timeout_sec,
                stream_raw_tool_names=layer_a_names,
                cwd=norm_cwd,
                **stream_extra,
            )

            if cancel_event is not None and cancel_event.is_set():
                raise RequestCancelledError("Client disconnected")

            cur_cursor = step_res.get("cursor") or cur_cursor
            with sess.lock:
                sess.record_web_cursor_locked(cur_cursor)
            last_cursor_committed = bool(step_res.get("cursor_committed", True))
            cur_turn_idx += 1
            cur_sys_sig = target_sys_sig
            cur_tools_sig = target_tools_sig

            if step_res.get("routed_model"):
                last_routed_model = str(step_res["routed_model"])
            if first_ttft is None:
                first_ttft = step_res.get("ttft_ms", 1)
            total_chunks += int(step_res.get("delta_chunks", 0))

            raw_calls = step_res.get("tool_calls") or []
            malformed_envs = step_res.get("malformed_envelopes") or []
            streamed_any_tool = bool(step_res.get("streamed_any_tool"))
            has_truncated_raw = any(
                isinstance(me, dict) and me.get("reason") == "truncated_raw_stream"
                for me in malformed_envs
            )

            # Enforce tool_choice ("any" / "tool") if model replied with plain text only
            need_forced_tool_retry = False
            if tool_choice_mode in ("any", "tool") and (layer_a_names or layer_b_tools) and tool_choice_retried < 1:
                if not raw_calls and not streamed_any_tool and not has_truncated_raw:
                    need_forced_tool_retry = True
                elif (
                    tool_choice_mode == "tool"
                    and forced_tool_name
                    and raw_calls
                    and not any(str(tc.get("name", "")).lower() == forced_tool_name.lower() for tc in raw_calls)
                    and not streamed_any_tool
                ):
                    need_forced_tool_retry = True

            if need_forced_tool_retry:
                tool_choice_retried += 1
                if step_res.get("thinking"):
                    accumulated_thinking.append(step_res["thinking"])
                if on_delta is not None and step_res.get("text"):
                    accumulated_text.append(step_res["text"])
                if on_sse_comment is not None:
                    on_sse_comment("dsh-tool-choice-retry")
                cur_is_delta = bool(any(cur_cursor))
                cur_tombstone = False
                cur_stitched = []
                if tool_choice_mode == "tool" and forced_tool_name:
                    enforce_msg = (
                        f"[Gateway Tool Choice Enforcement]: You MUST invoke the tool `{forced_tool_name}` using "
                        f"`<dsh_call_{nonce}>...</dsh_call_{nonce}>` now. Do not answer with plain text alone."
                    )
                else:
                    enforce_msg = (
                        f"[Gateway Tool Choice Enforcement]: tool_choice requires calling at least one active tool "
                        f"using `<dsh_call_{nonce}>...</dsh_call_{nonce}>` now. Do not answer with plain text alone."
                    )
                next_slice = [{"role": "user", "content": enforce_msg}]
                continue

            if step_res.get("text"):
                accumulated_text.append(step_res["text"])
            if step_res.get("thinking"):
                accumulated_thinking.append(step_res["thinking"])

            # Gateway-Internal Self-Heal Retry Hop when an envelope failed or RAW_STREAM truncated before streaming
            if malformed_envs and not raw_calls and self_healed_hops < 2 and (not streamed_any_tool or (has_truncated_raw and not is_streaming)):
                self_healed_hops += 1
                if on_sse_comment is not None:
                    on_sse_comment("dsh-envelope-self-heal")
                cur_is_delta = bool(any(cur_cursor))
                cur_tombstone = False
                cur_stitched = []
                next_slice = [{
                    "role": "user",
                    "content": (
                        "[Gateway Protocol Self-Heal]: Your previous tool call envelope was malformed or incomplete. "
                        "Re-emit ONLY the valid tool call envelope using the current TURN_NONCE "
                        "(use `mode=\"raw\"` with `<dsh_arg_NONCE>` when passing multi-line code or shell commands with quotes), "
                        "with zero surrounding explanation."
                    ),
                }]
                continue

            if not raw_calls:
                base_smode = str(step_res.get("stream_mode") or "plane1_webkit_native")
                smode = base_smode if hop == 0 else f"plane1_webkit+mcp_hops({hop})"
                if hop > 0 and "+rerouted(" in base_smode:
                    smode += base_smode[base_smode.find("+rerouted(") :]
                if self_healed_hops > 0:
                    smode += f"+self_heal({self_healed_hops})"
                if tool_choice_retried > 0:
                    smode += f"+tool_choice_retry({tool_choice_retried})"

                with sess.lock:
                    sess.record_web_cursor_locked(cur_cursor)
                    sess.web_turn_index = cur_turn_idx
                    sess.last_sys_sig = cur_sys_sig
                    sess.last_tools_sig = cur_tools_sig

                # Never emit stop_reason="tool_use" with tool_calls=[] when RAW_STREAM truncated!
                stop_r = "max_tokens" if has_truncated_raw else ("tool_use" if (streamed_any_tool and not has_truncated_raw) else "end_turn")
                return {
                    "stop_reason": stop_r,
                    "tool_calls": [],
                    "truncated_raw_stream": has_truncated_raw,
                    "routed_model": last_routed_model,
                    "text": "".join(accumulated_text),
                    "thinking": "".join(accumulated_thinking),
                    "ttft_ms": first_ttft or 1,
                    "elapsed_ms": max(1, int((time.time() - t_start) * 1000)),
                    "delta_chunks": total_chunks,
                    "stream_mode": smode,
                    "first_prompt_tokens": first_prompt_tokens,
                    "sent_prompt_tokens": sent_prompt_tokens,
                    "cursor_committed": last_cursor_committed,
                }

            # Partition calls into Layer A (client host) vs Internal/Layer B (Gateway-local MCP)
            layer_a_calls: List[dict] = []
            internal_results: List[dict] = []

            for tc in raw_calls:
                tname = str(tc.get("name", "")).strip()
                targs = tc.get("input") if isinstance(tc.get("input"), dict) else {}
                tc_id = str(tc.get("id") or ("toolu_" + random_hex(12)))

                if tname == PAGE_TOOL_NAME:
                    pg_res = sess.read_stored_page(
                        page_token=str(targs.get("page_token", "")),
                        page_num=_safe_int(targs.get("page", 1), 1),
                        grep_pat=str(targs.get("grep", "")),
                    )
                    pg_txt = "\n".join(x.get("text", "") for x in (pg_res.get("content") or []) if isinstance(x, dict))
                    internal_results.append({"server": "gateway-paginator", "tool": PAGE_TOOL_NAME, "text": pg_txt, "is_error": bool(pg_res.get("isError"))})
                    continue

                if allow_layer_b:
                    b_res = EMBEDDED_MCP_HOST.execute_layer_b_tool(tname, targs, layer_a_names, cwd=cwd)
                    if b_res is not None:
                        internal_results.append(b_res)
                        continue

                matched_host = next((h for h in layer_a_names if h == tname or h.lower() == tname.lower()), None)
                if matched_host:
                    schema = (allowed_schemas.get(matched_host) or {}).get("schema") or {}
                    norm_args = normalize_host_tool_args(matched_host, targs, schema, cwd=norm_cwd)
                    layer_a_calls.append({
                        "id": tc_id,
                        "name": matched_host,
                        "input": norm_args,
                        "streamed": bool(tc.get("streamed")),
                    })
                else:
                    internal_results.append({
                        "server": "gateway-router",
                        "tool": tname,
                        "text": (
                            f"Tool {tname!r} is not active. Active host tools: {', '.join(sorted(layer_a_names))}. "
                            f"Use `Bash` (`rg`/`find`/`ls`) and `Read` (`file_path`) instead."
                            if layer_a_names
                            else f"Tool {tname!r} is not active in this turn."
                        ),
                        "is_error": True,
                    })

            if layer_a_calls:
                if internal_results:
                    EMBEDDED_MCP_HOST.stash_mixed_results(layer_a_calls[-1]["id"], internal_results)
                base_smode = str(step_res.get("stream_mode") or "")
                smode = "plane1_webkit_tool_use" if not internal_results else "plane1_mixed_layerA+layerB"
                if "+rerouted(" in base_smode:
                    smode += base_smode[base_smode.find("+rerouted(") :]
                if self_healed_hops > 0:
                    smode += f"+self_heal({self_healed_hops})"
                if tool_choice_retried > 0:
                    smode += f"+tool_choice_retry({tool_choice_retried})"

                with sess.lock:
                    sess.record_web_cursor_locked(cur_cursor)
                    sess.web_turn_index = cur_turn_idx
                    sess.last_sys_sig = cur_sys_sig
                    sess.last_tools_sig = cur_tools_sig

                return {
                    "stop_reason": "tool_use",
                    "tool_calls": layer_a_calls,
                    "routed_model": last_routed_model,
                    "text": "".join(accumulated_text),
                    "thinking": "".join(accumulated_thinking),
                    "ttft_ms": first_ttft or 1,
                    "elapsed_ms": max(1, int((time.time() - t_start) * 1000)),
                    "delta_chunks": total_chunks,
                    "stream_mode": smode,
                    "first_prompt_tokens": first_prompt_tokens,
                    "sent_prompt_tokens": sent_prompt_tokens,
                    "cursor_committed": last_cursor_committed,
                }

            if on_sse_comment is not None:
                on_sse_comment("mcp-local-executing")
            cur_is_delta = bool(any(cur_cursor))
            cur_tombstone = False
            cur_stitched = []
            followup_txt = "\n\n".join(
                f"[Local MCP Tool Result ({r['server']}/{r['tool']})]:\n{sess.paginate_tool_text(r['text'], budget_chars=WEB_RPC_PAGE_CHARS)}"
                for r in internal_results
            )
            next_slice = [{"role": "user", "content": followup_txt}]

        with sess.lock:
            sess.record_web_cursor_locked(cur_cursor)
            sess.web_turn_index = cur_turn_idx
            sess.last_sys_sig = cur_sys_sig
            sess.last_tools_sig = cur_tools_sig

        return {
            "stop_reason": "end_turn",
            "tool_calls": [],
            "routed_model": last_routed_model,
            "text": "".join(accumulated_text) + f"\n\n[Gateway circuit breaker: reached {MAX_INTERNAL_MCP_HOPS} internal MCP hops]",
            "thinking": "".join(accumulated_thinking),
            "ttft_ms": first_ttft or 1,
            "elapsed_ms": max(1, int((time.time() - t_start) * 1000)),
            "delta_chunks": total_chunks,
            "stream_mode": "plane1_mcp_hop_limit",
            "first_prompt_tokens": first_prompt_tokens,
        }
    except Exception:
        _rollback_cursor()
        raise


def format_compact_tool_guide(tools: List[dict]) -> str:
    user_tools = [t for t in tools if t["name"] not in (AWAIT_TOOL_NAME, CALL_TOOL_NAME, PAGE_TOOL_NAME)]
    if not user_tools:
        return "No host workspace tools are attached on this turn."
    lines = ["[AVAILABLE HOST WORKSPACE TOOLS — ONLY CALL NAMES LISTED BELOW]"]
    for t in user_tools:
        schema = t.get("schema") or {}
        props = schema.get("properties") or {}
        reqs = schema.get("required") or []
        prop_summary = ", ".join(
            f"{k}{'*' if k in reqs else ''}:{v.get('type', 'any') if isinstance(v, dict) else 'any'}"
            for k, v in list(props.items())[:8]
        )
        lines.append(f"- {t['name']}({prop_summary}) — {t['description'][:110]}")
    names_set = {t["name"] for t in user_tools}
    if "Glob" not in names_set or "Grep" not in names_set:
        lines.append(
            "NOTE: `Glob` / `Grep` are NOT active in this turn; to list or search project files, call `Bash` "
            "(e.g. `{\"command\":\"ls -la\",\"description\":\"List project files\"}` or `rg --files`) and `Read` (`{\"file_path\":\"/abs/path\"}`)."
        )
    return "\n".join(lines)


def build_mcp_task_prompt(
    sess: SessionState,
    system_text: str,
    messages_slice: list,
    is_delta: bool,
    model_id: str,
    tier: str,
) -> str:
    with sess.lock:
        tools = list(sess.allowed_tools.values())
        cwd = sess.cwd
    tok = sess.session_token
    cli_cmd = f"/usr/bin/python3 /tmp/dsh_worker_cli.py --ipc-dir {IPC_DIR}"
    out_md = f"/tmp/dsh_out_{tok}.md"

    lines = [
        f"[DSH_SESSION_TOKEN: {tok}] [Model: {model_id}/{tier}] [CWD: {cwd}]",
        "Bridge Protocol (always run shell commands with `RequiresNetworkAccess=false`):",
        f"1. Optional live streaming chunk: `{cli_cmd} stream --token {tok} --thinking '<brief thought>' --delta '<partial text>'`",
        f"2. Host tool execution (use this for ALL file reads/edits/searches/commands in `{cwd}` so the host terminal records every step): "
        f"`{cli_cmd} call --token {tok} --tool <ExactToolName> --args '<json>'`",
        f"3. Complete turn & stay parked for Sticky Delta: write final markdown reply to `{out_md}` and run "
        f"`{cli_cmd} reply --token {tok} --content-file {out_md} --wait-next --model {model_id} --timeout 250`",
    ]
    if not is_delta:
        lines.append("\n" + format_compact_tool_guide(tools))
        if system_text:
            lines.append(f"\n[Workspace Context]\n{prune_client_system_prompt(system_text)[:120000]}")
    else:
        active_names = ", ".join(
            t["name"] for t in tools if t["name"] not in (AWAIT_TOOL_NAME, CALL_TOOL_NAME, PAGE_TOOL_NAME)
        )
        if active_names:
            lines.append(f"Active host tools: {active_names}")

    lines.append("\n[Conversation Messages]")
    for m in messages_slice:
        lines.append(f"{str(m.get('role', 'user')).upper()}: {format_single_message_clean(m)}")
    return "\n".join(lines)


def extract_anthropic_tool_results(messages: list) -> List[dict]:
    out = []
    for m in reversed(messages or []):
        if not isinstance(m, dict):
            continue
        if m.get("role") == "assistant":
            break
        content = m.get("content")
        if isinstance(content, list):
            for blk in content:
                if isinstance(blk, dict) and blk.get("type") == "tool_result":
                    tc_id = blk.get("tool_use_id", "")
                    c = blk.get("content", "")
                    if isinstance(c, list):
                        c = _extract_content_text(c)
                    elif c is None:
                        c = ""
                    out.append({"tool_call_id": tc_id, "content": str(c), "is_error": bool(blk.get("is_error", False))})
    out.reverse()
    return out


def extract_openai_tool_results(messages: list) -> List[dict]:
    out = []
    for m in reversed(messages or []):
        if not isinstance(m, dict):
            continue
        if m.get("role") == "assistant":
            break
        if m.get("role") == "tool" and m.get("tool_call_id"):
            out.append({
                "tool_call_id": m["tool_call_id"],
                "content": _extract_content_text(m.get("content", "")),
                "is_error": False,
            })
    out.reverse()
    return out


def _acquire_turn_lock(sess: SessionState, cancel_event: Optional[threading.Event]) -> None:
    while not sess.turn_lock.acquire(timeout=0.2):
        if cancel_event is not None and cancel_event.is_set():
            raise RequestCancelledError("Client disconnected while waiting for session turn lock")


def _checkpoint_matches_prefix(cp_len: int, cp_val: tuple, messages: list, strip_reminders: bool = True) -> bool:
    if cp_len <= 0 or len(messages) <= cp_len:
        return False
    asst_msg = messages[cp_len - 1]
    if not isinstance(asst_msg, dict) or str(asst_msg.get("role", "")).lower() != "assistant":
        return False
    cp_hash = str(cp_val[3]) if len(cp_val) > 3 else ""
    if cp_hash:
        return compute_messages_prefix_hash(messages[: cp_len - 1], strip_reminders=strip_reminders) == cp_hash
    return True


def classify_sideband_ephemeral_call(
    headers: dict,
    conv_key: str,
    system_text: str,
    active_raw_tools: list,
    messages: list,
    has_tool_results: bool,
) -> Tuple[bool, Optional[dict], str]:
    """
    Detects internal sideband helper requests (such as Claude Code's `<block>yes/no</block>` Bash
    permission classifier or background session recaps) that reuse the main `x-claude-code-session-id`
    without attaching tools. Isolating them prevents shattering the main `4 Carbon` session cursor
    and allows immediate sidebar auto-deletion upon completion.
    """
    if active_raw_tools or has_tool_results or not messages:
        return False, None, ""
    lower_hdrs = {k.lower(): str(v) for k, v in (headers or {}).items()}
    req_cls = lower_hdrs.get("x-claude-code-request-class", "").strip().lower()
    is_cc_caller = (
        "claude-cli" in lower_hdrs.get("user-agent", "").lower()
        or any(k.startswith("x-claude-code-") for k in lower_hdrs)
    )
    last_msg = messages[-1] if isinstance(messages[-1], dict) else {}
    tail_txt = _extract_content_text(last_msg.get("content", ""))[:4000]
    combined_low = (system_text[:4000] + "\n" + tail_txt).lower()

    if ("<block>" in combined_low and "</block>" in combined_low) or (
        "<block>" in combined_low and ("security" in combined_low or "command" in combined_low or "classifier" in combined_low or "reason>" in combined_low)
    ):
        return True, MODEL_BY_ID["gemini-app-fast"], "ephem_classifier"

    if is_cc_caller and (
        "omitting technical" in combined_low
        or "summary of the prior interaction" in combined_low
        or "conversational recap" in combined_low
        or ("in plain language" in combined_low and "summary" in combined_low)
    ):
        return True, MODEL_BY_ID["gemini-app-fast"], "ephem_recap"

    if req_cls and req_cls not in ("interactive", "main", "default", "chat", "agent", "primary"):
        return True, None, f"ephem_{req_cls}"

    if is_cc_caller and conv_key.startswith("sid:") and REGISTRY.has_active_tool_session(conv_key):
        return True, None, "ephem_sideband"

    return False, None, ""


def execute_turn(
    headers: dict,
    body: dict,
    model_info: dict,
    tier: str,
    system_text: str,
    messages: list,
    tools: list,
    is_openai: bool,
    on_delta: Optional[Callable[[dict], None]] = None,
    on_sse_comment: Optional[Callable[[str], None]] = None,
    timeout_sec: Optional[float] = None,
    cancel_event: Optional[threading.Event] = None,
    on_stream_open: Optional[Callable[[dict], None]] = None,
) -> dict:
    effective_upstream = _resolve_effective_upstream(headers)
    tool_choice_mode, forced_tool_name = parse_tool_choice(body, tools)
    active_raw_tools = [] if tool_choice_mode == "none" else (tools or [])

    model_id = model_info["id"]
    if timeout_sec is None:
        timeout_sec = 600.0 if ("carbon" in model_id.lower() or str(tier).lower() in ("xhigh", "max")) else 360.0
    cwd, explicit_cwd = detect_cwd_from_context(system_text, messages)
    lower_hdrs = {k.lower(): str(v) for k, v in (headers or {}).items()}
    strip_reminders = (
        "claude-cli" in lower_hdrs.get("user-agent", "").lower()
        or any(k.startswith("x-claude-code-") for k in lower_hdrs)
    )
    allow_layer_b = _should_enable_layer_b(headers, active_raw_tools, cwd, explicit_cwd, tool_choice_mode)
    trs = extract_openai_tool_results(messages) if is_openai else extract_anthropic_tool_results(messages)

    # 1. Fast in-flight resumption if a Plane-2 FIFO worker is parked on a host tool call
    if trs:
        parked_sess = REGISTRY.find_by_tool_result_ids([x["tool_call_id"] for x in trs])
        if parked_sess is not None:
            _acquire_turn_lock(parked_sess, cancel_event)
            try:
                with parked_sess.lock:
                    still_parked = any(x["tool_call_id"] in parked_sess.parked_calls for x in trs)
                if still_parked:
                    parked_sess.register_tools(active_raw_tools, cwd, system_text, messages=messages, explicit_cwd=explicit_cwd)
                    new_turn = ActiveTurn(on_delta=on_delta)
                    late_calls = []
                    added_tok = sum(est_tokens_str(tr["content"]) for tr in trs)
                    with parked_sess.lock:
                        ctx_before = parked_sess.net_context_tokens
                        parked_sess.active_turn = new_turn
                        parked_sess.net_context_tokens += added_tok
                        parked_sess.last_used_at = time.time()
                        for tr in trs:
                            p = parked_sess.parked_calls.pop(tr["tool_call_id"], None)
                            if p is not None:
                                paged_txt = parked_sess.paginate_tool_text(tr["content"] or "OK", budget_chars=MAX_TOOL_RESULT_CHARS)
                                p.result_q.put({"content": [{"type": "text", "text": paged_txt}], "isError": tr["is_error"]})
                        for cid, rem in list(parked_sess.parked_calls.items()):
                            if not rem.delivered:
                                late_calls.append(rem)
                            else:
                                parked_sess.parked_calls.pop(cid, None)
                                rem.result_q.put({"content": [{"type": "text", "text": "Tool result not provided by client."}], "isError": True})
                    for lc in late_calls:
                        parked_sess.enqueue_parked_call(lc)

                    if on_stream_open is not None:
                        on_stream_open({"cached_tokens": ctx_before, "model_id": model_id, "tier": tier, "turn_mode": "plane2_parked_resume"})

                    deadline = time.time() + timeout_sec
                    outcome = None
                    while True:
                        if cancel_event is not None and cancel_event.is_set():
                            with parked_sess.lock:
                                parked_sess.active_turn = None
                            raise RequestCancelledError("Client disconnected")
                        rem_t = deadline - time.time()
                        if rem_t <= 0:
                            raise RuntimeError("Plane 2 parked worker timed out waiting for tool resume")
                        try:
                            outcome = new_turn.outcome_q.get(timeout=min(0.25, rem_t))
                            break
                        except queue.Empty:
                            continue

                    if "error" in outcome:
                        raise RuntimeError(outcome["error"])
                    with parked_sess.lock:
                        parked_sess.synced_msg_count = len(messages) + 1
                        ctx_now = parked_sess.net_context_tokens
                    outcome["cached_tokens"] = ctx_now
                    outcome["turn_mode"] = "plane2_parked_resume"
                    REGISTRY.record_turn({"model": model_id, "tier": tier, "mode": "plane2_parked_resume", "ctx_tokens": ctx_now, **outcome})
                    return outcome
            finally:
                parked_sess.turn_lock.release()

    # 2. Stitch any stashed Layer-B local MCP results from a previous mixed batch
    stitched_layer_b = EMBEDDED_MCP_HOST.pop_stashed_results([x["tool_call_id"] for x in trs]) if trs else []

    # 3. Acquire per-session turn_lock (isolating toolless sideband classifiers/recaps onto ephemeral auto-deleted sessions)
    conv_key = derive_conv_key(headers, body, system_text, messages)
    is_ephem_sb, sb_model_override, sb_mode_label = classify_sideband_ephemeral_call(
        headers=headers,
        conv_key=conv_key,
        system_text=system_text,
        active_raw_tools=active_raw_tools,
        messages=messages,
        has_tool_results=bool(trs),
    )
    if is_ephem_sb:
        if sb_model_override is not None:
            model_info = sb_model_override
            model_id = model_info["id"]
            tier = model_info.get("default_tier", "low")
        sess = SessionState(f"{conv_key}:{sb_mode_label}:{random_hex(6)}", "tok_ephem_" + random_hex(8))
        sess.is_ephemeral = True
    else:
        sess = REGISTRY.get_or_create(conv_key)
    _acquire_turn_lock(sess, cancel_event)

    try:
        with sess.lock:
            orig_snap = sess.snapshot_state_locked()
            sess.register_tools(active_raw_tools, cwd, system_text, messages=messages, explicit_cwd=explicit_cwd)
            cur_fp = first_user_fingerprint(messages)
            in_tool_chain = bool(trs)

            prev_model = sess.model_id
            prev_cnt = sess.synced_msg_count
            prev_fp = sess.first_fp
            prev_ctx = sess.net_context_tokens
            worker_waiting = sess.worker_waiting_next

            same_or_shrank = prev_cnt > 0 and len(messages) <= prev_cnt
            shrank_strict = prev_cnt > 0 and len(messages) < prev_cnt
            fp_changed = bool(prev_fp and cur_fp and prev_fp != cur_fp)

            prefix_mismatch = False
            if prev_cnt > 0 and not same_or_shrank and not fp_changed:
                cp_prev = sess.cursor_checkpoints.get(prev_cnt)
                if cp_prev is not None:
                    if not _checkpoint_matches_prefix(prev_cnt, cp_prev, messages, strip_reminders=True):
                        prefix_mismatch = True
                elif len(messages) >= prev_cnt:
                    asst_slot = messages[prev_cnt - 1]
                    if not isinstance(asst_slot, dict) or str(asst_slot.get("role", "")).lower() != "assistant":
                        prefix_mismatch = True

            rewound_checkpoint = None
            if prev_cnt > 0 and (same_or_shrank or fp_changed or prefix_mismatch):
                for k_len in range(len(messages), 0, -1):
                    if k_len in sess.cursor_checkpoints:
                        cp_cand = sess.cursor_checkpoints[k_len]
                        if _checkpoint_matches_prefix(k_len, cp_cand, messages, strip_reminders=True):
                            rewound_checkpoint = (k_len, cp_cand)
                            break

            if rewound_checkpoint is not None:
                cp_len, cp_val = rewound_checkpoint
                cp_cursor, cp_tidx, cp_tok = cp_val[0], cp_val[1], cp_val[2]
                sess.web_cursor = cp_cursor
                sess.web_turn_index = cp_tidx
                sess.synced_msg_count = cp_len
                sess.net_context_tokens = cp_tok
                prev_cnt = cp_len
                prev_ctx = cp_tok
                same_or_shrank = False
                shrank_strict = False
                fp_changed = False
                prefix_mismatch = False

            need_cold_reset = (prev_cnt > 0) and (same_or_shrank or fp_changed or prefix_mismatch)
            unseen_msgs = messages[prev_cnt:] if (prev_cnt > 0 and not need_cold_reset) else messages
            est_delta_tok = est_tokens_str(json.dumps(unseen_msgs, ensure_ascii=False))
            if prev_cnt == 0 or need_cold_reset:
                est_delta_tok += est_tokens_str(prune_client_system_prompt(system_text)[:120000])

            effective_watermark = HARD_CEILING_TOKENS if in_tool_chain else SOFT_WATERMARK_TOKENS
            hit_watermark = (prev_cnt > 0) and (not need_cold_reset) and (prev_ctx + est_delta_tok > effective_watermark)
            model_switched_fifo = bool(prev_cnt > 0 and prev_model != model_id and worker_waiting)
            need_rollover = need_cold_reset or hit_watermark or model_switched_fifo

            if hit_watermark:
                sess.reset_for_rollover_locked()
                unseen_msgs = messages
                is_delta = False
                turn_mode = "rollover_256k_tombstone"
                cached_tok = 0
                sess.net_context_tokens = estimate_dehydrated_context_tokens(system_text, messages)
            elif need_cold_reset or model_switched_fifo:
                sess.reset_cold_locked()
                unseen_msgs = messages
                is_delta = False
                turn_mode = (
                    "rollover_compact" if shrank_strict
                    else ("rollover_prefix" if (fp_changed or prefix_mismatch or model_switched_fifo) else "cold_start")
                )
                cached_tok = 0
                sess.net_context_tokens = est_delta_tok
            elif prev_cnt > 0 and len(unseen_msgs) > 0:
                is_delta = True
                turn_mode = "sticky_delta"
                cached_tok = prev_ctx
                sess.net_context_tokens = prev_ctx + est_delta_tok
                sess.reuse_count += 1
            else:
                is_delta = False
                turn_mode = "cold_start"
                cached_tok = 0
                sess.net_context_tokens = est_delta_tok

            if sess.is_ephemeral and sb_mode_label:
                turn_mode = sb_mode_label
            sess.model_id = model_id
            sess.tier = tier
            sess.first_fp = cur_fp
            sess.last_used_at = time.time()
            ctx_after = sess.net_context_tokens

        # 4. Dual-Plane Dispatch Selection
        has_plane2_fifo = worker_waiting or (WORKER_POOL.idle_count() > 0)
        force_app_session_only = (effective_upstream == "mcp_task")
        plane1 = get_webkit_plane()

        if not has_plane2_fifo and not force_app_session_only:
            p1_status = plane1.ensure_daemon_running(auto_popup_login=False, wait_sec=3.5)
            if p1_status and not p1_status.get("logged_in"):
                # Cold-start hydration wait up to 5.0s if DSHWebKitBridge is still loading DOM / WIZ_global_data
                for _ in range(20):
                    if cancel_event is not None and cancel_event.is_set():
                        with sess.lock:
                            sess.restore_state_snapshot_locked(orig_snap)
                        raise RequestCancelledError("Client disconnected")
                    if p1_status.get("logged_in"):
                        break
                    time.sleep(0.25)
                    p1_status = plane1.status()
                    if p1_status.get("page_loaded") and not p1_status.get("logged_in"):
                        time.sleep(0.25)
                        p1_status = plane1.status()
                        break

            if p1_status and p1_status.get("logged_in"):
                active_acct_raw = str(p1_status.get("account", ""))
                aligned, pinned_acct, active_acct = ACCOUNT_STORE.check_alignment(active_acct_raw)
                if not aligned:
                    with sess.lock:
                        sess.restore_state_snapshot_locked(orig_snap)
                    raise GatewayHTTPError(
                        403,
                        "permission_error",
                        (
                            f"Account alignment blocked request: pinned account is {pinned_acct!r}, "
                            f"active WebKit account is {active_acct or 'unidentified'!r}. "
                            f"Run './cc-demo.sh switch-account' or './cc-demo.sh pin-account {active_acct}'."
                        ),
                        code="account_mismatch",
                    )

                if active_acct:
                    with sess.lock:
                        if sess.account and sess.account.lower() != active_acct.lower():
                            sess.reset_cold_locked()
                            is_delta = False
                            unseen_msgs = messages
                            turn_mode = "cold_start"
                            cached_tok = 0
                        sess.account = active_acct

                with sess.lock:
                    p1_has_cursor = bool(any(sess.web_cursor))
                p1_is_delta = bool(is_delta and p1_has_cursor)
                p1_msgs = unseen_msgs if p1_is_delta else messages
                if not p1_is_delta:
                    cached_tok = 0

                if on_stream_open is not None:
                    on_stream_open({
                        "cached_tokens": cached_tok,
                        "model_id": model_id,
                        "tier": tier,
                        "turn_mode": turn_mode,
                    })

                prio = 1 if is_subagent_request(headers, body) else 2
                emitted_visible_output = [False]

                def _tracked_on_delta(chunk: dict) -> None:
                    if chunk.get("type") in ("text", "tool_start", "tool_delta", "tool_stop"):
                        emitted_visible_output[0] = True
                    if on_delta is not None:
                        on_delta(chunk)

                try:
                    base_ctx_for_wire = cached_tok if p1_is_delta else 0
                    try:
                        outcome = execute_plane1_turn_with_local_mcp_loop(
                            sess=sess,
                            plane=plane1,
                            system_text=system_text,
                            unseen_msgs=p1_msgs,
                            is_delta=p1_is_delta,
                            tombstone_rollover=hit_watermark,
                            model_id=model_id,
                            tier=tier,
                            priority=prio,
                            stitched_layer_b=stitched_layer_b,
                            on_delta=_tracked_on_delta if on_delta is not None else None,
                            on_sse_comment=on_sse_comment,
                            timeout_sec=timeout_sec,
                            cancel_event=cancel_event,
                            allow_layer_b=allow_layer_b,
                            tool_choice=body.get("tool_choice"),
                            tool_choice_mode=tool_choice_mode,
                            forced_tool_name=forced_tool_name,
                            strip_reminders=strip_reminders,
                            explicit_cwd=explicit_cwd,
                            is_streaming=(on_delta is not None),
                        )
                    except RequestCancelledError:
                        raise
                    except Exception as first_exc:
                        if cancel_event is not None and cancel_event.is_set():
                            raise RequestCancelledError("Client disconnected") from first_exc
                        first_err = str(first_exc)
                        cursor_poison_errs = (
                            "STALE_CURSOR_OR_EMPTY",
                            "UPSTREAM_CANNED_ERROR",
                            "BRIDGE_PREMATURE_EOF",
                            "RPC_ERR_13",
                            "RPC_ERR_3",
                            "BardErrorInfo",
                            "[1097,",
                            "无法继续对话",
                            "Could not continue",
                            "no wrb.fr",
                            "no authenticated frames",
                        )
                        with sess.lock:
                            failed_cursor = sess.web_cursor
                            had_cursor = bool(any(failed_cursor))
                            fallback_cp: Optional[Tuple[int, tuple]] = None
                            if had_cursor:
                                for cand_len in sorted(sess.cursor_checkpoints.keys(), reverse=True):
                                    if cand_len >= len(messages):
                                        continue
                                    cand_val = sess.cursor_checkpoints[cand_len]
                                    cand_committed = bool(cand_val[4]) if len(cand_val) > 4 else True
                                    if (
                                        cand_committed
                                        and cand_val[0] != failed_cursor
                                        and any(cand_val[0])
                                        and _checkpoint_matches_prefix(cand_len, cand_val, messages, strip_reminders=True)
                                    ):
                                        fallback_cp = (cand_len, cand_val)
                                        break
                        if any(tok in first_err for tok in cursor_poison_errs) and had_cursor and not emitted_visible_output[0]:
                            recovered_via_cp = False
                            if fallback_cp is not None:
                                cp_len, cp_val = fallback_cp
                                with sess.lock:
                                    sess.web_cursor = cp_val[0]
                                    sess.web_turn_index = int(cp_val[1])
                                    sess.synced_msg_count = cp_len
                                    sess.net_context_tokens = int(cp_val[2])
                                sys.stderr.write(
                                    f"[gateway] Rewinding uncommitted/stale web_cursor to committed checkpoint msg={cp_len} ({first_err})\n"
                                )
                                sys.stderr.flush()
                                if on_sse_comment is not None:
                                    on_sse_comment("dsh-cursor-checkpoint-rewind")
                                try:
                                    outcome = execute_plane1_turn_with_local_mcp_loop(
                                        sess=sess,
                                        plane=plane1,
                                        system_text=system_text,
                                        unseen_msgs=messages[cp_len:],
                                        is_delta=True,
                                        tombstone_rollover=False,
                                        model_id=model_id,
                                        tier=tier,
                                        priority=prio,
                                        stitched_layer_b=stitched_layer_b,
                                        on_delta=_tracked_on_delta if on_delta is not None else None,
                                        on_sse_comment=on_sse_comment,
                                        timeout_sec=timeout_sec,
                                        cancel_event=cancel_event,
                                        allow_layer_b=allow_layer_b,
                                        tool_choice=body.get("tool_choice"),
                                        tool_choice_mode=tool_choice_mode,
                                        forced_tool_name=forced_tool_name,
                                        strip_reminders=strip_reminders,
                                        explicit_cwd=explicit_cwd,
                                        is_streaming=(on_delta is not None),
                                    )
                                    turn_mode = "cursor_checkpoint_rewind"
                                    cached_tok = int(cp_val[2])
                                    base_ctx_for_wire = cached_tok
                                    recovered_via_cp = True
                                except RequestCancelledError:
                                    raise
                                except Exception:
                                    if emitted_visible_output[0]:
                                        raise

                            if not recovered_via_cp:
                                with sess.lock:
                                    sess.reset_cold_locked()
                                sys.stderr.write(f"[gateway] Recovering poisoned/stale web_cursor via transparent cold replay ({first_err})\n")
                                sys.stderr.flush()
                                if on_sse_comment is not None:
                                    on_sse_comment("dsh-cursor-self-heal")
                                turn_mode = "cursor_self_heal"
                                cached_tok = 0
                                base_ctx_for_wire = 0
                                outcome = execute_plane1_turn_with_local_mcp_loop(
                                    sess=sess,
                                    plane=plane1,
                                    system_text=system_text,
                                    unseen_msgs=messages,
                                    is_delta=False,
                                    tombstone_rollover=hit_watermark,
                                    model_id=model_id,
                                    tier=tier,
                                    priority=prio,
                                    stitched_layer_b=stitched_layer_b,
                                    on_delta=_tracked_on_delta if on_delta is not None else None,
                                    on_sse_comment=on_sse_comment,
                                    timeout_sec=timeout_sec,
                                    cancel_event=cancel_event,
                                    allow_layer_b=allow_layer_b,
                                    tool_choice=body.get("tool_choice"),
                                    tool_choice_mode=tool_choice_mode,
                                    forced_tool_name=forced_tool_name,
                                    strip_reminders=strip_reminders,
                                    explicit_cwd=explicit_cwd,
                                    is_streaming=(on_delta is not None),
                                )
                        else:
                            raise

                    if cancel_event is not None and cancel_event.is_set():
                        raise RequestCancelledError("Client disconnected")

                    with sess.lock:
                        sent_wire_tok = int(outcome.get("sent_prompt_tokens") or outcome.get("first_prompt_tokens") or est_delta_tok)
                        out_wire_tok = estimate_output_tokens(outcome, include_thinking=False)
                        if hit_watermark:
                            sess.net_context_tokens = sent_wire_tok + out_wire_tok
                        else:
                            sess.net_context_tokens = base_ctx_for_wire + sent_wire_tok + out_wire_tok
                        new_cnt = len(messages) + 1
                        sess.synced_msg_count = new_cnt
                        cur_committed = bool(outcome.get("cursor_committed", True))
                        prefix_h = compute_messages_prefix_hash(messages, strip_reminders=True)
                        sess.cursor_checkpoints[new_cnt] = (
                            sess.web_cursor,
                            sess.web_turn_index,
                            sess.net_context_tokens,
                            prefix_h,
                            cur_committed,
                        )
                        if len(sess.cursor_checkpoints) > 24:
                            oldest_k = min(sess.cursor_checkpoints.keys())
                            sess.cursor_checkpoints.pop(oldest_k, None)
                        ctx_after = sess.net_context_tokens
                    if REGISTRY.cursor_store is not None:
                        REGISTRY.cursor_store.save_from(sess)
                    outcome["cached_tokens"] = cached_tok
                    outcome["turn_mode"] = turn_mode
                    REGISTRY.record_turn({"status": "ok", "model": model_id, "tier": tier, "mode": turn_mode, "ctx_tokens": ctx_after, **outcome})
                    return outcome
                except RequestCancelledError:
                    with sess.lock:
                        sess.restore_state_snapshot_locked(orig_snap)
                    raise
                except Exception as p1_exc:
                    err_s = str(p1_exc)
                    sys.stderr.write(f"[gateway] Plane 1 error: {err_s}\n")
                    sys.stderr.flush()
                    REGISTRY.record_turn({
                        "status": "error",
                        "model": model_id,
                        "tier": tier,
                        "mode": turn_mode,
                        "ctx_tokens": cached_tok,
                        "stream_mode": "plane1_error",
                        "error": err_s[:240],
                        "ttft_ms": 0,
                        "elapsed_ms": 0,
                        "thinking": "",
                        "text": "",
                    })
                    if any(tok in err_s for tok in ("AUTH_REQUIRED", "HTTP_401", "HTTP_403", "WEBCONTENT_TERMINATED")):
                        plane1.invalidate_auth()
                        if effective_upstream == "web":
                            with sess.lock:
                                sess.restore_state_snapshot_locked(orig_snap)
                            raise GatewayHTTPError(503, "api_error", f"Plane 1 WebKit authentication required: {err_s}", code="upstream_auth_required") from p1_exc
                    else:
                        with sess.lock:
                            sess.restore_state_snapshot_locked(orig_snap)
                        raise

        # 5. Plane 2 FIFO Dispatch (Zero-Login Direct Reuse of `Gemini.app` Session)
        if effective_upstream == "web":
            with sess.lock:
                sess.restore_state_snapshot_locked(orig_snap)
            raise GatewayHTTPError(503, "api_error", "Plane 1 WebKit bridge is offline or unauthenticated.", code="upstream_unavailable")

        with sess.lock:
            turn = ActiveTurn(on_delta=on_delta)
            sess.active_turn = turn
            sess.in_flight = True

        claimed_ev = threading.Event()
        dispatch_item: Optional[dict] = None
        item_lock = threading.Lock()

        if worker_waiting and not need_rollover:
            delta_prompt = build_mcp_task_prompt(sess, system_text, unseen_msgs, is_delta=True, model_id=model_id, tier=tier)
            claimed_ev.set()
            sess.next_delta_q.put({"prompt": delta_prompt, "model": model_id, "tier": tier, "claimed": claimed_ev})
        else:
            prompt_str = build_mcp_task_prompt(sess, system_text, unseen_msgs, is_delta=is_delta, model_id=model_id, tier=tier)
            dispatch_item = {
                "session_token": sess.session_token,
                "conv_key": conv_key,
                "model": model_id,
                "tier": tier,
                "prompt": prompt_str,
                "ts": time.time(),
                "claimed": claimed_ev,
                "cancelled": False,
                "claimed_done": False,
                "_lock": item_lock,
            }
            if not WORKER_POOL.try_dispatch(dispatch_item):
                REGISTRY.prompt_dispatch_q.put(dispatch_item)

        claim_deadline = time.time() + WORKER_CLAIM_TIMEOUT_SEC
        claimed_ok = False
        while True:
            if cancel_event is not None and cancel_event.is_set():
                if dispatch_item is not None:
                    with item_lock:
                        if not dispatch_item.get("claimed_done"):
                            dispatch_item["cancelled"] = True
                with sess.lock:
                    sess.active_turn = None
                    sess.in_flight = False
                    sess.restore_state_snapshot_locked(orig_snap)
                raise RequestCancelledError("Client disconnected")
            rem_c = claim_deadline - time.time()
            if claimed_ev.wait(timeout=max(0.0, min(0.2, rem_c))):
                claimed_ok = True
                break
            if time.time() >= claim_deadline:
                break

        if not claimed_ok:
            was_claimed_late = False
            if dispatch_item is not None:
                with item_lock:
                    if dispatch_item.get("claimed_done"):
                        was_claimed_late = True
                    else:
                        dispatch_item["cancelled"] = True
            if not was_claimed_late:
                with sess.lock:
                    sess.active_turn = None
                    sess.in_flight = False
                    sess.restore_state_snapshot_locked(orig_snap)
                raise GatewayHTTPError(
                    503,
                    "api_error",
                    (
                        f"Upstream unavailable for {model_id}/{tier}: Plane 1 WebKit bridge is offline/unauthenticated "
                        f"and no Plane 2 worker claimed the turn within {WORKER_CLAIM_TIMEOUT_SEC:.1f}s."
                    ),
                    code="upstream_unavailable",
                )

        if on_stream_open is not None:
            on_stream_open({
                "cached_tokens": cached_tok,
                "model_id": model_id,
                "tier": tier,
                "turn_mode": turn_mode,
            })

        deadline = time.time() + timeout_sec
        outcome = None
        while True:
            if cancel_event is not None and cancel_event.is_set():
                with sess.lock:
                    sess.active_turn = None
                    sess.in_flight = False
                    sess.restore_state_snapshot_locked(orig_snap)
                raise RequestCancelledError("Client disconnected")
            rem_t = deadline - time.time()
            if rem_t <= 0:
                with sess.lock:
                    sess.active_turn = None
                    sess.in_flight = False
                    sess.restore_state_snapshot_locked(orig_snap)
                raise RuntimeError("Upstream turn timed out waiting for worker response")
            try:
                outcome = turn.outcome_q.get(timeout=min(0.25, rem_t))
                break
            except queue.Empty:
                continue

        if "error" in outcome:
            with sess.lock:
                sess.restore_state_snapshot_locked(orig_snap)
            raise RuntimeError(outcome["error"])

        with sess.lock:
            sess.synced_msg_count = len(messages) + 1
            sess.net_context_tokens += est_tokens_str(outcome.get("text", ""))
        if REGISTRY.cursor_store is not None and (need_rollover or not any(sess.web_cursor)):
            REGISTRY.cursor_store.save_from(sess)
        outcome["cached_tokens"] = cached_tok
        outcome["turn_mode"] = turn_mode
        REGISTRY.record_turn({"model": model_id, "tier": tier, "mode": turn_mode, "ctx_tokens": ctx_after, **outcome})
        return outcome
    finally:
        if sess.is_ephemeral:
            with sess.lock:
                ephem_cids = sess.pop_all_web_cids_locked()
                sess.web_cursor = ("", "", "")
            if ephem_cids:
                schedule_web_conversation_cleanup(ephem_cids)
        sess.turn_lock.release()


def await_parked_call(sess: SessionState, parked: ParkedCall, stop_event: Optional[threading.Event] = None) -> dict:
    deadline = time.time() + RELAY_INTERVAL_SEC
    while True:
        if stop_event is not None and stop_event.is_set():
            return {"content": [{"type": "text", "text": "Host tool call cancelled."}], "isError": True}
        rem = deadline - time.time()
        if rem <= 0:
            break
        try:
            res = parked.result_q.get(timeout=min(0.4, rem))
            with sess.lock:
                sess.parked_calls.pop(parked.call_id, None)
            return res
        except queue.Empty:
            continue

    parked.relay_count += 1
    return {
        "content": [{
            "type": "text",
            "text": (
                f"Tool call {parked.call_id} ({parked.name}) is still running in the host harness "
                f"(relay #{parked.relay_count}). Call dsh_await_result with "
                f'{{"call_id":"{parked.call_id}"}} immediately to continue waiting for its result.'
            ),
        }],
        "isError": False,
    }


def execute_session_mcp_tool(sess: SessionState, raw_name: str, raw_args: dict, stop_event: Optional[threading.Event] = None) -> dict:
    clean_args = {k: v for k, v in (raw_args or {}).items() if k not in ("toolSummary", "session_token")}

    with sess.lock:
        cwd = sess.cwd
        explicit_cwd = sess.has_explicit_cwd
        host_tool = sess.mcp_to_host.get(raw_name)
        if not host_tool and raw_name.startswith("mcp_dsh-bridge_"):
            host_tool = sess.mcp_to_host.get(raw_name[len("mcp_dsh-bridge_") :])
        if not host_tool:
            host_tool = next((v for k, v in sess.mcp_to_host.items() if k.lower() == raw_name.lower()), None)
        active_user_tools = [
            k for k in sess.allowed_tools.keys()
            if k not in (AWAIT_TOOL_NAME, CALL_TOOL_NAME, PAGE_TOOL_NAME)
        ]

    if not host_tool:
        b_res = EMBEDDED_MCP_HOST.execute_layer_b_tool(raw_name, clean_args, set(active_user_tools), cwd=cwd)
        if b_res is not None:
            return {"content": [{"type": "text", "text": b_res["text"]}], "isError": b_res["is_error"]}
        return {
            "content": [{
                "type": "text",
                "text": (
                    f"Tool {raw_name!r} is not active in this turn. "
                    f"Active tools: {', '.join(active_user_tools)}. "
                    f"Use 'Bash' (e.g. ls/find/rg) and 'Read' ({{'file_path': '/abs/path'}}) instead."
                ),
            }],
            "isError": True,
        }

    if host_tool == AWAIT_TOOL_NAME:
        cid = str(clean_args.get("call_id", "")).strip()
        with sess.lock:
            parked = sess.parked_calls.get(cid)
        if parked is None:
            return {"content": [{"type": "text", "text": f"Unknown or already completed call_id {cid}."}], "isError": True}
        return await_parked_call(sess, parked, stop_event=stop_event)

    if host_tool == PAGE_TOOL_NAME:
        return sess.read_stored_page(
            page_token=str(clean_args.get("page_token", "")).strip(),
            page_num=_safe_int(clean_args.get("page", 1), 1),
            grep_pat=str(clean_args.get("grep", "")),
        )

    if host_tool == CALL_TOOL_NAME:
        target = str(clean_args.get("name", "")).strip()
        sub_args = clean_args.get("arguments")
        if isinstance(sub_args, str):
            try:
                sub_args = json.loads(sub_args)
            except Exception:
                sub_args = {}
        if not isinstance(sub_args, dict):
            sub_args = {}
        sub_args.pop("toolSummary", None)

        b_res = EMBEDDED_MCP_HOST.execute_layer_b_tool(target, sub_args, set(active_user_tools), cwd=cwd)
        if b_res is not None:
            return {"content": [{"type": "text", "text": b_res["text"]}], "isError": b_res["is_error"]}

        with sess.lock:
            resolved = sess.mcp_to_host.get(target) or next((k for k in sess.allowed_tools if k.lower() == target.lower()), None)
        if not resolved or resolved in (AWAIT_TOOL_NAME, CALL_TOOL_NAME):
            return {
                "content": [{
                    "type": "text",
                    "text": (
                        f"Tool {target!r} is not active in this turn. "
                        f"Active tools: {', '.join(active_user_tools)}. "
                        f"For file listing or code search, call 'Bash' with {{\"command\": \"rg --files\"}} and 'Read'."
                    ),
                }],
                "isError": True,
            }
        if resolved == PAGE_TOOL_NAME:
            return sess.read_stored_page(
                page_token=str(sub_args.get("page_token", "")).strip(),
                page_num=_safe_int(sub_args.get("page", 1), 1),
                grep_pat=str(sub_args.get("grep", "")),
            )
        host_tool = resolved
        clean_args = sub_args

    p1_authed = bool(get_webkit_plane().status(max_age_sec=0.8).get("logged_in"))
    if host_tool in SUBAGENT_TOOLS and WORKER_POOL.idle_count() == 0 and not p1_authed:
        return {
            "content": [{
                "type": "text",
                "text": (
                    f"No secondary idle Web Worker is free to serve a nested {host_tool} subagent call without blocking this worker. "
                    f"Inspect files directly in this turn using 'Bash' (ls/find/rg) and 'Read' ({{'file_path': '/abs/path'}})."
                ),
            }],
            "isError": True,
        }

    with sess.lock:
        tool_schema = (sess.allowed_tools.get(host_tool) or {}).get("schema") or {}
    clean_args = normalize_host_tool_args(host_tool, clean_args, tool_schema, cwd=(cwd if explicit_cwd else ""))

    sig = f"{host_tool}:{json.dumps(clean_args, sort_keys=True, ensure_ascii=False)}"
    with sess.lock:
        if len(sess.recent_tool_sigs) >= 3 and all(x == sig for x in sess.recent_tool_sigs[-3:]):
            sess.recent_tool_sigs.clear()
            return {
                "content": [{"type": "text", "text": f"Loop circuit breaker: {host_tool} was called 4 times with identical arguments. Proceed with a different step."}],
                "isError": True,
            }
        sess.recent_tool_sigs.append(sig)
        if len(sess.recent_tool_sigs) > 6:
            sess.recent_tool_sigs = sess.recent_tool_sigs[-6:]

    parked = ParkedCall("toolu_" + random_hex(12), host_tool, clean_args)
    sess.enqueue_parked_call(parked)
    return await_parked_call(sess, parked, stop_event=stop_event)


def build_tenant_mcp_tools_list(sess: SessionState) -> List[dict]:
    with sess.lock:
        cat = list(sess.tools_catalog)
    return [
        {
            "name": t["mcp_name"],
            "description": f"DSH tool {t['name']}: {t['description']}",
            "inputSchema": t["parameters"],
        }
        for t in cat
    ]


def handle_shared_mcp_rpc(body: dict) -> dict:
    msg_id = body.get("id")
    rpc_method = body.get("method", "")
    params = body.get("params") or {}

    if rpc_method == "tools/list":
        session_token = str(params.get("session_token", "")).strip()
        sess = REGISTRY.get_by_token(session_token) if session_token else None
        if sess is not None:
            return {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": build_tenant_mcp_tools_list(sess)}}
        return {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": []}}

    if rpc_method != "tools/call":
        return {"jsonrpc": "2.0", "id": msg_id, "result": {}}

    tool_name = params.get("name", "")
    args = params.get("arguments") or {}

    if tool_name in ("dsh_wait_request", "dsh_await_request"):
        wid = str(args.get("worker_id", "")).strip()
        wmodel = str(args.get("worker_model", "gemini-app-carbon")).strip()
        return {"jsonrpc": "2.0", "id": msg_id, "result": WORKER_POOL.wait_for_prompt(wid, wmodel)}

    session_token = str(args.get("session_token") or params.get("session_token") or "").strip()
    sess = REGISTRY.get_by_token(session_token)
    if sess is None:
        return {
            "jsonrpc": "2.0", "id": msg_id,
            "result": {"content": [{"type": "text", "text": f"Unknown session_token: {session_token}"}], "isError": True},
        }

    if tool_name == "dsh_stream_chunk":
        with sess.lock:
            turn = sess.active_turn
        if turn is not None:
            if args.get("thinking"):
                turn.push_delta("thinking", str(args["thinking"]))
            if args.get("delta"):
                turn.push_delta("text", str(args["delta"]))
        return {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": "ACK"}], "isError": False}}

    if tool_name == "dsh_reply":
        content = str(args.get("content", ""))
        thinking = str(args.get("thinking", ""))
        wait_next = bool(args.get("wait_next", False))
        if wait_next:
            with sess.lock:
                sess.worker_waiting_next = True
        ok = sess.complete_reply(content, thinking, stream_mode="plane2_fifo_stream")
        if not wait_next or not ok:
            with sess.lock:
                sess.worker_waiting_next = False
            return {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": "Delivered."}], "isError": not ok}}
        try:
            nxt = sess.next_delta_q.get(timeout=18.0)
            if nxt.get("heartbeat"):
                return {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": "Delivered! Session idle; call dsh_wait_request."}], "isError": False}}
            ev = nxt.get("claimed")
            if ev is not None:
                ev.set()
            return {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": nxt["prompt"]}], "isError": False}}
        except queue.Empty:
            return {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": "Delivered! Session idle >18s; call dsh_wait_request."}], "isError": False}}
        finally:
            with sess.lock:
                sess.worker_waiting_next = False

    res = execute_session_mcp_tool(sess, tool_name, args)
    return {"jsonrpc": "2.0", "id": msg_id, "result": res}


def normalize_responses_body(body: dict) -> dict:
    out = dict(body)
    sys_parts = []
    if body.get("instructions") is not None:
        inst = _extract_content_text(body["instructions"]).strip()
        if inst:
            sys_parts.append(inst)

    inp = body.get("input", [])
    if isinstance(inp, str):
        inp = [{"type": "message", "role": "user", "content": inp}]
    elif inp is None:
        inp = []
    elif not isinstance(inp, list):
        raise ValueError("'input' must be a string or an array of input items")

    conv_msgs = []
    for it in inp:
        if isinstance(it, str):
            conv_msgs.append({"role": "user", "content": it})
            continue
        if not isinstance(it, dict):
            continue
        t = it.get("type") or ("message" if "role" in it else "")
        if t == "message":
            r = str(it.get("role", "user")).lower()
            raw_c = it.get("content", "")
            if r in ("system", "developer"):
                txt_sys = _extract_content_text(raw_c).strip()
                if txt_sys:
                    sys_parts.append(txt_sys)
            else:
                conv_msgs.append({"role": "assistant" if r == "assistant" else "user", "content": raw_c})
        elif t == "function_call":
            cid = it.get("call_id") or it.get("id") or ("call_" + random_hex(4))
            args_raw = it.get("arguments", "{}")
            if isinstance(args_raw, dict):
                args_obj = args_raw
            elif isinstance(args_raw, str):
                stripped_a = args_raw.strip()
                if not stripped_a:
                    args_obj = {}
                else:
                    try:
                        parsed = json.loads(stripped_a)
                    except Exception:
                        parsed = lexical_repair_json(stripped_a)
                    if isinstance(parsed, dict):
                        args_obj = parsed
                    elif parsed is not None:
                        args_obj = {"value": parsed}
                    else:
                        args_obj = {"_raw_arguments": args_raw}
            else:
                args_obj = {}
            conv_msgs.append({"role": "assistant", "content": [{"type": "tool_use", "id": cid, "name": it.get("name", ""), "input": args_obj}]})
        elif t == "function_call_output":
            out_val = it.get("output", "")
            out_txt = _extract_content_text(out_val) if isinstance(out_val, list) else ("" if out_val is None else str(out_val))
            conv_msgs.append({"role": "user", "content": [{"type": "tool_result", "tool_use_id": it.get("call_id", ""), "content": out_txt}]})
        elif t in ("input_text", "output_text"):
            conv_msgs.append({"role": "user", "content": str(it.get("text", ""))})
    if sys_parts:
        out["system"] = "\n\n".join(sys_parts)
    out["messages"] = conv_msgs
    return out


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<title>Dual-Plane v4.6 (4.6.0-clean-carbon) · 满血 4 Carbon High x 网页会话无痕自净控制台</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif; background: #f7f6f2; color: #1f1f1f; margin: 24px; max-width: 1220px; }
  h1 { font-size: 20px; margin: 0 0 6px 0; }
  .sub { color: #5f6368; font-size: 13px; margin-bottom: 16px; }
  .cards { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 16px; }
  .card { background: #ffffff; border: 1px solid #e0ded8; border-radius: 14px; padding: 14px 18px; min-width: 200px; flex: 1; }
  .k { font-size: 12px; color: #5f6368; }
  .v { font-size: 18px; font-weight: 600; margin-top: 4px; }
  .meta { font-size: 11.5px; color: #5f6368; margin-top: 5px; word-break: break-all; }
  .toolbar { background: #ffffff; border: 1px solid #e0ded8; border-radius: 14px; padding: 12px 18px; margin-bottom: 18px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
  .btn { cursor: pointer; border: 1px solid #cfd4dc; background: #f8f9fa; color: #1f1f1f; padding: 7px 12px; border-radius: 10px; font-size: 12.5px; }
  .btn.pri { background: #dcf1ff; border-color: #3186ff; color: #192967; font-weight: 500; }
  .btn.ok { background: #e6f4ea; border-color: #34a853; color: #137333; font-weight: 500; }
  .btn.warn { background: #fef7e0; border-color: #f29900; color: #b06000; font-weight: 500; }
  .toast { font-size: 12px; color: #188038; margin-left: 8px; font-weight: 500; }
  table { width: 100%; border-collapse: collapse; background: #ffffff; border-radius: 14px; overflow: hidden; border: 1px solid #e0ded8; }
  th, td { text-align: left; padding: 10px 14px; font-size: 13px; border-bottom: 1px solid #efede8; }
  th { background: #f2f0ea; color: #444; font-weight: 600; }
  .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; background: #dcf1ff; color: #192967; margin-right: 4px; }
  .ok { background: #e6f4ea; color: #137333; }
  .warn { background: #fef7e0; color: #b06000; }
  .err { background: #fce8e6; color: #c5221f; }
</style>
</head>
<body>
  <h1>Gemini.app x Claude Code 本地网关 (Dual-Plane v4.6 · 满血 4 Carbon 深度思考 + 无痕自净)</h1>
  <div class="sub">默认满血 4 Carbon High (防超时降智) · 旁路审查隔离即用即焚 · 网页侧边栏会话全自动清理 · 支持 Gemini.app 单会话守护接管</div>
  <div class="cards" id="cards"></div>
  <div class="toolbar">
    <button class="btn pri" onclick="copyAppReuseCmd()">复制 Gemini.app 单会话守护口令 (Plane 2)</button>
    <button class="btn ok" onclick="openWebLogin()">登录同账号常驻引擎 (Plane 1)</button>
    <button class="btn warn" onclick="cleanWebChats()">立即清理网关网页会话</button>
    <button class="btn" onclick="switchWebAccount()">切换 / 重绑 Google 账号</button>
    <button class="btn" onclick="pinAccountPrompt()">锁定指定邮箱防串号</button>
    <button class="btn" onclick="copyCmd('/model gemini-app-carbon', '已复制切换 Carbon 命令')">/model Carbon</button>
    <span class="toast" id="toast"></span>
  </div>
  <table>
    <thead><tr><th>时间</th><th>生效模型 / 上游路由</th><th>会话调度</th><th>执行平面 / 流通道</th><th>思考字数</th><th>TTFT 首字</th><th>总耗时</th><th>净上下文</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
<script>
let latestState = {};
function flashToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  setTimeout(() => { if (el.textContent === msg) el.textContent = ''; }, 3200);
}
async function openWebLogin() {
  const r = await fetch('/_admin/open_login', {method: 'POST', body: '{}'});
  const d = await r.json();
  flashToast(d.ok ? '已打开登录窗，请登录与 Gemini.app 相同的 Google 邮箱！' : ('唤起失败: ' + (d.error || '')));
}
async function switchWebAccount() {
  const r = await fetch('/_admin/switch_account', {method: 'POST', body: '{}'});
  const d = await r.json();
  flashToast(d.ok ? '已清空旧网页会话并打开换绑窗口！' : ('切换失败: ' + (d.error || '')));
}
async function pinAccountPrompt() {
  const cur = ((latestState.account_alignment || {}).pinned_account) || ((latestState.webkit_plane || {}).account) || '';
  const val = window.prompt('输入要与 Gemini.app 对齐锁定的 Google 邮箱（留空代表解除锁定）：', cur);
  if (val === null) return;
  const r = await fetch('/_admin/pin_account', {method: 'POST', body: JSON.stringify({account: val.trim()})});
  const d = await r.json();
  flashToast(d.pinned_account ? ('已锁定对齐邮箱: ' + d.pinned_account) : '已解除邮箱锁定');
  refresh();
}
async function cleanWebChats() {
  const r = await fetch('/_admin/clean_chats', {method: 'POST', body: '{}'});
  const d = await r.json();
  flashToast(d.ok ? (`已从网页侧边栏清理 ${d.deleted_count || 0} 个网关会话！`) : ('清理失败: ' + (d.error || '')));
  refresh();
}
function copyAppReuseCmd() {
  const ipc = latestState.ipc_dir || '/tmp/dsh_gemini_ipc_18896';
  const txt = `请在当前 Gemini.app Task 或 Antigravity 会话中执行常驻守护值守命令（RequiresNetworkAccess=false）：/usr/bin/python3 /tmp/dsh_worker_cli.py daemon --ipc-dir ${ipc} --model gemini-app-carbon --max-idle 1800`;
  navigator.clipboard.writeText(txt).then(() => flashToast('已复制守护口令！粘贴进 Gemini.app 的 1 个 Task（省会话/走 Web 额度）或 Antigravity 面板（零 Web 记录/走 agy）即可接管'));
}
function copyCmd(txt, okMsg) {
  navigator.clipboard.writeText(txt).then(() => flashToast(okMsg));
}
async function refresh() {
  const r = await fetch('/health');
  const d = await r.json();
  latestState = d;
  const p1 = d.webkit_plane || {};
  const align = d.account_alignment || {};
  const acct = p1.account || '';
  const pinned = align.pinned_account || '';
  let p1Badge = '<span class="pill warn">未启用 (可零登录走 Plane 2)</span>';
  if (p1.logged_in) {
    if (pinned && acct && pinned.toLowerCase() !== acct.toLowerCase()) {
      p1Badge = `<span class="pill err">账号不匹配已拦截 (${acct})</span>`;
    } else {
      p1Badge = `<span class="pill ok">已对齐就绪 ${acct ? '(' + acct + ')' : ''}</span>`;
    }
  }
  const wCount = d.recent_web_workers || d.idle_task_workers || 0;
  const p2Badge = wCount > 0 ? `<span class="pill ok">${wCount} 节点直连复用 Gemini.app</span>` : '<span class="pill">随时热插拔接管</span>';
  const mcpInfo = d.embedded_local_mcp || {};
  document.getElementById('cards').innerHTML = `
    <div class="card"><div class="k">Plane 2 直连复用 Gemini.app 会话</div><div class="v">${p2Badge}</div><div class="meta">零二次登录 · 直接使用 App 当前登录态</div></div>
    <div class="card"><div class="k">Plane 1 常驻引擎 & 账号对齐锁</div><div class="v">${p1Badge}</div><div class="meta">锁定邮箱: ${pinned || '未锁定 (自动识别)'}</div></div>
    <div class="card"><div class="k">免公网双轨本地 MCP 池</div><div class="v"><span class="pill ok">Layer A + ${mcpInfo.server_count || 0} 个 stdio 服务</span></div><div class="meta">单向出站 443 · 零公网穿透</div></div>
    <div class="card"><div class="k">分页、水位与无痕自净</div><div class="v">${(d.watermark_tokens/1000).toFixed(0)}K / ${(d.hard_ceiling_tokens/1000).toFixed(0)}K</div><div class="meta">已自动清理网页会话: ${d.auto_cleaned_web_chats || 0} 个</div></div>
  `;
  const wmK = ((d.watermark_tokens || 192000) / 1000).toFixed(0);
  const items = (d.telemetry || []).slice().reverse();
  document.getElementById('rows').innerHTML = items.length ? items.map(t => `
    <tr>
      <td>${t.ts || '-'}</td>
      <td><b>${t.model}</b> / <span class="pill">${t.tier}</span> ${t.status === 'error' ? '<span class="pill err">ERROR</span>' : (t.routed_model ? '<span class="pill ok">' + t.routed_model + '</span>' : '')}</td>
      <td>${t.mode}</td>
      <td>${t.status === 'error' ? ('<span style="color:#c5221f;font-size:12px">' + (t.error || t.stream_mode || 'error') + '</span>') : (t.stream_mode || '-')}</td>
      <td>${t.think_chars || 0} chars</td>
      <td>${t.ttft_ms} ms</td>
      <td>${t.elapsed_ms} ms</td>
      <td>${((t.ctx_tokens||0)/1000).toFixed(1)}K / ${wmK}K</td>
    </tr>`).join('') : '<tr><td colspan="8" style="color:#777">Dual-Plane v4.6 网关已就绪：默认满血 4 Carbon High + 网页会话自动无痕清理...</td></tr>';
}
refresh();
setInterval(refresh, 2500);
</script>
</body>
</html>
"""


def _format_model_object(m: dict) -> dict:
    return {
        "type": "model",
        "object": "model",
        "id": m["id"],
        "display_name": m["label"],
        "description": f"{m['label']} · Dual-Plane Pure Web Quota · Zero-Public-IP Local MCP",
        "created_at": "2026-11-16T00:00:00Z",
        "context_window": m["context_window"],
        "owned_by": m["family"],
    }


def build_models_catalog_response() -> dict:
    data = [_format_model_object(m) for m in SUPPORTED_MODELS]
    return {
        "object": "list",
        "data": data,
        "has_more": False,
        "first_id": data[0]["id"],
        "last_id": data[-1]["id"],
    }


def build_readyz_snapshot() -> dict:
    p1_st = get_webkit_plane().status(max_age_sec=0.8)
    p1_ready = bool(p1_st.get("ready") or p1_st.get("logged_in"))
    p2_workers = max(WORKER_POOL.idle_count(), WORKER_POOL.recent_worker_count())
    ready = bool(p1_ready or p2_workers > 0 or UPSTREAM_MODE == "mcp_task")
    return {
        "ok": True,
        "ready": ready,
        "version": GATEWAY_VERSION,
        "plane1_ready": p1_ready,
        "plane1_page_loaded": bool(p1_st.get("page_loaded")),
        "plane2_workers": p2_workers,
    }


def build_health_snapshot() -> dict:
    EMBEDDED_MCP_HOST.sync_servers()
    p1_st = get_webkit_plane().status(max_age_sec=0.8)
    active_acct = str(p1_st.get("account", "")).strip()
    aligned, pinned_acct, _ = ACCOUNT_STORE.check_alignment(active_acct)
    align_state = ACCOUNT_STORE.load()
    with REGISTRY.lock:
        telem = list(REGISTRY.telemetry[-40:])
        sess_summary = []
        for s in REGISTRY.by_token.values():
            with s.lock:
                sess_summary.append({
                    "conv_key": s.conv_key,
                    "model": s.model_id,
                    "tier": s.tier,
                    "synced_msgs": s.synced_msg_count,
                    "net_tokens": s.net_context_tokens,
                    "web_cursor_active": bool(any(s.web_cursor)),
                    "reuses": s.reuse_count,
                    "rollovers": s.rollover_count,
                })
        sess_count = len(REGISTRY.by_token)
    return {
        "ok": True,
        "version": GATEWAY_VERSION,
        "script_mtime": STARTUP_RUNTIME_FILES.get("gateway_poc.py", {}).get("mtime", -1),
        "runtime_files": STARTUP_RUNTIME_FILES,
        "upstream": UPSTREAM_MODE,
        "acp_spawned": False,
        "ipc_dir": IPC_DIR,
        "webkit_sock": WEBKIT_SOCK,
        "webkit_plane": p1_st,
        "account_alignment": {
            "aligned": aligned,
            "pinned_account": pinned_acct,
            "active_webkit_account": active_acct,
            "last_seen_account": align_state.get("last_seen_account", ""),
        },
        "embedded_local_mcp": EMBEDDED_MCP_HOST.status_summary(),
        "watermark_tokens": SOFT_WATERMARK_TOKENS,
        "hard_ceiling_tokens": HARD_CEILING_TOKENS,
        "max_tool_chars": MAX_TOOL_RESULT_CHARS,
        "web_rpc_page_chars": WEB_RPC_PAGE_CHARS,
        "auto_cleaned_web_chats": getattr(get_webkit_plane(), "deleted_conversations_count", 0),
        "session_idle_clean_sec": SESSION_IDLE_CLEAN_SEC,
        "idle_task_workers": WORKER_POOL.idle_count(),
        "recent_web_workers": WORKER_POOL.recent_worker_count(),
        "idle_worker_models": WORKER_POOL.idle_worker_models(),
        "sessions": sess_count,
        "session_states": sess_summary,
        "telemetry": telem,
    }


class GatewayHTTPHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        self.timeout = 30.0
        self._requests_handled = 0
        super().setup()

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError, TimeoutError, socket.timeout):
            pass
        except OSError as exc:
            if exc.errno in DISCONNECT_ERRNOS or exc.errno == getattr(errno, "ECONNABORTED", 53):
                return
            raise

    def log_error(self, fmt, *args):
        if fmt == "Request timed out: %r":
            return
        super().log_error(fmt, *args)

    def _set_socket_timeout(self, timeout_sec: float) -> None:
        conn = getattr(self, "connection", None)
        if isinstance(conn, socket.socket):
            try:
                conn.settimeout(timeout_sec)
            except OSError:
                pass

    def _is_client_disconnected(self) -> bool:
        conn = getattr(self, "connection", None)
        if not isinstance(conn, socket.socket):
            return False
        try:
            r, _, _ = select.select([conn], [], [], 0.0)
            if not r:
                return False
            peeked = conn.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
            return len(peeked) == 0
        except BlockingIOError:
            return False
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return False
            return True

    def _start_disconnect_monitor(
        self,
        cancel_event: threading.Event,
        stop_ev: threading.Event,
        sse: Optional[SafeSSEWriter] = None,
    ) -> None:
        def _watch() -> None:
            while not stop_ev.wait(0.2):
                if cancel_event.is_set():
                    if sse is not None:
                        sse.mark_closed()
                    return
                if sse is not None and sse.closed:
                    cancel_event.set()
                    return
                if self._is_client_disconnected():
                    cancel_event.set()
                    if sse is not None:
                        sse.mark_closed()
                    return

        threading.Thread(target=_watch, daemon=True).start()

    def log_request(self, code="-", size="-"):
        if os.environ.get("DSH_QUIET") == "1":
            return
        try:
            code_int = int(code)
        except Exception:
            code_int = 200
        req_path = (self.path or "").split("?", 1)[0]
        if code_int < 400 and req_path in QUIET_POLL_PATHS:
            return
        super().log_request(code, size)

    def log_message(self, fmt, *args):
        if os.environ.get("DSH_QUIET") != "1":
            sys.stderr.write("[gateway] " + (fmt % args) + "\n")

    def _cors_headers(self, allow_all: bool = True) -> Dict[str, str]:
        if not allow_all:
            return {}
        return {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, HEAD, OPTIONS, DELETE",
            "Access-Control-Allow-Headers": (
                "Content-Type, Authorization, x-api-key, anthropic-version, anthropic-beta, "
                "x-session-id, x-claude-code-session-id, x-claude-code-agent-id, "
                "x-enable-local-mcp, x-upstream-mode, x-admin-token"
            ),
            "Access-Control-Max-Age": "86400",
        }

    def _apply_connection_header(self, force_close: bool = False) -> None:
        conn_hdr = str(self.headers.get("Connection", "")).lower()
        if force_close or "close" in conn_hdr or self.request_version == "HTTP/1.0":
            self.send_header("Connection", "close")
            self.close_connection = True
        else:
            self.send_header("Connection", "keep-alive")
            self.close_connection = False

    def _extract_bearer_or_key(self, hdrs: dict, extra_header: str = "") -> str:
        lower_hdrs = {k.lower(): str(v).strip() for k, v in (hdrs or {}).items()}
        if extra_header and lower_hdrs.get(extra_header.lower()):
            return lower_hdrs[extra_header.lower()]
        if lower_hdrs.get("x-api-key"):
            return lower_hdrs["x-api-key"]
        auth = lower_hdrs.get("authorization", "")
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        return ""

    def _check_api_auth(self, hdrs: dict, is_anthropic: bool) -> bool:
        expected = os.environ.get("DSH_API_KEY", "").strip()
        if not expected:
            return True
        provided = self._extract_bearer_or_key(hdrs)
        if provided and hmac.compare_digest(provided, expected):
            return True
        self._send_api_error(
            401,
            "authentication_error" if is_anthropic else "invalid_request_error",
            "Invalid or missing API key",
            is_anthropic=is_anthropic,
            api_code="invalid_api_key",
        )
        return False

    def _check_admin_guard(self, hdrs: dict) -> bool:
        lower_hdrs = {k.lower(): str(v).strip() for k, v in (hdrs or {}).items()}
        origin = lower_hdrs.get("origin", "")
        sec_fetch = lower_hdrs.get("sec-fetch-site", "").lower()
        if sec_fetch == "cross-site":
            self._send_json(403, {"ok": False, "error": "Cross-site admin request blocked"})
            return False
        if origin:
            if origin.lower() == "null":
                self._send_json(403, {"ok": False, "error": "Null-origin admin request blocked"})
                return False
            parsed = urllib.parse.urlparse(origin)
            host = (parsed.hostname or "").lower()
            if parsed.scheme not in ("http", "https") or host not in ("127.0.0.1", "localhost", "::1"):
                self._send_json(403, {"ok": False, "error": f"Cross-origin admin request blocked: {origin}"})
                return False
            req_host_hdr = lower_hdrs.get("host", "")
            req_port = None
            if ":" in req_host_hdr and not req_host_hdr.endswith("]"):
                try:
                    req_port = int(req_host_hdr.rsplit(":", 1)[1])
                except ValueError:
                    req_port = None
            expected_port = req_port or HTTP_PORT
            if parsed.port is not None and parsed.port != expected_port:
                self._send_json(403, {"ok": False, "error": f"Cross-port admin origin blocked: {origin}"})
                return False

        admin_tok = os.environ.get("DSH_ADMIN_TOKEN", "").strip()
        if admin_tok:
            provided = self._extract_bearer_or_key(hdrs, extra_header="x-admin-token")
            if not provided or not hmac.compare_digest(provided, admin_tok):
                self._send_json(401, {"ok": False, "error": "Invalid or missing admin token"})
                return False
        return True

    def _read_exact_bytes(self, n: int) -> bytes:
        chunks: List[bytes] = []
        rem = n
        while rem > 0:
            part = self.rfile.read(min(rem, 65536))
            if not part:
                raise ValueError("Incomplete HTTP request body")
            chunks.append(part)
            rem -= len(part)
        return b"".join(chunks)

    def _read_chunked_body(self) -> bytes:
        buf = bytearray()
        while True:
            sz_line = self.rfile.readline(4096)
            if not sz_line:
                raise ValueError("Truncated chunked request body")
            sz_hex = sz_line.split(b";", 1)[0].strip()
            try:
                chunk_len = int(sz_hex, 16)
            except ValueError as exc:
                raise ValueError(f"Invalid chunk size line: {sz_line!r}") from exc
            if chunk_len < 0:
                raise ValueError("Negative chunk size")
            if len(buf) + chunk_len > MAX_REQUEST_BODY_BYTES:
                raise PayloadTooLargeError("Request body exceeds 32 MB limit")
            if chunk_len == 0:
                while True:
                    trailer = self.rfile.readline(4096)
                    if not trailer or trailer in (b"\r\n", b"\n"):
                        break
                break
            buf.extend(self._read_exact_bytes(chunk_len))
            crlf = self.rfile.read(1)
            if crlf == b"\r":
                self.rfile.read(1)
            elif crlf != b"\n":
                raise ValueError("Malformed chunked frame terminator")
        return bytes(buf)

    def _read_raw_and_json(self) -> Tuple[bytes, dict]:
        te = str(self.headers.get("Transfer-Encoding", "")).lower()
        if "chunked" in te:
            raw = self._read_chunked_body()
            if not raw:
                raw = b"{}"
        else:
            raw_len = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_len)
            except ValueError as exc:
                raise ValueError(f"Invalid Content-Length: {raw_len!r}") from exc
            if length < 0:
                raise ValueError("Negative Content-Length")
            if length > MAX_REQUEST_BODY_BYTES:
                raise PayloadTooLargeError("Request body exceeds 32 MB limit")
            raw = self._read_exact_bytes(length) if length > 0 else b"{}"
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("JSON request body must be an object")
        return raw, parsed

    def _send_json(self, code: int, obj: dict, extra_headers: dict = None, force_close: bool = False) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self._apply_connection_header(force_close=force_close)
            if extra_headers:
                for k, v in extra_headers.items():
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except OSError as exc:
            if exc.errno not in DISCONNECT_ERRNOS:
                raise

    def _send_api_error(
        self,
        code: int,
        err_type: str,
        message: str,
        is_anthropic: bool,
        api_code: Optional[str] = None,
        param: Optional[str] = None,
    ) -> None:
        if is_anthropic:
            payload = {"type": "error", "error": {"type": err_type, "message": message}}
        else:
            payload = {"error": {"message": message, "type": err_type, "param": param, "code": api_code}}
        self._send_json(code, payload, extra_headers=self._cors_headers(True))

    def _classify_upstream_error(self, exc: Exception) -> Tuple[int, str, str, Optional[str]]:
        if isinstance(exc, GatewayHTTPError):
            return exc.status_code, exc.error_type, exc.message, exc.code
        if isinstance(exc, (ValueError, TypeError)):
            return 400, "invalid_request_error", str(exc), "invalid_request"
        msg = str(exc)
        low = msg.lower()
        if (
            "429" in msg
            or "rate_limit" in low
            or "rate limit" in low
            or "resource_exhausted" in low
            or "code=1037" in low
            or "code=8 " in low
        ):
            return 429, "rate_limit_error", msg, "rate_limit_exceeded"
        if "account_mismatch" in low:
            return 403, "permission_error", msg, "account_mismatch"
        return 503, "api_error", msg, "upstream_unavailable"

    def do_OPTIONS(self):
        p = self.path.split("?")[0]
        hdrs = {k: v for k, v in self.headers.items()}
        if p.startswith("/_admin/"):
            if not self._check_admin_guard(hdrs):
                return
            extra = {}
        else:
            extra = self._cors_headers(True)
        try:
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self._apply_connection_header()
            for k, v in extra.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.flush()
        except OSError:
            pass

    def do_HEAD(self):
        p = self.path.split("?")[0]
        if p in ("/api/hello", "/health", "/healthz", "/v1/health", "/readyz", "/"):
            try:
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self._apply_connection_header()
                self.end_headers()
                self.wfile.flush()
            except OSError:
                pass
            return
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self._apply_connection_header()
        self.end_headers()

    def do_DELETE(self):
        p = self.path.split("?")[0]
        if p.startswith("/mcp/"):
            try:
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self._apply_connection_header()
                self.end_headers()
                self.wfile.flush()
            except OSError:
                pass
            return
        self._send_json(404, {"error": "not found"})

    def do_GET(self):
        self._requests_handled = getattr(self, "_requests_handled", 0) + 1
        self._set_socket_timeout(30.0)
        p = self.path.split("?")[0]
        hdrs = {k: v for k, v in self.headers.items()}
        if p in ("/", "/dashboard"):
            raw = DASHBOARD_HTML.encode("utf-8")
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self._apply_connection_header()
                self.end_headers()
                self.wfile.write(raw)
                self.wfile.flush()
            except OSError:
                pass
            return
        if p == "/api/hello":
            self._send_json(200, {"ok": True, "message": "hello", "version": GATEWAY_VERSION})
            return
        if p == "/readyz":
            self._send_json(200, build_readyz_snapshot())
            return
        if p == "/_admin/state":
            if not self._check_admin_guard(hdrs):
                return
            self._send_json(200, build_health_snapshot())
            return
        if p in ("/health", "/healthz", "/v1/health"):
            self._send_json(200, build_health_snapshot())
            return
        if p == "/v1/models":
            if not self._check_api_auth(hdrs, is_anthropic=False):
                return
            self._send_json(200, build_models_catalog_response(), extra_headers=self._cors_headers(True))
            return
        if p.startswith("/v1/models/"):
            if not self._check_api_auth(hdrs, is_anthropic=False):
                return
            req_mid = urllib.parse.unquote(p[len("/v1/models/"):]).strip()
            info = MODEL_BY_ID.get(req_mid.lower())
            if info is None:
                self._send_api_error(404, "invalid_request_error", f"The model '{req_mid}' does not exist", is_anthropic=False, api_code="model_not_found", param="model")
                return
            self._send_json(200, _format_model_object(info), extra_headers=self._cors_headers(True))
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        self._requests_handled = getattr(self, "_requests_handled", 0) + 1
        self._set_socket_timeout(30.0)
        p = self.path.split("?")[0]
        hdrs = {k: v for k, v in self.headers.items()}

        if p.startswith("/_admin/"):
            if not self._check_admin_guard(hdrs):
                return
        elif p.startswith("/v1/"):
            is_anth = p.startswith("/v1/messages")
            if not self._check_api_auth(hdrs, is_anthropic=is_anth):
                return

        try:
            _, body = self._read_raw_and_json()
        except PayloadTooLargeError as exc:
            is_anth = p.startswith("/v1/messages")
            self._send_api_error(413, "invalid_request_error", str(exc), is_anthropic=is_anth, api_code="payload_too_large")
            return
        except Exception as exc:
            if p == "/mcp/shared":
                self._send_json(400, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
            else:
                is_anth = p.startswith("/v1/messages")
                self._send_api_error(400, "invalid_request_error", f"Invalid JSON request body: {exc}", is_anthropic=is_anth, api_code="invalid_json")
            return

        # Lift socket timeout from 30s header/body read protection to 600s during upstream inference
        # (Deep Think / Carbon turns can exceed 360s), then restore 30s for idle keep-alive waits.
        self._set_socket_timeout(600.0)
        try:
            self._dispatch_post(p, hdrs, body)
        finally:
            self._set_socket_timeout(30.0)

    def _dispatch_post(self, p: str, hdrs: dict, body: dict):

        if p == "/_admin/shutdown":
            if os.environ.get("DSH_KEEP_CHATS_ON_EXIT") != "1":
                try:
                    REGISTRY.clean_all_web_chats()
                except Exception:
                    pass
            WORKER_POOL.drain_for_restart()
            EMBEDDED_MCP_HOST.shutdown_all()
            self._send_json(200, {"ok": True}, force_close=True)
            threading.Timer(0.12, lambda: os._exit(0)).start()
            return

        if p == "/_admin/clean_chats":
            self._send_json(200, REGISTRY.clean_all_web_chats())
            return

        if p == "/_admin/open_login":
            self._send_json(200, get_webkit_plane().open_login_window())
            return

        if p == "/_admin/switch_account":
            self._send_json(200, get_webkit_plane().switch_account())
            return

        if p == "/_admin/pin_account":
            acct = str(body.get("account", "")).strip()
            saved = ACCOUNT_STORE.set_pinned(acct)
            self._send_json(200, {"ok": True, **saved})
            return

        if p == "/mcp/shared":
            self._send_json(200, handle_shared_mcp_rpc(body))
            return

        if p == "/v1/messages/count_tokens":
            self._send_json(200, {"input_tokens": estimate_count_tokens(body)}, extra_headers=self._cors_headers(True))
            return

        if p == "/v1/messages":
            self._serve_anthropic_messages(hdrs, body)
            return

        if p == "/v1/chat/completions":
            self._serve_openai_chat(hdrs, body)
            return

        if p == "/v1/responses":
            self._serve_openai_responses(hdrs, body)
            return

        self._send_json(404, {"error": "not found"})

    def _validate_messages_list(self, raw_messages: Any, allow_empty: bool = False) -> List[dict]:
        if not isinstance(raw_messages, list):
            raise ValueError("'messages' must be an array")
        if not allow_empty and len(raw_messages) == 0:
            raise ValueError("'messages' must be a non-empty array")
        for idx, m in enumerate(raw_messages):
            if not isinstance(m, dict):
                raise ValueError(f"messages[{idx}] must be an object")
            role = m.get("role")
            if not isinstance(role, str) or not role.strip():
                raise ValueError(f"messages[{idx}].role must be a non-empty string")
        return raw_messages

    def _send_sse_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> bool:
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("X-Accel-Buffering", "no")
            self._apply_connection_header(force_close=True)
            for k, v in self._cors_headers(True).items():
                self.send_header(k, v)
            if extra_headers:
                for k, v in extra_headers.items():
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.flush()
            return True
        except OSError:
            return False

    def _serve_anthropic_messages(self, hdrs: dict, body: dict):
        try:
            req_model = body.get("model", "gemini-app-carbon")
            model_info, tier = resolve_model_and_tier(req_model, body)
            raw_messages = self._validate_messages_list(body.get("messages"), allow_empty=False)
            tools = body.get("tools") or []
            if not isinstance(tools, list):
                raise ValueError("'tools' must be an array")
            raw_max_tok = body.get("max_tokens")
            if raw_max_tok is not None:
                if isinstance(raw_max_tok, bool) or not isinstance(raw_max_tok, int) or raw_max_tok < 1:
                    raise ValueError("'max_tokens' must be a positive integer")
                max_tok = raw_max_tok
            else:
                max_tok = 16384
            stop_seqs = extract_stop_sequences(body)
            tool_choice_mode, _ = parse_tool_choice(body, tools)
            _resolve_effective_upstream(hdrs)
            bad_mm = find_unsupported_multimodal_block(raw_messages)
            if bad_mm:
                raise ValueError(f"Unsupported multimodal content block type {bad_mm!r}: only text and tool blocks are supported.")
        except (ValueError, TypeError) as exc:
            self._send_api_error(400, "invalid_request_error", str(exc), is_anthropic=True, api_code="invalid_request")
            return

        model_id = model_info["id"]
        stream = bool(body.get("stream", False))
        allow_thinking = should_emit_thinking(body, model_info, tier, is_openai=False)
        msg_id = "msg_" + random_hex(12)
        sig_value = f"dsh-sig-{short_sha256(msg_id + ':' + model_id)}"

        sys_raw = body.get("system", "")
        base_sys = _extract_content_text(sys_raw) if isinstance(sys_raw, list) else ("" if sys_raw is None else str(sys_raw))
        sys_text, messages = fold_mid_stream_system_messages(base_sys, raw_messages)
        active_tools = [] if tool_choice_mode == "none" else tools

        fast_txt = classify_fast_utility_call(hdrs, sys_text, active_tools, messages, max_tok)
        if fast_txt is not None:
            if not stream:
                self._send_json(200, {
                    "id": msg_id, "type": "message", "role": "assistant", "model": req_model,
                    "content": [{"type": "text", "text": fast_txt}],
                    "stop_reason": "end_turn", "stop_sequence": None,
                    "usage": {"input_tokens": 10, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 1},
                }, extra_headers={"X-Served-Model": f"{model_id}/{tier}", "X-Turn-Mode": "fast_path", **self._cors_headers(True)})
                return
            if not self._send_sse_headers({"X-Served-Model": f"{model_id}/{tier}"}):
                return
            sse_fast = SafeSSEWriter(self.wfile)
            for ev, obj in [
                ("message_start", {"type": "message_start", "message": {"id": msg_id, "type": "message", "role": "assistant", "model": req_model, "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 10, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 0}}}),
                ("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
                ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": fast_txt}}),
                ("content_block_stop", {"type": "content_block_stop", "index": 0}),
                ("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 1}}),
                ("message_stop", {"type": "message_stop"}),
            ]:
                if not sse_fast.write_event(ev, obj):
                    return
            return

        in_tok = estimate_count_tokens(body)

        if not stream:
            cancel_event = threading.Event()
            stop_mon = threading.Event()
            self._start_disconnect_monitor(cancel_event, stop_mon)
            try:
                outcome = execute_turn(hdrs, body, model_info, tier, sys_text, messages, active_tools, is_openai=False, cancel_event=cancel_event)
            except RequestCancelledError:
                return
            except Exception as exc:
                st_code, err_t, err_m, err_c = self._classify_upstream_error(exc)
                self._send_api_error(st_code, err_t, err_m, is_anthropic=True, api_code=err_c)
                return
            finally:
                stop_mon.set()

            final_text = str(outcome.get("text") or "")
            matched_stop: Optional[str] = None
            if stop_seqs and not outcome.get("tool_calls"):
                final_text, matched_stop = apply_stop_sequences_to_text(final_text, stop_seqs)

            cblocks = []
            if allow_thinking and outcome.get("thinking"):
                cblocks.append({"type": "thinking", "thinking": outcome["thinking"], "signature": sig_value})
            if final_text:
                cblocks.append({"type": "text", "text": final_text})
            for tc in outcome.get("tool_calls", []):
                cblocks.append({"type": "tool_use", "id": tc["id"], "name": tc["name"], "input": tc["input"]})
            if not cblocks:
                cblocks.append({"type": "text", "text": ""})

            final_stop_reason = "stop_sequence" if matched_stop is not None else outcome["stop_reason"]
            if final_stop_reason == "tool_use" and not outcome.get("tool_calls"):
                final_stop_reason = "max_tokens" if outcome.get("truncated_raw_stream") else "end_turn"

            adj_outcome = dict(outcome, text=final_text)
            cached_tok = int(outcome.get("cached_tokens", 0))
            uncached_in = max(1, in_tok - cached_tok)
            out_tok = estimate_output_tokens(adj_outcome, include_thinking=allow_thinking)
            self._send_json(200, {
                "id": msg_id, "type": "message", "role": "assistant", "model": req_model,
                "content": cblocks, "stop_reason": final_stop_reason, "stop_sequence": matched_stop,
                "usage": {
                    "input_tokens": uncached_in,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": cached_tok,
                    "output_tokens": out_tok,
                },
            }, extra_headers={
                "X-Served-Model": f"{model_id}/{tier}",
                "X-Stream-Mode": outcome.get("stream_mode", ""),
                "X-Turn-Mode": outcome.get("turn_mode", ""),
                **self._cors_headers(True),
            })
            return

        # Streaming Anthropic SSE
        cancel_event = threading.Event()
        stop_ping = threading.Event()
        stream_opened_ev = threading.Event()
        sse = SafeSSEWriter(self.wfile, on_close=cancel_event.set)
        sse_lock = threading.Lock()
        stop_filter = StopSequenceFilter(stop_seqs)
        state: Dict[str, Any] = {
            "started": False,
            "cur_type": "",
            "next_idx": 0,
            "streamed_tool_ids": set(),
            "open_failed": False,
        }

        def close_block_locked() -> None:
            ct = state["cur_type"]
            if ct == "thinking":
                sse.write_event("content_block_delta", {
                    "type": "content_block_delta", "index": state["next_idx"] - 1,
                    "delta": {"type": "signature_delta", "signature": sig_value},
                })
                sse.write_event("content_block_stop", {"type": "content_block_stop", "index": state["next_idx"] - 1})
            elif ct in ("text", "tool_use"):
                sse.write_event("content_block_stop", {"type": "content_block_stop", "index": state["next_idx"] - 1})
            state["cur_type"] = ""

        def emit_text_locked(txt_val: str) -> None:
            if not txt_val:
                return
            if state["cur_type"] != "text":
                close_block_locked()
                state["cur_type"] = "text"
                idx = state["next_idx"]
                state["next_idx"] += 1
                sse.write_event("content_block_start", {
                    "type": "content_block_start",
                    "index": idx,
                    "content_block": {"type": "text", "text": ""},
                })
            sse.write_event("content_block_delta", {
                "type": "content_block_delta",
                "index": state["next_idx"] - 1,
                "delta": {"type": "text_delta", "text": txt_val},
            })

        def on_stream_open(open_meta: dict) -> None:
            with sse_lock:
                if state["started"] or state["open_failed"]:
                    return
                if not self._send_sse_headers({"X-Served-Model": f"{model_id}/{tier}"}):
                    state["open_failed"] = True
                    cancel_event.set()
                    sse.mark_closed()
                    return
                init_cached = int(open_meta.get("cached_tokens", 0))
                init_uncached = max(1, in_tok - init_cached)
                state["started"] = True
                sse.write_event("message_start", {
                    "type": "message_start",
                    "message": {
                        "id": msg_id, "type": "message", "role": "assistant", "model": req_model,
                        "content": [], "stop_reason": None, "stop_sequence": None,
                        "usage": {
                            "input_tokens": init_uncached,
                            "cache_creation_input_tokens": 0,
                            "cache_read_input_tokens": init_cached,
                            "output_tokens": 0,
                        },
                    },
                })
                stream_opened_ev.set()

            def ping_loop() -> None:
                while not stop_ping.wait(6.0):
                    if sse.closed or cancel_event.is_set():
                        stop_ping.set()
                        return
                    with sse_lock:
                        if state["started"]:
                            ok = sse.write_event("ping", {"type": "ping"})
                            if not ok:
                                stop_ping.set()
                                return

            threading.Thread(target=ping_loop, daemon=True).start()

        self._start_disconnect_monitor(cancel_event, stop_ping, sse=sse)

        def on_live_delta(chunk: dict) -> None:
            if sse.closed or cancel_event.is_set():
                return
            kind = chunk["type"]
            if kind == "thinking" and not allow_thinking:
                return
            with sse_lock:
                if not state["started"] or sse.closed:
                    return
                if kind == "thinking":
                    val = chunk.get("content", "")
                    if not val:
                        return
                    if state["cur_type"] != "thinking":
                        close_block_locked()
                        state["cur_type"] = "thinking"
                        idx = state["next_idx"]
                        state["next_idx"] += 1
                        sse.write_event("content_block_start", {
                            "type": "content_block_start",
                            "index": idx,
                            "content_block": {"type": "thinking", "thinking": ""},
                        })
                    sse.write_event("content_block_delta", {
                        "type": "content_block_delta",
                        "index": state["next_idx"] - 1,
                        "delta": {"type": "thinking_delta", "thinking": val},
                    })
                elif kind == "text":
                    val = chunk.get("content", "")
                    if not val:
                        return
                    emit_val, _ = stop_filter.feed(val)
                    if emit_val:
                        emit_text_locked(emit_val)
                elif kind == "tool_start":
                    if stop_filter.matched is not None:
                        return
                    rem_txt, _ = stop_filter.flush()
                    if rem_txt:
                        emit_text_locked(rem_txt)
                    close_block_locked()
                    tc_id = chunk["id"]
                    tc_name = chunk["name"]
                    state["cur_type"] = "tool_use"
                    state["streamed_tool_ids"].add(tc_id)
                    idx = state["next_idx"]
                    state["next_idx"] += 1
                    sse.write_event("content_block_start", {
                        "type": "content_block_start",
                        "index": idx,
                        "content_block": {"type": "tool_use", "id": tc_id, "name": tc_name, "input": {}},
                    })
                elif kind == "tool_delta":
                    pjson = chunk.get("partial_json", "")
                    if state["cur_type"] == "tool_use" and pjson:
                        sse.write_event("content_block_delta", {
                            "type": "content_block_delta",
                            "index": state["next_idx"] - 1,
                            "delta": {"type": "input_json_delta", "partial_json": pjson},
                        })
                elif kind == "tool_stop":
                    if state["cur_type"] == "tool_use":
                        sse.write_event("content_block_stop", {"type": "content_block_stop", "index": state["next_idx"] - 1})
                        state["cur_type"] = ""

        def on_sse_comment(comment: str) -> None:
            if not sse.closed and not cancel_event.is_set():
                with sse_lock:
                    if state["started"]:
                        sse.write_comment(comment)

        try:
            outcome = execute_turn(
                hdrs,
                body,
                model_info,
                tier,
                sys_text,
                messages,
                active_tools,
                is_openai=False,
                on_delta=on_live_delta,
                on_sse_comment=on_sse_comment,
                cancel_event=cancel_event,
                on_stream_open=on_stream_open,
            )
        except RequestCancelledError:
            stop_ping.set()
            return
        except Exception as exc:
            stop_ping.set()
            if not stream_opened_ev.is_set():
                st_code, err_t, err_m, err_c = self._classify_upstream_error(exc)
                self._send_api_error(st_code, err_t, err_m, is_anthropic=True, api_code=err_c)
                return
            if not sse.closed and not cancel_event.is_set():
                _, err_t, err_m, _ = self._classify_upstream_error(exc)
                with sse_lock:
                    close_block_locked()
                    sse.write_event("error", {"type": "error", "error": {"type": err_t, "message": err_m}})
                    sse.write_event("message_stop", {"type": "message_stop"})
            return
        finally:
            stop_ping.set()

        if sse.closed or cancel_event.is_set():
            return

        with sse_lock:
            rem_txt, _ = stop_filter.flush()
            if rem_txt:
                emit_text_locked(rem_txt)
            close_block_locked()

            out_tools = outcome.get("tool_calls", [])
            has_any_tools = bool(out_tools) or (bool(state["streamed_tool_ids"]) and not outcome.get("truncated_raw_stream"))
            if not has_any_tools and state["next_idx"] == 0 and stop_filter.matched is None:
                txt_fallback = str(outcome.get("text", ""))
                if stop_seqs:
                    txt_fallback, hit = apply_stop_sequences_to_text(txt_fallback, stop_seqs)
                    if hit is not None:
                        stop_filter.matched = hit
                idx = state["next_idx"]
                state["next_idx"] += 1
                sse.write_event("content_block_start", {"type": "content_block_start", "index": idx, "content_block": {"type": "text", "text": ""}})
                sse.write_event("content_block_delta", {"type": "content_block_delta", "index": idx, "delta": {"type": "text_delta", "text": txt_fallback}})
                sse.write_event("content_block_stop", {"type": "content_block_stop", "index": idx})

            for tc in out_tools:
                if tc["id"] in state["streamed_tool_ids"]:
                    continue
                idx = state["next_idx"]
                state["next_idx"] += 1
                sse.write_event("content_block_start", {"type": "content_block_start", "index": idx, "content_block": {"type": "tool_use", "id": tc["id"], "name": tc["name"], "input": {}}})
                sse.write_event("content_block_delta", {"type": "content_block_delta", "index": idx, "delta": {"type": "input_json_delta", "partial_json": json.dumps(tc["input"], ensure_ascii=False)}})
                sse.write_event("content_block_stop", {"type": "content_block_stop", "index": idx})

            matched_stop = stop_filter.matched
            if matched_stop is not None and not out_tools:
                final_stop = "stop_sequence"
            elif outcome.get("truncated_raw_stream"):
                final_stop = "max_tokens"
            elif has_any_tools:
                final_stop = "tool_use"
            else:
                final_stop = outcome.get("stop_reason", "end_turn")
                if final_stop == "tool_use" and not out_tools:
                    final_stop = "end_turn"

            cached_tok = int(outcome.get("cached_tokens", 0))
            uncached_in = max(1, in_tok - cached_tok)
            out_tok = estimate_output_tokens(outcome, include_thinking=allow_thinking)
            sse.write_event("message_delta", {
                "type": "message_delta",
                "delta": {"stop_reason": final_stop, "stop_sequence": matched_stop},
                "usage": {
                    "input_tokens": uncached_in,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": cached_tok,
                    "output_tokens": out_tok,
                },
            })
            sse.write_event("message_stop", {"type": "message_stop"})

    def _serve_openai_chat(self, hdrs: dict, body: dict):
        try:
            req_model = body.get("model", "gemini-app-carbon")
            model_info, tier = resolve_model_and_tier(req_model, body)
            messages = self._validate_messages_list(body.get("messages"), allow_empty=False)
            tools = body.get("tools") or []
            if not isinstance(tools, list):
                raise ValueError("'tools' must be an array")
            stop_seqs = extract_stop_sequences(body)
            tool_choice_mode, _ = parse_tool_choice(body, tools)
            _resolve_effective_upstream(hdrs)
            bad_mm = find_unsupported_multimodal_block(messages)
            if bad_mm:
                raise ValueError(f"Unsupported multimodal content block type {bad_mm!r}: only text and tool messages are supported.")
        except (ValueError, TypeError) as exc:
            self._send_api_error(400, "invalid_request_error", str(exc), is_anthropic=False, api_code="invalid_request")
            return

        stream = bool(body.get("stream", False))
        stream_opts = body.get("stream_options")
        include_usage = bool(isinstance(stream_opts, dict) and stream_opts.get("include_usage"))
        allow_thinking = should_emit_thinking(body, model_info, tier, is_openai=True)
        sys_parts = [_extract_content_text(m.get("content", "")) for m in messages if m.get("role") in ("system", "developer")]
        sys_text = "\n".join(p for p in sys_parts if p)
        non_sys = [m for m in messages if m.get("role") not in ("system", "developer")]
        active_tools = [] if tool_choice_mode == "none" else tools
        cid = "chatcmpl_" + random_hex(10)
        in_tok = estimate_count_tokens(body)

        if not stream:
            cancel_event = threading.Event()
            stop_mon = threading.Event()
            self._start_disconnect_monitor(cancel_event, stop_mon)
            try:
                outcome = execute_turn(hdrs, body, model_info, tier, sys_text, non_sys, active_tools, is_openai=True, cancel_event=cancel_event)
            except RequestCancelledError:
                return
            except Exception as exc:
                st_code, err_t, err_m, err_c = self._classify_upstream_error(exc)
                self._send_api_error(st_code, err_t, err_m, is_anthropic=False, api_code=err_c)
                return
            finally:
                stop_mon.set()

            raw_text = str(outcome.get("text") or "")
            if stop_seqs and not outcome.get("tool_calls"):
                raw_text, _ = apply_stop_sequences_to_text(raw_text, stop_seqs)

            oai_tcs = [
                {
                    "id": tc["id"],
                    "type": "function",
                    "function": {"name": tc["name"], "arguments": json.dumps(tc["input"], ensure_ascii=False)},
                }
                for tc in outcome.get("tool_calls", [])
            ]
            if outcome.get("truncated_raw_stream") or outcome.get("stop_reason") == "max_tokens":
                fin = "length"
            elif oai_tcs:
                fin = "tool_calls"
            else:
                fin = "stop"

            content_val = raw_text if raw_text else (None if oai_tcs else "")
            msg_obj: Dict[str, Any] = {"role": "assistant", "content": content_val}
            if allow_thinking and outcome.get("thinking"):
                msg_obj["reasoning_content"] = outcome["thinking"]
            if oai_tcs:
                msg_obj["tool_calls"] = oai_tcs

            cached_tok = int(outcome.get("cached_tokens", 0))
            adj_outcome = dict(outcome, text=raw_text)
            comp_tok = estimate_output_tokens(adj_outcome, include_thinking=allow_thinking)
            self._send_json(200, {
                "id": cid, "object": "chat.completion", "created": int(time.time()), "model": req_model,
                "choices": [{"index": 0, "message": msg_obj, "finish_reason": fin}],
                "usage": {
                    "prompt_tokens": in_tok,
                    "completion_tokens": comp_tok,
                    "total_tokens": in_tok + comp_tok,
                    "prompt_tokens_details": {"cached_tokens": cached_tok},
                },
            }, extra_headers={
                "X-Served-Model": f"{model_info['id']}/{tier}",
                "X-Turn-Mode": str(outcome.get("turn_mode") or "cold_start"),
                "X-Stream-Mode": str(outcome.get("stream_mode") or "plane1_webkit"),
                **self._cors_headers(True),
            })
            return

        cancel_event = threading.Event()
        stop_mon = threading.Event()
        stream_opened_ev = threading.Event()
        sse_oai = SafeSSEWriter(self.wfile, on_close=cancel_event.set)
        sse_lock = threading.Lock()
        stop_filter = StopSequenceFilter(stop_seqs)
        oai_state: Dict[str, Any] = {
            "opened": False,
            "open_failed": False,
            "role_sent": False,
            "tool_idx_by_id": {},
            "next_tool_idx": 0,
            "streamed_tool_ids": set(),
        }

        def send_oai_chunk_locked(delta: dict, fin: Optional[str] = None) -> None:
            if sse_oai.closed or cancel_event.is_set() or not oai_state["opened"]:
                return
            if not oai_state["role_sent"]:
                oai_state["role_sent"] = True
                p0 = {
                    "id": cid, "object": "chat.completion.chunk", "created": int(time.time()), "model": req_model,
                    "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}],
                }
                sse_oai.write_raw(f"data: {json.dumps(p0, ensure_ascii=False)}\n\n".encode("utf-8"))
            p = {
                "id": cid, "object": "chat.completion.chunk", "created": int(time.time()), "model": req_model,
                "choices": [{"index": 0, "delta": delta, "finish_reason": fin}],
            }
            sse_oai.write_raw(f"data: {json.dumps(p, ensure_ascii=False)}\n\n".encode("utf-8"))

        def on_stream_open(_meta: dict) -> None:
            with sse_lock:
                if oai_state["opened"] or oai_state["open_failed"]:
                    return
                if not self._send_sse_headers({"X-Served-Model": f"{model_info['id']}/{tier}"}):
                    oai_state["open_failed"] = True
                    cancel_event.set()
                    sse_oai.mark_closed()
                    return
                oai_state["opened"] = True
                stream_opened_ev.set()

        self._start_disconnect_monitor(cancel_event, stop_mon, sse=sse_oai)

        def on_delta(c: dict) -> None:
            if sse_oai.closed or cancel_event.is_set():
                return
            ctype = c.get("type")
            if ctype == "thinking" and not allow_thinking:
                return
            with sse_lock:
                if not oai_state["opened"] or sse_oai.closed:
                    return
                if ctype == "thinking":
                    val = c.get("content", "")
                    if val:
                        send_oai_chunk_locked({"reasoning_content": val})
                elif ctype == "text":
                    val = c.get("content", "")
                    if val:
                        emit_val, _ = stop_filter.feed(val)
                        if emit_val:
                            send_oai_chunk_locked({"content": emit_val})
                elif ctype == "tool_start":
                    if stop_filter.matched is not None:
                        return
                    rem_txt, _ = stop_filter.flush()
                    if rem_txt:
                        send_oai_chunk_locked({"content": rem_txt})
                    tc_id = c["id"]
                    tc_name = c["name"]
                    t_idx = oai_state["next_tool_idx"]
                    oai_state["next_tool_idx"] += 1
                    oai_state["tool_idx_by_id"][tc_id] = t_idx
                    oai_state["streamed_tool_ids"].add(tc_id)
                    send_oai_chunk_locked({
                        "tool_calls": [{"index": t_idx, "id": tc_id, "type": "function", "function": {"name": tc_name, "arguments": ""}}]
                    })
                elif ctype == "tool_delta":
                    tc_id = c["id"]
                    pjson = c.get("partial_json", "")
                    if tc_id in oai_state["tool_idx_by_id"] and pjson:
                        send_oai_chunk_locked({
                            "tool_calls": [{"index": oai_state["tool_idx_by_id"][tc_id], "function": {"arguments": pjson}}]
                        })

        try:
            outcome = execute_turn(
                hdrs,
                body,
                model_info,
                tier,
                sys_text,
                non_sys,
                active_tools,
                is_openai=True,
                on_delta=on_delta,
                cancel_event=cancel_event,
                on_stream_open=on_stream_open,
            )
        except RequestCancelledError:
            stop_mon.set()
            return
        except Exception as exc:
            stop_mon.set()
            if not stream_opened_ev.is_set():
                st_code, err_t, err_m, err_c = self._classify_upstream_error(exc)
                self._send_api_error(st_code, err_t, err_m, is_anthropic=False, api_code=err_c)
                return
            if not sse_oai.closed and not cancel_event.is_set():
                _, err_t, err_m, err_c = self._classify_upstream_error(exc)
                with sse_lock:
                    sse_oai.write_raw(
                        f"data: {json.dumps({'error': {'message': err_m, 'type': err_t, 'code': err_c}}, ensure_ascii=False)}\n\ndata: [DONE]\n\n".encode("utf-8")
                    )
            return
        finally:
            stop_mon.set()

        if sse_oai.closed or cancel_event.is_set():
            return

        with sse_lock:
            rem_txt, _ = stop_filter.flush()
            if rem_txt:
                send_oai_chunk_locked({"content": rem_txt})

            out_tools = outcome.get("tool_calls", [])
            for tc in out_tools:
                if tc["id"] in oai_state["streamed_tool_ids"]:
                    continue
                t_idx = oai_state["next_tool_idx"]
                oai_state["next_tool_idx"] += 1
                send_oai_chunk_locked({
                    "tool_calls": [{
                        "index": t_idx,
                        "id": tc["id"],
                        "type": "function",
                        "function": {"name": tc["name"], "arguments": json.dumps(tc["input"], ensure_ascii=False)},
                    }]
                })

            if outcome.get("truncated_raw_stream") or outcome.get("stop_reason") == "max_tokens":
                fin = "length"
            elif out_tools or (oai_state["streamed_tool_ids"] and not outcome.get("truncated_raw_stream")):
                fin = "tool_calls"
            else:
                fin = "stop"

            send_oai_chunk_locked({}, fin=fin)
            if include_usage:
                cached_tok = int(outcome.get("cached_tokens", 0))
                comp_tok = estimate_output_tokens(outcome, include_thinking=allow_thinking)
                usage_chunk = {
                    "id": cid,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": req_model,
                    "choices": [],
                    "usage": {
                        "prompt_tokens": in_tok,
                        "completion_tokens": comp_tok,
                        "total_tokens": in_tok + comp_tok,
                        "prompt_tokens_details": {"cached_tokens": cached_tok},
                    },
                }
                sse_oai.write_raw(f"data: {json.dumps(usage_chunk, ensure_ascii=False)}\n\n".encode("utf-8"))
            sse_oai.write_raw(b"data: [DONE]\n\n")

    def _serve_openai_responses(self, hdrs: dict, body: dict):
        try:
            norm = normalize_responses_body(body)
            req_model = norm.get("model", "gemini-app-carbon")
            model_info, tier = resolve_model_and_tier(req_model, norm)
            messages = norm.get("messages") or []
            sys_text = str(norm.get("system") or "")
            if not messages and not sys_text:
                raise ValueError("'input' must be non-empty")
            tools = norm.get("tools") or []
            if not isinstance(tools, list):
                raise ValueError("'tools' must be an array")
            stop_seqs = extract_stop_sequences(norm)
            tool_choice_mode, _ = parse_tool_choice(norm, tools)
            _resolve_effective_upstream(hdrs)
            bad_mm = find_unsupported_multimodal_block(messages, raw_input=body.get("input"))
            if bad_mm:
                raise ValueError(f"Unsupported multimodal content block type {bad_mm!r}: only text and tool content are supported.")
        except (ValueError, TypeError) as exc:
            self._send_api_error(400, "invalid_request_error", str(exc), is_anthropic=False, api_code="invalid_request")
            return

        stream = bool(norm.get("stream", False))
        allow_thinking = should_emit_thinking(norm, model_info, tier, is_openai=True)
        active_tools = [] if tool_choice_mode == "none" else tools
        rid = "resp_" + random_hex(10)
        created_at = int(time.time())
        in_tok = estimate_count_tokens(norm)

        def _build_response_obj(outcome_obj: dict, final_text: str) -> dict:
            out_items = []
            if allow_thinking and outcome_obj.get("thinking"):
                out_items.append({
                    "type": "reasoning",
                    "id": "rs_" + random_hex(8),
                    "status": "completed",
                    "summary": [{"type": "summary_text", "text": outcome_obj["thinking"]}],
                })
            if final_text or not outcome_obj.get("tool_calls"):
                out_items.append({
                    "type": "message",
                    "id": "msg_" + random_hex(8),
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": final_text, "annotations": []}],
                })
            for tc in outcome_obj.get("tool_calls", []):
                out_items.append({
                    "type": "function_call",
                    "id": "fc_" + random_hex(8),
                    "call_id": tc["id"],
                    "name": tc["name"],
                    "arguments": json.dumps(tc["input"], ensure_ascii=False),
                    "status": "completed",
                })
            cached_tok = int(outcome_obj.get("cached_tokens", 0))
            adj_out = dict(outcome_obj, text=final_text)
            out_tok = estimate_output_tokens(adj_out, include_thinking=allow_thinking)
            reason_tok = est_tokens_str(outcome_obj.get("thinking", "")) if (allow_thinking and outcome_obj.get("thinking")) else 0
            return {
                "id": rid,
                "object": "response",
                "created_at": created_at,
                "status": "completed",
                "model": req_model,
                "output": out_items,
                "output_text": final_text,
                "usage": {
                    "input_tokens": in_tok,
                    "input_tokens_details": {"cached_tokens": cached_tok},
                    "output_tokens": out_tok,
                    "output_tokens_details": {"reasoning_tokens": reason_tok},
                    "total_tokens": in_tok + out_tok,
                },
            }

        if not stream:
            cancel_event = threading.Event()
            stop_mon = threading.Event()
            self._start_disconnect_monitor(cancel_event, stop_mon)
            try:
                outcome = execute_turn(hdrs, norm, model_info, tier, sys_text, messages, active_tools, is_openai=False, cancel_event=cancel_event)
            except RequestCancelledError:
                return
            except Exception as exc:
                st_code, err_t, err_m, err_c = self._classify_upstream_error(exc)
                self._send_api_error(st_code, err_t, err_m, is_anthropic=False, api_code=err_c)
                return
            finally:
                stop_mon.set()

            final_text = str(outcome.get("text") or "")
            if stop_seqs and not outcome.get("tool_calls"):
                final_text, _ = apply_stop_sequences_to_text(final_text, stop_seqs)
            self._send_json(200, _build_response_obj(outcome, final_text), extra_headers={
                "X-Served-Model": f"{model_info['id']}/{tier}",
                "X-Turn-Mode": str(outcome.get("turn_mode") or "cold_start"),
                "X-Stream-Mode": str(outcome.get("stream_mode") or "plane1_webkit"),
                **self._cors_headers(True),
            })
            return

        # Streaming OpenAI /v1/responses SSE
        cancel_event = threading.Event()
        stop_mon = threading.Event()
        stream_opened_ev = threading.Event()
        sse_resp = SafeSSEWriter(self.wfile, on_close=cancel_event.set)
        sse_lock = threading.Lock()
        stop_filter = StopSequenceFilter(stop_seqs)
        r_state: Dict[str, Any] = {
            "opened": False,
            "open_failed": False,
            "output_index": 0,
            "msg_item_id": "",
            "msg_output_idx": -1,
            "msg_text_parts": [],
            "rs_item_id": "",
            "rs_output_idx": -1,
            "rs_parts": [],
            "fc_by_tc_id": {},
            "streamed_tool_ids": set(),
        }

        def _ensure_msg_open_locked() -> None:
            if not r_state["msg_item_id"]:
                mid = "msg_" + random_hex(8)
                oidx = r_state["output_index"]
                r_state["output_index"] += 1
                r_state["msg_item_id"] = mid
                r_state["msg_output_idx"] = oidx
                sse_resp.write_event("response.output_item.added", {
                    "type": "response.output_item.added",
                    "output_index": oidx,
                    "item": {"id": mid, "type": "message", "status": "in_progress", "role": "assistant", "content": []},
                })
                sse_resp.write_event("response.content_part.added", {
                    "type": "response.content_part.added",
                    "item_id": mid,
                    "output_index": oidx,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": "", "annotations": []},
                })

        def _close_msg_locked() -> None:
            if r_state["msg_item_id"]:
                mid = r_state["msg_item_id"]
                oidx = r_state["msg_output_idx"]
                full_m = "".join(r_state["msg_text_parts"])
                sse_resp.write_event("response.output_text.done", {
                    "type": "response.output_text.done",
                    "item_id": mid,
                    "output_index": oidx,
                    "content_index": 0,
                    "text": full_m,
                })
                sse_resp.write_event("response.content_part.done", {
                    "type": "response.content_part.done",
                    "item_id": mid,
                    "output_index": oidx,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": full_m, "annotations": []},
                })
                sse_resp.write_event("response.output_item.done", {
                    "type": "response.output_item.done",
                    "output_index": oidx,
                    "item": {"id": mid, "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": full_m, "annotations": []}]},
                })
                r_state["msg_item_id"] = ""

        def _close_reasoning_locked() -> None:
            if r_state["rs_item_id"]:
                rsid = r_state["rs_item_id"]
                oidx = r_state["rs_output_idx"]
                full_r = "".join(r_state["rs_parts"])
                sse_resp.write_event("response.reasoning_summary_text.done", {
                    "type": "response.reasoning_summary_text.done",
                    "item_id": rsid,
                    "output_index": oidx,
                    "summary_index": 0,
                    "text": full_r,
                })
                sse_resp.write_event("response.output_item.done", {
                    "type": "response.output_item.done",
                    "output_index": oidx,
                    "item": {"id": rsid, "type": "reasoning", "status": "completed", "summary": [{"type": "summary_text", "text": full_r}]},
                })
                r_state["rs_item_id"] = ""

        def on_stream_open(_meta: dict) -> None:
            with sse_lock:
                if r_state["opened"] or r_state["open_failed"]:
                    return
                if not self._send_sse_headers({"X-Served-Model": f"{model_info['id']}/{tier}"}):
                    r_state["open_failed"] = True
                    cancel_event.set()
                    sse_resp.mark_closed()
                    return
                r_state["opened"] = True
                base_resp = {"id": rid, "object": "response", "created_at": created_at, "status": "in_progress", "model": req_model, "output": []}
                sse_resp.write_event("response.created", {"type": "response.created", "response": base_resp})
                sse_resp.write_event("response.in_progress", {"type": "response.in_progress", "response": base_resp})
                stream_opened_ev.set()

        self._start_disconnect_monitor(cancel_event, stop_mon, sse=sse_resp)

        def on_delta(c: dict) -> None:
            if sse_resp.closed or cancel_event.is_set():
                return
            ctype = c.get("type")
            if ctype == "thinking" and not allow_thinking:
                return
            with sse_lock:
                if not r_state["opened"] or sse_resp.closed:
                    return
                if ctype == "thinking":
                    val = c.get("content", "")
                    if not val:
                        return
                    if not r_state["rs_item_id"]:
                        rsid = "rs_" + random_hex(8)
                        oidx = r_state["output_index"]
                        r_state["output_index"] += 1
                        r_state["rs_item_id"] = rsid
                        r_state["rs_output_idx"] = oidx
                        sse_resp.write_event("response.output_item.added", {
                            "type": "response.output_item.added",
                            "output_index": oidx,
                            "item": {"id": rsid, "type": "reasoning", "status": "in_progress", "summary": []},
                        })
                    r_state["rs_parts"].append(val)
                    sse_resp.write_event("response.reasoning_summary_text.delta", {
                        "type": "response.reasoning_summary_text.delta",
                        "item_id": r_state["rs_item_id"],
                        "output_index": r_state["rs_output_idx"],
                        "summary_index": 0,
                        "delta": val,
                    })
                elif ctype == "text":
                    val = c.get("content", "")
                    if not val:
                        return
                    emit_val, _ = stop_filter.feed(val)
                    if emit_val:
                        _close_reasoning_locked()
                        _ensure_msg_open_locked()
                        r_state["msg_text_parts"].append(emit_val)
                        sse_resp.write_event("response.output_text.delta", {
                            "type": "response.output_text.delta",
                            "item_id": r_state["msg_item_id"],
                            "output_index": r_state["msg_output_idx"],
                            "content_index": 0,
                            "delta": emit_val,
                        })
                elif ctype == "tool_start":
                    if stop_filter.matched is not None:
                        return
                    rem_txt, _ = stop_filter.flush()
                    if rem_txt:
                        _ensure_msg_open_locked()
                        r_state["msg_text_parts"].append(rem_txt)
                        sse_resp.write_event("response.output_text.delta", {
                            "type": "response.output_text.delta",
                            "item_id": r_state["msg_item_id"],
                            "output_index": r_state["msg_output_idx"],
                            "content_index": 0,
                            "delta": rem_txt,
                        })
                    _close_reasoning_locked()
                    _close_msg_locked()
                    tc_id = c["id"]
                    tc_name = c["name"]
                    fc_id = "fc_" + random_hex(8)
                    oidx = r_state["output_index"]
                    r_state["output_index"] += 1
                    r_state["streamed_tool_ids"].add(tc_id)
                    r_state["fc_by_tc_id"][tc_id] = {"fc_id": fc_id, "output_index": oidx, "name": tc_name, "args": []}
                    sse_resp.write_event("response.output_item.added", {
                        "type": "response.output_item.added",
                        "output_index": oidx,
                        "item": {"id": fc_id, "type": "function_call", "call_id": tc_id, "name": tc_name, "arguments": "", "status": "in_progress"},
                    })
                elif ctype == "tool_delta":
                    tc_id = c["id"]
                    pjson = c.get("partial_json", "")
                    info_fc = r_state["fc_by_tc_id"].get(tc_id)
                    if info_fc and pjson:
                        info_fc["args"].append(pjson)
                        sse_resp.write_event("response.function_call_arguments.delta", {
                            "type": "response.function_call_arguments.delta",
                            "item_id": info_fc["fc_id"],
                            "output_index": info_fc["output_index"],
                            "delta": pjson,
                        })
                elif ctype == "tool_stop":
                    tc_id = c["id"]
                    info_fc = r_state["fc_by_tc_id"].get(tc_id)
                    if info_fc:
                        full_args = "".join(info_fc["args"])
                        sse_resp.write_event("response.function_call_arguments.done", {
                            "type": "response.function_call_arguments.done",
                            "item_id": info_fc["fc_id"],
                            "output_index": info_fc["output_index"],
                            "arguments": full_args,
                        })
                        sse_resp.write_event("response.output_item.done", {
                            "type": "response.output_item.done",
                            "output_index": info_fc["output_index"],
                            "item": {"id": info_fc["fc_id"], "type": "function_call", "call_id": tc_id, "name": info_fc["name"], "arguments": full_args, "status": "completed"},
                        })

        try:
            outcome = execute_turn(
                hdrs,
                norm,
                model_info,
                tier,
                sys_text,
                messages,
                active_tools,
                is_openai=False,
                on_delta=on_delta,
                cancel_event=cancel_event,
                on_stream_open=on_stream_open,
            )
        except RequestCancelledError:
            stop_mon.set()
            return
        except Exception as exc:
            stop_mon.set()
            if not stream_opened_ev.is_set():
                st_code, err_t, err_m, err_c = self._classify_upstream_error(exc)
                self._send_api_error(st_code, err_t, err_m, is_anthropic=False, api_code=err_c)
                return
            if not sse_resp.closed and not cancel_event.is_set():
                _, err_t, err_m, err_c = self._classify_upstream_error(exc)
                with sse_lock:
                    _close_reasoning_locked()
                    _close_msg_locked()
                    sse_resp.write_event("response.failed", {
                        "type": "response.failed",
                        "response": {
                            "id": rid, "object": "response", "created_at": created_at,
                            "status": "failed", "model": req_model,
                            "error": {"code": err_c, "message": err_m},
                        },
                    })
                    sse_resp.write_event("error", {"type": "error", "error": {"type": err_t, "message": err_m, "code": err_c}})
            return
        finally:
            stop_mon.set()

        if sse_resp.closed or cancel_event.is_set():
            return

        with sse_lock:
            rem_txt, _ = stop_filter.flush()
            if rem_txt:
                _close_reasoning_locked()
                _ensure_msg_open_locked()
                r_state["msg_text_parts"].append(rem_txt)
                sse_resp.write_event("response.output_text.delta", {
                    "type": "response.output_text.delta",
                    "item_id": r_state["msg_item_id"],
                    "output_index": r_state["msg_output_idx"],
                    "content_index": 0,
                    "delta": rem_txt,
                })
            _close_reasoning_locked()
            _close_msg_locked()

            out_tools = outcome.get("tool_calls", [])
            for tc in out_tools:
                if tc["id"] in r_state["streamed_tool_ids"]:
                    continue
                fc_id = "fc_" + random_hex(8)
                oidx = r_state["output_index"]
                r_state["output_index"] += 1
                args_json = json.dumps(tc["input"], ensure_ascii=False)
                sse_resp.write_event("response.output_item.added", {
                    "type": "response.output_item.added",
                    "output_index": oidx,
                    "item": {"id": fc_id, "type": "function_call", "call_id": tc["id"], "name": tc["name"], "arguments": "", "status": "in_progress"},
                })
                sse_resp.write_event("response.function_call_arguments.delta", {
                    "type": "response.function_call_arguments.delta",
                    "item_id": fc_id,
                    "output_index": oidx,
                    "delta": args_json,
                })
                sse_resp.write_event("response.function_call_arguments.done", {
                    "type": "response.function_call_arguments.done",
                    "item_id": fc_id,
                    "output_index": oidx,
                    "arguments": args_json,
                })
                sse_resp.write_event("response.output_item.done", {
                    "type": "response.output_item.done",
                    "output_index": oidx,
                    "item": {"id": fc_id, "type": "function_call", "call_id": tc["id"], "name": tc["name"], "arguments": args_json, "status": "completed"},
                })

            final_text = "".join(r_state["msg_text_parts"]) or str(outcome.get("text") or "")
            if stop_seqs and not out_tools:
                final_text, _ = apply_stop_sequences_to_text(final_text, stop_seqs)
            resp_done = _build_response_obj(outcome, final_text)
            sse_resp.write_event("response.completed", {"type": "response.completed", "response": resp_done})


class ThreadingTCPHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, TimeoutError, socket.timeout)):
            return
        if isinstance(exc, OSError) and (exc.errno in DISCONNECT_ERRNOS or exc.errno == getattr(errno, "ECONNABORTED", 53)):
            return
        super().handle_error(request, client_address)


def write_fifo_safe(fifo_path: str, payload_bytes: bytes, timeout_sec: float = 8.0) -> bool:
    deadline = time.time() + timeout_sec
    wfd = -1
    while True:
        try:
            wfd = os.open(fifo_path, os.O_WRONLY | os.O_NONBLOCK)
            break
        except OSError as exc:
            if exc.errno == errno.ENXIO and time.time() < min(deadline, time.time() + 0.8):
                if time.time() >= deadline:
                    return False
                time.sleep(0.02)
                continue
            return False

    try:
        total = len(payload_bytes)
        sent = 0
        while sent < total:
            rem = deadline - time.time()
            if rem <= 0:
                return False
            _, wlist, _ = select.select([], [wfd], [], min(0.5, rem))
            if not wlist:
                continue
            try:
                n = os.write(wfd, payload_bytes[sent : sent + 16384])
                if n > 0:
                    sent += n
            except OSError as exc:
                if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    continue
                return False
        return True
    finally:
        try:
            os.close(wfd)
        except OSError:
            pass


def cleanup_stale_ipc_entries(ipc_dir: str, max_age_sec: float = 60.0) -> None:
    now = time.time()
    for pat in ("req_*.json", "req_*.tmp", "resp_*.fifo"):
        for p in glob.glob(os.path.join(ipc_dir, pat)):
            try:
                if now - os.stat(p).st_mtime > max_age_sec:
                    os.unlink(p)
            except OSError:
                pass


def serve_fifo_bus(ipc_dir: str, stop_event: Optional[threading.Event] = None):
    os.makedirs(ipc_dir, mode=0o700, exist_ok=True)
    cleanup_stale_ipc_entries(ipc_dir, max_age_sec=60.0)

    wake_fifo = os.path.join(ipc_dir, "wake.fifo")
    if os.path.exists(wake_fifo):
        try:
            if not stat.S_ISFIFO(os.stat(wake_fifo).st_mode):
                os.unlink(wake_fifo)
                os.mkfifo(wake_fifo, 0o600)
        except OSError:
            pass
    else:
        try:
            os.mkfifo(wake_fifo, 0o600)
        except OSError:
            pass

    wake_fd = os.open(wake_fifo, os.O_RDWR | os.O_NONBLOCK)
    claimed = set()
    claim_lock = threading.Lock()
    last_gc = time.time()

    def handle_req_file(req_path: str):
        try:
            with open(req_path, "r", encoding="utf-8") as fp:
                env = json.load(fp)
            try:
                os.unlink(req_path)
            except OSError:
                pass
            path = env.get("path", "/")
            method = env.get("method", "POST")
            body = env.get("body") or {}
            if method == "POST" and path == "/mcp/shared":
                res_obj = handle_shared_mcp_rpc(body)
                raw_body = json.dumps(res_obj, ensure_ascii=False)
                code, ctype = 200, "application/json"
            elif method == "GET" and path == "/health":
                code, ctype, raw_body = 200, "application/json", json.dumps({"ok": True, "version": GATEWAY_VERSION})
            elif method == "GET" and path == "/readyz":
                code, ctype, raw_body = 200, "application/json", json.dumps(build_readyz_snapshot(), ensure_ascii=False)
            elif method == "GET" and path == "/_admin/state":
                code, ctype, raw_body = 200, "application/json", json.dumps(build_health_snapshot(), ensure_ascii=False)
            else:
                code, ctype, raw_body = 404, "application/json", json.dumps({"error": "use HTTP"})
            resp_fifo = env["resp_fifo"]
            resp_bytes = json.dumps({"status": code, "content_type": ctype, "body": raw_body}, ensure_ascii=False).encode("utf-8")
            write_fifo_safe(resp_fifo, resp_bytes, timeout_sec=8.0)
        except Exception as exc:
            sys.stderr.write(f"[fifo-bus] worker error: {exc}\n")
        finally:
            with claim_lock:
                claimed.discard(req_path)

    def scan_pending_requests() -> None:
        for req_path in glob.glob(os.path.join(ipc_dir, "req_*.json")):
            with claim_lock:
                if req_path in claimed:
                    continue
                claimed.add(req_path)
            threading.Thread(target=handle_req_file, args=(req_path,), daemon=True).start()

    # Initial scan once in case requests arrived before wake.fifo opened
    scan_pending_requests()

    try:
        while stop_event is None or not stop_event.is_set():
            r, _, _ = select.select([wake_fd], [], [], 1.5)
            woken = bool(r)
            if woken:
                while True:
                    try:
                        chunk = os.read(wake_fd, 4096)
                        if not chunk:
                            break
                    except OSError:
                        break
                scan_pending_requests()
            now = time.time()
            if now - last_gc > 30.0:
                last_gc = now
                cleanup_stale_ipc_entries(ipc_dir, max_age_sec=90.0)
                if not woken:
                    scan_pending_requests()
    finally:
        try:
            os.close(wake_fd)
        except OSError:
            pass


def main():
    global HTTP_HOST, HTTP_PORT, UPSTREAM_MODE, SOFT_WATERMARK_TOKENS, IPC_DIR, WEBKIT_SOCK
    ap = argparse.ArgumentParser(description="Dual-Plane v4.6 Full-Carbon x Auto-Clean Web Gateway")
    ap.add_argument("--host", default=HTTP_HOST)
    ap.add_argument("--port", type=int, default=HTTP_PORT)
    ap.add_argument("--upstream", default="dual_plane_web", choices=["dual_plane_web", "mcp_task", "web"])
    ap.add_argument("--watermark", type=int, default=SOFT_WATERMARK_TOKENS)
    ap.add_argument("--webkit-sock", default="")
    args = ap.parse_args()

    HTTP_HOST = args.host
    HTTP_PORT = args.port
    UPSTREAM_MODE = args.upstream
    SOFT_WATERMARK_TOKENS = args.watermark
    IPC_DIR = os.environ.get("DSH_IPC_DIR", f"/tmp/dsh_gemini_ipc_{HTTP_PORT}")
    WEBKIT_SOCK = args.webkit_sock or os.environ.get("DSH_WEBKIT_SOCK", f"/tmp/dsh_webkit_{HTTP_PORT}.sock")

    stop_ev = threading.Event()

    def _on_sig(signum, _frame):
        if os.environ.get("DSH_KEEP_CHATS_ON_EXIT") != "1":
            try:
                REGISTRY.clean_all_web_chats()
            except Exception:
                pass
        WORKER_POOL.drain_for_restart()
        EMBEDDED_MCP_HOST.shutdown_all()
        stop_ev.set()

    signal.signal(signal.SIGTERM, _on_sig)
    signal.signal(signal.SIGINT, _on_sig)

    def _idle_chat_reaper():
        while not stop_ev.wait(30.0):
            try:
                REGISTRY.reap_idle_sessions(max_idle_sec=SESSION_IDLE_CLEAN_SEC)
            except Exception:
                pass

    threading.Thread(target=_idle_chat_reaper, daemon=True).start()

    EMBEDDED_MCP_HOST.sync_servers()
    if os.environ.get("DSH_DISABLE_WEBKIT_AUTOSPAWN") != "1":
        threading.Thread(target=lambda: get_webkit_plane().ensure_daemon_running(auto_popup_login=False), daemon=True).start()

    t_fifo = threading.Thread(target=serve_fifo_bus, args=(IPC_DIR, stop_ev), daemon=True)
    t_fifo.start()

    tcp_srv = None
    if os.environ.get("DSH_FIFO_ONLY") != "1":
        tcp_srv = ThreadingTCPHTTPServer((HTTP_HOST, HTTP_PORT), GatewayHTTPHandler)
        t_tcp = threading.Thread(target=tcp_srv.serve_forever, daemon=True)
        t_tcp.start()
        sys.stderr.write(
            f"[gateway {GATEWAY_VERSION}] Listening at http://{HTTP_HOST}:{HTTP_PORT} | "
            f"Dashboard: http://{HTTP_HOST}:{HTTP_PORT}/dashboard | "
            f"Quota=100% Pure Gemini Web (ZERO ACP) | Plane1={WEBKIT_SOCK} | Plane2={IPC_DIR}\n"
        )

    sys.stderr.flush()
    while not stop_ev.wait(0.5):
        pass
    if tcp_srv is not None:
        try:
            tcp_srv.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
