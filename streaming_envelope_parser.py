#!/usr/bin/env python3
"""
streaming_envelope_parser.py - DSH-TP v1.1 Zero-Drop Streaming Envelope Parser & Real-Time JSON Emitter
Capabilities:
  1. Per-turn 8-hex HMAC Nonce generation with outbound zero-collision verification.
  2. O(1) streaming lookahead + permissive tag recognition (`<dsh_call_{nonce}>`, `<dsh_call>`,
     `<tool_call>`, single/double/unquoted attributes, `<dsh_arg>`/`<arg>`/`<param>`).
  3. Column-0 Markdown code-fence immunity (` ``` ` / ` ~~~ `) so fenced code examples
     never misfire as live tool invocations.
  4. 3-Layer JSON Recovery Engine:
     - Layer 1: Single-pass lexical repair (control chars, lone regex backslashes, trailing commas).
     - Layer 2: `KEY_STR` vs `VAL_STR` phase-aware structural string repair (fixes unescaped inner
       quotes even when followed by colons `":` inside shell commands or code).
     - Layer 3: Schema-key anchor span extractor (recovers Mangled JSON envelopes by tool/arg anchors).
  5. Real-Time Homomorphic `input_json_delta` Streaming (`RAW_ARG_STREAM`) for Layer A host tools:
     eliminates multi-minute `chunks=0` UI freezes on large `Write` / `Edit` payloads, normalizes
     relative `file_path` against `cwd` inline, and seals mid-stream EOF truncations with a V8
     duplicate-key poison pill (`"file_path": null`) so truncated writes never corrupt disk files.
"""

import hashlib
import hmac
import json
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

PERMISSIVE_ATTR_RE = re.compile(
    r'([a-zA-Z_][a-zA-Z0-9_-]*)\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s"\'=<>`/]+))'
)
OPEN_CALL_TAG_RE = re.compile(
    r'^<(?:dsh_call(?:_([0-9a-fA-F]{8}))?|tool_call)(?=[\s>])((?:[^>"\']+|"[^"]*"|\'[^\']*\')*)>'
)
ANY_ARG_OPEN_RE = re.compile(
    r'<(dsh_arg(?:_[0-9a-fA-F]{8})?|arg|param)\b((?:[^>"\']+|"[^"]*"|\'[^\']*\')*)>'
)
KNOWN_TOOL_NAMES = (
    "Bash", "Read", "Write", "Edit", "MultiEdit", "Glob", "Grep", "LS",
    "Task", "Agent", "TodoWrite", "dsh_read_tool_page", "dsh_local_mcp_probe",
)
KNOWN_ARG_KEYS = (
    "file_path", "path", "content", "command", "description", "old_string",
    "new_string", "replace_all", "offset", "limit", "pattern", "timeout",
    "prompt", "subagent_type", "page_token", "page", "grep", "echo",
)
FILE_PATH_ALIASES = ("file_path", "path", "AbsolutePath", "TargetFile", "filepath")
SCALAR_BUFFER_KEYS = set(FILE_PATH_ALIASES) | {"replace_all", "offset", "limit", "timeout", "page"}

VALID_JSON_ESCAPES = set('"\\/bfnrtu')
HEX_CHARS = set("0123456789abcdefABCDEF")

_JSON_KEY_COLON_RE = re.compile(r'"[^"\\\n\r]*(?:\\.[^"\\\n\r]*)*"\s*:')
_JSON_SCALAR_VAL_RE = re.compile(
    r"(?:(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*(?:[,\]\}]|$)"
)
_JSON_UNESCAPE_MAP = {
    '"': '"',
    "\\": "\\",
    "/": "/",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
}


def _find_tag_close_gt(s: str, start: int = 0) -> int:
    """Finds the closing `>` of an opening XML-like tag outside single/double quotes."""
    quote = ""
    i = start
    n = len(s)
    if i < n and s[i] == "<":
        i += 1
    while i < n:
        ch = s[i]
        if quote:
            if ch == quote:
                quote = ""
        else:
            if ch in ('"', "'"):
                quote = ch
            elif ch == ">":
                return i
        i += 1
    return -1


def _unescape_json_fragment(s: str) -> str:
    """Single-pass JSON string unescaping preserving literal `\\\\n` vs newline `\\n`."""
    def _sub(m: re.Match) -> str:
        seq = m.group(1)
        if seq.startswith("u") and len(seq) == 5:
            try:
                return chr(int(seq[1:], 16))
            except ValueError:
                return m.group(0)
        return _JSON_UNESCAPE_MAP.get(seq, m.group(0))

    return re.sub(r"\\(u[0-9a-fA-F]{4}|.)", _sub, s)


def parse_permissive_attrs(attr_str: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for m in PERMISSIVE_ATTR_RE.finditer(attr_str or ""):
        k = m.group(1)
        v = m.group(2) if m.group(2) is not None else (m.group(3) if m.group(3) is not None else (m.group(4) or ""))
        out[k] = v
    return out


def generate_turn_nonce(outbound_text: str = "", secret_seed: Optional[bytes] = None) -> str:
    """Generates an 8-hex-char turn nonce guaranteed not to collide with outbound_text."""
    seed = secret_seed or os.urandom(16)
    counter = 0
    while True:
        msg = f"{uuid.uuid4().hex}:{counter}".encode("utf-8")
        cand = hmac.new(seed, msg, hashlib.sha256).hexdigest()[:8]
        if (
            f"dsh_call_{cand}" not in outbound_text
            and f"dsh_arg_{cand}" not in outbound_text
        ):
            return cand
        counter += 1


def _strip_code_fence_wrapper(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1 :].strip()
        if s.endswith("```"):
            s = s[:-3].strip()
    return s


def _skip_json_ws(s: str, pos: int) -> int:
    n = len(s)
    while pos < n and s[pos] in " \t\r\n":
        pos += 1
    return pos


def _looks_like_arr_next(s: str, pos: int) -> bool:
    if pos >= len(s):
        return True
    if s[pos] in ('"', "{", "["):
        return True
    return bool(_JSON_SCALAR_VAL_RE.match(s[pos:]))


def _looks_like_container_close(s: str, close_pos: int, stack: List[str]) -> bool:
    n = len(s)
    idx = close_pos
    sp = len(stack) - 1
    while sp >= 0:
        if idx >= n:
            return False
        expected = "}" if stack[sp].startswith("OBJ") else "]"
        if s[idx] != expected:
            return False
        sp -= 1
        p = _skip_json_ws(s, idx + 1)
        if sp < 0:
            return p >= n
        if p >= n:
            return True
        if s[p] == ",":
            q = _skip_json_ws(s, p + 1)
            if q >= n:
                return True
            if stack[sp].startswith("OBJ"):
                if s[q] == "}":
                    idx = q
                    continue
                return bool(_JSON_KEY_COLON_RE.match(s[q:]))
            if s[q] == "]":
                idx = q
                continue
            return _looks_like_arr_next(s, q)
        if s[p] in ("}", "]"):
            idx = p
            continue
        return False
    return True


def _is_closing_val_quote(s: str, quote_idx: int, stack: List[str]) -> bool:
    n = len(s)
    j = _skip_json_ws(s, quote_idx + 1)
    if j >= n:
        return True
    if not stack:
        return False
    top = stack[-1]
    if top.startswith("OBJ"):
        if s[j] == "}":
            return _looks_like_container_close(s, j, stack)
        if s[j] == ",":
            k = _skip_json_ws(s, j + 1)
            if k >= n:
                return True
            if s[k] == "}":
                return _looks_like_container_close(s, k, stack)
            return bool(_JSON_KEY_COLON_RE.match(s[k:]))
        return False
    # ARR
    if s[j] == "]":
        return _looks_like_container_close(s, j, stack)
    if s[j] == ",":
        k = _skip_json_ws(s, j + 1)
        if k >= n:
            return True
        if s[k] == "]":
            return _looks_like_container_close(s, k, stack)
        return _looks_like_arr_next(s, k)
    return False


def stateful_repair_json(raw: str) -> Optional[Any]:
    """
    Layer 2 Phase-Aware JSON Repair (`KEY_STR` vs `VAL_STR` across arbitrary root keys,
    arrays of objects, and arbitrarily deep nested objects/arrays):
    Correctly escapes unescaped inner double quotes even when immediately followed by `:` or `,`
    inside shell commands, regexes, or code snippets.
    """
    s = _strip_code_fence_wrapper(raw)
    if not s:
        return None
    out: List[str] = []
    stack: List[str] = []  # "OBJ_EXPECT_KEY" | "OBJ_AFTER_KEY" | "OBJ_EXPECT_VAL" | "OBJ_AFTER_VAL" | "ARR"
    str_role = ""  # "" | "KEY" | "VAL"
    n = len(s)
    i = 0

    while i < n:
        ch = s[i]
        if str_role:
            if ch == "\\":
                if i + 1 < n:
                    nxt = s[i + 1]
                    if nxt == "u" and i + 5 < n and all(c in HEX_CHARS for c in s[i + 2 : i + 6]):
                        out.append(s[i : i + 6])
                        i += 6
                        continue
                    if nxt in VALID_JSON_ESCAPES and nxt != "u":
                        out.append("\\" + nxt)
                        i += 2
                        continue
                out.append("\\\\")
                i += 1
                continue

            if ch == '"':
                if str_role == "KEY":
                    j = _skip_json_ws(s, i + 1)
                    is_closing = (j < n and s[j] == ":")
                else:
                    is_closing = _is_closing_val_quote(s, i, stack)

                if is_closing:
                    out.append('"')
                    if stack:
                        if str_role == "KEY" and stack[-1] == "OBJ_EXPECT_KEY":
                            stack[-1] = "OBJ_AFTER_KEY"
                        elif str_role == "VAL" and stack[-1] == "OBJ_EXPECT_VAL":
                            stack[-1] = "OBJ_AFTER_VAL"
                    str_role = ""
                else:
                    out.append('\\"')
                i += 1
                continue

            if ch == "\n":
                out.append("\\n")
            elif ch == "\r":
                out.append("\\r")
            elif ch == "\t":
                out.append("\\t")
            elif ord(ch) < 0x20:
                out.append(f"\\u{ord(ch):04x}")
            else:
                out.append(ch)
            i += 1
            continue

        # Outside string
        if ch in " \t\r\n":
            out.append(ch)
            i += 1
            continue
        if ch == "{":
            if stack and stack[-1] == "OBJ_EXPECT_VAL":
                stack[-1] = "OBJ_AFTER_VAL"
            stack.append("OBJ_EXPECT_KEY")
            out.append("{")
            i += 1
            continue
        if ch == "[":
            if stack and stack[-1] == "OBJ_EXPECT_VAL":
                stack[-1] = "OBJ_AFTER_VAL"
            stack.append("ARR")
            out.append("[")
            i += 1
            continue
        if ch in ("}", "]"):
            if stack:
                stack.pop()
            out.append(ch)
            i += 1
            continue
        if ch == ":":
            if stack and stack[-1] == "OBJ_AFTER_KEY":
                stack[-1] = "OBJ_EXPECT_VAL"
            out.append(":")
            i += 1
            continue
        if ch == ",":
            j = _skip_json_ws(s, i + 1)
            if j < n and s[j] in ("}", "]"):
                i += 1
                continue
            if stack and stack[-1] == "OBJ_AFTER_VAL":
                stack[-1] = "OBJ_EXPECT_KEY"
            out.append(",")
            i += 1
            continue
        if ch == '"':
            if stack and stack[-1] == "OBJ_EXPECT_KEY":
                str_role = "KEY"
            else:
                str_role = "VAL"
            out.append('"')
            i += 1
            continue
        out.append(ch)
        i += 1

    repaired = "".join(out)
    try:
        return json.loads(repaired)
    except Exception:
        return None


def schema_anchor_extract(
    raw: str,
    fallback_tool: str = "",
    known_tools: Optional[Any] = None,
    known_arg_keys: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """
    Layer 3 Structural Anchor Extractor: recovers tool name & known argument key spans even when
    outer JSON braces/quotes are truncated or severely corrupted.
    """
    s = _strip_code_fence_wrapper(raw)
    if not s:
        return None

    tool_candidates: List[str] = []
    seen_tools: Set[str] = set()
    for t in list(known_tools or ()) + list(KNOWN_TOOL_NAMES):
        ts = str(t).strip()
        if ts and ts not in seen_tools:
            seen_tools.add(ts)
            tool_candidates.append(ts)

    tool_name = fallback_tool
    if not tool_name:
        m_tool = re.search(r'"(?:name|tool)"\s*:\s*"([A-Za-z0-9_.-]+)"', s)
        if m_tool:
            tool_name = m_tool.group(1)
        else:
            for kt in tool_candidates:
                if re.search(rf"\b{re.escape(kt)}\b", s):
                    tool_name = kt
                    break
    if not tool_name:
        return None

    inp_region = s
    m_inp = re.search(r'"(?:input|arguments)"\s*:\s*\{', s)
    if m_inp:
        inp_region = s[m_inp.end() :]

    arg_candidates: List[str] = []
    seen_keys: Set[str] = set()
    for k in list(known_arg_keys or ()) + list(KNOWN_ARG_KEYS):
        ks = str(k).strip()
        if ks and ks not in seen_keys:
            seen_keys.add(ks)
            arg_candidates.append(ks)
    arg_candidates.sort(key=len, reverse=True)

    key_alt = "|".join(re.escape(k) for k in arg_candidates)
    key_matches = list(re.finditer(rf'"({key_alt})"\s*:\s*', inp_region))
    if not key_matches:
        return None

    extracted: Dict[str, Any] = {}
    for idx, km in enumerate(key_matches):
        kname = km.group(1)
        v_start = km.end()
        v_end = key_matches[idx + 1].start() if (idx + 1 < len(key_matches)) else len(inp_region)
        seg = inp_region[v_start:v_end].strip()
        if idx + 1 < len(key_matches):
            seg = re.sub(r",\s*$", "", seg)
        else:
            seg = re.sub(r",?\s*\}+\s*$", "", seg)
        if seg.startswith('"') and seg.endswith('"') and len(seg) >= 2:
            extracted[kname] = _unescape_json_fragment(seg[1:-1])
        elif seg.lower() in ("true", "false"):
            extracted[kname] = (seg.lower() == "true")
        elif seg.lower() == "null":
            extracted[kname] = None
        else:
            try:
                extracted[kname] = int(seg)
            except ValueError:
                try:
                    extracted[kname] = json.loads(seg)
                except Exception:
                    if seg.startswith('"'):
                        extracted[kname] = _unescape_json_fragment(seg[1:])
                    else:
                        extracted[kname] = seg.strip('"')

    return {"name": tool_name, "input": extracted} if extracted else None


def lexical_repair_json(raw: str) -> Optional[Any]:
    """
    Attempts strict `json.loads` first; on failure runs Layer 1 single-pass lexical repair,
    then falls back to Layer 2 `stateful_repair_json` (`KEY_STR` vs `VAL_STR` phase machine).
    """
    s = _strip_code_fence_wrapper(raw)
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        pass

    out: List[str] = []
    in_str = False
    n = len(s)
    i = 0
    while i < n:
        ch = s[i]
        if in_str:
            if ch == "\\":
                if i + 1 < n:
                    nxt = s[i + 1]
                    if nxt == "u" and i + 5 < n and all(c in HEX_CHARS for c in s[i + 2 : i + 6]):
                        out.append(s[i : i + 6])
                        i += 6
                        continue
                    if nxt in VALID_JSON_ESCAPES and nxt != "u":
                        out.append("\\" + nxt)
                        i += 2
                        continue
                    out.append("\\\\")
                    i += 1
                    continue
                else:
                    out.append("\\\\")
                    i += 1
                    continue
            if ch == '"':
                j = i + 1
                while j < n and s[j] in " \t\r\n":
                    j += 1
                if j < n and s[j] not in (",", ":", "}", "]"):
                    out.append('\\"')
                    i += 1
                    continue
                in_str = False
                out.append('"')
                i += 1
                continue
            if ch == "\n":
                out.append("\\n")
                i += 1
                continue
            if ch == "\r":
                out.append("\\r")
                i += 1
                continue
            if ch == "\t":
                out.append("\\t")
                i += 1
                continue
            if ord(ch) < 0x20:
                out.append(f"\\u{ord(ch):04x}")
                i += 1
                continue
            out.append(ch)
            i += 1
        else:
            if ch == '"':
                in_str = True
                out.append('"')
                i += 1
                continue
            if ch == ",":
                j = i + 1
                while j < n and s[j] in " \t\r\n":
                    j += 1
                if j < n and s[j] in ("}", "]"):
                    i += 1
                    continue
            out.append(ch)
            i += 1

    try:
        return json.loads("".join(out))
    except Exception:
        return stateful_repair_json(s)


def _normalize_arg_value(key: str, raw_val: str, attrs: Dict[str, str], cwd: str = "") -> Tuple[str, Any]:
    norm_key = "file_path" if key in FILE_PATH_ALIASES else key
    val = raw_val
    trim_eligible = (
        attrs.get("trim") == "1"
        or norm_key in ("file_path", "command", "description", "old_string", "new_string", "content")
    )
    if trim_eligible and len(val) >= 2 and val.startswith("\n") and val.endswith("\n"):
        val = val[1:-1]

    if norm_key == "file_path" and isinstance(val, str):
        v_clean = val.strip()
        if v_clean and cwd and not os.path.isabs(v_clean) and not v_clean.startswith("~"):
            val = os.path.normpath(os.path.join(cwd, v_clean))
        elif v_clean.startswith("~"):
            val = os.path.expanduser(v_clean)
        else:
            val = v_clean

    atype = attrs.get("type", "").strip().lower()
    if atype == "json":
        parsed_v = lexical_repair_json(val)
        return norm_key, (parsed_v if parsed_v is not None else val)
    if atype == "int":
        try:
            return norm_key, int(val.strip())
        except ValueError:
            return norm_key, val
    if atype == "bool":
        return norm_key, val.strip().lower() in ("true", "1", "yes")
    return norm_key, val


def parse_raw_mode_args(body: str, nonce: str = "", cwd: str = "") -> Dict[str, Any]:
    """
    Extracts `<dsh_arg_{nonce} name="key">value</dsh_arg_{nonce}>` AND permissive `<dsh_arg>` /
    `<arg>` / `<param>` blocks without JSON escaping.
    Nonced arg tags (`<dsh_arg_{nonce}>`) ONLY close on matching `</dsh_arg_{nonce}>` (or next
    sibling `<dsh_arg_{nonce}`), never prematurely on `</arg>` or `</param>` inside content.
    """
    args: Dict[str, Any] = {}
    s = body or ""
    pos = 0
    n = len(s)
    while pos < n:
        m = ANY_ARG_OPEN_RE.search(s, pos)
        if not m:
            break
        tag_name = m.group(1)
        attrs = parse_permissive_attrs(m.group(2))
        key = (attrs.get("name") or attrs.get("key") or "").strip()
        val_start = m.end()

        if tag_name.startswith("dsh_arg_"):
            close_re = re.compile(rf"</{re.escape(tag_name)}\s*>")
            sib_re = re.compile(rf"<{re.escape(tag_name)}\b")
        elif tag_name == "dsh_arg":
            close_re = re.compile(r"</dsh_arg\s*>")
            sib_re = re.compile(r"<dsh_arg(?:_[0-9a-fA-F]{8})?\b")
        else:
            close_re = re.compile(rf"</{re.escape(tag_name)}\s*>")
            sib_re = re.compile(r"<(?:dsh_arg(?:_[0-9a-fA-F]{8})?|arg|param)\b")

        m_sib = sib_re.search(s, val_start)
        bound_end = m_sib.start() if m_sib else n
        m_close = close_re.search(s, val_start, bound_end)
        if not m_close and tag_name in ("arg", "param"):
            alt_m = re.search(r"</(?:arg|param)\s*>", s[val_start:bound_end])
            if alt_m:
                raw_val = s[val_start : val_start + alt_m.start()]
                pos = val_start + alt_m.end()
            else:
                raw_val = s[val_start:bound_end]
                pos = bound_end
        elif m_close:
            raw_val = s[val_start : m_close.start()]
            pos = m_close.end()
        else:
            raw_val = s[val_start:bound_end]
            pos = bound_end

        if key:
            nk, nv = _normalize_arg_value(key, raw_val, attrs, cwd=cwd)
            args[nk] = nv
    return args


def _json_escape_fragment(s: str) -> str:
    if not s:
        return ""
    return json.dumps(s, ensure_ascii=False)[1:-1]


class StreamingEnvelopeParser:
    """
    O(1) tail-window FSM parser converting raw Web stream chunks into ordered tuples:
      - `("thinking", str)`
      - `("text", str)`
      - `("tool_start", {"id": str, "name": str})`              (when `stream_raw_tool_names` matches a `mode="raw"` call)
      - `("tool_delta", {"id": str, "partial_json": str})`      (real-time homomorphic `input_json_delta` chunks)
      - `("tool_stop", {"id": str})`
      - `("tool_call", {"id": str, "name": str, "input": dict, "streamed"?: bool})`
      - `("malformed_envelope", {"raw_preview": str, "attrs": dict, ...})`
    """

    MAX_ENVELOPE_BYTES = 2 * 1024 * 1024  # 2 MB safety ceiling per envelope
    OPEN_PREFIX_CANDIDATES = ("<dsh_call", "<tool_call")

    def __init__(
        self,
        nonce: str,
        respect_code_fences: bool = True,
        stream_raw_tool_names: Optional[Set[str]] = None,
        cwd: str = "",
        known_tools: Optional[Any] = None,
        known_arg_keys: Optional[Any] = None,
    ):
        self.nonce = nonce
        self.open_prefix = f"<dsh_call_{nonce}"
        self.close_tag = f"</dsh_call_{nonce}>"
        self.respect_code_fences = respect_code_fences
        self.cwd = cwd
        self.known_tools = list(known_tools) if known_tools is not None else None
        self.known_arg_keys = list(known_arg_keys) if known_arg_keys is not None else None
        self._stream_tool_lookup: Dict[str, str] = {
            t.lower(): t for t in (stream_raw_tool_names or set())
        }

        self._mode = "TEXT"  # "TEXT" | "TAG_OPEN" | "ENVELOPE_BODY" | "RAW_STREAM_BODY" | "RAW_STREAM_ARG"
        self._buf = ""
        self._open_attrs: Dict[str, str] = {}
        self._active_close_tag = self.close_tag

        # Real-time RAW streaming state
        self._rt_call_id = ""
        self._rt_tool_name = ""
        self._rt_arg_count = 0
        self._rt_cur_key = ""
        self._rt_cur_tag_name = ""
        self._rt_cur_attrs: Dict[str, str] = {}
        self._rt_cur_close_tag = ""
        self._rt_cur_buffered_scalar = False
        self._rt_cur_trim_eligible = False
        self._rt_cur_trim_buf_limit = 65536
        self._rt_cur_leading_nl_decided = True
        self._rt_cur_had_leading_nl = False
        self._rt_cur_strip_wrapper_nl = False
        self._rt_cur_parts: List[str] = []
        self._rt_collected_args: Dict[str, Any] = {}

        self._at_line_start = True
        self._fence_char = ""
        self._fence_len = 0
        self._fence_line_buf = ""

    def feed_thinking(self, delta: str) -> List[Tuple[str, Any]]:
        if not delta:
            return []
        return [("thinking", delta)]

    def _update_fence_state_on_emitted_text(self, emitted: str) -> None:
        if not self.respect_code_fences or not emitted:
            return
        for ch in emitted:
            if ch == "\n":
                self._evaluate_fence_line(self._fence_line_buf)
                self._fence_line_buf = ""
                self._at_line_start = True
            else:
                if len(self._fence_line_buf) < 32:
                    self._fence_line_buf += ch
                self._at_line_start = False

    def _evaluate_fence_line(self, line: str) -> None:
        stripped = line.lstrip(" ")
        if len(line) - len(stripped) > 3:
            return
        if not self._fence_char:
            if stripped.startswith("```") or stripped.startswith("~~~"):
                fc = stripped[0]
                flen = 0
                for c in stripped:
                    if c == fc:
                        flen += 1
                    else:
                        break
                info = stripped[flen:].strip().lower()
                if flen >= 3 and info not in ("xml", "dsh", "tool", "tool_call"):
                    self._fence_char = fc
                    self._fence_len = flen
        else:
            fc = self._fence_char
            if stripped.startswith(fc * self._fence_len) and stripped.rstrip(" \r") == fc * len(stripped.rstrip(" \r")):
                self._fence_char = ""
                self._fence_len = 0

    def _in_blocking_fence(self) -> bool:
        if not self.respect_code_fences:
            return False
        if not self._fence_char and self._fence_line_buf:
            s = self._fence_line_buf.lstrip(" ")
            if len(self._fence_line_buf) - len(s) <= 3 and (s.startswith("```") or s.startswith("~~~")):
                fc = s[0]
                flen = len(s) - len(s.lstrip(fc))
                info = s[flen:].strip().lower()
                if flen >= 3 and info not in ("xml", "dsh", "tool", "tool_call"):
                    return True
        return bool(self._fence_char)

    def _find_earliest_open_candidate(self, s: str) -> int:
        best = -1
        for p in self.OPEN_PREFIX_CANDIDATES:
            idx = s.find(p)
            if idx != -1 and (best == -1 or idx < best):
                best = idx
        return best

    def _tail_might_start_open_tag(self, tail: str) -> bool:
        if not tail.startswith("<"):
            return False
        if any(p.startswith(tail) for p in self.OPEN_PREFIX_CANDIDATES) or self.open_prefix.startswith(tail):
            return True
        # Also hold while `<dsh_call_` + 1..8 hex chars (before '>' or space arrives)
        if tail.startswith("<dsh_call_") and len(tail) <= 18:
            suffix = tail[len("<dsh_call_") :]
            if all(c in HEX_CHARS for c in suffix):
                return True
        return False

    def feed_text(self, chunk: str) -> List[Tuple[str, Any]]:
        if not chunk:
            return []
        self._buf += chunk
        events: List[Tuple[str, Any]] = []

        while self._buf:
            if self._mode == "TEXT":
                idx = self._find_earliest_open_candidate(self._buf)
                if idx != -1:
                    if idx > 0:
                        pre = self._buf[:idx]
                        self._update_fence_state_on_emitted_text(pre)
                        events.append(("text", pre))
                        self._buf = self._buf[idx:]

                    # Check if we have enough bytes to confirm `<dsh_call...` vs `<dsh_call_EXAMPLE>`
                    if self._buf.startswith("<dsh_call_") and len(self._buf) < 19 and ">" not in self._buf and " " not in self._buf:
                        sfx = self._buf[len("<dsh_call_") :]
                        if all(c in HEX_CHARS for c in sfx):
                            break  # Wait for remaining hex chars + delimiter
                    if self._buf in ("<dsh_call", "<tool_call"):
                        break  # Need 1 more char to confirm '>' or whitespace

                    # Validate open prefix boundary
                    matched_close = self.close_tag
                    valid_open = False
                    if self._buf.startswith("<tool_call"):
                        ch_next = self._buf[len("<tool_call")]
                        if ch_next in (">", " ", "\t", "\r", "\n"):
                            valid_open = True
                            matched_close = "</tool_call>"
                    elif self._buf.startswith("<dsh_call_"):
                        rem_s = self._buf[len("<dsh_call_") :]
                        if len(rem_s) >= 9 and all(c in HEX_CHARS for c in rem_s[:8]) and rem_s[8] in (">", " ", "\t", "\r", "\n"):
                            valid_open = True
                            matched_close = f"</dsh_call_{rem_s[:8]}>"
                    elif self._buf.startswith("<dsh_call"):
                        ch_next = self._buf[len("<dsh_call")]
                        if ch_next in (">", " ", "\t", "\r", "\n"):
                            valid_open = True
                            matched_close = "</dsh_call>"

                    if not valid_open or self._in_blocking_fence():
                        step_n = 9 if self._buf.startswith(("<dsh_call", "<tool_cal")) else 1
                        emit_skip = self._buf[:step_n]
                        self._update_fence_state_on_emitted_text(emit_skip)
                        events.append(("text", emit_skip))
                        self._buf = self._buf[step_n:]
                        continue

                    self._active_close_tag = matched_close
                    self._mode = "TAG_OPEN"
                    continue

                # Check tail `<` prefix within 18 chars
                search_start = max(0, len(self._buf) - 18)
                lt_pos = self._buf.rfind("<", search_start)
                if lt_pos != -1 and self._tail_might_start_open_tag(self._buf[lt_pos:]):
                    if lt_pos > 0:
                        safe = self._buf[:lt_pos]
                        self._update_fence_state_on_emitted_text(safe)
                        events.append(("text", safe))
                        self._buf = self._buf[lt_pos:]
                    break
                else:
                    safe = self._buf
                    self._buf = ""
                    self._update_fence_state_on_emitted_text(safe)
                    events.append(("text", safe))
                    break

            elif self._mode == "TAG_OPEN":
                gt_idx = _find_tag_close_gt(self._buf)
                if gt_idx == -1:
                    if len(self._buf) > 2048:
                        bad = self._buf[:240]
                        self._buf = ""
                        self._mode = "TEXT"
                        events.append(("malformed_envelope", {"reason": "unclosed_open_tag", "raw_preview": bad}))
                    break
                m_open = OPEN_CALL_TAG_RE.match(self._buf[: gt_idx + 1])
                if not m_open:
                    self._update_fence_state_on_emitted_text(self._buf[:1])
                    events.append(("text", self._buf[:1]))
                    self._buf = self._buf[1:]
                    self._mode = "TEXT"
                    continue

                self._open_attrs = parse_permissive_attrs(m_open.group(2))
                self._buf = self._buf[gt_idx + 1 :]

                attr_mode = self._open_attrs.get("mode", "").strip().lower()
                raw_tool = (self._open_attrs.get("tool") or self._open_attrs.get("name") or "").strip()
                matched_stream_tool = self._stream_tool_lookup.get(raw_tool.lower(), "")
                if attr_mode == "raw" and matched_stream_tool:
                    self._rt_call_id = "toolu_" + uuid.uuid4().hex[:24]
                    self._rt_tool_name = matched_stream_tool
                    self._rt_arg_count = 0
                    self._rt_collected_args = {}
                    events.append(("tool_start", {"id": self._rt_call_id, "name": self._rt_tool_name}))
                    events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": "{"}))
                    self._mode = "RAW_STREAM_BODY"
                else:
                    self._mode = "ENVELOPE_BODY"

            elif self._mode == "RAW_STREAM_BODY":
                # Look for either next `<dsh_arg...>` open tag OR closing `</dsh_call...>` tag
                m_arg = ANY_ARG_OPEN_RE.search(self._buf)
                close_idx = self._find_envelope_close(self._buf)
                if m_arg and (close_idx == -1 or m_arg.start() < close_idx):
                    tag_nm = m_arg.group(1)
                    attrs = parse_permissive_attrs(m_arg.group(2))
                    raw_key = (attrs.get("name") or attrs.get("key") or "").strip()
                    self._buf = self._buf[m_arg.end() :]
                    if not raw_key:
                        continue
                    self._rt_cur_key = "file_path" if raw_key in FILE_PATH_ALIASES else raw_key
                    self._rt_cur_tag_name = tag_nm
                    self._rt_cur_attrs = attrs
                    self._rt_cur_close_tag = f"</{tag_nm}>"
                    self._rt_cur_parts = []
                    self._rt_cur_buffered_scalar = (
                        self._rt_cur_key in SCALAR_BUFFER_KEYS
                        or attrs.get("type", "").strip().lower() in ("int", "bool", "json")
                    )
                    self._rt_cur_trim_eligible = (
                        attrs.get("trim") == "1"
                        or self._rt_cur_key in ("file_path", "command", "description", "old_string", "new_string", "content")
                    )
                    self._rt_cur_trim_buf_limit = 4096 if self._rt_cur_key == "content" else 65536
                    self._rt_cur_leading_nl_decided = not self._rt_cur_trim_eligible
                    self._rt_cur_had_leading_nl = False
                    self._rt_cur_strip_wrapper_nl = False
                    if not self._rt_cur_buffered_scalar:
                        sep = "," if self._rt_arg_count > 0 else ""
                        events.append((
                            "tool_delta",
                            {"id": self._rt_call_id, "partial_json": sep + json.dumps(self._rt_cur_key, ensure_ascii=False) + ':"'},
                        ))
                    self._mode = "RAW_STREAM_ARG"
                    continue

                if close_idx != -1:
                    _, close_end = self._find_envelope_close_span(self._buf)
                    self._buf = self._buf[close_end:]
                    events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": "}"}))
                    events.append(("tool_stop", {"id": self._rt_call_id}))
                    events.append((
                        "tool_call",
                        {
                            "id": self._rt_call_id,
                            "name": self._rt_tool_name,
                            "input": dict(self._rt_collected_args),
                            "streamed": True,
                        },
                    ))
                    self._reset_rt_state()
                    self._open_attrs = {}
                    self._mode = "TEXT"
                    continue

                # Discard stray whitespace/bytes before the next potential `<` tag
                first_lt = self._buf.find("<")
                if first_lt > 0:
                    self._buf = self._buf[first_lt:]
                elif first_lt == -1 and len(self._buf) > 64:
                    self._buf = ""
                break

            elif self._mode == "RAW_STREAM_ARG":
                end_idx, end_after = self._find_arg_close_span(self._buf, self._rt_cur_tag_name)
                if self._rt_cur_buffered_scalar:
                    if end_idx == -1:
                        if len(self._buf) > 4096:
                            # Fallback: switch oversized scalar to string stream
                            sep = "," if self._rt_arg_count > 0 else ""
                            events.append((
                                "tool_delta",
                                {"id": self._rt_call_id, "partial_json": sep + json.dumps(self._rt_cur_key, ensure_ascii=False) + ':"'},
                            ))
                            self._rt_cur_buffered_scalar = False
                            self._rt_cur_trim_buf_limit = min(self._rt_cur_trim_buf_limit, 4096)
                            continue
                        break
                    raw_scalar = self._buf[:end_idx]
                    self._buf = self._buf[end_after:]
                    nk, nv = _normalize_arg_value(self._rt_cur_key, raw_scalar, self._rt_cur_attrs, cwd=self.cwd)
                    self._rt_collected_args[nk] = nv
                    sep = "," if self._rt_arg_count > 0 else ""
                    events.append((
                        "tool_delta",
                        {"id": self._rt_call_id, "partial_json": sep + json.dumps(nk, ensure_ascii=False) + ":" + json.dumps(nv, ensure_ascii=False)},
                    ))
                    self._rt_arg_count += 1
                    self._mode = "RAW_STREAM_BODY"
                    continue

                # Streaming string argument (`content`, `new_string`, `old_string`, `command`, etc.)
                if not self._rt_cur_leading_nl_decided:
                    if not self._buf:
                        break
                    if self._buf[0] != "\n":
                        self._rt_cur_leading_nl_decided = True
                        self._rt_cur_had_leading_nl = False
                        self._rt_cur_strip_wrapper_nl = False
                    else:
                        self._rt_cur_had_leading_nl = True
                        if end_idx != -1:
                            raw_arg = self._buf[:end_idx]
                            self._buf = self._buf[end_after:]
                            if len(raw_arg) >= 2 and raw_arg.startswith("\n") and raw_arg.endswith("\n"):
                                raw_arg = raw_arg[1:-1]
                            if raw_arg:
                                self._rt_cur_parts.append(raw_arg)
                                events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": _json_escape_fragment(raw_arg)}))
                            events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": '"'}))
                            self._rt_collected_args[self._rt_cur_key] = raw_arg
                            self._rt_arg_count += 1
                            self._mode = "RAW_STREAM_BODY"
                            continue
                        if len(self._buf) <= self._rt_cur_trim_buf_limit:
                            break
                        self._buf = self._buf[1:]
                        self._rt_cur_leading_nl_decided = True
                        self._rt_cur_strip_wrapper_nl = True
                        end_idx, end_after = self._find_arg_close_span(self._buf, self._rt_cur_tag_name)

                if end_idx != -1:
                    tail_val = self._buf[:end_idx]
                    self._buf = self._buf[end_after:]
                    if self._rt_cur_strip_wrapper_nl and tail_val.endswith("\n"):
                        tail_val = tail_val[:-1]
                    if tail_val:
                        self._rt_cur_parts.append(tail_val)
                        events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": _json_escape_fragment(tail_val)}))
                    events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": '"'}))
                    self._rt_collected_args[self._rt_cur_key] = "".join(self._rt_cur_parts)
                    self._rt_arg_count += 1
                    self._mode = "RAW_STREAM_BODY"
                    continue

                holdback = self._compute_arg_holdback(
                    self._buf,
                    self._rt_cur_tag_name,
                    hold_trailing_nl=self._rt_cur_strip_wrapper_nl,
                )
                if len(self._buf) > holdback:
                    split_at = len(self._buf) - holdback
                    emit_part = self._buf[:split_at]
                    self._buf = self._buf[split_at:]
                    self._rt_cur_parts.append(emit_part)
                    events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": _json_escape_fragment(emit_part)}))
                break

            elif self._mode == "ENVELOPE_BODY":
                close_idx, close_end = self._find_envelope_close_span(self._buf)
                if close_idx == -1:
                    if len(self._buf) > self.MAX_ENVELOPE_BYTES:
                        bad_preview = self._buf[:240]
                        self._buf = ""
                        self._mode = "TEXT"
                        events.append(("malformed_envelope", {"reason": "exceeded_max_bytes", "raw_preview": bad_preview}))
                    break
                body_str = self._buf[:close_idx]
                self._buf = self._buf[close_end:]
                attrs = self._open_attrs
                self._open_attrs = {}
                self._mode = "TEXT"

                parsed_calls = self._decode_envelope(attrs, body_str)
                if parsed_calls:
                    for tc in parsed_calls:
                        events.append(("tool_call", tc))
                else:
                    events.append(("malformed_envelope", {"reason": "unparseable_body", "raw_preview": body_str[:240], "attrs": attrs}))

        # Coalesce adjacent text or adjacent tool_delta events for efficiency
        merged: List[Tuple[str, Any]] = []
        for kind, val in events:
            if merged and kind == "text" and merged[-1][0] == "text":
                merged[-1] = ("text", merged[-1][1] + val)
            elif (
                merged
                and kind == "tool_delta"
                and merged[-1][0] == "tool_delta"
                and merged[-1][1]["id"] == val["id"]
            ):
                merged[-1] = ("tool_delta", {"id": val["id"], "partial_json": merged[-1][1]["partial_json"] + val["partial_json"]})
            else:
                merged.append((kind, val))
        return merged

    def _reset_rt_state(self) -> None:
        self._rt_call_id = ""
        self._rt_tool_name = ""
        self._rt_arg_count = 0
        self._rt_cur_key = ""
        self._rt_cur_tag_name = ""
        self._rt_cur_attrs = {}
        self._rt_cur_close_tag = ""
        self._rt_cur_buffered_scalar = False
        self._rt_cur_trim_eligible = False
        self._rt_cur_trim_buf_limit = 65536
        self._rt_cur_leading_nl_decided = True
        self._rt_cur_had_leading_nl = False
        self._rt_cur_strip_wrapper_nl = False
        self._rt_cur_parts = []
        self._rt_collected_args = {}

    @staticmethod
    def _compute_arg_holdback(buf: str, open_tag_name: str, hold_trailing_nl: bool = False) -> int:
        if not buf:
            return 0
        close_prefix = f"</{open_tag_name}"
        last_lt = buf.rfind("<")
        if last_lt != -1:
            tail = buf[last_lt:]
            is_partial = (
                close_prefix.startswith(tail)
                if len(tail) <= len(close_prefix)
                else (tail.startswith(close_prefix) and all(c.isspace() for c in tail[len(close_prefix) :]))
            )
            if is_partial:
                extra = 1 if (hold_trailing_nl and last_lt > 0 and buf[last_lt - 1] == "\n") else 0
                return len(tail) + extra
        if hold_trailing_nl and buf.endswith("\n"):
            return 1
        return 0

    def _find_envelope_close(self, s: str) -> int:
        idx, _ = self._find_envelope_close_span(s)
        return idx

    def _find_envelope_close_span(self, s: str) -> Tuple[int, int]:
        if self._active_close_tag.startswith("</dsh_call_"):
            tag_inner = self._active_close_tag[2:-1]
            m = re.search(rf"</{re.escape(tag_inner)}\s*>", s)
            if m:
                return m.start(), m.end()
            return -1, -1
        idx = s.find(self._active_close_tag)
        if idx != -1:
            return idx, idx + len(self._active_close_tag)
        m = re.search(r"</(?:dsh_call(?:_[0-9a-fA-F]{8})?|tool_call)\s*>", s)
        if m:
            return m.start(), m.end()
        return -1, -1

    @staticmethod
    def _find_arg_close_span(s: str, open_tag_name: str) -> Tuple[int, int]:
        if open_tag_name.startswith("dsh_arg_"):
            m = re.search(rf"</{re.escape(open_tag_name)}\s*>", s)
        elif open_tag_name == "dsh_arg":
            m = re.search(r"</dsh_arg\s*>", s)
        elif open_tag_name in ("arg", "param"):
            m = re.search(rf"</{re.escape(open_tag_name)}\s*>", s)
        else:
            m = re.search(r"</(?:dsh_arg(?:_[0-9a-fA-F]{8})?|arg|param)\s*>", s)
        if m:
            return m.start(), m.end()
        return -1, -1

    def can_early_stop_after_tool_call(self) -> bool:
        if self._mode != "TEXT" or self._in_blocking_fence():
            return False
        rem = self._buf.strip()
        if not rem:
            return True
        if self._find_earliest_open_candidate(rem) != -1:
            return False
        lt_pos = rem.rfind("<")
        if lt_pos != -1 and self._tail_might_start_open_tag(rem[lt_pos:]):
            return False
        return True

    def finish(self) -> List[Tuple[str, Any]]:
        events: List[Tuple[str, Any]] = []
        if self._mode in ("RAW_STREAM_ARG", "RAW_STREAM_BODY"):
            # Stream truncated mid-RAW call: emit V8 duplicate-key poison pill so JSON stays valid
            # while nulling required fields (`file_path`/`command`) so host never writes a half-file!
            in_str = (self._mode == "RAW_STREAM_ARG" and not self._rt_cur_buffered_scalar)
            prefix = '"' if in_str else ""
            sep = "," if (in_str or self._rt_arg_count > 0) else ""
            poison = f'{prefix}{sep}"file_path":null,"command":null,"__dsh_incomplete__":true}}'
            events.append(("tool_delta", {"id": self._rt_call_id, "partial_json": poison}))
            events.append(("tool_stop", {"id": self._rt_call_id}))
            events.append((
                "malformed_envelope",
                {"reason": "truncated_raw_stream", "truncated_raw_stream": True, "streamed": True},
            ))
            self._buf = ""
            self._reset_rt_state()
            self._open_attrs = {}
            self._mode = "TEXT"
            return events

        if self._buf:
            if self._mode == "ENVELOPE_BODY":
                recovered = self._decode_envelope(self._open_attrs, self._buf)
                if recovered:
                    for tc in recovered:
                        events.append(("tool_call", tc))
                else:
                    events.append(("malformed_envelope", {"reason": "truncated_envelope", "raw_preview": self._buf[:240]}))
            elif self._mode == "TAG_OPEN" or (self._mode == "TEXT" and self._buf.startswith(self.open_prefix)):
                events.append(("malformed_envelope", {"reason": "truncated_open_tag", "raw_preview": self._buf[:240]}))
            else:
                events.append(("text", self._buf))
            self._buf = ""
        self._mode = "TEXT"
        self._open_attrs = {}
        return events

    def _decode_envelope(self, attrs: Dict[str, str], body_str: str) -> List[Dict[str, Any]]:
        mode = attrs.get("mode", "").strip().lower()
        attr_tool = (attrs.get("tool") or attrs.get("name") or "").strip()

        stripped_body = (body_str or "").lstrip()
        if mode == "raw" or (not stripped_body.startswith(("{", "[")) and ANY_ARG_OPEN_RE.search(body_str or "")):
            if not attr_tool:
                return []
            raw_args = parse_raw_mode_args(body_str, self.nonce, cwd=self.cwd)
            return [{
                "id": "toolu_" + uuid.uuid4().hex[:24],
                "name": attr_tool,
                "input": raw_args,
            }]

        obj = lexical_repair_json(body_str)
        if obj is None:
            obj = schema_anchor_extract(
                body_str,
                fallback_tool=attr_tool,
                known_tools=self.known_tools,
                known_arg_keys=self.known_arg_keys,
            )
        if obj is None:
            return []

        items = obj if isinstance(obj, list) else [obj]
        out: List[Dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            tname = str(it.get("name") or it.get("tool") or attr_tool or "").strip()
            if not tname:
                continue
            tinp = it.get("input")
            if tinp is None:
                tinp = it.get("arguments")
            if tinp is None:
                tinp = {k: v for k, v in it.items() if k not in ("name", "tool", "id")}
            if isinstance(tinp, str):
                parsed_in = lexical_repair_json(tinp)
                tinp = parsed_in if isinstance(parsed_in, dict) else {"value": tinp}
            if not isinstance(tinp, dict):
                tinp = {}
            out.append({
                "id": str(it.get("id") or ("toolu_" + uuid.uuid4().hex[:24])),
                "name": tname,
                "input": tinp,
            })
        return out


def _format_schema_type(spec: Any, depth: int = 0) -> str:
    if not isinstance(spec, dict):
        return "any"
    t = spec.get("type")
    if isinstance(t, list):
        base = "|".join(str(x) for x in t)
    elif t:
        base = str(t)
    elif "properties" in spec:
        base = "object"
    elif "items" in spec:
        base = "array"
    elif "enum" in spec:
        base = "enum"
    else:
        base = "any"

    if base == "array" and isinstance(spec.get("items"), dict) and depth < 2:
        base = f"array<{_format_schema_type(spec['items'], depth + 1)}>"
    elif base == "object" and isinstance(spec.get("properties"), dict) and depth < 2:
        nprops = spec["properties"]
        nreqs = set(spec.get("required") or [])
        nparts = []
        for nk, nv in nprops.items():
            nparts.append(f"{nk}{'*' if nk in nreqs else ''}:{_format_schema_type(nv, depth + 1)}")
        if nparts:
            base = "object{" + ", ".join(nparts) + "}"

    if isinstance(spec.get("enum"), list) and spec["enum"]:
        choices = ", ".join(json.dumps(c, ensure_ascii=False) for c in spec["enum"])
        base += f" enum=[{choices}]"
    return base


def _format_tool_choice_directive(tool_choice: Any) -> str:
    if not tool_choice:
        return ""
    if isinstance(tool_choice, str):
        tc = tool_choice.strip().lower()
        if tc in ("any", "required"):
            return "TOOL CHOICE DIRECTIVE: You MUST call at least one tool from [ACTIVE TOOLS CATALOG] in this turn; do NOT respond with plain text alone."
        if tc == "none":
            return "TOOL CHOICE DIRECTIVE: Do NOT call any tools in this turn; respond with plain text only."
        return ""
    if isinstance(tool_choice, dict):
        tc_type = str(tool_choice.get("type", "")).strip().lower()
        if tc_type in ("any", "required"):
            return "TOOL CHOICE DIRECTIVE: You MUST call at least one tool from [ACTIVE TOOLS CATALOG] in this turn; do NOT respond with plain text alone."
        if tc_type == "tool" and tool_choice.get("name"):
            tname = str(tool_choice["name"]).strip()
            return f"TOOL CHOICE DIRECTIVE: You MUST call tool `{tname}` in this turn."
        if tc_type == "function":
            fn = (tool_choice.get("function") or {}).get("name")
            if fn:
                return f"TOOL CHOICE DIRECTIVE: You MUST call tool `{str(fn).strip()}` in this turn."
        if tc_type == "none":
            return "TOOL CHOICE DIRECTIVE: Do NOT call any tools in this turn; respond with plain text only."
    return ""


def build_dsh_tp_system_instructions(
    nonce: str,
    tools_catalog: List[dict],
    embedded_mcp_tools: Optional[List[dict]] = None,
    cwd: str = "",
    tool_choice: Optional[Any] = None,
) -> str:
    """Builds the DSH-TP v1.2 ultra-compact client-framed tool-calling contract (-80% protocol tax)."""
    all_tools: List[dict] = []
    seen: Set[str] = set()
    for t in (tools_catalog or []):
        if not isinstance(t, dict) or not t.get("name"):
            continue
        if t["name"] not in seen:
            seen.add(t["name"])
            all_tools.append(t)
    for mt in (embedded_mcp_tools or []):
        if not isinstance(mt, dict) or not mt.get("name"):
            continue
        if mt["name"] not in seen:
            seen.add(mt["name"])
            all_tools.append(mt)

    if not all_tools:
        return ""

    names_set = {t["name"] for t in all_tools}
    has_write = "Write" in names_set
    has_edit = "Edit" in names_set
    has_bash = "Bash" in names_set
    has_read = "Read" in names_set
    cwd_hint = f" under `{cwd}`" if cwd else ""

    lines = [
        f"[DSH-TP v1.2 | TURN_NONCE={nonce}]",
        (
            f"This local coding IDE executes host tools on your behalf when you output top-level `<dsh_call_{nonce}>...</dsh_call_{nonce}>` outside code fences "
            f"(write/edit files via tools instead of pasting full files or opening Canvas; stop after emitting tool calls and wait for `[Tool Result]`)."
        ),
    ]

    tc_directive = _format_tool_choice_directive(tool_choice)
    if tc_directive:
        lines.append(tc_directive)

    lines.append(
        f'JSON: <dsh_call_{nonce}>{{"name":"Tool","input":{{...}}}}</dsh_call_{nonce}> | '
        f'RAW (multi-line/code, no JSON escaping; optional type="int"|"bool"|"json"): '
        f'<dsh_call_{nonce} tool="Write" mode="raw"><dsh_arg_{nonce} name="file_path">{cwd or "/abs/path"}/f</dsh_arg_{nonce}>'
        f'<dsh_arg_{nonce} name="content">...</dsh_arg_{nonce}></dsh_call_{nonce}>'
    )
    meta_notes = ["Never write live nonce in prose (use `<dsh_call_EXAMPLE>`)."]
    if has_bash:
        meta_notes.append("`Bash` runs in isolated subshells (`cd` does not persist).")
    if has_write or has_edit or has_read:
        meta_notes.append(f"Pass full absolute `file_path`{cwd_hint}.")
    lines.append(" ".join(meta_notes))
    lines.append("[TOOLS]")

    for t in all_tools:
        schema = t.get("parameters") or t.get("inputSchema") or {}
        props = schema.get("properties") or {}
        reqs = set(schema.get("required") or [])
        origin_tag = "[MCP] " if t.get("is_embedded_mcp") else ""
        sig_parts = []
        for k, v in props.items():
            v_dict = v if isinstance(v, dict) else {}
            vtype = _format_schema_type(v_dict)
            sig_parts.append(f"{k}{'*' if k in reqs else ''}:{vtype}")
        sig_str = ", ".join(sig_parts)
        tdesc = re.sub(r"\s+", " ", str(t.get("description", ""))).strip()
        if len(tdesc) > 120:
            tdesc = tdesc[:117] + "..."
        lines.append(f"- {origin_tag}{t['name']}({sig_str}) — {tdesc}")

    missing_search = [n for n in ("Glob", "Grep") if n not in names_set]
    if missing_search and (has_bash or has_read):
        alts = []
        if has_bash:
            alts.append("`Bash` (`rg`/`find`/`ls`)")
        if has_read:
            alts.append("`Read`")
        lines.append(f"Note: {'/'.join(missing_search)} inactive; use {' + '.join(alts)}.")
    return "\n".join(lines)
