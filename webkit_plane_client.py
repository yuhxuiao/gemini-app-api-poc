#!/usr/bin/env python3
"""
webkit_plane_client.py - Plane 1 Client, WrbStreamFrameDecoder, Gemini 4 Carbon/Hydrogen Routing & Priority AIMD Scheduler
Launches `bin/DSHWebKitBridge.app` cleanly via macOS LaunchServices (`/usr/bin/open -g -a`)
so `WKWebView` & `NSWindow` always hold full Aqua WindowServer rights (never `setsid`-detached),
and communicates over `0600` POSIX `AF_UNIX` socket (`/tmp/dsh_webkit_{port}.sock`).

v4.5 Resilient Highlights:
  - Aligns with November 2026 (`boq_gemini-web-uiserver_20261115+`) model lineup & `otAQ7b` policy:
    `Carbon` (`95a4c0abcba24364`, `mode=8`), `4 Argon` (`1bc6b5d98741cd3d`, policy-gated), `4 Hydrogen` (`56fdd199312815e2`, `mode=1`),
    `Flash-Lite` (`1d44b34bcaa1c04d`, `mode=6`), and verified thinking wire mapping (`low=1`, `medium=4`, `high/xhigh=2`).
  - Policy-aware `_discovered_hex_for_mode` (never lets disabled `4 Argon` or synthetic `Model_*` hijack `4 Hydrogen` / `4 Carbon`).
  - Two-phase semantic stream timeouts (`firstWrbTimeoutMs` + `idleTimeoutMs`) defeating BOQ keep-alive heartbeat hangs,
    multi-attempt `HTTP_429 [1095]` backoff + automatic `Carbon` -> `4 Hydrogen High` quota/timeout step-down, and
    `background_drain` early-stop so server-side `(cid, rid, rcid)` cursors commit cleanly without `3.0s` client stalls.
"""

from contextlib import contextmanager
import fcntl
import heapq
import json
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

from streaming_envelope_parser import StreamingEnvelopeParser

EXPECTED_BRIDGE_VERSION = "5.0.0-pure-api"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PROTO_PROFILE_PATH = os.environ.get(
    "DSH_WEB_PROTO_PROFILE",
    os.path.join(SCRIPT_DIR, "runtime", "web_proto_profile.json"),
)

# Strict 1:1 native model wire specs (zero cross-model auto-routing)
NATIVE_MODEL_WIRE_SPECS: Dict[str, Dict[str, Any]] = {
    "gemini-4-neon": {"mode": 3, "hex": "797f3d0293f288ad", "default_think_wire": 2},
    "gemini-4-carbon": {"mode": 8, "hex": "95a4c0abcba24364", "default_think_wire": 2},
    "gemini-4-hydrogen": {"mode": 1, "hex": "56fdd199312815e2", "default_think_wire": 2},
    "gemini-4-argon": {"mode": 8, "hex": "1bc6b5d98741cd3d", "default_think_wire": 2},
    "gemini-3.5-flash-lite": {"mode": 6, "hex": "1d44b34bcaa1c04d", "default_think_wire": 1},
}

MODE_HEX_DEFAULTS: Dict[int, str] = {
    1: "56fdd199312815e2",  # Gemini 4 Hydrogen
    3: "797f3d0293f288ad",  # Gemini 4 Neon
    6: "1d44b34bcaa1c04d",  # Gemini 3.5 Flash-Lite
    8: "95a4c0abcba24364",  # Gemini 4 Carbon
}

KNOWN_HEX_MODES: Dict[str, int] = {
    "95a4c0abcba24364": 8,
    "1bc6b5d98741cd3d": 8,
    "797f3d0293f288ad": 3,
    "e6fa609c3fa255c0": 3,
    "56fdd199312815e2": 1,
    "1d44b34bcaa1c04d": 6,
}

DEFAULT_CARBON_CAPACITY: List[int] = [4, 33, 22, 5, 6, 8, 16, 19, 20, 38]

TIER_THINK_WIRE_MAP: Dict[str, int] = {
    "disabled": 1,
    "none": 1,
    "minimal": 1,
    "low": 1,
    "medium": 4,
    "high": 2,
    "xhigh": 2,
    "max": 2,
}

MODEL_DEFAULT_THINK_WIRE: Dict[str, int] = {
    "gemini-4-neon": 2,
    "gemini-4-carbon": 2,
    "gemini-4-hydrogen": 2,
    "gemini-4-argon": 2,
    "gemini-3.5-flash-lite": 1,
}

DOWNGRADE_COOLDOWN_SEC = 1800.0
FORBIDDEN_OVERRIDE_HEADERS = {"cookie", "authorization", "host", "content-length", "content-type", "origin", "referer"}
PROTECTED_EXT_HEADERS = {"x-goog-ext-525001261-jspb", "x-goog-ext-525005358-jspb"}
IMMERSIVE_CHIP_RE = re.compile(r"https?://googleusercontent\.com/immersive_entry_chip/(\d+)")
ROUTED_MODEL_RE = re.compile(r"(?i)(Argon|Pro|Flash|Hydrogen|Carbon|Neon|Gemini)")
CANNED_UPSTREAM_PREFIXES = (
    "i seem to be encountering an error",
    "i encountered an error doing what you asked",
    "sorry, something went wrong",
    "i'm having a hard time fulfilling your request",
    "i am having a hard time fulfilling your request",
    "我似乎遇到了错误",
    "我似乎遇到错误",
    "我似乎遇到了一点错误",
    "抱歉，出了点问题",
    "抱歉,出了点问题",
    "抱歉，出了點問題",
    "抱歉，发生错误",
    "抱歉，发生了错误",
    "抱歉，發生錯誤",
    "出错了，请重试",
    "出错了，请稍后重试",
    "出錯了，請重試",
    "请求出错",
    "請求出錯",
    "发生错误",
    "发生了错误",
    "發生錯誤",
)
CANNED_UPSTREAM_ERR_RE = re.compile(
    r"(?is)^\s*(?:"
    r"(?:i seem to be encountering an error|i encountered an error doing what you asked|"
    r"sorry,\s*something went wrong|i['’]?m having a hard time fulfilling your request|i am having a hard time fulfilling your request)\b"
    r"|(?:我似乎遇到了一?[点點]?[错錯][误誤]|我似乎遇到[错錯][误誤]|抱歉[,，]\s*出了[点點][问問][题題]|抱歉[,，]\s*[发發]生了?[错錯][误誤]|"
    r"出[错錯]了[,，]\s*[请請](?:稍[后後])?重[试試]|[请請]求出[错錯]|[发發]生了?[错錯][误誤])(?=$|[。！!?.，,\s：:])"
    r")"
)
CTRL94_THOUGHT_TAG = "<ctrl94>thought"
CTRL95_TAG = "<ctrl95>"


@contextmanager
def _flock_guard(lock_path: str, exclusive: bool = True, create_parent: bool = True) -> Iterator[None]:
    """Acquire an advisory inter-process flock on `lock_path` if possible."""
    if not lock_path:
        yield
        return
    parent = os.path.dirname(os.path.abspath(lock_path))
    if create_parent:
        try:
            os.makedirs(parent, exist_ok=True)
        except OSError:
            pass
    elif not os.path.isdir(parent):
        yield
        return
    lfp = None
    try:
        lfp = open(lock_path, "a+", encoding="utf-8")
        fcntl.flock(lfp.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
    except OSError:
        if lfp is not None:
            try:
                lfp.close()
            except Exception:
                pass
            lfp = None
    try:
        yield
    finally:
        if lfp is not None:
            try:
                fcntl.flock(lfp.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                lfp.close()
            except Exception:
                pass


class WebProtoProfileCache:
    """
    Hot-reloads sniffed & `otAQ7b`-discovered Web protocol metadata from `runtime/web_proto_profile.json`
    keyed by `st_mtime_ns`, and tracks per-mode silent-downgrade cooldowns with inter-process `fcntl.flock`.
    """

    def __init__(self, path: str = DEFAULT_PROTO_PROFILE_PATH):
        self.path = path
        self._lock = threading.Lock()
        self._mtime_ns: int = -1
        self._size: int = -1
        self._ino: int = -1
        self._cached: Dict[str, Any] = {}
        self._downgraded_until: Dict[int, float] = {}

    def load(self) -> Dict[str, Any]:
        if not self.path:
            return {}
        lock_path = f"{self.path}.lock"
        with self._lock:
            with _flock_guard(lock_path, exclusive=False, create_parent=False):
                try:
                    st = os.stat(self.path)
                except OSError:
                    self._cached = {}
                    self._mtime_ns = -1
                    self._size = -1
                    self._ino = -1
                    return {}
                if (
                    st.st_mtime_ns == self._mtime_ns
                    and st.st_size == self._size
                    and st.st_ino == self._ino
                ):
                    return dict(self._cached)
                try:
                    with open(self.path, "r", encoding="utf-8") as fp:
                        raw = json.load(fp)
                    if isinstance(raw, dict):
                        self._cached = raw
                        self._mtime_ns = st.st_mtime_ns
                        self._size = st.st_size
                        self._ino = st.st_ino
                        persisted_dg = raw.get("downgraded_until")
                        if isinstance(persisted_dg, dict):
                            now = time.time()
                            for k_str, exp_ts in persisted_dg.items():
                                try:
                                    k_i = int(k_str)
                                    exp_f = float(exp_ts)
                                    if exp_f > now:
                                        self._downgraded_until[k_i] = max(
                                            self._downgraded_until.get(k_i, 0.0), exp_f
                                        )
                                except Exception:
                                    pass
                except Exception:
                    pass
                return dict(self._cached)

    def mark_mode_downgraded(self, mode: int, cooldown_sec: float = DOWNGRADE_COOLDOWN_SEC) -> None:
        now = time.time()
        exp = now + cooldown_sec
        mode_i = int(mode)
        lock_path = f"{self.path}.lock" if self.path else ""
        with self._lock:
            self._downgraded_until[mode_i] = exp
            if not self.path:
                return
            with _flock_guard(lock_path, exclusive=True, create_parent=True):
                try:
                    # Re-read latest on-disk JSON under exclusive lock so Swift-written
                    # `policy_models`, `discovered_models`, `bl`, `capacity`, etc. are preserved.
                    disk_obj: Dict[str, Any] = {}
                    if os.path.exists(self.path):
                        try:
                            with open(self.path, "r", encoding="utf-8") as fp:
                                loaded = json.load(fp)
                            if isinstance(loaded, dict):
                                disk_obj = loaded
                        except Exception:
                            disk_obj = dict(self._cached)
                    elif self._cached:
                        disk_obj = dict(self._cached)

                    dg_map: Dict[str, float] = {}
                    persisted_dg = disk_obj.get("downgraded_until")
                    if isinstance(persisted_dg, dict):
                        for k_str, exp_ts in persisted_dg.items():
                            try:
                                k_i = int(k_str)
                                exp_f = float(exp_ts)
                                if exp_f > now:
                                    dg_map[str(k_i)] = exp_f
                                    self._downgraded_until[k_i] = max(
                                        self._downgraded_until.get(k_i, 0.0), exp_f
                                    )
                            except Exception:
                                pass
                    for k_i, exp_f in list(self._downgraded_until.items()):
                        if exp_f > now:
                            dg_map[str(k_i)] = max(dg_map.get(str(k_i), 0.0), exp_f)
                        else:
                            self._downgraded_until.pop(k_i, None)
                    if exp > now:
                        dg_map[str(mode_i)] = exp
                        self._downgraded_until[mode_i] = exp
                    else:
                        dg_map.pop(str(mode_i), None)
                        self._downgraded_until.pop(mode_i, None)
                    disk_obj["downgraded_until"] = dg_map

                    parent = os.path.dirname(os.path.abspath(self.path))
                    os.makedirs(parent, exist_ok=True)
                    tmp_p = f"{self.path}.dg.{os.getpid()}.{threading.get_ident()}"
                    with open(tmp_p, "w", encoding="utf-8") as fp:
                        json.dump(disk_obj, fp, indent=2, ensure_ascii=False)
                    os.replace(tmp_p, self.path)
                    self._cached = disk_obj
                    try:
                        st = os.stat(self.path)
                        self._mtime_ns = st.st_mtime_ns
                        self._size = st.st_size
                        self._ino = st.st_ino
                    except OSError:
                        pass
                except Exception:
                    pass

    def merge_bridge_snapshot(self, snap: Dict[str, Any]) -> None:
        """Persist `otAQ7b` discovery & sniffed headers fetched over UNIX socket when `.app` lacks Desktop TCC."""
        if not self.path or not isinstance(snap, dict) or not snap.get("ok"):
            return
        lock_path = f"{self.path}.lock"
        with self._lock:
            with _flock_guard(lock_path, exclusive=True, create_parent=True):
                try:
                    disk_obj: Dict[str, Any] = {}
                    if os.path.exists(self.path):
                        try:
                            with open(self.path, "r", encoding="utf-8") as fp:
                                loaded = json.load(fp)
                            if isinstance(loaded, dict):
                                disk_obj = loaded
                        except Exception:
                            disk_obj = dict(self._cached)
                    bl = str(snap.get("bl") or "").strip()
                    if bl:
                        disk_obj["bl"] = bl
                    auth_user = str(snap.get("auth_user") or "").strip()
                    if auth_user:
                        disk_obj["auth_user"] = auth_user
                    cap = snap.get("capacity")
                    if isinstance(cap, list) and cap and all(isinstance(x, int) and not isinstance(x, bool) for x in cap):
                        prev_cap = disk_obj.get("capacity") or []
                        if not isinstance(prev_cap, list) or len(cap) >= len(prev_cap):
                            disk_obj["capacity"] = cap
                    dm = snap.get("discovered_models")
                    if isinstance(dm, dict) and dm:
                        disk_obj["discovered_models"] = dm
                    pm = snap.get("policy_models")
                    if isinstance(pm, (dict, list)) and pm:
                        disk_obj["policy_models"] = pm
                    eh = snap.get("extra_headers")
                    if isinstance(eh, dict) and eh:
                        cur_h = dict(disk_obj.get("extra_headers") or {})
                        for k, v in eh.items():
                            if isinstance(k, str) and isinstance(v, str) and v:
                                cur_h[k] = v
                        disk_obj["extra_headers"] = cur_h
                    disk_obj["updated_at"] = int(time.time())

                    parent = os.path.dirname(os.path.abspath(self.path))
                    os.makedirs(parent, exist_ok=True)
                    tmp_p = f"{self.path}.sync.{os.getpid()}.{threading.get_ident()}"
                    with open(tmp_p, "w", encoding="utf-8") as fp:
                        json.dump(disk_obj, fp, indent=2, ensure_ascii=False)
                    os.replace(tmp_p, self.path)
                    self._cached = disk_obj
                    try:
                        st = os.stat(self.path)
                        self._mtime_ns = st.st_mtime_ns
                        self._size = st.st_size
                        self._ino = st.st_ino
                    except OSError:
                        pass
                except Exception:
                    pass

    def is_mode_downgraded(self, mode: int) -> bool:
        now = time.time()
        with self._lock:
            exp = self._downgraded_until.get(int(mode), 0.0)
            if exp <= 0.0:
                return False
            if now >= exp:
                self._downgraded_until.pop(int(mode), None)
                return False
            return True

    def _extract_capacity(self, prof: Dict[str, Any]) -> List[int]:
        best: List[int] = [4]
        cap = prof.get("capacity")
        if isinstance(cap, list) and cap and all(isinstance(x, int) and not isinstance(x, bool) for x in cap):
            if len(cap) > len(best):
                best = list(cap)
        base_h = prof.get("extra_headers")
        if isinstance(base_h, dict):
            raw_525 = base_h.get("x-goog-ext-525001261-jspb")
            if isinstance(raw_525, str) and raw_525.startswith("["):
                try:
                    arr = json.loads(raw_525)
                    if isinstance(arr, list) and len(arr) > 8 and isinstance(arr[8], list):
                        ints = [int(x) for x in arr[8] if isinstance(x, int) and not isinstance(x, bool)]
                        if len(ints) > len(best):
                            best = ints
                except Exception:
                    pass
        if len(best) <= 1 and (self._evaluate_policy_models(prof, 8) is True or self._has_discovered_mode(prof, 8)):
            best = list(DEFAULT_CARBON_CAPACITY)
        return best

    def _classify_discovered_entry_mode(self, key: str, info: Dict[str, Any]) -> int:
        hx = str(info.get("hex") or "").strip().lower()
        if hx in KNOWN_HEX_MODES:
            return KNOWN_HEX_MODES[hx]
        lbl = str(info.get("label") or key or "").strip().lower()
        raw_mode = info.get("mode")
        is_synthetic = bool(info.get("synthetic")) or lbl.startswith("model_")
        if any(w in lbl for w in ("carbon", "argon", "ultra")):
            return 8
        if any(w in lbl for w in ("pro", "thinking")):
            return 3
        if "lite" in lbl:
            return 6
        if any(w in lbl for w in ("hydrogen", "flash")):
            return 1
        if isinstance(raw_mode, int) and not isinstance(raw_mode, bool) and 1 <= raw_mode <= 16:
            if is_synthetic and raw_mode == 1:
                return 0
            return raw_mode
        if key.strip().isdigit():
            ki = int(key.strip())
            if 1 <= ki <= 16:
                return ki
        return 0 if is_synthetic else 1

    @staticmethod
    def _entry_is_enabled(info: Dict[str, Any]) -> bool:
        if info.get("disabled") is True:
            return False
        if "enabled" in info:
            return bool(info["enabled"])
        if "is_enabled" in info:
            return bool(info["is_enabled"])
        return True

    def _policy_token_sets_for_mode(self, prof: Dict[str, Any], target_mode: int) -> Tuple[Set[str], Set[str]]:
        """Extracts (enabled_tokens, disabled_tokens) such as {'carbon'} vs {'argon'} from `policy_models`."""
        enabled_toks: Set[str] = set()
        disabled_toks: Set[str] = set()
        pm = prof.get("policy_models")
        if not pm:
            return enabled_toks, disabled_toks

        def _collect_tokens(raw_lbl: str, raw_slug: str) -> Set[str]:
            toks: Set[str] = set()
            for src in (raw_lbl, raw_slug):
                s = (src or "").strip().lower()
                if not s:
                    continue
                toks.add(s)
                for w in re.findall(r"[a-z][a-z0-9_-]{2,}", s):
                    if w not in ("gemini", "model", "multi"):
                        toks.add(w)
            return toks

        items: List[Tuple[str, Dict[str, Any], bool]] = []
        if isinstance(pm, dict):
            for k, v in pm.items():
                if isinstance(v, bool):
                    items.append((str(k), {}, v))
                elif isinstance(v, dict):
                    items.append((str(k), v, self._entry_is_enabled(v)))
        elif isinstance(pm, list):
            for it in pm:
                if isinstance(it, dict):
                    k = str(it.get("label") or it.get("name") or it.get("id") or "")
                    items.append((k, it, self._entry_is_enabled(it)))

        for k, info, is_en in items:
            eff_mode = self._classify_discovered_entry_mode(k, info)
            if eff_mode != target_mode:
                continue
            toks = _collect_tokens(str(info.get("label") or k), str(info.get("slug") or ""))
            if is_en:
                enabled_toks.update(toks)
            else:
                disabled_toks.update(toks)
        disabled_toks -= enabled_toks
        return enabled_toks, disabled_toks

    def _evaluate_policy_models(self, prof: Dict[str, Any], target_mode: int) -> Optional[bool]:
        pm = prof.get("policy_models")
        if not pm:
            return None
        saw_target = False
        any_enabled = False
        has_any_entry = False

        if isinstance(pm, dict):
            for k, v in pm.items():
                has_any_entry = True
                if isinstance(v, bool):
                    eff_mode = self._classify_discovered_entry_mode(str(k), {})
                    enabled = v
                elif isinstance(v, dict):
                    eff_mode = self._classify_discovered_entry_mode(str(k), v)
                    enabled = self._entry_is_enabled(v)
                else:
                    continue
                if eff_mode == target_mode:
                    saw_target = True
                    if enabled:
                        any_enabled = True
        elif isinstance(pm, list):
            for item in pm:
                if isinstance(item, int) and not isinstance(item, bool):
                    has_any_entry = True
                    if item == target_mode:
                        saw_target = True
                        any_enabled = True
                elif isinstance(item, dict):
                    has_any_entry = True
                    key = str(item.get("label") or item.get("name") or item.get("id") or "")
                    eff_mode = self._classify_discovered_entry_mode(key, item)
                    if eff_mode == target_mode:
                        saw_target = True
                        if self._entry_is_enabled(item):
                            any_enabled = True

        if not has_any_entry:
            return None
        if any_enabled:
            return True
        if saw_target:
            return False
        return False

    def _discovered_hex_for_mode(self, prof: Dict[str, Any], target_mode: int) -> Optional[str]:
        dm = prof.get("discovered_models")
        if not isinstance(dm, dict) or not dm:
            return None
        enabled_toks, disabled_toks = self._policy_token_sets_for_mode(prof, target_mode)
        default_hx = (MODE_HEX_DEFAULTS.get(target_mode) or "").lower()
        scored: List[Tuple[int, str, str]] = []

        for k, info in dm.items():
            if not isinstance(info, dict) or not self._entry_is_enabled(info):
                continue
            hx = str(info.get("hex", "")).strip()
            if len(hx) != 16:
                continue
            hx_low = hx.lower()
            lbl = str(info.get("label") or k or "").strip()
            lbl_low = lbl.lower()
            is_synth = bool(info.get("synthetic")) or lbl_low.startswith("model_") or bool(re.fullmatch(r"[0-9a-fA-F]{16}", str(k)))

            eff_mode = self._classify_discovered_entry_mode(str(k), info)
            if eff_mode != target_mode:
                continue
            if is_synth and default_hx:
                continue

            entry_words = set(re.findall(r"[a-z][a-z0-9_-]{2,}", lbl_low)) | {lbl_low}
            if hx_low == "1bc6b5d98741cd3d":
                entry_words.add("argon")
            elif hx_low == "95a4c0abcba24364":
                entry_words.add("carbon")
            elif hx_low == "56fdd199312815e2":
                entry_words.add("hydrogen")

            if disabled_toks and (entry_words & disabled_toks) and not (entry_words & enabled_toks):
                continue

            if enabled_toks and (entry_words & enabled_toks):
                rank = 0
            elif default_hx and hx_low == default_hx:
                rank = 1
            elif not is_synth:
                rank = 2
            else:
                rank = 3
            scored.append((rank, str(k), hx))

        if not scored:
            return None
        scored.sort(key=lambda item: (item[0], item[1]))
        return scored[0][2]

    def _has_discovered_mode(self, prof: Dict[str, Any], target_mode: int) -> bool:
        if self._discovered_hex_for_mode(prof, target_mode) is not None:
            return True
        dm = prof.get("discovered_models")
        if isinstance(dm, dict):
            for k, info in dm.items():
                if not isinstance(info, dict) or not self._entry_is_enabled(info):
                    continue
                if self._classify_discovered_entry_mode(str(k), info) == target_mode:
                    return True
        return False

    def _account_supports_argon_or_carbon(self, prof: Dict[str, Any]) -> bool:
        if self.is_mode_downgraded(8):
            return False
        policy_decision = self._evaluate_policy_models(prof, 8)
        if policy_decision is False:
            return False
        dm = prof.get("discovered_models")
        if isinstance(dm, dict) and len(dm) > 0:
            if not self._has_discovered_mode(prof, 8):
                return False
            return True if policy_decision is None else policy_decision
        if policy_decision is not None:
            return policy_decision
        cap = self._extract_capacity(prof)
        return 8 in cap

    def _account_supports_mode3_pro(self, prof: Dict[str, Any]) -> bool:
        if self.is_mode_downgraded(3):
            return False
        policy_decision = self._evaluate_policy_models(prof, 3)
        if policy_decision is False:
            return False
        dm = prof.get("discovered_models")
        if isinstance(dm, dict) and len(dm) > 0:
            return self._has_discovered_mode(prof, 3)
        if policy_decision is True:
            return True
        return False

    def _resolve_think_mode_from_prof(self, prof: Dict[str, Any], model_id: str, tier: str = "") -> int:
        t_clean = (tier or "").strip().lower()
        custom_thk = prof.get("think_modes")
        if isinstance(custom_thk, dict) and t_clean in custom_thk:
            val = custom_thk[t_clean]
            if isinstance(val, int) and not isinstance(val, bool) and 0 <= val <= 8:
                return val
        if t_clean in TIER_THINK_WIRE_MAP:
            return TIER_THINK_WIRE_MAP[t_clean]
        spec = NATIVE_MODEL_WIRE_SPECS.get((model_id or "").strip().lower())
        if spec:
            return int(spec["default_think_wire"])
        return MODEL_DEFAULT_THINK_WIRE.get(model_id, 2)

    def resolve_think_mode(self, model_id: str, tier: str = "") -> int:
        return self._resolve_think_mode_from_prof(self.load(), model_id, tier=tier)

    def _resolve_model_slots_from_prof(self, prof: Dict[str, Any], model_id: str, tier: str = "") -> Tuple[int, int]:
        # Pure-API strict 1:1 model pinning: NEVER cross-route or substitute another model.
        m_low = (model_id or "").strip().lower()
        think_wire = self._resolve_think_mode_from_prof(prof, m_low, tier=tier)
        custom_map = prof.get("model_slots")
        if isinstance(custom_map, dict):
            pair = custom_map.get(m_low) or custom_map.get(model_id)
            if (
                isinstance(pair, (list, tuple))
                and len(pair) == 2
                and isinstance(pair[0], int) and not isinstance(pair[0], bool)
                and isinstance(pair[1], int) and not isinstance(pair[1], bool)
                and 0 <= pair[0] <= 32
                and 0 <= pair[1] <= 32
            ):
                return (pair[0], think_wire if tier else pair[1])

        spec = NATIVE_MODEL_WIRE_SPECS.get(m_low)
        if spec is not None:
            return (int(spec["mode"]), think_wire)
        raise ValueError(f"Unsupported model_id for pure-API routing: {model_id!r}")

    def resolve_model_slots(self, model_id: str, tier: str = "") -> Tuple[int, int]:
        return self._resolve_model_slots_from_prof(self.load(), model_id, tier=tier)

    def _resolve_model_hex_from_prof(self, prof: Dict[str, Any], slot_79: int, model_id: str = "") -> Optional[str]:
        spec = NATIVE_MODEL_WIRE_SPECS.get((model_id or "").strip().lower())
        if spec and spec.get("hex"):
            return str(spec["hex"])
        disc = self._discovered_hex_for_mode(prof, slot_79)
        if disc:
            return disc
        return MODE_HEX_DEFAULTS.get(slot_79)

    def resolve_model_hex(self, slot_79: int, model_id: str = "") -> Optional[str]:
        return self._resolve_model_hex_from_prof(self.load(), slot_79, model_id=model_id)

    def resolve_extra_headers(
        self,
        model_id: str,
        tier: str = "",
        req_uuid: str = "",
        hl: str = "zh-CN",
        force_default_hex: bool = False,
        prof_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        prof = prof_snapshot if prof_snapshot is not None else self.load()
        slot_79, slot_80 = self._resolve_model_slots_from_prof(prof, model_id, tier=tier)
        spec = NATIVE_MODEL_WIRE_SPECS.get((model_id or "").strip().lower())
        if force_default_hex:
            model_hex = (spec["hex"] if spec else MODE_HEX_DEFAULTS.get(slot_79))
        else:
            model_hex = self._resolve_model_hex_from_prof(prof, slot_79, model_id=model_id)
        capacity = self._extract_capacity(prof)
        bl = str(prof.get("bl") or "").strip()
        ruuid = req_uuid or str(uuid.uuid4()).upper()

        # Template-First 28-element x-goog-ext-525001261-jspb (preserves live upstream trailing slots & timezone)
        ext_525_arr: Optional[List[Any]] = None
        base_h = prof.get("extra_headers")
        if isinstance(base_h, dict):
            raw_525 = base_h.get("x-goog-ext-525001261-jspb")
            if isinstance(raw_525, str) and raw_525.startswith("["):
                try:
                    parsed_525 = json.loads(raw_525)
                    if isinstance(parsed_525, list) and len(parsed_525) >= 18:
                        ext_525_arr = list(parsed_525)
                except Exception:
                    ext_525_arr = None
        if ext_525_arr is None:
            ext_525_arr = [None] * 28
            ext_525_arr[0] = 1
            ext_525_arr[27] = "Asia/Shanghai"
        elif len(ext_525_arr) < 28:
            ext_525_arr.extend([None] * (28 - len(ext_525_arr)))
            if not ext_525_arr[27]:
                ext_525_arr[27] = "Asia/Shanghai"
        ext_525_arr[0] = 1
        ext_525_arr[4] = model_hex
        ext_525_arr[6] = hl
        ext_525_arr[8] = capacity
        ext_525_arr[14] = slot_79
        ext_525_arr[15] = slot_80
        ext_525_arr[16] = ruuid
        ext_525_arr[17] = [bl] if bl else ext_525_arr[17]

        merged: Dict[str, str] = {
            "x-goog-ext-525001261-jspb": json.dumps(ext_525_arr, separators=(",", ":")),
            "x-goog-ext-525005358-jspb": json.dumps([ruuid, 1], separators=(",", ":")),
            "x-goog-ext-73010989-jspb": "[0]",
            "x-goog-ext-73010990-jspb": "[0,0,0]",
        }

        if isinstance(base_h, dict):
            for k, v in base_h.items():
                kl = str(k).strip().lower()
                if (
                    not kl
                    or not kl.startswith("x-goog-ext-")
                    or kl in FORBIDDEN_OVERRIDE_HEADERS
                    or kl in PROTECTED_EXT_HEADERS
                    or isinstance(v, bool)
                    or not isinstance(v, (str, int, float))
                ):
                    continue
                merged[kl] = str(v)

        model_h_map = prof.get("model_headers")
        if isinstance(model_h_map, dict) and isinstance(model_h_map.get(model_id), dict):
            for k, v in model_h_map[model_id].items():
                kl = str(k).strip().lower()
                if kl and kl not in FORBIDDEN_OVERRIDE_HEADERS and not isinstance(v, bool) and isinstance(v, (str, int, float)):
                    merged[kl] = str(v)
        return merged

    def resolve_fallback_bl(self, prof_snapshot: Optional[Dict[str, Any]] = None) -> str:
        prof = prof_snapshot if prof_snapshot is not None else self.load()
        bl = prof.get("bl")
        return str(bl).strip() if isinstance(bl, str) else ""


PROTO_PROFILE_CACHE = WebProtoProfileCache()


def build_stream_generate_freq(
    delta_prompt: str,
    cursor: Optional[Tuple[str, str, str]] = None,
    turn_index: int = 0,
    model_id: str = "gemini-4-neon",
    tier: str = "",
    hl: str = "zh-CN",
    proto_cache: Optional[WebProtoProfileCache] = None,
    prof_snapshot: Optional[Dict[str, Any]] = None,
) -> str:
    cid, rid, rcid = cursor if (cursor and len(cursor) == 3) else ("", "", "")
    cache = proto_cache or PROTO_PROFILE_CACHE
    prof = prof_snapshot if prof_snapshot is not None else cache.load()
    slot_79, slot_80 = cache._resolve_model_slots_from_prof(prof, model_id, tier=tier)
    safe_turn_idx = max(0, int(turn_index))

    inner: List[Any] = [None] * 97
    inner[0] = [delta_prompt, 0, None, None, None, None, 0]
    inner[1] = [hl]
    inner[2] = [cid, rid, rcid, None, None, None, None, None, None, ""]
    inner[7] = 1
    inner[17] = [[safe_turn_idx]]
    inner[30] = [4]
    inner[32] = 1
    inner[49] = None
    inner[68] = 1
    inner[79] = slot_79
    inner[80] = slot_80
    inner[96] = 1 if (not cid) else 0

    inner_str = json.dumps(inner, ensure_ascii=False, separators=(",", ":"))
    return json.dumps([None, inner_str], ensure_ascii=False, separators=(",", ":"))


def compute_safe_snapshot_delta(prev: str, curr: str) -> Tuple[str, str]:
    if not curr or curr == prev:
        return ("", prev)
    if not prev:
        return (curr, curr)
    if curr.startswith(prev):
        return (curr[len(prev) :], curr)
    if len(curr) <= len(prev):
        return ("", prev)

    anchor_len = min(24, len(prev))
    if anchor_len >= 4:
        tail_anchor = prev[-anchor_len:]
        pos = curr.rfind(tail_anchor)
        if pos != -1 and (pos + anchor_len) <= len(curr):
            return (curr[pos + anchor_len :], curr)

    return (curr[len(prev) :], curr)


def _collect_thought_strings(node: Any, out: List[str], depth: int = 0) -> None:
    if depth > 6 or node is None:
        return
    if isinstance(node, str):
        s = node.strip()
        if len(s) >= 2 and not s.startswith("http") and not re.fullmatch(r"[0-9a-fA-F_-]{8,}", s):
            out.append(node)
        return
    if isinstance(node, list):
        for it in node:
            _collect_thought_strings(it, out, depth + 1)


def extract_thought_from_cand(cand: List[Any]) -> str:
    if len(cand) <= 37 or not isinstance(cand[37], list) or not cand[37]:
        return ""
    c37 = cand[37]
    if isinstance(c37[0], list) and c37[0] and isinstance(c37[0][0], str) and c37[0][0]:
        return c37[0][0]
    if isinstance(c37[0], str) and c37[0]:
        return c37[0]
    if len(c37) > 1 and isinstance(c37[1], list):
        steps: List[str] = []
        for step in c37[1]:
            if isinstance(step, list) and step:
                if isinstance(step[0], list) and step[0] and isinstance(step[0][0], str) and step[0][0]:
                    steps.append(step[0][0])
                elif isinstance(step[0], str) and step[0]:
                    steps.append(step[0])
        if steps:
            return "\n\n".join(steps)
    fallback_parts: List[str] = []
    _collect_thought_strings(c37, fallback_parts, 0)
    return "\n\n".join(fallback_parts) if fallback_parts else ""


class WrbStreamFrameDecoder:
    LEN_LINE_RE = re.compile(r"^\s*\d+\s*$")
    NEXT_FRAME_RE = re.compile(r"\n\s*\d+\s*\n\s*\[\[")
    HTTPS_CHIP_PFX = "https://googleusercontent.com/immersive_entry_chip/"
    HTTP_CHIP_PFX = "http://googleusercontent.com/immersive_entry_chip/"

    def __init__(self):
        self._buf = ""
        self._xssi_stripped = False
        self._held_ctrl_tail = ""
        self._last_raw_text = ""
        self.prev_thinking = ""
        self.last_thoughts = ""
        self.prev_text = ""
        self.conversation_id = ""
        self.response_id = ""
        self.choice_id = ""
        self.routed_model = ""
        self.last_error = ""
        self.last_error_code: Optional[int] = None
        self.canvas_map: Dict[int, Dict[str, str]] = {}
        self.chip_url_to_id: Dict[str, str] = {}

    def cursor(self) -> Tuple[str, str, str]:
        return (self.conversation_id, self.response_id, self.choice_id)

    def feed_raw_chunk(self, raw_chunk: str) -> List[Tuple[str, str]]:
        if not raw_chunk:
            return []
        self._buf += raw_chunk

        if not self._xssi_stripped:
            s = self._buf.lstrip()
            if s.startswith(")]}'"):
                nl = self._buf.find("\n")
                if nl == -1:
                    return []
                self._buf = self._buf[nl + 1 :]
                self._xssi_stripped = True
            elif len(s) >= 4:
                self._xssi_stripped = True

        deltas: List[Tuple[str, str]] = []
        while self._buf:
            nl = self._buf.find("\n")
            if nl == -1:
                cand_line = self._buf.strip()
                if cand_line.startswith("[") and cand_line.endswith("]"):
                    parsed = self._try_parse_json(cand_line)
                    if parsed is not None:
                        self._buf = ""
                        deltas.extend(self._extract_from_outer(parsed))
                break

            line = self._buf[:nl].strip()
            if not line or self.LEN_LINE_RE.match(line):
                self._buf = self._buf[nl + 1 :]
                continue

            if line.startswith("["):
                parsed = self._try_parse_json(line)
                if parsed is not None:
                    self._buf = self._buf[nl + 1 :]
                    deltas.extend(self._extract_from_outer(parsed))
                    continue

                acc_end = nl
                found_multi = False
                while True:
                    nxt_nl = self._buf.find("\n", acc_end + 1)
                    probe_end = len(self._buf) if nxt_nl == -1 else nxt_nl
                    candidate = self._buf[:probe_end].strip()
                    if candidate.endswith("]"):
                        parsed_m = self._try_parse_json(candidate)
                        if parsed_m is not None:
                            self._buf = "" if nxt_nl == -1 else self._buf[nxt_nl + 1 :]
                            deltas.extend(self._extract_from_outer(parsed_m))
                            found_multi = True
                            break
                    if nxt_nl == -1:
                        break
                    acc_end = nxt_nl
                if not found_multi:
                    m_next = self.NEXT_FRAME_RE.search(self._buf, 1)
                    if m_next is not None:
                        self._buf = self._buf[m_next.start() + 1 :]
                        continue
                    if len(self._buf) > 4 * 1024 * 1024:
                        self._buf = self._buf[nl + 1 :]
                    break
            else:
                self._buf = self._buf[nl + 1 :]

        return deltas

    def flush(self) -> List[Tuple[str, str]]:
        """Flush any trailing un-terminated frame, held Canvas chip, or partial `<ctrl94>thought` prefix at EOF."""
        out: List[Tuple[str, str]] = []
        rem = self._buf.strip()
        if rem:
            first_br = rem.find("[")
            last_br = rem.rfind("]")
            if first_br != -1 and last_br > first_br:
                parsed = self._try_parse_json(rem[first_br : last_br + 1])
                if parsed is not None:
                    out.extend(self._extract_from_outer(parsed))
            self._buf = ""
        if self._last_raw_text:
            expanded_eof = self._expand_canvas_chips(self._last_raw_text)
            diff_eof, self.prev_text = compute_safe_snapshot_delta(self.prev_text, expanded_eof)
            if diff_eof:
                out.append(("text", diff_eof))
        if self._held_ctrl_tail:
            tail = self._held_ctrl_tail
            self._held_ctrl_tail = ""
            diff_txt, self.prev_text = compute_safe_snapshot_delta(self.prev_text, self.prev_text + tail)
            if diff_txt:
                out.append(("text", diff_txt))
        return out

    @staticmethod
    def _try_parse_json(s: str) -> Optional[Any]:
        try:
            return json.loads(s)
        except Exception:
            return None

    @staticmethod
    def _trim_incomplete_tag_tail(text: str, tag: str) -> Tuple[str, str]:
        lt = text.rfind("<")
        if lt != -1:
            tail = text[lt:]
            if 0 < len(tail) < len(tag) and tag.startswith(tail):
                return text[:lt], tail
        return text, ""

    @classmethod
    def _strip_inline_thought_tags(cls, raw_text: str, thought_str: str) -> Tuple[str, str, str]:
        if not raw_text:
            return raw_text, thought_str, ""
        inline_thoughts: List[str] = []
        cur = raw_text
        while CTRL94_THOUGHT_TAG in cur:
            c94 = cur.find(CTRL94_THOUGHT_TAG)
            pre94 = cur[:c94]
            after94 = cur[c94 + len(CTRL94_THOUGHT_TAG) :]
            c95 = after94.find(CTRL95_TAG)
            if c95 == -1:
                after_clean, _ = cls._trim_incomplete_tag_tail(after94, CTRL95_TAG)
                inline_thoughts.append(after_clean.lstrip())
                cur = pre94
                break
            inline_thoughts.append(after94[:c95].lstrip())
            cur = pre94 + after94[c95 + len(CTRL95_TAG) :]
        if not thought_str and inline_thoughts:
            thought_str = "\n".join(t for t in inline_thoughts if t)
        cur, held_tail = cls._trim_incomplete_tag_tail(cur, CTRL94_THOUGHT_TAG)
        return cur, thought_str, held_tail

    def _update_canvas_cache(self, cand: List[Any]) -> None:
        if len(cand) > 12 and isinstance(cand[12], list) and len(cand[12]) > 8 and isinstance(cand[12][8], dict):
            m57 = cand[12][8].get("57")
            if isinstance(m57, list):
                for cm in m57:
                    if (
                        isinstance(cm, list)
                        and len(cm) >= 2
                        and isinstance(cm[0], list)
                        and cm[0]
                        and isinstance(cm[0][0], str)
                        and isinstance(cm[1], str)
                    ):
                        self.chip_url_to_id[cm[0][0]] = cm[1]

        if len(cand) > 30 and isinstance(cand[30], list) and cand[30]:
            entries = cand[30] if isinstance(cand[30][0], list) else [cand[30]]
            for idx, entry in enumerate(entries):
                if not isinstance(entry, list) or len(entry) <= 4:
                    continue
                content = entry[4]
                if isinstance(content, str) and content:
                    kind_num = entry[10] if len(entry) > 10 else 1
                    is_code = (kind_num == 2)
                    fname = entry[9] if (len(entry) > 9 and isinstance(entry[9], str)) else ""
                    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
                    lang = ""
                    if len(entry) > 14 and isinstance(entry[14], list):
                        if is_code and len(entry[14]) > 1 and isinstance(entry[14][1], str):
                            lang = entry[14][1]
                        elif not is_code and len(entry[14]) > 4 and isinstance(entry[14][4], str):
                            lang = entry[14][4]
                    if not lang:
                        lang = ext or "html"
                    self.canvas_map[idx] = {
                        "id": entry[0] if isinstance(entry[0], str) else "",
                        "title": entry[2] if (len(entry) > 2 and isinstance(entry[2], str)) else fname,
                        "kind": "code" if is_code else "document",
                        "lang": str(lang),
                        "content": content,
                    }

    def _expand_canvas_chips(self, raw_text: str) -> str:
        if not raw_text or "immersive_entry_chip/" not in raw_text:
            return raw_text

        def _repl(m: re.Match) -> str:
            full_url = m.group(0)
            idx = int(m.group(1))
            item = self.canvas_map.get(idx)
            if item is None:
                target_id = self.chip_url_to_id.get(full_url)
                if not target_id:
                    alt_url = (
                        full_url.replace("https://", "http://", 1)
                        if full_url.startswith("https://")
                        else full_url.replace("http://", "https://", 1)
                    )
                    target_id = self.chip_url_to_id.get(alt_url)
                if target_id:
                    for v in self.canvas_map.values():
                        if v.get("id") == target_id:
                            item = v
                            break
            if item is None:
                return full_url
            body = item["content"].strip()
            if item["kind"] == "code" and not body.startswith("```"):
                lang = item.get("lang") or "html"
                return "```" + lang + "\n" + body + "\n```"
            return body

        return IMMERSIVE_CHIP_RE.sub(_repl, raw_text)

    @classmethod
    def _hold_unexpanded_chips(cls, text: str, allow_hold: bool = True) -> str:
        if not allow_hold or not text:
            return text
        m = IMMERSIVE_CHIP_RE.search(text)
        out = text[: m.start()] if m else text
        http_idx = out.rfind("http")
        if http_idx != -1 and (len(out) - http_idx) <= len(cls.HTTPS_CHIP_PFX):
            tail = out[http_idx:]
            if len(tail) >= 9 and (cls.HTTPS_CHIP_PFX.startswith(tail) or cls.HTTP_CHIP_PFX.startswith(tail)):
                out = out[:http_idx]
        return out

    def _extract_from_outer(self, outer: Any) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        if not isinstance(outer, list):
            return out
        for item in outer:
            if not isinstance(item, list) or len(item) < 3:
                continue
            if item[0] != "wrb.fr":
                continue
            if item[2] is None:
                if len(item) > 5 and item[5]:
                    err_payload = item[5]
                    err_code: Optional[int] = None
                    if (
                        isinstance(err_payload, list)
                        and err_payload
                        and isinstance(err_payload[0], int)
                        and not isinstance(err_payload[0], bool)
                    ):
                        err_code = err_payload[0]
                    elif isinstance(err_payload, int) and not isinstance(err_payload, bool):
                        err_code = err_payload
                    err_str = (
                        f"code={err_code} {json.dumps(err_payload, ensure_ascii=False)}"
                        if err_code is not None
                        else json.dumps(err_payload, ensure_ascii=False)
                    )
                    self.last_error = err_str
                    self.last_error_code = err_code
                    out.append(("error", err_str))
                continue
            if not isinstance(item[2], str):
                continue
            part = self._try_parse_json(item[2])
            if not isinstance(part, list):
                continue

            for probe_idx in (42, len(part) - 2):
                if 0 <= probe_idx < len(part) and isinstance(part[probe_idx], str):
                    lbl = part[probe_idx]
                    if len(lbl) < 64 and ROUTED_MODEL_RE.search(lbl):
                        self.routed_model = lbl
                        break

            if len(part) > 1 and isinstance(part[1], list):
                if len(part[1]) > 0 and isinstance(part[1][0], str) and part[1][0]:
                    self.conversation_id = part[1][0]
                if len(part[1]) > 1 and isinstance(part[1][1], str) and part[1][1]:
                    self.response_id = part[1][1]

            if len(part) <= 4 or not isinstance(part[4], list) or not part[4]:
                continue
            cand = part[4][0]
            if not isinstance(cand, list) or not cand:
                continue

            if isinstance(cand[0], str) and cand[0]:
                self.choice_id = cand[0]

            self._update_canvas_cache(cand)

            thought_str = extract_thought_from_cand(cand)
            raw_text = cand[1][0] if (len(cand) > 1 and isinstance(cand[1], list) and cand[1] and isinstance(cand[1][0], str)) else ""

            if raw_text:
                raw_text, thought_str, held_tail = self._strip_inline_thought_tags(raw_text, thought_str)
                self._held_ctrl_tail = held_tail

            if thought_str:
                self.last_thoughts = thought_str
                diff_thk, self.prev_thinking = compute_safe_snapshot_delta(self.prev_thinking, self.last_thoughts)
                if diff_thk:
                    out.append(("thinking", diff_thk))

            if raw_text:
                self._last_raw_text = raw_text
                expanded_txt = self._expand_canvas_chips(raw_text)
                emit_candidate = self._hold_unexpanded_chips(expanded_txt, allow_hold=True)
                if emit_candidate:
                    diff_txt, self.prev_text = compute_safe_snapshot_delta(self.prev_text, emit_candidate)
                    if diff_txt:
                        out.append(("text", diff_txt))

        return out

    @staticmethod
    def _compute_snapshot_delta(prev: str, curr: str) -> str:
        delta, _ = compute_safe_snapshot_delta(prev, curr)
        return delta


class PriorityAIMDGate:
    def __init__(self, max_concurrency: int = 4, min_interval_sec: float = 0.08):
        self._cond = threading.Condition()
        self._base_max_concurrency = max(1, int(max_concurrency))
        self._max_concurrency = self._base_max_concurrency
        self._active = 0
        self._seq = 0
        self._clean_streak = 0
        self._waiters: List[Tuple[int, int]] = []
        self._last_dispatch_ts = 0.0
        self._min_interval_sec = float(min_interval_sec)
        self._cooldown_until = 0.0

    def acquire(
        self,
        priority: int = 2,
        timeout_sec: float = 120.0,
        cancel_event: Optional[threading.Event] = None,
    ) -> bool:
        deadline = time.monotonic() + max(0.01, float(timeout_sec))
        with self._cond:
            self._seq += 1
            entry = (priority, self._seq)
            heapq.heappush(self._waiters, entry)
            while True:
                now = time.monotonic()
                cancelled = cancel_event is not None and cancel_event.is_set()
                if cancelled or now >= deadline:
                    if entry in self._waiters:
                        self._waiters.remove(entry)
                        heapq.heapify(self._waiters)
                        self._cond.notify_all()
                    return False
                wait_cd = max(0.0, self._cooldown_until - now, (self._last_dispatch_ts + self._min_interval_sec) - now)
                is_head = bool(self._waiters and self._waiters[0][1] == entry[1])
                if is_head and self._active < self._max_concurrency and wait_cd <= 0.0:
                    heapq.heappop(self._waiters)
                    self._active += 1
                    self._last_dispatch_ts = now
                    return True
                sleep_for = min(deadline - now, wait_cd if wait_cd > 0 else 0.2)
                if cancel_event is not None:
                    sleep_for = min(sleep_for, 0.2)
                self._cond.wait(timeout=max(0.01, sleep_for))

    def release(self, hit_rate_limit: bool = False) -> None:
        with self._cond:
            self._active = max(0, self._active - 1)
            if hit_rate_limit:
                self._clean_streak = 0
                self._max_concurrency = max(1, self._max_concurrency - 1)
                jitter = random.uniform(1.5, 4.0)
                self._cooldown_until = max(self._cooldown_until, time.monotonic() + jitter)
                self._min_interval_sec = min(1.2, max(0.05, self._min_interval_sec) * 1.5)
            else:
                self._min_interval_sec = max(0.02, self._min_interval_sec * 0.95)
                if self._max_concurrency < self._base_max_concurrency:
                    self._clean_streak += 1
                    if self._clean_streak >= 3:
                        self._max_concurrency = min(self._base_max_concurrency, self._max_concurrency + 1)
                        self._clean_streak = 0
                else:
                    self._clean_streak = 0
            self._cond.notify_all()


class WebKitPlaneClient:
    """Manages lifecycle and UNIX-socket RPC calls to `bin/DSHWebKitBridge.app`."""

    def __init__(
        self,
        socket_path: str,
        binary_path: str,
        auto_spawn: bool = True,
        socket_connector: Optional[Callable[[], socket.socket]] = None,
        proto_profile_path: str = DEFAULT_PROTO_PROFILE_PATH,
        early_stop_on_tool_call: bool = True,
        strict_downgrade_reroute: bool = True,
    ):
        self.socket_path = socket_path
        self.binary_path = binary_path
        self.auto_spawn = auto_spawn
        self.socket_connector = socket_connector
        self.proto_cache = WebProtoProfileCache(proto_profile_path)
        self.early_stop_on_tool_call = early_stop_on_tool_call
        self.strict_downgrade_reroute = strict_downgrade_reroute
        self._spawn_lock = threading.Lock()
        self._status_cache_lock = threading.Lock()
        self._cached_status: Optional[dict] = None
        self._cached_status_at: float = 0.0
        self._last_synced_proto_rev: int = -1
        self._delete_lock = threading.Lock()
        self._pending_delete_cids: List[str] = []
        self._deleted_cids_seen: Set[str] = set()
        self._delete_worker_running: bool = False
        self.deleted_conversations_count: int = 0
        self.gate = PriorityAIMDGate(max_concurrency=4, min_interval_sec=0.25)

    def _invalidate_status_cache(self) -> None:
        with self._status_cache_lock:
            self._cached_status = None
            self._cached_status_at = 0.0

    def _resolve_app_bundle_path(self) -> Optional[str]:
        bin_dir = os.path.dirname(os.path.abspath(self.binary_path))
        cand_1 = os.path.join(bin_dir, "DSHWebKitBridge.app")
        if os.path.isdir(cand_1):
            return cand_1
        parts = os.path.abspath(self.binary_path).split(os.sep)
        for idx, p in enumerate(parts):
            if p.endswith(".app"):
                bundle = os.sep.join(parts[: idx + 1])
                if os.path.isdir(bundle):
                    return bundle
        return None

    def _open_stream_socket(self, timeout_sec: float) -> socket.socket:
        if self.socket_connector is not None:
            s = self.socket_connector()
            s.settimeout(timeout_sec)
            return s
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * 1024 * 1024)
        except OSError:
            pass
        s.settimeout(timeout_sec)
        s.connect(self.socket_path)
        return s

    def _unix_rpc_oneshot(self, payload: dict, timeout_sec: float = 2.0) -> Optional[dict]:
        if self.socket_connector is None and not os.path.exists(self.socket_path):
            return None
        s: Optional[socket.socket] = None
        try:
            s = self._open_stream_socket(timeout_sec)
            s.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
            buf = b""
            while b"\n" not in buf:
                chunk = s.recv(16384)
                if not chunk:
                    break
                buf += chunk
            line = buf.split(b"\n", 1)[0].strip()
            return json.loads(line.decode("utf-8", errors="replace")) if line else None
        except Exception:
            return None
        finally:
            if s is not None:
                try:
                    s.close()
                except Exception:
                    pass

    def _is_compatible_status(self, st: Optional[dict]) -> bool:
        if not st or not st.get("ok"):
            return False
        if self.socket_connector is not None:
            return True
        return st.get("bridge_version") == EXPECTED_BRIDGE_VERSION and st.get("direct_delta") is True

    def _wait_for_old_daemon_exit(self, max_wait_sec: float = 4.0) -> None:
        shut_deadline = time.monotonic() + max_wait_sec
        saw_socket_gone = False
        while time.monotonic() < shut_deadline:
            sock_exists = os.path.exists(self.socket_path)
            st_check = self._unix_rpc_oneshot({"op": "status"}, timeout_sec=0.25) if sock_exists else None
            proc_alive = False
            try:
                pg = subprocess.run(
                    ["/usr/bin/pgrep", "-f", self.socket_path],
                    capture_output=True,
                    text=True,
                    timeout=0.5,
                )
                proc_alive = (pg.returncode == 0 and bool(pg.stdout.strip()))
            except Exception:
                proc_alive = False
            if not sock_exists and not st_check and not proc_alive:
                if not saw_socket_gone:
                    saw_socket_gone = True
                    time.sleep(0.15)
                return
            time.sleep(0.1)
        if os.path.exists(self.socket_path) and self._unix_rpc_oneshot({"op": "status"}, timeout_sec=0.2) is None:
            try:
                os.unlink(self.socket_path)
            except OSError:
                pass

    def _maybe_sync_proto_from_status(self, st: Optional[dict]) -> None:
        if not st or not st.get("ok") or self.socket_connector is not None:
            return
        try:
            rev = int(st.get("proto_rev") or 0)
        except (TypeError, ValueError):
            rev = 0
        if rev <= 0:
            return
        profile_missing = bool(self.proto_cache.path and not os.path.exists(self.proto_cache.path))
        if rev <= self._last_synced_proto_rev and not profile_missing:
            return
        snap = self._unix_rpc_oneshot({"op": "proto_profile"}, timeout_sec=1.0)
        if snap and snap.get("ok"):
            self.proto_cache.merge_bridge_snapshot(snap)
            self._last_synced_proto_rev = rev

    def ensure_daemon_running(self, auto_popup_login: bool = False, wait_sec: float = 5.0) -> Optional[dict]:
        st = self._unix_rpc_oneshot({"op": "status"}, timeout_sec=1.2)
        if self._is_compatible_status(st):
            self._maybe_sync_proto_from_status(st)
            return st
        if not self.auto_spawn:
            return None

        app_bundle = self._resolve_app_bundle_path()
        has_bin = os.path.isfile(self.binary_path) and os.access(self.binary_path, os.X_OK)
        if not app_bundle and not has_bin:
            return None

        spawn_lock_path = f"{self.socket_path}.spawn.lock"
        with self._spawn_lock:
            with _flock_guard(spawn_lock_path, exclusive=True, create_parent=True):
                st = self._unix_rpc_oneshot({"op": "status"}, timeout_sec=0.8)
                if self._is_compatible_status(st):
                    self._maybe_sync_proto_from_status(st)
                    return st
                if st and st.get("ok"):
                    self._invalidate_status_cache()
                    self._unix_rpc_oneshot({"op": "shutdown"}, timeout_sec=1.0)
                    self._wait_for_old_daemon_exit(max_wait_sec=4.0)
                else:
                    self._invalidate_status_cache()

                extra_args = ["--socket", self.socket_path]
                if self.proto_cache.path:
                    extra_args.extend(["--proto-profile", self.proto_cache.path])
                if auto_popup_login:
                    extra_args.append("--auto-popup-login")

                if app_bundle and os.path.isfile("/usr/bin/open"):
                    res = subprocess.run(
                        ["/usr/bin/open", "-g", "-a", app_bundle, "--args"] + extra_args,
                        capture_output=True,
                        text=True,
                    )
                    if res.returncode != 0:
                        return None
                elif has_bin:
                    log_path = f"{self.socket_path}.log"
                    with open(log_path, "ab") as lfp:
                        subprocess.Popen(
                            [self.binary_path] + extra_args,
                            stdin=subprocess.DEVNULL,
                            stdout=lfp,
                            stderr=lfp,
                            close_fds=True,
                        )
                else:
                    return None

                deadline = time.monotonic() + max(0.5, float(wait_sec))
                while time.monotonic() < deadline:
                    if self.socket_connector is not None or os.path.exists(self.socket_path):
                        st = self._unix_rpc_oneshot({"op": "status"}, timeout_sec=0.8)
                        if st and st.get("ok"):
                            self._invalidate_status_cache()
                            self._maybe_sync_proto_from_status(st)
                            return st
                    time.sleep(0.15)
                if self.socket_connector is not None or os.path.exists(self.socket_path):
                    st = self._unix_rpc_oneshot({"op": "status"}, timeout_sec=0.8)
                    if st and st.get("ok"):
                        self._invalidate_status_cache()
                        self._maybe_sync_proto_from_status(st)
                        return st
        return None

    def status(self, max_age_sec: float = 0.0) -> dict:
        if max_age_sec > 0.0 and self.socket_connector is None:
            now_m = time.monotonic()
            with self._status_cache_lock:
                if self._cached_status is not None and (now_m - self._cached_status_at) <= max_age_sec:
                    return dict(self._cached_status)
        st = self._unix_rpc_oneshot({"op": "status"}, timeout_sec=1.2)
        if st and st.get("ok"):
            self._maybe_sync_proto_from_status(st)
            st.setdefault("binary_exists", os.path.isfile(self.binary_path) or bool(self._resolve_app_bundle_path()))
            st.setdefault("socket_path", self.socket_path)
            with self._status_cache_lock:
                self._cached_status = dict(st)
                self._cached_status_at = time.monotonic()
            return st
        offline = {
            "ok": False,
            "plane": "webkit_native",
            "logged_in": False,
            "ready": False,
            "binary_exists": os.path.isfile(self.binary_path) or bool(self._resolve_app_bundle_path()),
            "socket_path": self.socket_path,
        }
        with self._status_cache_lock:
            self._cached_status = dict(offline)
            self._cached_status_at = time.monotonic()
        return offline

    def abort_stream(self, target_req_id: str) -> None:
        self._unix_rpc_oneshot({"op": "abort", "targetReqId": target_req_id}, timeout_sec=0.8)

    def background_drain_stream(self, target_req_id: str, max_drain_ms: int = 6000) -> bool:
        res = self._unix_rpc_oneshot(
            {"op": "background_drain", "targetReqId": target_req_id, "maxDrainMs": int(max_drain_ms)},
            timeout_sec=2.0,
        )
        return bool(res and res.get("ok"))

    def delete_conversations(self, cids: List[str], timeout_sec: float = 12.0) -> dict:
        clean_cids: List[str] = []
        seen_local: Set[str] = set()
        for raw_c in cids or []:
            c_str = str(raw_c or "").strip()
            if c_str.startswith("c_") and len(c_str) >= 10 and c_str not in seen_local:
                seen_local.add(c_str)
                clean_cids.append(c_str)
        if not clean_cids:
            return {"ok": True, "deleted": [], "requested": 0}
        res = self._unix_rpc_oneshot({"op": "delete_conversations", "cids": clean_cids}, timeout_sec=timeout_sec)
        if isinstance(res, dict) and isinstance(res.get("deleted"), list):
            with self._delete_lock:
                if len(self._deleted_cids_seen) > 4096:
                    self._deleted_cids_seen.clear()
                for d_cid in res["deleted"]:
                    self._deleted_cids_seen.add(str(d_cid))
                self.deleted_conversations_count += len(res["deleted"])
            return res
        return res or {"ok": False, "deleted": [], "requested": len(clean_cids), "error": "BRIDGE_DELETE_FAILED"}

    def delete_conversations_async(self, cids: List[str]) -> None:
        enqueue_list: List[str] = []
        with self._delete_lock:
            for raw_c in cids or []:
                c_str = str(raw_c or "").strip()
                if (
                    c_str.startswith("c_")
                    and len(c_str) >= 10
                    and c_str not in self._deleted_cids_seen
                    and c_str not in self._pending_delete_cids
                ):
                    enqueue_list.append(c_str)
            if not enqueue_list:
                return
            self._pending_delete_cids.extend(enqueue_list)
            if self._delete_worker_running:
                return
            self._delete_worker_running = True

        def _drain_deletes() -> None:
            try:
                time.sleep(0.35)
                while True:
                    with self._delete_lock:
                        if not self._pending_delete_cids:
                            return
                        batch = self._pending_delete_cids[:16]
                        del self._pending_delete_cids[:16]
                    try:
                        self.delete_conversations(batch, timeout_sec=10.0)
                    except Exception:
                        pass
            finally:
                restart_needed = False
                with self._delete_lock:
                    self._delete_worker_running = False
                    if self._pending_delete_cids:
                        self._delete_worker_running = True
                        restart_needed = True
                if restart_needed:
                    threading.Thread(target=_drain_deletes, daemon=True).start()

        threading.Thread(target=_drain_deletes, daemon=True).start()

    def invalidate_auth(self) -> None:
        self._invalidate_status_cache()
        self._unix_rpc_oneshot({"op": "auth_invalidate"}, timeout_sec=1.0)

    def open_login_window(self) -> dict:
        self._invalidate_status_cache()
        st = self.ensure_daemon_running(auto_popup_login=False, wait_sec=5.0)
        if not st:
            return {"ok": False, "error": f"Unable to launch DSHWebKitBridge.app ({self.binary_path})"}
        res = self._unix_rpc_oneshot({"op": "open_login"}, timeout_sec=3.0)
        self._invalidate_status_cache()
        return res or {"ok": False, "error": "Socket open_login failed"}

    def switch_account(self) -> dict:
        self._invalidate_status_cache()
        st = self.ensure_daemon_running(auto_popup_login=False, wait_sec=5.0)
        if not st:
            return {"ok": False, "error": f"Unable to launch DSHWebKitBridge.app ({self.binary_path})"}
        res = self._unix_rpc_oneshot({"op": "switch_account"}, timeout_sec=4.0)
        self._invalidate_status_cache()
        return res or {"ok": False, "error": "Socket switch_account failed"}

    def shutdown(self) -> None:
        self._invalidate_status_cache()
        self._unix_rpc_oneshot({"op": "shutdown"}, timeout_sec=1.0)

    def stream_turn(
        self,
        delta_prompt: str,
        nonce: str,
        cursor: Tuple[str, str, str],
        turn_index: int,
        model_id: str,
        tier: str = "",
        priority: int = 1,
        on_delta: Optional[Callable[[dict], None]] = None,
        timeout_sec: float = 600.0,
        stream_raw_tool_names: Optional[Set[str]] = None,
        cwd: str = "",
        cancel_event: Optional[threading.Event] = None,
        hl: str = "zh-CN",
    ) -> dict:
        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError("[CLIENT_CANCELLED] Client disconnected")

        t0 = time.monotonic()
        deadline = t0 + max(0.1, float(timeout_sec))

        gate_wait = min(45.0, max(0.1, deadline - time.monotonic()))
        if not self.gate.acquire(priority=priority, timeout_sec=gate_wait, cancel_event=cancel_event):
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError("[CLIENT_CANCELLED] Client disconnected")
            raise RuntimeError("Plane 1 concurrency queue timeout")

        hit_429 = False
        # Pure-API policy: at most 1 retry on the EXACT SAME model & SAME thinking level for 429/hijack;
        # brief warmup retries allowed for bridge hydration.
        warmup_retry_delays = [0.25, 0.6, 1.2]
        same_model_max_retries = 1
        attempt = 0
        same_model_retries = 0
        attempt_tier = tier
        input_cid = cursor[0] if (cursor and len(cursor) == 3) else ""

        def _wait_backoff_or_abort(delay_sec: float) -> None:
            rem = deadline - time.monotonic()
            if rem <= 0:
                raise RuntimeError(f"[STREAM_TIMEOUT] WebKit stream exceeded {timeout_sec:.1f}s deadline")
            wait_s = min(delay_sec, rem)
            if cancel_event is not None:
                if cancel_event.wait(timeout=wait_s):
                    raise RuntimeError("[CLIENT_CANCELLED] Client disconnected")
            else:
                time.sleep(wait_s)
            if time.monotonic() >= deadline:
                raise RuntimeError(f"[STREAM_TIMEOUT] WebKit stream exceeded {timeout_sec:.1f}s deadline")

        try:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    raise RuntimeError("[CLIENT_CANCELLED] Client disconnected")
                rem = deadline - time.monotonic()
                if rem <= 0:
                    raise RuntimeError(f"[STREAM_TIMEOUT] WebKit stream exceeded {timeout_sec:.1f}s deadline")

                req_id = "wreq_" + uuid.uuid4().hex[:12]
                req_uuid = str(uuid.uuid4()).upper()
                prof_snap = self.proto_cache.load()
                slot_79_used, slot_80_used = self.proto_cache._resolve_model_slots_from_prof(prof_snap, model_id, tier=attempt_tier)
                freq_str = build_stream_generate_freq(
                    delta_prompt=delta_prompt,
                    cursor=cursor,
                    turn_index=turn_index,
                    model_id=model_id,
                    tier=attempt_tier,
                    hl=hl,
                    proto_cache=self.proto_cache,
                    prof_snapshot=prof_snap,
                )
                extra_headers = self.proto_cache.resolve_extra_headers(
                    model_id,
                    tier=attempt_tier,
                    req_uuid=req_uuid,
                    hl=hl,
                    prof_snapshot=prof_snap,
                )
                fallback_bl = self.proto_cache.resolve_fallback_bl(prof_snapshot=prof_snap)

                should_guard_hijack = (
                    self.strict_downgrade_reroute
                    and slot_79_used != 6
                    and (model_id or "").strip().lower() != "gemini-3.5-flash-lite"
                )

                early_stopped = False
                first_tool_done_at: Optional[float] = None
                post_tool_buf: List[str] = []
                post_tool_hallucination = False
                sock: Optional[socket.socket] = None
                first_delta_at: Optional[float] = None
                emitted_visible_delta = False
                delta_chunks = 0
                text_parts: List[str] = []
                held_canned_suspect: List[str] = []
                think_parts: List[str] = []
                tool_calls: List[dict] = []
                malformed_envelopes: List[dict] = []
                routed_model_seen = ""
                streamed_any_tool = False

                model_confirmed = not should_guard_hijack
                held_guard_events: List[Tuple[str, Any]] = []
                held_guard_chars = 0
                first_guard_at: Optional[float] = None
                attempt_t0 = time.monotonic()

                wrb_decoder = WrbStreamFrameDecoder()
                env_parser = StreamingEnvelopeParser(
                    nonce=nonce,
                    respect_code_fences=True,
                    stream_raw_tool_names=stream_raw_tool_names if on_delta is not None else None,
                    cwd=cwd,
                )

                def _cleanup_orphan() -> None:
                    oc = wrb_decoder.conversation_id
                    if oc and oc != input_cid:
                        self.delete_conversations_async([oc])

                def flush_held_suspect() -> None:
                    nonlocal first_delta_at, delta_chunks, emitted_visible_delta
                    if not held_canned_suspect:
                        return
                    joined_s = "".join(held_canned_suspect)
                    held_canned_suspect.clear()
                    if first_delta_at is None:
                        first_delta_at = time.monotonic()
                    delta_chunks += 1
                    text_parts.append(joined_s)
                    if on_delta is not None:
                        emitted_visible_delta = True
                        on_delta({"type": "text", "content": joined_s})

                def emit_events(ev_list: List[Tuple[str, Any]]) -> None:
                    nonlocal first_delta_at, delta_chunks, streamed_any_tool, first_tool_done_at, post_tool_hallucination, emitted_visible_delta
                    for kind, val in ev_list:
                        if kind == "thinking":
                            if first_delta_at is None:
                                first_delta_at = time.monotonic()
                            delta_chunks += 1
                            think_parts.append(val)
                            if on_delta is not None:
                                emitted_visible_delta = True
                                on_delta({"type": "thinking", "content": val})
                        elif kind == "text":
                            if not val:
                                continue
                            if self.early_stop_on_tool_call and tool_calls and env_parser.can_early_stop_after_tool_call():
                                s_val = str(val).strip()
                                if s_val:
                                    post_tool_buf.append(s_val)
                                    joined_post = "".join(post_tool_buf)
                                    if len(joined_post) >= 12 or "[tool result" in joined_post.lower():
                                        post_tool_hallucination = True
                                continue
                            if not text_parts and not streamed_any_tool:
                                probe = ("".join(held_canned_suspect) + val).lstrip().lower()
                                if len(probe) < 220 and (
                                    any(probe.startswith(pfx) for pfx in CANNED_UPSTREAM_PREFIXES)
                                    or any(pfx.startswith(probe) for pfx in CANNED_UPSTREAM_PREFIXES if len(probe) <= 48)
                                ):
                                    held_canned_suspect.append(val)
                                    continue
                                flush_held_suspect()
                            if first_delta_at is None:
                                first_delta_at = time.monotonic()
                            delta_chunks += 1
                            text_parts.append(val)
                            if on_delta is not None:
                                emitted_visible_delta = True
                                on_delta({"type": "text", "content": val})
                        elif kind == "tool_start":
                            flush_held_suspect()
                            if first_delta_at is None:
                                first_delta_at = time.monotonic()
                            streamed_any_tool = True
                            delta_chunks += 1
                            if on_delta is not None:
                                emitted_visible_delta = True
                                on_delta({"type": "tool_start", "id": val["id"], "name": val["name"]})
                        elif kind == "tool_delta":
                            delta_chunks += 1
                            if on_delta is not None:
                                emitted_visible_delta = True
                                on_delta({"type": "tool_delta", "id": val["id"], "partial_json": val["partial_json"]})
                        elif kind == "tool_stop":
                            if on_delta is not None:
                                on_delta({"type": "tool_stop", "id": val["id"]})
                        elif kind == "tool_call":
                            flush_held_suspect()
                            tool_calls.append(val)
                            first_tool_done_at = time.monotonic()
                            post_tool_buf.clear()
                            post_tool_hallucination = False
                        elif kind == "malformed_envelope":
                            malformed_envelopes.append(val)

                def flush_guard_buffer() -> None:
                    nonlocal model_confirmed, held_guard_chars
                    if not model_confirmed:
                        model_confirmed = True
                        if held_guard_events:
                            pending = list(held_guard_events)
                            held_guard_events.clear()
                            held_guard_chars = 0
                            emit_events(pending)

                def handle_parser_events(ev_list: List[Tuple[str, Any]]) -> None:
                    nonlocal held_guard_chars, first_guard_at
                    if not ev_list:
                        return
                    if model_confirmed:
                        emit_events(ev_list)
                        return
                    if first_guard_at is None:
                        first_guard_at = time.monotonic()
                    for kind, val in ev_list:
                        held_guard_events.append((kind, val))
                        if isinstance(val, str):
                            held_guard_chars += len(val)
                        elif isinstance(val, dict):
                            held_guard_chars += len(str(val))
                    if (
                        (
                            self.early_stop_on_tool_call
                            and any(k == "tool_call" for k, _ in held_guard_events)
                            and env_parser.can_early_stop_after_tool_call()
                        )
                        or held_guard_chars >= 512
                        or (time.monotonic() - first_guard_at) >= 1.5
                    ):
                        flush_guard_buffer()

                hijack_detected_label: Optional[str] = None

                def check_hijack_or_confirm(rm_val: str) -> bool:
                    nonlocal routed_model_seen, hijack_detected_label
                    if not rm_val:
                        return False
                    routed_model_seen = rm_val
                    rm_low = rm_val.lower()
                    if should_guard_hijack and ("lite" in rm_low or "3.5 flash" in rm_low):
                        held_guard_events.clear()
                        hijack_detected_label = rm_val
                        self.abort_stream(req_id)
                        return True
                    flush_guard_buffer()
                    return False

                def maybe_early_stop() -> bool:
                    nonlocal early_stopped
                    if not (
                        self.early_stop_on_tool_call
                        and tool_calls
                        and post_tool_hallucination
                        and env_parser.can_early_stop_after_tool_call()
                    ):
                        return False
                    cur_now = wrb_decoder.cursor()
                    if len(cur_now) != 3 or not all(cur_now):
                        return False
                    # Only early-stop when post-tool hallucination is actually detected (0.12s);
                    # clean tool calls wait for upstream `done` so server-side cursor commits with zero 1097 rewinds.
                    if first_tool_done_at is None or (time.monotonic() - first_tool_done_at) < 0.12:
                        return False
                    flush_guard_buffer()
                    handle_parser_events(env_parser.finish())
                    early_stopped = True
                    self.background_drain_stream(req_id, max_drain_ms=6000)
                    return True

                retryable_err: Optional[str] = None
                done_seen = False

                def handle_wrb_rpc_error(err_val: str) -> None:
                    nonlocal hit_429, retryable_err, done_seen
                    d_low = err_val.lower()
                    is_rl = (
                        "code=429" in d_low
                        or "code=1037" in d_low
                        or "code=1095" in d_low
                        or "[1095]" in d_low
                        or "code=8 " in d_low
                        or "resource_exhausted" in d_low
                    )
                    self.abort_stream(req_id)
                    _cleanup_orphan()
                    if is_rl:
                        hit_429 = True
                        can_retry_cleanly = (not text_parts and not streamed_any_tool and not emitted_visible_delta)
                        if can_retry_cleanly and same_model_retries < same_model_max_retries and (deadline - time.monotonic()) > 4.0:
                            retryable_err = "HTTP_429"
                            done_seen = True
                            return
                        raise RuntimeError(f"[HTTP_429] Upstream rate limit/quota on model={model_id}: {err_val}")
                    raise RuntimeError(f"[WRB_RPC_ERROR] {err_val}")

                try:
                    sock = self._open_stream_socket(min(rem, 15.0))
                    low_t = (attempt_tier or "").lower()
                    if slot_80_used == 2 or slot_79_used in (8, 3):
                        hdr_ms, first_wrb_ms, idle_ms = 120000, 480000, 540000
                    elif slot_79_used == 6 or low_t in ("low", "disabled", "minimal", "none"):
                        hdr_ms, first_wrb_ms, idle_ms = 45000, 60000, 90000
                    else:
                        hdr_ms, first_wrb_ms, idle_ms = 90000, 180000, 240000
                    rem_ms = max(12000, int((rem - 1.0) * 1000))
                    req_obj: Dict[str, Any] = {
                        "op": "stream_generate",
                        "reqId": req_id,
                        "fReq": freq_str,
                        "model": model_id,
                        "hl": hl,
                        "hasCursor": bool(any(cursor)),
                        "headersTimeoutMs": min(hdr_ms, rem_ms),
                        "firstWrbTimeoutMs": min(first_wrb_ms, rem_ms),
                        "idleTimeoutMs": min(idle_ms, rem_ms),
                    }
                    if extra_headers:
                        req_obj["extraHeaders"] = extra_headers
                    if fallback_bl:
                        req_obj["fallbackBl"] = fallback_bl
                    sock.sendall((json.dumps(req_obj, ensure_ascii=False) + "\n").encode("utf-8"))
                    sock.settimeout(0.25)

                    sock_buf = bytearray()
                    while not done_seen and hijack_detected_label is None and not early_stopped:
                        if cancel_event is not None and cancel_event.is_set():
                            self.abort_stream(req_id)
                            _cleanup_orphan()
                            raise RuntimeError("[CLIENT_CANCELLED] Client disconnected")
                        if time.monotonic() >= deadline:
                            self.abort_stream(req_id)
                            _cleanup_orphan()
                            raise RuntimeError(f"[STREAM_TIMEOUT] WebKit stream exceeded {timeout_sec:.1f}s deadline")
                        sock.settimeout(0.08 if first_tool_done_at is not None else 0.25)
                        try:
                            chunk = sock.recv(65536)
                        except socket.timeout:
                            if maybe_early_stop():
                                break
                            continue
                        if not chunk:
                            break
                        sock_buf.extend(chunk)
                        while True:
                            nl = sock_buf.find(b"\n")
                            if nl == -1:
                                break
                            raw_ln = bytes(sock_buf[:nl]).strip()
                            del sock_buf[: nl + 1]
                            if not raw_ln:
                                continue
                            try:
                                msg = json.loads(raw_ln.decode("utf-8", errors="replace"))
                            except Exception:
                                continue
                            if not isinstance(msg, dict):
                                continue
                            ev = msg.get("event", "")
                            if ev == "meta":
                                if msg.get("routed_model"):
                                    if check_hijack_or_confirm(str(msg["routed_model"])):
                                        break
                            elif ev == "raw_chunk":
                                wrb_deltas = wrb_decoder.feed_raw_chunk(str(msg.get("data", "")))
                                if wrb_decoder.routed_model:
                                    if check_hijack_or_confirm(wrb_decoder.routed_model):
                                        break
                                for d_kind, d_val in wrb_deltas:
                                    if d_kind == "thinking":
                                        handle_parser_events(env_parser.feed_thinking(d_val))
                                    elif d_kind == "error":
                                        handle_wrb_rpc_error(d_val)
                                        break
                                    else:
                                        handle_parser_events(env_parser.feed_text(d_val))
                                if early_stopped or hijack_detected_label is not None or done_seen:
                                    break
                            elif ev == "direct_delta":
                                if msg.get("routed_model"):
                                    if check_hijack_or_confirm(str(msg["routed_model"])):
                                        break
                                d_kind = msg.get("kind", "text")
                                d_val = str(msg.get("data", ""))
                                if msg.get("conversation_id"):
                                    wrb_decoder.conversation_id = str(msg["conversation_id"])
                                if msg.get("response_id"):
                                    wrb_decoder.response_id = str(msg["response_id"])
                                if msg.get("choice_id"):
                                    wrb_decoder.choice_id = str(msg["choice_id"])
                                if d_kind == "thinking":
                                    handle_parser_events(env_parser.feed_thinking(d_val))
                                elif d_val:
                                    handle_parser_events(env_parser.feed_text(d_val))
                            elif ev == "error":
                                code = str(msg.get("code", ""))
                                err_msg = str(msg.get("message", "WebKit stream error"))
                                if code == "CLIENT_ABORTED" and early_stopped:
                                    done_seen = True
                                    break
                                if code == "CLIENT_ABORTED" and cancel_event is not None and cancel_event.is_set():
                                    _cleanup_orphan()
                                    raise RuntimeError("[CLIENT_CANCELLED] Client disconnected")
                                is_429 = ("429" in code or "RESOURCE_EXHAUSTED" in code.upper() or "[1095]" in err_msg or "1037" in err_msg)
                                if is_429:
                                    hit_429 = True
                                can_retry_cleanly = (not text_parts and not streamed_any_tool and not emitted_visible_delta)
                                if code == "BRIDGE_WARMING_UP" and can_retry_cleanly and attempt < len(warmup_retry_delays) and (deadline - time.monotonic()) > 3.0:
                                    retryable_err = code
                                    done_seen = True
                                    break
                                if (
                                    (
                                        code in (
                                            "TRANSIENT_EMPTY_STREAM",
                                            "FETCH_ERR",
                                            "FETCH_TIMEOUT",
                                            "FIRST_TOKEN_TIMEOUT",
                                            "STREAM_IDLE_TIMEOUT",
                                            "WEBCONTENT_TERMINATED",
                                            "NAVIGATION_FAILED",
                                            "JS_CALL_ERR",
                                        )
                                        or is_429
                                    )
                                    and can_retry_cleanly
                                    and same_model_retries < same_model_max_retries
                                    and (deadline - time.monotonic()) > 3.0
                                ):
                                    retryable_err = "HTTP_429" if is_429 else code
                                    done_seen = True
                                    break
                                _cleanup_orphan()
                                if is_429:
                                    raise RuntimeError(f"[HTTP_429] {err_msg}")
                                raise RuntimeError(f"[{code}] {err_msg}")
                            elif ev == "done":
                                for d_kind, d_val in wrb_decoder.flush():
                                    if d_kind == "thinking":
                                        handle_parser_events(env_parser.feed_thinking(d_val))
                                    elif d_kind == "error":
                                        handle_wrb_rpc_error(d_val)
                                        break
                                    else:
                                        handle_parser_events(env_parser.feed_text(d_val))
                                if hijack_detected_label is not None:
                                    break
                                flush_guard_buffer()
                                done_seen = True
                                break
                        if not done_seen and hijack_detected_label is None and not early_stopped:
                            if maybe_early_stop():
                                break
                finally:
                    if sock is not None:
                        try:
                            sock.close()
                        except Exception:
                            pass

                if hijack_detected_label is not None:
                    _cleanup_orphan()
                    if same_model_retries < same_model_max_retries and (deadline - time.monotonic()) > 4.0:
                        same_model_retries += 1
                        attempt += 1
                        if not os.environ.get("DSH_QUIET"):
                            sys.stderr.write(
                                f"[webkit_plane] Same-model retry {same_model_retries}/{same_model_max_retries} "
                                f"after upstream hijack to {hijack_detected_label!r} (model={model_id} tier={attempt_tier or 'default'})\n"
                            )
                            sys.stderr.flush()
                        _wait_backoff_or_abort(0.8)
                        continue
                    raise RuntimeError(
                        f"[UPSTREAM_MODEL_HIJACK] Requested model '{model_id}' (mode={slot_79_used}) was silently downgraded by upstream to '{hijack_detected_label}'"
                    )

                if retryable_err is not None:
                    _cleanup_orphan()
                    is_warmup = (retryable_err == "BRIDGE_WARMING_UP")
                    is_rl_retry = ("429" in retryable_err or "RESOURCE_EXHAUSTED" in retryable_err.upper())
                    if not is_warmup:
                        same_model_retries += 1
                    if not os.environ.get("DSH_QUIET"):
                        sys.stderr.write(
                            f"[webkit_plane] Pre-delta same-model retry (attempt={attempt + 1}) "
                            f"after {retryable_err} (model={model_id} tier={attempt_tier or 'default'} "
                            f"elapsed={time.monotonic() - attempt_t0:.1f}s)\n"
                        )
                        sys.stderr.flush()
                    wait_delay = (1.6 + random.uniform(0.0, 0.35)) if is_rl_retry else warmup_retry_delays[min(attempt, len(warmup_retry_delays) - 1)]
                    _wait_backoff_or_abort(wait_delay)
                    attempt += 1
                    continue

                if not done_seen and not (early_stopped and tool_calls):
                    _cleanup_orphan()
                    if not text_parts and not streamed_any_tool and not emitted_visible_delta and same_model_retries < same_model_max_retries:
                        same_model_retries += 1
                        attempt += 1
                        _wait_backoff_or_abort(0.4)
                        continue
                    raise RuntimeError("[BRIDGE_PREMATURE_EOF] WebKit bridge socket closed before stream done event")

                if not early_stopped:
                    handle_parser_events(env_parser.finish())
                    flush_guard_buffer()

                if held_canned_suspect and not text_parts and not tool_calls and not streamed_any_tool:
                    cand_err_txt = "".join(held_canned_suspect).strip()
                    if CANNED_UPSTREAM_ERR_RE.search(cand_err_txt):
                        _cleanup_orphan()
                        if not any(cursor) and same_model_retries < same_model_max_retries:
                            same_model_retries += 1
                            attempt += 1
                            _wait_backoff_or_abort(0.4)
                            continue
                        raise RuntimeError(f"[UPSTREAM_CANNED_ERROR] {cand_err_txt[:160]}")
                flush_held_suspect()

                new_cursor = wrb_decoder.cursor()
                if len(new_cursor) != 3 or not all(new_cursor):
                    _cleanup_orphan()
                    new_cursor = cursor

                now_m = time.monotonic()
                ttft_ms = max(1, int(((first_delta_at or now_m) - t0) * 1000))
                smode = "plane1_webkit_early_stop" if early_stopped else "plane1_webkit_native"
                if attempt > 0:
                    smode += f"+retried({attempt})"
                return {
                    "stop_reason": "tool_use" if tool_calls else "end_turn",
                    "tool_calls": tool_calls,
                    "malformed_envelopes": malformed_envelopes,
                    "streamed_any_tool": streamed_any_tool,
                    "routed_model": routed_model_seen,
                    "wire_slot_79": slot_79_used,
                    "wire_slot_80": slot_80_used,
                    "text": "".join(text_parts),
                    "thinking": "".join(think_parts),
                    "cursor": new_cursor,
                    "cursor_committed": bool(done_seen),
                    "early_stopped": early_stopped,
                    "retries": attempt,
                    "ttft_ms": ttft_ms,
                    "elapsed_ms": max(1, int((now_m - t0) * 1000)),
                    "delta_chunks": delta_chunks,
                    "stream_mode": smode,
                }
        finally:
            self.gate.release(hit_rate_limit=hit_429)
