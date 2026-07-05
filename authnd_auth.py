"""
authnd_auth.py - NVIDIA Build browser-backed chat route.

This module intentionally does not use the NVIDIA API key flow. It opens the
public build.nvidia.com model page in Qt WebEngine to obtain the same hCaptcha
token the page uses, then sends the hidden /v2/predict request with requests.
"""

from __future__ import annotations

import argparse
import codecs
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import requests


BUILD_BASE_URL = "https://build.nvidia.com"
API_BASE_URL = "https://api.ngc.nvidia.com"
DEFAULT_ORG_ID = "qc69jvmznzxy"
DEFAULT_HCAPTCHA_SITEKEY = "0c6a1e45-75d7-43cc-b836-a0c9d886b8ee"
DEFAULT_PUBLISHER = "deepseek-ai"
DEFAULT_MODEL = "moonshotai/kimi-k2.6"
DEFAULT_TIMEOUT = None
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

_cancel_event = threading.Event()
_thread_local = threading.local()
_metadata_cache: Dict[str, Dict[str, str]] = {}
_metadata_lock = threading.Lock()
_token_semaphores: Dict[str, threading.BoundedSemaphore] = {}
_token_semaphore_sizes: Dict[str, int] = {}
_global_token_semaphore: Optional[threading.BoundedSemaphore] = None
_global_token_semaphore_size: Optional[int] = None
_token_subprocess_semaphore: Optional[threading.BoundedSemaphore] = None
_token_subprocess_semaphore_size: Optional[int] = None


def _debug_enabled() -> bool:
    value = os.getenv("AUTHND_DEBUG", "").strip().lower()
    return value in ("1", "true", "yes", "on", "debug")


def _log(log_fn: Optional[Callable[[str], None]], message: str, *, debug_only: bool = False) -> None:
    if not log_fn:
        return
    if debug_only and not _debug_enabled():
        return
    log_fn(message)


def _labeled_log_fn(
    log_fn: Optional[Callable[[str], None]],
    request_label: Optional[str],
) -> Optional[Callable[[str], None]]:
    label = str(request_label or "").strip()
    if not log_fn or not label:
        return log_fn

    def _log_with_label(message: str) -> None:
        log_fn(f"[{label}] {message}")

    return _log_with_label


def _short_error(error: Any, limit: int = 1200) -> str:
    text = str(error or "").replace("\r", " ").replace("\n", " ").strip()
    html_summary = _html_error_summary(text)
    if html_summary:
        return html_summary
    lower_text = text.lower()
    if "rate-limited" in lower_text or "rate limited" in lower_text or "ratelimited" in lower_text:
        return "hCaptcha rate-limited; rerouting proxy retry"
    if (
        "authnd token helper failed" in lower_text
        and (
            "failed to create shared context" in lower_text
            or "contextresult::kfatalfailure" in lower_text
            or "failed to create gles" in lower_text
        )
    ):
        return "token browser/GPU helper failed; rerouting proxy retry"
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _html_error_summary(text: str) -> str:
    if not text or "<" not in text or ">" not in text:
        return ""
    cleaned = re.sub(r"<!--.*?-->", " ", text, flags=re.DOTALL)
    if not re.search(r"<(?:html|head|title|body|center|h1)\b", cleaned, flags=re.IGNORECASE):
        return ""
    prefix_match = re.match(r"^(AuthND HTTP\s+\d+\s*:\s*)", cleaned, flags=re.IGNORECASE)
    prefix = prefix_match.group(1) if prefix_match else ""
    for pattern in (
        r"<title[^>]*>(.*?)</title>",
        r"<h1[^>]*>(.*?)</h1>",
    ):
        match = re.search(pattern, cleaned, flags=re.IGNORECASE | re.DOTALL)
        if match:
            summary = _strip_html_text(match.group(1))
            if prefix and re.match(r"^\d{3}\s+", summary):
                summary = re.sub(r"^\d{3}\s+", "", summary, count=1)
            return f"{prefix}{summary}".strip()
    summary = _strip_html_text(cleaned)
    return f"{prefix}{summary}".strip() if summary else ""


def _strip_html_text(text: str) -> str:
    text = re.sub(r"<!--.*?-->", " ", str(text or ""), flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _http_error_detail(status_code: int, reason: str, nv_error: str, body: str) -> str:
    body_summary = _html_error_summary(body) or _strip_html_text(body)
    if body_summary and body_summary.startswith(f"{status_code} "):
        body_summary = body_summary[len(str(status_code)):].strip()
    if len(body_summary) > 900:
        body_summary = body_summary[:897].rstrip() + "..."
    detail = " ".join(part for part in (nv_error, body_summary) if part).strip()
    return detail or reason or "HTTP error"


def _message_summary(messages: List[Dict[str, str]]) -> Dict[str, Any]:
    roles: Dict[str, int] = {}
    chars = 0
    for message in messages:
        role = str(message.get("role") or "unknown")
        roles[role] = roles.get(role, 0) + 1
        chars += len(str(message.get("content") or ""))
    return {"count": len(messages), "roles": roles, "chars": chars}


def _payload_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    summary = {
        "model": payload.get("model"),
        "stream": payload.get("stream"),
        "messages": _message_summary(payload.get("messages") or []),
    }
    for key in ("temperature", "max_tokens", "top_p", "frequency_penalty", "presence_penalty", "reasoning_effort"):
        if key in payload:
            summary[key] = payload.get(key)
    if "chat_template_kwargs" in payload:
        summary["chat_template_kwargs"] = payload.get("chat_template_kwargs")
    return summary


def _stream_logging_enabled() -> bool:
    value = os.getenv("AUTHND_LOG_STREAM_CHUNKS")
    if value is None:
        value = os.getenv("LOG_STREAM_CHUNKS", "1")
    return str(value).strip().lower() not in ("0", "false", "no", "off")


def _stream_status_logging_enabled() -> bool:
    value = os.getenv("AUTHND_LOG_STREAM_STATUS")
    if value is None:
        value = os.getenv("LOG_STREAM_STATUS", "1")
    return str(value).strip().lower() not in ("0", "false", "no", "off")


def _stream_thinking_logging_enabled() -> bool:
    value = os.getenv("AUTHND_STREAM_THINKING_LOGS")
    if value is None:
        value = os.getenv("STREAM_THINKING_LOGS", "1")
    return str(value).strip().lower() not in ("0", "false", "no", "off")


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def cancel_stream() -> None:
    """Signal any active AuthND stream/request to stop."""
    _cancel_event.set()


def reset_cancel() -> None:
    """Clear the cancellation flag before a new request."""
    _cancel_event.clear()


def _is_cancelled() -> bool:
    return _cancel_event.is_set()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _token_lane(proxy: Optional[str]) -> str:
    return str(proxy or "direct")


def _get_token_semaphore(proxy: Optional[str] = None) -> threading.BoundedSemaphore:
    lane = _token_lane(proxy)
    size = max(1, _env_int("AUTHND_TOKEN_CONCURRENCY", 5))
    if lane not in _token_semaphores or _token_semaphore_sizes.get(lane) != size:
        _token_semaphores[lane] = threading.BoundedSemaphore(size)
        _token_semaphore_sizes[lane] = size
    return _token_semaphores[lane]


def _get_global_token_semaphore() -> threading.BoundedSemaphore:
    global _global_token_semaphore, _global_token_semaphore_size
    size = max(1, _env_int("AUTHND_TOKEN_GLOBAL_CONCURRENCY", 10))
    if _global_token_semaphore is None or _global_token_semaphore_size != size:
        _global_token_semaphore = threading.BoundedSemaphore(size)
        _global_token_semaphore_size = size
    return _global_token_semaphore


def _get_token_subprocess_semaphore() -> threading.BoundedSemaphore:
    global _token_subprocess_semaphore, _token_subprocess_semaphore_size
    size = max(1, _env_int("AUTHND_TOKEN_SUBPROCESS_CONCURRENCY", 10))
    if _token_subprocess_semaphore is None or _token_subprocess_semaphore_size != size:
        _token_subprocess_semaphore = threading.BoundedSemaphore(size)
        _token_subprocess_semaphore_size = size
    return _token_subprocess_semaphore


def _acquire_semaphore(semaphore: threading.BoundedSemaphore) -> None:
    while not _is_cancelled():
        if semaphore.acquire(timeout=0.5):
            return
    raise RuntimeError("stream cancelled")


def _run_with_token_slot(fn: Callable[[], str], proxy: Optional[str] = None) -> str:
    lane_semaphore = _get_token_semaphore(proxy)
    global_semaphore = _get_global_token_semaphore()
    _acquire_semaphore(lane_semaphore)
    global_acquired = False

    try:
        _acquire_semaphore(global_semaphore)
        global_acquired = True
        return fn()
    finally:
        if global_acquired:
            global_semaphore.release()
        lane_semaphore.release()


def _is_hcaptcha_rate_limited(error: Any) -> bool:
    message = str(error or "").lower()
    return "rate-limited" in message or "rate limited" in message or "ratelimited" in message


def _is_token_load_failure(error: Any) -> bool:
    message = str(error or "").lower()
    return (
        "browser failed to load" in message
        or "failed to create shared context" in message
        or "contextresult::kfatalfailure" in message
        or "failed to create gles" in message
    )


def _token_retry_sleep(error: Any, attempt_number: int) -> float:
    if _is_hcaptcha_rate_limited(error):
        base = max(1, _env_int("AUTHND_TOKEN_RATE_LIMIT_SLEEP", 30))
        return min(180.0, base + random.uniform(0.0, min(base, 30.0)))
    if _is_token_load_failure(error):
        base = max(1, _env_int("AUTHND_TOKEN_LOAD_FAIL_SLEEP", 8))
        return min(90.0, base + random.uniform(0.0, min(base, 15.0)))
    return min(10.0, 0.5 * attempt_number)


def _build_page_model_slug(model_id: str) -> str:
    """Return the NVIDIA Build page slug for a model id."""
    model_id = str(model_id or "").strip("/")
    return re.sub(r"(?<=\d)\.(?=\d)", "_", model_id)


def _normalize_model(model: str) -> Tuple[str, str, str]:
    """
    Return (publisher, model_id, page_url) from authnd/moonshotai/kimi-k2.6 or kimi-k2.6.
    """
    raw = (model or "").strip()
    raw = re.sub(r"^authnd\d{0,4}/", "", raw, flags=re.IGNORECASE)
    if raw.lower().startswith("authnd"):
        raw = raw[len("authnd"):].lstrip("/")
    raw = raw.strip("/")
    if not raw:
        raw = DEFAULT_MODEL

    if "/" in raw:
        publisher, model_id = raw.split("/", 1)
        model_id = model_id.strip("/")
    else:
        publisher = os.getenv("AUTHND_DEFAULT_PUBLISHER", DEFAULT_PUBLISHER).strip("/") or DEFAULT_PUBLISHER
        model_id = raw

    page_url = f"{BUILD_BASE_URL}/{publisher}/{_build_page_model_slug(model_id)}"
    return publisher, model_id, page_url


def _payload_model_name(model_path: str) -> str:
    model = model_path.strip("/")
    if model.startswith("stg/"):
        return model
    if os.getenv("AUTHND_ENABLE_STG_PREFIX", "0").lower() in ("1", "true", "yes"):
        return f"stg/{model}"
    return model


def _resolve_model_metadata(page_url: str, proxy: Optional[str] = None) -> Dict[str, str]:
    with _metadata_lock:
        cached = _metadata_cache.get(page_url)
        if cached:
            return dict(cached)

    response = requests.get(
        page_url,
        headers={"user-agent": USER_AGENT, "accept": "text/html,application/xhtml+xml"},
        timeout=60,
        proxies={"http": proxy, "https": proxy} if proxy else None,
    )
    response.raise_for_status()
    html = response.text or ""

    def _match(patterns: Iterable[str]) -> str:
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return ""

    function_id = os.getenv("AUTHND_NVCF_FUNCTION_ID", "").strip() or _match((
        r'\\"nvcfFunctionId\\":\\"([^"\\]+)\\"',
        r'"nvcfFunctionId"\s*:\s*"([^"]+)"',
    ))
    artifact_name = _match((
        r'\\"artifactName\\":\\"([^"\\]+)\\"',
        r'"artifactName"\s*:\s*"([^"]+)"',
    ))
    payload_model = _match((
        r'\\"model\\"\s*:\s*\\"([^"\\]+)\\"',
        r'"model"\s*:\s*"([^"]+)"',
    ))
    namespace = os.getenv("AUTHND_NGC_ORG", "").strip("/") or _match((
        r'\\"namespace\\":\\"([^"\\]+)\\"',
        r'"namespace"\s*:\s*"([^"]+)"',
    )) or DEFAULT_ORG_ID

    endpoint_id = os.getenv("AUTHND_PREDICT_ID", "").strip() or artifact_name or function_id
    metadata = {
        "endpoint_id": endpoint_id,
        "function_id": function_id,
        "artifact_name": artifact_name,
        "namespace": namespace,
        "payload_model": payload_model,
    }
    with _metadata_lock:
        _metadata_cache[page_url] = dict(metadata)
    return metadata


def _content_to_text(content: Any) -> str:
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
                item_type = item.get("type")
                if item_type in (None, "text", "input_text"):
                    parts.append(str(item.get("text") or ""))
        return "\n".join(p for p in parts if p)
    return str(content)


def _normalize_messages(messages: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    system_parts: List[str] = []
    normalized: List[Dict[str, str]] = []

    for message in messages or []:
        role = str(message.get("role", "user")).lower()
        content = _content_to_text(message.get("content"))
        if not content:
            continue
        if role == "system":
            system_parts.append(content)
            continue
        if role not in ("user", "assistant"):
            role = "user"
        normalized.append({"role": role, "content": content})

    if system_parts:
        system_text = "System instructions:\n" + "\n\n".join(system_parts)
        for message in normalized:
            if message["role"] == "user":
                message["content"] = f"{system_text}\n\n{message['content']}"
                break
        else:
            normalized.insert(0, {"role": "user", "content": system_text})

    if not normalized:
        normalized.append({"role": "user", "content": ""})
    return normalized


def _reasoning_toggle_enabled() -> bool:
    shared_toggle = os.getenv("ENABLE_GPT_THINKING")
    if shared_toggle is not None and shared_toggle.strip().lower() not in ("1", "true", "yes", "on", "enabled"):
        return False

    explicit = os.getenv("AUTHND_ENABLE_THINKING")
    if explicit is not None:
        if explicit.strip().lower() in ("0", "false", "no", "none", "off", "disabled"):
            return False
    return True


def _reasoning_effort() -> str:
    """Return the AuthND reasoning effort selected by shared GUI controls."""
    if not _reasoning_toggle_enabled():
        return "none"

    shared_toggle = os.getenv("ENABLE_GPT_THINKING")
    explicit = os.getenv("AUTHND_ENABLE_THINKING")
    effort = (
        os.getenv("AUTHND_REASONING_EFFORT")
        or os.getenv("GPT_EFFORT")
        or os.getenv("REASONING_EFFORT")
        or ""
    ).strip().lower()
    if effort in ("0", "false", "no", "none", "off", "disabled"):
        return "none"
    if effort in ("low", "medium", "high", "xhigh", "max", "heavy"):
        return effort

    return "medium" if explicit is not None or shared_toggle is not None else "none"


def _reasoning_control_configured() -> bool:
    return any(
        os.getenv(name) is not None
        for name in ("AUTHND_ENABLE_THINKING", "AUTHND_REASONING_EFFORT", "GPT_EFFORT", "REASONING_EFFORT", "ENABLE_GPT_THINKING")
    )


def _reasoning_enabled() -> bool:
    return _reasoning_control_configured() and _reasoning_effort() != "none"


def _normalize_reasoning_effort_value(value: Any, default: str = "medium") -> str:
    effort = str(value or "").strip().lower()
    if effort in ("0", "false", "no", "none", "off", "disabled"):
        return "none"
    if effort in ("low", "medium", "high", "xhigh", "max", "heavy"):
        return effort
    return default


def _apply_reasoning_payload(
    payload: Dict[str, Any],
    model_path: str,
    *,
    reasoning_enabled: Optional[bool] = None,
    reasoning_effort: Optional[str] = None,
) -> None:
    """
    NVIDIA NIM uses model-specific reasoning controls:
    - GPT-OSS supports top-level reasoning_effort: low/medium/high.
    - Nemotron 3 Nano supports chat_template_kwargs.parallel_reasoning_mode.
    - Kimi uses chat_template_kwargs.thinking.
    - Other thinking models generally use chat_template_kwargs.enable_thinking.
    """
    if reasoning_enabled is None and reasoning_effort is None and not _reasoning_control_configured():
        return

    if reasoning_enabled is None and reasoning_effort is None:
        effort = _reasoning_effort()
        enabled = _reasoning_toggle_enabled()
    else:
        enabled = True if reasoning_enabled is None else bool(reasoning_enabled)
        effort = (
            _normalize_reasoning_effort_value(reasoning_effort)
            if reasoning_effort is not None
            else (_reasoning_effort() if enabled else "none")
        )
    reasoning_disabled = not enabled or effort == "none"
    model_lower = (model_path or "").lower()
    is_kimi = "moonshotai/kimi" in model_lower or "kimi-k2" in model_lower

    def _kwargs() -> Dict[str, Any]:
        existing = payload.setdefault("chat_template_kwargs", {})
        return existing if isinstance(existing, dict) else {}

    if reasoning_disabled:
        kwargs = _kwargs()
        if is_kimi:
            kwargs["thinking"] = False
            kwargs.pop("enable_thinking", None)
            kwargs.pop("clear_thinking", None)
        else:
            kwargs["enable_thinking"] = False
        if "nemotron-3-nano" in model_lower:
            kwargs["parallel_reasoning_mode"] = "none"
        payload["chat_template_kwargs"] = kwargs
        return

    if "gpt-oss" in model_lower:
        payload["reasoning_effort"] = "high" if effort in ("xhigh", "max", "heavy") else effort
        return

    if "nemotron-3-nano" in model_lower:
        kwargs = _kwargs()
        kwargs["enable_thinking"] = True
        kwargs["parallel_reasoning_mode"] = "heavy" if effort in ("high", "xhigh", "max", "heavy") else effort
        payload["chat_template_kwargs"] = kwargs
        return

    if "deepseek-v4" in model_lower:
        payload["reasoning_effort"] = "max" if effort in ("xhigh", "max", "heavy") else "high"

    if is_kimi:
        kwargs = _kwargs()
        kwargs["thinking"] = True
        kwargs.pop("enable_thinking", None)
        kwargs.pop("clear_thinking", None)
        payload["chat_template_kwargs"] = kwargs
        return

    kwargs = _kwargs()
    kwargs.setdefault("enable_thinking", True)
    kwargs.setdefault("clear_thinking", False)
    payload["chat_template_kwargs"] = kwargs


def _get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"user-agent": USER_AGENT})
        _thread_local.session = session
    return session


def _extract_json_from_process(stdout: str) -> Dict[str, Any]:
    for line in reversed((stdout or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise RuntimeError("AuthND token helper did not return JSON")


def _is_frozen_app() -> bool:
    return bool(getattr(sys, "frozen", False))


def _qt_proxy_server_arg(proxy: Optional[str]) -> Optional[str]:
    if not proxy:
        return None
    proxy = str(proxy).strip()
    if not proxy:
        return None
    if proxy.lower().startswith("socks5h://"):
        proxy = "socks5://" + proxy[len("socks5h://"):]
    return f"--proxy-server={proxy}"


def _mint_captcha_token_subprocess(page_url: str, timeout: int, proxy: Optional[str] = None) -> str:
    helper_timeout = max(30, int(timeout))
    if _is_frozen_app():
        # In PyInstaller builds sys.executable is the Glossarion exe, not a
        # Python interpreter.  Use a dedicated app-level helper argument so the
        # child process mints a token and exits instead of launching the GUI.
        cmd = [
            sys.executable,
            "--authnd-mint-token",
            page_url,
            "--timeout",
            str(helper_timeout),
        ]
        if proxy:
            cmd.extend(["--proxy", proxy])
    else:
        cmd = [
            sys.executable,
            os.path.abspath(__file__),
            "--mint-token",
            page_url,
            "--timeout",
            str(helper_timeout),
        ]
        if proxy:
            cmd.extend(["--proxy", proxy])
    env = os.environ.copy()
    env["AUTHND_TOKEN_HELPER"] = "1"
    env.setdefault("QT_OPENGL", "software")
    env.setdefault("QT_QUICK_BACKEND", "software")
    env.setdefault("QTWEBENGINE_DISABLE_GPU", "1")
    flags = env.get("QTWEBENGINE_CHROMIUM_FLAGS", "")
    required_flags = " ".join(
        [
            "--disable-gpu",
            "--disable-gpu-compositing",
            "--disable-gpu-rasterization",
            "--disable-gpu-sandbox",
            "--disable-accelerated-2d-canvas",
            "--disable-accelerated-video-decode",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            # --- NEW RAM SAVING FLAGS ---
            "--blink-settings=imagesEnabled=false",
            "--disable-remote-fonts",
            "--disable-features=IsolateOrigins,site-per-process",
            # ----------------------------
        ]
    )
    proxy_flag = _qt_proxy_server_arg(proxy)
    if proxy_flag:
        required_flags = f"{required_flags} {proxy_flag}"
    env["QTWEBENGINE_CHROMIUM_FLAGS"] = f"{flags} {required_flags}".strip()

    creationflags = 0
    if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW"):
        creationflags = subprocess.CREATE_NO_WINDOW

    subprocess_semaphore = _get_token_subprocess_semaphore()
    _acquire_semaphore(subprocess_semaphore)
    try:
        try:
            proc = subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                timeout=helper_timeout + 20,
                creationflags=creationflags,
            )
        except OSError as exc:
            if getattr(exc, "errno", None) == 24:
                raise RuntimeError(
                    "AuthND token helper could not start: too many open files. "
                    "Lower AUTHND_TOKEN_SUBPROCESS_CONCURRENCY or raise ulimit -n."
                ) from exc
            raise
    finally:
        subprocess_semaphore.release()
    if proc.returncode != 0:
        try:
            result = _extract_json_from_process(proc.stdout)
            error = str(result.get("error") or "").strip()
            if error:
                raise RuntimeError(f"AuthND token helper failed ({proc.returncode}): {error}")
        except RuntimeError as exc:
            if str(exc).startswith("AuthND token helper failed"):
                raise
        detail = (proc.stdout or "").strip()
        raise RuntimeError(f"AuthND token helper failed ({proc.returncode}): {detail[-1200:]}")
    result = _extract_json_from_process(proc.stdout)
    token = str(result.get("token") or "").strip()
    if not token:
        raise RuntimeError(f"AuthND token helper returned no token: {result}")
    return token


def _mint_captcha_token_qt(page_url: str, timeout: int, proxy: Optional[str] = None) -> str:
    from PySide6.QtCore import QEventLoop, QTimer, QUrl
    # ADD QWebEngineUrlRequestInterceptor to the imports here:
    from PySide6.QtWebEngineCore import QWebEnginePage, QWebEngineProfile, QWebEngineUrlRequestInterceptor
    from PySide6.QtWidgets import QApplication

    os.environ.setdefault("QT_OPENGL", "software")
    os.environ.setdefault("QT_QUICK_BACKEND", "software")
    os.environ.setdefault("QTWEBENGINE_DISABLE_GPU", "1")
    
    # Add the RAM saving flags here as well:
    required_flags = (
        "--disable-gpu --disable-gpu-compositing --disable-gpu-rasterization "
        "--disable-gpu-sandbox --disable-accelerated-2d-canvas "
        "--disable-accelerated-video-decode --disable-dev-shm-usage --no-sandbox "
        "--blink-settings=imagesEnabled=false --disable-remote-fonts "
        "--disable-features=IsolateOrigins,site-per-process"
    )
    proxy_flag = _qt_proxy_server_arg(proxy)
    if proxy_flag:
        required_flags = f"{required_flags} {proxy_flag}"
    existing_flags = os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "")
    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = f"{existing_flags} {required_flags}".strip()

    # --- NEW INTERCEPTOR CLASS ---
    class AssetBlocker(QWebEngineUrlRequestInterceptor):
        def interceptRequest(self, info):
            res_type = info.resourceType()
            # Block Images, Stylesheets, Fonts, and Media
            if res_type in (
                QWebEngineUrlRequestInterceptor.ResourceType.ResourceTypeImage,
                QWebEngineUrlRequestInterceptor.ResourceType.ResourceTypeStylesheet,
                QWebEngineUrlRequestInterceptor.ResourceType.ResourceTypeFontResource,
                QWebEngineUrlRequestInterceptor.ResourceType.ResourceTypeMedia,
            ):
                info.block(True)
                return
            
            # Fallback string matching to kill rogue assets
            url = info.requestUrl().toString().lower()
            if any(url.endswith(ext) for ext in ('.png', '.jpg', '.jpeg', '.gif', '.css', '.woff', '.woff2', '.svg')):
                info.block(True)
    # -----------------------------

    app = QApplication.instance()
    created_app = False
    if app is None:
        app = QApplication(["authnd-token-helper"])
        created_app = True

    profile_root = os.path.join(
        os.path.expanduser("~"),
        ".glossarion",
        "authnd_browser",
        str(uuid.uuid4()),
    )
    os.makedirs(profile_root, exist_ok=True)

    profile = QWebEngineProfile(f"authnd-{uuid.uuid4().hex}", app)
    profile.setHttpUserAgent(USER_AGENT)
    
    # --- INJECT THE INTERCEPTOR INTO THE PROFILE ---
    interceptor = AssetBlocker()
    profile.setUrlRequestInterceptor(interceptor)
    profile._asset_interceptor = interceptor  # Store reference so Python garbage collection doesn't delete it
    # -----------------------------------------------

    try:
        profile.setPersistentStoragePath(profile_root)
        profile.setCachePath(os.path.join(profile_root, "cache"))
    except Exception:
        pass

    page = QWebEnginePage(profile, app)
    
    # ... [The rest of the function remains exactly the same] ...

    def run_js(script: str, js_timeout_ms: int = 15000) -> Any:
        holder: Dict[str, Any] = {}
        loop = QEventLoop()

        def _done(value: Any) -> None:
            holder["value"] = value
            loop.quit()

        page.runJavaScript(script, _done)
        QTimer.singleShot(js_timeout_ms, loop.quit)
        loop.exec()
        return holder.get("value")

    load_loop = QEventLoop()
    load_state: Dict[str, Any] = {"ok": False}

    def _loaded(ok: bool) -> None:
        load_state["ok"] = bool(ok)
        load_loop.quit()

    # --- THE EMPTY DOM TRICK ---
    empty_dom = """<!DOCTYPE html>
    <html>
    <head>
        <script src="https://js.hcaptcha.com/1/api.js?render=explicit" async defer></script>
    </head>
    <body>
        <h1>Spoofed DOM</h1>
        <div id="__authnd_hcaptcha"></div>
    </body>
    </html>"""

    page.loadFinished.connect(_loaded)
    # setHtml loads the lightweight fake DOM locally, but fakes the origin to trick hCaptcha
    page.setHtml(empty_dom, QUrl(page_url)) 
    QTimer.singleShot(min(max(timeout * 1000, 15000), 60000), load_loop.quit)
    load_loop.exec()
    if not load_state.get("ok"):
        raise RuntimeError(f"AuthND browser failed to load spoofed DOM for {page_url}")
    # ---------------------------

    sitekey = os.getenv("AUTHND_HCAPTCHA_SITEKEY", DEFAULT_HCAPTCHA_SITEKEY)
    script = f"""
(() => {{
  window.__authndResult = {{pending: true, step: "starting"}};
  const sitekey = {json.dumps(sitekey)};
  const waitFor = (fn, timeoutMs = 20000) => new Promise((resolve, reject) => {{
    const start = Date.now();
    const tick = () => {{
      try {{
        if (fn()) return resolve(true);
      }} catch (e) {{}}
      if (Date.now() - start > timeoutMs) return reject(new Error("timeout waiting for hcaptcha"));
      setTimeout(tick, 250);
    }};
    tick();
  }});
  const loadScript = () => new Promise((resolve, reject) => {{
    if (window.hcaptcha) return resolve(true);
    const existing = document.querySelector("script[src*='hcaptcha.com/1/api.js']");
    if (existing) {{
      existing.addEventListener("load", () => resolve(true), {{once: true}});
      existing.addEventListener("error", () => reject(new Error("hcaptcha script failed")), {{once: true}});
      return;
    }}
    const script = document.createElement("script");
    script.src = "https://js.hcaptcha.com/1/api.js?render=explicit";
    script.async = true;
    script.defer = true;
    script.onload = () => resolve(true);
    script.onerror = () => reject(new Error("hcaptcha script failed"));
    document.head.appendChild(script);
  }});
  (async () => {{
    try {{
      await loadScript();
      await waitFor(() => window.hcaptcha && window.hcaptcha.render && window.hcaptcha.execute);
      let container = document.getElementById("__authnd_hcaptcha");
      if (!container) {{
        container = document.createElement("div");
        container.id = "__authnd_hcaptcha";
        container.style.position = "fixed";
        container.style.left = "-10000px";
        container.style.top = "0";
        document.body.appendChild(container);
      }}
      let widgetId = window.__authndWidgetId;
      if (widgetId === undefined || widgetId === null) {{
        widgetId = window.hcaptcha.render(container, {{sitekey, size: "invisible"}});
        window.__authndWidgetId = widgetId;
      }}
      const execResult = await window.hcaptcha.execute(widgetId, {{async: true}});
      const token = (execResult && execResult.response) || window.hcaptcha.getResponse(widgetId) || "";
      window.__authndResult = {{pending: false, token, execResult, error: null}};
    }} catch (error) {{
      window.__authndResult = {{
        pending: false,
        token: "",
        error: String(error && (error.stack || error.message || error))
      }};
    }}
  }})();
  return true;
}})();
"""
    run_js(script, js_timeout_ms=10000)

    deadline = time.time() + max(timeout, 30)
    last_result: Dict[str, Any] = {}
    while time.time() < deadline:
        raw = run_js("JSON.stringify(window.__authndResult || {pending:true})", js_timeout_ms=5000)
        try:
            result = json.loads(raw or "{}")
        except Exception:
            result = {}
        last_result = result
        if result and not result.get("pending", True):
            token = str(result.get("token") or "").strip()
            if token:
                return token
            raise RuntimeError(f"AuthND hCaptcha failed: {result.get('error') or result}")
        if _is_cancelled():
            raise RuntimeError("stream cancelled")
        wait_loop = QEventLoop()
        QTimer.singleShot(100, wait_loop.quit)
        wait_loop.exec()

    raise RuntimeError(f"AuthND hCaptcha timed out: {last_result}")



# Add this new function right above get_captcha_token
def _mint_captcha_token_pool(page_url: str, timeout: int, proxy: Optional[str] = None) -> str:
    import requests
    payload = {"url": page_url, "proxy": proxy}
    try:
        # Pointing to the local persistent token server
        resp = requests.post("http://127.0.0.1:8080/get-token", json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json().get("token")
    except Exception as e:
        raise RuntimeError(f"Persistent token pool failed: {e}")

def get_captcha_token(page_url: str, timeout: int = 90, proxy: Optional[str] = None) -> str:
    mode = os.getenv("AUTHND_TOKEN_MODE", "subprocess").strip().lower()
    
    if mode == "inline":
        return _mint_captcha_token_qt(page_url, timeout, proxy=proxy)
    elif mode == "pool":
        # Routes traffic to the persistent tabs
        return _mint_captcha_token_pool(page_url, timeout, proxy=proxy)
        
    return _mint_captcha_token_subprocess(page_url, timeout, proxy=proxy)

def _extract_content_from_obj(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    choices = obj.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0] or {}
        delta = choice.get("delta") or {}
        message = choice.get("message") or {}
        for candidate in (
            delta.get("content"),
            message.get("content"),
            choice.get("text"),
            choice.get("content"),
        ):
            if isinstance(candidate, str) and candidate:
                return candidate
    for key in ("output_text", "text", "content", "response"):
        value = obj.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _extract_reasoning_from_obj(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    choices = obj.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0] or {}
        delta = choice.get("delta") or {}
        message = choice.get("message") or {}
        for candidate in (
            delta.get("reasoning_content"),
            delta.get("reasoning"),
            delta.get("thinking"),
            message.get("reasoning_content"),
            message.get("reasoning"),
            message.get("thinking"),
            choice.get("reasoning_content"),
            choice.get("reasoning"),
            choice.get("thinking"),
        ):
            if isinstance(candidate, str) and candidate:
                return candidate
    for key in ("reasoning_content", "reasoning", "thinking"):
        value = obj.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _extract_finish_reason(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    choices = obj.get("choices")
    if isinstance(choices, list) and choices:
        reason = (choices[0] or {}).get("finish_reason")
        if reason:
            return str(reason)
    reason = obj.get("finish_reason") or obj.get("stop_reason")
    return str(reason) if reason else None


def _usage_completion_tokens(usage: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(usage, dict):
        return None
    for key in ("completion_tokens", "output_tokens", "generated_tokens"):
        value = usage.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _usage_reasoning_tokens(usage: Optional[Dict[str, Any]]) -> int:
    if not isinstance(usage, dict):
        return 0
    for key in ("reasoning_tokens", "thinking_tokens", "thoughts_tokens", "thoughtsTokenCount"):
        value = usage.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    details = usage.get("completion_tokens_details")
    if isinstance(details, dict):
        value = details.get("reasoning_tokens")
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return 0


def _infer_finish_reason(
    *,
    explicit_finish_reason: Optional[str],
    content: str,
    usage: Optional[Dict[str, Any]],
    requested_max_tokens: Optional[int],
    saw_done: bool,
    saw_event: bool,
    stream: bool,
) -> Tuple[str, bool, str]:
    if explicit_finish_reason:
        return explicit_finish_reason, True, "provider"

    completion_tokens = _usage_completion_tokens(usage)
    if requested_max_tokens and completion_tokens is not None and completion_tokens >= int(requested_max_tokens):
        return "length", False, "completion_tokens_reached_max_tokens"

    if not (content or "").strip():
        if saw_done or saw_event or not stream:
            return "error", False, "empty_content_without_finish_reason"
        return "incomplete", False, "no_content_no_done"

    if stream and not saw_done:
        return "incomplete", False, "stream_ended_without_done"

    return "stop", False, "done_without_finish_reason"


def _iter_utf8_lines(byte_iter: Iterable[Any]) -> Iterable[str]:
    """Yield text lines from raw SSE bytes, decoded explicitly as UTF-8.

    This avoids letting requests/httpx infer text encodings from platform or
    headers, which is how UTF-8 punctuation can be decoded incorrectly before
    JSON parsing sees it.
    """
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    buffer = ""
    for chunk in byte_iter:
        if not chunk:
            continue
        if isinstance(chunk, str):
            text = chunk
        else:
            text = decoder.decode(bytes(chunk), final=False)
        buffer += text
        while True:
            newline = buffer.find("\n")
            if newline < 0:
                break
            line = buffer[:newline]
            buffer = buffer[newline + 1:]
            yield line.rstrip("\r")

    tail = decoder.decode(b"", final=True)
    if tail:
        buffer += tail
    if buffer:
        yield buffer.rstrip("\r")


def _parse_sse_lines(
    line_iter: Iterable[Any],
    *,
    close_fn: Optional[Callable[[], None]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    log_stream: bool = True,
    log_stream_content: bool = True,
    t_start: Optional[float] = None,
    requested_max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    parts: List[str] = []
    reasoning_parts: List[str] = []
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    raw_tail: List[Any] = []
    text_log_buf: List[str] = []
    thought_log_buf: List[str] = []
    first_token_ts: Optional[float] = None
    text_streaming_started = False
    thinking_started = False
    thinking_ended = False
    thinking_chunks = 0
    thinking_start_ts: Optional[float] = None
    stream_started_ts = t_start or time.time()
    stream_thinking = log_stream_content and _stream_thinking_logging_enabled()
    saw_done = False
    saw_event = False

    def _mark_first_token() -> None:
        nonlocal first_token_ts
        if first_token_ts is None:
            first_token_ts = time.time()
            if log_stream:
                _log(log_fn, f"⏱️ AuthND: First token in {first_token_ts - stream_started_ts:.1f}s, streaming...")

    def _emit_stream_text(fragment: str) -> None:
        if not log_fn or not log_stream_content or not fragment:
            return
        text_log_buf.append(fragment.replace("\x1f", "\\x1F"))
        combined = "".join(text_log_buf)
        for tag in ("</h1>", "</h2>", "</h3>", "</h4>", "</h5>", "</h6>", "</p>"):
            combined = combined.replace(tag, tag + "\n")
        if "\n" in combined:
            lines = combined.split("\n")
            for line in lines[:-1]:
                if line:
                    log_fn(line)
            text_log_buf[:] = [lines[-1]]
        elif len(combined) >= 160:
            log_fn(combined)
            text_log_buf.clear()

    def _mark_text_streaming() -> None:
        nonlocal text_streaming_started
        if text_streaming_started:
            return
        text_streaming_started = True
        if log_fn and log_stream:
            log_fn("📝 AuthND: Text streaming...")

    def _emit_thinking(fragment: str) -> None:
        nonlocal thinking_started, thinking_chunks, thinking_start_ts
        if not fragment:
            return
        if not thinking_started:
            thinking_started = True
            thinking_start_ts = time.time()
            thinking_chunks = 0
            if log_fn and log_stream:
                log_fn("🧠 [authnd] Thinking...")
        thinking_chunks += 1
        if not log_fn or not stream_thinking:
            return
        thought_log_buf.append(fragment.replace("\\n", "\n").replace("\x1f", "\\x1F"))
        combined = "".join(thought_log_buf)
        if "\n" in combined:
            lines = combined.split("\n")
            for line in lines[:-1]:
                log_fn(f"    {line}")
            thought_log_buf[:] = [lines[-1]]
        elif len(combined) >= 160:
            log_fn(f"    {combined}")
            thought_log_buf.clear()

    def _finish_thinking_before_text() -> None:
        nonlocal thinking_ended
        if not thinking_started or thinking_ended:
            return
        thinking_ended = True
        if not log_fn or not log_stream:
            thought_log_buf.clear()
            return
        if stream_thinking:
            remainder = "".join(thought_log_buf).rstrip("\n")
            if remainder:
                for line in remainder.split("\n"):
                    log_fn(f"    {line}")
        thought_log_buf.clear()
        duration = time.time() - (thinking_start_ts or stream_started_ts)
        if not log_stream_content:
            log_fn(f"[authnd] Thinking complete ({thinking_chunks} chunks, {duration:.1f}s)")
            _mark_text_streaming()
            return
        log_fn(f"🧠 [authnd] Thinking complete ({thinking_chunks} chunks, {duration:.1f}s)")
        log_fn("Рћђ" * 50)
        _mark_text_streaming()

    for raw_line in line_iter:
        if _is_cancelled():
            if close_fn:
                close_fn()
            raise RuntimeError("stream cancelled")
        if isinstance(raw_line, bytes):
            raw_line = raw_line.decode("utf-8", errors="replace")
        if not raw_line:
            continue
        line = raw_line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line or line == "[DONE]":
            if line == "[DONE]":
                saw_done = True
                break
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        saw_event = True
        raw_tail.append(obj)
        if len(raw_tail) > 5:
            raw_tail.pop(0)
        text = _extract_content_from_obj(obj)
        reasoning = _extract_reasoning_from_obj(obj)
        if reasoning:
            _mark_first_token()
            reasoning_parts.append(reasoning)
            _emit_thinking(reasoning)
        if text:
            _mark_first_token()
            _finish_thinking_before_text()
            _mark_text_streaming()
            parts.append(text)
            _emit_stream_text(text)
        reason = _extract_finish_reason(obj)
        if reason:
            finish_reason = reason
        if isinstance(obj, dict) and isinstance(obj.get("usage"), dict):
            usage = obj.get("usage")

    if log_fn and log_stream_content and text_log_buf:
        remainder = "".join(text_log_buf).strip()
        if remainder:
            log_fn(remainder)
    if log_fn and log_stream and thinking_started and not thinking_ended:
        if stream_thinking:
            remainder = "".join(thought_log_buf).rstrip("\n")
            if remainder:
                for line in remainder.split("\n"):
                    log_fn(f"    {line}")
        duration = time.time() - (thinking_start_ts or stream_started_ts)
        log_fn(f"🧠 [authnd] Thinking complete ({thinking_chunks} chunks, {duration:.1f}s)")
    thinking_tokens = _usage_reasoning_tokens(usage)
    if log_fn and log_stream:
        if thinking_tokens:
            log_fn(f"   🧮 Thinking tokens used: {thinking_tokens:,}")
        elif reasoning_parts:
            estimated_tokens = max(1, len("".join(reasoning_parts)) // 4)
            log_fn(f"   🧮 Thinking tokens used: ~{estimated_tokens:,}")
    if log_stream:
        _log(log_fn, f"✅ AuthND: Stream finished in {time.time() - stream_started_ts:.1f}s")

    content = "".join(parts)
    final_finish_reason, finish_reason_explicit, finish_reason_inference = _infer_finish_reason(
        explicit_finish_reason=finish_reason,
        content=content,
        usage=usage,
        requested_max_tokens=requested_max_tokens,
        saw_done=saw_done,
        saw_event=saw_event,
        stream=True,
    )

    return {
        "content": content,
        "finish_reason": final_finish_reason,
        "finish_reason_explicit": finish_reason_explicit,
        "finish_reason_inference": finish_reason_inference,
        "usage": usage,
        "reasoning_content": "".join(reasoning_parts) if reasoning_parts else None,
        "raw_response": raw_tail,
    }


def _parse_sse_response(
    response: requests.Response,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    log_stream: bool = True,
    log_stream_content: bool = True,
    t_start: Optional[float] = None,
    requested_max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    return _parse_sse_lines(
        _iter_utf8_lines(response.iter_content(chunk_size=1)),
        close_fn=response.close,
        log_fn=log_fn,
        log_stream=log_stream,
        log_stream_content=log_stream_content,
        t_start=t_start,
        requested_max_tokens=requested_max_tokens,
    )


def _parse_json_response(response: requests.Response, *, requested_max_tokens: Optional[int] = None) -> Dict[str, Any]:
    try:
        obj = response.json()
    except ValueError:
        text = response.text or ""
        final_finish_reason, finish_reason_explicit, finish_reason_inference = _infer_finish_reason(
            explicit_finish_reason=None,
            content=text,
            usage=None,
            requested_max_tokens=requested_max_tokens,
            saw_done=True,
            saw_event=bool(text),
            stream=False,
        )
        return {
            "content": text,
            "finish_reason": final_finish_reason,
            "finish_reason_explicit": finish_reason_explicit,
            "finish_reason_inference": finish_reason_inference,
            "usage": None,
            "reasoning_content": None,
            "raw_response": text,
        }

    finish_reason = _extract_finish_reason(obj)
    content = _extract_content_from_obj(obj)
    reasoning = _extract_reasoning_from_obj(obj)
    usage = obj.get("usage") if isinstance(obj, dict) else None
    final_finish_reason, finish_reason_explicit, finish_reason_inference = _infer_finish_reason(
        explicit_finish_reason=finish_reason,
        content=content,
        usage=usage,
        requested_max_tokens=requested_max_tokens,
        saw_done=True,
        saw_event=True,
        stream=False,
    )

    return {
        "content": content,
        "finish_reason": final_finish_reason,
        "finish_reason_explicit": finish_reason_explicit,
        "finish_reason_inference": finish_reason_inference,
        "usage": usage,
        "reasoning_content": reasoning or None,
        "raw_response": obj,
    }


def _log_non_stream_summary(
    result: Dict[str, Any],
    *,
    log_fn: Optional[Callable[[str], None]],
    started_at: float,
) -> None:
    if not log_fn:
        return
    content = str(result.get("content") or "")
    reasoning = str(result.get("reasoning_content") or "")
    usage = result.get("usage") if isinstance(result.get("usage"), dict) else None
    elapsed = time.time() - started_at
    _log(log_fn, f"✅ AuthND: Non-stream response finished in {elapsed:.1f}s ({len(content):,} chars)")
    thinking_tokens = _usage_reasoning_tokens(usage)
    if thinking_tokens:
        _log(log_fn, f"   🧮 Thinking tokens used: {thinking_tokens:,}")
    elif reasoning:
        estimated_tokens = max(1, len(reasoning) // 4)
        _log(log_fn, f"   🧮 Thinking tokens used: ~{estimated_tokens:,}")
    else:
        _log(log_fn, "   🧮 Thinking tokens used: 0")


def _raise_for_status(response: requests.Response) -> None:
    if response.status_code < 400:
        return
    nv_error = response.headers.get("x-nv-error-msg") or response.headers.get("x-nv-error-code") or ""
    body = (response.text or "").strip()
    detail = _http_error_detail(response.status_code, response.reason, nv_error, body)
    raise RuntimeError(f"AuthND HTTP {response.status_code}: {detail}")


def _httpx_status_error(resp: Any) -> RuntimeError:
    headers = getattr(resp, "headers", {}) or {}
    nv_error = headers.get("x-nv-error-msg") or headers.get("x-nv-error-code") or ""
    reason = getattr(resp, "reason_phrase", "") or ""
    try:
        body = resp.read().decode("utf-8", errors="replace").strip()
    except Exception:
        body = ""
    detail = _http_error_detail(resp.status_code, reason, nv_error, body)
    return RuntimeError(f"AuthND HTTP {resp.status_code}: {detail}")


def _post_prediction(
    *,
    messages: List[Dict[str, str]],
    model_id: str,
    model_path: str,
    page_url: str,
    captcha_token: str,
    temperature: Optional[float],
    max_tokens: Optional[int],
    top_p: Optional[float],
    frequency_penalty: Optional[float],
    presence_penalty: Optional[float],
    timeout: int,
    connect_timeout: Optional[float],
    stream: bool,
    log_stream: Optional[bool] = None,
    log_stream_content: Optional[bool] = None,
    reasoning_enabled: Optional[bool] = None,
    reasoning_effort: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    proxy: Optional[str] = None,
) -> Dict[str, Any]:
    metadata = _resolve_model_metadata(page_url)
    org_id = metadata.get("namespace") or DEFAULT_ORG_ID
    endpoint_id = metadata.get("endpoint_id") or model_id
    payload_model = metadata.get("payload_model") or _payload_model_name(model_path)
    url = f"{API_BASE_URL}/v2/predict/models/{org_id}/{endpoint_id}"
    payload: Dict[str, Any] = {
        "messages": messages,
        "model": payload_model,
        "stream": bool(stream),
    }
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens:
        payload["max_tokens"] = int(max_tokens)
    if top_p is not None:
        payload["top_p"] = float(top_p)
    if frequency_penalty is not None:
        payload["frequency_penalty"] = float(frequency_penalty)
    if presence_penalty is not None:
        payload["presence_penalty"] = float(presence_penalty)
    _apply_reasoning_payload(
        payload,
        model_path,
        reasoning_enabled=reasoning_enabled,
        reasoning_effort=reasoning_effort,
    )

    _log(
        log_fn,
        "🔎 AuthND debug: "
        + json.dumps(
            {
                "url": url,
                "page_url": page_url,
                "metadata": {
                    "namespace": org_id,
                    "endpoint_id": endpoint_id,
                    "function_id": metadata.get("function_id") or "",
                    "artifact_name": metadata.get("artifact_name") or "",
                    "payload_model": payload_model,
                },
                "payload": _payload_summary(payload),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        debug_only=True,
    )

    request_id = str(uuid.uuid4())
    headers = {
        "accept": "text/event-stream" if stream else "application/json",
        "content-type": "application/json",
        "accept-encoding": "identity",
        "origin": BUILD_BASE_URL,
        "referer": page_url,
        "host": "api.ngc.nvidia.com",
        "nv-captcha-token": captcha_token,
        "user-agent": USER_AGENT,
    }
    if os.getenv("AUTHND_LEGACY_EXTRA_HEADERS", "0").lower() in ("1", "true", "yes"):
        headers.update({
            "nv-function-id": endpoint_id,
            "nv-model-name": model_path,
            "nv-session-id": str(uuid.uuid4()),
            "nvcf-request-id": request_id,
        })
    _log(
        log_fn,
        "🔎 AuthND debug headers: "
        + json.dumps(
            {
                "accept": headers["accept"],
                "origin": headers["origin"],
                "referer": headers["referer"],
                "legacy_extra_headers": "nv-function-id" in headers,
                "nv-captcha-token-length": len(captcha_token or ""),
                "local_request_id": request_id,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        debug_only=True,
    )

    request_started = time.time()
    status_logs_enabled = _stream_status_logging_enabled() if log_stream is None else bool(log_stream)
    content_logs_enabled = _stream_logging_enabled() if log_stream_content is None else bool(log_stream_content)
    if stream:
        try:
            import httpx as _httpx

            _timeout = _httpx.Timeout(timeout, connect=connect_timeout)
            if proxy:
                try:
                    client_context = _httpx.Client(proxy=proxy, timeout=_timeout)
                except TypeError:
                    client_context = _httpx.Client(proxies={"http://": proxy, "https://": proxy}, timeout=_timeout)
            else:
                client_context = _httpx.Client(timeout=_timeout)

            with client_context as client:
                with client.stream(
                    "POST",
                    url,
                    headers=headers,
                    json=payload,
                ) as response:
                    _log(
                        log_fn,
                        f"🔎 AuthND debug response: status={response.status_code}, content_type={response.headers.get('content-type', '')}, transport=httpx",
                        debug_only=True,
                    )
                    if response.status_code >= 400:
                        exc = _httpx_status_error(response)
                        _log(log_fn, f"⚠️ AuthND HTTP failure: {_short_error(exc)}")
                        raise exc
                    if status_logs_enabled:
                        _log(log_fn, f"🌊 AuthND: Stream opened (status={response.status_code}, transport=httpx)")
                    return _parse_sse_lines(
                        _iter_utf8_lines(response.iter_raw()),
                        close_fn=response.close,
                        log_fn=log_fn,
                        log_stream=status_logs_enabled,
                        log_stream_content=content_logs_enabled,
                        t_start=request_started,
                        requested_max_tokens=max_tokens,
                    )
        except ImportError as exc:
            _log(log_fn, f"WARNING AuthND: httpx or SOCKS support unavailable ({_short_error(exc, 200)}); falling back to requests (streaming may be buffered)")

    request_timeout: Any = timeout
    if connect_timeout is not None:
        request_timeout = (connect_timeout, timeout)

    session = _get_session()
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
    else:
        session.proxies = {}

    response = session.post(
        url,
        headers=headers,
        json=payload,
        timeout=request_timeout,
        stream=stream,
    )
    _log(
        log_fn,
        f"🔎 AuthND debug response: status={response.status_code}, content_type={response.headers.get('content-type', '')}",
        debug_only=True,
    )
    if response.status_code >= 400:
        nv_error = response.headers.get("x-nv-error-msg") or response.headers.get("x-nv-error-code") or ""
        body = (response.text or "").strip()
        detail = _http_error_detail(response.status_code, response.reason, nv_error, body)
        _log(
            log_fn,
            f"⚠️ AuthND HTTP failure: AuthND HTTP {response.status_code}: {detail}",
        )
    _raise_for_status(response)
    content_type = (response.headers.get("content-type") or "").lower()
    if stream or "text/event-stream" in content_type:
        if status_logs_enabled:
            _log(log_fn, f"🌊 AuthND: Stream opened (status={response.status_code})")
        return _parse_sse_response(
            response,
            log_fn=log_fn,
            log_stream=status_logs_enabled,
            log_stream_content=content_logs_enabled,
            t_start=request_started,
            requested_max_tokens=max_tokens,
        )
    result = _parse_json_response(response, requested_max_tokens=max_tokens)
    _log_non_stream_summary(result, log_fn=log_fn, started_at=request_started)
    return result


def send_chat_completion(
    *,
    messages: Iterable[Dict[str, Any]],
    model: str,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    top_p: Optional[float] = None,
    frequency_penalty: Optional[float] = None,
    presence_penalty: Optional[float] = None,
    timeout: Optional[int] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    connect_timeout: Optional[float] = None,
    account_id: int = 0,
    stream: Optional[bool] = None,
    log_stream: Optional[bool] = None,
    progress_label: Optional[str] = None,
    proxy: Optional[str] = None,
    reasoning_enabled: Optional[bool] = None,
    reasoning_effort: Optional[str] = None,
    request_label: Optional[str] = None,
) -> Dict[str, Any]:
    del account_id  # AuthND has no account slots; kept for unified handler symmetry.
    if _is_cancelled():
        raise RuntimeError("stream cancelled")

    publisher, model_id, page_url = _normalize_model(model)
    model_path = f"{publisher}/{model_id}"
    timeout_value = timeout
    if timeout_value is None:
        timeout_env = os.getenv("AUTHND_TIMEOUT")
        if timeout_env is not None:
            try:
                timeout_value = int(timeout_env)
            except (ValueError, TypeError):
                timeout_value = None
        else:
            timeout_value = DEFAULT_TIMEOUT
    token_timeout = _env_int("AUTHND_TOKEN_TIMEOUT", min(max(timeout_value or 180, 60), 180))
    connect_timeout_value = connect_timeout
    if connect_timeout_value is None:
        try:
            connect_timeout_value = float(os.getenv("AUTHND_CONNECT_TIMEOUT", "60.0"))
        except (ValueError, TypeError):
            connect_timeout_value = 60.0
    use_stream = stream
    if use_stream is None:
        use_stream = os.getenv("AUTHND_STREAM", "0").lower() in ("1", "true", "yes", "on")
    use_stream_status_logs = _stream_status_logging_enabled() if log_stream is None else bool(log_stream)
    use_stream_content_logs = bool(use_stream) and _stream_logging_enabled()
    effective_reasoning_enabled = _reasoning_enabled() if reasoning_enabled is None else bool(reasoning_enabled)
    effective_reasoning_effort = (
        _reasoning_effort()
        if reasoning_effort is None
        else _normalize_reasoning_effort_value(reasoning_effort)
    )
    if not effective_reasoning_enabled:
        effective_reasoning_effort = "none"
    log_fn = _labeled_log_fn(log_fn, request_label)

    if log_fn:
        log_fn(f"🌐 AuthND: opening browser token flow for {page_url}")

    normalized_messages = _normalize_messages(messages)
    _log(
        log_fn,
        "🔎 AuthND debug request: "
        + json.dumps(
            {
                "model_path": model_path,
                "page_url": page_url,
                "timeouts": {"request": timeout_value, "token": token_timeout, "connect": connect_timeout_value},
                "stream": bool(use_stream),
                "transport_stream": True,
                "params": {
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "top_p": top_p,
                    "frequency_penalty": frequency_penalty,
                    "presence_penalty": presence_penalty,
                    "reasoning_enabled": effective_reasoning_enabled,
                    "reasoning_effort": effective_reasoning_effort,
                },
                "messages": _message_summary(normalized_messages),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        debug_only=True,
    )
    last_error: Optional[Exception] = None
    token_retries = max(1, _env_int("AUTHND_TOKEN_RETRIES", _env_int("AUTHND_PROVIDER_RETRIES", 100)))

    for attempt in range(token_retries):
        if _is_cancelled():
            raise RuntimeError("stream cancelled")
        try:
            token_proxy = proxy if _env_bool("AUTHND_TOKEN_USE_PROXY", False) else None
            captcha_token = _run_with_token_slot(
                lambda: get_captcha_token(page_url, token_timeout, proxy=token_proxy),
                proxy=token_proxy,
            )
        except RuntimeError as exc:
            last_error = exc
            sleep_for = _token_retry_sleep(exc, attempt + 1)
            _log(
                log_fn,
                f"⚠️ AuthND captcha token flow failed "
                f"(attempt {attempt + 1}/{token_retries}, rerouting proxy retry in {sleep_for:.1f}s): "
                f"{_short_error(exc)}",
            )
            if attempt + 1 >= token_retries:
                raise
            time.sleep(sleep_for)
            continue
        if _is_cancelled():
            raise RuntimeError("stream cancelled")
        _log(
            log_fn,
            f"🔎 AuthND debug: captcha token acquired (length={len(captcha_token)})",
            debug_only=True,
        )
        _log(log_fn, "📨 AuthND: captcha token acquired; opening NVIDIA stream request")
        if progress_label:
            _log(log_fn, progress_label)
        try:
            result = _post_prediction(
                messages=normalized_messages,
                model_id=model_id,
                model_path=model_path,
                page_url=page_url,
                captcha_token=captcha_token,
                temperature=temperature,
                max_tokens=max_tokens,
                top_p=top_p,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
                timeout=timeout_value,
                connect_timeout=connect_timeout_value,
                stream=True,
                log_stream=use_stream_status_logs,
                log_stream_content=use_stream_content_logs,
                reasoning_enabled=effective_reasoning_enabled,
                reasoning_effort=effective_reasoning_effort,
                log_fn=log_fn,
                proxy=proxy,
            )
            result["model"] = model_id
            result["page_url"] = page_url
            return result
        except RuntimeError as exc:
            last_error = exc
            message = str(exc).lower()
            if attempt + 1 < token_retries and ("captcha" in message or "400" in message):
                if log_fn:
                    log_fn(f"⚠️ AuthND: captcha token was rejected; retrying with a fresh browser token ({_short_error(exc)})")
                continue
            raise

    raise RuntimeError(f"AuthND request failed: {last_error}")


def _read_cli_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    if value == "-":
        return sys.stdin.read()
    return value


def _read_cli_file(path: Optional[str]) -> str:
    if not path:
        return ""
    if path == "-":
        return sys.stdin.read()
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _load_cli_messages(args: argparse.Namespace) -> List[Dict[str, Any]]:
    if args.messages:
        with open(args.messages, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict) and isinstance(data.get("messages"), list):
            data = data["messages"]
        if not isinstance(data, list):
            raise ValueError("--messages must contain a JSON list or an object with a messages list")
        return data

    prompt = _read_cli_text(args.prompt) or _read_cli_file(args.prompt_file)
    if not prompt and not sys.stdin.isatty():
        prompt = sys.stdin.read()
    if not prompt:
        raise ValueError("provide --prompt, --prompt-file, --messages, or pipe prompt text on stdin")

    messages: List[Dict[str, Any]] = []
    if args.system:
        messages.append({"role": "system", "content": _read_cli_text(args.system)})
    elif args.system_file:
        messages.append({"role": "system", "content": _read_cli_file(args.system_file)})
    messages.append({"role": "user", "content": prompt})
    return messages


def _main() -> int:
    for stream_name in ("stdout", "stderr"):
        stream_obj = getattr(sys, stream_name, None)
        if hasattr(stream_obj, "reconfigure"):
            try:
                stream_obj.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

    parser = argparse.ArgumentParser(
        description="Send chat requests through NVIDIA Build's browser-backed AuthND route.",
    )
    parser.add_argument("--mint-token", dest="page_url", help=argparse.SUPPRESS)
    parser.add_argument("--proxy", help=argparse.SUPPRESS)
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model path, e.g. moonshotai/kimi-k2.6")
    parser.add_argument("--prompt", help="User prompt text. Use '-' to read stdin.")
    parser.add_argument("--prompt-file", help="UTF-8 file containing the user prompt. Use '-' to read stdin.")
    parser.add_argument("--system", help="Optional system prompt text. Use '-' to read stdin.")
    parser.add_argument("--system-file", help="UTF-8 file containing the system prompt.")
    parser.add_argument("--messages", help="JSON file containing messages list or {'messages': [...]}.")
    parser.add_argument("--temperature", type=float)
    parser.add_argument("--max-tokens", type=int)
    parser.add_argument("--top-p", type=float)
    parser.add_argument("--frequency-penalty", type=float)
    parser.add_argument("--presence-penalty", type=float)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--token-timeout", type=int, help="Browser captcha token timeout in seconds.")
    parser.add_argument("--connect-timeout", type=float)
    parser.add_argument("--stream", dest="stream", action="store_true", default=None, help="Force SSE streaming.")
    parser.add_argument("--no-stream", dest="stream", action="store_false", help="Disable SSE streaming.")
    parser.add_argument("--json", action="store_true", help="Print the full result object as JSON.")
    parser.add_argument("--output", help="Write final content to a UTF-8 file.")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logs.")
    parser.add_argument("--debug", action="store_true", help="Enable sanitized AuthND debug logs.")
    args = parser.parse_args()
    if args.page_url:
        token = _mint_captcha_token_qt(args.page_url, args.timeout, proxy=args.proxy)
        print(json.dumps({"token": token}, separators=(",", ":")), flush=True)
        return 0

    if args.debug:
        os.environ["AUTHND_DEBUG"] = "1"
    if args.token_timeout:
        os.environ["AUTHND_TOKEN_TIMEOUT"] = str(args.token_timeout)

    try:
        messages = _load_cli_messages(args)
        log_fn = None
        if not args.quiet:
            log_fn = lambda message: print(message, file=sys.stderr, flush=True)
        result = send_chat_completion(
            messages=messages,
            model=args.model,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            top_p=args.top_p,
            frequency_penalty=args.frequency_penalty,
            presence_penalty=args.presence_penalty,
            timeout=args.timeout,
            connect_timeout=args.connect_timeout,
            stream=args.stream,
            log_fn=log_fn,
        )
    except Exception as exc:
        print(f"AuthND error: {_short_error(exc)}", file=sys.stderr)
        return 1

    content = str(result.get("content") or "")
    if args.output:
        with open(args.output, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    else:
        print(content, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
