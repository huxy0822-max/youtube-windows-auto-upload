#!/usr/bin/env python3
"""
Browser local API adapter.

This module normalizes HubStudio and BitBrowser browser APIs so the upload
pipeline can use one code path for listing environments, opening a browser
window, and closing it.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from path_helpers import resolve_config_file

DEFAULT_BROWSER_SETTINGS = {
    "hubstudio": {
        "base_url": "http://127.0.0.1:6873",
        "list_endpoint": "/api/v1/env/list",
        "open_endpoint": "/api/v1/browser/start",
        "stop_endpoint": "/api/v1/browser/stop",
        "list_payload": {"page": 1, "size": 200},
        "open_payload": {},
        "stop_payload": {},
        "open_payload_id_key": "containerCode",
        "stop_payload_id_key": "containerCode",
    },
    "bitbrowser": {
        "base_url": "http://127.0.0.1:54345",
        "list_endpoint": "/browser/list",
        "open_endpoint": "/browser/open",
        "stop_endpoint": "/browser/close",
        "list_payload": {"page": 0, "pageSize": 200},
        "open_payload": {},
        "stop_payload": {},
        "open_payload_id_key": "id",
        "stop_payload_id_key": "id",
    },
}

IS_WINDOWS = platform.system() == "Windows"
BITBROWSER_OPEN_RECOVERY_ERROR_MARKERS = (
    "正在打开中",
    "打开窗口失败",
    "Failed to launch the browser process",
)
BITBROWSER_ALREADY_OPEN_MARKERS = ("正在打开中",)
BITBROWSER_RELAUNCH_MARKERS = ("打开窗口失败", "Failed to launch the browser process")


def _config_path(upload_config_path: str | Path | None) -> Path:
    if upload_config_path:
        return Path(upload_config_path)
    base_dir = Path(__file__).resolve().parent
    env_path = os.environ.get("UPLOAD_CONFIG_PATH")
    if env_path:
        return Path(env_path)
    return resolve_config_file(base_dir, "upload_config.json")


def _load_upload_config(upload_config_path: str | Path | None = None) -> dict[str, Any]:
    path = _config_path(upload_config_path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_browser_settings(upload_config_path: str | Path | None = None) -> dict[str, Any]:
    config = _load_upload_config(upload_config_path)
    browser_cfg = config.get("browser_api", {})

    cfg_provider = (browser_cfg.get("provider") or config.get("browser_provider") or "").lower()
    provider = (
        os.environ.get("BROWSER_PROVIDER")
        or browser_cfg.get("provider")
        or config.get("browser_provider")
        or "hubstudio"
    ).lower()

    defaults = DEFAULT_BROWSER_SETTINGS.get(provider, DEFAULT_BROWSER_SETTINGS["hubstudio"]).copy()
    settings = defaults.copy()
    settings["provider"] = provider

    use_browser_cfg = (not cfg_provider) or (cfg_provider == provider)
    if use_browser_cfg:
        settings["base_url"] = browser_cfg.get("base_url", defaults["base_url"])
        settings["list_endpoint"] = browser_cfg.get("list_endpoint", defaults["list_endpoint"])
        settings["open_endpoint"] = browser_cfg.get("open_endpoint", defaults["open_endpoint"])
        settings["stop_endpoint"] = browser_cfg.get("stop_endpoint", defaults["stop_endpoint"])
        settings["list_payload"] = browser_cfg.get("list_payload", defaults["list_payload"])
        settings["open_payload"] = browser_cfg.get("open_payload", defaults["open_payload"])
        settings["stop_payload"] = browser_cfg.get("stop_payload", defaults["stop_payload"])
        settings["open_payload_id_key"] = browser_cfg.get(
            "open_payload_id_key", defaults["open_payload_id_key"]
        )
        settings["stop_payload_id_key"] = browser_cfg.get(
            "stop_payload_id_key", defaults["stop_payload_id_key"]
        )
    return settings


def _post_json(
    base_url: str,
    endpoint: str,
    payload: dict[str, Any],
    *,
    provider: str = "",
    attempts: int = 3,
    timeout: int = 60,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{endpoint}"
    last_error: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if provider == "bitbrowser":
                curl_result = _post_json_with_curl(url, payload, timeout=timeout)
                if curl_result is not None:
                    return curl_result
            if attempt >= attempts:
                break
            time.sleep(min(1.5 * attempt, 4.0))
    if last_error:
        raise last_error
    raise RuntimeError(f"Request failed: {url}")


def _post_json_with_curl(url: str, payload: dict[str, Any], *, timeout: int = 60) -> dict[str, Any] | None:
    """BitBrowser 在部分 macOS 环境下会对 Python HTTP 客户端返回 502，curl 更稳定。"""
    payload_text = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))
    try:
        completed = subprocess.run(
            [
                "curl",
                "-sS",
                "-X",
                "POST",
                url,
                "-H",
                "Content-Type: application/json",
                "--data-raw",
                payload_text,
                "--connect-timeout",
                str(min(timeout, 10)),
                "--max-time",
                str(timeout),
                "-w",
                "\n%{http_code}",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout + 5,
            check=False,
        )
    except Exception:
        return None

    if completed.returncode != 0:
        return None

    stdout = (completed.stdout or "").strip()
    if not stdout:
        return None

    body, _, status_text = stdout.rpartition("\n")
    if not body:
        body = stdout
        status_text = ""

    try:
        status_code = int(status_text.strip()) if status_text.strip() else 200
    except Exception:
        status_code = 200

    if status_code >= 400:
        return None

    try:
        return json.loads(body)
    except Exception:
        return None


def _extract_error_message(result: Any) -> str:
    if isinstance(result, dict):
        return str(result.get("msg") or result.get("message") or result.get("error") or "Unknown error")
    return "Unknown error"


def _is_success(provider: str, result: Any) -> bool:
    if not isinstance(result, dict):
        return False
    if provider == "hubstudio":
        return result.get("code") == 0
    if "success" in result:
        return bool(result.get("success"))
    if "code" in result:
        return result.get("code") in (0, 200)
    return True


def _extract_data(result: Any) -> Any:
    if isinstance(result, dict):
        return result.get("data", result)
    return result


def _as_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _extract_port_from_url(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return urlparse(value).port
    except Exception:
        return None


def _normalize_hubstudio_envs(result: dict[str, Any]) -> list[dict[str, Any]]:
    items = _extract_data(result)
    if isinstance(items, dict):
        items = items.get("list", [])
    if not isinstance(items, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in items:
        normalized.append(
            {
                "serialNumber": _as_int(item.get("serialNumber")),
                "containerCode": str(item.get("containerCode", "")),
                "name": item.get("name") or item.get("envName") or "",
                "tag": item.get("tagName") or item.get("tag") or item.get("groupName") or "",
                "remark": item.get("remark") or "",
                "_raw": item,
            }
        )
    return normalized


def _normalize_bitbrowser_envs(result: dict[str, Any]) -> list[dict[str, Any]]:
    data = _extract_data(result)
    if isinstance(data, dict):
        items = (
            data.get("list")
            or data.get("browserList")
            or data.get("rows")
            or data.get("items")
            or []
        )
    elif isinstance(data, list):
        items = data
    else:
        items = []

    normalized: list[dict[str, Any]] = []
    for item in items:
        normalized.append(
            {
                "serialNumber": _as_int(
                    item.get("seq")
                    or item.get("serialNumber")
                    or item.get("browserSeq")
                    or item.get("sortNum")
                ),
                "containerCode": str(
                    item.get("id")
                    or item.get("browserId")
                    or item.get("containerCode")
                    or ""
                ),
                "name": item.get("name") or item.get("browserName") or "",
                "tag": item.get("groupName") or item.get("tag") or "",
                "remark": item.get("remark") or item.get("browserRemark") or item.get("description") or "",
                "_raw": item,
            }
        )
    return normalized


def _should_try_bitbrowser_recovery(message: str) -> bool:
    if not message:
        return False
    return any(marker in message for marker in BITBROWSER_OPEN_RECOVERY_ERROR_MARKERS)


def _should_recover_existing_window(message: str) -> bool:
    if not message:
        return False
    return any(marker in message for marker in BITBROWSER_ALREADY_OPEN_MARKERS)


def _should_force_relaunch_window(message: str) -> bool:
    if not message:
        return False
    return any(marker in message for marker in BITBROWSER_RELAUNCH_MARKERS)


def _run_powershell_json(command: str) -> Any:
    if not IS_WINDOWS:
        return None
    try:
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )
    except Exception:
        return None
    if completed.returncode != 0:
        return None
    stdout = (completed.stdout or "").strip()
    if not stdout:
        return None
    try:
        return json.loads(stdout)
    except Exception:
        return None


def _normalize_process_rows(rows: Any) -> list[dict[str, Any]]:
    if rows is None:
        return []
    if isinstance(rows, dict):
        return [rows]
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _find_bitbrowser_processes(container_code: str | int) -> list[dict[str, Any]]:
    if not IS_WINDOWS:
        return []
    code = str(container_code).strip()
    if not code:
        return []
    command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -eq 'BitBrowser.exe' -and $_.CommandLine -like '*"
        + code
        + "*' } | "
        "Select-Object ProcessId,ParentProcessId,CommandLine | ConvertTo-Json -Compress"
    )
    return _normalize_process_rows(_run_powershell_json(command))


def _listening_ports_for_pid(pid: int) -> list[int]:
    try:
        completed = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )
    except Exception:
        return []
    if completed.returncode != 0:
        return []

    ports: set[int] = set()
    pid_text = str(pid)
    for raw_line in (completed.stdout or "").splitlines():
        line = raw_line.strip()
        if not line.startswith("TCP"):
            continue
        parts = line.split()
        if len(parts) < 5 or parts[3].upper() != "LISTENING" or parts[4] != pid_text:
            continue
        try:
            port = int(parts[1].rsplit(":", 1)[1])
        except Exception:
            continue
        ports.add(port)
    return sorted(ports)


def _probe_debug_port(port: int) -> bool:
    try:
        response = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=3)
        if response.status_code != 200:
            return False
        data = response.json()
        return bool(data.get("webSocketDebuggerUrl"))
    except Exception:
        return False


def _recover_existing_debug_port(container_code: str | int) -> int | None:
    for process in _find_bitbrowser_processes(container_code):
        pid = _as_int(process.get("ProcessId"))
        if not pid:
            continue
        for port in _listening_ports_for_pid(pid):
            if _probe_debug_port(port):
                return port
    return None


def _kill_stale_bitbrowser_window(container_code: str | int) -> bool:
    processes = _find_bitbrowser_processes(container_code)
    if not processes:
        return False

    matched_pids = {int(row["ProcessId"]) for row in processes if _as_int(row.get("ProcessId"))}
    root_pids: list[int] = []
    for row in processes:
        pid = _as_int(row.get("ProcessId"))
        parent_pid = _as_int(row.get("ParentProcessId"))
        command_line = str(row.get("CommandLine") or "")
        if not pid:
            continue
        if "--type=" not in command_line:
            root_pids.append(pid)
            continue
        if not parent_pid or parent_pid not in matched_pids:
            root_pids.append(pid)

    killed_any = False
    for pid in sorted(set(root_pids)):
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=20,
                check=False,
            )
            killed_any = True
        except Exception:
            continue
    if killed_any:
        time.sleep(3)
    return killed_any


def _parse_debug_port_from_result(result: Any) -> int | None:
    data = _extract_data(result)
    if not isinstance(data, dict):
        return None
    return (
        _as_int(data.get("debuggingPort"))
        or _as_int(data.get("debug_port"))
        or _as_int(data.get("debugPort"))
        or _extract_port_from_url(data.get("http"))
        or _extract_port_from_url(data.get("ws"))
        or _extract_port_from_url(data.get("websocket"))
    )


def list_browser_envs(upload_config_path: str | Path | None = None) -> list[dict[str, Any]]:
    settings = load_browser_settings(upload_config_path)
    result = _post_json(
        settings["base_url"],
        settings["list_endpoint"],
        settings["list_payload"],
        provider=str(settings.get("provider") or ""),
    )
    if not _is_success(settings["provider"], result):
        raise RuntimeError(_extract_error_message(result))
    if settings["provider"] == "bitbrowser":
        return _normalize_bitbrowser_envs(result)
    return _normalize_hubstudio_envs(result)


def start_browser_debug_port(container_code: str | int, upload_config_path: str | Path | None = None) -> int:
    settings = load_browser_settings(upload_config_path)
    payload = dict(settings["open_payload"])
    payload[settings["open_payload_id_key"]] = container_code

    try:
        result = _post_json(
            settings["base_url"],
            settings["open_endpoint"],
            payload,
            provider=str(settings.get("provider") or ""),
        )
    except requests.RequestException:
        recovered = _recover_existing_debug_port(container_code) if settings["provider"] == "bitbrowser" else None
        if recovered is not None:
            return recovered
        raise

    if not _is_success(settings["provider"], result):
        error_message = _extract_error_message(result)
        if settings["provider"] == "bitbrowser":
            if _should_recover_existing_window(error_message):
                recovered = _recover_existing_debug_port(container_code)
                if recovered is not None:
                    return recovered
            if _should_force_relaunch_window(error_message) and _kill_stale_bitbrowser_window(container_code):
                retry_result = _post_json(
                    settings["base_url"],
                    settings["open_endpoint"],
                    payload,
                    provider=str(settings.get("provider") or ""),
                    attempts=2,
                )
                if _is_success(settings["provider"], retry_result):
                    port = _parse_debug_port_from_result(retry_result)
                    if port is not None:
                        return port
            if _should_try_bitbrowser_recovery(error_message):
                recovered = _recover_existing_debug_port(container_code)
                if recovered is not None:
                    return recovered
        raise RuntimeError(error_message)

    port = _parse_debug_port_from_result(result)
    if port is not None:
        return port

    recovered = _recover_existing_debug_port(container_code) if settings["provider"] == "bitbrowser" else None
    if recovered is not None:
        return recovered
    raise RuntimeError("Unable to resolve debugging port from browser open result")


def stop_browser_container(container_code: str | int, upload_config_path: str | Path | None = None) -> bool:
    settings = load_browser_settings(upload_config_path)
    payload = dict(settings["stop_payload"])
    payload[settings["stop_payload_id_key"]] = container_code

    result = _post_json(
        settings["base_url"],
        settings["stop_endpoint"],
        payload,
        provider=str(settings.get("provider") or ""),
    )
    if not _is_success(settings["provider"], result):
        raise RuntimeError(_extract_error_message(result))
    if settings["provider"] == "bitbrowser":
        # BitBrowser occasionally reports success while the old window tree is
        # still alive. Kill the stale tree so the next open gets a clean port.
        _kill_stale_bitbrowser_window(container_code)
    return True
