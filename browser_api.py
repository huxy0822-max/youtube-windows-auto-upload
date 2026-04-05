#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Browser local API adapter.

This module normalizes HubStudio and BitBrowser browser APIs so the upload
pipeline can use one code path for listing environments, opening a browser
window, and closing it.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from path_helpers import detect_browser_api, resolve_config_file

logger = logging.getLogger(__name__)

# 浏览器默认端口
HUBSTUDIO_DEFAULT_PORT = 6873
BITBROWSER_DEFAULT_PORT = 54345

# 统一 API 请求超时（秒）
API_TIMEOUT = 30

# 运行时 provider 覆盖 — UI 选择后通过 set_runtime_provider() 设置
_RUNTIME_PROVIDER_OVERRIDE: str | None = None


def set_runtime_provider(provider: str | None) -> None:
    """设置运行时浏览器提供者覆盖（由 UI 调用）。

    传入 "auto" 或 None 表示恢复自动检测。
    传入 "hubstudio" 或 "bitbrowser" 强制使用指定提供者。
    """
    global _RUNTIME_PROVIDER_OVERRIDE
    if provider and provider.strip().lower() not in ("", "auto"):
        _RUNTIME_PROVIDER_OVERRIDE = provider.strip().lower()
    else:
        _RUNTIME_PROVIDER_OVERRIDE = None
    logger.info("Runtime browser provider override set to: %s", _RUNTIME_PROVIDER_OVERRIDE or "auto")


def get_runtime_provider() -> str | None:
    """获取当前运行时 provider 覆盖值（None 表示自动检测）。"""
    return _RUNTIME_PROVIDER_OVERRIDE


def probe_browser_providers() -> dict[str, bool]:
    """探测本地运行的浏览器管理器，返回各 provider 是否可达。

    用于 UI 自动检测提示。
    """
    import socket
    results: dict[str, bool] = {}
    for provider_name, port in [("hubstudio", HUBSTUDIO_DEFAULT_PORT), ("bitbrowser", BITBROWSER_DEFAULT_PORT)]:
        alive = False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                alive = True
        except OSError:
            pass
        if alive:
            defaults = DEFAULT_BROWSER_SETTINGS[provider_name]
            url = f"http://127.0.0.1:{port}{defaults['list_endpoint']}"
            try:
                resp = requests.post(url, json=defaults["list_payload"], timeout=15)
                alive = resp.status_code < 500
            except Exception:
                if provider_name == "bitbrowser":
                    alive = _post_json_with_curl(url, defaults["list_payload"], timeout=15) is not None
                else:
                    alive = False
        results[provider_name] = alive
    return results

DEFAULT_BROWSER_SETTINGS = {
    "hubstudio": {
        "base_url": f"http://127.0.0.1:{HUBSTUDIO_DEFAULT_PORT}",
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
        "base_url": f"http://127.0.0.1:{BITBROWSER_DEFAULT_PORT}",
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


def _autodetect_browser_api() -> dict[str, Any] | None:
    try:
        return detect_browser_api()
    except Exception as exc:
        logger.info("Auto-detect browser API failed: %s", exc)
        return None


AUTO_DETECTED_BROWSER_API = _autodetect_browser_api()
if AUTO_DETECTED_BROWSER_API is None:
    logger.info("No active browser local API detected on startup; browser features will wait for manual launch.")


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


def _channel_mapping_path() -> Path:
    env_path = str(os.environ.get("CHANNEL_MAPPING_PATH") or "").strip()
    if env_path:
        return Path(env_path)
    return resolve_config_file(Path(__file__).resolve().parent, "channel_mapping.json")


def _load_channel_mapping_lookup() -> tuple[dict[str, dict[str, Any]], dict[int, dict[str, Any]]]:
    path = _channel_mapping_path()
    if not path.exists():
        return {}, {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}

    raw_channels = payload.get("channels") if isinstance(payload, dict) else {}
    if not isinstance(raw_channels, dict):
        return {}, {}

    by_container: dict[str, dict[str, Any]] = {}
    by_serial: dict[int, dict[str, Any]] = {}
    for container_code, info in raw_channels.items():
        if not isinstance(info, dict):
            continue
        serial = _as_int(info.get("serial_number"))
        if serial is None:
            continue
        entry = {
            "serial": serial,
            "tag": str(info.get("tag") or "").strip(),
            "channel_name": str(info.get("channel_name") or "").strip(),
        }
        clean_container = str(container_code or "").strip()
        if clean_container:
            by_container[clean_container] = entry
        by_serial[serial] = entry
    return by_container, by_serial


def load_browser_settings(upload_config_path: str | Path | None = None) -> dict[str, Any]:
    config = _load_upload_config(upload_config_path)
    browser_cfg = config.get("browser_api", {}) if isinstance(config.get("browser_api"), dict) else {}

    runtime_provider = _RUNTIME_PROVIDER_OVERRIDE or ""
    env_provider = str(os.environ.get("BROWSER_PROVIDER") or "").strip().lower()
    env_base_url = str(os.environ.get("BROWSER_BASE_URL") or "").strip()
    cfg_provider = str(browser_cfg.get("provider") or config.get("browser_provider") or "").strip().lower()
    cfg_base_url = str(browser_cfg.get("base_url") or "").strip()
    cfg_port = browser_cfg.get("port") or _extract_port_from_url(cfg_base_url)

    detected = None
    if not env_base_url and not cfg_base_url and not cfg_port:
        detected = AUTO_DETECTED_BROWSER_API or _autodetect_browser_api()

    # 优先级: runtime UI 选择 > 环境变量 > 配置文件 > 自动检测 > 默认hubstudio
    provider = runtime_provider or env_provider or cfg_provider or str((detected or {}).get("provider") or "").lower() or "hubstudio"
    defaults = DEFAULT_BROWSER_SETTINGS.get(provider, DEFAULT_BROWSER_SETTINGS["hubstudio"]).copy()
    settings = defaults.copy()
    settings["provider"] = provider

    use_browser_cfg = (not cfg_provider) or (cfg_provider == provider)
    if use_browser_cfg:
        settings["base_url"] = cfg_base_url or defaults["base_url"]
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

    if detected and not env_base_url and not cfg_base_url and provider == str(detected.get("provider") or "").lower():
        settings["base_url"] = str(detected.get("base_url") or defaults["base_url"]).strip() or defaults["base_url"]
    elif env_base_url:
        settings["base_url"] = env_base_url
    elif not cfg_base_url and detected and not cfg_provider:
        detected_provider = str(detected.get("provider") or "").lower()
        if detected_provider in DEFAULT_BROWSER_SETTINGS:
            provider = detected_provider
            defaults = DEFAULT_BROWSER_SETTINGS[provider].copy()
            settings = defaults.copy()
            settings["provider"] = provider
            settings["base_url"] = str(detected.get("base_url") or defaults["base_url"]).strip() or defaults["base_url"]

    if not env_base_url and not cfg_base_url and not detected:
        logger.info("No browser API port responded; continuing with provider=%s default settings.", provider)
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
    """BitBrowser 在部分 macOS 环境对 requests 不稳定，curl 更稳。"""
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
    try:
        status_code = int(status_text.strip())
    except Exception:
        return None
    if status_code >= 500:
        return None
    try:
        return json.loads(body or "{}")
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

    by_container, by_serial = _load_channel_mapping_lookup()
    normalized: list[dict[str, Any]] = []
    for item in items:
        container_code = str(
            item.get("id")
            or item.get("browserId")
            or item.get("containerCode")
            or ""
        ).strip()
        mapped = by_container.get(container_code) or {}
        serial = (
            _as_int(mapped.get("serial"))
            or _as_int(item.get("serialNumber"))
            or _as_int(item.get("browserSeq"))
            or _as_int(item.get("sortNum"))
            or _as_int(item.get("seq"))
        )
        fallback_entry = by_serial.get(int(serial), {}) if serial is not None else {}
        normalized.append(
            {
                "serialNumber": serial,
                "containerCode": container_code,
                "name": item.get("name") or item.get("browserName") or mapped.get("channel_name") or fallback_entry.get("channel_name") or "",
                "tag": mapped.get("tag") or item.get("groupName") or item.get("tag") or fallback_entry.get("tag") or "",
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
    except Exception as e:
        logger.warning(f"PowerShell 执行失败: {e}")
        return None
    if completed.returncode != 0:
        return None
    stdout = (completed.stdout or "").strip()
    if not stdout:
        return None
    try:
        return json.loads(stdout)
    except Exception as e:
        logger.warning(f"PowerShell 输出 JSON 解析失败: {e}")
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
    code = str(container_code).strip()
    if not code:
        return []
    if IS_WINDOWS:
        command = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.Name -eq 'BitBrowser.exe' -and $_.CommandLine -like '*"
            + code
            + "*' } | "
            "Select-Object ProcessId,ParentProcessId,CommandLine | ConvertTo-Json -Compress"
        )
        return _normalize_process_rows(_run_powershell_json(command))
    # macOS / Linux: 用 ps 查找进程
    try:
        completed = subprocess.run(
            ["ps", "-eo", "pid,ppid,command"],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except Exception:
        return []
    results: list[dict[str, Any]] = []
    for line in (completed.stdout or "").splitlines():
        if code not in line:
            continue
        # 匹配 BitBrowser 或 chromium 类进程
        lower_line = line.lower()
        if "bitbrowser" not in lower_line and "chromium" not in lower_line and "chrome" not in lower_line:
            continue
        parts = line.split(None, 2)
        if len(parts) >= 3:
            results.append({
                "ProcessId": int(parts[0]),
                "ParentProcessId": int(parts[1]),
                "CommandLine": parts[2],
            })
    return results


def _listening_ports_for_pid(pid: int) -> list[int]:
    if IS_WINDOWS:
        return _listening_ports_for_pid_windows(pid)
    return _listening_ports_for_pid_unix(pid)


def _listening_ports_for_pid_windows(pid: int) -> list[int]:
    try:
        completed = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=15, check=False,
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
        except (ValueError, IndexError) as e:
            logger.debug(f"端口解析跳过: {line.strip()} | {e}")
            continue
        ports.add(port)
    return sorted(ports)


def _listening_ports_for_pid_unix(pid: int) -> list[int]:
    """macOS / Linux: 用 lsof 查找进程监听的端口"""
    try:
        completed = subprocess.run(
            ["lsof", "-nP", "-iTCP", "-sTCP:LISTEN", "-a", "-p", str(pid)],
            capture_output=True, text=True, timeout=15, check=False,
        )
    except Exception:
        return []
    ports: set[int] = set()
    for line in (completed.stdout or "").splitlines():
        # lsof 输出格式: COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME
        # NAME 列类似: *:9222 (LISTEN) 或 127.0.0.1:9222
        parts = line.split()
        if len(parts) < 9:
            continue
        name_col = parts[8]
        try:
            port = int(name_col.rsplit(":", 1)[1])
            ports.add(port)
        except (ValueError, IndexError):
            continue
    return sorted(ports)


def _probe_debug_port(port: int) -> bool:
    try:
        response = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=API_TIMEOUT)
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
            if IS_WINDOWS:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    capture_output=True, text=True, encoding="utf-8",
                    errors="replace", timeout=20, check=False,
                )
            else:
                # macOS / Linux: kill -9 进程树
                import signal
                os.kill(pid, signal.SIGKILL)
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
        provider=settings["provider"],
    )
    if not _is_success(settings["provider"], result):
        raise RuntimeError(_extract_error_message(result))
    if settings["provider"] == "bitbrowser":
        return _normalize_bitbrowser_envs(result)
    return _normalize_hubstudio_envs(result)


def list_browser_envs_for_provider(provider: str) -> list[dict[str, Any]]:
    """列出指定 provider 的浏览器环境，每个 env 带 _provider 字段。"""
    provider = provider.strip().lower()
    if provider not in DEFAULT_BROWSER_SETTINGS:
        raise ValueError(f"Unknown provider: {provider}")
    defaults = DEFAULT_BROWSER_SETTINGS[provider]
    base_url = defaults["base_url"]
    try:
        result = _post_json(
            base_url,
            defaults["list_endpoint"],
            defaults["list_payload"],
            provider=provider,
        )
    except Exception as exc:
        logger.info("list_browser_envs_for_provider(%s) failed: %s", provider, exc)
        return []
    if not _is_success(provider, result):
        logger.info("list_browser_envs_for_provider(%s) not success: %s", provider, _extract_error_message(result))
        return []
    envs = _normalize_bitbrowser_envs(result) if provider == "bitbrowser" else _normalize_hubstudio_envs(result)
    for env in envs:
        env["_provider"] = provider
    return envs


def list_all_browser_envs() -> list[dict[str, Any]]:
    """合并列出所有可达 provider 的浏览器环境。

    每个 env 都带 _provider 字段标记来源。
    """
    all_envs: list[dict[str, Any]] = []
    for provider_name in DEFAULT_BROWSER_SETTINGS:
        try:
            envs = list_browser_envs_for_provider(provider_name)
            all_envs.extend(envs)
        except Exception as exc:
            logger.info("list_all_browser_envs: %s failed: %s", provider_name, exc)
    return all_envs


def _settings_for_provider(provider: str | None, upload_config_path: str | Path | None = None) -> dict[str, Any]:
    """获取指定 provider 的设置。provider 为 None/"auto" 时走默认逻辑。"""
    if provider and provider.strip().lower() not in ("", "auto"):
        forced = provider.strip().lower()
        if forced in DEFAULT_BROWSER_SETTINGS:
            settings = DEFAULT_BROWSER_SETTINGS[forced].copy()
            settings["provider"] = forced
            return settings
    return load_browser_settings(upload_config_path)


def start_browser_debug_port(
    container_code: str | int,
    upload_config_path: str | Path | None = None,
    *,
    provider: str | None = None,
) -> int:
    settings = _settings_for_provider(provider, upload_config_path)
    payload = dict(settings["open_payload"])
    payload[settings["open_payload_id_key"]] = container_code

    try:
        result = _post_json(
            settings["base_url"],
            settings["open_endpoint"],
            payload,
            provider=settings["provider"],
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
                    provider=settings["provider"],
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


def stop_browser_container(
    container_code: str | int,
    upload_config_path: str | Path | None = None,
    *,
    provider: str | None = None,
) -> bool:
    settings = _settings_for_provider(provider, upload_config_path)
    payload = dict(settings["stop_payload"])
    payload[settings["stop_payload_id_key"]] = container_code

    result = _post_json(
        settings["base_url"],
        settings["stop_endpoint"],
        payload,
        provider=settings["provider"],
    )
    if not _is_success(settings["provider"], result):
        raise RuntimeError(_extract_error_message(result))
    if settings["provider"] == "bitbrowser":
        # BitBrowser occasionally reports success while the old window tree is
        # still alive. Kill the stale tree so the next open gets a clean port.
        _kill_stale_bitbrowser_window(container_code)
    return True


def _resolve_browser_identifier(
    identifier: str | int,
    upload_config_path: str | Path | None = None,
    *,
    provider: str | None = None,
) -> dict[str, Any]:
    clean_identifier = str(identifier or "").strip()
    if not clean_identifier:
        raise ValueError("Browser identifier cannot be empty.")

    # 如果指定了 provider，只在该 provider 中查找
    if provider and provider.strip().lower() not in ("", "auto"):
        envs = list_browser_envs_for_provider(provider.strip().lower())
    else:
        envs = list_browser_envs(upload_config_path)
    for env in envs:
        serial = str(env.get("serialNumber") or "").strip()
        container_code = str(env.get("containerCode") or "").strip()
        name = str(env.get("name") or "").strip()
        if clean_identifier in {serial, container_code, name}:
            return dict(env)
    raise RuntimeError(f"Browser env not found: {clean_identifier}")


def open_browser_env(
    identifier: str | int,
    upload_config_path: str | Path | None = None,
    *,
    provider: str | None = None,
) -> dict[str, Any]:
    env = _resolve_browser_identifier(identifier, upload_config_path, provider=provider)
    container_code = str(env.get("containerCode") or "").strip()
    env_provider = provider or env.get("_provider")
    attempts = 3
    last_error: Exception | None = None
    port = 0
    for attempt in range(1, attempts + 1):
        try:
            port = start_browser_debug_port(container_code, upload_config_path, provider=env_provider)
            break
        except RuntimeError as exc:
            last_error = exc
            message = str(exc)
            if "正在关闭中" not in message or attempt >= attempts:
                raise
            time.sleep(2.0 * attempt)
    if not port and last_error is not None:
        raise last_error
    return {
        "success": True,
        "msg": "ok",
        "data": {
            "id": container_code,
            "seq": int(env.get("serialNumber") or 0),
            "name": str(env.get("name") or ""),
            "http": str(port),
            "debug_port": int(port),
        },
    }


def close_browser_env(
    identifier: str | int,
    upload_config_path: str | Path | None = None,
    *,
    provider: str | None = None,
) -> dict[str, Any]:
    env = _resolve_browser_identifier(identifier, upload_config_path, provider=provider)
    container_code = str(env.get("containerCode") or "").strip()
    env_provider = provider or env.get("_provider")
    attempts = 3
    last_error: Exception | None = None
    closed = False
    for attempt in range(1, attempts + 1):
        try:
            closed = stop_browser_container(container_code, upload_config_path, provider=env_provider)
            break
        except RuntimeError as exc:
            last_error = exc
            message = str(exc)
            if "正在打开中" not in message or attempt >= attempts:
                raise
            time.sleep(2.0 * attempt)
    if not closed and last_error is not None:
        raise last_error
    return {
        "success": bool(closed),
        "msg": "ok" if closed else "failed",
        "data": {
            "id": container_code,
            "seq": int(env.get("serialNumber") or 0),
            "name": str(env.get("name") or ""),
        },
    }
