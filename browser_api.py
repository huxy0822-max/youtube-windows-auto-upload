#!/usr/bin/env python3
"""
浏览器本地 API 适配层。

目标：
- 让上传侧代码不再把 HubStudio 写死。
- 通过 `config/upload_config.json` 切换 `hubstudio` / `bitbrowser`。
- 对外仍然返回统一字段：`serialNumber`、`containerCode`。

说明：
- HubStudio 的字段映射是确定的。
- BitBrowser 的字段映射基于其本地 API 常见返回结构做了兼容化处理，
  如果你的版本字段不同，可以直接在 `config/upload_config.json` 里覆盖接口配置。
"""

from __future__ import annotations

import json
import os
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


def _config_path(upload_config_path: str | Path | None) -> Path:
    if upload_config_path:
        return Path(upload_config_path)
    base_dir = Path(__file__).resolve().parent
    env_path = os.environ.get("UPLOAD_CONFIG_PATH")
    if env_path:
        return Path(env_path)
    return resolve_config_file(base_dir, "upload_config.json")


def _load_upload_config(upload_config_path: str | Path | None = None) -> dict:
    path = _config_path(upload_config_path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_browser_settings(upload_config_path: str | Path | None = None) -> dict:
    config = _load_upload_config(upload_config_path)
    browser_cfg = config.get("browser_api", {})

    cfg_provider = (
        browser_cfg.get("provider")
        or config.get("browser_provider")
        or ""
    ).lower()

    provider = (
        os.environ.get("BROWSER_PROVIDER")
        or browser_cfg.get("provider")
        or config.get("browser_provider")
        or "hubstudio"
    ).lower()

    defaults = DEFAULT_BROWSER_SETTINGS.get(provider, DEFAULT_BROWSER_SETTINGS["hubstudio"]).copy()
    settings = defaults.copy()
    settings["provider"] = provider

    # 如果通过环境变量强制切 provider，而 config 的 browser_api 是另一个 provider，
    # 则优先使用该 provider 的默认接口，避免 endpoint/base_url 串台。
    use_browser_cfg = (not cfg_provider) or (cfg_provider == provider)

    if use_browser_cfg:
        settings["base_url"] = browser_cfg.get("base_url", defaults["base_url"])
        settings["list_endpoint"] = browser_cfg.get("list_endpoint", defaults["list_endpoint"])
        settings["open_endpoint"] = browser_cfg.get("open_endpoint", defaults["open_endpoint"])
        settings["stop_endpoint"] = browser_cfg.get("stop_endpoint", defaults["stop_endpoint"])
        settings["list_payload"] = browser_cfg.get("list_payload", defaults["list_payload"])
        settings["open_payload"] = browser_cfg.get("open_payload", defaults["open_payload"])
        settings["stop_payload"] = browser_cfg.get("stop_payload", defaults["stop_payload"])
        settings["open_payload_id_key"] = browser_cfg.get("open_payload_id_key", defaults["open_payload_id_key"])
        settings["stop_payload_id_key"] = browser_cfg.get("stop_payload_id_key", defaults["stop_payload_id_key"])
    return settings


def _post_json(base_url: str, endpoint: str, payload: dict) -> dict:
    url = f"{base_url.rstrip('/')}{endpoint}"
    response = requests.post(url, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def _extract_error_message(result: Any) -> str:
    if isinstance(result, dict):
        return str(result.get("msg") or result.get("message") or result.get("error") or "未知错误")
    return "未知错误"


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


def _normalize_hubstudio_envs(result: dict) -> list[dict]:
    items = _extract_data(result)
    if isinstance(items, dict):
        items = items.get("list", [])
    if not isinstance(items, list):
        return []

    normalized = []
    for item in items:
        normalized.append({
            "serialNumber": _as_int(item.get("serialNumber")),
            "containerCode": str(item.get("containerCode", "")),
            "name": item.get("name") or item.get("envName") or "",
            "tag": item.get("tagName") or item.get("tag") or item.get("groupName") or "",
            "remark": item.get("remark") or "",
            "_raw": item,
        })
    return normalized


def _normalize_bitbrowser_envs(result: dict) -> list[dict]:
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

    normalized = []
    for item in items:
        normalized.append({
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
        })
    return normalized


def list_browser_envs(upload_config_path: str | Path | None = None) -> list[dict]:
    settings = load_browser_settings(upload_config_path)
    result = _post_json(settings["base_url"], settings["list_endpoint"], settings["list_payload"])
    if not _is_success(settings["provider"], result):
        raise RuntimeError(_extract_error_message(result))

    if settings["provider"] == "bitbrowser":
        return _normalize_bitbrowser_envs(result)
    return _normalize_hubstudio_envs(result)


def _extract_port_from_url(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return urlparse(value).port
    except Exception:
        return None


def start_browser_debug_port(container_code: str | int, upload_config_path: str | Path | None = None) -> int:
    settings = load_browser_settings(upload_config_path)
    payload = dict(settings["open_payload"])
    payload[settings["open_payload_id_key"]] = container_code

    result = _post_json(settings["base_url"], settings["open_endpoint"], payload)
    if not _is_success(settings["provider"], result):
        raise RuntimeError(_extract_error_message(result))

    data = _extract_data(result)
    if not isinstance(data, dict):
        raise RuntimeError("浏览器启动接口返回格式不正确")

    port = (
        _as_int(data.get("debuggingPort"))
        or _as_int(data.get("debug_port"))
        or _as_int(data.get("debugPort"))
        or _extract_port_from_url(data.get("http"))
        or _extract_port_from_url(data.get("ws"))
        or _extract_port_from_url(data.get("websocket"))
    )
    if port is None:
        raise RuntimeError("未从启动结果中解析到调试端口")
    return port


def stop_browser_container(container_code: str | int, upload_config_path: str | Path | None = None) -> bool:
    """关闭浏览器容器，成功返回 True。"""
    settings = load_browser_settings(upload_config_path)
    payload = dict(settings["stop_payload"])
    payload[settings["stop_payload_id_key"]] = container_code

    result = _post_json(settings["base_url"], settings["stop_endpoint"], payload)
    if not _is_success(settings["provider"], result):
        raise RuntimeError(_extract_error_message(result))
    return True
