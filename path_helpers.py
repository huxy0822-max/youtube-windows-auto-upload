#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓库内路径解析与环境检测辅助函数。
"""

from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable
from urllib import error, request

logger = logging.getLogger(__name__)

BITBROWSER_DEFAULT_PORT = 54345
HUBSTUDIO_DEFAULT_PORT = 6873
HTTP_PROBE_TIMEOUT = 0.8


def normalize_path(value: str | Path | None, base_dir: str | Path) -> Path:
    """把相对路径规范化为基于仓库目录的绝对路径。"""
    base = Path(base_dir).resolve(strict=False)
    if value is None or str(value).strip() == "":
        return base

    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base / path
    return path.resolve(strict=False)


def first_existing(candidates: Iterable[Path]) -> Path | None:
    """返回第一个存在的候选路径。"""
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def resolve_config_file(base_dir: str | Path, filename: str) -> Path:
    """
    查找配置文件。

    优先级：
    1. 当前仓库 `config/`
    2. 当前仓库父目录 `config/`
    3. 当前仓库内 `上传视频自动化/config/`
    4. 父目录下的 `上传视频自动化/config/`
    """
    base = Path(base_dir).resolve(strict=False)
    candidates = [
        base / "config" / filename,
        base.parent / "config" / filename,
        base / "上传视频自动化" / "config" / filename,
        base.parent / "上传视频自动化" / "config" / filename,
    ]
    found = first_existing(candidates)
    if found:
        return found
    default = candidates[0]
    logger.info("配置文件 %s 未找到，使用默认路径: %s", filename, default)
    return default


def resolve_upload_script(base_dir: str | Path) -> Path:
    """查找外部上传脚本；找不到时返回当前仓库下的默认占位路径。"""
    base = Path(base_dir).resolve(strict=False)
    candidates = [
        base / "batch_upload.py",
        base / "scripts" / "batch_upload.py",
        base / "上传视频自动化" / "scripts" / "batch_upload.py",
        base.parent / "上传视频自动化" / "scripts" / "batch_upload.py",
    ]
    return first_existing(candidates) or candidates[0]


def companion_local_config(path_value: str | Path) -> Path:
    path = Path(path_value)
    return path.with_name(f"{path.stem}.local{path.suffix}")


def _read_json_file(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def merge_dict_overlay(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged = dict(base)
        for key, value in override.items():
            if key in merged:
                merged[key] = merge_dict_overlay(merged[key], value)
            else:
                merged[key] = value
        return merged
    return override if override is not None else base


def load_json_with_local_override(path_value: str | Path, fallback: Any) -> Any:
    path = Path(path_value)
    base_data = _read_json_file(path, fallback)
    local_data = _read_json_file(companion_local_config(path), None)
    if local_data is None:
        return base_data
    return merge_dict_overlay(base_data, local_data)


def default_scheduler_config(base_dir: str | Path) -> dict[str, Any]:
    """生成适合当前仓库的默认调度配置。"""
    base = Path(base_dir).resolve(strict=False)
    output_root = (base / "workspace" / "AutoTask").resolve(strict=False)
    metadata_root = (base / "workspace" / "metadata").resolve(strict=False)
    ffmpeg_bin = "ffmpeg"
    if sys.platform == "darwin":
        for candidate in ("/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
            if Path(candidate).exists():
                ffmpeg_bin = candidate
                break
    return {
        "music_dir": str((base / "workspace" / "music").resolve(strict=False)),
        "base_image_dir": str((base / "workspace" / "base_image").resolve(strict=False)),
        "metadata_root": str(metadata_root),
        "output_root": str(output_root),
        "upload_config": str(resolve_config_file(base, "upload_config.json")),
        "ffmpeg_bin": ffmpeg_bin,
        "ffmpeg_path": ffmpeg_bin,
        "used_media_root": str((output_root / "_used_media").resolve(strict=False)),
        "render_cleanup_days": 0,
        "group_source_bindings": {},
    }


def normalize_scheduler_config(raw_cfg: dict | None, base_dir: str | Path) -> dict[str, Any]:
    """把调度配置中的目录项都规范成绝对路径。"""
    cfg = default_scheduler_config(base_dir)
    if raw_cfg:
        cfg.update(raw_cfg)
    if not str(cfg.get("metadata_root") or "").strip():
        cfg["metadata_root"] = cfg.get("base_image_dir") or cfg["metadata_root"]

    base = Path(base_dir).resolve(strict=False)
    for key in ("music_dir", "base_image_dir", "metadata_root", "output_root", "upload_config", "used_media_root"):
        cfg[key] = str(normalize_path(cfg.get(key), base))

    ffmpeg_bin = cfg.get("ffmpeg_bin") or cfg.get("ffmpeg_path") or "ffmpeg"
    if ffmpeg_bin not in ("ffmpeg", "ffmpeg.exe"):
        ffmpeg_bin = str(normalize_path(ffmpeg_bin, base))
    cfg["ffmpeg_bin"] = ffmpeg_bin
    cfg["ffmpeg_path"] = ffmpeg_bin
    try:
        cfg["render_cleanup_days"] = max(0, int(cfg.get("render_cleanup_days", 0)))
    except Exception:
        cfg["render_cleanup_days"] = 0

    raw_bindings = cfg.get("group_source_bindings") or {}
    normalized_bindings: dict[str, str] = {}
    if isinstance(raw_bindings, dict):
        for tag, value in raw_bindings.items():
            text = str(value or "").strip()
            if not text:
                continue
            normalized_bindings[str(tag)] = str(normalize_path(text, base))
    cfg["group_source_bindings"] = normalized_bindings
    return cfg


def _amf_dll_candidates() -> list[Path]:
    system_root = Path(os.environ.get("SystemRoot") or r"C:\Windows")
    program_files = Path(os.environ.get("ProgramFiles") or r"C:\Program Files")
    program_files_x86 = Path(os.environ.get("ProgramFiles(x86)") or r"C:\Program Files (x86)")
    return [
        system_root / "System32" / "amfrt64.dll",
        system_root / "SysWOW64" / "amfrt64.dll",
        program_files / "AMD" / "CNext" / "CNext" / "amfrt64.dll",
        program_files_x86 / "AMD" / "CNext" / "CNext" / "amfrt64.dll",
    ]


def detect_gpu() -> str:
    """检测当前机器更适合的硬件编码后端。"""
    if sys.platform == "darwin":
        return "videotoolbox"
    if sys.platform == "win32":
        if shutil.which("nvidia-smi"):
            return "nvenc"
        if first_existing(_amf_dll_candidates()):
            return "amf"
    return "cpu"


def _is_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=HTTP_PROBE_TIMEOUT):
            return True
    except OSError:
        return False


def _probe_http_endpoint(url: str) -> bool:
    try:
        with request.urlopen(request.Request(url, method="GET"), timeout=HTTP_PROBE_TIMEOUT):
            return True
    except error.HTTPError as exc:
        return exc.code in {200, 400, 401, 403, 404, 405}
    except Exception:
        return False


def detect_browser_api() -> dict[str, Any] | None:
    """检测 BitBrowser / HubStudio 本地 API。"""
    probes = [
        (
            "bitbrowser",
            BITBROWSER_DEFAULT_PORT,
            [
                f"http://127.0.0.1:{BITBROWSER_DEFAULT_PORT}/browser/list",
                f"http://127.0.0.1:{BITBROWSER_DEFAULT_PORT}/api/browser/list",
                f"http://127.0.0.1:{BITBROWSER_DEFAULT_PORT}",
            ],
        ),
        (
            "hubstudio",
            HUBSTUDIO_DEFAULT_PORT,
            [
                f"http://127.0.0.1:{HUBSTUDIO_DEFAULT_PORT}/api/v1/env/list",
                f"http://127.0.0.1:{HUBSTUDIO_DEFAULT_PORT}",
            ],
        ),
    ]
    for provider, port, urls in probes:
        if not _is_port_open(port):
            continue
        if any(_probe_http_endpoint(url) for url in urls):
            return {
                "provider": provider,
                "port": port,
                "base_url": f"http://127.0.0.1:{port}",
            }
    return None


def generate_default_config(result: dict[str, Any], config_path: Path) -> dict[str, Any]:
    """根据检测结果生成默认 scheduler_config.json。"""
    config = default_scheduler_config(config_path.parent)
    ffmpeg_path = str(result.get("ffmpeg") or "").strip()
    if ffmpeg_path:
        config["ffmpeg_bin"] = ffmpeg_path
        config["ffmpeg_path"] = ffmpeg_path
    config["gpu"] = str(result.get("gpu") or "cpu")
    browser_api = result.get("browser_api") if isinstance(result.get("browser_api"), dict) else None
    if browser_api:
        config["browser_api"] = {
            "provider": str(browser_api.get("provider") or "").strip(),
            "base_url": str(browser_api.get("base_url") or "").strip(),
            "port": int(browser_api.get("port") or 0),
        }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return config


def ensure_environment() -> dict[str, Any]:
    """首次运行时自动检测环境并生成默认配置。"""
    result: dict[str, Any] = {
        "platform": sys.platform,
        "ffmpeg": shutil.which("ffmpeg"),
        "gpu": detect_gpu(),
        "browser_api": detect_browser_api(),
    }

    config_path = Path(__file__).resolve().parent / "scheduler_config.json"
    if not config_path.exists():
        generate_default_config(result, config_path)

    return result


def open_path_in_file_manager(path_value: str | Path) -> None:
    path = Path(path_value).expanduser().resolve(strict=False)
    system = platform.system()
    if system == "Windows":
        os.startfile(str(path))
        return
    if system == "Darwin":
        subprocess.run(["open", str(path)], check=False)
        return
    subprocess.run(["xdg-open", str(path)], check=False)
