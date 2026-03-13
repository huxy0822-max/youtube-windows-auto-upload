#!/usr/bin/env python3
"""
仓库内路径解析辅助函数。

这套项目原本夹杂了“当前仓库内路径”和“外部兄弟目录路径”两种假设，
在 Windows 上尤其容易失效。这里统一做三件事：
1. 相对路径一律按当前仓库根目录解析。
2. 配置文件优先从当前仓库 `config/` 查找，再兼容旧目录结构。
3. 外部上传脚本路径做多候选查找，避免写死 macOS 绝对路径。
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


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
    return first_existing(candidates) or candidates[0]


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


def default_scheduler_config(base_dir: str | Path) -> dict:
    """生成适合当前仓库的默认调度配置。"""
    base = Path(base_dir).resolve(strict=False)
    output_root = (base / "workspace" / "AutoTask").resolve(strict=False)
    return {
        "music_dir": str((base / "workspace" / "music").resolve(strict=False)),
        "base_image_dir": str((base / "workspace" / "base_image").resolve(strict=False)),
        "output_root": str(output_root),
        "upload_config": str(resolve_config_file(base, "upload_config.json")),
        "ffmpeg_bin": "ffmpeg",
        "ffmpeg_path": "ffmpeg",
        "used_media_root": str((output_root / "_used_media").resolve(strict=False)),
        "render_cleanup_days": 5,
        "group_source_bindings": {},
    }


def normalize_scheduler_config(raw_cfg: dict | None, base_dir: str | Path) -> dict:
    """把调度配置中的目录项都规范成绝对路径。"""
    cfg = default_scheduler_config(base_dir)
    if raw_cfg:
        cfg.update(raw_cfg)

    base = Path(base_dir).resolve(strict=False)
    for key in ("music_dir", "base_image_dir", "output_root", "upload_config", "used_media_root"):
        cfg[key] = str(normalize_path(cfg.get(key), base))

    ffmpeg_bin = cfg.get("ffmpeg_bin") or cfg.get("ffmpeg_path") or "ffmpeg"
    if ffmpeg_bin not in ("ffmpeg", "ffmpeg.exe"):
        ffmpeg_bin = str(normalize_path(ffmpeg_bin, base))
    cfg["ffmpeg_bin"] = ffmpeg_bin
    cfg["ffmpeg_path"] = ffmpeg_bin
    try:
        cfg["render_cleanup_days"] = max(0, int(cfg.get("render_cleanup_days", 5)))
    except Exception:
        cfg["render_cleanup_days"] = 5

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
