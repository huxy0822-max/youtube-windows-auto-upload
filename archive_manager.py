# -*- coding: utf-8 -*-
from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return max(0, int(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _resolve_optional_path(value: object) -> Path | None:
    text = str(value or "").strip()
    return Path(text) if text else None


def _dedupe_destination(target: Path) -> Path:
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        candidate = target.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


class ArchiveManager:
    def __init__(self, path_template: dict):
        self.used_materials_dir = _resolve_optional_path(path_template.get("used_materials_dir"))
        self.used_videos_dir = _resolve_optional_path(path_template.get("used_videos_dir"))
        self.auto_delete_days = _safe_int(path_template.get("auto_delete_days"), default=0)

    def archive_materials(self, source_dir: str, files_used: Iterable[str | Path]) -> list[str]:
        if self.used_materials_dir is None:
            return []

        moved: list[str] = []
        self.used_materials_dir.mkdir(parents=True, exist_ok=True)
        date_dir = self.used_materials_dir / datetime.now().strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        seen: set[str] = set()
        for raw_path in files_used:
            source_path = Path(str(raw_path)).expanduser()
            key = str(source_path.resolve(strict=False)).lower()
            if key in seen or not source_path.exists():
                continue
            seen.add(key)
            destination = _dedupe_destination(date_dir / source_path.name)
            try:
                shutil.move(str(source_path), str(destination))
                moved.append(str(destination))
            except (OSError, shutil.Error) as exc:
                print(f"[archive] 移动文件失败 {source_path} → {destination}: {exc}")
        return moved

    def archive_video(self, video_path: str | Path) -> str:
        if self.used_videos_dir is None:
            return ""

        self.used_videos_dir.mkdir(parents=True, exist_ok=True)
        date_dir = self.used_videos_dir / datetime.now().strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        source_path = Path(str(video_path)).expanduser()
        if not source_path.exists():
            return ""

        destination = _dedupe_destination(date_dir / source_path.name)
        shutil.move(str(source_path), str(destination))
        return str(destination)

    def cleanup_old_videos(self) -> int:
        if self.auto_delete_days <= 0 or self.used_videos_dir is None or not self.used_videos_dir.exists():
            return 0

        cutoff = datetime.now() - timedelta(days=self.auto_delete_days)
        deleted = 0
        for date_dir in self.used_videos_dir.iterdir():
            if not date_dir.is_dir():
                continue
            try:
                dir_date = datetime.strptime(date_dir.name, "%Y-%m-%d")
            except ValueError:
                continue
            if dir_date < cutoff:
                shutil.rmtree(str(date_dir))
                deleted += 1
        return deleted
