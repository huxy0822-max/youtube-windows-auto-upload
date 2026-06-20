# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from prompt_studio import normalize_tag_key

logger = logging.getLogger(__name__)

# 缓存锁，保证线程安全
_CACHE_LOCK = threading.RLock()

# 元数据行数上限
MAX_METADATA_ROWS = 20000

SCRIPT_DIR = Path(__file__).parent
UPLOAD_RECORDS_ROOT = SCRIPT_DIR / "upload_records"
LEGACY_METADATA_HISTORY_PATH = SCRIPT_DIR / "data" / "metadata_history.json"
_UPLOAD_HISTORY_SYNC_CACHE: dict[str, float] = {}

LogFunc = Callable[[str], None]


def _noop_log(_message: str) -> None:
    return


class BatchDedup:
    """同批次文案去重 — 仅在内存中，不做历史持久化"""

    def __init__(self) -> None:
        self.used_titles: set[str] = set()
        self.used_descriptions: set[str] = set()

    def is_duplicate(self, title: str, description: str) -> bool:
        return title in self.used_titles or description in self.used_descriptions

    def record(self, title: str, description: str) -> None:
        self.used_titles.add(title)
        self.used_descriptions.add(description)

    def reset(self) -> None:
        self.used_titles.clear()
        self.used_descriptions.clear()


def _read_json(path: Path, fallback: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return fallback
    except json.JSONDecodeError as e:
        logger.warning(f"JSON 解析失败 {path}: {e}")
        return fallback
    except Exception as e:
        logger.warning(f"读取文件失败 {path}: {e}")
        return fallback


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _metadata_key(tag: str) -> str:
    normalized = normalize_tag_key(tag)
    return normalized or str(tag or "").strip()


def _normalize_text_key(value: Any) -> str:
    return "".join(str(value or "").strip().lower().split())


def _thumbnail_prompt_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if str(value or "").strip():
        return [str(value).strip()]
    return []


def get_used_metadata_root(config: dict[str, Any] | None = None) -> Path:
    cfg = config or {}
    raw_root = str(cfg.get("used_media_root") or "").strip()
    if raw_root:
        return Path(raw_root) / "metadata"
    return SCRIPT_DIR / "workspace" / "AutoTask" / "_used_media" / "metadata"


def get_used_metadata_history_path(config: dict[str, Any] | None = None) -> Path:
    return get_used_metadata_root(config) / "used_metadata_history.json"


def load_used_metadata_history(config: dict[str, Any] | None = None) -> dict[str, Any]:
    path = get_used_metadata_history_path(config)
    raw = _read_json(path, {"tags": {}})
    if not isinstance(raw, dict):
        return {"tags": {}}
    tags = raw.get("tags")
    if not isinstance(tags, dict):
        raw["tags"] = {}
    return raw


def _iter_metadata_rows(data: dict[str, Any], *, tag: str | None = None) -> list[dict[str, Any]]:
    tags = data.get("tags", {})
    if not isinstance(tags, dict):
        return []
    if tag:
        rows = tags.get(_metadata_key(tag), [])
        return [item for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []

    merged: list[dict[str, Any]] = []
    for rows in tags.values():
        if not isinstance(rows, list):
            continue
        merged.extend(item for item in rows if isinstance(item, dict))
    return merged


def _iter_legacy_metadata_rows(tag: str | None = None) -> list[dict[str, Any]]:
    raw = _read_json(LEGACY_METADATA_HISTORY_PATH, {"tags": {}})
    if not isinstance(raw, dict):
        return []
    tags = raw.get("tags", {})
    if not isinstance(tags, dict):
        return []

    wanted_tag = _metadata_key(tag) if tag else ""
    rows: list[dict[str, Any]] = []
    for raw_tag, items in tags.items():
        if wanted_tag and _metadata_key(raw_tag) != wanted_tag:
            continue
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            description = str(item.get("description") or "").strip()
            tag_signature = str(item.get("tag_signature") or "").strip()
            thumbnail_prompts = _thumbnail_prompt_values(item.get("thumbnail_prompts"))
            if not any([title, description, tag_signature, thumbnail_prompts]):
                continue
            rows.append(
                {
                    "tag": str(raw_tag or "").strip(),
                    "serial": None,
                    "date_mmdd": "",
                    "title": title,
                    "description": description,
                    "tag_signature": tag_signature,
                    "thumbnail_prompts": thumbnail_prompts,
                    "thumbnails": [],
                    "source": "legacy_history",
                    "saved_at": str(item.get("saved_at") or "").strip(),
                }
            )
    return rows


def get_used_metadata_scope(
    tag: str,
    *,
    config: dict[str, Any] | None = None,
    limit: int | None = None,
    global_scope: bool = True,
) -> dict[str, list[str]]:
    sync_uploaded_history_into_used_metadata(
        config=config,
        tag=None if global_scope else tag,
        min_interval_seconds=0.0,
    )
    data = load_used_metadata_history(config)
    rows = _merge_metadata_rows_with_upload_records(
        _iter_metadata_rows(data, tag=None if global_scope else tag),
        tag=None if global_scope else tag,
    )
    if limit and limit > 0:
        rows = rows[-limit:]
    titles = [str(item.get("title") or "").strip() for item in rows if isinstance(item, dict) and str(item.get("title") or "").strip()]
    descriptions = [str(item.get("description") or "").strip() for item in rows if isinstance(item, dict) and str(item.get("description") or "").strip()]
    thumbnail_prompts = [
        str(prompt).strip()
        for item in rows
        if isinstance(item, dict)
        for prompt in _thumbnail_prompt_values(item.get("thumbnail_prompts"))
        if str(prompt).strip()
    ]
    tag_signatures = [
        str(item.get("tag_signature") or "").strip()
        for item in rows
        if isinstance(item, dict) and str(item.get("tag_signature") or "").strip()
    ]
    return {
        "titles": titles,
        "descriptions": descriptions,
        "thumbnail_prompts": thumbnail_prompts,
        "tag_signatures": tag_signatures,
    }


def _cache_key_for_history_sync(config: dict[str, Any] | None, tag: str | None) -> str:
    root = str(get_used_metadata_root(config))
    return f"{root.lower()}::{_metadata_key(tag or '*')}"


def _iter_upload_record_paths() -> list[Path]:
    if not UPLOAD_RECORDS_ROOT.exists():
        return []
    return sorted(UPLOAD_RECORDS_ROOT.rglob("channel_*.json"))


def _extract_manifest_metadata(upload_record: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    tag_list: list[str] = []
    thumbnail_prompts: list[str] = []
    thumbnails: list[str] = []
    video_info = upload_record.get("video") if isinstance(upload_record.get("video"), dict) else {}
    video_path_text = str(video_info.get("path") or "").strip()
    serial = upload_record.get("serial")
    if not video_path_text or serial in (None, ""):
        return tag_list, thumbnail_prompts, thumbnails
    manifest_path = Path(video_path_text).parent / "upload_manifest.json"
    manifest = _read_json(manifest_path, {})
    channels = manifest.get("channels") if isinstance(manifest, dict) else {}
    channel = channels.get(str(serial)) if isinstance(channels, dict) else {}
    if not isinstance(channel, dict):
        return tag_list, thumbnail_prompts, thumbnails
    tag_list = [str(item).strip() for item in channel.get("tag_list", []) if str(item).strip()]
    thumbnail_prompts = [
        str(item).strip()
        for item in channel.get("thumbnail_prompts", [])
        if str(item).strip()
    ]
    thumbnails = [str(item).strip() for item in channel.get("thumbnails", []) if str(item).strip()]
    return tag_list, thumbnail_prompts, thumbnails


def sync_uploaded_history_into_used_metadata(
    *,
    config: dict[str, Any] | None = None,
    tag: str | None = None,
    min_interval_seconds: float = 15.0,
) -> bool:
    cache_key = _cache_key_for_history_sync(config, tag)
    now = time.time()
    with _CACHE_LOCK:
        last_sync = _UPLOAD_HISTORY_SYNC_CACHE.get(cache_key, 0.0)
    if min_interval_seconds > 0 and now - last_sync < min_interval_seconds:
        return False

    data = load_used_metadata_history(config)
    tags = data.setdefault("tags", {})
    existing_signatures = {
        _record_signature(item)
        for item in _iter_metadata_rows(data)
        if isinstance(item, dict)
    }
    wanted_tag = _metadata_key(tag) if tag else ""
    changed = False

    for path in _iter_upload_record_paths():
        record = _read_json(path, {})
        if not isinstance(record, dict) or not bool(record.get("success")):
            continue
        record_tag = str(record.get("tag") or "").strip()
        if not record_tag:
            continue
        if wanted_tag and _metadata_key(record_tag) != wanted_tag:
            continue

        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        title = str(metadata.get("title") or "").strip()
        description = str(metadata.get("description") or "").strip()
        if not title:
            continue

        tag_list, thumbnail_prompts, thumbnails = _extract_manifest_metadata(record)
        if not thumbnails:
            thumbnails = [
                str((item or {}).get("path") or "").strip()
                for item in (record.get("thumbnails") or [])
                if str((item or {}).get("path") or "").strip()
            ]

        built = _build_metadata_record(
            tag=record_tag,
            serial=int(record["serial"]) if record.get("serial") is not None else None,
            date_mmdd=str(record.get("date") or "").strip(),
            title=title,
            description=description,
            tag_list=tag_list,
            thumbnail_prompts=thumbnail_prompts,
            thumbnails=thumbnails,
            source="uploaded_history",
        )
        signature = _record_signature(built)
        if signature and signature in existing_signatures:
            continue

        tag_key = _metadata_key(record_tag)
        rows = tags.setdefault(tag_key, [])
        if not isinstance(rows, list):
            rows = []
            tags[tag_key] = rows
        rows.append(built)
        existing_signatures.add(signature)
        changed = True

    if changed:
        for tag_key, rows in list(tags.items()):
            if isinstance(rows, list):
                tags[tag_key] = rows[-MAX_METADATA_ROWS:]
        _write_json(get_used_metadata_history_path(config), data)

    with _CACHE_LOCK:
        _UPLOAD_HISTORY_SYNC_CACHE[cache_key] = now
    return changed


def _collect_uploaded_history_rows(tag: str | None = None) -> list[dict[str, Any]]:
    wanted_tag = _metadata_key(tag) if tag else ""
    rows: list[dict[str, Any]] = []
    for path in _iter_upload_record_paths():
        record = _read_json(path, {})
        if not isinstance(record, dict) or not bool(record.get("success")):
            continue
        record_tag = str(record.get("tag") or "").strip()
        if not record_tag:
            continue
        if wanted_tag and _metadata_key(record_tag) != wanted_tag:
            continue
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        title = str(metadata.get("title") or "").strip()
        description = str(metadata.get("description") or "").strip()
        if not title:
            continue
        tag_list, thumbnail_prompts, thumbnails = _extract_manifest_metadata(record)
        if not thumbnails:
            thumbnails = [
                str((item or {}).get("path") or "").strip()
                for item in (record.get("thumbnails") or [])
                if str((item or {}).get("path") or "").strip()
            ]
        rows.append(
            _build_metadata_record(
                tag=record_tag,
                serial=int(record["serial"]) if record.get("serial") is not None else None,
                date_mmdd=str(record.get("date") or "").strip(),
                title=title,
                description=description,
                tag_list=tag_list,
                thumbnail_prompts=thumbnail_prompts,
                thumbnails=thumbnails,
                source="uploaded_history",
            )
        )
    return rows


def _merge_metadata_rows_with_upload_records(existing_rows: list[dict[str, Any]], *, tag: str | None = None) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in [*existing_rows, *_iter_legacy_metadata_rows(tag), *_collect_uploaded_history_rows(tag)]:
        if not isinstance(item, dict):
            continue
        signature = _record_signature(item)
        if signature and signature in seen:
            continue
        if signature:
            seen.add(signature)
        merged.append(item)
    return merged


def _build_metadata_record(
    *,
    tag: str,
    serial: int | None,
    date_mmdd: str,
    title: str,
    description: str,
    tag_list: list[str],
    thumbnail_prompts: list[str],
    thumbnails: list[str] | None = None,
    source: str = "",
) -> dict[str, Any]:
    return {
        "tag": str(tag or "").strip(),
        "serial": int(serial) if serial is not None else None,
        "date_mmdd": str(date_mmdd or "").strip(),
        "title": str(title or "").strip(),
        "description": str(description or "").strip(),
        "tag_signature": " | ".join(str(item).strip() for item in tag_list if str(item).strip()),
        "thumbnail_prompts": [str(item).strip() for item in thumbnail_prompts if str(item).strip()],
        "thumbnails": [str(item).strip() for item in (thumbnails or []) if str(item).strip()],
        "source": str(source or "").strip(),
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _record_signature(record: dict[str, Any]) -> str:
    return "||".join(
        [
            _normalize_text_key(record.get("title")),
            _normalize_text_key(record.get("description")),
            _normalize_text_key(record.get("tag_signature")),
            " | ".join(_normalize_text_key(item) for item in record.get("thumbnail_prompts", [])),
        ]
    )


def record_used_metadata(
    *,
    tag: str,
    title: str,
    description: str,
    tag_list: list[str],
    thumbnail_prompts: list[str],
    config: dict[str, Any] | None = None,
    serial: int | None = None,
    date_mmdd: str = "",
    thumbnails: list[Path] | None = None,
    source: str = "",
    keep_per_tag: int = 20000,
) -> None:
    data = load_used_metadata_history(config)
    tags = data.setdefault("tags", {})
    tag_key = _metadata_key(tag)
    rows = tags.setdefault(tag_key, [])
    if not isinstance(rows, list):
        rows = []
        tags[tag_key] = rows
    record = _build_metadata_record(
        tag=tag,
        serial=serial,
        date_mmdd=date_mmdd,
        title=title,
        description=description,
        tag_list=tag_list,
        thumbnail_prompts=thumbnail_prompts,
        thumbnails=[str(path) for path in (thumbnails or [])],
        source=source,
    )
    signature = _record_signature(record)
    all_rows = _iter_metadata_rows(data)
    if signature and any(_record_signature(item) == signature for item in all_rows):
        _write_json(get_used_metadata_history_path(config), data)
        return
    rows.append(record)
    tags[tag_key] = rows[-max(1000, keep_per_tag):]
    _write_json(get_used_metadata_history_path(config), data)


def archive_uploaded_metadata(
    *,
    tag: str,
    serial: int,
    date_mmdd: str,
    title: str,
    description: str,
    tag_list: list[str],
    thumbnail_prompts: list[str],
    thumbnails: list[Path],
    config: dict[str, Any] | None = None,
    move_files: bool = True,
    log: LogFunc = _noop_log,
) -> Path:
    root = get_used_metadata_root(config)
    archive_dir = root / _metadata_key(tag) / f"{date_mmdd}_{serial}"
    archive_dir.mkdir(parents=True, exist_ok=True)

    archived_thumbnail_paths: list[str] = []
    for thumb in thumbnails:
        if not thumb or not thumb.exists():
            continue
        target = archive_dir / thumb.name
        if move_files:
            if target.exists():
                target.unlink()
            shutil.move(str(thumb), str(target))
            log(f"[Metadata] 已归档缩略图: {target}")
        else:
            if target.exists():
                target.unlink()
            shutil.copy2(thumb, target)
        archived_thumbnail_paths.append(str(target))

    record = _build_metadata_record(
        tag=tag,
        serial=serial,
        date_mmdd=date_mmdd,
        title=title,
        description=description,
        tag_list=tag_list,
        thumbnail_prompts=thumbnail_prompts,
        thumbnails=archived_thumbnail_paths,
        source="uploaded",
    )
    bundle_path = archive_dir / "metadata_bundle.json"
    _write_json(bundle_path, record)
    record_used_metadata(
        tag=tag,
        title=title,
        description=description,
        tag_list=tag_list,
        thumbnail_prompts=thumbnail_prompts,
        config=config,
        serial=serial,
        date_mmdd=date_mmdd,
        thumbnails=[Path(item) for item in archived_thumbnail_paths],
        source="uploaded",
    )
    log(f"[Metadata] 已归档文案记录: {bundle_path}")
    return bundle_path
