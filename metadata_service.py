from __future__ import annotations

import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from prompt_studio import normalize_tag_key

SCRIPT_DIR = Path(__file__).parent

LogFunc = Callable[[str], None]


def _noop_log(_message: str) -> None:
    return


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
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


def get_used_metadata_scope(
    tag: str,
    *,
    config: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, list[str]]:
    data = load_used_metadata_history(config)
    rows = data.get("tags", {}).get(_metadata_key(tag), [])
    if not isinstance(rows, list):
        rows = []
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
    if signature and any(_record_signature(item) == signature for item in rows if isinstance(item, dict)):
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
