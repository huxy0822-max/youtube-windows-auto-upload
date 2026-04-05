# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PATH_TEMPLATES_FILE = SCRIPT_DIR / "config" / "path_templates.json"
DEFAULT_PATH_TEMPLATE_NAME = "默认路径"
DEFAULT_PATH_TEMPLATE = {
    "description": "通用默认路径配置",
    "source_root": "",
    "copywriting_output": "",
    "thumbnail_output": "",
    "render_output": "",
    "used_materials_dir": "",
    "used_videos_dir": "",
    "auto_delete_days": 7,
}


def _normalize_days(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(str(value).strip()))
    except (TypeError, ValueError):
        return default


def normalize_path_template(name: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    return {
        "description": str(data.get("description") or "").strip(),
        "source_root": str(data.get("source_root") or "").strip(),
        "copywriting_output": str(data.get("copywriting_output") or "").strip(),
        "thumbnail_output": str(data.get("thumbnail_output") or "").strip(),
        "render_output": str(data.get("render_output") or "").strip(),
        "used_materials_dir": str(data.get("used_materials_dir") or "").strip(),
        "used_videos_dir": str(data.get("used_videos_dir") or "").strip(),
        "auto_delete_days": _normalize_days(data.get("auto_delete_days"), default=0),
    }


def _write_templates(path: Path, payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def load_path_templates(path: Path = PATH_TEMPLATES_FILE) -> dict[str, dict[str, Any]]:
    raw: dict[str, Any] = {}
    if path.exists():
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                raw = parsed
        except Exception as exc:
            print(f"[path_templates] JSON读取失败 {path}: {exc}")
            raw = {}

    templates: dict[str, dict[str, Any]] = {}
    for raw_name, raw_payload in raw.items():
        clean_name = str(raw_name or "").strip()
        if not clean_name:
            continue
        templates[clean_name] = normalize_path_template(clean_name, raw_payload)

    changed = False
    if DEFAULT_PATH_TEMPLATE_NAME not in templates:
        templates[DEFAULT_PATH_TEMPLATE_NAME] = normalize_path_template(
            DEFAULT_PATH_TEMPLATE_NAME,
            DEFAULT_PATH_TEMPLATE,
        )
        changed = True
    if changed or not path.exists():
        _write_templates(path, templates)
    return templates


def save_path_templates(
    templates: dict[str, dict[str, Any]],
    path: Path = PATH_TEMPLATES_FILE,
) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for raw_name, raw_payload in (templates or {}).items():
        clean_name = str(raw_name or "").strip()
        if not clean_name:
            continue
        normalized[clean_name] = normalize_path_template(clean_name, raw_payload)
    if DEFAULT_PATH_TEMPLATE_NAME not in normalized:
        normalized[DEFAULT_PATH_TEMPLATE_NAME] = normalize_path_template(
            DEFAULT_PATH_TEMPLATE_NAME,
            DEFAULT_PATH_TEMPLATE,
        )
    return _write_templates(path, normalized)


def get_path_template(
    name: str | None,
    templates: dict[str, dict[str, Any]] | None = None,
    path: Path = PATH_TEMPLATES_FILE,
) -> tuple[str, dict[str, Any]]:
    loaded = templates if isinstance(templates, dict) else load_path_templates(path)
    clean_name = str(name or "").strip()
    if clean_name and clean_name in loaded:
        return clean_name, dict(loaded[clean_name])
    if DEFAULT_PATH_TEMPLATE_NAME in loaded:
        return DEFAULT_PATH_TEMPLATE_NAME, dict(loaded[DEFAULT_PATH_TEMPLATE_NAME])
    first_name = next(iter(loaded.keys()), DEFAULT_PATH_TEMPLATE_NAME)
    return first_name, normalize_path_template(first_name, loaded.get(first_name))


def resolve_source_dir(
    template: dict[str, Any],
    *,
    group_tag: str = "",
    fallback: str = "",
) -> str:
    source_root = str(template.get("source_root") or "").strip()
    if source_root:
        root = Path(source_root)
        clean_group = str(group_tag or "").strip()
        group_candidate = root / clean_group if clean_group else root
        if clean_group and group_candidate.exists():
            return str(group_candidate)
        return str(root)
    return str(fallback or "").strip()


def build_runtime_config(
    base_config: dict[str, Any],
    template: dict[str, Any],
    *,
    template_name: str = "",
    source_dir: str = "",
) -> dict[str, Any]:
    config = dict(base_config or {})
    source_text = str(source_dir or "").strip() or str(template.get("source_root") or "").strip()
    metadata_root = str(template.get("copywriting_output") or "").strip() or source_text
    render_root = str(template.get("render_output") or "").strip() or source_text
    thumbnail_root = str(template.get("thumbnail_output") or "").strip() or metadata_root

    config.update(
        {
            "metadata_root": metadata_root,
            "output_root": render_root,
            "music_dir": source_text,
            "base_image_dir": source_text,
            "thumbnail_output_root": thumbnail_root,
            "path_template_name": str(template_name or "").strip(),
            "path_template": dict(template or {}),
            # 素材归档改由 archive_manager 在上传成功后处理，避免渲染阶段提前搬走。
            "used_media_root": "",
            "render_cleanup_days": 0,
        }
    )
    return config
