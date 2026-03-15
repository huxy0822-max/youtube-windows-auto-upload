#!/usr/bin/env python3
"""
分组批量上传的编排层。

目标：
1. 扫描“现成视频文件夹”；
2. 按 tag 下的频道顺序，把视频一一映射到频道；
3. 生成 upload_manifest.json 与 generation_map.json；
4. 把现成视频/缩略图整理到 batch_upload.py 可消费的标准目录。
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from content_generation import (
    call_image_model,
    generate_content_bundle,
    save_data_url_image,
)
from path_helpers import default_scheduler_config, normalize_scheduler_config
from prompt_studio import load_generation_map, save_generation_map
from utils import get_tag_info


VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".m4v", ".webm"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}


def default_upload_batch_settings() -> dict[str, Any]:
    return {
        "version": 1,
        "generation_mode": "site_api",
        "source_video_dir": "",
        "source_thumbnail_dir": "",
        "selected_serials_text": "",
        "visibility": "public",
        "category": "Music",
        "made_for_kids": False,
        "altered_content": True,
        "schedule_enabled": False,
        "schedule_start": "",
        "schedule_interval_minutes": 60,
    }


def load_upload_batch_settings(path: Path) -> dict[str, Any]:
    settings = default_upload_batch_settings()
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                settings.update(loaded)
        except Exception:
            pass
    return settings


def save_upload_batch_settings(path: Path, settings: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_mmdd(text: str) -> str:
    raw = str(text).strip()
    if "." in raw:
        month, day = raw.split(".", 1)
        return f"{int(month):02d}{int(day):02d}"
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 3:
        return digits.zfill(4)
    if len(digits) == 4:
        return digits
    raise ValueError("日期格式应为 MMDD 或 M.DD，例如 0312 / 3.12")


def parse_serials_text(raw: str) -> list[int]:
    serials: list[int] = []
    seen = set()
    normalized = str(raw or "").replace("，", ",").replace("；", ",").replace(";", ",")
    for chunk in normalized.split(","):
        value = chunk.strip()
        if not value or not value.isdigit():
            continue
        serial = int(value)
        if serial not in seen:
            serials.append(serial)
            seen.add(serial)
    return serials


def list_sorted_files(folder: Path, suffixes: set[str]) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(
        [path for path in folder.iterdir() if path.is_file() and path.suffix.lower() in suffixes],
        key=lambda item: item.name.lower(),
    )


def load_channel_name_map(path: Path) -> dict[int, str]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    mapping: dict[int, str] = {}
    for info in (raw.get("channels") or {}).values():
        serial = info.get("serial_number")
        name = str(info.get("channel_name") or "").strip()
        if serial is not None and name:
            mapping[int(serial)] = name
    return mapping


def load_runtime_paths(script_dir: Path, scheduler_config_path: Path) -> dict[str, Path]:
    if scheduler_config_path.exists():
        try:
            raw = json.loads(scheduler_config_path.read_text(encoding="utf-8"))
            cfg = normalize_scheduler_config(raw, script_dir)
        except Exception:
            cfg = default_scheduler_config(script_dir)
    else:
        cfg = default_scheduler_config(script_dir)
    return {
        "output_root": Path(cfg.get("output_root") or (script_dir / "workspace" / "AutoTask")),
        "base_image_dir": Path(cfg.get("base_image_dir") or (script_dir / "workspace" / "base_image")),
    }


def _link_or_copy(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        if source.resolve(strict=False) == target.resolve(strict=False):
            return target
    except Exception:
        pass
    if target.exists():
        target.unlink()
    try:
        os.link(source, target)
    except Exception:
        shutil.copy2(source, target)
    return target


def _pick_manual_thumbnails(source_images: list[Path], start_index: int, count: int) -> tuple[list[Path], int]:
    if not source_images or count <= 0:
        return [], start_index
    picked = source_images[start_index : start_index + count]
    return picked, start_index + len(picked)


def _build_schedule_time(start_text: str, interval_minutes: int, index: int) -> str | None:
    if not start_text:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            base = datetime.strptime(start_text.strip(), fmt)
            current = base + timedelta(minutes=max(0, interval_minutes) * index)
            return current.strftime("%Y-%m-%d %H:%M")
        except Exception:
            continue
    raise ValueError("定时发布时间格式应为 YYYY-MM-DD HH:MM")


def _append_hashtags(description: str, seo_hashtags: list[str]) -> str:
    desc = str(description or "").strip()
    hashtags = " ".join([tag for tag in seo_hashtags if tag])
    if not hashtags:
        return desc
    if hashtags in desc:
        return desc
    return f"{desc}\n\n{hashtags}".strip()


def _load_legacy_entry(tag_dir: Path, date_mmdd: str, serial: int) -> dict[str, Any] | None:
    generation_map = load_generation_map(tag_dir / "generation_map.json")
    channel_info = (generation_map.get("channels") or {}).get(str(serial))
    if not isinstance(channel_info, dict):
        return None
    day_info = (channel_info.get("days") or {}).get(date_mmdd)
    if not isinstance(day_info, dict):
        return None
    return {
        "is_ypp": bool(channel_info.get("is_ypp", False)),
        "title": str(day_info.get("title") or "").strip(),
        "description": str(day_info.get("description") or "").strip(),
        "covers": [str(item).strip() for item in (day_info.get("covers") or []) if str(item).strip()],
        "ab_titles": [str(item).strip() for item in (day_info.get("ab_titles") or []) if str(item).strip()],
        "set": int(day_info.get("set") or 1),
    }


def _save_generation_entry(
    generation_map_path: Path,
    *,
    date_mmdd: str,
    serial: int,
    is_ypp: bool,
    title: str,
    description: str,
    covers: list[str],
    ab_titles: list[str],
) -> None:
    generation_map = load_generation_map(generation_map_path)
    channel_info = generation_map.setdefault("channels", {}).setdefault(str(serial), {"is_ypp": is_ypp, "days": {}})
    channel_info["is_ypp"] = bool(is_ypp)
    channel_info.setdefault("days", {})
    channel_info["days"][date_mmdd] = {
        "title": title,
        "description": description,
        "covers": covers,
        "ab_titles": ab_titles,
        "set": 1,
    }
    save_generation_map(generation_map_path, generation_map)


def prepare_group_upload_batch(
    *,
    script_dir: Path,
    scheduler_config_path: Path,
    prompt_studio_path: Path,
    channel_mapping_path: Path,
    tag: str,
    date_value: str,
    source_video_dir: Path,
    thumbnail_dir: Path | None = None,
    selected_serials: list[int] | None = None,
    generation_mode: str = "site_api",
    visibility: str = "public",
    category: str = "Music",
    made_for_kids: bool = False,
    altered_content: bool = True,
    schedule_enabled: bool = False,
    schedule_start: str = "",
    schedule_interval_minutes: int = 60,
) -> dict[str, Any]:
    date_mmdd = normalize_mmdd(date_value)
    if schedule_enabled and not str(schedule_start or "").strip():
        raise ValueError("启用定时发布时，必须填写开始发布时间")
    tag_info = get_tag_info(tag)
    if not tag_info:
        raise ValueError(f"未找到分组标签: {tag}")

    all_serials = list(tag_info.get("all_serials") or [])
    ypp_serials = set(tag_info.get("ypp_serials") or [])
    serials = selected_serials or all_serials
    serials = [serial for serial in serials if serial in all_serials]
    if not serials:
        raise ValueError("当前分组没有可用频道，或你填写的频道序号不属于该分组")

    source_video_dir = Path(source_video_dir)
    if not source_video_dir.exists():
        raise ValueError(f"视频文件夹不存在: {source_video_dir}")
    source_videos = list_sorted_files(source_video_dir, VIDEO_EXTENSIONS)
    if not source_videos:
        raise ValueError(f"视频文件夹里未找到支持的视频文件: {source_video_dir}")

    runtime_paths = load_runtime_paths(script_dir, scheduler_config_path)
    output_dir = runtime_paths["output_root"] / f"{date_mmdd}_{tag}"
    tag_base_image_dir = runtime_paths["base_image_dir"] / tag
    generation_map_path = tag_base_image_dir / "generation_map.json"
    channel_name_map = load_channel_name_map(channel_mapping_path)

    source_images = list_sorted_files(Path(thumbnail_dir), IMAGE_EXTENSIONS) if thumbnail_dir else []
    thumb_cursor = 0
    assigned_count = min(len(source_videos), len(serials))
    warnings: list[str] = []
    preview_lines: list[str] = []
    channels: dict[str, Any] = {}

    if len(source_videos) > len(serials):
        warnings.append(f"视频数量 {len(source_videos)} 大于频道数量 {len(serials)}，只会使用前 {assigned_count} 个视频")
    if len(serials) > len(source_videos):
        warnings.append(f"频道数量 {len(serials)} 大于视频数量 {len(source_videos)}，后面的频道会被跳过")

    for index in range(assigned_count):
        serial = serials[index]
        source_video = source_videos[index]
        is_ypp = serial in ypp_serials
        staged_video = output_dir / f"{date_mmdd}_{serial}{source_video.suffix.lower()}"
        _link_or_copy(source_video, staged_video)

        wanted_thumb_count = 3 if is_ypp else 1
        manual_thumbs, thumb_cursor = _pick_manual_thumbnails(source_images, thumb_cursor, wanted_thumb_count)

        titles: list[str] = []
        descriptions: list[str] = []
        seo_hashtags: list[str] = []
        tag_list: list[str] = []
        thumbnail_prompts: list[str] = []
        cover_paths: list[Path] = []

        if generation_mode == "site_api":
            bundle = generate_content_bundle(prompt_studio_path, tag, is_ypp=is_ypp)
            titles = bundle["titles"]
            descriptions = bundle["descriptions"]
            seo_hashtags = bundle["seo_hashtags"]
            tag_list = bundle["tag_list"]
            thumbnail_prompts = [item.get("prompt", "") for item in bundle.get("thumbnail_prompts", [])]

            for thumb_index, image_path in enumerate(manual_thumbs, 1):
                target = tag_base_image_dir / f"{date_mmdd}_{serial}_cover_{thumb_index:02d}{image_path.suffix.lower()}"
                cover_paths.append(_link_or_copy(image_path, target))

            if not cover_paths and str(bundle["api_preset"].get("autoImageEnabled") or "0") == "1":
                for thumb_index, item in enumerate(bundle.get("thumbnail_prompts", []), 1):
                    target = tag_base_image_dir / f"{date_mmdd}_{serial}_cover_{thumb_index:02d}.png"
                    try:
                        image_result = call_image_model(bundle["api_preset"], item.get("prompt", ""))
                        if image_result.get("data_url"):
                            cover_paths.append(save_data_url_image(image_result["data_url"], target))
                    except Exception as exc:
                        warnings.append(f"序号 {serial} 缩略图 {thumb_index} 自动生成失败: {exc}")
            elif not cover_paths:
                warnings.append(f"序号 {serial} 未生成缩略图：未提供缩略图目录，且图片 API 未开启")
        else:
            legacy = _load_legacy_entry(tag_base_image_dir, date_mmdd, serial)
            if legacy:
                titles = [legacy["title"]] if legacy.get("title") else []
                descriptions = [legacy["description"]] if legacy.get("description") else []
                tag_list = []
                seo_hashtags = []
                for cover_name in legacy.get("covers", []):
                    cover_path = tag_base_image_dir / Path(cover_name).name
                    if cover_path.exists():
                        cover_paths.append(cover_path)
            else:
                warnings.append(f"序号 {serial} 在原始 generation_map 里没有 {date_mmdd} 的文案，已回退为文件名标题")
                titles = [source_video.stem]
                descriptions = [f"{tag} | {source_video.stem}"]

        title = (titles[0] if titles else source_video.stem).strip()
        description = _append_hashtags(descriptions[0] if descriptions else "", seo_hashtags)
        ab_titles = titles[:3] if is_ypp else []

        if schedule_enabled:
            scheduled_publish_at = _build_schedule_time(schedule_start, schedule_interval_minutes, index)
        else:
            scheduled_publish_at = None

        cover_names = [path.name for path in cover_paths]
        _save_generation_entry(
            generation_map_path,
            date_mmdd=date_mmdd,
            serial=serial,
            is_ypp=is_ypp,
            title=title,
            description=description,
            covers=cover_names,
            ab_titles=ab_titles,
        )

        channel_payload = {
            "video": staged_video.name,
            "source_video": str(source_video),
            "channel_name": channel_name_map.get(serial, ""),
            "title": title,
            "description": description,
            "thumbnails": [str(path) for path in cover_paths],
            "thumbnail_prompts": thumbnail_prompts,
            "seo_hashtags": seo_hashtags,
            "tag_list": tag_list,
            "is_ypp": bool(is_ypp),
            "ab_titles": ab_titles,
            "set": 1,
            "upload_options": {
                "made_for_kids": bool(made_for_kids),
                "altered_content": bool(altered_content),
                "category": category,
                "visibility": visibility,
                "scheduled_publish_at": scheduled_publish_at,
            },
        }
        channels[str(serial)] = channel_payload

        preview_lines.append(
            f"[{index + 1}] 序号 {serial} / {channel_name_map.get(serial, '未知频道')} / "
            f"视频={source_video.name} / 标题={title[:28]}"
        )

    manifest = {
        "date": date_mmdd,
        "tag": tag,
        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source": "group_upload_batch",
        "generation_mode": generation_mode,
        "source_video_dir": str(source_video_dir),
        "source_thumbnail_dir": str(thumbnail_dir) if thumbnail_dir else "",
        "channels": channels,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "upload_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    assigned_serials = [int(key) for key in channels.keys()]
    skipped_serials = [serial for serial in all_serials if serial not in assigned_serials]
    return {
        "date": date_mmdd,
        "tag": tag,
        "manifest_path": manifest_path,
        "output_dir": output_dir,
        "assigned_count": len(channels),
        "assigned_serials": assigned_serials,
        "skipped_serials": skipped_serials,
        "preview_lines": preview_lines,
        "warnings": warnings,
    }


def prepare_window_task_upload_batch(
    *,
    script_dir: Path,
    scheduler_config_path: Path,
    prompt_studio_path: Path,
    channel_mapping_path: Path,
    window_plan: dict[str, Any],
    date_value: str,
    source_video_dir: Path,
    thumbnail_dir: Path | None = None,
    metadata_mode: str = "prompt_api",
    fill_title_desc_tags: bool = True,
    fill_thumbnails: bool = True,
    sync_daily_content: bool = True,
) -> dict[str, Any]:
    date_mmdd = normalize_mmdd(date_value)
    tasks = [dict(item) for item in (window_plan.get("tasks") or []) if isinstance(item, dict)]
    if not tasks:
        raise ValueError("窗口任务计划里没有可执行的上传任务")

    source_video_dir = Path(source_video_dir)
    if not source_video_dir.exists():
        raise ValueError(f"视频文件夹不存在: {source_video_dir}")
    source_videos = list_sorted_files(source_video_dir, VIDEO_EXTENSIONS)
    if not source_videos:
        raise ValueError(f"视频文件夹里未找到支持的视频文件: {source_video_dir}")

    runtime_paths = load_runtime_paths(script_dir, scheduler_config_path)
    channel_name_map = load_channel_name_map(channel_mapping_path)
    source_images = list_sorted_files(Path(thumbnail_dir), IMAGE_EXTENSIONS) if thumbnail_dir else []
    thumb_cursor = 0
    assigned_count = min(len(source_videos), len(tasks))
    warnings: list[str] = []
    preview_lines: list[str] = []
    manifest_by_tag: dict[str, dict[str, Any]] = {}
    output_dirs: dict[str, Path] = {}
    default_upload_options = dict(window_plan.get("default_upload_options") or {})

    if len(source_videos) > len(tasks):
        warnings.append(f"视频数量 {len(source_videos)} 大于窗口任务数量 {len(tasks)}，只会使用前 {assigned_count} 个视频")
    if len(tasks) > len(source_videos):
        warnings.append(f"窗口任务数量 {len(tasks)} 大于视频数量 {len(source_videos)}，后面的窗口会被跳过")

    for index in range(assigned_count):
        task = dict(tasks[index])
        tag = str(task.get("tag") or "").strip()
        serial = int(task.get("serial") or 0)
        if not tag or not serial:
            warnings.append(f"第 {index + 1} 个窗口任务缺少 tag 或 serial，已跳过")
            continue

        tag_info = get_tag_info(tag)
        if not tag_info:
            warnings.append(f"未找到分组标签 {tag}，序号 {serial} 已跳过")
            continue

        ypp_serials = set(tag_info.get("ypp_serials") or [])
        is_ypp = bool(task["is_ypp"]) if "is_ypp" in task else serial in ypp_serials
        source_video = source_videos[index]
        output_dir = runtime_paths["output_root"] / f"{date_mmdd}_{tag}"
        output_dirs[tag] = output_dir
        staged_video = output_dir / f"{date_mmdd}_{serial}{source_video.suffix.lower()}"
        _link_or_copy(source_video, staged_video)

        tag_base_image_dir = runtime_paths["base_image_dir"] / tag
        generation_map_path = tag_base_image_dir / "generation_map.json"
        wanted_thumb_count = 3 if is_ypp else 1
        manual_thumbs, thumb_cursor = _pick_manual_thumbnails(
            source_images,
            thumb_cursor,
            wanted_thumb_count if fill_thumbnails else 0,
        )

        titles: list[str] = []
        descriptions: list[str] = []
        seo_hashtags: list[str] = []
        tag_list: list[str] = []
        thumbnail_prompts: list[str] = []
        cover_paths: list[Path] = []

        if metadata_mode == "prompt_api":
            bundle = None
            if fill_title_desc_tags or fill_thumbnails:
                bundle = generate_content_bundle(prompt_studio_path, tag, is_ypp=is_ypp)
                thumbnail_prompts = [item.get("prompt", "") for item in bundle.get("thumbnail_prompts", [])]
            if bundle and fill_title_desc_tags:
                titles = bundle["titles"]
                descriptions = bundle["descriptions"]
                seo_hashtags = bundle["seo_hashtags"]
                tag_list = bundle["tag_list"]

            if fill_thumbnails:
                explicit_covers = [Path(item) for item in task.get("thumbnails", []) if Path(item).exists()]
                if explicit_covers:
                    cover_paths.extend(explicit_covers[:wanted_thumb_count])
                if not cover_paths:
                    for thumb_index, image_path in enumerate(manual_thumbs, 1):
                        target = tag_base_image_dir / f"{date_mmdd}_{serial}_cover_{thumb_index:02d}{image_path.suffix.lower()}"
                        cover_paths.append(_link_or_copy(image_path, target))
                if (
                    not cover_paths
                    and bundle
                    and str(bundle["api_preset"].get("autoImageEnabled") or "0") == "1"
                ):
                    for thumb_index, item in enumerate(bundle.get("thumbnail_prompts", []), 1):
                        target = tag_base_image_dir / f"{date_mmdd}_{serial}_cover_{thumb_index:02d}.png"
                        try:
                            image_result = call_image_model(bundle["api_preset"], item.get("prompt", ""))
                            if image_result.get("data_url"):
                                cover_paths.append(save_data_url_image(image_result["data_url"], target))
                        except Exception as exc:
                            warnings.append(f"序号 {serial} 缩略图 {thumb_index} 自动生成失败: {exc}")
                elif not cover_paths:
                    warnings.append(f"序号 {serial} 未生成缩略图：未提供缩略图目录，且图片 API 未开启")
        else:
            legacy = _load_legacy_entry(tag_base_image_dir, date_mmdd, serial)
            if legacy:
                if fill_title_desc_tags:
                    titles = [legacy["title"]] if legacy.get("title") else []
                    descriptions = [legacy["description"]] if legacy.get("description") else []
                if fill_thumbnails:
                    for cover_name in legacy.get("covers", []):
                        cover_path = tag_base_image_dir / Path(cover_name).name
                        if cover_path.exists():
                            cover_paths.append(cover_path)
            else:
                warnings.append(f"序号 {serial} 在当日内容里没有 {date_mmdd} 的文案，已回退为文件名标题")

        if fill_title_desc_tags:
            title = (str(task.get("title") or "").strip() or (titles[0] if titles else source_video.stem).strip())
            description = str(task.get("description") or "").strip() or _append_hashtags(
                descriptions[0] if descriptions else "",
                seo_hashtags,
            )
            tag_list = [str(item).strip() for item in task.get("tag_list", []) if str(item).strip()] or tag_list
        else:
            title = str(task.get("title") or "").strip() or source_video.stem
            description = str(task.get("description") or "").strip()
            tag_list = [str(item).strip() for item in task.get("tag_list", []) if str(item).strip()]

        if fill_thumbnails and task.get("thumbnails"):
            override_covers = [Path(item) for item in task.get("thumbnails", []) if Path(item).exists()]
            if override_covers:
                cover_paths = override_covers[:wanted_thumb_count]
        if not fill_thumbnails:
            cover_paths = []

        ab_titles = [str(item).strip() for item in task.get("ab_titles", []) if str(item).strip()]
        if not ab_titles:
            ab_titles = titles[:3] if is_ypp and fill_title_desc_tags else []

        upload_options = {
            "made_for_kids": bool(default_upload_options.get("made_for_kids", False)),
            "altered_content": bool(default_upload_options.get("altered_content", True)),
            "category": str(default_upload_options.get("category") or "Music").strip() or "Music",
            "visibility": str(default_upload_options.get("visibility") or "public").strip() or "public",
            "scheduled_publish_at": str(task.get("scheduled_publish_at") or default_upload_options.get("scheduled_publish_at") or "").strip() or None,
        }
        for key in ("made_for_kids", "altered_content", "category", "visibility", "scheduled_publish_at"):
            if key in task:
                upload_options[key] = task[key]

        cover_names = [path.name for path in cover_paths]
        if sync_daily_content:
            _save_generation_entry(
                generation_map_path,
                date_mmdd=date_mmdd,
                serial=serial,
                is_ypp=is_ypp,
                title=title,
                description=description,
                covers=cover_names,
                ab_titles=ab_titles,
            )

        manifest = manifest_by_tag.setdefault(
            tag,
            {
                "date": date_mmdd,
                "tag": tag,
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "source": "window_task_group_upload",
                "metadata_mode": metadata_mode,
                "source_video_dir": str(source_video_dir),
                "source_thumbnail_dir": str(thumbnail_dir) if thumbnail_dir else "",
                "channels": {},
            },
        )
        manifest["channels"][str(serial)] = {
            "video": staged_video.name,
            "source_video": str(source_video),
            "channel_name": str(task.get("channel_name") or channel_name_map.get(serial, "")).strip(),
            "title": title,
            "description": description,
            "thumbnails": [str(path) for path in cover_paths],
            "thumbnail_prompts": thumbnail_prompts,
            "seo_hashtags": seo_hashtags,
            "tag_list": tag_list,
            "is_ypp": bool(is_ypp),
            "ab_titles": ab_titles,
            "set": 1,
            "upload_options": upload_options,
        }

        preview_lines.append(
            f"[{index + 1}] {tag} / 序号 {serial} / 视频={source_video.name} / 标题={title[:28]}"
        )

    manifest_paths: dict[str, str] = {}
    for tag, manifest in manifest_by_tag.items():
        output_dir = output_dirs[tag]
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = output_dir / "upload_manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        manifest_paths[tag] = str(manifest_path)

    assigned_serials = {
        tag: sorted(int(serial) for serial in (manifest.get("channels") or {}).keys())
        for tag, manifest in manifest_by_tag.items()
    }
    return {
        "date": date_mmdd,
        "tags": sorted(manifest_by_tag.keys()),
        "manifest_paths": manifest_paths,
        "assigned_count": sum(len(item.get("channels", {})) for item in manifest_by_tag.values()),
        "assigned_serials": assigned_serials,
        "preview_lines": preview_lines,
        "warnings": warnings,
    }
