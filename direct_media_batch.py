from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from content_generation import call_image_model, generate_content_bundle, save_data_url_image
from daily_scheduler import RenderOptions, build_effect_kwargs, clean_incomplete, get_audio_duration, render_video_task
from effects_library import get_effect
from group_upload_workflow import IMAGE_EXTENSIONS, load_channel_name_map, normalize_mmdd
from path_helpers import default_scheduler_config, normalize_scheduler_config
from prompt_studio import load_generation_map, save_generation_map
from upload_window_planner import derive_tags_and_skip_channels
from utils import get_tag_info

SCRIPT_DIR = Path(__file__).parent
SCHEDULER_CONFIG_FILE = SCRIPT_DIR / "scheduler_config.json"
PROMPT_STUDIO_FILE = SCRIPT_DIR / "config" / "prompt_studio.json"
CHANNEL_MAPPING_FILE = SCRIPT_DIR / "config" / "channel_mapping.json"
UPLOAD_SCRIPT = SCRIPT_DIR / "batch_upload.py"

AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg"}


def load_scheduler_settings(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return normalize_scheduler_config(raw, SCRIPT_DIR)
        except Exception:
            pass
    return default_scheduler_config(SCRIPT_DIR)


def load_window_plan(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(f"窗口任务计划不存在: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"窗口任务计划不是有效 JSON: {exc}") from exc
    tasks = data.get("tasks") or []
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("窗口任务计划里没有可执行任务")
    return data


def list_direct_files(folder: Path, suffixes: set[str]) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(
        [item for item in folder.iterdir() if item.is_file() and item.suffix.lower() in suffixes and not item.name.startswith(".")],
        key=lambda item: item.name.lower(),
    )


def save_generation_entry(
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
    channel_info = generation_map.setdefault("channels", {}).setdefault(str(serial), {"is_ypp": bool(is_ypp), "days": {}})
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


def cleanup_old_uploaded_videos(output_root: Path, retention_days: int) -> None:
    if retention_days <= 0 or not output_root.exists():
        return
    cutoff = datetime.now() - timedelta(days=retention_days)
    cleaned_count = 0
    for folder in sorted(output_root.iterdir()):
        if not folder.is_dir():
            continue
        report_file = folder / "upload_report.json"
        if not report_file.exists():
            continue
        if datetime.fromtimestamp(report_file.stat().st_mtime) >= cutoff:
            continue
        for video in folder.glob("*.mp4"):
            try:
                video.unlink()
                Path(str(video) + ".done").unlink(missing_ok=True)
                cleaned_count += 1
            except Exception:
                continue
    if cleaned_count:
        print(f"🧹 已清理超过 {retention_days} 天的已上传视频 {cleaned_count} 个")


def move_to_used(source: Path, used_root: Path, *, tag: str, kind: str) -> Path:
    target_dir = used_root / tag / kind
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / source.name
    if target.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = target_dir / f"{source.stem}_{stamp}{source.suffix.lower()}"
    shutil.move(str(source), str(target))
    return target


def pick_source_binding(cfg: dict[str, Any], tag: str) -> Path:
    bindings = cfg.get("group_source_bindings") or {}
    source_text = str(bindings.get(tag) or "").strip()
    if not source_text:
        raise ValueError(f"分组 {tag} 还没绑定素材文件夹，请先去路径配置里设置。")
    source_dir = Path(source_text)
    if not source_dir.exists():
        raise ValueError(f"分组 {tag} 的素材文件夹不存在: {source_dir}")
    return source_dir


def load_legacy_daily_entry(tag_dir: Path, date_mmdd: str, serial: int) -> dict[str, Any]:
    generation_map_path = tag_dir / "generation_map.json"
    if not generation_map_path.exists():
        return {}
    generation_map = load_generation_map(generation_map_path)
    channel = (generation_map.get("channels") or {}).get(str(serial)) or {}
    return (channel.get("days") or {}).get(date_mmdd) or {}


def build_upload_options(task: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    upload_options = {
        "visibility": str(defaults.get("visibility") or "public").strip() or "public",
        "made_for_kids": bool(defaults.get("made_for_kids", False)),
        "altered_content": bool(defaults.get("altered_content", True)),
        "category": str(defaults.get("category") or "Music").strip() or "Music",
        "scheduled_publish_at": str(defaults.get("scheduled_publish_at") or "").strip() or None,
    }
    for key in ("visibility", "made_for_kids", "altered_content", "category", "scheduled_publish_at"):
        if key in task:
            upload_options[key] = task[key]
    return upload_options


def make_cover_fallbacks(source_image: Path, tag_dir: Path, date_mmdd: str, serial: int, count: int) -> list[Path]:
    covers: list[Path] = []
    for index in range(1, count + 1):
        target = tag_dir / f"{date_mmdd}_{serial}_cover_{index:02d}{source_image.suffix.lower()}"
        shutil.copy2(source_image, target)
        covers.append(target)
    return covers


def render_for_plan(
    *,
    cfg: dict[str, Any],
    window_plan: dict[str, Any],
    date_mmdd: str,
    metadata_mode: str,
    fill_text: bool,
    fill_thumbnails: bool,
    sync_daily_content: bool,
    randomize_effects: bool,
) -> tuple[list[str], list[int]]:
    output_root = Path(cfg["output_root"])
    base_image_root = Path(cfg["base_image_dir"])
    used_media_root = Path(cfg.get("used_media_root") or (output_root / "_used_media"))
    retention_days = int(cfg.get("render_cleanup_days", 5))
    cleanup_old_uploaded_videos(output_root, retention_days)

    tasks: list[dict[str, Any]] = [dict(item) for item in (window_plan.get("tasks") or []) if isinstance(item, dict)]
    channel_name_map = load_channel_name_map(CHANNEL_MAPPING_FILE)
    defaults = dict(window_plan.get("default_upload_options") or {})
    tasks_by_tag: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        tag = str(task.get("tag") or "").strip()
        if tag:
            tasks_by_tag[tag].append(task)

    render_options = RenderOptions()
    render_options.fx_randomize = bool(randomize_effects)
    used_tags: list[str] = []

    for tag, tag_tasks in tasks_by_tag.items():
        source_dir = pick_source_binding(cfg, tag)
        image_pool = list_direct_files(source_dir, IMAGE_EXTENSIONS)
        audio_pool = list_direct_files(source_dir, AUDIO_EXTENSIONS)
        if not image_pool:
            raise ValueError(f"{tag} 的素材文件夹里没有图片: {source_dir}")
        if not audio_pool:
            raise ValueError(f"{tag} 的素材文件夹里没有音频: {source_dir}")

        random.shuffle(image_pool)
        random.shuffle(audio_pool)
        usable_count = min(len(tag_tasks), len(image_pool), len(audio_pool))
        if usable_count <= 0:
            raise ValueError(f"{tag} 没有可执行的素材组合")
        if len(tag_tasks) > usable_count:
            print(f"⚠️ {tag}: 窗口 {len(tag_tasks)} 个，但图片/音频只够做 {usable_count} 个视频")

        output_dir = output_root / f"{date_mmdd}_{tag}"
        output_dir.mkdir(parents=True, exist_ok=True)
        tag_base_dir = base_image_root / tag
        tag_base_dir.mkdir(parents=True, exist_ok=True)
        generation_map_path = tag_base_dir / "generation_map.json"
        manifest_path = output_dir / "upload_manifest.json"
        manifest = {
            "date": date_mmdd,
            "tag": tag,
            "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "bound_media_folder",
            "channels": {},
        }

        for index in range(usable_count):
            task = tag_tasks[index]
            serial = int(task.get("serial") or 0)
            if serial <= 0:
                continue
            source_image = image_pool[index]
            source_audio = audio_pool[index]
            tag_info = get_tag_info(tag) or {}
            is_ypp = bool(task.get("is_ypp", serial in set(tag_info.get("ypp_serials", []))))
            output_video = output_dir / f"{date_mmdd}_{serial}.mp4"
            clean_incomplete(output_video)
            output_video.unlink(missing_ok=True)
            Path(str(output_video) + ".done").unlink(missing_ok=True)

            duration = get_audio_duration(source_audio)
            effect_kwargs = build_effect_kwargs(render_options)
            filter_str, effect_desc, extra_inputs = get_effect(duration, **effect_kwargs)
            print(f"🎬 {tag}/{serial}: {source_image.name} + {source_audio.name} -> {output_video.name}")
            render_result = render_video_task(tag, source_image, source_audio, output_video, filter_str, extra_inputs=extra_inputs)
            if not render_result.get("success"):
                raise RuntimeError(f"{tag}/{serial} 渲染失败: {render_result.get('error')}")

            title = str(task.get("title") or "").strip()
            description = ""
            tag_list: list[str] = []
            ab_titles: list[str] = [str(item).strip() for item in task.get("ab_titles", []) if str(item).strip()]
            thumbnail_prompts: list[str] = []
            cover_paths: list[Path] = []

            bundle = None
            if metadata_mode == "prompt_api" and (fill_text or fill_thumbnails):
                bundle = generate_content_bundle(PROMPT_STUDIO_FILE, tag, is_ypp=is_ypp)
                thumbnail_prompts = [str(item.get("prompt") or "").strip() for item in bundle.get("thumbnail_prompts", []) if str(item.get("prompt") or "").strip()]

            if fill_text:
                if bundle:
                    if not title:
                        title = str((bundle.get("titles") or [output_video.stem])[0]).strip() or output_video.stem
                    description = str((bundle.get("descriptions") or [""])[0]).strip()
                    tag_list = [str(item).strip() for item in bundle.get("tag_list", []) if str(item).strip()]
                    if not ab_titles and is_ypp:
                        ab_titles = [str(item).strip() for item in (bundle.get("titles") or [])[:3] if str(item).strip()]
                else:
                    legacy = load_legacy_daily_entry(tag_base_dir, date_mmdd, serial)
                    if not title:
                        title = str(legacy.get("title") or output_video.stem).strip() or output_video.stem
                    description = str(legacy.get("description") or "").strip()
                    tag_list = [str(item).strip() for item in legacy.get("tag_list", []) if str(item).strip()]
                    if not ab_titles and is_ypp:
                        ab_titles = [str(item).strip() for item in legacy.get("ab_titles", []) if str(item).strip()]
            elif not title:
                title = output_video.stem

            wanted_thumb_count = 3 if is_ypp else 1
            if fill_thumbnails:
                explicit = [Path(item) for item in task.get("thumbnails", []) if Path(item).exists()]
                if explicit:
                    cover_paths.extend(explicit[:wanted_thumb_count])
                elif bundle and str(bundle["api_preset"].get("autoImageEnabled") or "0") == "1":
                    for thumb_index, prompt in enumerate(thumbnail_prompts[:wanted_thumb_count], 1):
                        target = tag_base_dir / f"{date_mmdd}_{serial}_cover_{thumb_index:02d}.png"
                        try:
                            image_result = call_image_model(bundle["api_preset"], prompt)
                            if image_result.get("data_url"):
                                cover_paths.append(save_data_url_image(image_result["data_url"], target))
                        except Exception as exc:
                            print(f"⚠️ {tag}/{serial} 缩略图 {thumb_index} 自动生成失败: {exc}")
                if not cover_paths:
                    cover_paths.extend(make_cover_fallbacks(source_image, tag_base_dir, date_mmdd, serial, wanted_thumb_count))

            cover_names = [path.name for path in cover_paths]
            if sync_daily_content:
                save_generation_entry(
                    generation_map_path,
                    date_mmdd=date_mmdd,
                    serial=serial,
                    is_ypp=is_ypp,
                    title=title,
                    description=description,
                    covers=cover_names,
                    ab_titles=ab_titles,
                )

            upload_options = build_upload_options(task, defaults)
            manifest["channels"][str(serial)] = {
                "video": output_video.name,
                "source_image": str(source_image),
                "source_audio": str(source_audio),
                "effect_desc": effect_desc,
                "channel_name": channel_name_map.get(serial, ""),
                "title": title,
                "description": description,
                "thumbnails": [str(path) for path in cover_paths],
                "thumbnail_prompts": thumbnail_prompts,
                "tag_list": tag_list,
                "is_ypp": bool(is_ypp),
                "ab_titles": ab_titles,
                "set": 1,
                "upload_options": upload_options,
            }

            move_to_used(source_image, used_media_root, tag=tag, kind="images")
            move_to_used(source_audio, used_media_root, tag=tag, kind="audio")

        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        if manifest["channels"]:
            used_tags.append(tag)
            print(f"📦 {tag}: 已准备 {len(manifest['channels'])} 个视频 / manifest={manifest_path.name}")

    tags, skip_channels = derive_tags_and_skip_channels(window_plan, lambda current_tag: get_tag_info(current_tag) or {})
    final_tags = [tag for tag in tags if tag in used_tags]
    return final_tags, skip_channels


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="按分组素材文件夹直接生成视频并上传")
    parser.add_argument("--date", required=True, help="日期，支持 0314 或 3.14")
    parser.add_argument("--window-plan-file", required=True, help="窗口任务计划 JSON")
    parser.add_argument("--metadata-mode", default="prompt_api", choices=["prompt_api", "daily_content"])
    parser.add_argument("--render-only", action="store_true")
    parser.add_argument("--skip-fill-text", action="store_true")
    parser.add_argument("--skip-fill-thumbnails", action="store_true")
    parser.add_argument("--no-sync-daily-content", action="store_true")
    parser.add_argument("--auto-close-browser", action="store_true")
    parser.add_argument("--randomize-effects", action="store_true")
    parser.add_argument("--retain-video-days", type=int, default=-1, help="上传成功后保留视频天数；0 表示沿用旧逻辑立即删除")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    cfg = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
    date_mmdd = normalize_mmdd(args.date)
    window_plan_file = Path(args.window_plan_file)
    window_plan = load_window_plan(window_plan_file)

    fill_text = not args.skip_fill_text
    fill_thumbnails = not args.skip_fill_thumbnails
    sync_daily_content = not args.no_sync_daily_content
    randomize_effects = bool(args.randomize_effects)

    try:
        tags, skip_channels = render_for_plan(
            cfg=cfg,
            window_plan=window_plan,
            date_mmdd=date_mmdd,
            metadata_mode=args.metadata_mode,
            fill_text=fill_text,
            fill_thumbnails=fill_thumbnails,
            sync_daily_content=sync_daily_content,
            randomize_effects=randomize_effects,
        )
    except Exception as exc:
        print(f"❌ 直出工作流失败: {exc}")
        return 1

    if not tags:
        print("❌ 没有生成任何可上传视频")
        return 1

    if args.render_only:
        print("✅ 已完成渲染与 manifest 生成（当前为 render-only，不继续上传）")
        return 0

    retain_days = args.retain_video_days if args.retain_video_days >= 0 else int(cfg.get("render_cleanup_days", 5))
    upload_cmd = [
        sys.executable,
        str(UPLOAD_SCRIPT),
        "--tag",
        ",".join(tags),
        "--date",
        date_mmdd,
        "--auto-confirm",
        "--window-plan-file",
        str(window_plan_file),
        "--retain-video-days",
        str(retain_days),
    ]
    if skip_channels:
        upload_cmd.append("--skip-channels=" + ",".join(str(item) for item in skip_channels))
    if args.auto_close_browser:
        upload_cmd.append("--auto-close-browser")

    print(f"🚀 开始上传: {', '.join(tags)}")
    completed = subprocess.run(upload_cmd, cwd=str(SCRIPT_DIR))
    return int(completed.returncode or 0)


if __name__ == "__main__":
    raise SystemExit(main())
