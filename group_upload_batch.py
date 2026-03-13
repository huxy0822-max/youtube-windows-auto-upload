#!/usr/bin/env python3
"""
分组批量上传入口：
1. 扫描一个现成视频文件夹；
2. 把视频按顺序映射到同分组频道；
3. 生成 manifest / generation_map；
4. 调用现有 batch_upload.py 完成上传。
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from batch_upload import batch_upload
from group_upload_workflow import (
    load_upload_batch_settings,
    parse_serials_text,
    prepare_group_upload_batch,
    prepare_window_task_upload_batch,
)
from upload_window_planner import derive_tags_and_skip_channels, load_window_upload_plan
from utils import get_tag_info


SCRIPT_DIR = Path(__file__).parent
SCHEDULER_CONFIG_FILE = SCRIPT_DIR / "scheduler_config.json"
PROMPT_STUDIO_FILE = SCRIPT_DIR / "config" / "prompt_studio.json"
CHANNEL_MAPPING_FILE = SCRIPT_DIR / "config" / "channel_mapping.json"
UPLOAD_BATCH_SETTINGS_FILE = SCRIPT_DIR / "config" / "upload_batch_settings.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同分组频道的分组批量上传")
    parser.add_argument("--tag", required=True, help="上传分组/tag，例如 面壁者")
    parser.add_argument("--date", required=True, help="日期，例如 0312 或 3.12")
    parser.add_argument("--source-dir", required=True, help="现成视频所在文件夹")
    parser.add_argument("--thumb-dir", default="", help="可选：现成缩略图文件夹")
    parser.add_argument("--serials", default="", help="可选：限定频道序号，逗号分隔")
    parser.add_argument("--window-plan-file", default="", help="可选：窗口任务计划文件，启用后可跨分组/跨窗口批量上传")
    parser.add_argument("--generation-mode", default="", choices=["", "site_api", "legacy"], help="标题/简介/缩略图来源")
    parser.add_argument("--metadata-mode", default="", choices=["", "prompt_api", "daily_content"], help="统一上传页的文案来源")
    parser.add_argument("--fill-text", action="store_true", help="为现成视频自动生成/填写标题、简介、标签")
    parser.add_argument("--no-fill-text", action="store_true", help="不自动生成/填写标题、简介、标签")
    parser.add_argument("--fill-thumbnails", action="store_true", help="为现成视频自动处理缩略图")
    parser.add_argument("--no-fill-thumbnails", action="store_true", help="不自动处理缩略图")
    parser.add_argument("--sync-daily-content", action="store_true", help="把上传前准备的内容写回 generation_map")
    parser.add_argument("--no-sync-daily-content", action="store_true", help="不写回 generation_map")
    parser.add_argument("--visibility", default="", choices=["", "public", "private", "unlisted", "schedule"], help="发布可见性")
    parser.add_argument("--category", default="", help="YouTube 分类，默认 Music")
    parser.add_argument("--schedule-start", default="", help="定时发布时间起点，格式 YYYY-MM-DD HH:MM")
    parser.add_argument("--schedule-interval-minutes", type=int, default=-1, help="定时发布间隔（分钟）")
    parser.add_argument("--made-for-kids", action="store_true", help="设为儿童内容")
    parser.add_argument("--not-made-for-kids", action="store_true", help="设为非儿童内容")
    parser.add_argument("--altered-content-yes", action="store_true", help="AI/合成内容=Yes")
    parser.add_argument("--altered-content-no", action="store_true", help="AI/合成内容=No")
    parser.add_argument("--prepare-only", action="store_true", help="只准备 manifest，不实际上传")
    parser.add_argument("--dry-run", action="store_true", help="准备后调用 batch_upload 的 dry-run 预览")
    parser.add_argument("--auto-confirm", action="store_true", help="跳过上传确认")
    parser.add_argument("--auto-close-browser", action="store_true", help="每个频道上传后自动关闭浏览器")
    return parser


def resolve_args(args: argparse.Namespace) -> dict:
    defaults = load_upload_batch_settings(UPLOAD_BATCH_SETTINGS_FILE)

    if args.not_made_for_kids:
        made_for_kids = False
    elif args.made_for_kids:
        made_for_kids = True
    else:
        made_for_kids = bool(defaults.get("made_for_kids", False))

    if args.altered_content_yes:
        altered_content = True
    elif args.altered_content_no:
        altered_content = False
    else:
        altered_content = bool(defaults.get("altered_content", True))

    visibility = args.visibility or str(defaults.get("visibility", "public"))
    schedule_start = args.schedule_start or str(defaults.get("schedule_start", ""))
    schedule_enabled = visibility == "schedule" or bool(defaults.get("schedule_enabled", False) and schedule_start)
    if visibility != "schedule":
        schedule_enabled = False

    return {
        "generation_mode": args.generation_mode or str(defaults.get("generation_mode", "site_api")),
        "visibility": visibility or "public",
        "category": args.category or str(defaults.get("category", "Music")),
        "schedule_enabled": schedule_enabled,
        "schedule_start": schedule_start,
        "schedule_interval_minutes": args.schedule_interval_minutes if args.schedule_interval_minutes >= 0 else int(defaults.get("schedule_interval_minutes", 60)),
        "made_for_kids": made_for_kids,
        "altered_content": altered_content,
    }


async def main_async(args: argparse.Namespace) -> int:
    resolved = resolve_args(args)
    window_plan = load_window_upload_plan(args.window_plan_file)

    if args.no_fill_text:
        fill_text = False
    elif args.fill_text:
        fill_text = True
    else:
        fill_text = True

    if args.no_fill_thumbnails:
        fill_thumbnails = False
    elif args.fill_thumbnails:
        fill_thumbnails = True
    else:
        fill_thumbnails = True

    if args.no_sync_daily_content:
        sync_daily_content = False
    elif args.sync_daily_content:
        sync_daily_content = True
    else:
        sync_daily_content = True

    metadata_mode = args.metadata_mode or ("prompt_api" if resolved["generation_mode"] == "site_api" else "daily_content")

    if window_plan:
        prepared = prepare_window_task_upload_batch(
            script_dir=SCRIPT_DIR,
            scheduler_config_path=SCHEDULER_CONFIG_FILE,
            prompt_studio_path=PROMPT_STUDIO_FILE,
            channel_mapping_path=CHANNEL_MAPPING_FILE,
            window_plan=window_plan,
            date_value=args.date,
            source_video_dir=Path(args.source_dir),
            thumbnail_dir=Path(args.thumb_dir) if args.thumb_dir else None,
            metadata_mode=metadata_mode,
            fill_title_desc_tags=fill_text,
            fill_thumbnails=fill_thumbnails,
            sync_daily_content=sync_daily_content,
        )
        tags, skip_channels = derive_tags_and_skip_channels(window_plan, lambda tag: get_tag_info(tag) or {})
    else:
        prepared = prepare_group_upload_batch(
            script_dir=SCRIPT_DIR,
            scheduler_config_path=SCHEDULER_CONFIG_FILE,
            prompt_studio_path=PROMPT_STUDIO_FILE,
            channel_mapping_path=CHANNEL_MAPPING_FILE,
            tag=args.tag,
            date_value=args.date,
            source_video_dir=Path(args.source_dir),
            thumbnail_dir=Path(args.thumb_dir) if args.thumb_dir else None,
            selected_serials=parse_serials_text(args.serials),
            generation_mode=resolved["generation_mode"],
            visibility=resolved["visibility"],
            category=resolved["category"],
            made_for_kids=resolved["made_for_kids"],
            altered_content=resolved["altered_content"],
            schedule_enabled=resolved["schedule_enabled"],
            schedule_start=resolved["schedule_start"],
            schedule_interval_minutes=resolved["schedule_interval_minutes"],
        )
        tags = [args.tag]
        skip_channels = prepared["skipped_serials"]

    print("=" * 60)
    print("分组批量上传准备完成")
    print(f"tags={','.join(tags)} date={prepared['date']}")
    if window_plan:
        print(f"manifests={prepared['manifest_paths']}")
    else:
        print(f"manifest={prepared['manifest_path']}")
    print(f"assigned={prepared['assigned_count']}")
    if prepared["preview_lines"]:
        print("\n".join(prepared["preview_lines"]))
    if prepared["warnings"]:
        print("\n警告:")
        for item in prepared["warnings"]:
            print(f"- {item}")
    print("=" * 60)

    if args.prepare_only:
        return 0

    result = await batch_upload(
        tag=",".join(tags),
        date=args.date,
        dry_run=args.dry_run,
        auto_confirm=args.auto_confirm,
        auto_close_browser=args.auto_close_browser,
        skip_channels=skip_channels,
        window_plan=window_plan,
    )
    return 0 if int(result.get("failed_count", 0)) == 0 else 1


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
