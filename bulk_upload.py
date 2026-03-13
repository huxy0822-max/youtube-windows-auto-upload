#!/usr/bin/env python3
"""
多赛道批量上传入口。

作用：
1. 按多个 tag 依次调用 `batch_upload.py` 的核心协程。
2. 给统一控制台提供一个稳定的“批量上传”脚本入口。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import List

from batch_upload import batch_upload, parse_serial_list


def parse_tags(raw: str) -> List[str]:
    tags: list[str] = []
    seen = set()
    for chunk in str(raw or "").replace("，", ",").replace("；", ",").replace(";", ",").split(","):
        tag = chunk.strip()
        if tag and tag not in seen:
            tags.append(tag)
            seen.add(tag)
    return tags


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="多赛道批量上传")
    parser.add_argument("--date", required=True, help="视频日期，如 0312 或 3.12")
    parser.add_argument("--tags", required=True, help="多个 tag，逗号分隔，如 面壁者,芝加哥蓝调")
    parser.add_argument("--dry-run", action="store_true", help="只预览不执行")
    parser.add_argument("--auto-confirm", action="store_true", help="免交互确认")
    parser.add_argument("--auto-close-browser", action="store_true", help="每个频道上传后自动关闭浏览器")
    parser.add_argument("--skip-channels", default="", help="跳过指定频道序号，逗号分隔")
    parser.add_argument("--max-open-windows", type=int, default=10, help="成功发布后保留的最大窗口数")
    parser.add_argument("--window-ttl-hours", type=float, default=2.0, help="成功发布后窗口保留时长（小时）")
    parser.add_argument("--continue-on-error", action="store_true", help="单个 tag 失败后继续后续 tag")
    return parser


async def run_bulk_upload(args: argparse.Namespace) -> int:
    tags = parse_tags(args.tags)
    if not tags:
        print("❌ 未提供有效 tag")
        return 1

    print("=" * 60)
    print("   YouTube 多赛道批量上传")
    print("=" * 60)
    print(f"日期: {args.date}")
    print(f"标签: {', '.join(tags)}")
    print()

    failed_tags: list[str] = []
    total_success = 0
    total_failed = 0
    total_pending = 0

    for index, tag in enumerate(tags, 1):
        print(f"\n{'=' * 60}")
        print(f"🚀 [{index}/{len(tags)}] 开始上传: {tag}")
        print(f"{'=' * 60}")
        try:
            result = await batch_upload(
                tag=tag,
                date=args.date,
                dry_run=args.dry_run,
                auto_confirm=args.auto_confirm,
                auto_close_browser=args.auto_close_browser,
                skip_channels=parse_serial_list(args.skip_channels),
                max_open_windows=args.max_open_windows,
                window_ttl_hours=args.window_ttl_hours,
            )
            success_count = int(result.get("success_count", 0))
            failed_count = int(result.get("failed_count", 0))
            pending_count = int(result.get("pending_count", 0))
            total_success += success_count
            total_failed += failed_count
            total_pending += pending_count

            if failed_count > 0:
                failed_tags.append(tag)
                print(f"⚠️ {tag} 完成，但有失败频道: failed={failed_count}")
                if not args.continue_on_error:
                    break
            else:
                print(f"✅ {tag} 完成: success={success_count}")
        except Exception as exc:
            failed_tags.append(tag)
            total_failed += 1
            print(f"❌ {tag} 异常: {exc}")
            if not args.continue_on_error:
                break

    print(f"\n{'=' * 60}")
    print("批量上传结束")
    print(f"success_count={total_success}")
    print(f"failed_count={total_failed}")
    print(f"pending_count={total_pending}")
    if failed_tags:
        print(f"failed_tags={', '.join(failed_tags)}")
    print(f"{'=' * 60}")

    return 1 if failed_tags or total_failed > 0 else 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = asyncio.run(run_bulk_upload(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
