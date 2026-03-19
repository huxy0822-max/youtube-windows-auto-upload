#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from playwright.async_api import async_playwright

from batch_upload import (
    UPLOAD_RECORDS_DIR,
    click_visible_upload_dialog_button,
    ensure_public_visibility_selected,
    get_best_upload_monitor_snapshot,
    get_visible_upload_dialog_button_state,
    handle_publish_anyway_dialog,
    human_click,
    is_safe_to_close_after_publish,
    log,
    select_best_upload_page,
    stop_browser,
    summarize_upload_monitor,
    try_click_publish_button,
)

PUBLISH_PATTERN = "done|save|publish|schedule|完成|保存|yayınla|發佈|发布|公開|定時|定时|排程"


async def _dialog_open(page) -> bool:
    try:
        save_btn = page.locator("ytcp-button#save-button, #save-button").first
        if await save_btn.count() > 0 and await save_btn.is_visible():
            return True
    except Exception:
        pass
    try:
        public_radio = page.locator("tp-yt-paper-radio-button[name='PUBLIC']").first
        if await public_radio.count() > 0 and await public_radio.is_visible():
            return True
    except Exception:
        pass
    return False


async def maybe_click_edit_page_publish(page, *, serial: int) -> bool:
    current_url = str(getattr(page, "url", "") or "")
    if "/video/" not in current_url:
        return False

    try:
        visibility_button = page.locator("ytcp-video-metadata-visibility #select-button").first
        if not (await visibility_button.count() > 0 and await visibility_button.is_visible()):
            return False
    except Exception:
        return False

    dialog_opened = await _dialog_open(page)
    if not dialog_opened:
        clicked = await human_click(page, visibility_button, "tail-watcher 编辑页公开范围")
        if not clicked:
            try:
                await visibility_button.click(force=True, timeout=5000)
            except Exception:
                return False
        await asyncio.sleep(1.0)
        dialog_opened = await _dialog_open(page)
    if not dialog_opened:
        return False

    await ensure_public_visibility_selected(page)

    publish_state = await get_visible_upload_dialog_button_state(
        page,
        button_id="save-button",
        text_pattern=PUBLISH_PATTERN,
    )
    if not publish_state.get("found") or publish_state.get("disabled"):
        return False

    clicked = await click_visible_upload_dialog_button(
        page,
        "Tail watcher Done / Publish / Schedule",
        button_id="save-button",
        text_pattern=PUBLISH_PATTERN,
    )
    if not clicked:
        clicked = await try_click_publish_button(page)
    if clicked:
        log(f"序号 {serial}: 尾程 watcher 已点击编辑页最终提交按钮", "OK")
        await asyncio.sleep(2)
        await handle_publish_anyway_dialog(page, serial=serial, max_wait_seconds=15, poll_seconds=1)
    return clicked


async def maybe_click_upload_dialog_publish(page, *, serial: int) -> bool:
    publish_state = await get_visible_upload_dialog_button_state(
        page,
        button_id="done-button",
        text_pattern=PUBLISH_PATTERN,
    )
    if publish_state.get("found") and not publish_state.get("disabled"):
        clicked = await click_visible_upload_dialog_button(
            page,
            "Tail watcher Done / Publish / Schedule",
            button_id="done-button",
            text_pattern=PUBLISH_PATTERN,
        )
        if not clicked:
            clicked = await try_click_publish_button(page)
        if clicked:
            log(f"序号 {serial}: 尾程 watcher 已点击上传弹窗最终提交按钮", "OK")
            await asyncio.sleep(2)
            await handle_publish_anyway_dialog(page, serial=serial, max_wait_seconds=15, poll_seconds=1)
        return clicked

    publish_state = await get_visible_upload_dialog_button_state(
        page,
        button_id="save-button",
        text_pattern=PUBLISH_PATTERN,
    )
    if publish_state.get("found") and not publish_state.get("disabled"):
        clicked = await click_visible_upload_dialog_button(
            page,
            "Tail watcher Save / Publish / Schedule",
            button_id="save-button",
            text_pattern=PUBLISH_PATTERN,
        )
        if not clicked:
            clicked = await try_click_publish_button(page)
        if clicked:
            log(f"序号 {serial}: 尾程 watcher 已点击 save-button 最终提交按钮", "OK")
            await asyncio.sleep(2)
            await handle_publish_anyway_dialog(page, serial=serial, max_wait_seconds=15, poll_seconds=1)
        return clicked

    return False


async def run_tail_watcher(*, serial: int, container_code: str, port: int, timeout_seconds: int) -> int:
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
        context = browser.contexts[0]
        deadline = time.monotonic() + max(30, timeout_seconds)
        last_summary = None
        poll_count = 0

        while time.monotonic() < deadline:
            poll_count += 1
            page = await select_best_upload_page(context, log_selection=False)

            await handle_publish_anyway_dialog(page, serial=serial, max_wait_seconds=5, poll_seconds=0.5)
            snapshot = await get_best_upload_monitor_snapshot(page, context=context)
            summary = summarize_upload_monitor(snapshot)
            safe = is_safe_to_close_after_publish(snapshot)

            if summary != last_summary or poll_count == 1 or poll_count % 6 == 0:
                log(
                    f"序号 {serial}: 尾程 watcher#{poll_count} {summary}",
                    "OK" if safe else "WAIT",
                )
                last_summary = summary

            if safe:
                stop_browser(container_code)
                log(f"序号 {serial}: 尾程 watcher 已确认安全关闭并关闭浏览器", "OK")
                return 0

            clicked = await maybe_click_edit_page_publish(page, serial=serial)
            if not clicked:
                await maybe_click_upload_dialog_publish(page, serial=serial)

            await asyncio.sleep(10)

        log(f"序号 {serial}: 尾程 watcher 超时，保留浏览器现场供后续检查", "WARN")
        return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch Studio upload tail state and close browser safely.")
    parser.add_argument("--serial", type=int, required=True)
    parser.add_argument("--container-code", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=6 * 60 * 60)
    parser.add_argument("--tag")
    parser.add_argument("--date")
    return parser.parse_args()


def mark_upload_record_success(*, tag: str | None, date_mmdd: str | None, serial: int) -> None:
    clean_tag = str(tag or "").strip()
    clean_date = str(date_mmdd or "").strip()
    if not clean_tag or not clean_date:
        return

    record_file = UPLOAD_RECORDS_DIR / clean_date / clean_tag / f"channel_{serial}.json"
    if not record_file.exists():
        return

    try:
        record = json.loads(record_file.read_text(encoding="utf-8"))
    except Exception:
        return

    record["success"] = True
    record["watcher_confirmed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    record_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    try:
        result = asyncio.run(
            run_tail_watcher(
                serial=args.serial,
                container_code=args.container_code,
                port=args.port,
                timeout_seconds=args.timeout_seconds,
            )
        )
        if result == 0:
            mark_upload_record_success(tag=args.tag, date_mmdd=args.date, serial=args.serial)
        return result
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        log(f"尾程 watcher 异常: {exc}", "ERR")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
