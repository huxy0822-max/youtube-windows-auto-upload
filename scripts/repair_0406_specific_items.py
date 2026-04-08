# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import shutil
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from playwright.async_api import async_playwright

from batch_upload import (
    execute_group_job,
    human_click,
    human_fill,
    set_altered_content_setting,
    set_made_for_kids_setting,
    set_video_category,
)
from browser_api import close_browser_env, open_browser_env
from path_templates import build_runtime_config, get_path_template, load_path_templates
from run_queue import GroupJob
from scripts.recover_global_film_0406 import (
    DATE_MMDD,
    GROUP_TAG,
    ROOT_DIR,
    SNAPSHOT_FILE,
    STATE_FILE,
    FileLogger,
    _build_defaults,
    _build_task,
    _build_upload_job,
    _load_json,
    _upload_progress_logger,
)
from workflow_core import execute_metadata_only_workflow, load_scheduler_settings

LOG_DIR = BASE_DIR / "logs"
REPORT_DIR = BASE_DIR / "data" / "repair_reports"

SERIAL_1 = 1
SERIAL_28 = 28
SLOT_01 = 1
VIDEO_1_ID = "kQFx8UdH-Uw"
ARCHIVED_VIDEO_1 = Path(r"F:\已用视频\2026-04-07\0322_1_01.mp4")
SOURCE_IMAGE_1 = ROOT_DIR / "asset_2ohckovl9_1775405600106.png"
SOURCE_AUDIO_1 = ROOT_DIR / "output_04.mp3"
USED_MATERIALS_ROOT = Path(r"F:\已用素材")


def _task_dir(serial: int, slot_index: int) -> Path:
    return ROOT_DIR / f"{DATE_MMDD}_{int(serial)}_{int(slot_index):02d}"


def _find_snapshot_row(serial: int, slot_index: int) -> dict[str, Any]:
    snapshot = _load_json(SNAPSHOT_FILE)
    for row in snapshot.get("tasks", []):
        if int(row.get("serial") or 0) == int(serial) and int(row.get("slot_index") or 0) == int(slot_index):
            return dict(row)
    raise RuntimeError(f"snapshot 中缺少 serial={serial} slot={slot_index} 的任务")


def _load_base_job_and_defaults(logger: FileLogger) -> tuple[dict[str, Any], GroupJob, dict[str, Any], Any]:
    state = _load_json(STATE_FILE)
    queue_jobs = ((state.get("run_queue") or {}).get("jobs") or [])
    base_job: GroupJob | None = None
    for payload in queue_jobs:
        try:
            candidate = GroupJob.from_dict(payload)
        except Exception:
            continue
        if str(candidate.group_tag or "").strip() == GROUP_TAG:
            base_job = candidate
            break
    if base_job is None:
        logger("[Plan] dashboard_state 中未找到全球电影队列，改用最小回退任务配置")
        base_job = GroupJob(
            group_tag=GROUP_TAG,
            window_serials=[SERIAL_1, SERIAL_28],
            source_dir=str(ROOT_DIR),
            prompt_template="default",
            api_template="default",
            path_template="默认路径",
        )
    templates = load_path_templates()
    template_name, template_payload = get_path_template(base_job.path_template, templates=templates)
    runtime_config = build_runtime_config(
        load_scheduler_settings(),
        template_payload,
        template_name=template_name,
        source_dir=str(base_job.source_dir or ROOT_DIR),
    )
    runtime_config["output_root"] = str(ROOT_DIR)
    runtime_config["metadata_root"] = str(ROOT_DIR)
    runtime_config["music_dir"] = str(ROOT_DIR)
    runtime_config["base_image_dir"] = str(ROOT_DIR)
    defaults = _build_defaults(state, base_job)
    return state, base_job, runtime_config, defaults


def _copy_file(src: Path, dst: Path, logger: FileLogger) -> None:
    if not src.exists():
        raise RuntimeError(f"缺少源文件: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists() or src.stat().st_size != dst.stat().st_size:
        shutil.copy2(str(src), str(dst))
        logger(f"[Copy] {src.name} -> {dst}")


def _resolve_material_path(preferred: Path, logger: FileLogger) -> Path:
    if preferred.exists():
        return preferred
    if USED_MATERIALS_ROOT.exists():
        matches = list(USED_MATERIALS_ROOT.rglob(preferred.name))
        if matches:
            chosen = sorted(matches, key=lambda item: str(item))[-1]
            logger(f"[Lookup] {preferred.name} 改从归档素材定位: {chosen}")
            return chosen
    raise RuntimeError(f"缺少源文件: {preferred}")


def _prepare_slot_1_workdir(logger: FileLogger) -> Path:
    folder = _task_dir(SERIAL_1, SLOT_01)
    folder.mkdir(parents=True, exist_ok=True)
    _copy_file(ARCHIVED_VIDEO_1, folder / ARCHIVED_VIDEO_1.name, logger)
    _copy_file(_resolve_material_path(SOURCE_IMAGE_1, logger), folder / SOURCE_IMAGE_1.name, logger)
    _copy_file(_resolve_material_path(SOURCE_AUDIO_1, logger), folder / SOURCE_AUDIO_1.name, logger)

    for pattern in (
        "upload_manifest.json",
        "generation_map.json",
        f"{DATE_MMDD}_{SERIAL_1}_cover_*",
        f"{DATE_MMDD}_{SERIAL_1}_thumbnail*",
        "*cover_seed.png",
    ):
        for candidate in folder.glob(pattern):
            if candidate.is_file():
                candidate.unlink()
                logger(f"[Cleanup] 删除旧文件: {candidate}")
    return folder


def _load_slot_manifest(folder: Path, serial: int) -> dict[str, Any]:
    manifest_path = folder / "upload_manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(f"缺少 manifest: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    channel = (payload.get("channels") or {}).get(str(int(serial)))
    if not isinstance(channel, dict):
        raise RuntimeError(f"manifest 未包含 serial={serial} 的 channel")
    return channel


def _metadata_payload_ready(channel: dict[str, Any]) -> bool:
    title = str(channel.get("title") or "").strip()
    description = str(channel.get("description") or "").strip()
    thumbnails = [str(item).strip() for item in channel.get("thumbnails", []) if str(item).strip()]
    return bool(title and description and thumbnails)


def regenerate_metadata_for_1(logger: FileLogger) -> dict[str, Any]:
    _, base_job, runtime_config, defaults = _load_base_job_and_defaults(logger)
    row = _find_snapshot_row(SERIAL_1, SLOT_01)
    folder = _prepare_slot_1_workdir(logger)
    task = _build_task(row, existing_done=True)
    task.source_dir = str(folder)
    task.thumbnails = []
    manifest_payload: dict[str, Any] | None = None

    for attempt in range(1, 4):
        logger(f"[Repair-1] metadata attempt {attempt}/3")
        execute_metadata_only_workflow(
            tasks=[task],
            defaults=defaults,
            config=runtime_config,
            output_dir_overrides={GROUP_TAG: str(ROOT_DIR)},
            metadata_dir_overrides={GROUP_TAG: str(ROOT_DIR)},
            log=logger,
        )
        channel = _load_slot_manifest(folder, SERIAL_1)
        if _metadata_payload_ready(channel):
            manifest_payload = channel
            logger(
                f"[Repair-1] metadata ready | title={str(channel.get('title') or '')[:80]} | "
                f"covers={len(channel.get('thumbnails') or [])}"
            )
            break
        logger("[Repair-1] metadata 尚未完整，准备重试")
        time.sleep(2)

    if manifest_payload is None:
        raise RuntimeError("0322_1_01 文案/封面重生失败，3 次尝试后仍不完整")

    return {
        "folder": str(folder),
        "channel": manifest_payload,
        "base_job": base_job.to_dict(),
    }


async def _wait_save_finished(page, logger: FileLogger, timeout_seconds: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            disabled = await page.evaluate(
                """
                () => {
                    const host = document.querySelector('ytcp-button#save');
                    if (!host) return false;
                    const inner = host.querySelector('button');
                    const hostDisabled = host.getAttribute('aria-disabled') === 'true' || host.hasAttribute('disabled');
                    const innerDisabled = inner ? (inner.getAttribute('aria-disabled') === 'true' || inner.disabled === true) : false;
                    return hostDisabled || innerDisabled;
                }
                """
            )
            if disabled:
                logger("[Repair-1] 保存按钮已回到禁用态，视为保存完成")
                return True
        except Exception:
            pass
        await asyncio.sleep(1.0)
    return False


async def edit_existing_video_1(payload: dict[str, Any], logger: FileLogger) -> dict[str, Any]:
    result = open_browser_env(str(SERIAL_1))
    data = result.get("data") or {}
    debug_port = data.get("http") or data.get("debugPort") or data.get("port")
    if not debug_port:
        raise RuntimeError(f"窗口 {SERIAL_1} 启动失败: {result}")

    title = str(payload.get("title") or "").strip()
    description = str(payload.get("description") or "").strip()
    thumbnails = [Path(item) for item in payload.get("thumbnails", []) if str(item).strip()]
    thumbnail_path = thumbnails[0] if thumbnails else None
    upload_options = payload.get("upload_options") if isinstance(payload.get("upload_options"), dict) else {}

    edit_url = f"https://studio.youtube.com/video/{VIDEO_1_ID}/edit"
    artifacts_dir = BASE_DIR / "output" / "playwright"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    screenshot_before = artifacts_dir / "repair_0322_1_01_before_save.png"
    screenshot_after = artifacts_dir / "repair_0322_1_01_after_save.png"

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}")
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = context.pages[0] if context.pages else await context.new_page()
        try:
            await page.goto(edit_url, wait_until="domcontentloaded", timeout=120000)
            await page.wait_for_selector("#title-textarea #textbox", timeout=60000)
            await page.wait_for_timeout(3000)
            await page.screenshot(path=str(screenshot_before), full_page=True)

            title_input = page.locator("#title-textarea #textbox").first
            description_input = page.locator("#description-textarea #textbox").first

            if not await human_fill(page, title_input, title[:100], "修复标题"):
                raise RuntimeError("编辑页标题填写失败")
            await page.wait_for_timeout(800)

            if not await human_fill(page, description_input, description[:5000], "修复简介"):
                raise RuntimeError("编辑页简介填写失败")
            await page.wait_for_timeout(1000)

            if thumbnail_path and thumbnail_path.exists():
                logger(f"[Repair-1] 上传缩略图: {thumbnail_path}")
                thumb_input = page.locator("input#file-loader").first
                await thumb_input.set_input_files(str(thumbnail_path))
                await page.wait_for_timeout(4000)
            else:
                logger("[Repair-1] 未找到可上传缩略图，跳过封面更新")

            toggle_button = page.locator("ytcp-button#toggle-button").first
            try:
                if await toggle_button.count() > 0 and await toggle_button.is_visible():
                    await human_click(page, toggle_button, "展开高级设置")
                    await page.wait_for_timeout(1500)
            except Exception:
                pass

            category = str(upload_options.get("category") or "Music").strip() or "Music"
            made_for_kids = bool(upload_options.get("made_for_kids", False))
            altered_content = bool(upload_options.get("altered_content", True))

            category_ok = await set_video_category(page, category)
            kids_ok = await set_made_for_kids_setting(page, made_for_kids)
            altered_ok = await set_altered_content_setting(page, altered_content)
            logger(
                f"[Repair-1] advanced verify | category={category_ok} kids={kids_ok} altered={altered_ok}"
            )

            save_button = page.locator("ytcp-button#save").first
            await save_button.wait_for(state="visible", timeout=30000)
            if not await human_click(page, save_button, "保存修复后的视频详情"):
                await save_button.click(force=True, timeout=5000)
            if not await _wait_save_finished(page, logger):
                raise RuntimeError("点击保存后，保存按钮长时间未回到禁用态")

            await page.wait_for_timeout(3000)
            await page.screenshot(path=str(screenshot_after), full_page=True)
            return {
                "success": True,
                "edit_url": edit_url,
                "title": title,
                "description_length": len(description),
                "thumbnail": str(thumbnail_path) if thumbnail_path else "",
                "screenshot_before": str(screenshot_before),
                "screenshot_after": str(screenshot_after),
            }
        finally:
            try:
                await browser.close()
            except Exception:
                pass
            close_browser_env(str(SERIAL_1))


async def reupload_28(logger: FileLogger) -> dict[str, Any]:
    _, base_job, _, _ = _load_base_job_and_defaults(logger)
    manifest_dir = _task_dir(SERIAL_28, SLOT_01)
    if not (manifest_dir / "upload_manifest.json").exists():
        raise RuntimeError(f"缺少待补传 manifest: {manifest_dir}")

    upload_job = _build_upload_job(base_job, SERIAL_28, manifest_dir)
    progress = _upload_progress_logger(logger)
    last_result: dict[str, Any] | None = None

    for attempt in range(1, 4):
        logger(f"[Repair-28] upload attempt {attempt}/3 | dir={manifest_dir}")
        result = await execute_group_job(
            upload_job,
            upload_job.upload_defaults,
            progress_callback=progress,
        )
        last_result = dict(result)
        nested_results = [item for item in (result.get("results") or []) if isinstance(item, dict)]
        if any(bool(item.get("success")) for item in nested_results):
            logger(f"[Repair-28] upload success on attempt {attempt}")
            return {
                "success": True,
                "attempts": attempt,
                "summary": result,
            }
        stages = [str(item.get("stage") or "") for item in nested_results]
        logger(f"[Repair-28] upload failed | stages={stages}")
        await asyncio.sleep(5)

    return {
        "success": False,
        "attempts": 3,
        "summary": last_result or {},
    }


async def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"repair_0406_specific_items_{timestamp}.log"
    report_path = REPORT_DIR / f"repair_0406_specific_items_{timestamp}.json"
    logger = FileLogger(log_path)
    report: dict[str, Any] = {
        "started_at": datetime.now().isoformat(),
        "log_path": str(log_path),
        "repair_0322_1_01": {},
        "reupload_0322_28_01": {},
    }

    try:
        logger("[Start] repair 0322_1_01 metadata/edit + 0322_28_01 reupload")

        metadata_result = regenerate_metadata_for_1(logger)
        report["repair_0322_1_01"]["metadata"] = metadata_result
        edit_result = await edit_existing_video_1(metadata_result["channel"], logger)
        report["repair_0322_1_01"]["edit"] = edit_result

        upload_28_result = await reupload_28(logger)
        report["reupload_0322_28_01"] = upload_28_result

        report["finished_at"] = datetime.now().isoformat()
        report["success"] = bool(
            report["repair_0322_1_01"].get("edit", {}).get("success")
            and report["reupload_0322_28_01"].get("success")
        )
    except Exception as exc:
        report["finished_at"] = datetime.now().isoformat()
        report["success"] = False
        report["error"] = str(exc)
        report["traceback"] = traceback.format_exc()
        logger(f"[Fatal] {exc}\n{report['traceback']}")
        raise
    finally:
        with open(report_path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(report, ensure_ascii=False, indent=2))
        logger(f"[Finish] report={report_path}")


if __name__ == "__main__":
    asyncio.run(main())
