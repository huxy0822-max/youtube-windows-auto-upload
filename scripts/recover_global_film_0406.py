# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import threading
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from batch_upload import execute_group_job
from path_templates import build_runtime_config, get_path_template, load_path_templates
from run_queue import GroupJob, UploadDefaults
from workflow_core import (
    WindowTask,
    WorkflowDefaults,
    assign_media_to_tasks,
    execute_direct_media_workflow,
    execute_metadata_only_workflow,
    load_scheduler_settings,
    task_runtime_key,
)

STATE_FILE = BASE_DIR / "dashboard_state.json"
SNAPSHOT_FILE = BASE_DIR / "data" / "last_run_snapshot.json"
ROOT_DIR = Path(r"F:\CineMood\全球电影\0406")
LOG_DIR = BASE_DIR / "logs"
REPORT_DIR = BASE_DIR / "data" / "recovery_reports"
DATE_MMDD = "0322"
GROUP_TAG = "全球电影"
EXISTING_METADATA_WORKERS = 4


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


class FileLogger:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def __call__(self, message: str) -> None:
        line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {str(message or '').rstrip()}"
        print(line, flush=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def _task_dir(serial: int, slot_index: int) -> Path:
    return ROOT_DIR / f"{DATE_MMDD}_{int(serial)}_{int(slot_index):02d}"


def _classify_task(row: dict[str, Any]) -> str:
    folder = _task_dir(int(row.get("serial") or 0), int(row.get("slot_index") or 1))
    video = folder / f"{DATE_MMDD}_{int(row.get('serial') or 0)}_{int(row.get('slot_index') or 1):02d}.mp4"
    done = Path(str(video) + ".done")
    if video.exists() and done.exists():
        return "existing_done"
    if video.exists():
        return "needs_render_retry"
    return "needs_render"


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _build_defaults(state: dict[str, Any], base_job: GroupJob) -> WorkflowDefaults:
    upload_defaults = UploadDefaults.from_dict(base_job.upload_defaults.to_dict())
    schedule_date = str(state.get("schedule_date") or "").strip()
    schedule_time = str(state.get("schedule_time") or "").strip()
    schedule_enabled = bool(state.get("schedule_enabled")) and bool(schedule_date and schedule_time)
    schedule_start = f"{schedule_date} {schedule_time}".strip() if schedule_enabled else ""
    return WorkflowDefaults(
        date_mmdd=str(state.get("date_mmdd") or DATE_MMDD).strip() or DATE_MMDD,
        visibility=str(upload_defaults.visibility or "private").strip() or "private",
        category=str(upload_defaults.category or "Music").strip() or "Music",
        made_for_kids=bool(upload_defaults.is_for_kids),
        altered_content=_bool(upload_defaults.altered_content, default=True),
        notify_subscribers=_bool(upload_defaults.notify_subscribers, default=False),
        schedule_enabled=schedule_enabled,
        schedule_start=schedule_start,
        schedule_interval_minutes=max(1, int(str(state.get("schedule_interval") or "60").strip() or "60")),
        schedule_timezone=str(upload_defaults.timezone or "Asia/Taipei").strip() or "Asia/Taipei",
        metadata_mode=str(state.get("metadata_mode") or "prompt_api").strip() or "prompt_api",
        generate_text=bool(state.get("generate_text", True)),
        generate_thumbnails=bool(state.get("generate_thumbnails", True)),
        sync_daily_content=True,
        randomize_effects=bool(state.get("randomize_effects", False)),
        visual_settings=dict(base_job.visual_settings or {}),
    )


def _build_task(row: dict[str, Any], *, existing_done: bool) -> WindowTask:
    serial = int(row.get("serial") or 0)
    slot_index = int(row.get("slot_index") or 1)
    folder = _task_dir(serial, slot_index)
    source_dir = str(folder if existing_done else ROOT_DIR)
    return WindowTask(
        tag=str(row.get("tag") or GROUP_TAG).strip() or GROUP_TAG,
        serial=serial,
        quantity=1,
        is_ypp=bool(row.get("is_ypp", False)),
        title=str(row.get("title") or "").strip(),
        description=str(row.get("description") or "").strip(),
        visibility=str(row.get("visibility") or "private").strip() or "private",
        category=str(row.get("category") or "Music").strip() or "Music",
        made_for_kids=bool(row.get("made_for_kids", False)),
        altered_content=bool(row.get("altered_content", True)),
        notify_subscribers=bool(row.get("notify_subscribers", False)),
        scheduled_publish_at=str(row.get("scheduled_publish_at") or "").strip(),
        schedule_timezone=str(row.get("schedule_timezone") or "").strip(),
        source_dir=source_dir,
        channel_name=str(row.get("channel_name") or "").strip(),
        slot_index=slot_index,
        total_slots=int(row.get("total_slots") or 1),
        round_index=int(row.get("round_index") or slot_index or 1),
    )


def _extract_seed_frame(video_path: Path, ffmpeg_bin: str, logger: FileLogger) -> Path | None:
    if not video_path.exists():
        return None
    target = video_path.with_name(f"{video_path.stem}_cover_seed.png")
    if target.exists():
        return target
    cmd = [
        str(ffmpeg_bin or "ffmpeg"),
        "-y",
        "-ss",
        "00:00:03",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(target),
    ]
    try:
        completed = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if completed.stderr.strip():
            logger(f"[SeedFrame] {video_path.name} -> {target.name}")
        return target if target.exists() else None
    except Exception as exc:
        logger(f"[SeedFrame] 提取封面帧失败 {video_path}: {exc}")
        return None


def _build_upload_job(base_job: GroupJob, serial: int, manifest_dir: Path) -> GroupJob:
    upload_job = GroupJob.from_dict(base_job.to_dict())
    upload_job.window_serials = [int(serial)]
    upload_job.upload_defaults = UploadDefaults.from_dict(base_job.upload_defaults.to_dict())
    upload_job.modules = ["upload"]
    upload_job.steps = ["upload"]
    upload_job.videos_per_window = 1
    upload_job.source_dir = str(manifest_dir)
    setattr(upload_job, "_prepared_manifest_dirs", [str(manifest_dir)])
    return upload_job


def _upload_progress_logger(logger: FileLogger):
    def _callback(event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip()
        if event_type == "log":
            logger(str(event.get("message") or ""))
            return
        if event_type == "window_finished":
            logger(
                "[UploadResult] {label} success={success} stage={stage} detail={detail}".format(
                    label=str(event.get("label") or ""),
                    success=bool(event.get("success")),
                    stage=str(event.get("stage") or ""),
                    detail=str(event.get("detail") or ""),
                )
            )
            return
        if event_type in {"group_started", "group_finished", "window_started"}:
            logger(f"[UploadEvent] {event_type} {json.dumps(event, ensure_ascii=False)}")
    return _callback


async def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"recover_global_film_0406_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = FileLogger(log_path)
    report_path = REPORT_DIR / f"recover_global_film_0406_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    logger(f"[Start] rescue batch started | log={log_path}")

    state = _load_json(STATE_FILE)
    snapshot = _load_json(SNAPSHOT_FILE)
    queue_jobs = ((state.get("run_queue") or {}).get("jobs") or [])
    if not queue_jobs:
        raise RuntimeError("dashboard_state.json 缺少运行队列任务。")

    base_job = GroupJob.from_dict(queue_jobs[0])
    if str(base_job.group_tag or "").strip() != GROUP_TAG:
        raise RuntimeError(f"当前保存队列不是 {GROUP_TAG}，而是 {base_job.group_tag}")

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
    ffmpeg_bin = str(runtime_config.get("ffmpeg_bin") or runtime_config.get("ffmpeg_path") or "ffmpeg")

    existing_tasks: list[WindowTask] = []
    render_tasks: list[WindowTask] = []
    ready_upload_entries: list[tuple[int, int, Path]] = []
    classification_rows: list[dict[str, Any]] = []
    for row in snapshot.get("tasks", []):
        if str(row.get("tag") or "").strip() != GROUP_TAG:
            continue
        status = _classify_task(row)
        classification_rows.append(
            {
                "serial": int(row.get("serial") or 0),
                "slot_index": int(row.get("slot_index") or 1),
                "status": status,
                "folder": str(_task_dir(int(row.get("serial") or 0), int(row.get("slot_index") or 1))),
            }
        )
        if status == "existing_done":
            task = _build_task(row, existing_done=True)
            video_path = _task_dir(task.serial, task.slot_index) / f"{DATE_MMDD}_{task.serial}_{task.slot_index:02d}.mp4"
            seed_frame = _extract_seed_frame(video_path, ffmpeg_bin, logger)
            if seed_frame is not None:
                task.thumbnails = [str(seed_frame)]
            ready_manifest = _task_dir(task.serial, task.slot_index) / "upload_manifest.json"
            if ready_manifest.exists():
                ready_upload_entries.append((int(task.serial), int(task.slot_index), ready_manifest.parent))
            existing_tasks.append(task)
        else:
            render_tasks.append(_build_task(row, existing_done=False))

    logger(
        f"[Plan] existing_done={len(existing_tasks)} | needs_render={len(render_tasks)} | "
        f"group={GROUP_TAG} | api={base_job.api_template} | prompt={base_job.prompt_template} | "
        f"visual={base_job.visual_mode} | path_template={base_job.path_template}"
    )

    loop = asyncio.get_running_loop()
    upload_locks: dict[int, threading.Lock] = defaultdict(threading.Lock)
    enqueued_manifest_dirs: set[str] = set()
    upload_results: list[dict[str, Any]] = []
    upload_futures: list[Any] = []
    upload_progress = _upload_progress_logger(logger)
    upload_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="recover-0406-upload")

    def _run_single_upload(upload_job: GroupJob, serial: int, manifest_dir: str) -> dict[str, Any]:
        try:
            with upload_locks[int(serial)]:
                logger(f"[UploadTask] start serial={serial} dir={manifest_dir}")
                result = asyncio.run(
                    execute_group_job(
                        upload_job,
                        upload_job.upload_defaults,
                        progress_callback=upload_progress,
                    )
                )
                upload_results.append(result)
                logger(
                    f"[UploadTask] done serial={serial} "
                    f"success={result.get('success_count', 0)} failed={result.get('failed_count', 0)}"
                )
                return result
        except Exception as exc:
            logger(f"[UploadTask] failed serial={serial} dir={manifest_dir}: {exc}\n{traceback.format_exc()}")
            raise

    def on_metadata_ready(task: WindowTask, task_output_dir: Path, metadata_payload: dict[str, Any]) -> None:
        logger(
            f"[MetadataReady] {task.serial}#{task.slot_index:02d} | "
            f"title={str(metadata_payload.get('title') or '')[:60]} | "
            f"covers={len((metadata_payload or {}).get('cover_paths', []) or [])} | "
            f"dir={task_output_dir}"
        )

    def on_item_ready(task: WindowTask, task_output_dir: Path, manifest_path: Path) -> None:
        manifest_dir = manifest_path.parent
        manifest_key = str(manifest_dir.resolve(strict=False)).lower()
        if manifest_key in enqueued_manifest_dirs:
            return
        enqueued_manifest_dirs.add(manifest_key)
        serial = int(task.serial)
        upload_job = _build_upload_job(base_job, serial, manifest_dir)
        logger(f"[Queue] enqueue upload serial={serial} slot={task.slot_index}/{task.total_slots} dir={manifest_dir}")
        try:
            upload_future = upload_executor.submit(_run_single_upload, upload_job, serial, str(manifest_dir))
            upload_futures.append(upload_future)
            logger(f"[Queue] enqueue confirmed serial={serial} dir={manifest_dir}")
        except Exception as exc:
            logger(f"[Queue] enqueue failed serial={serial} dir={manifest_dir}: {exc}")

    def run_existing_metadata_only() -> None:
        if not existing_tasks:
            logger("[Existing] no completed outputs to enrich")
            return
        logger(
            f"[Existing] metadata+upload start count={len(existing_tasks)} "
            f"| workers={EXISTING_METADATA_WORKERS}"
        )

        def _run_single_existing(task: WindowTask) -> None:
            task_label = f"{task.serial}#{task.slot_index:02d}"
            logger(f"[ExistingWorker] start {task_label}")
            execute_metadata_only_workflow(
                tasks=[task],
                defaults=defaults,
                config=runtime_config,
                output_dir_overrides={GROUP_TAG: str(ROOT_DIR)},
                metadata_dir_overrides={GROUP_TAG: str(ROOT_DIR)},
                on_metadata_ready=on_metadata_ready,
                on_item_ready=on_item_ready,
                log=logger,
            )
            logger(f"[ExistingWorker] done {task_label}")

        with ThreadPoolExecutor(
            max_workers=max(1, min(EXISTING_METADATA_WORKERS, len(existing_tasks))),
            thread_name_prefix="recover-0406-existing",
        ) as existing_executor:
            futures = [existing_executor.submit(_run_single_existing, task) for task in existing_tasks]
            for future in futures:
                future.result()
        logger("[Existing] metadata+upload prepare finished")

    def run_pending_render() -> None:
        if not render_tasks:
            logger("[Render] no pending tasks to render")
            return
        allocation, warnings = assign_media_to_tasks(render_tasks, config=runtime_config)
        for warning in warnings:
            logger(f"[Render] warning: {warning}")
        for task in render_tasks:
            pair = allocation.get(task_runtime_key(task))
            if pair:
                task.assigned_image = str(pair[0])
                task.assigned_audio = str(pair[1])
        logger(f"[Render] render+upload start count={len(render_tasks)}")
        execute_direct_media_workflow(
            tasks=render_tasks,
            defaults=defaults,
            simulation=None,
            config=runtime_config,
            output_dir_overrides={GROUP_TAG: str(ROOT_DIR)},
            metadata_dir_overrides={GROUP_TAG: str(ROOT_DIR)},
            on_metadata_ready=on_metadata_ready,
            on_item_ready=on_item_ready,
            log=logger,
        )
        logger("[Render] render prepare finished")

    for ready_serial, ready_slot, ready_manifest_dir in ready_upload_entries:
        ready_key = str(ready_manifest_dir.resolve(strict=False)).lower()
        if ready_key in enqueued_manifest_dirs:
            continue
        enqueued_manifest_dirs.add(ready_key)
        ready_job = _build_upload_job(base_job, ready_serial, ready_manifest_dir)
        logger(f"[BootstrapUpload] serial={ready_serial} slot={ready_slot} dir={ready_manifest_dir}")
        try:
            upload_future = upload_executor.submit(_run_single_upload, ready_job, ready_serial, str(ready_manifest_dir))
            upload_futures.append(upload_future)
            logger(f"[BootstrapUpload] submitted serial={ready_serial} dir={ready_manifest_dir}")
        except Exception as exc:
            logger(f"[BootstrapUpload] failed serial={ready_serial} dir={ready_manifest_dir}: {exc}")

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="recover-0406") as executor:
        producer_tasks = []
        if existing_tasks:
            producer_tasks.append(loop.run_in_executor(executor, run_existing_metadata_only))
        if render_tasks:
            producer_tasks.append(loop.run_in_executor(executor, run_pending_render))
        if producer_tasks:
            await asyncio.gather(*producer_tasks)

    if upload_futures:
        for future in upload_futures:
            try:
                future.result()
            except Exception:
                pass
    upload_executor.shutdown(wait=True, cancel_futures=False)

    report = {
        "started_at": datetime.now().isoformat(),
        "group_tag": GROUP_TAG,
        "root_dir": str(ROOT_DIR),
        "existing_done_count": len(existing_tasks),
        "render_needed_count": len(render_tasks),
        "classification": classification_rows,
        "enqueued_manifest_dirs": sorted(enqueued_manifest_dirs),
        "upload_results": upload_results,
        "log_path": str(log_path),
    }
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(report, ensure_ascii=False, indent=2))
    logger(f"[Finish] rescue batch finished | report={report_path}")


if __name__ == "__main__":
    asyncio.run(main())
