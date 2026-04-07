# -*- coding: utf-8 -*-
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Any, Callable

from archive_manager import ArchiveManager
from batch_upload import execute_group_job
from daily_scheduler import VIDEO_CODEC
from path_templates import get_path_template, load_path_templates
from run_queue import GroupJob, RunQueue, UploadDefaults
from workflow_core import (
    ArtifactReadyCallback,
    ExecutionControl,
    MetadataReadyCallback,
    SimulationOptions,
    WindowTask,
    WorkflowDefaults,
    WorkflowResult,
    build_window_plan,
    expand_window_tasks_by_round,
    execute_direct_media_workflow,
    execute_metadata_only_workflow,
    _find_existing_video,
    _output_dir_matches_tasks,
    get_metadata_root,
    load_scheduler_settings,
    resolve_task_audio_dir,
    resolve_task_image_dir,
    task_runtime_key,
    assign_media_to_tasks,
    validate_group_sources,
)

LogFunc = Callable[[str], None]
_DATACLASS_KWARGS = {"slots": True} if sys.version_info >= (3, 10) else {}

MODULE_LABELS = {
    "metadata": "生成标题/简介/标签/缩略图",
    "render": "剪辑",
    "upload": "上传",
}


def _noop_log(_message: str) -> None:
    return


@dataclass(**_DATACLASS_KWARGS)
class ModuleSelection:
    metadata: bool = False
    render: bool = False
    upload: bool = False

    def any_selected(self) -> bool:
        return bool(self.metadata or self.render or self.upload)

    def as_dict(self) -> dict[str, bool]:
        return {
            "metadata": bool(self.metadata),
            "render": bool(self.render),
            "upload": bool(self.upload),
        }

    def labels(self) -> list[str]:
        return [label for key, label in MODULE_LABELS.items() if self.as_dict().get(key)]


@dataclass(**_DATACLASS_KWARGS)
class MediaScope:
    tag: str
    image_dir: str
    audio_dir: str
    serials: list[int] = field(default_factory=list)
    source_overrides: list[str] = field(default_factory=list)


@dataclass(**_DATACLASS_KWARGS)
class RunPlan:
    logical_tasks: list[WindowTask]
    tasks: list[WindowTask]
    defaults: WorkflowDefaults
    modules: ModuleSelection
    config: dict[str, Any]
    window_plan: dict[str, Any]
    metadata_root: str
    music_root: str
    image_root: str
    output_root: str
    media_scopes: list[MediaScope] = field(default_factory=list)


@dataclass(**_DATACLASS_KWARGS)
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    resolved_output_dirs: dict[str, str] = field(default_factory=dict)

    def ok(self) -> bool:
        return not self.errors

    def raise_if_failed(self) -> None:
        if self.errors:
            raise ValueError("\n".join(self.errors))


@dataclass(**_DATACLASS_KWARGS)
class ExecutionResult:
    run_plan: RunPlan
    validation: ValidationReport
    workflow_result: WorkflowResult | None = None
    prepared_output_dirs: dict[str, str] = field(default_factory=dict)


def build_module_selection(*, metadata: bool, render: bool, upload: bool) -> ModuleSelection:
    return ModuleSelection(metadata=bool(metadata), render=bool(render), upload=bool(upload))


def _resolve_task_plan_dirs(
    *,
    task: WindowTask,
    defaults: WorkflowDefaults,
    modules: ModuleSelection,
    config: dict[str, Any],
    single_tag_mode: bool,
) -> tuple[Path, Path]:
    output_root = Path(str(config.get("output_root") or "").strip())
    metadata_root = get_metadata_root(config)
    clean_tag = str(task.tag or "").strip()
    source_override = str(task.source_dir or "").strip()

    # ── 多 slot 子目录：必须与 workflow_core._task_output_dir 保持一致 ──
    # _task_output_dir 格式: base / f"{date_mmdd}_{serial}_{slot_index:02d}"
    total_slots = int(getattr(task, "total_slots", 1) or 1)
    slot_index = int(getattr(task, "slot_index", 1) or 1)
    serial = int(getattr(task, "serial", 0) or 0)
    has_slot_subdir = total_slots > 1

    # In metadata/upload flows that operate on existing videos, the user-selected
    # folder on the upload page must win. We should create/read manifests directly
    # in that folder instead of silently drifting back to stale global roots.
    if source_override and not modules.render and (modules.metadata or modules.upload):
        source_root = Path(source_override)
        if has_slot_subdir:
            slot_folder = source_root / f"{defaults.date_mmdd}_{serial}_{slot_index:02d}"
            return slot_folder, slot_folder
        return source_root, source_root

    if single_tag_mode:
        if has_slot_subdir:
            slot_folder_name = f"{defaults.date_mmdd}_{serial}_{slot_index:02d}"
            return output_root / slot_folder_name, metadata_root / slot_folder_name
        return output_root, metadata_root
    output_dir = output_root / f"{defaults.date_mmdd}_{clean_tag}"
    metadata_dir = metadata_root / clean_tag
    if has_slot_subdir:
        slot_folder_name = f"{defaults.date_mmdd}_{serial}_{slot_index:02d}"
        return output_dir / slot_folder_name, metadata_dir / slot_folder_name
    return output_dir, metadata_dir


def reconcile_run_plan_directories(run_plan: RunPlan) -> RunPlan:
    cfg = deepcopy(run_plan.config or {})
    output_root_text = str(cfg.get("output_root") or run_plan.output_root or "").strip()
    metadata_root_text = str(cfg.get("metadata_root") or run_plan.metadata_root or "").strip()
    music_root_text = str(cfg.get("music_dir") or run_plan.music_root or "").strip()
    image_root_text = str(cfg.get("base_image_dir") or run_plan.image_root or "").strip()
    if not output_root_text:
        raise ValueError("RunPlan is missing output_root.")
    if not metadata_root_text:
        raise ValueError("RunPlan is missing metadata_root.")

    unique_tags = [str(task.tag or "").strip() for task in run_plan.tasks if str(task.tag or "").strip()]
    unique_tags = list(dict.fromkeys(unique_tags))
    single_tag_mode = len(unique_tags) == 1
    tag_output_dirs: dict[str, str] = {}
    tag_metadata_dirs: dict[str, str] = {}
    task_output_dirs: dict[str, str] = {}
    task_metadata_dirs: dict[str, str] = {}
    for task in run_plan.tasks:
        clean_tag = str(task.tag or "").strip()
        if not clean_tag:
            continue
        resolved_output, resolved_metadata = _resolve_task_plan_dirs(
            task=task,
            defaults=run_plan.defaults,
            modules=run_plan.modules,
            config=cfg,
            single_tag_mode=single_tag_mode,
        )
        task_output_dirs[task_runtime_key(task)] = str(resolved_output)
        task_metadata_dirs[task_runtime_key(task)] = str(resolved_metadata)
        tag_output_dirs.setdefault(clean_tag, str(resolved_output))
        tag_metadata_dirs.setdefault(clean_tag, str(resolved_metadata))

    plan = deepcopy(run_plan.window_plan)
    plan["tag_output_dirs"] = tag_output_dirs
    plan["tag_metadata_dirs"] = tag_metadata_dirs
    plan["task_output_dirs"] = task_output_dirs
    plan["task_metadata_dirs"] = task_metadata_dirs
    cfg["output_root"] = output_root_text
    cfg["metadata_root"] = metadata_root_text
    cfg["music_dir"] = music_root_text
    cfg["base_image_dir"] = image_root_text
    run_plan.config = cfg
    run_plan.window_plan = plan
    run_plan.output_root = output_root_text
    run_plan.metadata_root = metadata_root_text
    run_plan.music_root = music_root_text
    run_plan.image_root = image_root_text
    return run_plan


def _resolve_media_scopes(tasks: list[WindowTask], config: dict[str, Any]) -> list[MediaScope]:
    grouped: dict[tuple[str, str, str], MediaScope] = {}
    for task in tasks:
        image_dir = resolve_task_image_dir(task, config)
        audio_dir = resolve_task_audio_dir(task, config)
        key = (
            task.tag,
            str(image_dir.resolve(strict=False)).lower(),
            str(audio_dir.resolve(strict=False)).lower(),
        )
        scope = grouped.get(key)
        if scope is None:
            scope = MediaScope(
                tag=task.tag,
                image_dir=str(image_dir),
                audio_dir=str(audio_dir),
            )
            grouped[key] = scope
        scope.serials.append(int(task.serial))
        if task.source_dir and task.source_dir.strip():
            scope.source_overrides.append(task.source_dir.strip())
    return sorted(grouped.values(), key=lambda item: (item.tag.lower(), item.image_dir.lower(), item.audio_dir.lower()))


def build_run_plan(
    *,
    tasks: list[WindowTask],
    defaults: WorkflowDefaults,
    modules: ModuleSelection,
    config: dict[str, Any] | None = None,
) -> RunPlan:
    cfg = deepcopy(config or load_scheduler_settings())
    logical_tasks = list(tasks)
    expanded_tasks = expand_window_tasks_by_round(logical_tasks)
    plan = build_window_plan(expanded_tasks, defaults)
    unique_tags = [str(task.tag or "").strip() for task in expanded_tasks if str(task.tag or "").strip()]
    unique_tags = list(dict.fromkeys(unique_tags))
    single_tag_mode = len(unique_tags) == 1
    tag_output_dirs: dict[str, str] = {}
    tag_metadata_dirs: dict[str, str] = {}
    task_output_dirs: dict[str, str] = {}
    task_metadata_dirs: dict[str, str] = {}
    for task in expanded_tasks:
        clean_tag = str(task.tag or "").strip()
        if not clean_tag:
            continue
        resolved_output, resolved_metadata = _resolve_task_plan_dirs(
            task=task,
            defaults=defaults,
            modules=modules,
            config=cfg,
            single_tag_mode=single_tag_mode,
        )
        task_output_dirs[task_runtime_key(task)] = str(resolved_output)
        task_metadata_dirs[task_runtime_key(task)] = str(resolved_metadata)
        tag_output_dirs.setdefault(clean_tag, str(resolved_output))
        tag_metadata_dirs.setdefault(clean_tag, str(resolved_metadata))
    if tag_output_dirs:
        plan["tag_output_dirs"] = tag_output_dirs
    if tag_metadata_dirs:
        plan["tag_metadata_dirs"] = tag_metadata_dirs
    if task_output_dirs:
        plan["task_output_dirs"] = task_output_dirs
    if task_metadata_dirs:
        plan["task_metadata_dirs"] = task_metadata_dirs
    run_plan = RunPlan(
        logical_tasks=logical_tasks,
        tasks=expanded_tasks,
        defaults=defaults,
        modules=modules,
        config=cfg,
        window_plan=plan,
        metadata_root=str(get_metadata_root(cfg)),
        music_root=str(cfg.get("music_dir") or "").strip(),
        image_root=str(cfg.get("base_image_dir") or "").strip(),
        output_root=str(cfg.get("output_root") or "").strip(),
        media_scopes=_resolve_media_scopes(tasks, cfg),
    )
    return reconcile_run_plan_directories(run_plan)


def _visual_mode_line(defaults: WorkflowDefaults) -> str:
    settings = defaults.visual_settings or {}
    visual_mode = str(settings.get("visual_mode") or settings.get("preset") or "manual").strip() or "manual"
    if visual_mode == "random":
        return "Visual mode: Random"
    if visual_mode not in {"manual", "none"}:
        return f"Visual mode: Preset | name={visual_mode}"
    return (
        "Visual mode: Manual | "
        f"style={settings.get('style', 'bar')} | "
        f"particle={settings.get('particle', 'none')} | "
        f"tint={settings.get('color_tint', 'none')}"
    )


def preview_run_plan(run_plan: RunPlan) -> list[str]:
    run_plan = reconcile_run_plan_directories(run_plan)
    max_round = max((int(getattr(task, "round_index", 1) or 1) for task in run_plan.tasks), default=1)
    lines = [
        f"Selected modules: {', '.join(run_plan.modules.labels())}",
        f"Date: {run_plan.defaults.date_mmdd}",
        f"Window count: {len(run_plan.logical_tasks)}",
        f"Total items: {len(run_plan.tasks)}",
        f"Rounds: {max_round}",
        f"Metadata root: {run_plan.metadata_root}",
        f"Music root: {run_plan.music_root}",
        f"Image root: {run_plan.image_root}",
        f"Video output root: {run_plan.output_root}",
        _visual_mode_line(run_plan.defaults),
        "",
    ]
    lines.extend(run_plan.window_plan.get("preview_lines", []))
    output_dirs = dict(run_plan.window_plan.get("tag_output_dirs") or {})
    metadata_dirs = dict(run_plan.window_plan.get("tag_metadata_dirs") or {})
    if output_dirs:
        lines.append("")
        lines.append("Resolved output folders:")
        for tag, folder in output_dirs.items():
            metadata_folder = metadata_dirs.get(tag, run_plan.metadata_root)
            lines.append(f"  - {tag}: video={folder} | metadata={metadata_folder}")
    if run_plan.media_scopes:
        lines.append("")
        lines.append("Resolved media scopes:")
        for scope in run_plan.media_scopes:
            override_text = ""
            if scope.source_overrides:
                joined = ", ".join(sorted(set(scope.source_overrides)))
                override_text = f" | override={joined}"
            serial_text = ", ".join(str(item) for item in scope.serials)
            lines.append(
                f"  - {scope.tag}: windows=[{serial_text}] | image={scope.image_dir} | "
                f"audio={scope.audio_dir}{override_text}"
            )
    return lines


def validate_run_plan(run_plan: RunPlan, *, log: LogFunc = _noop_log) -> ValidationReport:
    run_plan = reconcile_run_plan_directories(run_plan)
    report = ValidationReport()
    modules = run_plan.modules
    if not modules.any_selected():
        report.errors.append("至少需要选择一个模块。")
        return report
    if not run_plan.tasks:
        report.errors.append("至少需要一个窗口任务。")
        return report

    if modules.metadata and run_plan.defaults.metadata_mode != "prompt_api":
        report.errors.append("Metadata generation requires prompt_api mode.")
        return report

    if modules.render:
        errors, warnings = validate_group_sources(run_plan.tasks, config=run_plan.config, log=log)
        report.errors.extend(errors)
        report.warnings.extend(warnings)

    if modules.upload and not modules.render:
        errors, warnings, resolved_dirs = _validate_explicit_output_dirs(
            run_plan,
            allow_bootstrap=bool(modules.metadata),
            log=log,
        )
        report.errors.extend(errors)
        report.warnings.extend(warnings)
        report.resolved_output_dirs.update(resolved_dirs)

    return report


def _validate_explicit_output_dirs(
    run_plan: RunPlan,
    *,
    allow_bootstrap: bool = False,
    log: LogFunc = _noop_log,
) -> tuple[list[str], list[str], dict[str, str]]:
    errors: list[str] = []
    warnings: list[str] = []
    resolved_dirs: dict[str, str] = {}
    explicit_dirs = {
        str(tag or "").strip(): str(path or "").strip()
        for tag, path in dict(run_plan.window_plan.get("tag_output_dirs") or {}).items()
        if str(tag or "").strip() and str(path or "").strip()
    }
    if not explicit_dirs:
        return ["RunPlan is missing explicit output folders for upload-only mode."], warnings, resolved_dirs

    grouped: dict[str, list[WindowTask]] = {}
    for task in run_plan.tasks:
        clean_tag = str(task.tag or "").strip()
        if clean_tag:
            grouped.setdefault(clean_tag, []).append(task)

    for tag, tag_tasks in grouped.items():
        folder_text = explicit_dirs.get(tag, "").strip()
        if not folder_text:
            errors.append(f"{tag} 缺少当前任务的明确输出目录。")
            continue

        folder = Path(folder_text)
        if not folder.exists():
            errors.append(f"{tag} 当前任务指定的输出目录不存在: {folder}")
            continue

        ok, details = _output_dir_matches_tasks(folder, tag_tasks)

        # ========== 子目录搜索：如果根目录没找到manifest，搜索子目录 ==========
        if not ok and folder.exists():
            # 限制搜索深度（最多3层），避免遍历过深
            sub_manifests = sorted(folder.rglob("upload_manifest.json"))
            if sub_manifests:
                sub_details: list[str] = []
                matched_dirs: list[str] = []
                for manifest_path in sub_manifests:
                    sub_dir = manifest_path.parent
                    sub_ok, sub_errs = _output_dir_matches_tasks(sub_dir, tag_tasks)
                    if sub_ok:
                        matched_dirs.append(str(sub_dir))
                    else:
                        sub_details.extend(sub_errs)
                if matched_dirs:
                    # 多子目录匹配时选最新的（按修改时间），单个直接用
                    if len(matched_dirs) == 1:
                        resolved_dirs[tag] = matched_dirs[0]
                    else:
                        # 选最近修改的manifest所在目录
                        best = max(matched_dirs, key=lambda d: Path(d, "upload_manifest.json").stat().st_mtime)
                        resolved_dirs[tag] = best
                        log(f"[检查] {tag}: 多个子目录匹配，选最新: {Path(best).name}")
                    log(f"[检查] {tag}: 在子目录中找到 {len(matched_dirs)} 个有效 manifest")
                    continue
                details = sub_details if sub_details else details

        if not ok and allow_bootstrap:
            bootstrap_errors: list[str] = []
            # 也在子目录中搜索视频文件
            for task in tag_tasks:
                video = _find_existing_video(folder, run_plan.defaults.date_mmdd, task.serial, {}, task=task)
                if not video:
                    # 再搜索子目录
                    found_in_sub = False
                    for sub_dir in sorted(folder.iterdir()):
                        if sub_dir.is_dir():
                            video = _find_existing_video(sub_dir, run_plan.defaults.date_mmdd, task.serial, {}, task=task)
                            if video:
                                found_in_sub = True
                                break
                    if not found_in_sub:
                        bootstrap_errors.append(f"{task_runtime_key(task)} 缺少现成视频文件")
            if not bootstrap_errors:
                resolved_dirs[tag] = str(folder)
                warning = f"{tag} 将从现成视频目录自举 metadata/manifest: {folder}"
                warnings.append(warning)
                log(f"[检查] {warning}")
                continue
            details = [*details, *bootstrap_errors]
        if not ok:
            detail_text = " | ".join(details[:3]) if details else ""
            errors.append(
                f"{tag} 当前任务指定的输出目录不可上传: {folder}"
                + (f" | {detail_text}" if detail_text else "")
            )
            continue

        resolved_dirs[tag] = str(folder)
        log(f"[检查] {tag}: 只按本次任务指定目录上传 | {folder}")

    return errors, warnings, resolved_dirs


def collect_output_dirs(workflow_result: WorkflowResult | None) -> dict[str, str]:
    prepared: dict[str, str] = {}
    if not workflow_result:
        return prepared
    for item in getattr(workflow_result, "items", []) or []:
        output_video = str(getattr(item, "output_video", "") or "").strip()
        tag = str(getattr(item, "tag", "") or "").strip()
        if not output_video or not tag:
            continue
        folder = Path(output_video).parent
        if folder.exists():
            prepared.setdefault(tag, str(folder))
    return prepared


def execute_simulation_plan(
    run_plan: RunPlan,
    *,
    simulate_seconds: int,
    control: ExecutionControl | None = None,
    log: LogFunc = _noop_log,
) -> WorkflowResult:
    run_plan = reconcile_run_plan_directories(run_plan)
    validation = validate_run_plan(run_plan, log=log)
    for warning in validation.warnings:
        log(f"[Validate] {warning}")
    validation.raise_if_failed()
    return execute_direct_media_workflow(
        tasks=run_plan.tasks,
        defaults=run_plan.defaults,
        simulation=SimulationOptions(
            simulate_seconds=simulate_seconds,
            consume_sources=True,
            save_manifest=True,
        ),
        config=run_plan.config,
        output_dir_overrides=dict(run_plan.window_plan.get("tag_output_dirs") or {}),
        metadata_dir_overrides=dict(run_plan.window_plan.get("tag_metadata_dirs") or {}),
        control=control,
        log=log,
    )


def execute_run_plan(
    run_plan: RunPlan,
    *,
    control: ExecutionControl | None = None,
    on_metadata_ready: MetadataReadyCallback | None = None,
    on_item_ready: ArtifactReadyCallback | None = None,
    log: LogFunc = _noop_log,
) -> ExecutionResult:
    run_plan = reconcile_run_plan_directories(run_plan)
    validation = validate_run_plan(run_plan, log=log)
    for warning in validation.warnings:
        log(f"[Validate] {warning}")
    validation.raise_if_failed()
    result = ExecutionResult(
        run_plan=run_plan,
        validation=validation,
        prepared_output_dirs=dict(validation.resolved_output_dirs),
    )

    if run_plan.modules.render:
        log("[Start] Render module")
        workflow_result = execute_direct_media_workflow(
            tasks=run_plan.tasks,
            defaults=run_plan.defaults,
            simulation=SimulationOptions(simulate_seconds=0, consume_sources=True, save_manifest=True),
            config=run_plan.config,
            output_dir_overrides=dict(run_plan.window_plan.get("tag_output_dirs") or {}),
            metadata_dir_overrides=dict(run_plan.window_plan.get("tag_metadata_dirs") or {}),
            control=control,
            on_metadata_ready=on_metadata_ready,
            on_item_ready=on_item_ready if run_plan.modules.upload else None,
            log=log,
        )
        result.workflow_result = workflow_result
        result.prepared_output_dirs.update(collect_output_dirs(workflow_result))
        return result

    if run_plan.modules.metadata:
        log("[Start] Metadata module")
        result.workflow_result = execute_metadata_only_workflow(
            tasks=run_plan.tasks,
            defaults=run_plan.defaults,
            config=run_plan.config,
            output_dir_overrides=dict(run_plan.window_plan.get("tag_output_dirs") or {}),
            metadata_dir_overrides=dict(run_plan.window_plan.get("tag_metadata_dirs") or {}),
            control=control,
            on_metadata_ready=on_metadata_ready,
            on_item_ready=on_item_ready if run_plan.modules.upload else None,
            log=log,
        )
        result.prepared_output_dirs.update(collect_output_dirs(result.workflow_result))

    return result


ProgressCallback = Callable[[dict[str, Any]], None]
BeforeJobCallback = Callable[[GroupJob], None]
BuildRunPlanCallback = Callable[[GroupJob], RunPlan]
ExecutionResultCallback = Callable[[GroupJob, ExecutionResult], None]


def _emit_progress(progress_callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if not callable(progress_callback):
        return
    try:
        progress_callback(dict(payload))
    except Exception as exc:
        import traceback
        print(f"[_emit_progress] callback error: {exc}\n{traceback.format_exc()}")


def merge_defaults(job_defaults: UploadDefaults, defaults: UploadDefaults) -> UploadDefaults:
    merged = defaults.to_dict()
    for key, value in job_defaults.to_dict().items():
        if value not in (None, ""):
            merged[key] = value
    return UploadDefaults.from_dict(merged)


def _metadata_prepare_worker_count(window_total: int) -> int:
    return max(1, min(4, int(window_total or 1)))


def _upload_worker_count(window_total: int) -> int:
    if int(window_total or 0) <= 1:
        return 1
    return max(1, min(3, int(window_total)))


async def execute_run_queue(
    queue: RunQueue,
    defaults: UploadDefaults,
    *,
    control: ExecutionControl | None = None,
    before_job_callback: BeforeJobCallback | None = None,
    build_run_plan_for_job: BuildRunPlanCallback | None = None,
    execution_result_callback: ExecutionResultCallback | None = None,
    progress_callback: ProgressCallback | None = None,
    log: LogFunc = _noop_log,
) -> list[dict[str, Any]]:
    if queue.is_empty():
        return []

    results: list[dict[str, Any]] = []
    job_total = len(queue.jobs)
    path_templates = load_path_templates()
    for job_index, raw_job in enumerate(queue.jobs, 1):
        if control:
            control.check_cancelled()
            control.wait_if_paused(log=log, label=f"queue/{job_index}")
        job = GroupJob.from_dict(raw_job.to_dict()) if isinstance(raw_job, GroupJob) else GroupJob.from_dict(raw_job)
        job.upload_defaults = merge_defaults(job.upload_defaults, UploadDefaults.from_dict(defaults.to_dict()))
        modules = {
            str(module_name or "").strip().lower()
            for module_name in (job.modules or [])
            if str(module_name or "").strip()
        }
        _emit_progress(
            progress_callback,
            {
                "type": "job_started",
                "job_index": job_index,
                "job_total": job_total,
                "group_tag": job.group_tag,
                "window_count": len(job.window_serials),
                "window_serials": [int(serial) for serial in job.window_serials],
                "modules": sorted(modules),
            },
        )
        log(
            f"[Queue] {job_index}/{job_total} -> {job.group_tag} | "
            f"windows={job.window_serials} | modules={sorted(modules)}"
        )
        job_result: dict[str, Any] = {
            "group_tag": job.group_tag,
            "job_index": job_index,
            "job_total": job_total,
            "success_count": 0,
            "failed_count": 0,
            "results": [],
        }
        try:
            if callable(before_job_callback):
                before_job_callback(job)

            has_prepare = modules.intersection({"metadata", "render"})
            has_upload = "upload" in modules

            if has_prepare and not callable(build_run_plan_for_job):
                raise ValueError(f"{job.group_tag} requires build_run_plan_for_job for metadata/render modules.")

            import asyncio as _aio
            import concurrent.futures as _cf

            all_window_results: list[dict[str, Any]] = []
            window_total = len(job.window_serials)
            allocated_media_by_key: dict[str, tuple[str, str]] = {}
            has_hw_encoder = str(VIDEO_CODEC or "").strip().lower() != "libx264"
            hybrid_prepare = bool(
                has_prepare
                and "render" in modules
                and has_hw_encoder
                and window_total > 1
            )
            executors: dict[str, _cf.ThreadPoolExecutor] = {}
            if has_prepare:
                if hybrid_prepare:
                    executors["gpu"] = _cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="prepare-gpu")
                    executors["cpu"] = _cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="prepare-cpu")
                    log(f"[Pipeline] {job.group_tag} 启用混合渲染准备: GPU 编码 + CPU 编码 并行")
                else:
                    auto_workers = 1 if "render" in modules else _metadata_prepare_worker_count(window_total)
                    executors["auto"] = _cf.ThreadPoolExecutor(max_workers=auto_workers, thread_name_prefix="prepare")
                    if auto_workers > 1:
                        log(f"[Pipeline] {job.group_tag} metadata workers={auto_workers} running in parallel")
                full_plan = build_run_plan_for_job(job)
                allocated_media_by_key, allocation_warnings = assign_media_to_tasks(
                    full_plan.tasks,
                    config=full_plan.config,
                )
                for warning in allocation_warnings:
                    log(f"[检查] {warning}")
                if allocated_media_by_key:
                    log(f"[Pipeline] {job.group_tag} 已预分配素材 {len(allocated_media_by_key)} 组")

            loop = _aio.get_running_loop()
            streamed_manifest_dirs: dict[int, set[str]] = {
                int(serial): set()
                for serial in job.window_serials
            }
            upload_serial_locks: dict[int, _aio.Lock] = {
                int(serial): _aio.Lock()
                for serial in job.window_serials
            }

            def _build_single_item_upload_job(
                serial: int,
                manifest_dir: Path,
            ) -> GroupJob:
                upload_job = GroupJob.from_dict(job.to_dict())
                upload_job.window_serials = [int(serial)]
                upload_job.upload_defaults = UploadDefaults.from_dict(job.upload_defaults.to_dict())
                upload_job.modules = ["upload"]
                upload_job.steps = ["upload"]
                upload_job.videos_per_window = 1
                upload_job.source_dir = str(manifest_dir)
                setattr(upload_job, "_prepared_manifest_dirs", [str(manifest_dir)])
                return upload_job

            def _queue_streamed_upload(task: Any, task_output_dir: Path, manifest_path: Path) -> None:
                try:
                    serial = int(getattr(task, "serial", 0) or 0)
                except Exception:
                    serial = 0
                if serial <= 0 or not has_upload:
                    return
                manifest_dir = Path(manifest_path).parent
                manifest_dir_key = str(manifest_dir.resolve(strict=False)).lower()
                if not manifest_dir.exists():
                    log(f"[Pipeline] streamed upload: manifest_dir does not exist: {manifest_dir}")
                    return
                if manifest_dir_key in streamed_manifest_dirs.setdefault(serial, set()):
                    log(f"[Pipeline] streamed upload: already queued {manifest_dir}")
                    return
                streamed_manifest_dirs[serial].add(manifest_dir_key)
                upload_job = _build_single_item_upload_job(serial, manifest_dir)
                slot_index = int(getattr(task, "slot_index", 1) or 1)
                total_slots = int(getattr(task, "total_slots", 1) or 1)

                def _enqueue() -> None:
                    try:
                        upload_queue.put_nowait((upload_job, serial))
                    except Exception as _eq_exc:
                        log(f"[Pipeline] FAILED to enqueue upload for window {serial}: {_eq_exc}")

                try:
                    loop.call_soon_threadsafe(_enqueue)
                except Exception as _cs_exc:
                    log(f"[Pipeline] call_soon_threadsafe FAILED for window {serial}: {_cs_exc}")
                    return
                log(
                    f"[Pipeline] {job.group_tag} 窗口 {serial} 素材已就绪，"
                    f"立即加入上传队列 ({slot_index}/{total_slots}) | {manifest_dir}"
                )

            def _emit_metadata_ready(task: Any, task_output_dir: Path, metadata_payload: dict[str, Any]) -> None:
                try:
                    serial = int(getattr(task, "serial", 0) or 0)
                except Exception:
                    serial = 0
                if serial <= 0:
                    return
                slot_index = int(getattr(task, "slot_index", 1) or 1)
                total_slots = int(getattr(task, "total_slots", 1) or 1)
                title = str((metadata_payload or {}).get("title") or "").strip()
                cover_count = len((metadata_payload or {}).get("cover_paths", []) or [])
                bundle = (metadata_payload or {}).get("bundle") if isinstance(metadata_payload, dict) else {}
                api_preset = (bundle or {}).get("api_preset") if isinstance(bundle, dict) else {}
                content_template = (bundle or {}).get("content_template") if isinstance(bundle, dict) else {}
                _emit_progress(
                    progress_callback,
                    {
                        "type": "metadata_ready",
                        "job_index": job_index,
                        "job_total": job_total,
                        "group_tag": job.group_tag,
                        "serial": int(serial),
                        "label": f"{job.group_tag}/{int(serial)}" if total_slots <= 1 else f"{job.group_tag}/{int(serial)}#{int(slot_index):02d}",
                        "slot_index": slot_index,
                        "total_slots": total_slots,
                        "title": title,
                        "cover_count": int(cover_count),
                        "api_preset_name": str((api_preset or {}).get("name") or "").strip(),
                        "content_template_name": str((content_template or {}).get("name") or "").strip(),
                        "output_dir": str(task_output_dir),
                    },
                )
                log(
                    f"[Pipeline] {job.group_tag} 窗口 {serial} 文案/缩略图已完成 "
                    f"({slot_index}/{total_slots}) | title={title[:36]} | covers={cover_count}"
                )

            def _prepare_backend_for_window(window_index: int) -> str:
                if not hybrid_prepare:
                    return "auto"
                return "gpu" if window_index % 2 == 1 else "cpu"

            async def _do_upload(upload_job: GroupJob, win_serial: int) -> dict[str, Any]:
                _job_provider = str(getattr(upload_job, "browser_provider", "") or "").strip().lower()
                if _job_provider and _job_provider != "auto":
                    try:
                        from browser_api import set_runtime_provider
                        set_runtime_provider(_job_provider)
                    except Exception:
                        pass
                return await execute_group_job(
                    upload_job,
                    upload_job.upload_defaults,
                    progress_callback=progress_callback,
                )

            async def _prepare_single_window(win_idx: int, serial: int) -> tuple[GroupJob, int]:
                if control:
                    control.check_cancelled()
                    control.wait_if_paused(log=log, label=f"queue/{job_index}/window/{serial}")
                backend = _prepare_backend_for_window(win_idx)
                backend_label = backend.upper() if backend != "auto" else "AUTO"
                log(
                    f"[Pipeline] {job.group_tag} 窗口 {serial} ({win_idx}/{window_total}) 开始准备 | "
                    f"渲染设备={backend_label}"
                )
                single_upload_job = GroupJob.from_dict(job.to_dict())
                single_upload_job.window_serials = [serial]
                single_upload_job.upload_defaults = UploadDefaults.from_dict(job.upload_defaults.to_dict())
                if not has_prepare:
                    return single_upload_job, serial

                single_prepare_job = GroupJob.from_dict(job.to_dict())
                single_prepare_job.window_serials = [serial]
                _emit_progress(
                    progress_callback,
                    {
                        "type": "prepare_started",
                        "job_index": job_index,
                        "job_total": job_total,
                        "group_tag": job.group_tag,
                        "window_serial": int(serial),
                        "window_index": win_idx,
                        "window_total": window_total,
                        "render_backend": backend,
                    },
                )
                _plan = build_run_plan_for_job(single_prepare_job)
                _plan.config = deepcopy(_plan.config or {})
                if backend in {"gpu", "cpu"}:
                    _plan.config["render_device_preference"] = backend
                if allocated_media_by_key:
                    for planned_task in _plan.tasks:
                        media_pair = allocated_media_by_key.get(task_runtime_key(planned_task))
                        if not media_pair:
                            continue
                        planned_task.assigned_image = str(media_pair[0] or "").strip()
                        planned_task.assigned_audio = str(media_pair[1] or "").strip()
                executor_key = backend if backend in executors else "auto"
                execution = await loop.run_in_executor(
                    executors[executor_key],
                    lambda plan=_plan: execute_run_plan(
                        plan,
                        control=control,
                        on_metadata_ready=_emit_metadata_ready,
                        on_item_ready=_queue_streamed_upload if has_upload else None,
                        log=log,
                    ),
                )

                prepared_output_dir = str(
                    execution.prepared_output_dirs.get(job.group_tag)
                    or execution.run_plan.window_plan.get("tag_output_dirs", {}).get(job.group_tag)
                    or job.source_dir
                ).strip()
                if prepared_output_dir:
                    single_upload_job.source_dir = prepared_output_dir

                manifest_dirs: list[str] = []
                workflow_result = execution.workflow_result
                if workflow_result is not None:
                    for manifest_path_text in workflow_result.manifest_paths:
                        manifest_path = Path(str(manifest_path_text or "").strip())
                        if not manifest_path_text or not manifest_path.name:
                            continue
                        manifest_dir = manifest_path.parent
                        if not manifest_dir.exists():
                            continue
                        manifest_dir_text = str(manifest_dir)
                        if manifest_dir_text not in manifest_dirs:
                            manifest_dirs.append(manifest_dir_text)
                if manifest_dirs:
                    setattr(single_upload_job, "_prepared_manifest_dirs", manifest_dirs)
                    residual_manifest_dirs = [
                        manifest_dir_text
                        for manifest_dir_text in manifest_dirs
                        if str(Path(manifest_dir_text).resolve(strict=False)).lower()
                        not in streamed_manifest_dirs.setdefault(int(serial), set())
                    ]
                    if residual_manifest_dirs:
                        setattr(single_upload_job, "_prepared_manifest_dirs", residual_manifest_dirs)
                        single_upload_job.source_dir = residual_manifest_dirs[0]
                        single_upload_job.videos_per_window = len(residual_manifest_dirs)
                    else:
                        setattr(single_upload_job, "_prepared_manifest_dirs", [])
                        single_upload_job.videos_per_window = 1

                if callable(execution_result_callback):
                    execution_result_callback(single_prepare_job, execution)

                _emit_progress(
                    progress_callback,
                    {
                        "type": "prepare_finished",
                        "job_index": job_index,
                        "job_total": job_total,
                        "group_tag": job.group_tag,
                        "window_serial": int(serial),
                        "window_index": win_idx,
                        "window_total": window_total,
                        "render_backend": backend,
                    },
                )
                return single_upload_job, serial

            upload_queue: _aio.Queue[tuple[GroupJob, int] | None] = _aio.Queue()
            uploader_tasks: list[_aio.Task] = []
            upload_worker_total = _upload_worker_count(window_total) if has_upload else 0
            log(
                f"[Pipeline] {job.group_tag} has_prepare={bool(has_prepare)} has_upload={has_upload} "
                f"upload_workers={upload_worker_total} window_total={window_total} "
                f"modules={sorted(modules)}"
            )

            async def _upload_worker(worker_index: int) -> None:
                log(f"[Pipeline/U{worker_index}] upload worker started, waiting for items...")
                while True:
                    payload = await upload_queue.get()
                    if payload is None:
                        log(f"[Pipeline/U{worker_index}] received sentinel, shutting down")
                        upload_queue.task_done()
                        break
                    upload_job, serial = payload
                    try:
                        if control:
                            control.check_cancelled()
                            control.wait_if_paused(log=log, label=f"queue/{job_index}/upload/{serial}")
                        _pdirs = getattr(upload_job, "_prepared_manifest_dirs", None) or []
                        log(
                            f"[Pipeline/U{worker_index}] {job.group_tag} window {serial} upload started | "
                            f"source_dir={upload_job.source_dir} | manifest_dirs={len(_pdirs)}"
                        )
                        async with upload_serial_locks.setdefault(int(serial), _aio.Lock()):
                            upload_result = await _do_upload(upload_job, serial)
                        for row in upload_result.get("results", []):
                            all_window_results.append(row)
                        log(
                            f"[Pipeline/U{worker_index}] {job.group_tag} window {serial} upload finished "
                            f"success={upload_result.get('success_count', 0)} "
                            f"failed={upload_result.get('failed_count', 0)}"
                        )
                    except Exception as _upload_exc:
                        import traceback as _tb
                        log(
                            f"[Pipeline/U{worker_index}] {job.group_tag} window {serial} upload EXCEPTION: "
                            f"{_upload_exc}\n{_tb.format_exc()}"
                        )
                        all_window_results.append({
                            "group_tag": job.group_tag,
                            "serial": int(serial),
                            "label": f"{job.group_tag}/{serial}",
                            "success": False,
                            "stage": "upload_worker_exception",
                            "detail": str(_upload_exc),
                        })
                    finally:
                        upload_queue.task_done()

            try:
                if has_upload:
                    for worker_index in range(1, upload_worker_total + 1):
                        uploader_tasks.append(_aio.create_task(_upload_worker(worker_index)))

                prepare_tasks: list[_aio.Task] = []
                if has_prepare:
                    for win_idx, serial in enumerate(job.window_serials, 1):
                        prepare_tasks.append(_aio.create_task(_prepare_single_window(win_idx, serial)))
                    log(f"[Pipeline] {job.group_tag} created {len(prepare_tasks)} prepare tasks, waiting for completion...")
                    _prepare_done_count = 0
                    for completed_prepare in _aio.as_completed(prepare_tasks):
                        try:
                            prepared_job, prepared_serial = await completed_prepare
                        except Exception as _prepare_exc:
                            import traceback as _tb
                            _prepare_done_count += 1
                            log(
                                f"[Pipeline] {job.group_tag} prepare task {_prepare_done_count}/{len(prepare_tasks)} "
                                f"FAILED: {_prepare_exc}\n{_tb.format_exc()}"
                            )
                            all_window_results.append({
                                "group_tag": job.group_tag,
                                "label": job.group_tag,
                                "success": False,
                                "stage": "prepare_exception",
                                "detail": str(_prepare_exc),
                            })
                            continue
                        _prepare_done_count += 1
                        prepared_manifest_dirs = list(getattr(prepared_job, "_prepared_manifest_dirs", []) or [])
                        log(
                            f"[Pipeline] {job.group_tag} window {prepared_serial} prepare done "
                            f"({_prepare_done_count}/{len(prepare_tasks)}) | "
                            f"residual_manifest_dirs={len(prepared_manifest_dirs)} | "
                            f"source_dir={prepared_job.source_dir}"
                        )
                        if has_upload and prepared_manifest_dirs:
                            log(f"[Pipeline] queuing residual upload for window {prepared_serial}")
                            await upload_queue.put((prepared_job, prepared_serial))
                        elif has_upload and not prepared_manifest_dirs:
                            log(f"[Pipeline] window {prepared_serial}: all slots already streamed to upload queue")
                    log(f"[Pipeline] {job.group_tag} all {len(prepare_tasks)} prepare tasks completed")
                else:
                    for serial in job.window_serials:
                        single_upload_job = GroupJob.from_dict(job.to_dict())
                        single_upload_job.window_serials = [serial]
                        single_upload_job.upload_defaults = UploadDefaults.from_dict(job.upload_defaults.to_dict())
                        if has_upload:
                            await upload_queue.put((single_upload_job, serial))

                if has_upload:
                    log(f"[Pipeline] {job.group_tag} sending {upload_worker_total} sentinel(s) to upload workers...")
                    for _ in range(upload_worker_total):
                        await upload_queue.put(None)
                    log(f"[Pipeline] {job.group_tag} waiting for upload queue to drain (qsize={upload_queue.qsize()})...")
                    await upload_queue.join()
                    log(f"[Pipeline] {job.group_tag} upload queue drained, waiting for workers to finish...")
                    if uploader_tasks:
                        await _aio.gather(*uploader_tasks, return_exceptions=True)
                    log(f"[Pipeline] {job.group_tag} all upload workers finished")
            finally:
                for uploader_task in uploader_tasks:
                    if uploader_task.done():
                        continue
                    uploader_task.cancel()
                    try:
                        await uploader_task
                    except BaseException:
                        pass
                for executor in executors.values():
                    executor.shutdown(wait=False)

            all_window_results.sort(
                key=lambda item: (
                    int(item.get("serial") or 0),
                    int(item.get("slot_index") or 1),
                    str(item.get("stage") or ""),
                )
            )
            job_result["results"] = all_window_results
            job_result["success_count"] = len([r for r in all_window_results if r.get("success")])
            job_result["failed_count"] = len(all_window_results) - job_result["success_count"]
        except Exception as exc:
            import traceback
            log(f"[Queue] {job.group_tag} failed: {exc}\n{traceback.format_exc()}")
            job_result["failed_count"] = max(1, len(job.window_serials))
            job_result["results"] = [
                {
                    "group_tag": job.group_tag,
                    "label": job.group_tag,
                    "success": False,
                    "stage": "queue_job_exception",
                    "detail": str(exc),
                }
            ]
            _emit_progress(
                progress_callback,
                {
                    "type": "job_error",
                    "job_index": job_index,
                    "job_total": job_total,
                    "group_tag": job.group_tag,
                    "detail": str(exc),
                },
            )
        results.append(job_result)
        _emit_progress(
            progress_callback,
            {
                "type": "job_finished",
                "job_index": job_index,
                "job_total": job_total,
                "group_tag": job.group_tag,
                "window_count": len(job.window_serials),
                "success_count": int(job_result.get("success_count") or 0),
                "failed_count": int(job_result.get("failed_count") or 0),
            },
        )
    cleaned_templates: set[str] = set()
    for raw_job in queue.jobs:
        job = GroupJob.from_dict(raw_job.to_dict()) if isinstance(raw_job, GroupJob) else GroupJob.from_dict(raw_job)
        template_name, template_payload = get_path_template(job.path_template, templates=path_templates)
        if template_name in cleaned_templates:
            continue
        cleaned_templates.add(template_name)
        try:
            deleted = ArchiveManager(template_payload).cleanup_old_videos()
            if deleted:
                log(f"[Archive] {template_name} 清理过期已用视频目录: {deleted} 个")
        except Exception as exc:
            log(f"[Archive] {template_name} cleanup failed: {exc}")
    _emit_progress(
        progress_callback,
        {
            "type": "queue_finished",
            "job_total": job_total,
            "results": results,
        },
    )
    return results
