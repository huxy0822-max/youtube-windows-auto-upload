# -*- coding: utf-8 -*-
from __future__ import annotations

from copy import deepcopy
import sys
from dataclasses import dataclass as _dataclass, field
from pathlib import Path
from typing import Any, Callable

from archive_manager import ArchiveManager
from batch_upload import execute_group_job
from path_templates import get_path_template, load_path_templates
from run_queue import GroupJob, RunQueue, UploadDefaults
from workflow_core import (
    ArtifactReadyCallback,
    ExecutionControl,
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
    validate_group_sources,
)

LogFunc = Callable[[str], None]


def dataclass(*args, **kwargs):
    if sys.version_info < (3, 10) and "slots" in kwargs:
        kwargs = dict(kwargs)
        kwargs.pop("slots", None)
    return _dataclass(*args, **kwargs)

MODULE_LABELS = {
    "metadata": "生成标题/简介/标签/缩略图",
    "render": "剪辑",
    "upload": "上传",
}


def _noop_log(_message: str) -> None:
    return


@dataclass(slots=True)
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


@dataclass(slots=True)
class MediaScope:
    tag: str
    image_dir: str
    audio_dir: str
    serials: list[int] = field(default_factory=list)
    source_overrides: list[str] = field(default_factory=list)


@dataclass(slots=True)
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


@dataclass(slots=True)
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    resolved_output_dirs: dict[str, str] = field(default_factory=dict)

    def ok(self) -> bool:
        return not self.errors

    def raise_if_failed(self) -> None:
        if self.errors:
            raise ValueError("\n".join(self.errors))


@dataclass(slots=True)
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
    round_suffix = ""
    if int(getattr(task, "total_slots", 1) or 1) > 1:
        round_suffix = f"_r{int(getattr(task, 'round_index', 1) or 1):02d}"

    # In metadata/upload flows that operate on existing videos, the user-selected
    # folder on the upload page must win. We should create/read manifests directly
    # in that folder instead of silently drifting back to stale global roots.
    if source_override and not modules.render and (modules.metadata or modules.upload):
        source_root = Path(source_override)
        if round_suffix:
            round_folder = source_root / f"{defaults.date_mmdd}{round_suffix}"
            return round_folder, round_folder
        return source_root, source_root

    if single_tag_mode:
        if round_suffix:
            return output_root / f"round_{int(getattr(task, 'round_index', 1) or 1):02d}", metadata_root / f"round_{int(getattr(task, 'round_index', 1) or 1):02d}"
        return output_root, metadata_root
    output_dir = output_root / f"{defaults.date_mmdd}_{clean_tag}"
    metadata_dir = metadata_root / clean_tag
    if round_suffix:
        return Path(str(output_dir) + round_suffix), Path(str(metadata_dir) + round_suffix)
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
            consume_sources=False,
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
            simulation=SimulationOptions(simulate_seconds=0, consume_sources=False, save_manifest=True),
            config=run_plan.config,
            output_dir_overrides=dict(run_plan.window_plan.get("tag_output_dirs") or {}),
            metadata_dir_overrides=dict(run_plan.window_plan.get("tag_metadata_dirs") or {}),
            control=control,
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

            # ========== 并行流水线：渲染与上传重叠执行 ==========
            # 窗口N渲染完立刻启动上传(后台)，同时窗口N+1开始渲染
            # 上传之间串行（同一时间只上传一个，避免浏览器冲突）
            import asyncio as _aio

            all_window_results: list[dict[str, Any]] = []
            window_total = len(job.window_serials)
            _prev_upload_task: _aio.Task | None = None  # 前一个窗口的上传任务

            async def _do_upload(upload_job: GroupJob, win_serial: int) -> dict[str, Any]:
                """后台执行单窗口上传"""
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

            for win_idx, serial in enumerate(job.window_serials, 1):
                if control:
                    control.check_cancelled()
                    control.wait_if_paused(log=log, label=f"queue/{job_index}/window/{serial}")

                log(f"[Pipeline] {job.group_tag} 窗口 {serial} ({win_idx}/{window_total}) 开始")

                # --- 单窗口 prepare（渲染+文案，与前一个窗口的上传并行）---
                single_upload_job = GroupJob.from_dict(job.to_dict())
                single_upload_job.window_serials = [serial]
                single_upload_job.upload_defaults = UploadDefaults.from_dict(defaults.to_dict())

                if has_prepare:
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
                        },
                    )

                    prepare_plan = build_run_plan_for_job(single_prepare_job)
                    execution = await _aio.to_thread(
                        execute_run_plan,
                        prepare_plan,
                        control=control,
                        log=log,
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
                        },
                    )

                # --- 等前一个窗口的上传完成（保证同时只有一个上传）---
                if _prev_upload_task is not None:
                    try:
                        prev_result = await _prev_upload_task
                        for r in prev_result.get("results", []):
                            all_window_results.append(r)
                        log(
                            f"[Pipeline] 前一窗口上传完成 "
                            f"成功={prev_result.get('success_count', 0)} "
                            f"失败={prev_result.get('failed_count', 0)}"
                        )
                    except Exception as prev_exc:
                        log(f"[Pipeline] 前一窗口上传异常: {prev_exc}")
                    _prev_upload_task = None

                # --- 当前窗口上传（后台启动，不阻塞下一个窗口的渲染）---
                if has_upload:
                    if control:
                        control.check_cancelled()
                        control.wait_if_paused(log=log, label=f"queue/{job_index}/upload/{serial}")
                    log(f"[Pipeline] {job.group_tag} 窗口 {serial} 开始上传（后台）")
                    _prev_upload_task = _aio.create_task(
                        _do_upload(single_upload_job, serial)
                    )

            # --- 等最后一个窗口的上传完成 ---
            if _prev_upload_task is not None:
                try:
                    last_result = await _prev_upload_task
                    for r in last_result.get("results", []):
                        all_window_results.append(r)
                    log(
                        f"[Pipeline] 最后窗口上传完成 "
                        f"成功={last_result.get('success_count', 0)} "
                        f"失败={last_result.get('failed_count', 0)}"
                    )
                except Exception as last_exc:
                    log(f"[Pipeline] 最后窗口上传异常: {last_exc}")

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
