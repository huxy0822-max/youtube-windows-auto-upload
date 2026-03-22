from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from workflow_core import (
    ArtifactReadyCallback,
    ExecutionControl,
    PROMPT_STUDIO_FILE,
    SimulationOptions,
    WindowTask,
    WorkflowDefaults,
    WorkflowResult,
    _find_existing_video,
    _output_dir_matches_tasks,
    build_window_plan,
    execute_direct_media_workflow,
    execute_metadata_only_workflow,
    _find_existing_video,
    _output_dir_matches_tasks,
    get_metadata_root,
    load_scheduler_settings,
    resolve_task_audio_dir,
    resolve_task_image_dir,
    validate_group_sources,
    validate_prompt_bindings,
    validate_task_containers,
)

LogFunc = Callable[[str], None]

MODULE_LABELS = {
    "metadata": "生成标题/简介/标签/缩略图",
    "render": "剪辑",
    "upload": "上传",
}


def _noop_log(_message: str) -> None:
    return


@dataclass
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


@dataclass
class MediaScope:
    tag: str
    image_dir: str
    audio_dir: str
    serials: list[int] = field(default_factory=list)
    source_overrides: list[str] = field(default_factory=list)


@dataclass
class RunPlan:
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


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    resolved_output_dirs: dict[str, str] = field(default_factory=dict)

    def ok(self) -> bool:
        return not self.errors

    def raise_if_failed(self) -> None:
        if self.errors:
            raise ValueError("\n".join(self.errors))


@dataclass
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

    # In metadata/upload flows that operate on existing videos, the user-selected
    # folder on the upload page must win. We should create/read manifests directly
    # in that folder instead of silently drifting back to stale global roots.
    if source_override and not modules.render and (modules.metadata or modules.upload):
        source_root = Path(source_override)
        return source_root, source_root

    if single_tag_mode:
        return output_root, metadata_root
    return output_root / f"{defaults.date_mmdd}_{clean_tag}", metadata_root / clean_tag


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
        tag_output_dirs.setdefault(clean_tag, str(resolved_output))
        tag_metadata_dirs.setdefault(clean_tag, str(resolved_metadata))

    plan = deepcopy(run_plan.window_plan)
    plan["tag_output_dirs"] = tag_output_dirs
    plan["tag_metadata_dirs"] = tag_metadata_dirs
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
        if task.source_dir.strip():
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
    plan = build_window_plan(tasks, defaults)
    unique_tags = [str(task.tag or "").strip() for task in tasks if str(task.tag or "").strip()]
    unique_tags = list(dict.fromkeys(unique_tags))
    single_tag_mode = len(unique_tags) == 1
    tag_output_dirs: dict[str, str] = {}
    tag_metadata_dirs: dict[str, str] = {}
    for task in tasks:
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
        tag_output_dirs.setdefault(clean_tag, str(resolved_output))
        tag_metadata_dirs.setdefault(clean_tag, str(resolved_metadata))
    if tag_output_dirs:
        plan["tag_output_dirs"] = tag_output_dirs
    if tag_metadata_dirs:
        plan["tag_metadata_dirs"] = tag_metadata_dirs
    run_plan = RunPlan(
        tasks=list(tasks),
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
    if defaults.randomize_effects:
        return "Visual mode: Random"
    settings = defaults.visual_settings or {}
    return (
        "Visual mode: Manual | "
        f"style={settings.get('style', 'bar')} | "
        f"particle={settings.get('particle', 'none')} | "
        f"tint={settings.get('color_tint', 'none')}"
    )


def preview_run_plan(run_plan: RunPlan) -> list[str]:
    run_plan = reconcile_run_plan_directories(run_plan)
    lines = [
        f"Selected modules: {', '.join(run_plan.modules.labels())}",
        f"Date: {run_plan.defaults.date_mmdd}",
        f"Window count: {len(run_plan.tasks)}",
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

    if modules.metadata:
        prompt_errors, prompt_warnings = validate_prompt_bindings(
            tags=[task.tag for task in run_plan.tasks],
            require_text_generation=bool(run_plan.defaults.generate_text),
            require_image_generation=bool(run_plan.defaults.generate_thumbnails),
            path=PROMPT_STUDIO_FILE,
        )
        report.errors.extend(prompt_errors)
        report.warnings.extend(prompt_warnings)

    if modules.upload:
        container_errors, container_warnings = validate_task_containers(run_plan.tasks)
        report.errors.extend(container_errors)
        report.warnings.extend(container_warnings)

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
        if not ok and allow_bootstrap:
            bootstrap_errors: list[str] = []
            for task in tag_tasks:
                video = _find_existing_video(folder, run_plan.defaults.date_mmdd, task.serial, {})
                if not video:
                    bootstrap_errors.append(f"窗口 {task.serial} 缺少现成视频文件")
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
            simulation=SimulationOptions(simulate_seconds=0, consume_sources=True, save_manifest=True),
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

    return result
