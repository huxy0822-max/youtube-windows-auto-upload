from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from workflow_core import (
    ExecutionControl,
    SimulationOptions,
    WindowTask,
    WorkflowDefaults,
    WorkflowResult,
    build_window_plan,
    execute_direct_media_workflow,
    execute_metadata_only_workflow,
    get_metadata_root,
    load_scheduler_settings,
    refresh_existing_output_metadata,
    resolve_task_audio_dir,
    resolve_task_image_dir,
    validate_existing_output_dirs,
    validate_group_sources,
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
    metadata_root = get_metadata_root(cfg)
    return RunPlan(
        tasks=list(tasks),
        defaults=defaults,
        modules=modules,
        config=cfg,
        window_plan=plan,
        metadata_root=str(metadata_root),
        music_root=str(cfg.get("music_dir") or "").strip(),
        image_root=str(cfg.get("base_image_dir") or "").strip(),
        output_root=str(cfg.get("output_root") or "").strip(),
        media_scopes=_resolve_media_scopes(tasks, cfg),
    )


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
    report = ValidationReport()
    modules = run_plan.modules
    if not modules.any_selected():
        report.errors.append("至少需要选择一个模块。")
        return report
    if not run_plan.tasks:
        report.errors.append("至少需要一个窗口任务。")
        return report

    if modules.render or modules.metadata:
        errors, warnings = validate_group_sources(run_plan.tasks, config=run_plan.config, log=log)
        report.errors.extend(errors)
        report.warnings.extend(warnings)

    if modules.upload and not modules.render:
        errors, warnings, resolved_dirs = validate_existing_output_dirs(
            run_plan.tasks,
            date_mmdd=run_plan.defaults.date_mmdd,
            config=run_plan.config,
            log=log,
        )
        report.errors.extend(errors)
        report.warnings.extend(warnings)
        report.resolved_output_dirs.update(resolved_dirs)

    return report


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
        control=control,
        log=log,
    )


def execute_run_plan(
    run_plan: RunPlan,
    *,
    control: ExecutionControl | None = None,
    log: LogFunc = _noop_log,
) -> ExecutionResult:
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
            control=control,
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
            control=control,
            log=log,
        )

    if run_plan.modules.upload and run_plan.modules.metadata:
        log("[Upload] Refresh existing manifests using current metadata settings")
        result.prepared_output_dirs = refresh_existing_output_metadata(
            tasks=run_plan.tasks,
            defaults=run_plan.defaults,
            prepared_output_dirs=result.prepared_output_dirs,
            config=run_plan.config,
            control=control,
            log=log,
        )

    return result
