# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
import json
import pathlib
from typing import Any, Optional

DEFAULT_PATH_TEMPLATE_NAME = "默认路径"
DEFAULT_MODULES = ["metadata", "render", "upload"]
DEFAULT_STEPS = ["generate", "render", "upload"]
STEP_TO_MODULE = {
    "generate": "metadata",
    "render": "render",
    "upload": "upload",
}
MODULE_TO_STEP = {value: key for key, value in STEP_TO_MODULE.items()}


def _normalize_modules(values: list[str] | None) -> list[str]:
    modules: list[str] = []
    seen: set[str] = set()
    for raw_value in values or []:
        clean_value = str(raw_value or "").strip().lower()
        if clean_value not in STEP_TO_MODULE.values() or clean_value in seen:
            continue
        seen.add(clean_value)
        modules.append(clean_value)
    return modules


def _normalize_steps(values: list[str] | None) -> list[str]:
    steps: list[str] = []
    seen: set[str] = set()
    for raw_value in values or []:
        clean_value = str(raw_value or "").strip().lower()
        if clean_value not in STEP_TO_MODULE or clean_value in seen:
            continue
        seen.add(clean_value)
        steps.append(clean_value)
    return steps


def _steps_from_modules(values: list[str] | None) -> list[str]:
    normalized = set(_normalize_modules(values))
    return [step_name for step_name in DEFAULT_STEPS if STEP_TO_MODULE[step_name] in normalized]


def _modules_from_steps(values: list[str] | None) -> list[str]:
    normalized = set(_normalize_steps(values))
    return [module_name for module_name in DEFAULT_MODULES if MODULE_TO_STEP[module_name] in normalized]


@dataclass
class UploadDefaults:
    visibility: str = "private"
    category: str = "Music"
    is_for_kids: bool = False
    ai_content: str = "yes"
    altered_content: str = "yes"
    notify_subscribers: bool = False
    schedule_date: Optional[str] = None
    schedule_time: Optional[str] = None
    timezone: str = "Asia/Taipei"
    auto_close_after: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "visibility": str(self.visibility or "").strip() or "private",
            "category": str(self.category or "").strip() or "Music",
            "is_for_kids": bool(self.is_for_kids),
            "ai_content": str(self.ai_content or "").strip() or "yes",
            "altered_content": str(self.altered_content or "").strip() or "yes",
            "notify_subscribers": bool(self.notify_subscribers),
            "schedule_date": str(self.schedule_date or "").strip() or None,
            "schedule_time": str(self.schedule_time or "").strip() or None,
            "timezone": str(self.timezone or "").strip() or "Asia/Taipei",
            "auto_close_after": bool(self.auto_close_after),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "UploadDefaults":
        payload = data if isinstance(data, dict) else {}
        return cls(
            visibility=str(payload.get("visibility") or "private").strip() or "private",
            category=str(payload.get("category") or "Music").strip() or "Music",
            is_for_kids=bool(payload.get("is_for_kids", False)),
            ai_content=str(payload.get("ai_content") or "yes").strip() or "yes",
            altered_content=str(payload.get("altered_content") or "yes").strip() or "yes",
            notify_subscribers=bool(payload.get("notify_subscribers", False)),
            schedule_date=str(payload.get("schedule_date") or "").strip() or None,
            schedule_time=str(payload.get("schedule_time") or "").strip() or None,
            timezone=str(payload.get("timezone") or "Asia/Taipei").strip() or "Asia/Taipei",
            auto_close_after=bool(payload.get("auto_close_after", False)),
        )


@dataclass
class WindowOverride:
    serial: int
    ypp: str = ""
    visibility: str = ""
    category: str = ""
    kids_content: str = ""
    ai_content: str = ""
    notify_subscribers: str = ""
    schedule_mode: str = ""
    schedule_date: str = ""
    schedule_time: str = ""

    def is_empty(self) -> bool:
        return not any(
            [
                str(self.ypp or "").strip(),
                str(self.visibility or "").strip(),
                str(self.category or "").strip(),
                str(self.kids_content or "").strip(),
                str(self.ai_content or "").strip(),
                str(self.notify_subscribers or "").strip(),
                str(self.schedule_mode or "").strip(),
                str(self.schedule_date or "").strip(),
                str(self.schedule_time or "").strip(),
            ]
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "serial": int(self.serial),
            "ypp": str(self.ypp or "").strip(),
            "visibility": str(self.visibility or "").strip(),
            "category": str(self.category or "").strip(),
            "kids_content": str(self.kids_content or "").strip(),
            "ai_content": str(self.ai_content or "").strip(),
            "notify_subscribers": str(self.notify_subscribers or "").strip(),
            "schedule_mode": str(self.schedule_mode or "").strip(),
            "schedule_date": str(self.schedule_date or "").strip(),
            "schedule_time": str(self.schedule_time or "").strip(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "WindowOverride":
        payload = data if isinstance(data, dict) else {}
        try:
            serial = int(payload.get("serial") or 0)
        except (TypeError, ValueError):
            serial = 0
        return cls(
            serial=serial,
            ypp=str(payload.get("ypp") or "").strip(),
            visibility=str(payload.get("visibility") or "").strip(),
            category=str(payload.get("category") or "").strip(),
            kids_content=str(payload.get("kids_content") or "").strip(),
            ai_content=str(payload.get("ai_content") or "").strip(),
            notify_subscribers=str(payload.get("notify_subscribers") or "").strip(),
            schedule_mode=str(payload.get("schedule_mode") or "").strip(),
            schedule_date=str(payload.get("schedule_date") or "").strip(),
            schedule_time=str(payload.get("schedule_time") or "").strip(),
        )


@dataclass
class GroupJob:
    group_tag: str
    window_serials: list[int]
    source_dir: str
    visual_mode: str = "random"
    prompt_template: str = "default"
    path_template: str = DEFAULT_PATH_TEMPLATE_NAME
    videos_per_window: int = 1
    window_overrides: list[WindowOverride] = field(default_factory=list)
    api_template: str = "default"
    api_preset: str = ""
    visual_settings: Optional[dict[str, Any]] = None
    upload_defaults: UploadDefaults = field(default_factory=UploadDefaults)
    steps: list[str] = field(default_factory=lambda: list(DEFAULT_STEPS))
    modules: list[str] = field(default_factory=lambda: list(DEFAULT_MODULES))
    browser_provider: str = "auto"  # "auto" / "hubstudio" / "bitbrowser"

    def __post_init__(self) -> None:
        clean_steps = _normalize_steps(self.steps)
        clean_modules = _normalize_modules(self.modules)
        modules_from_steps = _modules_from_steps(clean_steps)
        if clean_modules and clean_steps and clean_modules != modules_from_steps:
            if clean_modules != list(DEFAULT_MODULES):
                clean_steps = _steps_from_modules(clean_modules)
            else:
                clean_modules = modules_from_steps
        elif clean_steps and not clean_modules:
            clean_modules = modules_from_steps
        elif clean_modules and not clean_steps:
            clean_steps = _steps_from_modules(clean_modules)

        clean_api_template = str(self.api_template or "").strip()
        clean_api_preset = str(self.api_preset or "").strip()
        if clean_api_preset and not clean_api_template:
            clean_api_template = clean_api_preset
        if clean_api_template and not clean_api_preset:
            clean_api_preset = clean_api_template

        self.api_template = clean_api_template or "default"
        self.api_preset = clean_api_preset or self.api_template
        self.steps = clean_steps or list(DEFAULT_STEPS)
        self.modules = clean_modules or _modules_from_steps(self.steps) or list(DEFAULT_MODULES)

    def get_window_override(self, serial: int) -> WindowOverride | None:
        for override in self.window_overrides:
            if int(override.serial) == int(serial):
                return override
        return None

    def set_window_override(self, override: WindowOverride) -> None:
        clean_override = WindowOverride.from_dict(override.to_dict())
        for index, existing in enumerate(self.window_overrides):
            if int(existing.serial) == int(clean_override.serial):
                if clean_override.is_empty():
                    self.window_overrides.pop(index)
                else:
                    self.window_overrides[index] = clean_override
                return
        if not clean_override.is_empty():
            self.window_overrides.append(clean_override)
            self.window_overrides.sort(key=lambda item: int(item.serial))

    def clear_window_overrides(self) -> None:
        self.window_overrides.clear()

    def to_dict(self) -> dict[str, Any]:
        clean_steps = _normalize_steps(self.steps)
        clean_modules = _normalize_modules(self.modules)
        modules_from_steps = _modules_from_steps(clean_steps)
        if clean_modules and clean_steps and clean_modules != modules_from_steps:
            if clean_modules != list(DEFAULT_MODULES):
                clean_steps = _steps_from_modules(clean_modules)
            else:
                clean_modules = modules_from_steps
        elif clean_steps and not clean_modules:
            clean_modules = modules_from_steps
        elif clean_modules and not clean_steps:
            clean_steps = _steps_from_modules(clean_modules)

        clean_steps = clean_steps or list(DEFAULT_STEPS)
        clean_modules = clean_modules or _modules_from_steps(clean_steps) or list(DEFAULT_MODULES)
        return {
            "group_tag": str(self.group_tag or "").strip(),
            "window_serials": [int(serial) for serial in self.window_serials],
            "source_dir": str(self.source_dir or "").strip(),
            "visual_mode": str(self.visual_mode or "").strip() or "random",
            "prompt_template": str(self.prompt_template or "").strip() or "default",
            "path_template": str(self.path_template or "").strip() or DEFAULT_PATH_TEMPLATE_NAME,
            "videos_per_window": max(1, int(self.videos_per_window or 1)),
            "window_overrides": [item.to_dict() for item in self.window_overrides if not item.is_empty()],
            "api_template": str(self.api_template or "").strip() or "default",
            "api_preset": str(self.api_preset or self.api_template or "").strip() or "default",
            "visual_settings": dict(self.visual_settings or {}) if isinstance(self.visual_settings, dict) else None,
            "upload_defaults": self.upload_defaults.to_dict(),
            "steps": [str(step_name) for step_name in clean_steps],
            "modules": [str(module_name) for module_name in clean_modules],
            "browser_provider": str(self.browser_provider or "auto").strip().lower() or "auto",
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "GroupJob":
        payload = data if isinstance(data, dict) else {}
        window_serials: list[int] = []
        for raw_serial in payload.get("window_serials") or []:
            try:
                window_serials.append(int(raw_serial))
            except (TypeError, ValueError):
                continue

        overrides = [
            WindowOverride.from_dict(item)
            for item in (payload.get("window_overrides") or [])
            if isinstance(item, dict)
        ]
        clean_overrides = [item for item in overrides if int(item.serial) > 0 and not item.is_empty()]
        steps = [
            str(step_name).strip()
            for step_name in (payload.get("steps") or [])
            if str(step_name).strip()
        ]
        modules = [
            str(module_name).strip()
            for module_name in (payload.get("modules") or DEFAULT_MODULES)
            if str(module_name).strip()
        ]
        if not steps and modules:
            steps = _steps_from_modules(modules)
        if not modules and steps:
            modules = _modules_from_steps(steps)
        if not steps:
            steps = list(DEFAULT_STEPS)
        if not modules:
            modules = list(DEFAULT_MODULES)

        visual_settings = payload.get("visual_settings")
        return cls(
            group_tag=str(payload.get("group_tag") or "").strip(),
            window_serials=window_serials,
            source_dir=str(payload.get("source_dir") or "").strip(),
            visual_mode=str(payload.get("visual_mode") or "random").strip() or "random",
            prompt_template=str(payload.get("prompt_template") or "default").strip() or "default",
            path_template=str(payload.get("path_template") or DEFAULT_PATH_TEMPLATE_NAME).strip() or DEFAULT_PATH_TEMPLATE_NAME,
            videos_per_window=max(1, int(payload.get("videos_per_window") or 1)),
            window_overrides=clean_overrides,
            api_template=str(payload.get("api_template") or payload.get("api_preset") or "default").strip() or "default",
            api_preset=str(payload.get("api_preset") or payload.get("api_template") or "default").strip() or "default",
            visual_settings=dict(visual_settings) if isinstance(visual_settings, dict) else None,
            upload_defaults=UploadDefaults.from_dict(payload.get("upload_defaults")),
            steps=steps,
            modules=modules,
            browser_provider=str(payload.get("browser_provider") or "auto").strip().lower() or "auto",
        )


@dataclass
class RunQueue:
    jobs: list[GroupJob] = field(default_factory=list)

    def add_job(self, job: GroupJob) -> None:
        if not isinstance(job, GroupJob):
            raise TypeError("job must be a GroupJob instance")
        self.jobs.append(GroupJob.from_dict(job.to_dict()))

    def remove_job(self, index: int) -> GroupJob:
        if index < 0 or index >= len(self.jobs):
            raise IndexError("job index out of range")
        return self.jobs.pop(index)

    def get_summary(self) -> list[dict[str, Any]]:
        summary: list[dict[str, Any]] = []
        for index, job in enumerate(self.jobs):
            summary.append(
                {
                    "index": index,
                    "group_tag": job.group_tag,
                    "window_count": len(job.window_serials),
                    "window_serials": [int(serial) for serial in job.window_serials],
                    "window_serials_text": ", ".join(str(int(serial)) for serial in job.window_serials),
                    "source_dir": str(job.source_dir or "").strip(),
                    "api_template": str(job.api_template or job.api_preset or "").strip() or "default",
                    "api_preset": str(job.api_preset or job.api_template or "").strip() or "default",
                    "prompt_template": job.prompt_template,
                    "visual_mode": job.visual_mode,
                    "path_template": job.path_template,
                    "videos_per_window": max(1, int(job.videos_per_window or 1)),
                    "steps": [str(step_name) for step_name in job.steps],
                    "window_overrides": [item.to_dict() for item in job.window_overrides if not item.is_empty()],
                    "modules": [str(module_name) for module_name in job.modules],
                    "browser_provider": str(job.browser_provider or "auto").strip().lower() or "auto",
                }
            )
        return summary

    def is_empty(self) -> bool:
        return not self.jobs

    def clear(self) -> None:
        self.jobs.clear()

    def to_dict(self) -> dict[str, Any]:
        return {"jobs": [job.to_dict() for job in self.jobs]}

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "RunQueue":
        payload = data if isinstance(data, dict) else {}
        jobs = [
            GroupJob.from_dict(item)
            for item in (payload.get("jobs") or [])
            if isinstance(item, dict)
        ]
        return cls(jobs=jobs)


def _json_round_trip(value: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(value, ensure_ascii=False))


def normalize_queue_payload(path: pathlib.Path | str | None, data: dict[str, Any]) -> dict[str, Any]:
    _ = pathlib.Path(path) if path else None
    return _json_round_trip(data)
