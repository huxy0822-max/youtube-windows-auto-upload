from __future__ import annotations

import json
import os
import random
import re
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from content_generation import (
    call_image_model,
    generate_content_bundle,
    save_data_url_image,
)
from browser_api import list_browser_envs
from daily_scheduler import (
    RenderOptions,
    VIDEO_BITRATE,
    VIDEO_CODEC,
    _CODEC_EXTRA_ARGS,
    _safe_path_for_ffmpeg,
    build_effect_kwargs,
    clean_incomplete,
    get_audio_duration,
    mark_complete,
)
from effects_library import get_effect
from group_upload_workflow import IMAGE_EXTENSIONS, load_channel_name_map, normalize_mmdd
from metadata_service import get_used_metadata_scope, record_used_metadata
from path_helpers import normalize_scheduler_config
from prompt_studio import (
    default_api_preset,
    default_content_template,
    get_bound_api_preset_name,
    get_bound_content_template_name,
    load_generation_map,
    load_prompt_studio_config,
    save_generation_map,
    save_prompt_studio_config,
)
from utils import get_all_tags, get_tag_info

SCRIPT_DIR = Path(__file__).parent
SCHEDULER_CONFIG_FILE = SCRIPT_DIR / "scheduler_config.json"
PROMPT_STUDIO_FILE = SCRIPT_DIR / "config" / "prompt_studio.json"
CHANNEL_MAPPING_FILE = SCRIPT_DIR / "config" / "channel_mapping.json"

AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg"}
PROGRESS_INTERVAL_SECONDS = 3.0

LogFunc = Callable[[str], None]


def _noop_log(_message: str) -> None:
    return


class WorkflowCancelledError(RuntimeError):
    pass


@dataclass
class ExecutionControl:
    pause_event: threading.Event = field(default_factory=threading.Event)
    cancel_event: threading.Event = field(default_factory=threading.Event)

    def request_pause(self) -> None:
        self.pause_event.set()

    def request_resume(self) -> None:
        self.pause_event.clear()

    def request_cancel(self) -> None:
        self.cancel_event.set()

    def is_paused(self) -> bool:
        return self.pause_event.is_set()

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def check_cancelled(self) -> None:
        if self.cancel_event.is_set():
            raise WorkflowCancelledError("任务已取消")

    def wait_if_paused(self, *, log: LogFunc = _noop_log, label: str = "") -> None:
        warned = False
        while self.pause_event.is_set():
            self.check_cancelled()
            if not warned:
                suffix = f" | {label}" if label else ""
                log(f"[Control] Paused{suffix}")
                warned = True
            time.sleep(0.2)
        if warned:
            suffix = f" | {label}" if label else ""
            log(f"[Control] Resumed{suffix}")


def _suspend_process(pid: int) -> None:
    if pid <= 0 or os.name != "nt":
        return
    import ctypes
    from ctypes import wintypes

    PROCESS_SUSPEND_RESUME = 0x0800
    handle = ctypes.windll.kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
    if not handle:
        raise OSError(f"无法打开进程 {pid} 进行暂停")
    try:
        status = ctypes.windll.ntdll.NtSuspendProcess(wintypes.HANDLE(handle))
        if status != 0:
            raise OSError(f"NtSuspendProcess failed: {status}")
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


def _resume_process(pid: int) -> None:
    if pid <= 0 or os.name != "nt":
        return
    import ctypes
    from ctypes import wintypes

    PROCESS_SUSPEND_RESUME = 0x0800
    handle = ctypes.windll.kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
    if not handle:
        raise OSError(f"无法打开进程 {pid} 进行恢复")
    try:
        status = ctypes.windll.ntdll.NtResumeProcess(wintypes.HANDLE(handle))
        if status != 0:
            raise OSError(f"NtResumeProcess failed: {status}")
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


@dataclass
class WindowInfo:
    tag: str
    serial: int
    channel_name: str = ""
    container_code: str = ""
    is_ypp: bool = False


@dataclass
class WindowTask:
    tag: str
    serial: int
    is_ypp: bool = False
    title: str = ""
    description: str = ""
    visibility: str = "public"
    category: str = "Music"
    made_for_kids: bool = False
    altered_content: bool = True
    notify_subscribers: bool = False
    scheduled_publish_at: str = ""
    schedule_timezone: str = ""
    source_dir: str = ""
    channel_name: str = ""
    container_code: str = ""
    tag_list: list[str] = field(default_factory=list)
    thumbnails: list[str] = field(default_factory=list)
    ab_titles: list[str] = field(default_factory=list)

    def to_plan_dict(self, index: int) -> dict[str, Any]:
        row = {
            "index": index,
            "tag": self.tag,
            "serial": int(self.serial),
            "is_ypp": bool(self.is_ypp),
            "visibility": self.visibility,
            "category": self.category,
            "made_for_kids": bool(self.made_for_kids),
            "altered_content": bool(self.altered_content),
            "notify_subscribers": bool(self.notify_subscribers),
        }
        if self.title.strip():
            row["title"] = self.title.strip()
        if self.description.strip():
            row["description"] = self.description.strip()
        if self.scheduled_publish_at.strip():
            row["scheduled_publish_at"] = self.scheduled_publish_at.strip()
        if self.schedule_timezone.strip():
            row["schedule_timezone"] = self.schedule_timezone.strip()
        if self.source_dir.strip():
            row["source_dir"] = self.source_dir.strip()
        if self.channel_name.strip():
            row["channel_name"] = self.channel_name.strip()
        if self.container_code.strip():
            row["container_code"] = self.container_code.strip()
        if self.tag_list:
            row["tag_list"] = [item for item in self.tag_list if str(item).strip()]
        if self.thumbnails:
            row["thumbnails"] = [item for item in self.thumbnails if str(item).strip()]
        if self.ab_titles:
            row["ab_titles"] = [item for item in self.ab_titles if str(item).strip()]
        return row


@dataclass
class WorkflowDefaults:
    date_mmdd: str
    visibility: str = "public"
    category: str = "Music"
    made_for_kids: bool = False
    altered_content: bool = True
    notify_subscribers: bool = False
    schedule_enabled: bool = False
    schedule_start: str = ""
    schedule_interval_minutes: int = 60
    schedule_timezone: str = "Asia/Taipei (+08:00)"
    metadata_mode: str = "prompt_api"
    generate_text: bool = True
    generate_thumbnails: bool = True
    sync_daily_content: bool = True
    randomize_effects: bool = False
    visual_settings: dict[str, Any] = field(default_factory=dict)

    def upload_defaults(self) -> dict[str, Any]:
        values = {
            "visibility": self.visibility,
            "category": self.category,
            "made_for_kids": bool(self.made_for_kids),
            "altered_content": bool(self.altered_content),
            "notify_subscribers": bool(self.notify_subscribers),
        }
        if self.schedule_enabled and self.visibility == "schedule" and self.schedule_start.strip():
            values["scheduled_publish_at"] = self.schedule_start.strip()
            values["schedule_timezone"] = self.schedule_timezone.strip()
        return values


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _coerce_visual_numeric(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        if text.lower() == "random":
            return "random"
        if re.fullmatch(r"\s*-?\d+(?:\.\d+)?\s*[-~,]\s*-?\d+(?:\.\d+)?\s*", text):
            return text
    return value if value not in (None, "") else default


def _build_render_options_from_defaults(defaults: WorkflowDefaults) -> RenderOptions:
    settings = defaults.visual_settings or {}
    opts = RenderOptions()
    opts.fx_randomize = False

    opts.fx_spectrum = bool(settings.get("spectrum", True))
    opts.fx_timeline = bool(settings.get("timeline", True))
    opts.fx_letterbox = bool(settings.get("letterbox", False))
    opts.fx_zoom = str(settings.get("zoom", "normal") or "normal")
    opts.fx_style = str(settings.get("style", "bar") or "bar")
    opts.fx_color_spectrum = str(settings.get("color_spectrum", "WhiteGold") or "WhiteGold")
    opts.fx_color_timeline = str(settings.get("color_timeline", "WhiteGold") or "WhiteGold")
    opts.fx_spectrum_y = settings.get("spectrum_y", 530)
    opts.fx_spectrum_x = settings.get("spectrum_x", -1)
    opts.fx_spectrum_w = settings.get("spectrum_w", 1200)
    opts.fx_film_grain = bool(settings.get("film_grain", False))
    opts.fx_grain_strength = _coerce_visual_numeric(settings.get("grain_strength", 15), 15)
    opts.fx_vignette = bool(settings.get("vignette", False))
    opts.fx_color_tint = str(settings.get("color_tint", "none") or "none")
    opts.fx_soft_focus = bool(settings.get("soft_focus", False))
    opts.fx_soft_focus_sigma = _coerce_visual_numeric(settings.get("soft_focus_sigma", 1.5), 1.5)
    opts.fx_particle = str(settings.get("particle", "none") or "none")
    opts.fx_particle_opacity = _coerce_visual_numeric(settings.get("particle_opacity", 0.6), 0.6)
    opts.fx_particle_speed = _coerce_visual_numeric(settings.get("particle_speed", 1.0), 1.0)
    opts.fx_text = str(settings.get("text", "") or "")
    opts.fx_text_font = str(settings.get("text_font", "default") or "default")
    opts.fx_text_pos = str(settings.get("text_pos", "center") or "center")
    opts.fx_text_size = _coerce_visual_numeric(settings.get("text_size", 60), 60)
    opts.fx_text_style = str(settings.get("text_style", "Classic") or "Classic")
    return opts


@dataclass
class SimulationOptions:
    simulate_seconds: int = 90
    consume_sources: bool = False
    save_manifest: bool = True


@dataclass
class RenderedItem:
    tag: str
    serial: int
    output_video: str
    source_image: str
    source_audio: str
    title: str
    description: str
    thumbnails: list[str]
    tag_list: list[str]
    ab_titles: list[str]
    effect_desc: str


@dataclass
class WorkflowResult:
    date_mmdd: str
    plan_path: str
    manifest_paths: list[str] = field(default_factory=list)
    output_dirs: list[str] = field(default_factory=list)
    items: list[RenderedItem] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "date_mmdd": self.date_mmdd,
            "plan_path": self.plan_path,
            "manifest_paths": list(self.manifest_paths),
            "output_dirs": list(self.output_dirs),
            "warnings": list(self.warnings),
            "items": [asdict(item) for item in self.items],
        }


def load_scheduler_settings(path: Path = SCHEDULER_CONFIG_FILE) -> dict[str, Any]:
    raw = _read_json(path, {})
    return normalize_scheduler_config(raw, SCRIPT_DIR)


def save_scheduler_settings(config: dict[str, Any], path: Path = SCHEDULER_CONFIG_FILE) -> dict[str, Any]:
    normalized = normalize_scheduler_config(config, SCRIPT_DIR)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return normalized


def load_prompt_settings(path: Path = PROMPT_STUDIO_FILE) -> dict[str, Any]:
    return load_prompt_studio_config(path)


def save_prompt_settings(config: dict[str, Any], path: Path = PROMPT_STUDIO_FILE) -> None:
    save_prompt_studio_config(path, config)


def save_api_preset(
    *,
    name: str,
    payload: dict[str, Any],
    path: Path = PROMPT_STUDIO_FILE,
) -> dict[str, Any]:
    config = load_prompt_settings(path)
    clean_name = name.strip() or "默认API模板"
    api_value = default_api_preset(clean_name)
    api_value.update(payload or {})
    api_value["name"] = clean_name
    config.setdefault("apiPresets", {})[clean_name] = api_value
    save_prompt_settings(config, path)
    return config


def save_content_template(
    *,
    name: str,
    payload: dict[str, Any],
    path: Path = PROMPT_STUDIO_FILE,
) -> dict[str, Any]:
    config = load_prompt_settings(path)
    clean_name = name.strip() or "默认内容模板"
    content_value = default_content_template(clean_name)
    content_value.update(payload or {})
    content_value["name"] = clean_name
    config.setdefault("contentTemplates", {})[clean_name] = content_value
    save_prompt_settings(config, path)
    return config


def bind_group_api_preset(
    *,
    tag: str,
    api_name: str,
    path: Path = PROMPT_STUDIO_FILE,
) -> dict[str, Any]:
    config = load_prompt_settings(path)
    clean_tag = tag.strip()
    clean_name = api_name.strip()
    if clean_tag and clean_name:
        config.setdefault("tagApiBindings", {})[clean_tag] = clean_name
        save_prompt_settings(config, path)
    return config


def bind_group_content_template(
    *,
    tag: str,
    content_name: str,
    path: Path = PROMPT_STUDIO_FILE,
) -> dict[str, Any]:
    config = load_prompt_settings(path)
    clean_tag = tag.strip()
    clean_name = content_name.strip()
    if clean_tag and clean_name:
        config.setdefault("tagBindings", {})[clean_tag] = clean_name
        save_prompt_settings(config, path)
    return config


def get_metadata_root(config: dict[str, Any] | None = None) -> Path:
    cfg = config or load_scheduler_settings()
    raw = str(cfg.get("metadata_root") or cfg.get("base_image_dir") or "").strip()
    return Path(raw) if raw else SCRIPT_DIR / "workspace" / "metadata"


def get_tag_metadata_dir(tag: str, config: dict[str, Any] | None = None, *, root: Path | None = None) -> Path:
    metadata_root = root or get_metadata_root(config)
    clean_tag = str(tag or "").strip()
    if not clean_tag:
        return metadata_root
    if _simple_tag_key(metadata_root.name) == _simple_tag_key(clean_tag):
        return metadata_root
    return metadata_root / clean_tag


def ensure_prompt_presets(
    *,
    api_name: str,
    api_payload: dict[str, Any],
    content_name: str,
    content_payload: dict[str, Any],
    tag: str | None = None,
    bind_api: bool = False,
    bind_content: bool = False,
    path: Path = PROMPT_STUDIO_FILE,
) -> dict[str, Any]:
    config = load_prompt_settings(path)
    clean_api_name = api_name.strip() or "默认API模板"
    clean_content_name = content_name.strip() or "默认内容模板"
    api_value = default_api_preset(clean_api_name)
    api_value.update(api_payload or {})
    api_value["name"] = clean_api_name
    content_value = default_content_template(clean_content_name)
    content_value.update(content_payload or {})
    content_value["name"] = clean_content_name
    config.setdefault("apiPresets", {})[clean_api_name] = api_value
    config.setdefault("contentTemplates", {})[clean_content_name] = content_value
    if tag:
        if bind_api:
            config.setdefault("tagApiBindings", {})[tag] = clean_api_name
        if bind_content:
            config.setdefault("tagBindings", {})[tag] = clean_content_name
    save_prompt_settings(config, path)
    return config


def validate_prompt_bindings(
    *,
    tags: list[str],
    require_text_generation: bool,
    require_image_generation: bool,
    path: Path = PROMPT_STUDIO_FILE,
) -> tuple[list[str], list[str]]:
    config = load_prompt_settings(path)
    errors: list[str] = []
    warnings: list[str] = []
    seen_tags: set[str] = set()

    for raw_tag in tags:
        tag = str(raw_tag or "").strip()
        if not tag or tag in seen_tags:
            continue
        seen_tags.add(tag)

        api_name = get_bound_api_preset_name(config, tag)
        content_name = get_bound_content_template_name(config, tag)
        api_presets = config.get("apiPresets") or {}
        content_templates = config.get("contentTemplates") or {}

        if not api_name:
            errors.append(f"分组 {tag} 未绑定 API 模板。")
            continue
        if not content_name:
            errors.append(f"分组 {tag} 未绑定内容模板。")
            continue

        api_preset = dict(api_presets.get(api_name) or {})
        content_template = dict(content_templates.get(content_name) or {})

        if not api_preset:
            errors.append(f"分组 {tag} 绑定的 API 模板不存在：{api_name}")
            continue
        if not content_template:
            errors.append(f"分组 {tag} 绑定的内容模板不存在：{content_name}")
            continue

        if require_text_generation:
            missing = [
                field
                for field in ("baseUrl", "apiKey", "model")
                if not str(api_preset.get(field) or "").strip()
            ]
            if missing:
                errors.append(f"分组 {tag} 绑定的 API 模板 {api_name} 缺少 {', '.join(missing)}。")

        if require_image_generation and str(api_preset.get("autoImageEnabled") or "0") == "1":
            missing = [
                field
                for field in ("imageBaseUrl", "imageApiKey", "imageModel")
                if not str(api_preset.get(field) or "").strip()
            ]
            if missing:
                errors.append(f"分组 {tag} 绑定的 API 模板 {api_name} 缺少 {', '.join(missing)}。")
        elif require_image_generation and str(api_preset.get("autoImageEnabled") or "0") != "1":
            warnings.append(f"分组 {tag} 的 API 模板 {api_name} 未开启自动出图，将回退为源图缩略图。")

    return errors, warnings


def load_channel_container_map(path: Path = CHANNEL_MAPPING_FILE) -> dict[int, str]:
    data = _read_json(path, {})
    channels = data.get("channels") if isinstance(data, dict) else {}
    if not isinstance(channels, dict):
        return {}

    registry: dict[int, str] = {}
    for container_code, info in channels.items():
        if not isinstance(info, dict):
            continue
        try:
            serial = int(info.get("serial_number") or 0)
        except (TypeError, ValueError):
            continue
        if serial <= 0:
            continue
        clean_code = str(container_code or "").strip()
        if clean_code:
            registry[serial] = clean_code
    return registry


def get_serial_container_catalog() -> dict[int, str]:
    registry = load_channel_container_map(CHANNEL_MAPPING_FILE)
    try:
        for env in list_browser_envs():
            try:
                serial = int(env.get("serialNumber") or 0)
            except (TypeError, ValueError):
                continue
            if serial <= 0:
                continue
            container_code = str(env.get("containerCode") or env.get("browserId") or "").strip()
            if container_code:
                registry[serial] = container_code
    except Exception:
        pass
    return registry


def validate_task_containers(tasks: list[WindowTask]) -> tuple[list[str], list[str]]:
    registry = get_serial_container_catalog()
    errors: list[str] = []
    warnings: list[str] = []
    for task in tasks:
        task_code = task.container_code.strip()
        known_code = str(registry.get(int(task.serial)) or "").strip()
        if task_code and known_code and task_code != known_code:
            warnings.append(
                f"{task.tag}/{task.serial} 的 container_code 与当前 BitBrowser 映射不一致，将以任务值为准。"
            )
            continue
        if task_code or known_code:
            continue
        errors.append(f"{task.tag}/{task.serial} 缺少可用的 container_code，无法稳定上传。")
    return errors, warnings


def get_group_catalog() -> dict[str, list[WindowInfo]]:
    channel_name_map = load_channel_name_map(CHANNEL_MAPPING_FILE)
    container_map = load_channel_container_map(CHANNEL_MAPPING_FILE)
    static_catalog: dict[str, list[WindowInfo]] = {}
    ypp_map: dict[str, set[int]] = {}
    for tag in get_all_tags():
        info = get_tag_info(tag) or {}
        ypp_serials = {int(item) for item in info.get("ypp_serials", [])}
        ypp_map[tag] = ypp_serials
        windows: list[WindowInfo] = []
        for serial in info.get("all_serials", []):
            windows.append(
                WindowInfo(
                    tag=tag,
                    serial=int(serial),
                    channel_name=channel_name_map.get(int(serial), ""),
                    container_code=container_map.get(int(serial), ""),
                    is_ypp=int(serial) in ypp_serials,
                )
            )
        static_catalog[tag] = sorted(windows, key=lambda item: item.serial)

    live_catalog: dict[str, dict[int, WindowInfo]] = {}
    try:
        for env in list_browser_envs():
            serial = env.get("serialNumber")
            if serial is None:
                continue
            clean_serial = int(serial)
            tag = str(env.get("tag") or "").strip() or "未分组"
            channel_name = str(env.get("name") or "").strip() or channel_name_map.get(clean_serial, "")
            container_code = str(
                env.get("containerCode")
                or env.get("browserId")
                or container_map.get(clean_serial)
                or ""
            ).strip()
            is_ypp = clean_serial in ypp_map.get(tag, set())
            group_rows = live_catalog.setdefault(tag, {})
            group_rows[clean_serial] = WindowInfo(
                tag=tag,
                serial=clean_serial,
                channel_name=channel_name,
                container_code=container_code,
                is_ypp=is_ypp,
            )
    except Exception:
        live_catalog = {}

    if live_catalog:
        catalog: dict[str, list[WindowInfo]] = {}
        for tag, rows in live_catalog.items():
            merged: dict[int, WindowInfo] = {item.serial: item for item in static_catalog.get(tag, [])}
            merged.update(rows)
            catalog[tag] = sorted(merged.values(), key=lambda item: item.serial)

        for tag, rows in static_catalog.items():
            if tag not in catalog:
                catalog[tag] = list(rows)

        bindings = get_group_bindings()
        for tag in bindings:
            catalog.setdefault(tag, [])
        return catalog

    catalog = dict(static_catalog)
    bindings = get_group_bindings()
    for tag in bindings:
        catalog.setdefault(tag, [])
    return catalog


def get_group_bindings(config: dict[str, Any] | None = None) -> dict[str, str]:
    cfg = config or load_scheduler_settings()
    bindings = cfg.get("group_source_bindings") or {}
    return {str(tag): str(path) for tag, path in bindings.items() if str(path).strip()}


def set_group_binding(tag: str, folder: str, *, config_path: Path = SCHEDULER_CONFIG_FILE) -> dict[str, Any]:
    config = load_scheduler_settings(config_path)
    bindings = dict(config.get("group_source_bindings") or {})
    clean_tag = str(tag).strip()
    clean_folder = str(folder).strip()
    if clean_tag:
        if clean_folder:
            bindings[clean_tag] = clean_folder
        else:
            bindings.pop(clean_tag, None)
    config["group_source_bindings"] = bindings
    return save_scheduler_settings(config, config_path)


def build_window_plan(tasks: list[WindowTask], defaults: WorkflowDefaults) -> dict[str, Any]:
    groups: dict[str, list[int]] = {}
    ordered: list[dict[str, Any]] = []
    preview_lines: list[str] = []

    for index, task in enumerate(tasks, 1):
        row = task.to_plan_dict(index)
        ordered.append(row)
        groups.setdefault(task.tag, []).append(int(task.serial))

    for tag in groups:
        groups[tag] = sorted(groups[tag])
        preview_lines.append(f"[{tag}] {', '.join(str(item) for item in groups[tag])}")

    if defaults.schedule_enabled and defaults.visibility == "schedule" and defaults.schedule_start.strip():
        start = _parse_schedule_time(defaults.schedule_start)
        for index, row in enumerate(ordered):
            if row.get("scheduled_publish_at"):
                continue
            current = start + timedelta(minutes=max(1, defaults.schedule_interval_minutes) * index)
            row["scheduled_publish_at"] = current.strftime("%Y-%m-%d %H:%M")

    default_upload_options = defaults.upload_defaults()
    if default_upload_options:
        preview_lines.append(
            "默认上传规则: "
            + ", ".join(f"{key}={value}" for key, value in default_upload_options.items())
        )

    return {
        "scope_mode": "manual_windows",
        "default_tag": "",
        "default_upload_options": default_upload_options,
        "schedule_start": defaults.schedule_start.strip(),
        "schedule_interval_minutes": max(1, int(defaults.schedule_interval_minutes)),
        "tasks": ordered,
        "groups": groups,
        "tags": sorted(groups.keys()),
        "warnings": [],
        "preview_lines": preview_lines,
    }


def save_window_plan(plan: dict[str, Any], date_mmdd: str, path: Path | None = None) -> Path:
    target = path or (SCRIPT_DIR / "data" / f"window_upload_plan_{date_mmdd}.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def list_media_files(folder: Path, suffixes: set[str]) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(
        [
            item
            for item in folder.iterdir()
            if item.is_file() and item.suffix.lower() in suffixes and not item.name.startswith(".")
        ],
        key=lambda item: item.name.lower(),
    )


def resolve_task_source_dir(task: WindowTask, config: dict[str, Any]) -> Path:
    if task.source_dir.strip():
        return Path(task.source_dir.strip())
    bindings = get_group_bindings(config)
    path_text = str(bindings.get(task.tag) or "").strip()
    if not path_text:
        raise ValueError(f"{task.tag} 还没有绑定素材目录")
    return Path(path_text)


def _candidate_media_dirs(task: WindowTask, config: dict[str, Any], root_key: str) -> list[Path]:
    candidates: list[Path] = []

    def add(path_value: str | Path | None) -> None:
        text = str(path_value or "").strip()
        if not text:
            return
        path = Path(text)
        normalized = str(path.resolve(strict=False)).lower()
        for existing in candidates:
            if str(existing.resolve(strict=False)).lower() == normalized:
                return
        candidates.append(path)

    add(task.source_dir)
    add(get_group_bindings(config).get(task.tag))
    add(config.get(root_key))
    return candidates


def _pick_media_dir(candidates: list[Path], suffixes: set[str]) -> Path:
    existing_dirs: list[Path] = []
    for candidate in candidates:
        if list_media_files(candidate, suffixes):
            return candidate
        if candidate.exists():
            existing_dirs.append(candidate)
    if existing_dirs:
        return existing_dirs[0]
    return candidates[0] if candidates else Path()


def resolve_task_audio_dir(task: WindowTask, config: dict[str, Any]) -> Path:
    return _pick_media_dir(_candidate_media_dirs(task, config, "music_dir"), AUDIO_EXTENSIONS)


def resolve_task_image_dir(task: WindowTask, config: dict[str, Any]) -> Path:
    return _pick_media_dir(_candidate_media_dirs(task, config, "base_image_dir"), IMAGE_EXTENSIONS)


def _group_tasks_by_media_scope(tasks: list[WindowTask], config: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for task in tasks:
        image_dir = resolve_task_image_dir(task, config)
        audio_dir = resolve_task_audio_dir(task, config)
        key = (
            task.tag,
            str(image_dir.resolve(strict=False)).lower(),
            str(audio_dir.resolve(strict=False)).lower(),
        )
        entry = grouped.setdefault(
            key,
            {
                "tag": task.tag,
                "image_dir": image_dir,
                "audio_dir": audio_dir,
                "tasks": [],
            },
        )
        entry["tasks"].append(task)
    return list(grouped.values())


def validate_group_sources(
    tasks: list[WindowTask],
    *,
    config: dict[str, Any] | None = None,
    log: LogFunc = _noop_log,
) -> tuple[list[str], list[str]]:
    cfg = config or load_scheduler_settings()
    errors: list[str] = []
    warnings: list[str] = []
    for tag in sorted({task.tag for task in tasks}):
        current_tasks = [task for task in tasks if task.tag == tag]
        override_folders = {task.source_dir.strip() for task in current_tasks if task.source_dir.strip()}
        if len(override_folders) > 1:
            warnings.append(f"{tag} 有多个窗口使用不同素材目录，将按窗口覆盖逐个处理。")
        image_dir = resolve_task_image_dir(current_tasks[0], cfg)
        audio_dir = resolve_task_audio_dir(current_tasks[0], cfg)
        if not image_dir.exists():
            errors.append(f"{tag} 的底图目录不存在: {image_dir}")
            continue
        if not audio_dir.exists():
            errors.append(f"{tag} 的音乐目录不存在: {audio_dir}")
            continue
        image_count = len(list_media_files(image_dir, IMAGE_EXTENSIONS))
        audio_count = len(list_media_files(audio_dir, AUDIO_EXTENSIONS))
        if image_count <= 0:
            errors.append(f"{tag} 的底图目录没有图片: {image_dir}")
        if audio_count <= 0:
            errors.append(f"{tag} 的音乐目录没有音频: {audio_dir}")
        log(f"[检查] {tag}: 图片目录 {image_dir} | 图片 {image_count}")
        log(f"[检查] {tag}: 音频目录 {audio_dir} | 音频 {audio_count}")
    return errors, warnings


def _output_dir_matches_tasks(folder: Path, tasks: list[WindowTask]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    manifest_path = folder / "upload_manifest.json"
    if not folder.exists():
        return False, [f"目录不存在: {folder}"]
    if not manifest_path.exists():
        return False, [f"缺少 upload_manifest.json: {manifest_path}"]
    manifest_data = _read_json(manifest_path, {})
    channels = manifest_data.get("channels") if isinstance(manifest_data, dict) else {}
    if not isinstance(channels, dict):
        return False, [f"manifest 格式无效: {manifest_path}"]
    for task in tasks:
        channel = channels.get(str(task.serial))
        if not isinstance(channel, dict):
            errors.append(f"缺少窗口 {task.serial} 的 manifest 数据")
            continue
        video_name = str(channel.get("video") or f"{task.serial}.mp4").strip()
        video_path = Path(video_name)
        if not video_path.is_absolute():
            video_path = folder / video_name
        if not video_path.exists():
            errors.append(f"窗口 {task.serial} 缺少视频文件: {video_path}")
    return not errors, errors


def validate_existing_output_dirs(
    tasks: list[WindowTask],
    *,
    date_mmdd: str,
    config: dict[str, Any] | None = None,
    allow_bootstrap: bool = False,
    log: LogFunc = _noop_log,
) -> tuple[list[str], list[str], dict[str, str]]:
    cfg = config or load_scheduler_settings()
    output_root = Path(cfg["output_root"])
    errors: list[str] = []
    warnings: list[str] = []
    resolved_dirs: dict[str, str] = {}

    if not output_root.exists():
        return [f"输出目录不存在: {output_root}"], warnings, resolved_dirs

    grouped: dict[str, list[WindowTask]] = {}
    for task in tasks:
        grouped.setdefault(task.tag, []).append(task)

    for tag, tag_tasks in grouped.items():
        expected_dir = output_root / f"{date_mmdd}_{tag}"
        matched, details = _output_dir_matches_tasks(expected_dir, tag_tasks)
        if matched:
            resolved_dirs[tag] = str(expected_dir)
            log(f"[检查] {tag}: 现成成品可直接上传 | {expected_dir}")
            continue

        candidates: list[Path] = []
        for folder in sorted(output_root.glob(f"{date_mmdd}_*")):
            if not folder.is_dir():
                continue
            candidate_ok, _candidate_errors = _output_dir_matches_tasks(folder, tag_tasks)
            if candidate_ok:
                candidates.append(folder)

        if len(candidates) == 1:
            resolved_dirs[tag] = str(candidates[0])
            warning = f"{tag} 未在标准目录找到完整成品，将改用 {candidates[0]}"
            warnings.append(warning)
            log(f"[检查] {warning}")
            continue

        if len(candidates) > 1:
            errors.append(
                f"{tag} 找到多个可上传成品目录，请只保留一个: "
                + ", ".join(str(item) for item in candidates)
            )
            continue

        if allow_bootstrap and expected_dir.exists():
            bootstrap_errors: list[str] = []
            for task in tag_tasks:
                existing_video = _find_existing_video(expected_dir, date_mmdd, task.serial, {})
                if not existing_video:
                    bootstrap_errors.append(f"窗口 {task.serial} 缺少现成视频文件")
            if not bootstrap_errors:
                resolved_dirs[tag] = str(expected_dir)
                warning = f"{tag} 将从现成视频目录自举 metadata/manifest: {expected_dir}"
                warnings.append(warning)
                log(f"[检查] {warning}")
                continue
            if details:
                bootstrap_errors = [*details, *bootstrap_errors]
            error_text = f"{tag} 没找到可直接上传的成品目录: {expected_dir}"
            if bootstrap_errors:
                error_text += " | " + "；".join(bootstrap_errors[:3])
            errors.append(error_text)
            continue

        error_text = f"{tag} 没找到可直接上传的成品目录: {expected_dir}"
        if details:
            error_text += " | " + "；".join(details[:3])
        errors.append(error_text)

    return errors, warnings, resolved_dirs


def _parse_schedule_time(value: str) -> datetime:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    raise ValueError("定时发布时间格式应为 YYYY-MM-DD HH:MM")


def _copy_if_needed(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve(strict=False) == target.resolve(strict=False):
        return target
    if target.exists():
        target.unlink()
    shutil.copy2(source, target)
    return target


def _move_to_used(source: Path, used_root: Path, *, tag: str, kind: str) -> Path:
    target_dir = used_root / tag / kind
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / source.name
    if target.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = target_dir / f"{source.stem}_{stamp}{source.suffix.lower()}"
    shutil.move(str(source), str(target))
    return target


def cleanup_old_uploaded_videos(output_root: Path, retention_days: int, log: LogFunc = _noop_log) -> int:
    if retention_days <= 0 or not output_root.exists():
        return 0
    cutoff = datetime.now() - timedelta(days=retention_days)
    cleaned = 0
    for folder in output_root.iterdir():
        if not folder.is_dir():
            continue
        report_path = folder / "upload_report.json"
        if not report_path.exists():
            continue
        if datetime.fromtimestamp(report_path.stat().st_mtime) >= cutoff:
            continue
        for video_path in folder.glob("*.mp4"):
            try:
                video_path.unlink()
                Path(str(video_path) + ".done").unlink(missing_ok=True)
                cleaned += 1
            except Exception:
                continue
    if cleaned:
        log(f"[清理] 已删除超过 {retention_days} 天的本地成品 {cleaned} 个")
    return cleaned

def _save_daily_entry(
    generation_map_path: Path,
    *,
    date_mmdd: str,
    serial: int,
    is_ypp: bool,
    title: str,
    description: str,
    covers: list[str],
    ab_titles: list[str],
) -> None:
    generation_map = load_generation_map(generation_map_path)
    channels = generation_map.setdefault("channels", {})
    channel_info = channels.setdefault(str(serial), {"is_ypp": bool(is_ypp), "days": {}})
    channel_info["is_ypp"] = bool(is_ypp)
    channel_info.setdefault("days", {})
    channel_info["days"][date_mmdd] = {
        "title": title,
        "description": description,
        "covers": covers,
        "ab_titles": ab_titles,
        "set": 1,
    }
    save_generation_map(generation_map_path, generation_map)


def _make_cover_fallbacks(source_image: Path, tag_dir: Path, date_mmdd: str, serial: int, count: int) -> list[Path]:
    covers: list[Path] = []
    for index in range(1, count + 1):
        target = tag_dir / f"{date_mmdd}_{serial}_cover_{index:02d}{source_image.suffix.lower()}"
        covers.append(_copy_if_needed(source_image, target))
    return covers


def _write_manifest(
    *,
    output_dir: Path,
    date_mmdd: str,
    tag: str,
    channels: dict[str, Any],
    source_label: str,
) -> Path:
    manifest = {
        "date": date_mmdd,
        "tag": tag,
        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source": source_label,
        "channels": channels,
    }
    target = output_dir / "upload_manifest.json"
    target.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _build_unique_seed(date_mmdd: str, tag: str, serial: int, *parts: str) -> str:
    core = [str(date_mmdd).strip(), str(tag).strip(), str(serial).strip()]
    core.extend(str(part).strip() for part in parts if str(part).strip())
    core.append(str(time.time_ns()))
    core.append(str(random.randint(1000, 9999)))
    return "|".join(item for item in core if item)


def _describe_effect_kwargs(effect_kwargs: dict[str, Any]) -> str:
    parts = [
        f"style={effect_kwargs.get('style')}",
        f"spectrum={effect_kwargs.get('spectrum')}",
        f"timeline={effect_kwargs.get('timeline')}",
        f"particle={effect_kwargs.get('particle')}",
        f"opacity={effect_kwargs.get('particle_opacity')}",
        f"speed={effect_kwargs.get('particle_speed')}",
        f"text_pos={effect_kwargs.get('text_pos')}",
        f"text_size={effect_kwargs.get('text_size')}",
        f"text_style={effect_kwargs.get('text_style')}",
        f"font={effect_kwargs.get('text_font')}",
    ]
    return " | ".join(parts)


def _resolve_manifest_media_path(folder: Path, value: Any) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else (folder / path)


def _normalized_text_key(value: Any) -> str:
    return "".join(str(value or "").strip().lower().split())


def _pick_fresh_candidate(candidates: list[str], used_values: list[str], fallback: str) -> str:
    cleaned = [str(item).strip() for item in candidates if str(item).strip()]
    blocked = {_normalized_text_key(item) for item in used_values if _normalized_text_key(item)}
    for item in cleaned:
        if _normalized_text_key(item) not in blocked:
            return item
    return cleaned[0] if cleaned else str(fallback or "").strip()


def _is_fresh_value(value: str, used_values: list[str]) -> bool:
    key = _normalized_text_key(value)
    if not key:
        return True
    return key not in {_normalized_text_key(item) for item in used_values if _normalized_text_key(item)}


def _force_unique_text(
    value: str,
    used_values: list[str],
    variants: list[str],
    *,
    max_len: int = 0,
) -> str:
    base = str(value or "").strip()
    if not base:
        base = variants[0] if variants else "备用版本"
    if _is_fresh_value(base, used_values):
        return base
    for variant in variants:
        variant_text = str(variant or "").strip()
        if not variant_text:
            continue
        if max_len > 0:
            room = max_len - len(variant_text) - 1
            if room > 0:
                candidate = f"{base[:room].rstrip()}｜{variant_text}"
            else:
                candidate = variant_text[:max_len]
        else:
            candidate = f"{base}｜{variant_text}"
        if _is_fresh_value(candidate, used_values):
            return candidate
    return base


def _find_existing_video(output_dir: Path, date_mmdd: str, serial: int, channel: dict[str, Any] | None = None) -> Path | None:
    channel = channel or {}
    preferred = _resolve_manifest_media_path(output_dir, channel.get("video"))
    if preferred and preferred.exists():
        return preferred
    candidates = [
        output_dir / f"{date_mmdd}_{serial}.mp4",
        output_dir / f"{serial}.mp4",
    ]
    candidates.extend(sorted(output_dir.glob(f"*_{serial}.mp4")))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_existing_cover_paths(
    metadata_dir: Path,
    date_mmdd: str,
    serial: int,
    *,
    channel: dict[str, Any] | None = None,
    legacy: dict[str, Any] | None = None,
) -> list[Path]:
    channel = channel or {}
    legacy = legacy or {}
    cover_paths: list[Path] = []
    for name in legacy.get("covers", []) or []:
        candidate = metadata_dir / str(name).strip()
        if candidate.exists():
            cover_paths.append(candidate)
    if cover_paths:
        return cover_paths
    for candidate in sorted(metadata_dir.glob(f"{date_mmdd}_{serial}_cover_*")):
        if candidate.is_file():
            cover_paths.append(candidate)
    if cover_paths:
        return cover_paths
    for item in channel.get("thumbnails", []) or []:
        path = Path(item)
        if path.exists():
            cover_paths.append(path)
    return cover_paths


def _load_source_dir_cover_paths(source_dir: str, date_mmdd: str, serial: int) -> list[Path]:
    folder_text = str(source_dir or "").strip()
    if not folder_text:
        return []
    folder = Path(folder_text)
    if not folder.exists() or not folder.is_dir():
        return []

    patterns = [
        f"{date_mmdd}_{serial}_cover_*",
        f"{serial}_cover_*",
        f"{date_mmdd}_{serial}_thumb*",
        f"{serial}_thumb*",
        f"{date_mmdd}_{serial}_thumbnail*",
        f"{serial}_thumbnail*",
        f"*{serial}*cover*",
        f"*{serial}*thumb*",
        f"*{serial}*thumbnail*",
    ]
    matches: list[Path] = []
    seen: set[str] = set()
    for pattern in patterns:
        for candidate in sorted(folder.glob(pattern)):
            if not candidate.is_file() or candidate.suffix.lower() not in IMAGE_EXTENSIONS:
                continue
            key = str(candidate.resolve())
            if key in seen:
                continue
            seen.add(key)
            matches.append(candidate)
    return matches


def _pick_preferred_cover_paths(
    *,
    task: WindowTask,
    metadata_dir: Path,
    date_mmdd: str,
    serial: int,
    channel: dict[str, Any] | None = None,
    legacy: dict[str, Any] | None = None,
) -> tuple[list[Path], str]:
    explicit = [Path(item) for item in task.thumbnails if Path(item).exists()]
    if explicit:
        return explicit, "task"

    source_override = _load_source_dir_cover_paths(task.source_dir, date_mmdd, serial)
    if source_override:
        return source_override, "source_dir"

    existing = _load_existing_cover_paths(
        metadata_dir,
        date_mmdd,
        serial,
        channel=channel,
        legacy=legacy,
    )
    if existing:
        return existing, "existing"
    return [], ""


def _resolve_task_container_code(task: WindowTask, channel: dict[str, Any] | None = None) -> str:
    if task.container_code.strip():
        return task.container_code.strip()
    if isinstance(channel, dict):
        return str(channel.get("container_code") or "").strip()
    return ""


def _generate_prompt_metadata(
    *,
    tag: str,
    task: WindowTask,
    defaults: WorkflowDefaults,
    unique_seed: str,
    title_fallback: str,
    description_fallback: str,
    used_titles: list[str],
    used_descriptions: list[str],
    used_thumbnail_prompts: list[str],
    used_tag_signatures: list[str],
    log: LogFunc,
) -> dict[str, Any]:
    retry_titles = list(used_titles)
    retry_descriptions = list(used_descriptions)
    retry_thumbnail_prompts = list(used_thumbnail_prompts)
    retry_tag_signatures = list(used_tag_signatures)
    last_result: dict[str, Any] | None = None

    for attempt in range(1, 5):
        retry_seed = unique_seed if attempt == 1 else _build_unique_seed(defaults.date_mmdd, tag, task.serial, unique_seed, f"retry-{attempt}")
        bundle = generate_content_bundle(
            PROMPT_STUDIO_FILE,
            tag,
            is_ypp=task.is_ypp,
            unique_seed=retry_seed,
            avoid_titles=retry_titles,
            avoid_descriptions=retry_descriptions,
            avoid_thumbnail_prompts=retry_thumbnail_prompts,
            avoid_tag_signatures=retry_tag_signatures,
        )
        title_candidates = [str(item).strip() for item in bundle.get("titles", []) if str(item).strip()]
        description_candidates = [str(item).strip() for item in bundle.get("descriptions", []) if str(item).strip()]
        thumbnail_prompts = [
            str(item.get("prompt") or "").strip()
            for item in bundle.get("thumbnail_prompts", [])
            if str(item.get("prompt") or "").strip()
        ]
        if thumbnail_prompts:
            first_prompt = _pick_fresh_candidate(
                thumbnail_prompts,
                retry_thumbnail_prompts,
                thumbnail_prompts[0],
            )
            thumbnail_prompts = [first_prompt] + [
                item
                for item in thumbnail_prompts
                if _normalized_text_key(item) != _normalized_text_key(first_prompt)
            ]

        chosen_title = title_fallback.strip() if task.title.strip() else (
            _pick_fresh_candidate(title_candidates, retry_titles, title_fallback) or title_fallback
        )
        chosen_description = _pick_fresh_candidate(
            description_candidates,
            retry_descriptions,
            description_fallback,
        )
        tag_list = [str(item).strip() for item in bundle.get("tag_list", []) if str(item).strip()]
        tag_signature = " | ".join(tag_list)
        ab_titles = (
            [str(item).strip() for item in title_candidates[:3] if str(item).strip()]
            if task.is_ypp
            else []
        )

        fresh_title = task.title.strip() or _is_fresh_value(chosen_title, used_titles)
        fresh_description = _is_fresh_value(chosen_description, used_descriptions)
        fresh_prompt = not thumbnail_prompts or _is_fresh_value(thumbnail_prompts[0], used_thumbnail_prompts)
        fresh_tags = not tag_signature or _is_fresh_value(tag_signature, used_tag_signatures)

        last_result = {
            "bundle": bundle,
            "title": chosen_title,
            "description": chosen_description,
            "tag_list": tag_list,
            "ab_titles": ab_titles,
            "thumbnail_prompts": thumbnail_prompts,
            "attempts": attempt,
        }
        if fresh_title and fresh_description and fresh_prompt and fresh_tags:
            if attempt > 1:
                log(f"[文案] {tag}/{task.serial}: 第 {attempt} 次重试后拿到去重结果")
            return last_result

        retry_titles.append(chosen_title)
        if chosen_description:
            retry_descriptions.append(chosen_description)
        retry_thumbnail_prompts.extend(thumbnail_prompts)
        if tag_signature:
            retry_tag_signatures.append(tag_signature)

    assert last_result is not None
    last_result["title"] = _force_unique_text(
        last_result.get("title", ""),
        used_titles,
        ["今夜慢聽版", "安靜陪伴版", "耐聽夜晚版", f"窗口{task.serial}版"],
        max_len=95,
    )
    last_result["description"] = _force_unique_text(
        last_result.get("description", ""),
        used_descriptions,
        [
            "這一版更偏向安靜、陪伴與長時間播放。",
            "這一版更適合夜晚閱讀、整理房間與放空時慢慢聽。",
            "這一版把情緒放得更穩，適合耐聽型受眾。",
        ],
    )
    prompt_list = [str(item).strip() for item in last_result.get("thumbnail_prompts", []) if str(item).strip()]
    if prompt_list:
        prompt_list[0] = _force_unique_text(
            prompt_list[0],
            used_thumbnail_prompts,
            [
                "Use a noticeably different composition and color palette.",
                "Use a different room setting and warmer lighting.",
                f"Use a distinct layout variation for window {task.serial}.",
            ],
        )
        last_result["thumbnail_prompts"] = prompt_list
    tag_signature = " | ".join(str(item).strip() for item in last_result.get("tag_list", []) if str(item).strip())
    if tag_signature and not _is_fresh_value(tag_signature, used_tag_signatures):
        existing_keys = {_normalized_text_key(entry) for entry in last_result.get("tag_list", [])}
        for extra_tag in [f"{tag}推薦", f"{tag}夜晚版", f"{tag}耐聽版", f"窗口{task.serial}"]:
            if _normalized_text_key(extra_tag) not in existing_keys:
                last_result.setdefault("tag_list", []).append(extra_tag)
                break
    log(f"[警告] {tag}/{task.serial}: 多次重试后仍存在重复倾向，将使用最后一组结果")
    return last_result


def refresh_existing_output_metadata(
    *,
    tasks: list[WindowTask],
    defaults: WorkflowDefaults,
    prepared_output_dirs: dict[str, str],
    config: dict[str, Any] | None = None,
    metadata_dir_overrides: dict[str, str] | None = None,
    control: ExecutionControl | None = None,
    log: LogFunc = _noop_log,
) -> dict[str, str]:
    cfg = config or load_scheduler_settings()
    metadata_root = get_metadata_root(cfg)
    refreshed: dict[str, str] = {}
    grouped: dict[str, list[WindowTask]] = {}
    for task in tasks:
        grouped.setdefault(task.tag, []).append(task)

    for tag, tag_tasks in grouped.items():
        if control:
            control.check_cancelled()
            control.wait_if_paused(log=log, label=f"{tag}/metadata_refresh")
        folder_text = str(prepared_output_dirs.get(tag) or "").strip()
        if not folder_text:
            raise ValueError(f"{tag} 没有可用的现成成品目录")

        output_dir = Path(folder_text)
        manifest_path = output_dir / "upload_manifest.json"
        manifest = _read_json(manifest_path, {})
        channels = manifest.get("channels") if isinstance(manifest, dict) and isinstance(manifest.get("channels"), dict) else {}

        override_metadata = str((metadata_dir_overrides or {}).get(tag) or "").strip()
        tag_metadata_dir = Path(override_metadata) if override_metadata else get_tag_metadata_dir(tag, root=metadata_root)
        tag_metadata_dir.mkdir(parents=True, exist_ok=True)
        generation_map_path = tag_metadata_dir / "generation_map.json"
        current_titles: list[str] = []
        current_descriptions: list[str] = []
        current_thumbnail_prompts: list[str] = []
        current_tag_signatures: list[str] = []

        for task in tag_tasks:
            if control:
                control.check_cancelled()
                control.wait_if_paused(log=log, label=f"{tag}/{task.serial}")
            channel = channels.get(str(task.serial)) if isinstance(channels.get(str(task.serial)), dict) else {}
            video_path = _find_existing_video(output_dir, defaults.date_mmdd, task.serial, channel)
            if not video_path:
                raise ValueError(f"{tag}/{task.serial} 缺少现成视频文件: {video_path}")

            source_image = _resolve_manifest_media_path(output_dir, channel.get("source_image"))
            source_audio = _resolve_manifest_media_path(output_dir, channel.get("source_audio"))
            title = task.title.strip() or str(channel.get("title") or video_path.stem).strip() or video_path.stem
            description = task.description.strip() or str(channel.get("description") or "").strip()
            tag_list = [str(item).strip() for item in task.tag_list if str(item).strip()]
            if not tag_list:
                tag_list = [str(item).strip() for item in channel.get("tag_list", []) if str(item).strip()]
            ab_titles = [str(item).strip() for item in task.ab_titles if str(item).strip()]
            if not ab_titles:
                ab_titles = [
                    str(item).strip()
                    for item in (channel.get("ab_titles") or [])
                    if str(item).strip()
                ]
            cover_paths, cover_source = _pick_preferred_cover_paths(
                task=task,
                metadata_dir=tag_metadata_dir,
                date_mmdd=defaults.date_mmdd,
                serial=task.serial,
                channel=channel,
                legacy={},
            )
            if (
                not defaults.generate_thumbnails
                and cover_source in {"", "existing"}
                and source_image
                and source_image.exists()
            ):
                cover_paths = [source_image]
                cover_source = "source_image"
            thumbnail_prompts: list[str] = []
            bundle = None

            history_scope = get_used_metadata_scope(tag, config=cfg)
            unique_seed = _build_unique_seed(
                defaults.date_mmdd,
                tag,
                task.serial,
                source_audio.stem if source_audio else video_path.stem,
                source_image.stem if source_image else video_path.stem,
                "upload_only_refresh",
            )

            if defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
                if control:
                    control.check_cancelled()
                    control.wait_if_paused(log=log, label=f"{tag}/{task.serial} 文案生成")
                generated = _generate_prompt_metadata(
                    tag=tag,
                    task=task,
                    defaults=defaults,
                    unique_seed=unique_seed,
                    title_fallback=title or video_path.stem,
                    description_fallback=description,
                    used_titles=[*(history_scope.get("titles") or []), *current_titles],
                    used_descriptions=[*(history_scope.get("descriptions") or []), *current_descriptions],
                    used_thumbnail_prompts=[*(history_scope.get("thumbnail_prompts") or []), *current_thumbnail_prompts],
                    used_tag_signatures=[*(history_scope.get("tag_signatures") or []), *current_tag_signatures],
                    log=log,
                )
                bundle = generated["bundle"]
                log(
                    f"[文案] {tag}/{task.serial}: API={bundle['api_preset'].get('name', '')} | "
                    f"模板={bundle['content_template'].get('name', '')} | 重试={generated['attempts']}"
                )
                thumbnail_prompts = list(generated["thumbnail_prompts"])
                if defaults.generate_text:
                    title = generated["title"] or title or video_path.stem
                    description = generated["description"]
                    if not task.tag_list:
                        tag_list = list(generated["tag_list"])
                    if task.is_ypp and not task.ab_titles:
                        ab_titles = list(generated["ab_titles"])

            if defaults.generate_text and not bundle:
                title = str(title or video_path.stem).strip() or video_path.stem
                description = str(description or "").strip()

            cover_count = 3 if task.is_ypp else 1
            if defaults.generate_thumbnails and not task.thumbnails:
                if not cover_paths and bundle and str(bundle["api_preset"].get("autoImageEnabled") or "0") == "1":
                    for cover_index, prompt in enumerate(thumbnail_prompts[:cover_count], 1):
                        if control:
                            control.check_cancelled()
                            control.wait_if_paused(log=log, label=f"{tag}/{task.serial} 缩略图生成")
                        target = tag_metadata_dir / f"{defaults.date_mmdd}_{task.serial}_cover_{cover_index:02d}.png"
                        try:
                            image_result = call_image_model(bundle["api_preset"], prompt)
                            if image_result.get("data_url"):
                                cover_paths.append(save_data_url_image(image_result["data_url"], target))
                                cover_source = "generated"
                                cover_source = "generated"
                                cover_source = "generated"
                        except Exception as exc:
                            log(f"[警告] {tag}/{task.serial} 缩略图重生成失败: {exc}")
                if not cover_paths and source_image and source_image.exists():
                    cover_paths = [source_image]
                    cover_source = "source_image"

            if cover_paths:
                log(
                    f"[缂╃暐鍥?] {tag}/{task.serial}: 鏉ユ簮={cover_source or 'existing'} | "
                    f"{', '.join(str(path) for path in cover_paths[:3])}"
                )

            if cover_paths:
                thumb_preview = ", ".join(str(path) for path in cover_paths[:3])
                log(f"[thumb] {tag}/{task.serial}: source={cover_source or 'existing'} | {thumb_preview}")

            if defaults.generate_text or defaults.generate_thumbnails:
                _save_daily_entry(
                    generation_map_path,
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                    is_ypp=task.is_ypp,
                    title=title,
                    description=description,
                    covers=[path.name for path in cover_paths],
                    ab_titles=ab_titles,
                )

            if defaults.generate_text or defaults.generate_thumbnails:
                record_used_metadata(
                    tag=tag,
                    title=title,
                    description=description,
                    tag_list=tag_list,
                    thumbnail_prompts=thumbnail_prompts,
                    config=cfg,
                    serial=task.serial,
                    date_mmdd=defaults.date_mmdd,
                    thumbnails=cover_paths,
                    source="metadata_refresh",
                    log=log,
                )
                current_titles.append(title)
                if description:
                    current_descriptions.append(description)
                current_thumbnail_prompts.extend(thumbnail_prompts)
                if tag_list:
                    current_tag_signatures.append(" | ".join(tag_list))

            updated_channel = dict(channel)
            updated_channel.update(
                {
                    "video": str(video_path),
                    "channel_name": task.channel_name.strip() or str(channel.get("channel_name") or "").strip(),
                    "container_code": _resolve_task_container_code(task, channel),
                    "title": title,
                    "description": description,
                    "thumbnails": [str(path) for path in cover_paths],
                    "thumbnail_source": cover_source or "existing",
                    "thumbnail_prompts": thumbnail_prompts,
                    "tag_list": tag_list,
                    "is_ypp": bool(task.is_ypp),
                    "ab_titles": ab_titles,
                    "set": int(channel.get("set") or 1),
                    "upload_options": _build_upload_options(task),
                }
            )
            if source_image:
                updated_channel["source_image"] = str(source_image)
            if source_audio:
                updated_channel["source_audio"] = str(source_audio)
            channels[str(task.serial)] = updated_channel

        manifest["date"] = defaults.date_mmdd
        manifest["tag"] = tag
        manifest["created_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        manifest["source"] = "metadata_refresh"
        manifest["channels"] = channels
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        refreshed[tag] = str(output_dir)
        log(f"[上传] {tag}: 已按当前配置重写 manifest | {manifest_path}")

    return refreshed


def _pair_media(
    tasks: list[WindowTask],
    image_folder: Path,
    audio_folder: Path,
    *,
    shuffle: bool = False,
) -> list[tuple[WindowTask, Path, Path]]:
    images = list_media_files(image_folder, IMAGE_EXTENSIONS)
    audio = list_media_files(audio_folder, AUDIO_EXTENSIONS)
    if shuffle:
        random.shuffle(images)
        random.shuffle(audio)
    usable = min(len(tasks), len(images), len(audio))
    return [(tasks[index], images[index], audio[index]) for index in range(usable)]




def _build_upload_options(task: WindowTask) -> dict[str, Any]:
    values = {
        "visibility": task.visibility.strip() or "public",
        "category": task.category.strip() or "Music",
        "made_for_kids": bool(task.made_for_kids),
        "altered_content": bool(task.altered_content),
        "notify_subscribers": bool(task.notify_subscribers),
    }
    if task.scheduled_publish_at.strip():
        values["scheduled_publish_at"] = task.scheduled_publish_at.strip()
    if task.schedule_timezone.strip():
        values["schedule_timezone"] = task.schedule_timezone.strip()
    return values


def _render_with_progress(
    *,
    image_path: Path,
    audio_path: Path,
    output_path: Path,
    filter_complex: str,
    extra_inputs: list[str] | None,
    clip_seconds: int | None,
    log: LogFunc,
    control: ExecutionControl | None = None,
) -> dict[str, Any]:
    from daily_scheduler import FFMPEG_BIN

    safe_image, tmp_img = _safe_path_for_ffmpeg(image_path, "img")
    safe_audio, tmp_aud = _safe_path_for_ffmpeg(audio_path, "aud")
    target_duration = get_audio_duration(audio_path)
    if clip_seconds:
        target_duration = min(target_duration, float(max(clip_seconds, 1)))

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-progress",
        "pipe:1",
        "-nostats",
        "-loop",
        "1",
        "-r",
        "25",
        "-i",
        safe_image,
        "-i",
        safe_audio,
    ]
    if extra_inputs:
        cmd.extend(extra_inputs)
    cmd.extend(
        [
            "-filter_complex",
            filter_complex,
            "-map",
            "[outv]",
            "-map",
            "1:a",
            "-c:v",
            VIDEO_CODEC,
            "-b:v",
            VIDEO_BITRATE,
        ]
        + list(_CODEC_EXTRA_ARGS)
        + [
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-color_range",
            "pc",
            "-aspect",
            "16:9",
            "-c:a",
            "copy",
        ]
    )
    if clip_seconds:
        cmd.extend(["-t", str(int(clip_seconds))])
    cmd.extend(["-shortest", str(output_path)])

    start = time.time()
    last_report = start
    last_ratio = -1.0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    suspended = False

    try:
        while True:
            if control:
                control.check_cancelled()
                if control.is_paused():
                    if not suspended:
                        try:
                            _suspend_process(process.pid)
                            suspended = True
                        except Exception as exc:
                            log(f"[Control] 暂停渲染失败 {output_path.name}: {exc}")
                    control.wait_if_paused(log=log, label=output_path.name)
                    if suspended:
                        try:
                            _resume_process(process.pid)
                            suspended = False
                        except Exception as exc:
                            log(f"[Control] 恢复渲染失败 {output_path.name}: {exc}")
                else:
                    if suspended:
                        try:
                            _resume_process(process.pid)
                        except Exception as exc:
                            log(f"[Control] 恢复渲染失败 {output_path.name}: {exc}")
                        finally:
                            suspended = False
            line = process.stdout.readline() if process.stdout else ""
            if not line:
                if process.poll() is not None:
                    break
                now = time.time()
                if now - last_report >= PROGRESS_INTERVAL_SECONDS:
                    log(f"[渲染] 仍在处理 {output_path.name} | 已耗时 {now - start:.0f}s")
                    last_report = now
                time.sleep(0.2)
                continue

            text = line.strip()
            if text.startswith("out_time_ms="):
                try:
                    out_seconds = int(text.split("=", 1)[1]) / 1_000_000
                except Exception:
                    continue
                now = time.time()
                ratio = 0.0 if target_duration <= 0 else min(out_seconds / target_duration, 1.0)
                if (ratio - last_ratio) >= 0.05 or (now - last_report) >= PROGRESS_INTERVAL_SECONDS:
                    ratio = 0.0 if target_duration <= 0 else min(out_seconds / target_duration, 1.0)
                    log(
                        f"[渲染] {output_path.name} 进度 {ratio * 100:.0f}% "
                        f"({out_seconds:.0f}/{target_duration:.0f}s)"
                    )
                    last_report = now
                    last_ratio = ratio
            elif text.startswith("progress=end"):
                break

        return_code = process.wait(timeout=30)
        if return_code != 0:
            raise RuntimeError(f"ffmpeg 退出码 {return_code}")
        elapsed = time.time() - start
        mark_complete(output_path, duration=target_duration)
        log(f"[渲染] 完成 {output_path.name} | 耗时 {elapsed:.1f}s")
        return {"success": True, "time": elapsed}
    except WorkflowCancelledError:
        try:
            process.terminate()
            process.wait(timeout=5)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
        for _ in range(10):
            try:
                output_path.unlink(missing_ok=True)
                break
            except PermissionError:
                time.sleep(0.2)
        Path(str(output_path) + ".done").unlink(missing_ok=True)
        log(f"[Control] 已取消渲染 {output_path.name}")
        raise
    finally:
        if suspended and process.poll() is None:
            try:
                _resume_process(process.pid)
            except Exception:
                pass
        for temp_path in (tmp_img, tmp_aud):
            if temp_path:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass




def describe_group_bindings(config: dict[str, Any] | None = None) -> str:
    bindings = get_group_bindings(config)
    if not bindings:
        return "还没有任何分组绑定。"
    lines = []
    for tag in get_all_tags():
        if bindings.get(tag):
            lines.append(f"{tag}: {bindings[tag]}")
    return "\n".join(lines) if lines else "还没有任何分组绑定。"


def create_task(
    *,
    tag: str,
    serial: int,
    is_ypp: bool = False,
    title: str = "",
    description: str = "",
    visibility: str = "public",
    category: str = "Music",
    made_for_kids: bool = False,
    altered_content: bool = True,
    notify_subscribers: bool = False,
    scheduled_publish_at: str = "",
    schedule_timezone: str = "",
    source_dir: str = "",
    channel_name: str = "",
    container_code: str = "",
) -> WindowTask:
    return WindowTask(
        tag=str(tag).strip(),
        serial=int(serial),
        is_ypp=bool(is_ypp),
        title=str(title or "").strip(),
        description=str(description or "").strip(),
        visibility=str(visibility or "public").strip() or "public",
        category=str(category or "Music").strip() or "Music",
        made_for_kids=bool(made_for_kids),
        altered_content=bool(altered_content),
        notify_subscribers=bool(notify_subscribers),
        scheduled_publish_at=str(scheduled_publish_at or "").strip(),
        schedule_timezone=str(schedule_timezone or "").strip(),
        source_dir=str(source_dir or "").strip(),
        channel_name=str(channel_name or "").strip(),
        container_code=str(container_code or "").strip(),
    )


def _simple_tag_key(tag: str) -> str:
    normalized = str(tag or "").strip().translate(
        str.maketrans(
            {
                "风": "風",
                "乐": "樂",
                "华": "華",
                "尔": "爾",
                "蓝": "藍",
                "调": "調",
                "门": "門",
                "东": "東",
            }
        )
    )
    return "".join(ch.lower() for ch in normalized if ch.isalnum())


def validate_group_sources(
    tasks: list[WindowTask],
    *,
    config: dict[str, Any] | None = None,
    log: LogFunc = _noop_log,
) -> tuple[list[str], list[str]]:
    cfg = config or load_scheduler_settings()
    errors: list[str] = []
    warnings: list[str] = []
    for scope in _group_tasks_by_media_scope(tasks, cfg):
        tag = str(scope["tag"])
        image_dir = Path(scope["image_dir"])
        audio_dir = Path(scope["audio_dir"])
        scope_tasks = list(scope["tasks"])
        if not image_dir.exists():
            errors.append(f"{tag} 的底图目录不存在: {image_dir}")
            continue
        if not audio_dir.exists():
            errors.append(f"{tag} 的音乐目录不存在: {audio_dir}")
            continue
        image_count = len(list_media_files(image_dir, IMAGE_EXTENSIONS))
        audio_count = len(list_media_files(audio_dir, AUDIO_EXTENSIONS))
        if image_count <= 0:
            errors.append(f"{tag} 的底图目录没有图片: {image_dir}")
        if audio_count <= 0:
            errors.append(f"{tag} 的音乐目录没有音频: {audio_dir}")
        usable = min(image_count, audio_count)
        if usable < len(scope_tasks):
            warnings.append(
                f"{tag} 当前目录只够处理 {usable} 个窗口，但本次计划有 {len(scope_tasks)} 个窗口 | image={image_dir} | audio={audio_dir}"
            )
        log(f"[检查] {tag}: 图片目录 {image_dir} | 图片 {image_count}")
        log(f"[检查] {tag}: 音频目录 {audio_dir} | 音频 {audio_count}")
    return errors, warnings


def _output_dir_score(folder: Path, tag: str) -> int:
    score = 0
    tag_key = _simple_tag_key(tag)
    folder_key = _simple_tag_key(folder.name)
    if folder_key == tag_key or folder_key.endswith(tag_key):
        score += 1
    manifest = _read_json(folder / "upload_manifest.json", {})
    manifest_tag = str((manifest or {}).get("tag") or "").strip()
    if manifest_tag and _simple_tag_key(manifest_tag) == tag_key:
        score += 2
    return score


def _iter_output_dir_candidates(output_root: Path, date_mmdd: str, tag: str) -> list[Path]:
    seen: set[str] = set()
    ordered: list[Path] = []

    def add(candidate: Path) -> None:
        key = str(candidate.resolve(strict=False)).lower()
        if key in seen:
            return
        seen.add(key)
        ordered.append(candidate)

    add(output_root)
    add(output_root / f"{date_mmdd}_{tag}")
    if output_root.exists():
        for folder in sorted(output_root.glob(f"{date_mmdd}_*"), key=lambda item: item.name.lower()):
            if folder.is_dir():
                add(folder)
        for folder in sorted(output_root.iterdir(), key=lambda item: item.name.lower()):
            if folder.is_dir():
                add(folder)
    return ordered


def validate_existing_output_dirs(
    tasks: list[WindowTask],
    *,
    date_mmdd: str,
    config: dict[str, Any] | None = None,
    allow_bootstrap: bool = False,
    log: LogFunc = _noop_log,
) -> tuple[list[str], list[str], dict[str, str]]:
    cfg = config or load_scheduler_settings()
    output_root = Path(cfg["output_root"])
    errors: list[str] = []
    warnings: list[str] = []
    resolved_dirs: dict[str, str] = {}

    if not output_root.exists():
        return [f"输出目录不存在: {output_root}"], warnings, resolved_dirs

    grouped: dict[str, list[WindowTask]] = {}
    for task in tasks:
        grouped.setdefault(task.tag, []).append(task)

    for tag, tag_tasks in grouped.items():
        matched_folders: list[tuple[int, Path]] = []
        last_details: list[str] = []
        for candidate in _iter_output_dir_candidates(output_root, date_mmdd, tag):
            ok, details = _output_dir_matches_tasks(candidate, tag_tasks)
            if ok:
                matched_folders.append((_output_dir_score(candidate, tag), candidate))
            elif candidate == output_root / f"{date_mmdd}_{tag}":
                last_details = details

        if not matched_folders:
            error_text = f"{tag} 没找到可直接上传的成品目录"
            expected_dir = output_root / f"{date_mmdd}_{tag}"
            if allow_bootstrap and expected_dir.exists():
                bootstrap_errors: list[str] = []
                for task in tag_tasks:
                    existing_video = _find_existing_video(expected_dir, date_mmdd, task.serial, {})
                    if not existing_video:
                        bootstrap_errors.append(f"窗口 {task.serial} 缺少现成视频文件")
                if not bootstrap_errors:
                    resolved_dirs[tag] = str(expected_dir)
                    warning = f"{tag} 将从现成视频目录自举 metadata/manifest: {expected_dir}"
                    warnings.append(warning)
                    log(f"[检查] {warning}")
                    continue
            error_text += f": {expected_dir}"
            if last_details:
                error_text += " | " + "；".join(last_details[:3])
            elif allow_bootstrap and expected_dir.exists():
                error_text += " | " + "；".join(bootstrap_errors[:3])
            errors.append(error_text)
            continue

        matched_folders.sort(key=lambda item: (-item[0], str(item[1]).lower()))
        top_score = matched_folders[0][0]
        top_candidates = [folder for score, folder in matched_folders if score == top_score]
        if len(top_candidates) > 1:
            errors.append(
                f"{tag} 找到多个可上传成品目录，请只保留一个或手动清理: "
                + ", ".join(str(item) for item in top_candidates)
            )
            continue

        chosen = top_candidates[0]
        resolved_dirs[tag] = str(chosen)
        if chosen != output_root / f"{date_mmdd}_{tag}":
            warning = f"{tag} 未使用标准目录，改用 {chosen}"
            warnings.append(warning)
            log(f"[检查] {warning}")
        else:
            log(f"[检查] {tag}: 现成成品可直接上传 | {chosen}")

    return errors, warnings, resolved_dirs


def execute_metadata_only_workflow(
    *,
    tasks: list[WindowTask],
    defaults: WorkflowDefaults,
    config: dict[str, Any] | None = None,
    output_dir_overrides: dict[str, str] | None = None,
    metadata_dir_overrides: dict[str, str] | None = None,
    control: ExecutionControl | None = None,
    log: LogFunc = _noop_log,
) -> WorkflowResult:
    if not tasks:
        raise ValueError("至少需要一个窗口任务")

    config = config or load_scheduler_settings()
    metadata_root = get_metadata_root(config)
    output_root = Path(config["output_root"])
    plan = build_window_plan(tasks, defaults)
    plan_path = save_window_plan(plan, defaults.date_mmdd)
    result = WorkflowResult(date_mmdd=defaults.date_mmdd, plan_path=str(plan_path))
    log(f"[计划] 已写入窗口计划: {plan_path}")

    tag_states: dict[str, dict[str, Any]] = {}

    def state_for(tag: str) -> dict[str, Any]:
        state = tag_states.get(tag)
        if state:
            return state
        override_metadata = str((metadata_dir_overrides or {}).get(tag) or "").strip()
        tag_metadata_dir = Path(override_metadata) if override_metadata else get_tag_metadata_dir(tag, root=metadata_root)
        tag_metadata_dir.mkdir(parents=True, exist_ok=True)
        override_output = str((output_dir_overrides or {}).get(tag) or "").strip()
        output_dir = Path(override_output) if override_output else (output_root / f"{defaults.date_mmdd}_{tag}")
        existing_channels: dict[str, Any] = {}
        if output_dir.exists():
            manifest_data = _read_json(output_dir / "upload_manifest.json", {})
            channels = manifest_data.get("channels") if isinstance(manifest_data, dict) else {}
            if isinstance(channels, dict):
                existing_channels = dict(channels)
        state = {
            "tag": tag,
            "metadata_dir": tag_metadata_dir,
            "generation_map_path": tag_metadata_dir / "generation_map.json",
            "output_dir": output_dir,
            "channels": existing_channels,
            "titles": [],
            "descriptions": [],
            "thumbnail_prompts": [],
            "tag_signatures": [],
        }
        tag_states[tag] = state
        result.output_dirs.append(str(tag_metadata_dir))
        return state

    for scope in _group_tasks_by_media_scope(tasks, config):
        tag = str(scope["tag"])
        if control:
            control.check_cancelled()
            control.wait_if_paused(log=log, label=f"{tag}/metadata")
        state = state_for(tag)
        image_dir = Path(scope["image_dir"])
        audio_dir = Path(scope["audio_dir"])
        scope_tasks = list(scope["tasks"])
        if not image_dir.exists():
            raise ValueError(f"{tag} 的底图目录不存在: {image_dir}")
        if not audio_dir.exists():
            raise ValueError(f"{tag} 的音乐目录不存在: {audio_dir}")
        paired = _pair_media(scope_tasks, image_dir, audio_dir, shuffle=False)
        if not paired:
            raise ValueError(f"{tag} 的图/音目录没有可用的图音组合")
        if len(paired) < len(scope_tasks):
            warning = f"{tag} 只够生成 {len(paired)} 组文案，剩余窗口会跳过"
            result.warnings.append(warning)
            log(f"[警告] {warning}")

        for task, source_image, source_audio in paired:
            if control:
                control.check_cancelled()
                control.wait_if_paused(log=log, label=f"{tag}/{task.serial}")
            history_scope = get_used_metadata_scope(tag, config=config)
            unique_seed = _build_unique_seed(
                defaults.date_mmdd,
                tag,
                task.serial,
                source_audio.stem,
                source_image.stem,
                "metadata_only",
            )
            channel = state["channels"].get(str(task.serial), {})
            title = task.title.strip() or f"{defaults.date_mmdd}_{task.serial}"
            description = task.description.strip()
            tag_list = [item for item in task.tag_list if str(item).strip()]
            ab_titles = [item for item in task.ab_titles if str(item).strip()]
            cover_paths = _load_existing_cover_paths(
                state["metadata_dir"],
                defaults.date_mmdd,
                task.serial,
                channel=channel,
                legacy={},
            )
            thumbnail_prompts: list[str] = []
            bundle = None

            if defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
                if control:
                    control.check_cancelled()
                    control.wait_if_paused(log=log, label=f"{tag}/{task.serial} 文案生成")
                generated = _generate_prompt_metadata(
                    tag=tag,
                    task=task,
                    defaults=defaults,
                    unique_seed=unique_seed,
                    title_fallback=title,
                    description_fallback=description,
                    used_titles=[*(history_scope.get("titles") or []), *state["titles"]],
                    used_descriptions=[*(history_scope.get("descriptions") or []), *state["descriptions"]],
                    used_thumbnail_prompts=[*(history_scope.get("thumbnail_prompts") or []), *state["thumbnail_prompts"]],
                    used_tag_signatures=[*(history_scope.get("tag_signatures") or []), *state["tag_signatures"]],
                    log=log,
                )
                bundle = generated["bundle"]
                log(
                    f"[文案] {tag}/{task.serial}: API={bundle['api_preset'].get('name', '')} | 模板={bundle['content_template'].get('name', '')} | 重试={generated['attempts']}"
                )
                if defaults.generate_text:
                    if not task.title.strip():
                        title = generated["title"] or title
                    description = generated["description"]
                    if not tag_list:
                        tag_list = list(generated["tag_list"])
                    if task.is_ypp and not ab_titles:
                        ab_titles = list(generated["ab_titles"])
                thumbnail_prompts = list(generated["thumbnail_prompts"])

            if defaults.generate_thumbnails:
                cover_count = 3 if task.is_ypp else 1
                if bundle and str(bundle["api_preset"].get("autoImageEnabled") or "0") == "1":
                    cover_paths = []
                    for cover_index, prompt in enumerate(thumbnail_prompts[:cover_count], 1):
                        if control:
                            control.check_cancelled()
                            control.wait_if_paused(log=log, label=f"{tag}/{task.serial} 缩略图生成")
                        target = state["metadata_dir"] / f"{defaults.date_mmdd}_{task.serial}_cover_{cover_index:02d}.png"
                        try:
                            image_result = call_image_model(bundle["api_preset"], prompt)
                            if image_result.get("data_url"):
                                cover_paths.append(save_data_url_image(image_result["data_url"], target))
                        except Exception as exc:
                            result.warnings.append(f"{tag}/{task.serial} 缩略图生成失败: {exc}")
                            log(f"[警告] {tag}/{task.serial} 缩略图生成失败: {exc}")
                if not cover_paths:
                    cover_paths = _make_cover_fallbacks(source_image, state["metadata_dir"], defaults.date_mmdd, task.serial, cover_count)

            _save_daily_entry(
                state["generation_map_path"],
                date_mmdd=defaults.date_mmdd,
                serial=task.serial,
                is_ypp=task.is_ypp,
                title=title,
                description=description,
                covers=[path.name for path in cover_paths],
                ab_titles=ab_titles,
            )
            record_used_metadata(
                tag=tag,
                title=title,
                description=description,
                tag_list=tag_list,
                thumbnail_prompts=thumbnail_prompts,
                config=config,
                serial=task.serial,
                date_mmdd=defaults.date_mmdd,
                thumbnails=cover_paths,
                source="metadata_only",
                log=log,
            )
            state["titles"].append(title)
            if description:
                state["descriptions"].append(description)
            state["thumbnail_prompts"].extend(thumbnail_prompts)
            if tag_list:
                state["tag_signatures"].append(" | ".join(tag_list))

            existing_video = _find_existing_video(state["output_dir"], defaults.date_mmdd, task.serial, channel) if state["output_dir"].exists() else None
            if existing_video:
                state["channels"][str(task.serial)] = {
                    "video": str(existing_video),
                    "source_image": str(source_image),
                    "source_audio": str(source_audio),
                    "effect_desc": str(channel.get("effect_desc") or ""),
                    "channel_name": task.channel_name.strip(),
                    "container_code": _resolve_task_container_code(task, channel),
                    "title": title,
                    "description": description,
                    "thumbnails": [str(path) for path in cover_paths],
                    "thumbnail_prompts": thumbnail_prompts,
                    "tag_list": tag_list,
                    "is_ypp": bool(task.is_ypp),
                    "ab_titles": ab_titles,
                    "set": 1,
                    "upload_options": _build_upload_options(task),
                }

            result.items.append(
                RenderedItem(
                    tag=tag,
                    serial=task.serial,
                    output_video=str(existing_video or ""),
                    source_image=str(source_image),
                    source_audio=str(source_audio),
                    title=title,
                    description=description,
                    thumbnails=[str(path) for path in cover_paths],
                    tag_list=tag_list,
                    ab_titles=ab_titles,
                    effect_desc="metadata_only",
                )
            )

    for state in tag_states.values():
        output_dir = Path(state["output_dir"])
        channels = dict(state["channels"])
        if output_dir.exists() and channels:
            manifest_path = _write_manifest(
                output_dir=output_dir,
                date_mmdd=defaults.date_mmdd,
                tag=str(state["tag"]),
                channels=channels,
                source_label="metadata_only",
            )
            result.manifest_paths.append(str(manifest_path))
            log(f"[清单] {state['tag']} metadata manifest 已更新: {manifest_path}")

    return result


def execute_direct_media_workflow(
    *,
    tasks: list[WindowTask],
    defaults: WorkflowDefaults,
    simulation: SimulationOptions | None = None,
    config: dict[str, Any] | None = None,
    control: ExecutionControl | None = None,
    log: LogFunc = _noop_log,
) -> WorkflowResult:
    if not tasks:
        raise ValueError("至少需要一个窗口任务")

    config = config or load_scheduler_settings()
    output_root = Path(config["output_root"])
    metadata_root = get_metadata_root(config)
    used_media_root = Path(config.get("used_media_root") or (SCRIPT_DIR / "workspace" / "AutoTask" / "_used_media"))
    cleanup_old_uploaded_videos(output_root, int(config.get("render_cleanup_days", 5)), log=log)

    plan = build_window_plan(tasks, defaults)
    plan_path = save_window_plan(plan, defaults.date_mmdd)
    result = WorkflowResult(date_mmdd=defaults.date_mmdd, plan_path=str(plan_path))
    log(f"[计划] 已写入窗口计划: {plan_path}")

    tag_states: dict[str, dict[str, Any]] = {}

    def state_for(tag: str) -> dict[str, Any]:
        state = tag_states.get(tag)
        if state:
            return state
        output_dir = output_root / f"{defaults.date_mmdd}_{tag}"
        output_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir = get_tag_metadata_dir(tag, root=metadata_root)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "tag": tag,
            "output_dir": output_dir,
            "metadata_dir": metadata_dir,
            "generation_map_path": metadata_dir / "generation_map.json",
            "manifest_channels": {},
            "titles": [],
            "descriptions": [],
            "thumbnail_prompts": [],
            "tag_signatures": [],
        }
        tag_states[tag] = state
        result.output_dirs.append(str(output_dir))
        return state

    for scope in _group_tasks_by_media_scope(tasks, config):
        tag = str(scope["tag"])
        if control:
            control.check_cancelled()
            control.wait_if_paused(log=log, label=f"{tag}/render")
        state = state_for(tag)
        image_dir = Path(scope["image_dir"])
        audio_dir = Path(scope["audio_dir"])
        scope_tasks = list(scope["tasks"])
        if not image_dir.exists():
            raise ValueError(f"{tag} 的底图目录不存在: {image_dir}")
        if not audio_dir.exists():
            raise ValueError(f"{tag} 的音乐目录不存在: {audio_dir}")
        paired = _pair_media(scope_tasks, image_dir, audio_dir, shuffle=False)
        if not paired:
            raise ValueError(f"{tag} 的图/音目录没有可用的图音组合")
        if len(paired) < len(scope_tasks):
            warning = f"{tag} 只够处理 {len(paired)} 个窗口，剩余窗口会跳过"
            result.warnings.append(warning)
            log(f"[警告] {warning}")

        for task, source_image, source_audio in paired:
            if control:
                control.check_cancelled()
                control.wait_if_paused(log=log, label=f"{tag}/{task.serial}")
            unique_seed = _build_unique_seed(
                defaults.date_mmdd,
                tag,
                task.serial,
                source_audio.stem,
                source_image.stem,
            )
            output_video = Path(state["output_dir"]) / f"{defaults.date_mmdd}_{task.serial}.mp4"
            clean_incomplete(output_video)
            output_video.unlink(missing_ok=True)
            Path(str(output_video) + ".done").unlink(missing_ok=True)

            render_options = _build_render_options_from_defaults(defaults)
            effect_rng = random.Random(f"{unique_seed}|visual")
            effect_kwargs = build_effect_kwargs(render_options, rng=effect_rng)
            duration = get_audio_duration(source_audio)
            filter_complex, effect_desc, extra_inputs = get_effect(duration, rng=effect_rng, **effect_kwargs)
            log(f"[任务] {tag}/{task.serial}: {source_image.name} + {source_audio.name} -> {output_video.name}")
            log(f"[渲染] 编码器={VIDEO_CODEC} | 码率 {VIDEO_BITRATE} | 特效 {effect_desc}")
            log(f"[视觉] {tag}/{task.serial}: {_describe_effect_kwargs(effect_kwargs)}")
            _render_with_progress(
                image_path=source_image,
                audio_path=source_audio,
                output_path=output_video,
                filter_complex=filter_complex,
                extra_inputs=extra_inputs,
                clip_seconds=simulation.simulate_seconds if simulation else None,
                control=control,
                log=log,
            )

            bundle = None
            title = task.title.strip()
            description = task.description.strip()
            tag_list = [item for item in task.tag_list if str(item).strip()]
            ab_titles = [item for item in task.ab_titles if str(item).strip()]
            cover_count = 3 if task.is_ypp else 1
            cover_paths, cover_source = _pick_preferred_cover_paths(
                task=task,
                metadata_dir=Path(state["metadata_dir"]),
                date_mmdd=defaults.date_mmdd,
                serial=task.serial,
            )
            if not defaults.generate_thumbnails and source_image.exists():
                cover_paths = _make_cover_fallbacks(
                    source_image,
                    Path(state["metadata_dir"]),
                    defaults.date_mmdd,
                    task.serial,
                    cover_count,
                )
                cover_source = "source_image"
            thumbnail_prompts: list[str] = []
            history_scope = get_used_metadata_scope(tag, config=config)

            if defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
                if control:
                    control.check_cancelled()
                    control.wait_if_paused(log=log, label=f"{tag}/{task.serial} 文案生成")
                generated = _generate_prompt_metadata(
                    tag=tag,
                    task=task,
                    defaults=defaults,
                    unique_seed=unique_seed,
                    title_fallback=title or output_video.stem,
                    description_fallback=description,
                    used_titles=[*(history_scope.get("titles") or []), *state["titles"]],
                    used_descriptions=[*(history_scope.get("descriptions") or []), *state["descriptions"]],
                    used_thumbnail_prompts=[*(history_scope.get("thumbnail_prompts") or []), *state["thumbnail_prompts"]],
                    used_tag_signatures=[*(history_scope.get("tag_signatures") or []), *state["tag_signatures"]],
                    log=log,
                )
                bundle = generated["bundle"]
                log(
                    f"[文案] {tag}/{task.serial}: API={bundle['api_preset'].get('name', '')} | 模板={bundle['content_template'].get('name', '')} | 重试={generated['attempts']}"
                )
                thumbnail_prompts = list(generated["thumbnail_prompts"])
                if defaults.generate_text:
                    if not task.title.strip():
                        title = generated["title"] or title or output_video.stem
                    description = generated["description"]
                    if not tag_list:
                        tag_list = list(generated["tag_list"])
                    if task.is_ypp and not ab_titles:
                        ab_titles = list(generated["ab_titles"])

            if defaults.generate_text:
                if not bundle:
                    if not title:
                        title = output_video.stem
                    if not description:
                        description = ""
            elif not title:
                title = output_video.stem

            if defaults.generate_thumbnails:
                if bundle and str(bundle["api_preset"].get("autoImageEnabled") or "0") == "1" and not cover_paths:
                    for cover_index, prompt in enumerate(thumbnail_prompts[:cover_count], 1):
                        if control:
                            control.check_cancelled()
                            control.wait_if_paused(log=log, label=f"{tag}/{task.serial} 缩略图生成")
                        target = Path(state["metadata_dir"]) / f"{defaults.date_mmdd}_{task.serial}_cover_{cover_index:02d}.png"
                        try:
                            image_result = call_image_model(bundle["api_preset"], prompt)
                            if image_result.get("data_url"):
                                cover_paths.append(save_data_url_image(image_result["data_url"], target))
                                cover_source = "generated"
                        except Exception as exc:
                            result.warnings.append(f"{tag}/{task.serial} 缩略图生成失败: {exc}")
                            log(f"[警告] {tag}/{task.serial} 缩略图生成失败: {exc}")
                if not cover_paths:
                    cover_paths = _make_cover_fallbacks(
                        source_image,
                        Path(state["metadata_dir"]),
                        defaults.date_mmdd,
                        task.serial,
                        cover_count,
                    )
                    cover_source = "source_image"

            if cover_paths:
                log(
                    f"[缂╃暐鍥?] {tag}/{task.serial}: 鏉ユ簮={cover_source or 'existing'} | "
                    f"{', '.join(str(path) for path in cover_paths[:3])}"
                )

            thumb_preview = ", ".join(str(path) for path in cover_paths[:3]) if cover_paths else ""
            if thumb_preview:
                log(f"[thumb] {tag}/{task.serial}: source={cover_source or 'existing'} | {thumb_preview}")

            if defaults.generate_text or defaults.generate_thumbnails:
                _save_daily_entry(
                    Path(state["generation_map_path"]),
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                    is_ypp=task.is_ypp,
                    title=title,
                    description=description,
                    covers=[path.name for path in cover_paths],
                    ab_titles=ab_titles,
                )

            record_used_metadata(
                tag=tag,
                title=title,
                description=description,
                tag_list=tag_list,
                thumbnail_prompts=thumbnail_prompts,
                config=config,
                serial=task.serial,
                date_mmdd=defaults.date_mmdd,
                thumbnails=cover_paths,
                source="render",
                log=log,
            )
            state["titles"].append(title)
            if description:
                state["descriptions"].append(description)
            state["thumbnail_prompts"].extend(thumbnail_prompts)
            if tag_list:
                state["tag_signatures"].append(" | ".join(tag_list))

            state["manifest_channels"][str(task.serial)] = {
                "video": output_video.name,
                "source_image": str(source_image),
                "source_audio": str(source_audio),
                "effect_desc": effect_desc,
                "channel_name": task.channel_name.strip(),
                "container_code": _resolve_task_container_code(task),
                "title": title,
                "description": description,
                "thumbnails": [str(path) for path in cover_paths],
                "thumbnail_source": cover_source or "existing",
                "thumbnail_prompts": thumbnail_prompts,
                "tag_list": tag_list,
                "is_ypp": bool(task.is_ypp),
                "ab_titles": ab_titles,
                "set": 1,
                "upload_options": _build_upload_options(task),
            }
            result.items.append(
                RenderedItem(
                    tag=tag,
                    serial=task.serial,
                    output_video=str(output_video),
                    source_image=str(source_image),
                    source_audio=str(source_audio),
                    title=title,
                    description=description,
                    thumbnails=[str(path) for path in cover_paths],
                    tag_list=tag_list,
                    ab_titles=ab_titles,
                    effect_desc=effect_desc,
                )
            )

            if simulation and simulation.consume_sources:
                _move_to_used(source_image, used_media_root, tag=tag, kind="images")
                _move_to_used(source_audio, used_media_root, tag=tag, kind="audio")

    if simulation is None or simulation.save_manifest:
        for state in tag_states.values():
            manifest_path = _write_manifest(
                output_dir=Path(state["output_dir"]),
                date_mmdd=defaults.date_mmdd,
                tag=str(state["tag"]),
                channels=dict(state["manifest_channels"]),
                source_label="group_bound_media",
            )
            result.manifest_paths.append(str(manifest_path))
            log(f"[清单] {state['tag']} manifest 已写入: {manifest_path}")

    return result
