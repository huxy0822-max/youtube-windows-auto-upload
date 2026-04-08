# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import unicodedata
import concurrent.futures as _cf
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable

from PIL import Image, ImageDraw, ImageFilter, ImageFont

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
try:
    from group_upload_workflow import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS, load_channel_name_map
except ImportError:
    VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".m4v", ".webm"}
    IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
    def load_channel_name_map(*_args, **_kwargs):  # type: ignore[misc]
        return {}
from metadata_service import BatchDedup, get_used_metadata_scope, record_used_metadata
from path_helpers import normalize_scheduler_config
from prompt_studio import (
    default_api_preset,
    default_content_template,
    find_explicit_api_preset_name,
    find_explicit_content_template_name,
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
VISUAL_PRESETS_FILE = SCRIPT_DIR / "config" / "visual_presets.json"

AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg"}
PROGRESS_INTERVAL_SECONDS = 3.0
THUMBNAIL_CANVAS = (1280, 720)

LogFunc = Callable[[str], None]
ArtifactReadyCallback = Callable[["WindowTask", Path, Path], None]
MetadataReadyCallback = Callable[["WindowTask", Path, dict[str, Any]], None]
_DATACLASS_KWARGS = {"slots": True} if sys.version_info >= (3, 10) else {}


def _noop_log(_message: str) -> None:
    return


class WorkflowCancelledError(RuntimeError):
    pass


@dataclass(**_DATACLASS_KWARGS)
class RenderCodecProfile:
    name: str
    video_codec: str
    video_bitrate: str
    codec_extra_args: list[str] = field(default_factory=list)
    ffmpeg_args: list[str] = field(default_factory=list)


def _double_video_bitrate(text: str) -> str:
    matched = re.fullmatch(r"\s*(\d+)\s*([kKmM])\s*", str(text or "").strip())
    if not matched:
        return str(text or "16000k").strip() or "16000k"
    value = int(matched.group(1)) * 2
    suffix = matched.group(2).lower()
    return f"{value}{suffix}"


def _cpu_render_profile() -> RenderCodecProfile:
    bitrate = str(VIDEO_BITRATE or "8000k").strip() or "8000k"
    cpu_threads = max(2, min(12, int(os.cpu_count() or 8) - 1))
    return RenderCodecProfile(
        name="cpu-x264",
        video_codec="libx264",
        video_bitrate=bitrate,
        codec_extra_args=[
            "-preset",
            "medium",
            "-crf",
            "18",
            "-profile:v",
            "high",
            "-level",
            "4.2",
            "-maxrate",
            bitrate,
            "-bufsize",
            _double_video_bitrate(bitrate),
        ],
        ffmpeg_args=[
            "-threads",
            str(cpu_threads),
            "-filter_threads",
            "2",
            "-filter_complex_threads",
            "2",
        ],
    )


def _default_render_profile() -> RenderCodecProfile:
    return RenderCodecProfile(
        name=f"default-{str(VIDEO_CODEC or 'libx264').strip() or 'libx264'}",
        video_codec=str(VIDEO_CODEC or "libx264").strip() or "libx264",
        video_bitrate=str(VIDEO_BITRATE or "8000k").strip() or "8000k",
        codec_extra_args=list(_CODEC_EXTRA_ARGS),
    )


def _resolve_render_profile(config: dict[str, Any] | None = None) -> RenderCodecProfile:
    preference = str((config or {}).get("render_device_preference") or "auto").strip().lower() or "auto"
    default_profile = _default_render_profile()
    has_hw_encoder = str(default_profile.video_codec).lower() != "libx264"
    if preference == "cpu":
        return _cpu_render_profile()
    if preference == "gpu":
        return default_profile if has_hw_encoder else _cpu_render_profile()
    return default_profile if has_hw_encoder else _cpu_render_profile()


@dataclass(**_DATACLASS_KWARGS)
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
            raise WorkflowCancelledError("Current batch was cancelled.")

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
    if pid <= 0:
        return
    if os.name != "nt":
        # macOS / Linux: 用 SIGSTOP 暂停进程
        import signal
        os.kill(pid, signal.SIGSTOP)
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
    if pid <= 0:
        return
    if os.name != "nt":
        # macOS / Linux: 用 SIGCONT 恢复进程
        import signal
        os.kill(pid, signal.SIGCONT)
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


@dataclass(**_DATACLASS_KWARGS)
class WindowInfo:
    tag: str
    serial: int
    channel_name: str = ""
    is_ypp: bool = False


@dataclass(**_DATACLASS_KWARGS)
class WindowTask:
    tag: str
    serial: int
    quantity: int = 1
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
    tag_list: list[str] = field(default_factory=list)
    thumbnails: list[str] = field(default_factory=list)
    ab_titles: list[str] = field(default_factory=list)
    assigned_image: str = ""
    assigned_audio: str = ""
    slot_index: int = 1
    total_slots: int = 1
    round_index: int = 1

    def to_plan_dict(self, index: int) -> dict[str, Any]:
        row = {
            "index": index,
            "tag": self.tag,
            "serial": int(self.serial),
            "quantity": max(1, int(self.quantity or 1)),
            "is_ypp": bool(self.is_ypp),
            "visibility": self.visibility,
            "category": self.category,
            "made_for_kids": bool(self.made_for_kids),
            "altered_content": bool(self.altered_content),
            "notify_subscribers": bool(self.notify_subscribers),
        }
        if int(self.total_slots or 1) > 1:
            row["slot_index"] = int(self.slot_index or 1)
            row["total_slots"] = int(self.total_slots or 1)
            row["round_index"] = int(self.round_index or self.slot_index or 1)
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
        if self.tag_list:
            row["tag_list"] = [item for item in self.tag_list if str(item).strip()]
        if self.thumbnails:
            row["thumbnails"] = [item for item in self.thumbnails if str(item).strip()]
        if self.ab_titles:
            row["ab_titles"] = [item for item in self.ab_titles if str(item).strip()]
        if self.assigned_image.strip():
            row["assigned_image"] = self.assigned_image.strip()
        if self.assigned_audio.strip():
            row["assigned_audio"] = self.assigned_audio.strip()
        return row


@dataclass(**_DATACLASS_KWARGS)
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


def _parse_toggle(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _visual_preset_token(name: str) -> str:
    text = str(name or "").strip()
    if not text or text.lower() == "none":
        return "none"
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", text).replace(" ", "_").lower()
    if snake in {"megabass", "mega_bass"}:
        return "mega_bass"
    return snake


def _load_visual_presets() -> dict[str, dict[str, Any]]:
    raw = _read_json(VISUAL_PRESETS_FILE, {})
    presets: dict[str, dict[str, Any]] = {}
    for name, payload in raw.items():
        clean_name = str(name or "").strip()
        if clean_name and isinstance(payload, dict):
            presets[clean_name] = dict(payload)
    return presets


def _normalize_visual_metric(value: Any, default: int, *, reference: int) -> int:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        number = float(text)
    except Exception:
        return _coerce_int(text, default)
    if number == -1:
        return -1
    if "." in text and -1.0 < number <= 1.0:
        return int(round(number * reference))
    return int(round(number))


def _resolve_effective_visual_settings(defaults: WorkflowDefaults) -> dict[str, Any]:
    raw_settings = dict(defaults.visual_settings or {})
    visual_mode = str(
        raw_settings.get("visual_mode")
        or raw_settings.get("preset")
        or raw_settings.get("preset_name")
        or "manual"
    ).strip() or "manual"
    if visual_mode == "random":
        settings = dict(raw_settings)
        settings["preset"] = "none"
        settings["visual_mode"] = "random"
        settings["spectrum_asset"] = str(raw_settings.get("spectrum_asset") or "random_asset")
        settings["sticker"] = str(raw_settings.get("sticker") or "random")
        for key in (
            "zoom",
            "style",
            "color_spectrum",
            "color_timeline",
            "color_tint",
            "particle",
            "text_font",
            "text_pos",
            "text_style",
        ):
            settings[key] = "random"
        return settings
    if visual_mode not in {"manual", "none"}:
        preset_payload = dict(_load_visual_presets().get(visual_mode) or {})
        if not preset_payload:
            preset_payload = dict(raw_settings)
        spectrum_raw = str(preset_payload.get("spectrum") or "").strip()
        spectrum_toggle = (
            _parse_toggle(spectrum_raw, True)
            if spectrum_raw.lower() in {"0", "1", "true", "false", "yes", "no", "on", "off"}
            else True
        )
        style_value = str(preset_payload.get("style") or "").strip()
        if not style_value and spectrum_raw.lower() not in {"", "0", "1", "true", "false", "yes", "no", "on", "off"}:
            style_value = spectrum_raw
        particle_value = str(
            preset_payload.get("particles") or preset_payload.get("particle") or "none"
        ).strip()
        sticker_value = str(preset_payload.get("sticker") or preset_payload.get("sticker_file") or "none").strip() or "none"
        text_value = str(preset_payload.get("text") or "").strip()
        if not text_value and sticker_value not in {"", "none", "random"} and not sticker_value.lower().endswith(".png"):
            text_value = sticker_value
            sticker_value = "none"
        bass_pulse = _parse_toggle(preset_payload.get("bass_pulse"), False)
        if particle_value == "bass_pulse":
            particle_value = "none"
            bass_pulse = True
        return {
            "preset": visual_mode,
            "visual_mode": visual_mode,
            "spectrum": spectrum_toggle,
            "spectrum_asset": str(preset_payload.get("spectrum_asset") or "code").strip() or "code",
            "reactive_spectrum_enabled": _parse_toggle(
                preset_payload.get("reactive_spectrum_enabled"), False
            ),
            "reactive_spectrum_preset": str(
                preset_payload.get("reactive_spectrum_preset") or "random"
            ).strip()
            or "random",
            "timeline": _parse_toggle(preset_payload.get("timeline"), True),
            "letterbox": _parse_toggle(preset_payload.get("letterbox"), False),
            "zoom": str(preset_payload.get("zoom") or "normal").strip() or "normal",
            "style": style_value or "bar",
            "color_spectrum": str(
                preset_payload.get("color_spectrum") or preset_payload.get("tint") or "WhiteGold"
            ).strip() or "WhiteGold",
            "color_timeline": str(
                preset_payload.get("color_timeline") or preset_payload.get("tint") or "WhiteGold"
            ).strip() or "WhiteGold",
            "spectrum_y": _normalize_visual_metric(preset_payload.get("spectrum_y"), 530, reference=1080),
            "spectrum_x": _normalize_visual_metric(preset_payload.get("spectrum_x"), -1, reference=1920),
            "spectrum_w": _normalize_visual_metric(preset_payload.get("spectrum_w"), 1200, reference=1920),
            "film_grain": _parse_toggle(preset_payload.get("film_grain"), False),
            "grain_strength": str(preset_payload.get("grain_strength") or "15").strip() or "15",
            "vignette": _parse_toggle(preset_payload.get("vignette"), False),
            "color_tint": str(
                preset_payload.get("color_tint") or preset_payload.get("tint") or "none"
            ).strip() or "none",
            "soft_focus": _parse_toggle(preset_payload.get("soft_focus"), False),
            "soft_focus_sigma": str(preset_payload.get("soft_focus_sigma") or "1.5").strip() or "1.5",
            "particle": particle_value or "none",
            "particle_opacity": str(preset_payload.get("particle_opacity") or "0.6").strip() or "0.6",
            "particle_speed": str(preset_payload.get("particle_speed") or "1.0").strip() or "1.0",
            "sticker": sticker_value,
            "sticker_count": str(preset_payload.get("sticker_count") or "2,4").strip() or "2,4",
            "sticker_opacity": str(preset_payload.get("sticker_opacity") or "0.35,0.55").strip() or "0.35,0.55",
            "text": text_value,
            "text_font": str(preset_payload.get("text_font") or "default").strip() or "default",
            "text_pos": str(preset_payload.get("text_pos") or "center").strip() or "center",
            "text_size": str(preset_payload.get("text_size") or "60").strip() or "60",
            "text_style": str(preset_payload.get("text_style") or "Classic").strip() or "Classic",
            "bass_pulse": bass_pulse,
            "bass_pulse_scale": str(preset_payload.get("bass_pulse_scale") or "0.03").strip() or "0.03",
            "bass_pulse_brightness": str(
                preset_payload.get("bass_pulse_brightness") or "0.04"
            ).strip() or "0.04",
        }
    settings = dict(raw_settings)
    settings["preset"] = "none"
    settings["visual_mode"] = "manual"
    return settings


def _build_render_options_from_defaults(defaults: WorkflowDefaults) -> RenderOptions:
    settings = _resolve_effective_visual_settings(defaults)
    opts = RenderOptions()
    opts.fx_randomize = str(settings.get("visual_mode") or "").strip().lower() == "random"

    opts.fx_spectrum = bool(settings.get("spectrum", True))
    opts.fx_spectrum_asset = str(settings.get("spectrum_asset", "code") or "code")
    opts.fx_reactive_spectrum_enabled = bool(settings.get("reactive_spectrum_enabled", False))
    opts.fx_reactive_spectrum_preset = str(
        settings.get("reactive_spectrum_preset", "random") or "random"
    ).strip() or "random"
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
    opts.fx_grain_strength = settings.get("grain_strength", 15)
    opts.fx_vignette = bool(settings.get("vignette", False))
    opts.fx_color_tint = str(settings.get("color_tint", "none") or "none")
    opts.fx_soft_focus = bool(settings.get("soft_focus", False))
    opts.fx_soft_focus_sigma = settings.get("soft_focus_sigma", 1.5)
    opts.fx_particle = str(settings.get("particle", "none") or "none")
    opts.fx_particle_opacity = settings.get("particle_opacity", 0.6)
    opts.fx_particle_speed = settings.get("particle_speed", 1.0)
    opts.fx_sticker = str(settings.get("sticker", "none") or "none")
    opts.fx_sticker_count = settings.get("sticker_count", "2,4")
    opts.fx_sticker_opacity = settings.get("sticker_opacity", "0.35,0.55")
    opts.fx_text = str(settings.get("text", "") or "")
    opts.fx_text_font = str(settings.get("text_font", "default") or "default")
    opts.fx_text_pos = str(settings.get("text_pos", "center") or "center")
    opts.fx_text_size = settings.get("text_size", 60)
    opts.fx_text_style = str(settings.get("text_style", "Classic") or "Classic")
    opts.fx_visual_preset = _visual_preset_token(str(settings.get("preset", "none") or "none"))
    opts.fx_bass_pulse = bool(settings.get("bass_pulse", False) or opts.fx_visual_preset == "mega_bass")
    opts.fx_bass_pulse_scale = settings.get("bass_pulse_scale", 0.03)
    opts.fx_bass_pulse_brightness = settings.get("bass_pulse_brightness", 0.04)
    return opts


@dataclass(**_DATACLASS_KWARGS)
class SimulationOptions:
    simulate_seconds: int = 90
    consume_sources: bool = False
    save_manifest: bool = True


@dataclass(**_DATACLASS_KWARGS)
class RenderedItem:
    tag: str
    serial: int
    slot_index: int = 1
    total_slots: int = 1
    round_index: int = 1
    output_video: str = ""
    source_image: str = ""
    source_audio: str = ""
    title: str = ""
    description: str = ""
    thumbnails: list[str] = field(default_factory=list)
    tag_list: list[str] = field(default_factory=list)
    ab_titles: list[str] = field(default_factory=list)
    effect_desc: str = ""


def task_slot_token(task: WindowTask) -> str:
    if int(task.total_slots or 1) <= 1:
        return ""
    return f"_{int(task.slot_index or 1):02d}"


def task_runtime_key(task: WindowTask) -> str:
    base = f"{str(task.tag or '').strip()}/{int(task.serial)}"
    if int(task.total_slots or 1) <= 1:
        return base
    return f"{base}#{int(task.slot_index or 1):02d}"


def task_round_label(task: WindowTask) -> str:
    if int(task.total_slots or 1) <= 1:
        return f"{int(task.serial)}"
    return f"{int(task.serial)}[{int(task.slot_index or 1)}/{int(task.total_slots or 1)}]"


def _task_output_dir(base_dir: Path, date_mmdd: str, task: WindowTask) -> Path:
    if int(task.total_slots or 1) <= 1:
        return base_dir
    return base_dir / f"{date_mmdd}_{int(task.serial)}_{int(task.slot_index or 1):02d}"


def _task_video_filename(date_mmdd: str, task: WindowTask) -> str:
    return f"{date_mmdd}_{int(task.serial)}{task_slot_token(task)}.mp4"


def _task_metadata_dir(base_dir: Path, date_mmdd: str, task: WindowTask) -> Path:
    return _task_output_dir(base_dir, date_mmdd, task)


def _expand_window_tasks_by_round(tasks: list[WindowTask]) -> list[WindowTask]:
    expanded: list[WindowTask] = []
    for task in tasks:
        requested_quantity = max(1, int(getattr(task, "quantity", 1) or 1))
        # Already-expanded tasks should pass through unchanged.
        if int(getattr(task, "total_slots", 1) or 1) > 1 or int(getattr(task, "slot_index", 1) or 1) > 1:
            expanded.append(task)
            continue
        for slot_index in range(1, requested_quantity + 1):
            cloned = create_task(
                tag=task.tag,
                serial=task.serial,
                quantity=1,
                is_ypp=task.is_ypp,
                title=task.title,
                description=task.description,
                visibility=task.visibility,
                category=task.category,
                made_for_kids=task.made_for_kids,
                altered_content=task.altered_content,
                notify_subscribers=task.notify_subscribers,
                scheduled_publish_at=task.scheduled_publish_at,
                schedule_timezone=task.schedule_timezone,
                source_dir=task.source_dir,
                channel_name=task.channel_name,
                slot_index=slot_index,
                total_slots=requested_quantity,
                round_index=slot_index,
            )
            cloned.tag_list = [str(item).strip() for item in task.tag_list if str(item).strip()]
            cloned.thumbnails = [str(item).strip() for item in task.thumbnails if str(item).strip()]
            cloned.ab_titles = [str(item).strip() for item in task.ab_titles if str(item).strip()]
            cloned.assigned_image = str(getattr(task, "assigned_image", "") or "").strip()
            cloned.assigned_audio = str(getattr(task, "assigned_audio", "") or "").strip()
            expanded.append(cloned)
    return expanded


def expand_window_tasks_by_round(tasks: list[WindowTask]) -> list[WindowTask]:
    return _expand_window_tasks_by_round(tasks)


@dataclass(**_DATACLASS_KWARGS)
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
        config.setdefault("tagApiBindings", {})[tag] = clean_api_name
        config.setdefault("tagBindings", {})[tag] = clean_content_name
    save_prompt_settings(config, path)
    return config


def get_group_catalog() -> dict[str, list[WindowInfo]]:
    channel_name_map = load_channel_name_map(CHANNEL_MAPPING_FILE)
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
            is_ypp = clean_serial in ypp_map.get(tag, set())
            group_rows = live_catalog.setdefault(tag, {})
            group_rows[clean_serial] = WindowInfo(
                tag=tag,
                serial=clean_serial,
                channel_name=channel_name,
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
    round_groups: dict[int, list[str]] = {}

    for index, task in enumerate(tasks, 1):
        row = task.to_plan_dict(index)
        ordered.append(row)
        groups.setdefault(task.tag, []).append(int(task.serial))
        round_groups.setdefault(int(getattr(task, "round_index", 1) or 1), []).append(
            f"[{task.tag}] {task_round_label(task)}"
        )

    for tag in groups:
        groups[tag] = sorted(groups[tag])
    if len(round_groups) > 1:
        for round_index in sorted(round_groups):
            preview_lines.append(f"Round {round_index}: " + " | ".join(round_groups[round_index]))
    else:
        for tag in groups:
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


def build_runtime_window_plan(
    *,
    tasks: list[WindowTask],
    defaults: WorkflowDefaults,
    config: dict[str, Any] | None = None,
    output_dir_overrides: dict[str, str] | None = None,
    metadata_dir_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    cfg = config or load_scheduler_settings()
    plan = build_window_plan(tasks, defaults)
    output_root = Path(str(cfg.get("output_root") or "").strip())
    metadata_root = get_metadata_root(cfg)
    unique_tags = [str(task.tag or "").strip() for task in tasks if str(task.tag or "").strip()]
    unique_tags = list(dict.fromkeys(unique_tags))
    single_tag_mode = len(unique_tags) == 1
    tag_output_dirs: dict[str, str] = {}
    tag_metadata_dirs: dict[str, str] = {}

    for task in tasks:
        clean_tag = str(task.tag or "").strip()
        if not clean_tag:
            continue

        override_output = str((output_dir_overrides or {}).get(clean_tag) or "").strip()
        override_metadata = str((metadata_dir_overrides or {}).get(clean_tag) or "").strip()

        if override_output:
            resolved_output = Path(override_output)
        elif single_tag_mode:
            resolved_output = output_root
        else:
            resolved_output = output_root / f"{defaults.date_mmdd}_{clean_tag}"

        if override_metadata:
            resolved_metadata = Path(override_metadata)
        elif single_tag_mode:
            resolved_metadata = metadata_root
        else:
            resolved_metadata = metadata_root / clean_tag

        tag_output_dirs.setdefault(clean_tag, str(resolved_output))
        tag_metadata_dirs.setdefault(clean_tag, str(resolved_metadata))

    if tag_output_dirs:
        plan["tag_output_dirs"] = tag_output_dirs
    if tag_metadata_dirs:
        plan["tag_metadata_dirs"] = tag_metadata_dirs
    return plan


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
        raise ValueError(f"{task.tag} has no bound media folder.")
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

    # Priority must follow the current run, not stale historical bindings:
    # explicit task override > current page/global path > saved group binding.
    add(task.source_dir)
    add(config.get(root_key))
    add(get_group_bindings(config).get(task.tag))
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
            warnings.append(f"{tag} uses multiple override folders; windows will be processed with their own overrides.")
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
    target.unlink(missing_ok=True)
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
        log(f"[Cleanup] Deleted {cleaned} local outputs older than {retention_days} days")
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


def _load_cover_font(size: int, *, family: str | None = None) -> ImageFont.ImageFont:
    font_root = Path(__file__).resolve().parent / "fonts"
    family_candidates = {
        "sans": [
            font_root / "noto_sans_tc.otf",
            font_root / "honglei_banshu_ft.ttf",
            font_root / "edu_songti.ttf",
        ],
        "serif": [
            font_root / "noto_serif_tc.otf",
            font_root / "edu_songti.ttf",
            font_root / "noto_sans_tc.otf",
        ],
        "kai": [
            font_root / "edu_kaishu.ttf",
            font_root / "noto_serif_tc.otf",
            font_root / "noto_sans_tc.otf",
        ],
        "hand": [
            font_root / "honglei_banshu_ft.ttf",
            font_root / "edu_kaishu.ttf",
            font_root / "noto_sans_tc.otf",
        ],
        "display": [
            font_root / "honglei_banshu_ft.ttf",
            font_root / "noto_sans_tc.otf",
            font_root / "edu_songti.ttf",
        ],
        "latin": [
            font_root / "DINNextLTPro-Bold.ttf",
            font_root / "BebasNeue-Regular.ttf",
            font_root / "Anton-Regular.ttf",
        ],
    }
    candidates = list(family_candidates.get(str(family or "").strip(), [])) + [
        font_root / "noto_sans_tc.otf",
        font_root / "noto_serif_tc.otf",
        font_root / "edu_songti.ttf",
        font_root / "edu_kaishu.ttf",
        font_root / "honglei_banshu_ft.ttf",
        Path("C:/Windows/Fonts/msyhbd.ttc"),
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/simhei.ttf"),
        Path("C:/Windows/Fonts/simsun.ttc"),
        Path("C:/Windows/Fonts/arialbd.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
    ]
    for candidate in candidates:
        try:
            if candidate.exists():
                return ImageFont.truetype(str(candidate), size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _wrap_cover_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    compact = re.sub(r"\s+", " ", text or "").strip()
    if not compact:
        return []
    tokens = list(compact)
    lines: list[str] = []
    current = ""
    for token in tokens:
        candidate = f"{current}{token}"
        bbox = draw.textbbox((0, 0), candidate, font=font)
        width = bbox[2] - bbox[0]
        if current and width > max_width:
            lines.append(current)
            current = token
        else:
            current = candidate
    if current:
        lines.append(current)
    return lines


def _fit_cover_background(source_image: Path) -> Image.Image:
    canvas_w, canvas_h = THUMBNAIL_CANVAS
    image = Image.open(source_image).convert("RGB")
    scale = max(canvas_w / image.width, canvas_h / image.height)
    resized = image.resize((max(1, int(image.width * scale)), max(1, int(image.height * scale))), Image.LANCZOS)
    left = max(0, (resized.width - canvas_w) // 2)
    top = max(0, (resized.height - canvas_h) // 2)
    return resized.crop((left, top, left + canvas_w, top + canvas_h))


def _normalize_cover_genre_text(text: str) -> str:
    raw_value = unicodedata.normalize("NFKC", str(text or "")).strip()
    raw_value = re.sub(r"^[0-9_+\-\s]+", "", raw_value).strip("-_+ ")
    raw_value = re.sub(r"[（(][^）)]*[）)]", "", raw_value).strip()
    if not raw_value:
        return "純音樂"

    suspicious_fragments = ("锛", "闊", "妯", "�", "Ã", "Â", "Ð", "Ñ", "é", "ä", "ï")
    if any(fragment in raw_value for fragment in suspicious_fragments):
        return "純音樂"

    lowered = raw_value.lower()
    if any(marker in lowered for marker in ("spy", "間諜", "间谍")):
        return "間諜爵士"
    if any(marker in lowered for marker in ("lounge", "酒廊")):
        return "爵士酒廊"
    if any(marker in lowered for marker in ("dark", "暗黑")):
        return "暗黑爵士"
    keyword_map = [
        (("megabass", "mega bass", "bassboost", "bass boosted", "slap house", "edm"), "重低音電音"),
        (("jazz", "爵士"), "爵士純音樂"),
        (("guitar", "木吉他", "吉他"), "木吉他純音樂"),
        (("piano", "鋼琴", "钢琴"), "鋼琴純音樂"),
        (("sax", "薩克斯", "萨克斯"), "薩克斯純音樂"),
        (("ghibli", "吉卜力"), "吉卜力純音樂"),
        (("古風", "古风"), "古風純音樂"),
        (("movie", "電影", "电影", "cinematic"), "電影音樂"),
        (("lofi", "lo-fi", "lo fi"), "LO-FI MUSIC"),
        (("disco", "迪斯科", "的士高"), "DISCO MUSIC"),
    ]
    for markers, label in keyword_map:
        if any(marker in lowered for marker in markers):
            return label

    split_markers = ("｜", "|", "/", "／", "、", "\n", "\r", "，", ",", "。", "；", ";")
    for marker in split_markers:
        if marker in raw_value:
            head = str(raw_value.split(marker, 1)[0] or "").strip()
            if head:
                raw_value = head
                break

    if any(fragment in raw_value for fragment in suspicious_fragments):
        return "純音樂"

    if any(ord(ch) > 127 for ch in raw_value):
        compact = re.sub(r"\s+", "", raw_value).replace("音乐", "音樂")
        compact = compact[:8].strip()
        if not compact:
            return "純音樂"
        if "音樂" not in compact:
            compact = f"{compact}音樂"
        return compact

    ascii_value = re.sub(r"[^A-Za-z0-9+& -]", "", raw_value).strip().upper()
    if not ascii_value:
        return "純音樂"
    if len(ascii_value) > 16:
        ascii_value = ascii_value[:16].rstrip()
    return ascii_value


def _derive_cover_genre_text(tag: str, bundle: dict[str, Any] | None) -> str:
    content_template = (bundle or {}).get("content_template") if isinstance(bundle, dict) else {}
    candidates = [
        str((content_template or {}).get("musicGenre") or "").strip(),
        str((content_template or {}).get("name") or "").strip(),
        str(tag or "").strip(),
    ]
    lowered_candidates = " ".join(candidates).lower()
    if any(marker in lowered_candidates for marker in ("spy", "間諜", "间谍")):
        return "間諜爵士"
    if any(marker in lowered_candidates for marker in ("lounge", "酒廊")):
        return "爵士酒廊"
    if any(marker in lowered_candidates for marker in ("dark", "暗黑")):
        return "暗黑爵士"
    keyword_map = [
        (("megabass", "mega bass", "bassboost", "bass boosted", "slap house", "edm"), "重低音電音"),
        (("jazz", "爵士"), "爵士純音樂"),
        (("guitar", "木吉他", "吉他"), "木吉他純音樂"),
        (("piano", "鋼琴", "钢琴"), "鋼琴純音樂"),
        (("sax", "薩克斯", "萨克斯"), "薩克斯純音樂"),
        (("ghibli", "吉卜力"), "吉卜力純音樂"),
        (("古風", "古风"), "古風純音樂"),
        (("movie", "電影", "电影", "cinematic"), "電影音樂"),
        (("lofi", "lo-fi", "lo fi"), "LO-FI MUSIC"),
        (("disco", "迪斯科", "的士高"), "DISCO MUSIC"),
    ]
    for markers, label in keyword_map:
        if any(marker in lowered_candidates for marker in markers):
            return label
    for candidate in candidates:
        normalized = _normalize_cover_genre_text(candidate)
        if normalized and normalized != "純音樂":
            return normalized
    return "純音樂"


_FALLBACK_COVER_TEMPLATE_IDS = (1, 5, 7, 8, 9, 10, 11, 12, 13, 14)


def _pick_cover_font_family(template_id: int, *, primary: bool) -> str:
    families = {
        1: ("sans", "serif"),
        5: ("serif", "kai"),
        7: ("display", "sans"),
        8: ("serif", "sans"),
        9: ("kai", "serif"),
        10: ("sans", "serif"),
        11: ("display", "serif"),
        12: ("serif", "sans"),
        13: ("kai", "serif"),
        14: ("sans", "kai"),
    }
    fallback = random.choice(("sans", "serif", "kai", "display"))
    pair = families.get(template_id, (fallback, "serif"))
    return pair[0] if primary else pair[1]


def _clean_cover_short_label(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "")).strip()
    raw = re.sub(r"[#＃\"'“”‘’\[\]【】()（）<>《》]+", "", raw)
    raw = re.sub(r"\s+", "", raw).replace("音乐", "音樂")
    if any(fragment in raw for fragment in ("锛", "闊", "妯", "�", "Ã", "Â", "Ð", "Ñ", "é", "ä", "ï")):
        return ""
    return raw[:8].strip()


def _pick_cover_subtitle_text(genre_text: str) -> str:
    normalized = _normalize_cover_genre_text(genre_text or "純音樂")
    lowered = f"{genre_text} {normalized}".lower()
    variants = [normalized]
    if any(marker in lowered for marker in ("spy", "間諜", "间谍")):
        variants.extend(["間諜爵士", "爵士酒廊", "暗黑爵士", "雨夜酒廊", "東亞夜爵士"])
    elif any(marker in lowered for marker in ("lounge", "酒廊")):
        variants.extend(["爵士酒廊", "雨夜酒廊", "深夜酒廊", "東亞夜爵士", "暗黑爵士"])
    elif any(marker in lowered for marker in ("dark", "暗黑")):
        variants.extend(["暗黑爵士", "雨夜爵士", "深夜爵士", "間諜爵士", "電影爵士"])
    elif "jazz" in lowered or "爵士" in lowered:
        variants.extend(["爵士純音樂", "深夜爵士", "雨夜爵士", "東亞夜爵士", "電影爵士"])
    elif any(marker in lowered for marker in ("movie", "cinematic", "電影", "电影")):
        variants.extend(["電影音樂", "電影感純音樂", "雨夜電影感", "沉浸純音樂"])
    elif any(marker in lowered for marker in ("piano", "鋼琴", "钢琴")):
        variants.extend(["鋼琴純音樂", "深夜鋼琴", "沉浸鋼琴"])
    elif any(marker in lowered for marker in ("megabass", "bass", "edm", "重低音")):
        variants.extend(["重低音電音", "深夜低音", "BASS MUSIC"])
    else:
        variants.extend(["沉浸純音樂", "耳機純音樂", "深夜純音樂"])
    clean_variants = []
    seen = set()
    for item in variants:
        clean = _clean_cover_short_label(item)
        if clean and clean not in seen:
            seen.add(clean)
            clean_variants.append(clean)
    return random.choice(clean_variants or ["純音樂"])


def _cover_draw_text_shadow(
    draw: ImageDraw.ImageDraw,
    pos: tuple[float, float],
    text: str,
    font: ImageFont.FreeTypeFont,
    fill: tuple[int, int, int, int],
    *,
    shadow: tuple[int, int, int, int] = (0, 0, 0, 150),
    offset: tuple[int, int] = (6, 8),
    anchor: str = "mm",
) -> None:
    x, y = pos
    ox, oy = offset
    draw.text((x + ox, y + oy), text, font=font, fill=shadow, anchor=anchor)
    draw.text((x, y), text, font=font, fill=fill, anchor=anchor)


def _cover_draw_pill(
    draw: ImageDraw.ImageDraw,
    center: tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
    *,
    fill: tuple[int, int, int, int],
    text_fill: tuple[int, int, int, int],
    pad_x: int = 42,
    pad_y: int = 20,
    radius: int = 26,
    outline: tuple[int, int, int, int] | None = None,
) -> None:
    bbox = draw.textbbox((0, 0), text, font=font, anchor="lt")
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    cx, cy = center
    box = (
        cx - width // 2 - pad_x,
        cy - height // 2 - pad_y,
        cx + width // 2 + pad_x,
        cy + height // 2 + pad_y,
    )
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=2 if outline else 0)
    draw.text((cx, cy), text, font=font, fill=text_fill, anchor="mm")


def _fit_cover_text_to_width(
    draw: ImageDraw.ImageDraw,
    text: str,
    *,
    initial_size: int,
    max_width: int,
    min_size: int = 34,
    family: str | None = None,
) -> tuple[str, ImageFont.FreeTypeFont]:
    clean = re.sub(r"\s+", "", str(text or "").strip())
    if not clean:
        clean = "純音樂"
    for font_size in range(int(initial_size), int(min_size) - 1, -4):
        font = _load_cover_font(font_size, family=family)
        bbox = draw.textbbox((0, 0), clean, font=font, anchor="lt")
        width = bbox[2] - bbox[0]
        if width <= max_width:
            return clean, font
    font = _load_cover_font(min_size, family=family)
    cropped = clean
    while len(cropped) > 2:
        candidate = f"{cropped}…"
        bbox = draw.textbbox((0, 0), candidate, font=font, anchor="lt")
        width = bbox[2] - bbox[0]
        if width <= max_width:
            return candidate, font
        cropped = cropped[:-1]
    return clean[:8], font


def _render_cover_fallback_variant(
    source_image: Path,
    target: Path,
    headline: str,
    genre_text: str,
    template_id: int,
) -> Path:
    image = _fit_cover_background(source_image).filter(ImageFilter.GaussianBlur(radius=1.8))
    draw = ImageDraw.Draw(image, "RGBA")
    canvas_w, canvas_h = image.size
    draw.rectangle((0, 0, canvas_w, canvas_h), fill=(18, 12, 10, 88))

    primary = "超好聽"
    secondary = _pick_cover_subtitle_text(genre_text or "純音樂")
    primary_family = _pick_cover_font_family(template_id, primary=True)
    secondary_family = _pick_cover_font_family(template_id, primary=False)
    primary_t1, primary_font_t1 = _fit_cover_text_to_width(draw, primary, initial_size=202, max_width=1020, min_size=72, family=primary_family)
    primary_t5, primary_font_t5 = _fit_cover_text_to_width(draw, primary, initial_size=184, max_width=760, min_size=68, family=primary_family)
    primary_t7, primary_font_t7 = _fit_cover_text_to_width(draw, primary, initial_size=166, max_width=710, min_size=58, family=primary_family)
    primary_t8, primary_font_t8 = _fit_cover_text_to_width(draw, primary, initial_size=188, max_width=900, min_size=68, family=primary_family)
    primary_t9, primary_font_t9 = _fit_cover_text_to_width(draw, primary, initial_size=170, max_width=980, min_size=62, family=primary_family)
    primary_t10, primary_font_t10 = _fit_cover_text_to_width(draw, primary, initial_size=198, max_width=820, min_size=68, family=primary_family)
    secondary_t1, secondary_font_t1 = _fit_cover_text_to_width(draw, secondary, initial_size=84, max_width=760, min_size=42, family=secondary_family)
    secondary_t5, secondary_font_t5 = _fit_cover_text_to_width(draw, secondary, initial_size=70, max_width=620, min_size=40, family=secondary_family)
    secondary_t7, secondary_font_t7 = _fit_cover_text_to_width(draw, secondary, initial_size=62, max_width=430, min_size=34, family=secondary_family)
    secondary_t8, secondary_font_t8 = _fit_cover_text_to_width(draw, secondary, initial_size=68, max_width=700, min_size=38, family=secondary_family)
    secondary_t9, secondary_font_t9 = _fit_cover_text_to_width(draw, secondary, initial_size=66, max_width=720, min_size=38, family=secondary_family)
    secondary_t10, secondary_font_t10 = _fit_cover_text_to_width(draw, secondary, initial_size=86, max_width=600, min_size=40, family=secondary_family)
    warm_white = (252, 245, 236, 255)
    gold = (225, 198, 146, 255)

    if template_id == 1:
        draw.rounded_rectangle((70, 95, 1210, 635), radius=44, fill=(28, 19, 17, 110), outline=(255, 255, 255, 42), width=2)
        _cover_draw_text_shadow(draw, (640, 340), primary_t1, primary_font_t1, warm_white)
        _cover_draw_pill(draw, (640, 520), secondary_t1, secondary_font_t1, fill=(35, 22, 19, 180), text_fill=warm_white, outline=(255, 255, 255, 24))
    elif template_id == 5:
        draw.ellipse((245, 80, 1035, 670), fill=(28, 18, 15, 120), outline=(255, 255, 255, 28), width=3)
        _cover_draw_text_shadow(draw, (640, 330), primary_t5, primary_font_t5, warm_white)
        _cover_draw_pill(draw, (640, 500), secondary_t5, secondary_font_t5, fill=(22, 13, 11, 182), text_fill=gold)
    elif template_id == 7:
        _cover_draw_pill(draw, (245, 120), "MUSIC COVER", _load_cover_font(28), fill=(35, 23, 19, 160), text_fill=(242, 223, 198, 255), pad_x=24, pad_y=12, radius=20)
        draw.rounded_rectangle((60, 170, 920, 610), radius=36, fill=(19, 11, 10, 132), outline=(255, 255, 255, 28), width=2)
        _cover_draw_text_shadow(draw, (152, 345), primary_t7, primary_font_t7, warm_white, anchor="lm")
        _cover_draw_pill(draw, (285, 505), secondary_t7, secondary_font_t7, fill=(58, 37, 28, 204), text_fill=warm_white)
    elif template_id == 8:
        draw.rounded_rectangle((140, 115, 1140, 605), radius=18, fill=(34, 21, 18, 118), outline=(215, 186, 145, 88), width=2)
        draw.rounded_rectangle((175, 150, 1105, 570), radius=10, fill=(255, 248, 241, 20))
        _cover_draw_text_shadow(draw, (640, 332), primary_t8, primary_font_t8, warm_white)
        _cover_draw_pill(draw, (640, 505), secondary_t8, secondary_font_t8, fill=(34, 24, 18, 202), text_fill=gold, outline=(210, 174, 120, 82))
    elif template_id == 9:
        draw.polygon([(0, 520), (1280, 360), (1280, 720), (0, 720)], fill=(17, 11, 9, 165))
        draw.polygon([(0, 570), (1280, 410), (1280, 720), (0, 720)], fill=(255, 255, 255, 16))
        _cover_draw_text_shadow(draw, (640, 430), primary_t9, primary_font_t9, warm_white)
        _cover_draw_pill(draw, (640, 585), secondary_t9, secondary_font_t9, fill=(30, 18, 14, 195), text_fill=warm_white)
    elif template_id == 10:
        draw.rounded_rectangle((180, 95, 1100, 625), radius=58, fill=(16, 11, 10, 142), outline=(255, 255, 255, 24), width=2)
        draw.rounded_rectangle((280, 420, 1000, 568), radius=42, fill=(255, 248, 241, 18))
        _cover_draw_text_shadow(draw, (640, 298), primary_t10, primary_font_t10, warm_white)
        draw.text((640, 495), secondary_t10, font=secondary_font_t10, fill=gold, anchor="mm")
    elif template_id == 11:
        draw.rounded_rectangle((64, 74, 780, 382), radius=34, fill=(18, 11, 10, 142), outline=(255, 255, 255, 30), width=2)
        _cover_draw_text_shadow(draw, (118, 205), primary_t7, primary_font_t7, warm_white, anchor="lm")
        _cover_draw_pill(draw, (335, 318), secondary_t7, secondary_font_t7, fill=(35, 22, 19, 190), text_fill=gold, outline=(255, 255, 255, 22))
    elif template_id == 12:
        draw.rounded_rectangle((500, 74, 1216, 382), radius=34, fill=(18, 11, 10, 142), outline=(255, 255, 255, 30), width=2)
        _cover_draw_text_shadow(draw, (1162, 205), primary_t7, primary_font_t7, warm_white, anchor="rm")
        _cover_draw_pill(draw, (945, 318), secondary_t7, secondary_font_t7, fill=(35, 22, 19, 190), text_fill=gold, outline=(255, 255, 255, 22))
    elif template_id == 13:
        draw.rounded_rectangle((64, 350, 790, 650), radius=34, fill=(18, 11, 10, 155), outline=(255, 255, 255, 30), width=2)
        _cover_draw_text_shadow(draw, (118, 478), primary_t7, primary_font_t7, warm_white, anchor="lm")
        _cover_draw_pill(draw, (335, 590), secondary_t7, secondary_font_t7, fill=(35, 22, 19, 200), text_fill=warm_white, outline=(255, 255, 255, 22))
    elif template_id == 14:
        draw.rounded_rectangle((490, 350, 1216, 650), radius=34, fill=(18, 11, 10, 155), outline=(255, 255, 255, 30), width=2)
        _cover_draw_text_shadow(draw, (1162, 478), primary_t7, primary_font_t7, warm_white, anchor="rm")
        _cover_draw_pill(draw, (945, 590), secondary_t7, secondary_font_t7, fill=(35, 22, 19, 200), text_fill=warm_white, outline=(255, 255, 255, 22))
    else:
        raise ValueError(f"Unsupported fallback cover template: {template_id}")

    vignette = Image.new("L", image.size, 0)
    vdraw = ImageDraw.Draw(vignette)
    vdraw.ellipse((-140, -90, canvas_w + 140, canvas_h + 90), fill=175)
    vignette = vignette.filter(ImageFilter.GaussianBlur(50))
    dark = Image.new("RGBA", image.size, (0, 0, 0, 68))
    dark.putalpha(vignette.point(lambda value: max(0, 190 - value)))
    final_image = Image.alpha_composite(image.convert("RGBA"), dark)
    target.parent.mkdir(parents=True, exist_ok=True)
    final_image.convert("RGB").save(target, quality=95)
    return target


def _render_cover_fallback(source_image: Path, target: Path, headline: str, genre_text: str) -> Path:
    template_id = random.choice(_FALLBACK_COVER_TEMPLATE_IDS)
    return _render_cover_fallback_variant(source_image, target, headline, genre_text or "純音樂", template_id)


def _thumbnail_error_needs_balance_hint(exc: Exception | None) -> bool:
    if exc is None:
        return False
    text = str(exc).lower()
    markers = (
        "quota",
        "insufficient",
        "balance",
        "billing",
        "credit",
        "payment",
        "402",
        "余额",
        "额度",
        "欠费",
    )
    return any(marker in text for marker in markers)


def _make_cover_fallbacks(
    source_image: Path,
    tag_dir: Path,
    date_mmdd: str,
    serial: int,
    count: int,
    *,
    headline: str = "",
    genre_text: str = "",
) -> list[Path]:
    covers: list[Path] = []
    for index in range(1, count + 1):
        target = tag_dir / f"{date_mmdd}_{serial}_cover_{index:02d}.jpg"
        covers.append(_render_cover_fallback(source_image, target, headline, genre_text))
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


def _resolve_manifest_media_path(folder: Path, value: Any) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else (folder / path)


def _folder_matches_serial(folder: Path, serial: int) -> bool:
    text = str(folder.name or "").strip()
    if not text:
        return False
    return bool(re.search(rf"(^|[_\-\s]){int(serial)}($|[_\-\s])", text))


def _iter_media_search_dirs(base_dir: Path, serial: int) -> list[Path]:
    if not base_dir.exists() or not base_dir.is_dir():
        return []
    ordered: list[Path] = []
    seen: set[str] = set()

    def register(folder: Path) -> None:
        try:
            key = str(folder.resolve(strict=False)).lower()
        except Exception:
            key = str(folder).lower()
        if key in seen or not folder.exists() or not folder.is_dir():
            return
        seen.add(key)
        ordered.append(folder)

    register(base_dir)
    for child in sorted(
        (item for item in base_dir.iterdir() if item.is_dir() and _folder_matches_serial(item, serial)),
        key=lambda item: item.name.lower(),
    ):
        register(child)
    for child in sorted(
        (
            item
            for item in base_dir.rglob("*")
            if item.is_dir() and _folder_matches_serial(item, serial)
        ),
        key=lambda item: (len(item.relative_to(base_dir).parts), item.name.lower()),
    ):
        register(child)
    return ordered


def _first_media_file(folder: Path, suffixes: set[str]) -> Path | None:
    for item in sorted(folder.iterdir(), key=lambda value: value.name.lower()):
        if item.is_file() and item.suffix.lower() in suffixes:
            return item
    return None


def _normalized_text_key(value: Any) -> str:
    return "".join(str(value or "").strip().lower().split())


def _duplicate_prefix_key(value: Any, prefix_chars: int = 28) -> str:
    normalized = _normalized_text_key(value)
    return normalized[:prefix_chars]


def _is_effectively_duplicate(value: str, used_values: list[str]) -> bool:
    key = _normalized_text_key(value)
    if not key:
        return False
    prefix = _duplicate_prefix_key(value)
    for used in used_values:
        used_key = _normalized_text_key(used)
        if not used_key:
            continue
        if key == used_key:
            return True
        if prefix and prefix == _duplicate_prefix_key(used):
            return True
        if SequenceMatcher(None, key, used_key).ratio() >= 0.92:
            return True
    return False


def _pick_fresh_candidate(candidates: list[str], used_values: list[str], fallback: str) -> str:
    cleaned = [str(item).strip() for item in candidates if str(item).strip()]
    for item in cleaned:
        if not _is_effectively_duplicate(item, used_values):
            return item
    return cleaned[0] if cleaned else str(fallback or "").strip()


def _is_fresh_value(value: str, used_values: list[str]) -> bool:
    return not _is_effectively_duplicate(value, used_values)


def _force_unique_text(
    value: str,
    used_values: list[str],
    variants: list[str],
    *,
    max_len: int = 0,
) -> str:
    base = str(value or "").strip()
    if not base:
        base = variants[0] if variants else "澶囩敤鐗堟湰"
    if _is_fresh_value(base, used_values):
        return base
    for variant in variants:
        variant_text = str(variant or "").strip()
        if not variant_text:
            continue
        if max_len > 0:
            room = max_len - len(variant_text) - 1
            if room > 0:
                candidate = f"{base[:room].rstrip()} {variant_text}"
            else:
                candidate = variant_text[:max_len]
        else:
            candidate = f"{base} {variant_text}"
        if _is_fresh_value(candidate, used_values):
            return candidate
    return base


def _find_existing_video(
    output_dir: Path,
    date_mmdd: str,
    serial: int,
    channel: dict[str, Any] | None = None,
    task: WindowTask | None = None,
) -> Path | None:
    channel = channel or {}
    preferred = _resolve_manifest_media_path(output_dir, channel.get("video"))
    if preferred and preferred.exists():
        return preferred
    for folder in _iter_media_search_dirs(output_dir, serial) or [output_dir]:
        candidates: list[Path] = []
        if task is not None:
            slot_suffix = task_slot_token(task)
            candidates.extend(
                [
                    folder / _task_video_filename(date_mmdd, task),
                    folder / f"{serial}{slot_suffix}.mp4",
                ]
            )
            if slot_suffix:
                candidates.extend(sorted(folder.glob(f"*_{serial}{slot_suffix}.mp4")))
                candidates.extend(sorted(folder.glob(f"*_{serial}_{int(task.slot_index or 1):02d}.mp4")))
        candidates.extend(
            [
                folder / f"{date_mmdd}_{serial}.mp4",
                folder / f"{serial}.mp4",
            ]
        )
        candidates.extend(sorted(folder.glob(f"*_{serial}.mp4")))
        for candidate in candidates:
            if candidate.exists() and candidate.is_file():
                return candidate
        fallback_video = _first_media_file(folder, VIDEO_EXTENSIONS)
        if fallback_video is not None:
            return fallback_video
    return None


def _claim_bootstrap_source_video(
    *,
    task: WindowTask,
    output_dir: Path,
    date_mmdd: str,
    serial: int,
    claimed_videos: dict[str, set[str]],
) -> Path | None:
    folder_text = str(task.source_dir or "").strip()
    if not folder_text:
        return None
    source_dir = Path(folder_text)
    if not source_dir.exists() or not source_dir.is_dir():
        return None
    source_key = str(source_dir.resolve(strict=False)).lower()
    claimed = claimed_videos.setdefault(source_key, set())
    seen: set[str] = set()
    for folder in _iter_media_search_dirs(source_dir, serial) or [source_dir]:
        preferred_candidates: list[Path] = [
            folder / f"{date_mmdd}_{serial}.mp4",
            folder / f"{serial}.mp4",
        ]
        preferred_candidates.extend(sorted(folder.glob(f"*_{serial}.mp4")))
        all_candidates = preferred_candidates + _list_folder_media(folder, VIDEO_EXTENSIONS)
        for candidate in all_candidates:
            resolved = str(candidate.resolve(strict=False)).lower()
            if resolved in seen:
                continue
            seen.add(resolved)
            if not candidate.exists() or not candidate.is_file():
                continue
            if resolved in claimed:
                continue
            claimed.add(resolved)
            target = output_dir / f"{date_mmdd}_{serial}{task_slot_token(task)}{candidate.suffix.lower()}"
            return _copy_if_needed(candidate, target)
    return None


def _list_folder_media(folder: Path, suffixes: set[str]) -> list[Path]:
    if not folder.exists() or not folder.is_dir():
        return []
    files = [
        item
        for item in sorted(folder.iterdir(), key=lambda value: value.name.lower())
        if item.is_file() and item.suffix.lower() in suffixes
    ]
    return files


def _find_bootstrap_source_image(
    *,
    task: WindowTask,
    output_dir: Path,
    date_mmdd: str,
    serial: int,
) -> Path | None:
    explicit = [Path(item) for item in task.thumbnails if Path(item).exists()]
    if explicit:
        return explicit[0]

    for folder_text in [str(task.source_dir or "").strip(), str(output_dir)]:
        if not folder_text:
            continue
        for folder in _iter_media_search_dirs(Path(folder_text), serial) or [Path(folder_text)]:
            preferred = _load_source_dir_cover_paths(str(folder), date_mmdd, serial)
            if preferred:
                return preferred[0]
            images = _list_folder_media(folder, IMAGE_EXTENSIONS)
            if images:
                return images[0]
    return None


def _find_bootstrap_source_audio(
    *,
    task: WindowTask,
    output_dir: Path,
    date_mmdd: str,
    serial: int,
) -> Path | None:
    for folder_text in [str(task.source_dir or "").strip(), str(output_dir)]:
        if not folder_text:
            continue
        for folder in _iter_media_search_dirs(Path(folder_text), serial) or [Path(folder_text)]:
            preferred_candidates = [
                folder / f"{date_mmdd}_{serial}.mp3",
                folder / f"{serial}.mp3",
            ]
            preferred_candidates.extend(sorted(folder.glob(f"*_{serial}.mp3")))
            for candidate in preferred_candidates:
                if candidate.exists() and candidate.is_file():
                    return candidate
            audio_files = _list_folder_media(folder, AUDIO_EXTENSIONS)
            if audio_files:
                return audio_files[0]
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


def _generate_prompt_metadata(
    *,
    tag: str,
    task: WindowTask,
    defaults: WorkflowDefaults,
    config: dict[str, Any] | None,
    unique_seed: str,
    title_fallback: str,
    description_fallback: str,
    used_titles: list[str],
    used_descriptions: list[str],
    used_thumbnail_prompts: list[str],
    used_tag_signatures: list[str],
    log: LogFunc,
) -> dict[str, Any]:
    prompt_config = load_prompt_studio_config(PROMPT_STUDIO_FILE)
    resolved_api_name = find_explicit_api_preset_name(prompt_config, tag)
    resolved_content_name = find_explicit_content_template_name(prompt_config, tag)
    if not resolved_api_name:
        raise RuntimeError(f"{tag}/{task.serial} 未绑定 API 模板；当前文案链只允许走 API，不允许回退默认模板。")
    if not resolved_content_name:
        raise RuntimeError(f"{tag}/{task.serial} 未绑定内容模板；当前文案链只允许走 API，不允许回退默认模板。")
    log(
        f"[文案] {tag}/{task.serial}: API={resolved_api_name} | 模板={resolved_content_name} | seed={unique_seed}"
    )
    bundle = generate_content_bundle(
        PROMPT_STUDIO_FILE,
        tag,
        is_ypp=task.is_ypp,
        api_preset_name=resolved_api_name,
        content_template_name=resolved_content_name,
        unique_seed=unique_seed,
        avoid_titles=None,
        avoid_descriptions=None,
        avoid_thumbnail_prompts=None,
        avoid_tag_signatures=None,
        log=log,
    )
    if str(bundle.get("generation_source") or "").strip().lower() != "api":
        raise RuntimeError(f"{tag}/{task.serial} 文案链路未返回 API 来源标记，已拒绝继续。")
    if str((bundle.get("api_preset") or {}).get("name") or "") != str(resolved_api_name):
        raise RuntimeError(
            f"{tag}/{task.serial} API preset mismatch: expected={resolved_api_name} "
            f"actual={str((bundle.get('api_preset') or {}).get('name') or '')}"
        )
    if str((bundle.get("content_template") or {}).get("name") or "") != str(resolved_content_name):
        raise RuntimeError(
            f"{tag}/{task.serial} content template mismatch: expected={resolved_content_name} "
            f"actual={str((bundle.get('content_template') or {}).get('name') or '')}"
        )

    title_candidates = [str(item).strip() for item in bundle.get("titles", []) if str(item).strip()]
    description_candidates = [str(item).strip() for item in bundle.get("descriptions", []) if str(item).strip()]
    thumbnail_prompts = [
        str(item.get("prompt") or "").strip()
        for item in bundle.get("thumbnail_prompts", [])
        if str(item.get("prompt") or "").strip()
    ]
    chosen_title = title_candidates[0] if title_candidates else ""
    chosen_description = description_candidates[0] if description_candidates else ""
    tag_list = [str(item).strip() for item in bundle.get("tag_list", []) if str(item).strip()]
    ab_titles = (
        [str(item).strip() for item in title_candidates[:3] if str(item).strip()]
        if task.is_ypp
        else []
    )

    if not chosen_title:
        raise RuntimeError(f"{tag}/{task.serial} API 未返回标题。")
    if not chosen_description:
        raise RuntimeError(f"{tag}/{task.serial} API 未返回简介。")
    if not tag_list:
        raise RuntimeError(f"{tag}/{task.serial} API 未返回标签。")
    if defaults.generate_thumbnails and not thumbnail_prompts:
        raise RuntimeError(f"{tag}/{task.serial} API 未返回缩略图提示词。")

    return {
        "bundle": bundle,
        "title": chosen_title,
        "description": chosen_description,
        "tag_list": tag_list,
        "ab_titles": ab_titles,
        "thumbnail_prompts": thumbnail_prompts,
        "attempts": 1,
    }


def _generate_prompt_metadata_with_dedup(
    *,
    tag: str,
    task: WindowTask,
    defaults: WorkflowDefaults,
    config: dict[str, Any] | None,
    unique_seed: str,
    title_fallback: str,
    description_fallback: str,
    used_titles: list[str],
    used_descriptions: list[str],
    used_thumbnail_prompts: list[str],
    used_tag_signatures: list[str],
    batch_dedup: BatchDedup | None,
    log: LogFunc,
) -> dict[str, Any]:
    retry_limit = 3
    dedup_titles = list(batch_dedup.used_titles) if batch_dedup else []
    dedup_descriptions = list(batch_dedup.used_descriptions) if batch_dedup else []
    duplicate_error = ""

    for attempt in range(1, retry_limit + 1):
        generated = _generate_prompt_metadata(
            tag=tag,
            task=task,
            defaults=defaults,
            config=config,
            unique_seed=unique_seed if attempt == 1 else f"{unique_seed}|retry{attempt}",
            title_fallback=title_fallback,
            description_fallback=description_fallback,
            used_titles=[*used_titles, *dedup_titles],
            used_descriptions=[*used_descriptions, *dedup_descriptions],
            used_thumbnail_prompts=used_thumbnail_prompts,
            used_tag_signatures=used_tag_signatures,
            log=log,
        )
        title = str(generated.get("title") or "").strip()
        description = str(generated.get("description") or "").strip()
        if batch_dedup and batch_dedup.is_duplicate(title, description):
            duplicate_error = f"{tag}/{task.serial} duplicate metadata detected"
            log(f"[Dedup] {tag}/{task.serial}: duplicate detected, retry {attempt}/{retry_limit}")
            continue
        if batch_dedup:
            batch_dedup.record(title, description)
        generated["attempts"] = attempt
        return generated

    raise RuntimeError(duplicate_error or f"{tag}/{task.serial} duplicate metadata retry exhausted")


def _generate_task_metadata_payload(
    *,
    tag: str,
    task: WindowTask,
    defaults: WorkflowDefaults,
    config: dict[str, Any] | None,
    unique_seed: str,
    title_fallback: str,
    description_fallback: str,
    metadata_dir: Path,
    date_mmdd: str,
    source_image: Path | None,
    used_titles: list[str],
    used_descriptions: list[str],
    used_thumbnail_prompts: list[str],
    used_tag_signatures: list[str],
    batch_dedup: BatchDedup | None,
    control: ExecutionControl | None,
    log: LogFunc,
) -> dict[str, Any]:
    title = task.title.strip() or title_fallback
    description = task.description.strip() or description_fallback
    tag_list = [item for item in task.tag_list if str(item).strip()]
    ab_titles = [item for item in task.ab_titles if str(item).strip()]
    cover_paths, cover_source = _pick_preferred_cover_paths(
        task=task,
        metadata_dir=metadata_dir,
        date_mmdd=date_mmdd,
        serial=task.serial,
    )
    if not defaults.generate_thumbnails and source_image and source_image.exists():
        cover_paths = [source_image]
        cover_source = "source_image"

    bundle = None
    thumbnail_prompts: list[str] = []
    attempts = 0
    if defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
        if control:
            control.check_cancelled()
            control.wait_if_paused(log=log, label=f"{tag}/{task_round_label(task)} metadata")
        generated = _generate_prompt_metadata_with_dedup(
            tag=tag,
            task=task,
            defaults=defaults,
            config=config,
            unique_seed=unique_seed,
            title_fallback=title or title_fallback,
            description_fallback=description,
            used_titles=used_titles,
            used_descriptions=used_descriptions,
            used_thumbnail_prompts=used_thumbnail_prompts,
            used_tag_signatures=used_tag_signatures,
            batch_dedup=batch_dedup,
            log=log,
        )
        bundle = generated["bundle"]
        attempts = int(generated.get("attempts") or 0)
        thumbnail_prompts = list(generated["thumbnail_prompts"])
        if defaults.generate_text:
            title = generated["title"]
            description = generated["description"]
            if not tag_list:
                tag_list = list(generated["tag_list"])
            if task.is_ypp and not ab_titles:
                ab_titles = list(generated["ab_titles"])

    if defaults.generate_text:
        if not bundle:
            raise RuntimeError(f"{tag}/{task_round_label(task)} text generation requires API result, but no API bundle is available.")
    elif not title:
        title = title_fallback

    if defaults.generate_thumbnails:
        cover_count = 3 if task.is_ypp else 1
        cover_paths, cover_source = _generate_thumbnail_covers(
            bundle=bundle,
            thumbnail_prompts=thumbnail_prompts,
            source_image=source_image,
            target_dir=metadata_dir,
            date_mmdd=date_mmdd,
            serial=task.serial,
            cover_count=cover_count,
            tag=tag,
            title_text=title,
            control=control,
            log=log,
        )

    return {
        "bundle": bundle,
        "title": title,
        "description": description,
        "tag_list": tag_list,
        "ab_titles": ab_titles,
        "thumbnail_prompts": thumbnail_prompts,
        "cover_paths": cover_paths,
        "cover_source": cover_source,
        "attempts": attempts,
    }


def _build_api_debug_payload(bundle: dict[str, Any] | None, *, unique_seed: str) -> dict[str, str]:
    api_preset = (bundle or {}).get("api_preset") or {}
    return {
        "generation_source": str((bundle or {}).get("generation_source") or ""),
        "api_preset_name": str(api_preset.get("name") or ""),
        "api_provider": str(api_preset.get("provider") or ""),
        "api_base_url": str(api_preset.get("baseUrl") or ""),
        "api_model": str(api_preset.get("model") or ""),
        "image_base_url": str(api_preset.get("imageBaseUrl") or ""),
        "image_model": str(api_preset.get("imageModel") or ""),
        "unique_seed": str(unique_seed or ""),
    }


def _assert_api_metadata_is_unique(
    *,
    tag: str,
    task: WindowTask,
    title: str,
    description: str,
    tag_list: list[str],
    thumbnail_prompts: list[str],
    config: dict[str, Any] | None,
    transient_titles: list[str],
    transient_descriptions: list[str],
    transient_thumbnail_prompts: list[str],
    transient_tag_signatures: list[str],
) -> None:
    live_scope = get_used_metadata_scope(tag, config=config, global_scope=True)
    title_pool = [str(item).strip() for item in [*live_scope.get("titles", []), *transient_titles] if str(item).strip()]
    description_pool = [
        str(item).strip()
        for item in [*live_scope.get("descriptions", []), *transient_descriptions]
        if str(item).strip()
    ]
    prompt_pool = [
        str(item).strip()
        for item in [*live_scope.get("thumbnail_prompts", []), *transient_thumbnail_prompts]
        if str(item).strip()
    ]
    tag_signature = " | ".join([str(item).strip() for item in tag_list if str(item).strip()])
    tag_pool = [
        str(item).strip()
        for item in [*live_scope.get("tag_signatures", []), *transient_tag_signatures]
        if str(item).strip()
    ]

    duplicate_fields: list[str] = []
    if title and _is_effectively_duplicate(title, title_pool):
        duplicate_fields.append("title")
    if description and _is_effectively_duplicate(description, description_pool):
        duplicate_fields.append("description")
    if tag_signature and _is_effectively_duplicate(tag_signature, tag_pool):
        duplicate_fields.append("tags")
    first_prompt = next((str(item).strip() for item in thumbnail_prompts if str(item).strip()), "")
    if first_prompt and _is_effectively_duplicate(first_prompt, prompt_pool):
        duplicate_fields.append("thumbnail_prompt")

    if duplicate_fields:
        raise RuntimeError(
            f"{tag}/{task.serial} API metadata duplicated existing history after generation: {', '.join(duplicate_fields)}"
        )


def _generate_thumbnail_covers(
    *,
    bundle: dict[str, Any] | None,
    thumbnail_prompts: list[str],
    source_image: Path | None,
    target_dir: Path,
    date_mmdd: str,
    serial: int,
    cover_count: int,
    tag: str,
    title_text: str,
    control: ExecutionControl | None,
    log: LogFunc,
) -> tuple[list[Path], str]:
    prompts = [str(item).strip() for item in thumbnail_prompts if str(item).strip()][:cover_count]
    last_error: Exception | None = None
    generated: list[Path] = []
    api_preset = bundle.get("api_preset") if isinstance(bundle, dict) else {}
    api_enabled = bool(bundle) and str((api_preset or {}).get("autoImageEnabled") or "0") == "1"
    if not prompts:
        raise RuntimeError(f"{tag}/{serial} thumbnail prompts are missing from text API output.")
    if not api_enabled:
        raise RuntimeError(f"{tag}/{serial} thumbnail image API is required but not enabled.")

    for cover_index, prompt in enumerate(prompts, 1):
        target = target_dir / f"{date_mmdd}_{serial}_cover_{cover_index:02d}.png"
        success = False
        for image_attempt in range(1, 3):
            if control:
                control.check_cancelled()
                control.wait_if_paused(log=log, label=f"{tag}/{serial} 缩略图生成")
            try:
                image_result = call_image_model(api_preset, prompt)
                if not image_result.get("data_url"):
                    raise RuntimeError("thumbnail API returned no image data")
                generated.append(save_data_url_image(image_result["data_url"], target))
                success = True
                break
            except Exception as exc:
                last_error = exc
                if image_attempt >= 2:
                    break
        if not success:
            generated = []
            break
    if len(generated) == cover_count:
        return generated, "generated"

    if source_image and source_image.exists():
        if last_error:
            if _thumbnail_error_needs_balance_hint(last_error):
                log(f"[提醒] {tag}/{serial} 图片 API 可能余额或额度不足，已自动切换到备用封面方案。")
            log(f"[警告] {tag}/{serial} 缩略图 API 连续失败两次，回落到底图加大字兜底: {last_error}")
        fallback = _make_cover_fallbacks(
            source_image,
            target_dir,
            date_mmdd,
            serial,
            cover_count,
            headline=title_text,
            genre_text=_derive_cover_genre_text(tag, bundle),
        )
        return fallback, "source_image_text_fallback"

    if last_error:
        raise RuntimeError(f"{tag}/{serial} thumbnail API generation failed twice: {last_error}") from last_error
    raise RuntimeError(f"{tag}/{serial} thumbnail generation requires API image output, but prompts or source image are unavailable.")
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
            raise ValueError(f"{tag} has no available rendered output folder.")

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
        batch_dedup = BatchDedup()

        for task in tag_tasks:
            if control:
                control.check_cancelled()
                control.wait_if_paused(log=log, label=f"{tag}/{task.serial}")
            channel = channels.get(str(task.serial)) if isinstance(channels.get(str(task.serial)), dict) else {}
            video_path = _find_existing_video(output_dir, defaults.date_mmdd, task.serial, channel)
            if not video_path:
                raise ValueError(f"{tag}/{task.serial} 缺少现成视频文件: {output_dir}")

            source_image = _resolve_manifest_media_path(output_dir, channel.get("source_image"))
            source_audio = _resolve_manifest_media_path(output_dir, channel.get("source_audio"))
            if not source_image or not source_image.exists():
                source_image = _find_bootstrap_source_image(
                    task=task,
                    output_dir=video_path.parent,
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                )
            if not source_audio or not source_audio.exists():
                source_audio = _find_bootstrap_source_audio(
                    task=task,
                    output_dir=video_path.parent,
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                )
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
            metadata_future = None
            try:
                if metadata_future is not None:
                    metadata_payload = metadata_future.result()
                    bundle = metadata_payload.get("bundle")
                    log(
                        f"[并行文案] {tag}/{task_label}: API={bundle['api_preset'].get('name', '') if bundle else ''} | "
                        f"模板={bundle['content_template'].get('name', '') if bundle else ''} | "
                        f"来源={bundle.get('generation_source', 'unknown') if bundle else 'unknown'} | 重试={metadata_payload.get('attempts', 0)}"
                    )
                    thumbnail_prompts = [
                        str(item).strip()
                        for item in metadata_payload.get("thumbnail_prompts", [])
                        if str(item).strip()
                    ]
                    title = str(metadata_payload.get("title") or title).strip() or output_video.stem
                    description = str(metadata_payload.get("description") or description).strip()
                    payload_tags = [
                        str(item).strip()
                        for item in metadata_payload.get("tag_list", [])
                        if str(item).strip()
                    ]
                    if payload_tags:
                        tag_list = payload_tags
                    payload_ab_titles = [
                        str(item).strip()
                        for item in metadata_payload.get("ab_titles", [])
                        if str(item).strip()
                    ]
                    if payload_ab_titles:
                        ab_titles = payload_ab_titles
                    payload_covers = [
                        Path(str(path).strip())
                        for path in metadata_payload.get("cover_paths", [])
                        if str(path).strip()
                    ]
                    if payload_covers:
                        cover_paths = payload_covers
                    cover_source = str(metadata_payload.get("cover_source") or cover_source).strip()
                elif defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
                    if control:
                        control.check_cancelled()
                        control.wait_if_paused(log=log, label=f"{tag}/{task.serial} 文案生成")
                    generated = _generate_prompt_metadata_with_dedup(
                        tag=tag,
                        task=task,
                        defaults=defaults,
                        config=cfg,
                        unique_seed=unique_seed,
                        title_fallback=title or video_path.stem,
                        description_fallback=description,
                        used_titles=[*(history_scope.get("titles") or []), *current_titles],
                        used_descriptions=[*(history_scope.get("descriptions") or []), *current_descriptions],
                        used_thumbnail_prompts=[*(history_scope.get("thumbnail_prompts") or []), *current_thumbnail_prompts],
                        used_tag_signatures=[*(history_scope.get("tag_signatures") or []), *current_tag_signatures],
                        batch_dedup=batch_dedup,
                        log=log,
                    )
                    bundle = generated["bundle"]
                    log(
                        f"[文案] {tag}/{task.serial}: API={bundle['api_preset'].get('name', '')} | "
                        f"模板={bundle['content_template'].get('name', '')} | "
                        f"来源={bundle.get('generation_source', 'unknown')} | 重试={generated['attempts']}"
                    )
                    thumbnail_prompts = list(generated["thumbnail_prompts"])
                    if defaults.generate_text:
                        title = generated["title"]
                        description = generated["description"]
                        if not task.tag_list:
                            tag_list = list(generated["tag_list"])
                        if task.is_ypp and not task.ab_titles:
                            ab_titles = list(generated["ab_titles"])

                if defaults.generate_text and not bundle:
                    raise RuntimeError(f"{tag}/{task.serial} text generation requires API result, but no API bundle is available.")

                cover_count = 3 if task.is_ypp else 1
                if defaults.generate_thumbnails and not task.thumbnails:
                    cover_paths, cover_source = _generate_thumbnail_covers(
                        bundle=bundle,
                        thumbnail_prompts=thumbnail_prompts,
                        source_image=source_image,
                        target_dir=tag_metadata_dir,
                        date_mmdd=defaults.date_mmdd,
                        serial=task.serial,
                        cover_count=cover_count,
                        tag=tag,
                        title_text=title,
                        control=control,
                        log=log,
                    )

                if cover_paths:
                    log(
                        f"[封面] {tag}/{task.serial}: 来源={cover_source or 'existing'} | "
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
                        "title": title,
                        "description": description,
                        **_build_api_debug_payload(
                            bundle or ({"generation_source": "api"} if (defaults.generate_text or defaults.generate_thumbnails) else {}),
                            unique_seed=unique_seed,
                        ),
                        "content_template_name": str((((bundle or {}).get("content_template") or {}).get("name")) or ""),
                        "thumbnails": [str(path) for path in cover_paths],
                        "thumbnail_source": cover_source or "existing",
                        "thumbnail_prompt_source": "api_text" if thumbnail_prompts else "",
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
            except Exception as exc:
                log(f"[错误] {tag}/{task.serial} 文案/封面阶段失败，已跳过当前视频，其它视频继续处理: {exc}")
                continue

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


def _describe_effect_kwargs(effect_kwargs: dict[str, Any]) -> str:
    parts = [
        f"style={effect_kwargs.get('style')}",
        f"reactive={effect_kwargs.get('reactive_spectrum_enabled')}",
        f"reactive_preset={effect_kwargs.get('reactive_spectrum_preset')}",
        f"spectrum={effect_kwargs.get('color_spectrum')}",
        f"timeline={effect_kwargs.get('color_timeline')}",
        f"particle={effect_kwargs.get('particle')}",
        f"opacity={effect_kwargs.get('particle_opacity')}",
        f"speed={effect_kwargs.get('particle_speed')}",
        f"text_pos={effect_kwargs.get('text_pos')}",
        f"text_size={effect_kwargs.get('text_size')}",
        f"text_style={effect_kwargs.get('text_style')}",
        f"font={effect_kwargs.get('text_font')}",
    ]
    return " | ".join(parts)


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
    render_profile: RenderCodecProfile | None = None,
    log_prefix: str = "[渲染]",
) -> dict[str, Any]:
    from daily_scheduler import FFMPEG_BIN

    profile = render_profile or _default_render_profile()
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
    ]
    if profile.ffmpeg_args:
        cmd.extend(profile.ffmpeg_args)
    cmd.extend(
        [
            "-loop",
            "1",
            "-r",
            "25",
            "-i",
            safe_image,
            "-i",
            safe_audio,
        ]
    )
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
            profile.video_codec,
            "-b:v",
            profile.video_bitrate,
        ]
        + list(profile.codec_extra_args)
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
    stderr_tail: deque[str] = deque(maxlen=20)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log(f"{log_prefix} CMD => {' '.join(str(part) for part in cmd)}")
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
                    log(f"{log_prefix} 仍在处理 {output_path.name} | 已耗时 {now - start:.0f}s")
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
                    log(
                        f"{log_prefix} {output_path.name} 进度 {ratio * 100:.0f}% "
                        f"({out_seconds:.0f}/{target_duration:.0f}s)"
                    )
                    last_report = now
                    last_ratio = ratio
            elif text.startswith("progress=end"):
                break
            elif text:
                stderr_tail.append(text)
                if any(keyword in text.lower() for keyword in ("error", "failed", "invalid", "unable", "no such file")):
                    log(f"{log_prefix} ffmpeg: {text}")

        return_code = process.wait(timeout=30)
        if return_code != 0:
            if stderr_tail:
                tail_text = " | ".join(list(stderr_tail)[-5:])
                log(f"{log_prefix} 失败尾日志: {tail_text}")
            raise RuntimeError(f"ffmpeg 退出码 {return_code}")
        elapsed = time.time() - start
        mark_complete(output_path, duration=target_duration)
        log(f"{log_prefix} 完成 {output_path.name} | 耗时 {elapsed:.1f}s")
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
        return "No group bindings yet."
    lines = []
    for tag in get_all_tags():
        if bindings.get(tag):
            lines.append(f"{tag}: {bindings[tag]}")
    return "\n".join(lines) if lines else "No group bindings yet."


def create_task(
    *,
    tag: str,
    serial: int,
    quantity: int = 1,
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
    slot_index: int = 1,
    total_slots: int = 1,
    round_index: int = 1,
) -> WindowTask:
    return WindowTask(
        tag=str(tag).strip(),
        serial=int(serial),
        quantity=max(1, int(quantity or 1)),
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
        slot_index=max(1, int(slot_index or 1)),
        total_slots=max(1, int(total_slots or 1)),
        round_index=max(1, int(round_index or 1)),
    )


def _simple_tag_key(tag: str) -> str:
    normalized = str(tag or "").strip().lower()
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", normalized)


def validate_group_sources(
    tasks: list[WindowTask],
    *,
    config: dict[str, Any] | None = None,
    log: LogFunc = _noop_log,
) -> tuple[list[str], list[str]]:
    cfg = config or load_scheduler_settings()
    errors: list[str] = []
    warnings: list[str] = []
    assigned_tasks = [
        task for task in tasks
        if str(getattr(task, "assigned_image", "") or "").strip()
        or str(getattr(task, "assigned_audio", "") or "").strip()
    ]
    if assigned_tasks:
        for task in assigned_tasks:
            tag = str(task.tag or "").strip()
            task_label = task_round_label(task)
            assigned_image = str(getattr(task, "assigned_image", "") or "").strip()
            assigned_audio = str(getattr(task, "assigned_audio", "") or "").strip()
            if assigned_image and not Path(assigned_image).exists():
                errors.append(f"{tag}/{task_label} 已分配图片不存在: {assigned_image}")
            if assigned_audio and not Path(assigned_audio).exists():
                errors.append(f"{tag}/{task_label} 已分配音乐不存在: {assigned_audio}")
        if errors:
            return errors, warnings
        log(f"[检查] 使用预分配素材，共 {len(assigned_tasks)} 个任务")
        return errors, warnings
    for scope in _group_tasks_by_media_scope(tasks, cfg):
        tag = str(scope["tag"])
        image_dir = Path(scope["image_dir"])
        audio_dir = Path(scope["audio_dir"])
        scope_tasks = list(scope["tasks"])
        if not image_dir.exists():
            errors.append(f"{tag} 的图片目录不存在: {image_dir}")
            continue
        if not audio_dir.exists():
            errors.append(f"{tag} 的音乐目录不存在: {audio_dir}")
            continue
        image_count = len(list_media_files(image_dir, IMAGE_EXTENSIONS))
        audio_count = len(list_media_files(audio_dir, AUDIO_EXTENSIONS))
        if image_count <= 0:
            errors.append(f"{tag} 的图片目录没有图片: {image_dir}")
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


def assign_media_to_tasks(
    tasks: list[WindowTask],
    *,
    config: dict[str, Any] | None = None,
) -> tuple[dict[str, tuple[str, str]], list[str]]:
    cfg = config or load_scheduler_settings()
    assignment: dict[str, tuple[str, str]] = {}
    warnings: list[str] = []
    for scope in _group_tasks_by_media_scope(tasks, cfg):
        scope_tasks = list(scope["tasks"])
        images = list_media_files(Path(scope["image_dir"]), IMAGE_EXTENSIONS)
        audio = list_media_files(Path(scope["audio_dir"]), AUDIO_EXTENSIONS)
        usable = min(len(scope_tasks), len(images), len(audio))
        if usable < len(scope_tasks):
            tag = str(scope.get("tag") or "").strip()
            warnings.append(
                f"{tag} 预分配素材不足: tasks={len(scope_tasks)}, images={len(images)}, audio={len(audio)}"
            )
        for index in range(usable):
            task = scope_tasks[index]
            assignment[task_runtime_key(task)] = (str(images[index]), str(audio[index]))
    return assignment, warnings


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
            error_text = f"{tag} did not find an uploadable output folder"
            expected_dir = output_root / f"{date_mmdd}_{tag}"
            error_text += f": {expected_dir}"
            if last_details:
                error_text += " | " + " ; ".join(last_details[:3])
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
    on_metadata_ready: MetadataReadyCallback | None = None,
    on_item_ready: ArtifactReadyCallback | None = None,
    log: LogFunc = _noop_log,
) -> WorkflowResult:
    if not tasks:
        raise ValueError("At least one window task is required.")

    config = config or load_scheduler_settings()
    metadata_root = get_metadata_root(config)
    output_root = Path(config["output_root"])
    plan = build_runtime_window_plan(
        tasks=tasks,
        defaults=defaults,
        config=config,
        output_dir_overrides=output_dir_overrides,
        metadata_dir_overrides=metadata_dir_overrides,
    )
    plan_path = save_window_plan(plan, defaults.date_mmdd)
    result = WorkflowResult(date_mmdd=defaults.date_mmdd, plan_path=str(plan_path))
    log(f"[计划] 已写入窗口计划: {plan_path}")

    tag_states: dict[str, dict[str, Any]] = {}
    claimed_videos: dict[str, set[str]] = {}
    unique_tags = [str(task.tag or "").strip() for task in tasks if str(task.tag or "").strip()]
    unique_tags = list(dict.fromkeys(unique_tags))
    single_tag_mode = len(unique_tags) == 1

    def state_for(tag: str) -> dict[str, Any]:
        state = tag_states.get(tag)
        if state:
            return state
        override_metadata = str((metadata_dir_overrides or {}).get(tag) or "").strip()
        if override_metadata:
            tag_metadata_dir = Path(override_metadata)
        elif single_tag_mode:
            tag_metadata_dir = metadata_root
        else:
            tag_metadata_dir = get_tag_metadata_dir(tag, root=metadata_root)
        tag_metadata_dir.mkdir(parents=True, exist_ok=True)
        override_output = str((output_dir_overrides or {}).get(tag) or "").strip()
        if override_output:
            output_dir = Path(override_output)
        elif single_tag_mode:
            output_dir = output_root
        else:
            output_dir = output_root / f"{defaults.date_mmdd}_{tag}"
        output_dir.mkdir(parents=True, exist_ok=True)
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
            "batch_dedup": BatchDedup(),
        }
        tag_states[tag] = state
        result.output_dirs.append(str(output_dir))
        return state

    grouped_tasks: dict[str, list[WindowTask]] = {}
    for task in tasks:
        grouped_tasks.setdefault(str(task.tag or "").strip(), []).append(task)

    for tag, tag_tasks in grouped_tasks.items():
        if control:
            control.check_cancelled()
            control.wait_if_paused(log=log, label=f"{tag}/metadata")
        state = state_for(tag)
        output_dir = Path(state["output_dir"])
        if not output_dir.exists():
            raise ValueError(f"{tag} 当前任务的现成视频目录不存在: {output_dir}")

        for task in tag_tasks:
            task_label = task_round_label(task)
            if control:
                control.check_cancelled()
                control.wait_if_paused(log=log, label=f"{tag}/{task_label}")
            task_output_dir = _task_output_dir(output_dir, defaults.date_mmdd, task)
            task_output_dir.mkdir(parents=True, exist_ok=True)
            task_metadata_dir = _task_metadata_dir(Path(state["metadata_dir"]), defaults.date_mmdd, task)
            task_metadata_dir.mkdir(parents=True, exist_ok=True)
            generation_map_path = task_metadata_dir / "generation_map.json"
            if str(task_output_dir) not in result.output_dirs:
                result.output_dirs.append(str(task_output_dir))

            channel = state["channels"].get(str(task.serial), {})
            existing_video = _find_existing_video(task_output_dir, defaults.date_mmdd, task.serial, channel, task=task)
            if not existing_video and task_output_dir != output_dir:
                fallback_video = _find_existing_video(output_dir, defaults.date_mmdd, task.serial, channel, task=task)
                if fallback_video:
                    existing_video = _copy_if_needed(fallback_video, task_output_dir / fallback_video.name)
            if not existing_video:
                existing_video = _claim_bootstrap_source_video(
                    task=task,
                    output_dir=task_output_dir,
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                    claimed_videos=claimed_videos,
                )
            if not existing_video:
                raise ValueError(f"{tag}/{task_label} 缺少现成视频文件: {task_output_dir}")

            source_image = _resolve_manifest_media_path(task_output_dir, channel.get("source_image"))
            if not source_image or not source_image.exists():
                source_image = _resolve_manifest_media_path(output_dir, channel.get("source_image"))
            if not source_image or not source_image.exists():
                source_image = _find_bootstrap_source_image(
                    task=task,
                    output_dir=task_output_dir if task_output_dir.exists() else output_dir,
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                )

            source_audio = _resolve_manifest_media_path(task_output_dir, channel.get("source_audio"))
            if not source_audio or not source_audio.exists():
                source_audio = _resolve_manifest_media_path(output_dir, channel.get("source_audio"))
            if not source_audio or not source_audio.exists():
                source_audio = _find_bootstrap_source_audio(
                    task=task,
                    output_dir=task_output_dir if task_output_dir.exists() else output_dir,
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                )

            history_scope = get_used_metadata_scope(tag, config=config)
            unique_seed = _build_unique_seed(
                defaults.date_mmdd,
                tag,
                task.serial,
                source_audio.stem if source_audio else existing_video.stem,
                source_image.stem if source_image else existing_video.stem,
                "metadata_only_existing_video",
            )
            title = task.title.strip() or f"{defaults.date_mmdd}_{task.serial}"
            description = task.description.strip()
            tag_list = [item for item in task.tag_list if str(item).strip()]
            ab_titles = [item for item in task.ab_titles if str(item).strip()]
            cover_paths, cover_source = _pick_preferred_cover_paths(
                task=task,
                metadata_dir=task_metadata_dir,
                date_mmdd=defaults.date_mmdd,
                serial=task.serial,
                channel=channel,
                legacy={},
            )
            if not defaults.generate_thumbnails and source_image and source_image.exists():
                cover_paths = [source_image]
                cover_source = "source_image"
            thumbnail_prompts: list[str] = []
            bundle = None
            item_failed = False
            metadata_future = None
            metadata_attempts = 0
            try:
                if metadata_future is not None:
                    metadata_payload = metadata_future.result()
                    bundle = metadata_payload.get("bundle")
                    log(
                        f"[并行文案] {tag}/{task_label}: API={bundle['api_preset'].get('name', '') if bundle else ''} | "
                        f"模板={bundle['content_template'].get('name', '') if bundle else ''} | "
                        f"来源={bundle.get('generation_source', 'unknown') if bundle else 'unknown'} | 重试={metadata_payload.get('attempts', 0)}"
                    )
                    thumbnail_prompts = [
                        str(item).strip()
                        for item in metadata_payload.get("thumbnail_prompts", [])
                        if str(item).strip()
                    ]
                    title = str(metadata_payload.get("title") or title).strip() or output_video.stem
                    description = str(metadata_payload.get("description") or description).strip()
                    payload_tags = [
                        str(item).strip()
                        for item in metadata_payload.get("tag_list", [])
                        if str(item).strip()
                    ]
                    if payload_tags:
                        tag_list = payload_tags
                    payload_ab_titles = [
                        str(item).strip()
                        for item in metadata_payload.get("ab_titles", [])
                        if str(item).strip()
                    ]
                    if payload_ab_titles:
                        ab_titles = payload_ab_titles
                    payload_covers = [
                        Path(str(path).strip())
                        for path in metadata_payload.get("cover_paths", [])
                        if str(path).strip()
                    ]
                    if payload_covers:
                        cover_paths = payload_covers
                    cover_source = str(metadata_payload.get("cover_source") or cover_source).strip()
                    metadata_attempts = int(metadata_payload.get("attempts") or 0)
                elif defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
                    if control:
                        control.check_cancelled()
                        control.wait_if_paused(log=log, label=f"{tag}/{task_label} 文案生成")
                    generated = _generate_prompt_metadata_with_dedup(
                        tag=tag,
                        task=task,
                        defaults=defaults,
                        config=config,
                        unique_seed=unique_seed,
                        title_fallback=title or existing_video.stem,
                        description_fallback=description,
                        used_titles=[*(history_scope.get("titles") or []), *state["titles"]],
                        used_descriptions=[*(history_scope.get("descriptions") or []), *state["descriptions"]],
                        used_thumbnail_prompts=[*(history_scope.get("thumbnail_prompts") or []), *state["thumbnail_prompts"]],
                        used_tag_signatures=[*(history_scope.get("tag_signatures") or []), *state["tag_signatures"]],
                        batch_dedup=state["batch_dedup"],
                        log=log,
                    )
                    bundle = generated["bundle"]
                    log(
                        f"[文案] {tag}/{task_label}: API={bundle['api_preset'].get('name', '')} | "
                        f"模板={bundle['content_template'].get('name', '')} | "
                        f"来源={bundle.get('generation_source', 'unknown')} | 重试={generated['attempts']}"
                    )
                    metadata_attempts = int(generated.get("attempts") or 0)
                    if defaults.generate_text:
                        title = generated["title"]
                        description = generated["description"]
                        if not tag_list:
                            tag_list = list(generated["tag_list"])
                        if task.is_ypp and not ab_titles:
                            ab_titles = list(generated["ab_titles"])
                    thumbnail_prompts = list(generated["thumbnail_prompts"])

                if defaults.generate_text and not bundle:
                    raise RuntimeError(f"{tag}/{task_label} text generation requires API result, but no API bundle is available.")

                if defaults.generate_thumbnails:
                    cover_count = 3 if task.is_ypp else 1
                    cover_paths, cover_source = _generate_thumbnail_covers(
                        bundle=bundle,
                        thumbnail_prompts=thumbnail_prompts,
                        source_image=source_image,
                        target_dir=task_metadata_dir,
                        date_mmdd=defaults.date_mmdd,
                        serial=task.serial,
                        cover_count=cover_count,
                        tag=tag,
                        title_text=title,
                        control=control,
                        log=log,
                    )

                if cover_paths:
                    thumb_preview = ", ".join(str(path) for path in cover_paths[:3])
                    log(f"[thumb] {tag}/{task_label}: source={cover_source or 'existing'} | {thumb_preview}")

                if on_metadata_ready:
                    try:
                        on_metadata_ready(
                            task,
                            task_output_dir,
                            {
                                "title": title,
                                "description": description,
                                "tag_list": list(tag_list),
                                "thumbnail_prompts": list(thumbnail_prompts),
                                "cover_paths": [str(path) for path in cover_paths],
                                "cover_source": cover_source,
                                "attempts": metadata_attempts,
                                "bundle": bundle,
                            },
                        )
                    except Exception as callback_exc:
                        log(f"[Metadata] {tag}/{task_label}: metadata_ready callback failed: {callback_exc}")

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
                )
                state["titles"].append(title)
                if description:
                    state["descriptions"].append(description)
                state["thumbnail_prompts"].extend(thumbnail_prompts)
                if tag_list:
                    state["tag_signatures"].append(" | ".join(tag_list))

                if existing_video:
                    updated_channel = {
                        "video": str(existing_video),
                        "effect_desc": str(channel.get("effect_desc") or ""),
                        "channel_name": task.channel_name.strip() or str(channel.get("channel_name") or "").strip(),
                        "title": title,
                        "description": description,
                        **_build_api_debug_payload(
                            bundle or ({"generation_source": "api"} if (defaults.generate_text or defaults.generate_thumbnails) else {}),
                            unique_seed=unique_seed,
                        ),
                        "content_template_name": str((((bundle or {}).get("content_template") or {}).get("name")) or ""),
                        "thumbnails": [str(path) for path in cover_paths],
                        "thumbnail_source": cover_source or "existing",
                        "thumbnail_prompt_source": "api_text" if thumbnail_prompts else "",
                        "thumbnail_prompts": thumbnail_prompts,
                        "tag_list": tag_list,
                        "is_ypp": bool(task.is_ypp),
                        "ab_titles": ab_titles,
                        "set": 1,
                        "upload_options": _build_upload_options(task),
                    }
                    if source_image:
                        updated_channel["source_image"] = str(source_image)
                    if source_audio:
                        updated_channel["source_audio"] = str(source_audio)
                    manifest_path = _write_manifest(
                        output_dir=task_output_dir,
                        date_mmdd=defaults.date_mmdd,
                        tag=str(state["tag"]),
                        channels={str(task.serial): updated_channel},
                        source_label="metadata_only",
                    )
                    manifest_path_text = str(manifest_path)
                    if manifest_path_text not in result.manifest_paths:
                        result.manifest_paths.append(manifest_path_text)
                    log(f"[清单] {state['tag']} metadata manifest 已更新: {manifest_path}")
                    if on_item_ready:
                        on_item_ready(task, task_output_dir, manifest_path)
            except Exception as exc:
                item_failed = True
                warning = f"{tag}/{task_label} 文案/封面阶段失败，已跳过当前视频，其它视频继续处理: {exc}"
                result.warnings.append(warning)
                log(f"[错误] {warning}")
            result.items.append(
                RenderedItem(
                    tag=tag,
                    serial=task.serial,
                    slot_index=task.slot_index,
                    total_slots=task.total_slots,
                    round_index=task.round_index,
                    output_video=str(existing_video or ""),
                    source_image=str(source_image or ""),
                    source_audio=str(source_audio or ""),
                    title=title,
                    description=description,
                    thumbnails=[str(path) for path in cover_paths],
                    tag_list=tag_list,
                    ab_titles=ab_titles,
                    effect_desc="metadata_only",
                )
            )

            if item_failed:
                continue

    return result


def execute_direct_media_workflow(
    *,
    tasks: list[WindowTask],
    defaults: WorkflowDefaults,
    simulation: SimulationOptions | None = None,
    config: dict[str, Any] | None = None,
    output_dir_overrides: dict[str, str] | None = None,
    metadata_dir_overrides: dict[str, str] | None = None,
    control: ExecutionControl | None = None,
    on_metadata_ready: MetadataReadyCallback | None = None,
    on_item_ready: ArtifactReadyCallback | None = None,
    log: LogFunc = _noop_log,
) -> WorkflowResult:
    if not tasks:
        raise ValueError("At least one window task is required.")

    config = config or load_scheduler_settings()
    output_root = Path(config["output_root"])
    metadata_root = get_metadata_root(config)
    used_media_root = Path(config.get("used_media_root") or (SCRIPT_DIR / "workspace" / "AutoTask" / "_used_media"))
    cleanup_old_uploaded_videos(output_root, int(config.get("render_cleanup_days", 5)), log=log)

    plan = build_runtime_window_plan(
        tasks=tasks,
        defaults=defaults,
        config=config,
        output_dir_overrides=output_dir_overrides,
        metadata_dir_overrides=metadata_dir_overrides,
    )
    plan_path = save_window_plan(plan, defaults.date_mmdd)
    result = WorkflowResult(date_mmdd=defaults.date_mmdd, plan_path=str(plan_path))
    log(f"[计划] 已写入窗口计划: {plan_path}")
    render_profile = _resolve_render_profile(config)
    render_log_prefix = f"[渲染/{render_profile.name}]"

    tag_states: dict[str, dict[str, Any]] = {}
    unique_tags = [str(task.tag or "").strip() for task in tasks if str(task.tag or "").strip()]
    unique_tags = list(dict.fromkeys(unique_tags))
    single_tag_mode = len(unique_tags) == 1

    def state_for(tag: str) -> dict[str, Any]:
        state = tag_states.get(tag)
        if state:
            return state
        override_output = str((output_dir_overrides or {}).get(tag) or "").strip()
        if single_tag_mode:
            output_dir = output_root
        else:
            output_dir = Path(override_output) if override_output else (output_root / f"{defaults.date_mmdd}_{tag}")
        output_dir.mkdir(parents=True, exist_ok=True)
        override_metadata = str((metadata_dir_overrides or {}).get(tag) or "").strip()
        if single_tag_mode:
            metadata_dir = metadata_root
        else:
            metadata_dir = Path(override_metadata) if override_metadata else get_tag_metadata_dir(tag, root=metadata_root)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "tag": tag,
            "output_dir": output_dir,
            "metadata_dir": metadata_dir,
            "titles": [],
            "descriptions": [],
            "thumbnail_prompts": [],
            "tag_signatures": [],
            "batch_dedup": BatchDedup(),
        }
        tag_states[tag] = state
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
        assigned_pairs: list[tuple[WindowTask, Path, Path]] = []
        missing_assigned: list[str] = []
        for task in scope_tasks:
            assigned_image = str(getattr(task, "assigned_image", "") or "").strip()
            assigned_audio = str(getattr(task, "assigned_audio", "") or "").strip()
            if not assigned_image and not assigned_audio:
                assigned_pairs = []
                break
            image_path = Path(assigned_image) if assigned_image else None
            audio_path = Path(assigned_audio) if assigned_audio else None
            if not image_path or not image_path.exists() or not audio_path or not audio_path.exists():
                missing_assigned.append(task_round_label(task))
                continue
            assigned_pairs.append((task, image_path, audio_path))
        paired = assigned_pairs if assigned_pairs and not missing_assigned else _pair_media(scope_tasks, image_dir, audio_dir, shuffle=False)
        if not paired:
            raise ValueError(f"{tag} 的图/音目录没有可用的图音组合")
        if len(paired) < len(scope_tasks):
            warning = f"{tag} only processed {len(paired)} windows; remaining windows will be skipped."
            result.warnings.append(warning)
            log(f"[警告] {warning}")

        for task, source_image, source_audio in paired:
            task_label = task_round_label(task)
            if control:
                control.check_cancelled()
                control.wait_if_paused(log=log, label=f"{tag}/{task_label}")
            unique_seed = _build_unique_seed(
                defaults.date_mmdd,
                tag,
                task.serial,
                source_audio.stem,
                source_image.stem,
            )
            task_output_dir = _task_output_dir(Path(state["output_dir"]), defaults.date_mmdd, task)
            task_output_dir.mkdir(parents=True, exist_ok=True)
            task_metadata_dir = _task_metadata_dir(Path(state["metadata_dir"]), defaults.date_mmdd, task)
            task_metadata_dir.mkdir(parents=True, exist_ok=True)
            generation_map_path = task_metadata_dir / "generation_map.json"
            if str(task_output_dir) not in result.output_dirs:
                result.output_dirs.append(str(task_output_dir))
            output_video = task_output_dir / _task_video_filename(defaults.date_mmdd, task)
            done_marker = Path(str(output_video) + ".done")
            # ── 已渲染完成的视频（有 .done 标记）直接跳过，不重复渲染 ──
            if output_video.exists() and done_marker.exists() and output_video.stat().st_size > 1024:
                log(f"[跳过] {task_label}: 已存在完成的视频 {output_video.name} ({output_video.stat().st_size / 1024 / 1024:.0f}MB)，跳过渲染")
                effect_desc = "已跳过渲染(使用已有视频)"
                # 仍需生成文案和 manifest，跳到文案阶段
                render_skipped = True
            else:
                clean_incomplete(output_video)
                output_video.unlink(missing_ok=True)
                done_marker.unlink(missing_ok=True)
                render_skipped = False

            history_scope = get_used_metadata_scope(tag, config=config)
            metadata_executor: _cf.ThreadPoolExecutor | None = None
            metadata_future: _cf.Future | None = None
            render_options = _build_render_options_from_defaults(defaults)
            effect_rng = random.Random(f"{unique_seed}|visual")
            effect_kwargs = build_effect_kwargs(render_options, rng=effect_rng)
            duration = get_audio_duration(source_audio)
            filter_complex, effect_desc, extra_inputs = get_effect(duration, rng=effect_rng, **effect_kwargs)
            if defaults.generate_text or defaults.generate_thumbnails:
                metadata_executor = _cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="render-meta")
                metadata_future = metadata_executor.submit(
                    _generate_task_metadata_payload,
                    tag=tag,
                    task=task,
                    defaults=defaults,
                    config=config,
                    unique_seed=unique_seed,
                    title_fallback=output_video.stem,
                    description_fallback=task.description.strip(),
                    metadata_dir=task_metadata_dir,
                    date_mmdd=defaults.date_mmdd,
                    source_image=source_image,
                    used_titles=[*(history_scope.get("titles") or []), *state["titles"]],
                    used_descriptions=[*(history_scope.get("descriptions") or []), *state["descriptions"]],
                    used_thumbnail_prompts=[*(history_scope.get("thumbnail_prompts") or []), *state["thumbnail_prompts"]],
                    used_tag_signatures=[*(history_scope.get("tag_signatures") or []), *state["tag_signatures"]],
                    batch_dedup=state["batch_dedup"],
                    control=control,
                    log=log,
                )
                if on_metadata_ready:
                    def _notify_metadata_ready(
                        completed_future: _cf.Future,
                        *,
                        _task: WindowTask = task,
                        _task_output_dir: Path = task_output_dir,
                        _tag: str = tag,
                        _task_label: str = task_label,
                        _log: LogFunc = log,
                    ) -> None:
                        try:
                            metadata_payload = completed_future.result()
                        except Exception as callback_exc:
                            _log(f"[Metadata] {_tag}/{_task_label}: 并行文案线程失败: {callback_exc}")
                            return
                        try:
                            on_metadata_ready(_task, _task_output_dir, dict(metadata_payload or {}))
                        except Exception as callback_exc:
                            _log(f"[Metadata] {_tag}/{_task_label}: metadata_ready callback failed: {callback_exc}")
                            return
                        title_preview = str((metadata_payload or {}).get("title") or "").strip()
                        cover_count = len((metadata_payload or {}).get("cover_paths", []) or [])
                        _log(
                            f"[Metadata] {_tag}/{_task_label}: 文案/缩略图已就绪 | "
                            f"title={title_preview[:48]} | covers={cover_count}"
                        )
                    metadata_future.add_done_callback(_notify_metadata_ready)
                log(f"[Pipeline] {tag}/{task_label}: metadata running in parallel with render")
            log(f"[任务] {tag}/{task_label}: {source_image.name} + {source_audio.name} -> {output_video.name}")
            if not render_skipped:
                log(
                    f"{render_log_prefix} 编码器={render_profile.video_codec} | "
                    f"码率 {render_profile.video_bitrate} | 特效 {effect_desc}"
                )
                log(f"[视觉] {tag}/{task_label}: {_describe_effect_kwargs(effect_kwargs)}")
                try:
                    _render_with_progress(
                        image_path=source_image,
                        audio_path=source_audio,
                        output_path=output_video,
                        filter_complex=filter_complex,
                        extra_inputs=extra_inputs,
                        clip_seconds=simulation.simulate_seconds if simulation else None,
                        control=control,
                        log=log,
                        render_profile=render_profile,
                        log_prefix=render_log_prefix,
                    )
                except Exception:
                    if metadata_executor is not None:
                        metadata_executor.shutdown(wait=False, cancel_futures=True)
                    raise

            bundle = None
            title = task.title.strip()
            description = task.description.strip()
            tag_list = [item for item in task.tag_list if str(item).strip()]
            ab_titles = [item for item in task.ab_titles if str(item).strip()]
            cover_paths, cover_source = _pick_preferred_cover_paths(
                task=task,
                metadata_dir=task_metadata_dir,
                date_mmdd=defaults.date_mmdd,
                serial=task.serial,
            )
            if not defaults.generate_thumbnails and source_image.exists():
                cover_paths = [source_image]
                cover_source = "source_image"
            thumbnail_prompts: list[str] = []
            metadata_failed = False
            metadata_error = ""
            try:
                if metadata_future is not None:
                    metadata_payload = metadata_future.result()
                    bundle = metadata_payload.get("bundle")
                    log(
                        f"[并行文案] {tag}/{task_label}: API={bundle['api_preset'].get('name', '') if bundle else ''} | "
                        f"模板={bundle['content_template'].get('name', '') if bundle else ''} | "
                        f"来源={bundle.get('generation_source', 'unknown') if bundle else 'unknown'} | 重试={metadata_payload.get('attempts', 0)}"
                    )
                    thumbnail_prompts = [
                        str(item).strip()
                        for item in metadata_payload.get("thumbnail_prompts", [])
                        if str(item).strip()
                    ]
                    title = str(metadata_payload.get("title") or title).strip() or output_video.stem
                    description = str(metadata_payload.get("description") or description).strip()
                    payload_tags = [
                        str(item).strip()
                        for item in metadata_payload.get("tag_list", [])
                        if str(item).strip()
                    ]
                    if payload_tags:
                        tag_list = payload_tags
                    payload_ab_titles = [
                        str(item).strip()
                        for item in metadata_payload.get("ab_titles", [])
                        if str(item).strip()
                    ]
                    if payload_ab_titles:
                        ab_titles = payload_ab_titles
                    payload_covers = [
                        Path(str(path).strip())
                        for path in metadata_payload.get("cover_paths", [])
                        if str(path).strip()
                    ]
                    if payload_covers:
                        cover_paths = payload_covers
                    cover_source = str(metadata_payload.get("cover_source") or cover_source).strip()
                elif defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
                    if control:
                        control.check_cancelled()
                        control.wait_if_paused(log=log, label=f"{tag}/{task_label} 文案生成")
                    generated = _generate_prompt_metadata_with_dedup(
                        tag=tag,
                        task=task,
                        defaults=defaults,
                        config=config,
                        unique_seed=unique_seed,
                        title_fallback=title or output_video.stem,
                        description_fallback=description,
                        used_titles=[*(history_scope.get("titles") or []), *state["titles"]],
                        used_descriptions=[*(history_scope.get("descriptions") or []), *state["descriptions"]],
                        used_thumbnail_prompts=[*(history_scope.get("thumbnail_prompts") or []), *state["thumbnail_prompts"]],
                        used_tag_signatures=[*(history_scope.get("tag_signatures") or []), *state["tag_signatures"]],
                        batch_dedup=state["batch_dedup"],
                        log=log,
                    )
                    bundle = generated["bundle"]
                    log(
                        f"[文案] {tag}/{task_label}: API={bundle['api_preset'].get('name', '')} | "
                        f"模板={bundle['content_template'].get('name', '')} | "
                        f"来源={bundle.get('generation_source', 'unknown')} | 重试={generated['attempts']}"
                    )
                    thumbnail_prompts = list(generated["thumbnail_prompts"])
                    if defaults.generate_text:
                        title = generated["title"]
                        description = generated["description"]
                        if not tag_list:
                            tag_list = list(generated["tag_list"])
                        if task.is_ypp and not ab_titles:
                            ab_titles = list(generated["ab_titles"])

                if defaults.generate_text:
                    if not bundle:
                        raise RuntimeError(f"{tag}/{task_label} text generation requires API result, but no API bundle is available.")
                elif not title:
                    title = output_video.stem

                cover_count = 3 if task.is_ypp else 1
                if defaults.generate_thumbnails and metadata_future is None:
                    cover_paths, cover_source = _generate_thumbnail_covers(
                        bundle=bundle,
                        thumbnail_prompts=thumbnail_prompts,
                        source_image=source_image,
                        target_dir=task_metadata_dir,
                        date_mmdd=defaults.date_mmdd,
                        serial=task.serial,
                        cover_count=cover_count,
                        tag=tag,
                        title_text=title,
                        control=control,
                        log=log,
                    )

                if cover_paths:
                    log(
                        f"[封面] {tag}/{task_label}: 来源={cover_source or 'existing'} | "
                        f"{', '.join(str(path) for path in cover_paths[:3])}"
                    )

                thumb_preview = ", ".join(str(path) for path in cover_paths[:3]) if cover_paths else ""
                if thumb_preview:
                    log(f"[thumb] {tag}/{task_label}: source={cover_source or 'existing'} | {thumb_preview}")

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
                )
                state["titles"].append(title)
                if description:
                    state["descriptions"].append(description)
                state["thumbnail_prompts"].extend(thumbnail_prompts)
                if tag_list:
                    state["tag_signatures"].append(" | ".join(tag_list))

                channel_payload = {
                    "video": output_video.name,
                    "source_image": str(source_image),
                    "source_audio": str(source_audio),
                    "effect_desc": effect_desc,
                    "channel_name": task.channel_name.strip(),
                    "title": title,
                    "description": description,
                    **_build_api_debug_payload(
                        bundle or ({"generation_source": "api"} if (defaults.generate_text or defaults.generate_thumbnails) else {}),
                        unique_seed=unique_seed,
                    ),
                    "content_template_name": str((((bundle or {}).get("content_template") or {}).get("name")) or ""),
                    "thumbnails": [str(path) for path in cover_paths],
                    "thumbnail_source": cover_source or "existing",
                    "thumbnail_prompt_source": "api_text" if thumbnail_prompts else "",
                    "thumbnail_prompts": thumbnail_prompts,
                    "tag_list": tag_list,
                    "is_ypp": bool(task.is_ypp),
                    "ab_titles": ab_titles,
                    "set": 1,
                    "slot_index": int(task.slot_index),
                    "total_slots": int(task.total_slots),
                    "upload_options": _build_upload_options(task),
                }
                if simulation is None or simulation.save_manifest:
                    manifest_path = _write_manifest(
                        output_dir=task_output_dir,
                        date_mmdd=defaults.date_mmdd,
                        tag=str(state["tag"]),
                        channels={str(task.serial): channel_payload},
                        source_label="group_bound_media",
                    )
                    manifest_path_text = str(manifest_path)
                    if manifest_path_text not in result.manifest_paths:
                        result.manifest_paths.append(manifest_path_text)
                    log(f"[清单] {state['tag']} manifest 已写入: {manifest_path}")
                    if on_item_ready:
                        on_item_ready(task, task_output_dir, manifest_path)
            except Exception as exc:
                metadata_failed = True
                metadata_error = str(exc)
                warning = f"{tag}/{task_label} 文案/封面阶段失败，已保留渲染成品并跳过该视频上传: {exc}"
                result.warnings.append(warning)
                log(f"[错误] {warning}")
                if not title:
                    title = output_video.stem
                # ── 文案失败时仅写入恢复用 manifest，禁止自动加入上传队列，避免裸传默认文件名。 ──
                try:
                    fallback_channel = {
                        "video": output_video.name,
                        "source_image": str(source_image),
                        "source_audio": str(source_audio),
                        "effect_desc": effect_desc,
                        "channel_name": task.channel_name.strip(),
                        "title": title,
                        "description": description or "",
                        "thumbnails": [str(p) for p in cover_paths],
                        "tag_list": tag_list or [],
                        "is_ypp": bool(task.is_ypp),
                        "ab_titles": ab_titles,
                        "set": 1,
                        "slot_index": int(task.slot_index),
                        "total_slots": int(task.total_slots),
                        "upload_options": _build_upload_options(task),
                        "_metadata_failed": True,
                        "_metadata_failure_reason": metadata_error or str(exc),
                    }
                    fallback_manifest = _write_manifest(
                        output_dir=task_output_dir,
                        date_mmdd=defaults.date_mmdd,
                        tag=str(state["tag"]),
                        channels={str(task.serial): fallback_channel},
                        source_label="fallback_metadata_failed",
                    )
                    fallback_manifest_text = str(fallback_manifest)
                    if fallback_manifest_text not in result.manifest_paths:
                        result.manifest_paths.append(fallback_manifest_text)
                    log(f"[清单] {state['tag']} fallback manifest 已写入（文案失败，仅供恢复，不自动上传）: {fallback_manifest}")
                except Exception as manifest_exc:
                    log(f"[错误] {tag}/{task_label} fallback manifest 写入也失败: {manifest_exc}")
            if metadata_executor is not None:
                metadata_executor.shutdown(wait=False, cancel_futures=True)
            result.items.append(
                RenderedItem(
                    tag=tag,
                    serial=task.serial,
                    slot_index=task.slot_index,
                    total_slots=task.total_slots,
                    round_index=task.round_index,
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

            if metadata_failed:
                if simulation and simulation.consume_sources:
                    _move_to_used(source_image, used_media_root, tag=tag, kind="images")
                    _move_to_used(source_audio, used_media_root, tag=tag, kind="audio")
                continue

            if simulation and simulation.consume_sources:
                _move_to_used(source_image, used_media_root, tag=tag, kind="images")
                _move_to_used(source_audio, used_media_root, tag=tag, kind="audio")

    return result
