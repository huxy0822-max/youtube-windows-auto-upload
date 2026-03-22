from __future__ import annotations

import base64
from copy import deepcopy
import io
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

import customtkinter as ctk
from PIL import Image, ImageGrab
from tkinter import filedialog, messagebox, ttk

from content_generation import _parse_json_like, analyze_audience_screenshot, call_image_model, call_text_model
from effects_library import (
    list_effects,
    list_font_names,
    list_palette_names,
    list_particle_effects,
    list_text_positions,
    list_text_styles,
    list_tint_names,
    list_zoom_modes,
)
from prompt_studio import (
    find_explicit_api_preset_name,
    find_explicit_content_template_name,
    load_prompt_studio_config,
    pick_api_preset_name,
    pick_content_template_name,
    save_prompt_studio_config,
)
from path_helpers import normalize_scheduler_config, open_path_in_file_manager
from run_plan_service import (
    build_module_selection,
    build_run_plan,
    execute_run_plan,
    execute_simulation_plan,
    preview_run_plan,
    validate_run_plan,
)
from group_upload_workflow import normalize_mmdd
from upload_window_planner import derive_tags_and_skip_channels
from utils import get_all_tags, get_tag_info
from workflow_core import (
    CHANNEL_MAPPING_FILE,
    ExecutionControl,
    PROMPT_STUDIO_FILE,
    SCHEDULER_CONFIG_FILE,
    WindowTask,
    WorkflowDefaults,
    WorkflowCancelledError,
    create_task,
    describe_group_bindings,
    ensure_prompt_presets,
    get_group_bindings,
    get_group_catalog,
    get_metadata_root,
    load_prompt_settings,
    load_scheduler_settings,
    save_scheduler_settings,
    save_window_plan,
    set_group_binding,
    task_round_label,
    task_runtime_key,
)

SCRIPT_DIR = Path(__file__).parent
STATE_FILE = SCRIPT_DIR / "dashboard_state.json"
UPLOAD_SCRIPT = SCRIPT_DIR / "batch_upload.py"
RUN_SNAPSHOT_FILE = SCRIPT_DIR / "data" / "last_run_snapshot.json"

TASK_MODE_VALUES = {
    "本日只上传": "upload_only",
    "本日只剪辑": "render_only",
    "本日剪辑并上传": "render_and_upload",
}
TASK_MODE_LABELS = {value: label for label, value in TASK_MODE_VALUES.items()}
MODULE_LABELS = {
    "metadata": "生成标题/简介/标签/缩略图",
    "render": "剪辑",
    "upload": "上传",
}
YES_NO_VALUES = ["yes", "no"]
VISIBILITY_VALUES = ["public", "private", "unlisted", "schedule"]
CATEGORY_VALUES = [
    "Music",
    "People & Blogs",
    "Education",
    "Entertainment",
    "News & Politics",
    "Gaming",
    "Sports",
    "Travel & Events",
]
PROVIDER_VALUES = ["openai_compatible", "anthropic", "gemini"]
LANGUAGE_VALUES = ["zh-TW", "zh-CN", "en-US", "ja-JP", "ko-KR"]
AUTO_IMAGE_VALUES = ["0", "1"]
METADATA_MODE_LABELS = {
    "提示词那套": "prompt_api",
}
METADATA_MODE_VALUES = list(METADATA_MODE_LABELS.keys())
SCHEDULE_TIMEZONE_VALUES = ["Asia/Taipei (+08:00)"]
WINDOW_BUTTONS_PER_ROW = 6
VISUAL_TOGGLE_VALUES = ["yes", "no"]
RANDOM_OPTION = "random"


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


def _subprocess_utf8_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def _bool_from_yes_no(value: str) -> bool:
    return str(value).strip().lower() == "yes"


def _yes_no_from_bool(value: bool) -> str:
    return "yes" if value else "no"


def _with_random(values: list[str]) -> list[str]:
    base = [str(item) for item in values if str(item)]
    filtered = [item for item in base if item != RANDOM_OPTION]
    if filtered and filtered[0].lower() == "none":
        return [filtered[0], RANDOM_OPTION, *filtered[1:]]
    return [RANDOM_OPTION, *filtered]


def _with_random_first(values: list[str]) -> list[str]:
    base = [str(item) for item in values if str(item)]
    filtered = [item for item in base if item != RANDOM_OPTION]
    return [RANDOM_OPTION, *filtered]


def _split_range_value(raw: Any, default_min: Any, default_max: Any) -> tuple[str, str]:
    value = str(raw or "").strip()
    if not value:
        return str(default_min), str(default_max)
    if "-" in value:
        left, right = value.split("-", 1)
        left = left.strip()
        right = right.strip()
        if not left and not right:
            return str(default_min), str(default_max)
        if not left:
            left = right
        if not right:
            right = left
        return left, right
    return value, value


def _compose_range_value(min_value: str, max_value: str, fallback: Any) -> str:
    min_text = str(min_value or "").strip()
    max_text = str(max_value or "").strip()
    if not min_text and not max_text:
        return str(fallback)
    if not min_text:
        min_text = max_text
    if not max_text:
        max_text = min_text
    if min_text == max_text:
        return min_text
    return f"{min_text}-{max_text}"


def _today_mmdd() -> str:
    from datetime import datetime

    return datetime.now().strftime("%m%d")


def _default_schedule_date() -> str:
    from datetime import datetime, timedelta

    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def _schedule_date_values(days: int = 120) -> list[str]:
    from datetime import datetime, timedelta

    start = datetime.now().date()
    return [(start + timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)]


def _schedule_time_values(step_minutes: int = 15) -> list[str]:
    values: list[str] = []
    for hour in range(24):
        for minute in range(0, 60, step_minutes):
            values.append(f"{hour:02d}:{minute:02d}")
    return values


def _split_schedule_text(raw: str) -> tuple[str, str]:
    from datetime import datetime

    value = str(raw or "").strip()
    if not value:
        return "", ""
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime("%Y-%m-%d"), parsed.strftime("%H:%M")
        except Exception:
            continue
    return "", ""


def _compose_schedule_text(date_value: str, time_value: str) -> str:
    date_text = str(date_value or "").strip()
    time_text = str(time_value or "").strip()
    if not date_text or not time_text:
        return ""
    return f"{date_text} {time_text}"


def _format_runtime_duration(seconds: float | int | None) -> str:
    total = max(0, int(seconds or 0))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _suspend_windows_process(pid: int) -> None:
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


def _resume_windows_process(pid: int) -> None:
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


def _to_data_url(image: Image.Image) -> str:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _ensure_parent(widget: ctk.CTkBaseClass, row: int) -> ctk.CTkFrame:
    frame = ctk.CTkFrame(widget, fg_color="transparent")
    frame.grid(row=row, column=0, sticky="ew", padx=12, pady=6)
    frame.grid_columnconfigure(1, weight=1)
    return frame


class DashboardApp(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        self.title("YouTube 自动化统一控制台")
        self.geometry("1500x960")
        self.minsize(1320, 840)

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.ui_action_queue: queue.Queue[Callable[[], None]] = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.worker_process: subprocess.Popen[str] | None = None
        self.upload_monitor_thread: threading.Thread | None = None
        self.worker_processes: list[subprocess.Popen[str]] = []
        self.upload_monitor_threads: list[threading.Thread] = []
        self._upload_process_lock = threading.Lock()
        self._upload_failures: list[str] = []
        self._run_result_map: dict[str, dict[str, dict[str, str]]] = {}
        self._run_plan_for_summary: Any = None
        self._run_execution_result: Any = None
        self._run_report_logged = False
        self._audience_data_url: str = ""
        self._state = self._load_state()
        self.window_tasks: list[WindowTask] = []
        if self._state.pop("window_tasks", None) is not None:
            try:
                STATE_FILE.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
        self.scheduler_config = load_scheduler_settings()
        self.prompt_config = load_prompt_settings()
        self.group_catalog = get_group_catalog()

        self._build_variables()
        self._build_layout()
        self._refresh_groups()
        self._refresh_prompt_dropdowns()
        self._load_prompt_for_group()
        self._refresh_task_tree()
        self._refresh_bindings_box()
        self._bind_variable_events()
        self._sync_schedule_mode_state()
        self._refresh_schedule_controls()
        self._apply_run_status()
        self.after(150, self._drain_log_queue)
        self.after(1000, self._tick_run_status)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_variables(self) -> None:
        state = self._state
        add_schedule_date, add_schedule_time = _split_schedule_text(state.get("add_schedule", ""))
        default_schedule_date, default_schedule_time = _split_schedule_text(state.get("schedule_start", ""))
        legacy_mode = state.get("task_mode", "render_only")
        self.task_mode_var = ctk.StringVar(value=legacy_mode)
        self.run_metadata_var = ctk.BooleanVar(
            value=bool(
                state.get(
                    "run_metadata",
                    bool(state.get("generate_text", True) or state.get("generate_thumbnails", True)),
                )
            )
        )
        self.run_render_var = ctk.BooleanVar(
            value=bool(state.get("run_render", legacy_mode in {"render_only", "render_and_upload"}))
        )
        self.run_upload_var = ctk.BooleanVar(
            value=bool(state.get("run_upload", legacy_mode in {"upload_only", "render_and_upload"}))
        )
        self.date_var = ctk.StringVar(value=state.get("date_mmdd", _today_mmdd()))
        self.simulate_seconds_var = ctk.StringVar(value=str(state.get("simulate_seconds", 90)))
        self.randomize_effects_var = ctk.BooleanVar(value=False)
        visual_cfg = dict(self.scheduler_config.get("visual_settings") or {})
        self.visual_spectrum_var = ctk.StringVar(value=str(state.get("visual_spectrum", visual_cfg.get("spectrum", "yes"))))
        self.visual_timeline_var = ctk.StringVar(value=str(state.get("visual_timeline", visual_cfg.get("timeline", "yes"))))
        self.visual_letterbox_var = ctk.StringVar(value=str(state.get("visual_letterbox", visual_cfg.get("letterbox", "no"))))
        self.visual_zoom_var = ctk.StringVar(value=str(state.get("visual_zoom", visual_cfg.get("zoom", "normal"))))
        self.visual_style_var = ctk.StringVar(value=str(state.get("visual_style", visual_cfg.get("style", "bar"))))
        self.visual_color_spectrum_var = ctk.StringVar(value=str(state.get("visual_color_spectrum", visual_cfg.get("color_spectrum", "WhiteGold"))))
        self.visual_color_timeline_var = ctk.StringVar(value=str(state.get("visual_color_timeline", visual_cfg.get("color_timeline", "WhiteGold"))))
        self.visual_spectrum_y_var = ctk.StringVar(value=str(state.get("visual_spectrum_y", visual_cfg.get("spectrum_y", 530))))
        self.visual_spectrum_x_var = ctk.StringVar(value=str(state.get("visual_spectrum_x", visual_cfg.get("spectrum_x", -1))))
        spectrum_w_min, spectrum_w_max = _split_range_value(
            state.get("visual_spectrum_w", visual_cfg.get("spectrum_w", 1200)),
            1200,
            1200,
        )
        self.visual_spectrum_w_min_var = ctk.StringVar(value=spectrum_w_min)
        self.visual_spectrum_w_max_var = ctk.StringVar(value=spectrum_w_max)
        self.visual_film_grain_var = ctk.StringVar(value=str(state.get("visual_film_grain", visual_cfg.get("film_grain", "no"))))
        grain_strength_min, grain_strength_max = _split_range_value(
            state.get("visual_grain_strength", visual_cfg.get("grain_strength", 15)),
            15,
            15,
        )
        self.visual_grain_strength_min_var = ctk.StringVar(value=grain_strength_min)
        self.visual_grain_strength_max_var = ctk.StringVar(value=grain_strength_max)
        self.visual_vignette_var = ctk.StringVar(value=str(state.get("visual_vignette", visual_cfg.get("vignette", "no"))))
        self.visual_tint_var = ctk.StringVar(value=str(state.get("visual_tint", visual_cfg.get("color_tint", "none"))))
        self.visual_soft_focus_var = ctk.StringVar(value=str(state.get("visual_soft_focus", visual_cfg.get("soft_focus", "no"))))
        soft_focus_min, soft_focus_max = _split_range_value(
            state.get("visual_soft_focus_sigma", visual_cfg.get("soft_focus_sigma", 1.5)),
            1.5,
            1.5,
        )
        self.visual_soft_focus_sigma_min_var = ctk.StringVar(value=soft_focus_min)
        self.visual_soft_focus_sigma_max_var = ctk.StringVar(value=soft_focus_max)
        self.visual_particle_var = ctk.StringVar(value=str(state.get("visual_particle", visual_cfg.get("particle", "none"))))
        particle_opacity_min, particle_opacity_max = _split_range_value(
            state.get("visual_particle_opacity", visual_cfg.get("particle_opacity", 0.6)),
            0.6,
            0.6,
        )
        self.visual_particle_opacity_min_var = ctk.StringVar(value=particle_opacity_min)
        self.visual_particle_opacity_max_var = ctk.StringVar(value=particle_opacity_max)
        particle_speed_min, particle_speed_max = _split_range_value(
            state.get("visual_particle_speed", visual_cfg.get("particle_speed", 1.0)),
            1.0,
            1.0,
        )
        self.visual_particle_speed_min_var = ctk.StringVar(value=particle_speed_min)
        self.visual_particle_speed_max_var = ctk.StringVar(value=particle_speed_max)
        self.visual_text_var = ctk.StringVar(value=str(state.get("visual_text", visual_cfg.get("text", ""))))
        self.visual_text_font_var = ctk.StringVar(value=str(state.get("visual_text_font", visual_cfg.get("text_font", "default"))))
        self.visual_text_pos_var = ctk.StringVar(value=str(state.get("visual_text_pos", visual_cfg.get("text_pos", "center"))))
        text_size_min, text_size_max = _split_range_value(
            state.get("visual_text_size", visual_cfg.get("text_size", 60)),
            60,
            60,
        )
        self.visual_text_size_min_var = ctk.StringVar(value=text_size_min)
        self.visual_text_size_max_var = ctk.StringVar(value=text_size_max)
        self.visual_text_style_var = ctk.StringVar(value=str(state.get("visual_text_style", visual_cfg.get("text_style", "Classic"))))
        self.visual_preset_var = ctk.StringVar(value=str(state.get("visual_preset", visual_cfg.get("preset", "none"))))
        self.visual_bass_pulse_var = ctk.StringVar(
            value=str(state.get("visual_bass_pulse", "yes" if visual_cfg.get("bass_pulse", False) else "no"))
        )
        bass_scale_min, bass_scale_max = _split_range_value(
            state.get("visual_bass_pulse_scale", visual_cfg.get("bass_pulse_scale", 0.03)),
            0.03,
            0.03,
        )
        self.visual_bass_pulse_scale_min_var = ctk.StringVar(value=bass_scale_min)
        self.visual_bass_pulse_scale_max_var = ctk.StringVar(value=bass_scale_max)
        bass_brightness_min, bass_brightness_max = _split_range_value(
            state.get("visual_bass_pulse_brightness", visual_cfg.get("bass_pulse_brightness", 0.04)),
            0.04,
            0.04,
        )
        self.visual_bass_pulse_brightness_min_var = ctk.StringVar(value=bass_brightness_min)
        self.visual_bass_pulse_brightness_max_var = ctk.StringVar(value=bass_brightness_max)
        self.generate_text_var = ctk.BooleanVar(value=bool(state.get("generate_text", True)))
        self.generate_thumbnails_var = ctk.BooleanVar(value=bool(state.get("generate_thumbnails", True)))
        metadata_mode = state.get("metadata_mode", "prompt_api")
        metadata_label = next((label for label, value in METADATA_MODE_LABELS.items() if value == metadata_mode), "提示词那套")
        self.metadata_mode_var = ctk.StringVar(value=metadata_label)

        first_group = next(iter(self.group_catalog.keys()), "")
        current_group = state.get("current_group") or first_group
        self.current_group_var = ctk.StringVar(value=current_group)
        self.source_dir_override_var = ctk.StringVar(value=state.get("source_dir_override", ""))
        self.add_ypp_var = ctk.StringVar(value=state.get("add_ypp", "no"))
        self.add_quantity_var = ctk.StringVar(value=str(state.get("add_quantity", "1")))
        self.add_title_var = ctk.StringVar(value=state.get("add_title", ""))
        self.add_visibility_var = ctk.StringVar(value=state.get("add_visibility", "public"))
        self.add_category_var = ctk.StringVar(value=state.get("add_category", "Music"))
        self.add_kids_var = ctk.StringVar(value=state.get("add_kids", "no"))
        self.add_ai_var = ctk.StringVar(value=state.get("add_ai", "yes"))
        self.add_notify_var = ctk.BooleanVar(value=bool(state.get("add_notify_subscribers", False)))
        self.add_schedule_enabled_var = ctk.BooleanVar(
            value=bool(state.get("add_schedule_enabled", bool(add_schedule_date))),
        )
        self.add_schedule_date_var = ctk.StringVar(
            value=state.get("add_schedule_date", add_schedule_date or _default_schedule_date()),
        )
        self.add_schedule_time_var = ctk.StringVar(
            value=state.get("add_schedule_time", add_schedule_time or "06:00"),
        )
        self.add_schedule_timezone_var = ctk.StringVar(
            value=state.get(
                "add_schedule_timezone",
                state.get("schedule_timezone", SCHEDULE_TIMEZONE_VALUES[0]),
            ),
        )

        self.default_visibility_var = ctk.StringVar(value=state.get("default_visibility", "public"))
        self.default_category_var = ctk.StringVar(value=state.get("default_category", "Music"))
        self.default_kids_var = ctk.StringVar(value=state.get("default_kids", "no"))
        self.default_ai_var = ctk.StringVar(value=state.get("default_ai", "yes"))
        self.default_notify_var = ctk.BooleanVar(value=bool(state.get("default_notify_subscribers", False)))
        self.schedule_enabled_var = ctk.BooleanVar(value=bool(state.get("schedule_enabled", False)))
        self.schedule_date_var = ctk.StringVar(
            value=state.get("schedule_date", default_schedule_date or _default_schedule_date()),
        )
        self.schedule_time_var = ctk.StringVar(
            value=state.get("schedule_time", default_schedule_time or "06:00"),
        )
        self.schedule_timezone_var = ctk.StringVar(
            value=state.get("schedule_timezone", SCHEDULE_TIMEZONE_VALUES[0]),
        )
        self.schedule_interval_var = ctk.StringVar(value=str(state.get("schedule_interval", 60)))
        self.upload_auto_close_var = ctk.BooleanVar(value=bool(state.get("upload_auto_close", False)))
        legacy_upload_click_delay_ms = str(state.get("upload_click_delay_ms", "0"))
        legacy_upload_click_delay_min_ms = str(state.get("upload_click_delay_min_ms", legacy_upload_click_delay_ms))
        legacy_upload_click_delay_max_ms = str(state.get("upload_click_delay_max_ms", legacy_upload_click_delay_ms))
        self.upload_delay_mode_var = ctk.StringVar(value=str(state.get("upload_delay_mode", "抖动")))
        self.upload_file_delay_min_ms_var = ctk.StringVar(
            value=str(state.get("upload_file_delay_min_ms", legacy_upload_click_delay_min_ms))
        )
        self.upload_file_delay_max_ms_var = ctk.StringVar(
            value=str(state.get("upload_file_delay_max_ms", legacy_upload_click_delay_max_ms))
        )
        self.upload_next_delay_min_ms_var = ctk.StringVar(
            value=str(state.get("upload_next_delay_min_ms", legacy_upload_click_delay_min_ms))
        )
        self.upload_next_delay_max_ms_var = ctk.StringVar(
            value=str(state.get("upload_next_delay_max_ms", legacy_upload_click_delay_max_ms))
        )
        self.upload_done_delay_min_ms_var = ctk.StringVar(
            value=str(state.get("upload_done_delay_min_ms", legacy_upload_click_delay_min_ms))
        )
        self.upload_done_delay_max_ms_var = ctk.StringVar(
            value=str(state.get("upload_done_delay_max_ms", legacy_upload_click_delay_max_ms))
        )
        self.upload_publish_delay_min_ms_var = ctk.StringVar(
            value=str(state.get("upload_publish_delay_min_ms", legacy_upload_click_delay_min_ms))
        )
        self.upload_publish_delay_max_ms_var = ctk.StringVar(
            value=str(state.get("upload_publish_delay_max_ms", legacy_upload_click_delay_max_ms))
        )
        self.upload_click_delay_min_ms_var = ctk.StringVar(
            value=str(state.get("upload_click_delay_min_ms", legacy_upload_click_delay_ms))
        )
        self.upload_click_delay_max_ms_var = ctk.StringVar(
            value=str(state.get("upload_click_delay_max_ms", legacy_upload_click_delay_ms))
        )

        self.music_dir_var = ctk.StringVar(value=str(self.scheduler_config.get("music_dir", "")))
        self.base_image_dir_var = ctk.StringVar(value=str(self.scheduler_config.get("base_image_dir", "")))
        self.metadata_root_var = ctk.StringVar(value=str(get_metadata_root(self.scheduler_config)))
        self.output_root_var = ctk.StringVar(value=str(self.scheduler_config.get("output_root", "")))
        self.ffmpeg_var = ctk.StringVar(value=str(self.scheduler_config.get("ffmpeg_bin", "ffmpeg")))
        self.used_media_root_var = ctk.StringVar(value=str(self.scheduler_config.get("used_media_root", "")))
        self.cleanup_days_var = ctk.StringVar(value=str(self.scheduler_config.get("render_cleanup_days", 5)))
        self._runtime_path_widgets: dict[str, Any] = {}
        self.binding_group_var = ctk.StringVar(value=current_group)
        self.binding_folder_var = ctk.StringVar(value=get_group_bindings(self.scheduler_config).get(current_group, ""))

        self.prompt_group_var = ctk.StringVar(value=state.get("prompt_group", current_group))
        self.api_preset_var = ctk.StringVar(value="")
        self.content_template_var = ctk.StringVar(value="")
        self.api_save_name_var = ctk.StringVar(value="")
        self.content_save_name_var = ctk.StringVar(value="")
        self._loading_prompt_form = False
        self.prompt_form_dirty = False
        self.provider_var = ctk.StringVar(value="openai_compatible")
        self.base_url_var = ctk.StringVar(value="")
        self.model_var = ctk.StringVar(value="")
        self.api_key_var = ctk.StringVar(value="")
        self.temperature_var = ctk.StringVar(value="0.9")
        self.max_tokens_var = ctk.StringVar(value="16000")
        self.auto_image_var = ctk.StringVar(value="0")
        self.image_base_url_var = ctk.StringVar(value="")
        self.image_api_key_var = ctk.StringVar(value="")
        self.image_model_var = ctk.StringVar(value="")
        self.image_concurrency_var = ctk.StringVar(value="3")
        self.content_language_var = ctk.StringVar(value="zh-TW")
        self.music_genre_var = ctk.StringVar(value="")
        self.angle_var = ctk.StringVar(value="")
        self.audience_var = ctk.StringVar(value="")
        self.title_count_var = ctk.StringVar(value="3")
        self.desc_count_var = ctk.StringVar(value="1")
        self.thumb_count_var = ctk.StringVar(value="3")
        self.title_min_var = ctk.StringVar(value="80")
        self.title_max_var = ctk.StringVar(value="95")
        self.desc_len_var = ctk.StringVar(value="300")
        self.tag_range_var = ctk.StringVar(value="10-20")


        self.run_status_var = ctk.StringVar(value="空闲")
        self.run_phase_var = ctk.StringVar(value="等待任务")
        self.run_detail_var = ctk.StringVar(value="当前没有在运行的任务")
        self.run_progress_var = ctk.StringVar(value="0/0")
        self.run_elapsed_var = ctk.StringVar(value="00:00")
        self.run_eta_var = ctk.StringVar(value="--")
        self.run_last_log_var = ctk.StringVar(value="最近日志会显示在这里")
        self.pause_button_text_var = ctk.StringVar(value="暂停")

        self._run_started_at: float | None = None
        self._run_mode_label: str = ""
        self._run_total_items: int = 0
        self._run_total_steps: int = 0
        self._run_completed_steps: int = 0
        self._run_current_ratio: float = 0.0
        self._run_current_item: str = ""
        self._run_phase: str = "空闲"
        self._run_include_upload: bool = False
        self._run_render_done: set[str] = set()
        self._run_upload_done: set[str] = set()
        self.execution_control: ExecutionControl | None = None
        self._run_paused: bool = False
        self._cancel_requested: bool = False

    def _build_layout(self) -> None:
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        header = ctk.CTkFrame(self, corner_radius=18)
        header.grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 8))
        header.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            header,
            text="YouTube 自动化统一控制台",
            font=ctk.CTkFont(size=34, weight="bold"),
        ).grid(row=0, column=0, sticky="w", padx=20, pady=(18, 4))
        ctk.CTkLabel(
            header,
            text="上传页负责今天哪些窗口要工作，其他页面分别管理提示词、路径和高级视觉。",
            text_color="#b8c1cc",
            font=ctk.CTkFont(size=14),
        ).grid(row=1, column=0, sticky="w", padx=20, pady=(0, 18))

        status_frame = ctk.CTkFrame(header)
        status_frame.grid(row=2, column=0, sticky="ew", padx=20, pady=(0, 18))
        for column in range(4):
            status_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(status_frame, text="运行状态", font=ctk.CTkFont(size=18, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=14, pady=(12, 4)
        )
        ctk.CTkLabel(status_frame, textvariable=self.run_status_var).grid(
            row=0, column=1, sticky="w", padx=8, pady=(12, 4)
        )
        ctk.CTkLabel(status_frame, text="阶段").grid(row=1, column=0, sticky="w", padx=14, pady=(0, 4))
        ctk.CTkLabel(status_frame, textvariable=self.run_phase_var).grid(
            row=1, column=1, columnspan=3, sticky="w", padx=8, pady=(0, 4)
        )
        ctk.CTkLabel(status_frame, text="当前任务").grid(row=2, column=0, sticky="w", padx=14, pady=(0, 4))
        ctk.CTkLabel(status_frame, textvariable=self.run_detail_var).grid(
            row=2, column=1, columnspan=3, sticky="w", padx=8, pady=(0, 4)
        )
        ctk.CTkLabel(status_frame, text="进度").grid(row=3, column=0, sticky="w", padx=14, pady=(0, 4))
        ctk.CTkLabel(status_frame, textvariable=self.run_progress_var).grid(
            row=3, column=1, sticky="w", padx=8, pady=(0, 4)
        )
        ctk.CTkLabel(status_frame, text="已运行").grid(row=3, column=2, sticky="w", padx=8, pady=(0, 4))
        ctk.CTkLabel(status_frame, textvariable=self.run_elapsed_var).grid(
            row=3, column=3, sticky="w", padx=8, pady=(0, 4)
        )
        ctk.CTkLabel(status_frame, text="预计剩余").grid(row=4, column=0, sticky="w", padx=14, pady=(0, 4))
        ctk.CTkLabel(status_frame, textvariable=self.run_eta_var).grid(
            row=4, column=1, sticky="w", padx=8, pady=(0, 4)
        )
        ctk.CTkLabel(status_frame, text="最近日志").grid(row=4, column=2, sticky="w", padx=8, pady=(0, 4))
        ctk.CTkLabel(status_frame, textvariable=self.run_last_log_var).grid(
            row=4, column=3, sticky="w", padx=8, pady=(0, 4)
        )
        self.run_progress_bar = ctk.CTkProgressBar(status_frame)
        self.run_progress_bar.grid(row=5, column=0, columnspan=4, sticky="ew", padx=14, pady=(6, 12))
        self.run_progress_bar.set(0.0)
        control_bar = ctk.CTkFrame(status_frame, fg_color="transparent")
        control_bar.grid(row=6, column=0, columnspan=4, sticky="w", padx=10, pady=(0, 12))
        self.pause_button = ctk.CTkButton(
            control_bar,
            textvariable=self.pause_button_text_var,
            command=self._toggle_pause_current_task,
            width=120,
        )
        self.pause_button.pack(side="left", padx=4)
        self.cancel_button = ctk.CTkButton(
            control_bar,
            text="取消当前批次",
            command=self._cancel_current_task,
            width=140,
            fg_color="#7a1f1f",
            hover_color="#932525",
        )
        self.cancel_button.pack(side="left", padx=4)

        self.tabview = ctk.CTkTabview(self, corner_radius=18)
        self.tabview.grid(row=1, column=0, sticky="nsew", padx=18, pady=(0, 18))
        for name in ("快捷开始", "上传", "提示词", "路径配置", "日志"):
            self.tabview.add(name)
        self.tabview.add("高级视觉")

        self._build_start_tab()
        self._build_upload_tab()
        self._build_visual_tab()
        self._build_prompt_tab()
        self._build_paths_tab()
        self._build_log_tab()

    def _bind_variable_events(self) -> None:
        self.current_group_var.trace_add("write", lambda *_: self._refresh_window_buttons())
        self.binding_group_var.trace_add("write", lambda *_: self.binding_folder_var.set(get_group_bindings(self.scheduler_config).get(self.binding_group_var.get(), "")))
        self.prompt_group_var.trace_add("write", lambda *_: self._load_prompt_for_group())
        self.run_metadata_var.trace_add("write", lambda *_: self._preview_plan())
        self.run_render_var.trace_add("write", lambda *_: self._preview_plan())
        self.run_upload_var.trace_add("write", lambda *_: self._preview_plan())
        self.add_visibility_var.trace_add("write", self._on_add_visibility_change)
        self.default_visibility_var.trace_add("write", self._on_default_visibility_change)
        self.add_schedule_enabled_var.trace_add("write", self._on_add_schedule_toggle)
        self.schedule_enabled_var.trace_add("write", self._on_default_schedule_toggle)
        for runtime_var in (
            self.metadata_root_var,
            self.music_dir_var,
            self.base_image_dir_var,
            self.output_root_var,
            self.ffmpeg_var,
            self.used_media_root_var,
            self.cleanup_days_var,
        ):
            runtime_var.trace_add("write", lambda *_: self._refresh_runtime_config_cache())

        for prompt_var in (
            self.api_preset_var,
            self.content_template_var,
            self.api_save_name_var,
            self.content_save_name_var,
            self.provider_var,
            self.base_url_var,
            self.model_var,
            self.api_key_var,
            self.temperature_var,
            self.max_tokens_var,
            self.auto_image_var,
            self.image_base_url_var,
            self.image_api_key_var,
            self.image_model_var,
            self.image_concurrency_var,
            self.music_genre_var,
            self.angle_var,
            self.audience_var,
            self.content_language_var,
            self.title_count_var,
            self.desc_count_var,
            self.thumb_count_var,
            self.title_min_var,
            self.title_max_var,
            self.desc_len_var,
            self.tag_range_var,
        ):
            prompt_var.trace_add("write", lambda *_: self._mark_prompt_form_dirty())

    def _refresh_runtime_config_cache(self) -> None:
        try:
            self.scheduler_config = self._current_runtime_config()
        except Exception:
            pass

    def _mark_prompt_form_dirty(self) -> None:
        if self._loading_prompt_form:
            return
        self.prompt_form_dirty = True


    def _build_upload_tab(self) -> None:
        base_tab = self.tabview.tab("上传")
        base_tab.grid_columnconfigure(0, weight=1)
        base_tab.grid_rowconfigure(0, weight=1)
        tab = ctk.CTkScrollableFrame(base_tab)
        tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        tab.grid_columnconfigure(0, weight=1)

        group_frame = ctk.CTkFrame(tab)
        group_frame.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        for column in range(6):
            group_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(group_frame, text="今天哪些窗口要工作", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkLabel(group_frame, text="分组").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 10))
        self.current_group_menu = ctk.CTkOptionMenu(group_frame, variable=self.current_group_var, values=[""])
        self.current_group_menu.grid(row=1, column=1, sticky="ew", padx=(0, 12), pady=(0, 10))
        ctk.CTkButton(group_frame, text="刷新分组", command=self._refresh_groups).grid(
            row=1, column=2, sticky="ew", padx=(0, 12), pady=(0, 10)
        )
        ctk.CTkLabel(group_frame, text="新增窗口用素材目录覆盖").grid(
            row=1, column=3, sticky="w", padx=(0, 8), pady=(0, 10)
        )
        ctk.CTkEntry(group_frame, textvariable=self.source_dir_override_var).grid(
            row=1, column=4, sticky="ew", padx=(0, 12), pady=(0, 10)
        )
        ctk.CTkButton(group_frame, text="选择文件夹", command=self._pick_source_override).grid(
            row=1, column=5, sticky="ew", padx=(0, 16), pady=(0, 10)
        )
        ctk.CTkLabel(group_frame, text="文案来源").grid(row=2, column=0, sticky="w", padx=(16, 8), pady=(0, 10))
        ctk.CTkLabel(
            group_frame,
            text="提示词那套（唯一文案来源）",
            text_color="#d7e3f4",
        ).grid(row=2, column=1, sticky="w", padx=(0, 12), pady=(0, 10))
        ctk.CTkSwitch(group_frame, text="生成标题/简介/标签", variable=self.generate_text_var).grid(
            row=2, column=2, sticky="w", padx=(0, 12), pady=(0, 10)
        )
        ctk.CTkSwitch(group_frame, text="重生成缩略图", variable=self.generate_thumbnails_var).grid(
            row=2, column=3, sticky="w", padx=(0, 12), pady=(0, 10)
        )
        ctk.CTkLabel(
            group_frame,
            text="文案结果会直接写入当前文案输出目录和上传清单",
            text_color="#9fb2c8",
        ).grid(row=2, column=4, columnspan=2, sticky="w", padx=(0, 16), pady=(0, 10))

        add_frame = ctk.CTkFrame(tab)
        add_frame.grid(row=1, column=0, sticky="ew", padx=16, pady=8)
        for column in range(6):
            add_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(add_frame, text="????? YPP").grid(row=0, column=0, sticky="w", padx=(16, 8), pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_ypp_var, values=YES_NO_VALUES).grid(
            row=1, column=0, sticky="ew", padx=(16, 8), pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="??(???)").grid(row=0, column=1, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkEntry(add_frame, textvariable=self.add_quantity_var).grid(
            row=1, column=1, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="???").grid(row=0, column=2, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_visibility_var, values=VISIBILITY_VALUES).grid(
            row=1, column=2, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="??").grid(row=0, column=3, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_category_var, values=CATEGORY_VALUES).grid(
            row=1, column=3, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="????").grid(row=0, column=4, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_kids_var, values=YES_NO_VALUES).grid(
            row=1, column=4, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="AI ??").grid(row=0, column=5, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_ai_var, values=YES_NO_VALUES).grid(
            row=1, column=5, sticky="ew", padx=(8, 16), pady=(0, 14)
        )
        ctk.CTkCheckBox(
            add_frame,
            text="通知订阅用户",
            variable=self.add_notify_var,
        ).grid(row=2, column=5, sticky="w", padx=8, pady=(0, 6))
        self.add_schedule_checkbox = ctk.CTkCheckBox(
            add_frame,
            text="窗口定时覆盖",
            variable=self.add_schedule_enabled_var,
        )
        self.add_schedule_checkbox.grid(row=2, column=0, sticky="w", padx=16, pady=(0, 6))
        ctk.CTkLabel(add_frame, text="日期").grid(row=2, column=1, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkLabel(add_frame, text="时间").grid(row=2, column=2, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkLabel(add_frame, text="时区").grid(row=2, column=3, sticky="w", padx=8, pady=(0, 6))
        self.add_schedule_date_menu = ctk.CTkOptionMenu(
            add_frame,
            variable=self.add_schedule_date_var,
            values=_schedule_date_values(),
        )
        self.add_schedule_date_menu.grid(row=3, column=1, sticky="ew", padx=8, pady=(0, 14))
        self.add_schedule_time_menu = ctk.CTkOptionMenu(
            add_frame,
            variable=self.add_schedule_time_var,
            values=_schedule_time_values(),
        )
        self.add_schedule_time_menu.grid(row=3, column=2, sticky="ew", padx=8, pady=(0, 14))
        self.add_schedule_timezone_menu = ctk.CTkOptionMenu(
            add_frame,
            variable=self.add_schedule_timezone_var,
            values=SCHEDULE_TIMEZONE_VALUES,
        )
        self.add_schedule_timezone_menu.grid(row=3, column=3, sticky="ew", padx=8, pady=(0, 14))

        default_frame = ctk.CTkFrame(tab)
        default_frame.grid(row=2, column=0, sticky="ew", padx=16, pady=8)
        for column in range(7):
            default_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(default_frame, text="统一默认规则", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkLabel(default_frame, text="可见性").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        ctk.CTkOptionMenu(default_frame, variable=self.default_visibility_var, values=VISIBILITY_VALUES).grid(
            row=2, column=0, sticky="ew", padx=(16, 8), pady=(0, 12)
        )
        ctk.CTkLabel(default_frame, text="分类").grid(row=1, column=1, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkOptionMenu(default_frame, variable=self.default_category_var, values=CATEGORY_VALUES).grid(
            row=2, column=1, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkLabel(default_frame, text="儿童内容").grid(row=1, column=2, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkOptionMenu(default_frame, variable=self.default_kids_var, values=YES_NO_VALUES).grid(
            row=2, column=2, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkLabel(default_frame, text="AI 内容").grid(row=1, column=3, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkOptionMenu(default_frame, variable=self.default_ai_var, values=YES_NO_VALUES).grid(
            row=2, column=3, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkCheckBox(
            default_frame,
            text="通知订阅用户",
            variable=self.default_notify_var,
        ).grid(
            row=2, column=4, sticky="w", padx=8, pady=(0, 12)
        )
        self.schedule_enabled_checkbox = ctk.CTkCheckBox(
            default_frame,
            text="启用定时发布",
            variable=self.schedule_enabled_var,
        )
        self.schedule_enabled_checkbox.grid(
            row=2, column=5, sticky="w", padx=8, pady=(0, 12)
        )
        ctk.CTkSwitch(
            default_frame,
            text="上传完成后自动关闭窗口",
            variable=self.upload_auto_close_var,
        ).grid(
            row=2, column=6, sticky="w", padx=(8, 16), pady=(0, 12)
        )
        ctk.CTkLabel(default_frame, text="发布日期").grid(row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        ctk.CTkLabel(default_frame, text="发布时间").grid(row=3, column=1, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkLabel(default_frame, text="时区").grid(row=3, column=2, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkLabel(default_frame, text="定时间隔(分钟)").grid(row=3, column=3, sticky="w", padx=8, pady=(0, 6))
        self.schedule_date_menu = ctk.CTkOptionMenu(
            default_frame,
            variable=self.schedule_date_var,
            values=_schedule_date_values(),
        )
        self.schedule_date_menu.grid(row=4, column=0, sticky="ew", padx=(16, 8), pady=(0, 14))
        self.schedule_time_menu = ctk.CTkOptionMenu(
            default_frame,
            variable=self.schedule_time_var,
            values=_schedule_time_values(),
        )
        self.schedule_time_menu.grid(row=4, column=1, sticky="ew", padx=8, pady=(0, 14))
        self.schedule_timezone_menu = ctk.CTkOptionMenu(
            default_frame,
            variable=self.schedule_timezone_var,
            values=SCHEDULE_TIMEZONE_VALUES,
        )
        self.schedule_timezone_menu.grid(row=4, column=2, sticky="ew", padx=8, pady=(0, 14))
        ctk.CTkEntry(default_frame, textvariable=self.schedule_interval_var).grid(
            row=4, column=3, sticky="ew", padx=8, pady=(0, 14)
        )

        self.window_button_frame = ctk.CTkFrame(tab)
        self.window_button_frame.grid(row=3, column=0, sticky="ew", padx=16, pady=8)

        task_frame = ctk.CTkFrame(tab)
        task_frame.grid(row=4, column=0, sticky="ew", padx=16, pady=(8, 16))
        task_frame.grid_columnconfigure(0, weight=1)
        action_bar = ctk.CTkFrame(task_frame, fg_color="transparent")
        action_bar.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
        ctk.CTkButton(action_bar, text="移除选中", command=self._remove_selected_tasks).pack(side="left", padx=6)
        ctk.CTkButton(action_bar, text="清空任务", command=self._clear_tasks).pack(side="left", padx=6)
        ctk.CTkButton(action_bar, text="恢复分组绑定目录", command=self._fill_binding_source).pack(side="left", padx=6)

        columns = ("tag", "serial", "quantity", "ypp", "visibility", "category", "kids", "ai", "schedule", "source")
        self.task_tree = ttk.Treeview(task_frame, columns=columns, show="headings", height=14)
        for key, width in {
            "tag": 180,
            "serial": 80,
            "quantity": 70,
            "ypp": 60,
            "visibility": 90,
            "category": 130,
            "kids": 70,
            "ai": 70,
            "schedule": 160,
            "source": 360,
        }.items():
            self.task_tree.heading(key, text=key)
            self.task_tree.column(key, width=width, stretch=key in {"tag", "source", "schedule"})
        self.task_tree.grid(row=1, column=0, sticky="ew", padx=8, pady=(4, 8))

    def _build_visual_tab(self) -> None:
        base_tab = self.tabview.tab("高级视觉")
        base_tab.grid_columnconfigure(0, weight=1)
        base_tab.grid_rowconfigure(0, weight=1)
        tab = ctk.CTkScrollableFrame(base_tab)
        tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        tab.grid_columnconfigure(0, weight=1)

        intro = ctk.CTkFrame(tab)
        intro.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
        intro.grid_columnconfigure(0, weight=1)
        intro.grid_columnconfigure(1, weight=0)
        intro.grid_columnconfigure(2, weight=0)
        ctk.CTkLabel(intro, text="高级视觉控制", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 8)
        )
        ctk.CTkButton(intro, text="保存视觉设置", command=self._save_visual_settings).grid(
            row=0, column=1, sticky="e", padx=16, pady=(14, 8)
        )
        ctk.CTkButton(intro, text="套用 MEGA BASS 预设", command=self._apply_visual_preset_mega_bass).grid(
            row=0, column=2, sticky="e", padx=(0, 16), pady=(14, 8)
        )
        ctk.CTkLabel(
            intro,
            text=(
                "这里改的是渲染特效，不影响上传规则。涉及“有没有”的开关仍按你手动勾选执行；"
                "只有你选成 random 的样式、配色、贴纸、字体和数值区间，才会按每个视频单独随机。"
            ),
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=1, column=0, columnspan=2, sticky="w", padx=16, pady=(0, 14))

        basic = ctk.CTkFrame(tab)
        basic.grid(row=1, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            basic.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(basic, text="基础效果", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._entry_row(basic, 1, "频谱", self.visual_spectrum_var, values=VISUAL_TOGGLE_VALUES)
        self._entry_row(basic, 2, "时间轴", self.visual_timeline_var, values=VISUAL_TOGGLE_VALUES)
        self._entry_row(basic, 3, "黑边", self.visual_letterbox_var, values=VISUAL_TOGGLE_VALUES)
        self._entry_row(basic, 4, "镜头缩放", self.visual_zoom_var, values=_with_random(list_zoom_modes()))
        self._entry_row(basic, 5, "频谱样式", self.visual_style_var, values=_with_random(list_effects()))
        self._entry_row(basic, 6, "频谱 Y", self.visual_spectrum_y_var)
        self._entry_row(basic, 7, "频谱 X (-1=居中)", self.visual_spectrum_x_var)
        self._range_row(basic, 8, "频谱宽度", self.visual_spectrum_w_min_var, self.visual_spectrum_w_max_var)

        mood = ctk.CTkFrame(tab)
        mood.grid(row=2, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            mood.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(mood, text="色彩与氛围", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._entry_row(mood, 1, "频谱配色", self.visual_color_spectrum_var, values=_with_random(list_palette_names()))
        self._entry_row(mood, 2, "时间轴配色", self.visual_color_timeline_var, values=_with_random(list_palette_names()))
        self._entry_row(mood, 3, "胶片颗粒", self.visual_film_grain_var, values=VISUAL_TOGGLE_VALUES)
        self._range_row(
            mood,
            4,
            "颗粒强度",
            self.visual_grain_strength_min_var,
            self.visual_grain_strength_max_var,
        )
        self._entry_row(mood, 5, "暗角", self.visual_vignette_var, values=VISUAL_TOGGLE_VALUES)
        self._entry_row(mood, 6, "色调", self.visual_tint_var, values=_with_random(list_tint_names()))
        self._entry_row(mood, 7, "柔焦", self.visual_soft_focus_var, values=VISUAL_TOGGLE_VALUES)
        self._range_row(
            mood,
            8,
            "柔焦强度",
            self.visual_soft_focus_sigma_min_var,
            self.visual_soft_focus_sigma_max_var,
        )

        preset = ctk.CTkFrame(tab)
        preset.grid(row=3, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            preset.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(preset, text="节奏联动 / 预设", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._entry_row(preset, 1, "视觉预设", self.visual_preset_var, values=["none", "mega_bass"])
        self._entry_row(preset, 2, "低频脉冲", self.visual_bass_pulse_var, values=VISUAL_TOGGLE_VALUES)
        self._range_row(
            preset,
            3,
            "脉冲缩放",
            self.visual_bass_pulse_scale_min_var,
            self.visual_bass_pulse_scale_max_var,
        )
        self._range_row(
            preset,
            4,
            "脉冲亮度",
            self.visual_bass_pulse_brightness_min_var,
            self.visual_bass_pulse_brightness_max_var,
        )

        overlay = ctk.CTkFrame(tab)
        overlay.grid(row=4, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            overlay.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(overlay, text="贴纸 / 粒子 / 叠字", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._entry_row(
            overlay,
            1,
            "贴纸 / 粒子",
            self.visual_particle_var,
            values=_with_random_first(list_particle_effects()),
        )
        self._range_row(
            overlay,
            2,
            "贴纸透明度",
            self.visual_particle_opacity_min_var,
            self.visual_particle_opacity_max_var,
        )
        self._range_row(
            overlay,
            3,
            "贴纸速度",
            self.visual_particle_speed_min_var,
            self.visual_particle_speed_max_var,
        )
        self._entry_row(overlay, 4, "叠字内容", self.visual_text_var)
        self._entry_row(overlay, 5, "字体", self.visual_text_font_var, values=_with_random(list_font_names()))
        self._entry_row(overlay, 6, "文字位置", self.visual_text_pos_var, values=_with_random(list_text_positions()))
        self._range_row(overlay, 7, "文字大小", self.visual_text_size_min_var, self.visual_text_size_max_var)
        self._entry_row(overlay, 8, "文字样式", self.visual_text_style_var, values=_with_random(list_text_styles()))

        help_frame = ctk.CTkFrame(tab)
        help_frame.grid(row=5, column=0, sticky="ew", padx=8, pady=(8, 16))
        help_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(help_frame, text="如何添加更多贴纸 / 特效", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 8)
        )
        ctk.CTkLabel(
            help_frame,
            text=(
                "1. 把新的 mp4 / mov / webm / mkv 叠层素材直接放进 overlays 文件夹。\n"
                "2. 重开控制台后，新文件名会自动出现在“贴纸 / 粒子”下拉里。\n"
                "3. 想按视频随机时，把下拉切成 random；像频谱宽度、颗粒强度、贴纸透明度、贴纸速度、文字大小，"
                "现在直接填左边最小值、右边最大值，系统会按每个视频单独随机。\n"
                "4. 如果要新增真正的新滤镜，再扩 effects_library.py 里的 get_effect。"
            ),
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=1, column=0, sticky="w", padx=16, pady=(0, 14))

    def _build_prompt_tab(self) -> None:
        base_tab = self.tabview.tab("提示词")
        base_tab.grid_columnconfigure(0, weight=1)
        base_tab.grid_rowconfigure(0, weight=1)
        tab = ctk.CTkScrollableFrame(base_tab)
        tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        tab.grid_columnconfigure(0, weight=1)

        top = ctk.CTkFrame(tab)
        top.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
        for column in range(6):
            top.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(top, text="模板入口", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkLabel(top, text="分组").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        self.prompt_group_menu = ctk.CTkOptionMenu(top, variable=self.prompt_group_var, values=[""])
        self.prompt_group_menu.grid(row=2, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
        ctk.CTkLabel(top, text="API 模板").grid(row=1, column=1, sticky="w", padx=8, pady=(0, 6))
        self.api_preset_menu = ctk.CTkOptionMenu(top, variable=self.api_preset_var, values=[""])
        self.api_preset_menu.grid(row=2, column=1, sticky="ew", padx=8, pady=(0, 12))
        ctk.CTkLabel(top, text="内容模板").grid(row=1, column=2, sticky="w", padx=8, pady=(0, 6))
        self.content_template_menu = ctk.CTkOptionMenu(top, variable=self.content_template_var, values=[""])
        self.content_template_menu.grid(row=2, column=2, sticky="ew", padx=8, pady=(0, 12))
        ctk.CTkButton(top, text="载入当前模板", command=self._load_prompt_for_group).grid(
            row=2, column=3, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkButton(top, text="绑定分组到 API 模板", command=self._bind_group_api).grid(
            row=2, column=4, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkButton(top, text="绑定分组到内容模板", command=self._bind_group_content).grid(
            row=2, column=5, sticky="ew", padx=(8, 16), pady=(0, 12)
        )
        ctk.CTkLabel(top, text="API 模板另存为").grid(row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        ctk.CTkEntry(top, textvariable=self.api_save_name_var).grid(
            row=4, column=0, sticky="ew", padx=(16, 8), pady=(0, 14)
        )
        ctk.CTkLabel(top, text="内容模板另存为").grid(row=3, column=1, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkEntry(top, textvariable=self.content_save_name_var).grid(
            row=4, column=1, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkButton(top, text="保存当前 API 模板", command=self._save_api_preset).grid(
            row=4, column=2, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkButton(top, text="保存当前内容模板", command=self._save_content_template).grid(
            row=4, column=3, sticky="ew", padx=8, pady=(0, 14)
        )

        api_frame = ctk.CTkFrame(tab)
        api_frame.grid(row=1, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            api_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(api_frame, text="文本 / 图片 API 模板", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._entry_row(api_frame, 1, "文本 Provider", self.provider_var, values=PROVIDER_VALUES)
        self._entry_row(api_frame, 2, "文本 Base URL", self.base_url_var)
        self._entry_row(api_frame, 3, "文本 Model", self.model_var)
        self._entry_row(api_frame, 4, "文本 API Key", self.api_key_var, show="*")
        self._entry_row(api_frame, 5, "Temperature", self.temperature_var)
        self._entry_row(api_frame, 6, "Max Tokens", self.max_tokens_var)
        self._entry_row(api_frame, 7, "自动出图", self.auto_image_var, values=AUTO_IMAGE_VALUES)
        self._entry_row(api_frame, 8, "图片 Base URL", self.image_base_url_var)
        self._entry_row(api_frame, 9, "图片 API Key", self.image_api_key_var, show="*")
        self._entry_row(api_frame, 10, "图片 Model", self.image_model_var)
        self._entry_row(api_frame, 11, "图片并发", self.image_concurrency_var)
        button_bar = ctk.CTkFrame(api_frame, fg_color="transparent")
        button_bar.grid(row=12, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 14))
        ctk.CTkButton(button_bar, text="测试文本连通性", command=self._test_text_api).pack(side="left", padx=6)
        ctk.CTkButton(button_bar, text="测试图片连通性", command=self._test_image_api).pack(side="left", padx=6)

        content_frame = ctk.CTkFrame(tab)
        content_frame.grid(row=2, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            content_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(content_frame, text="内容模板", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._entry_row(content_frame, 1, "音乐类型", self.music_genre_var)
        self._entry_row(content_frame, 2, "切入角度", self.angle_var)
        self._entry_row(content_frame, 3, "目标受众", self.audience_var)
        self._entry_row(content_frame, 4, "输出语言", self.content_language_var, values=LANGUAGE_VALUES)
        self._entry_row(content_frame, 5, "标题数", self.title_count_var)
        self._entry_row(content_frame, 6, "简介数", self.desc_count_var)
        self._entry_row(content_frame, 7, "缩略图数", self.thumb_count_var)
        self._entry_row(content_frame, 8, "标题最小字数", self.title_min_var)
        self._entry_row(content_frame, 9, "标题最大字数", self.title_max_var)
        self._entry_row(content_frame, 10, "简介目标字数", self.desc_len_var)
        self._entry_row(content_frame, 11, "标签数量区间", self.tag_range_var)
        ctk.CTkLabel(content_frame, text="主提示词").grid(row=12, column=0, sticky="w", padx=16, pady=(0, 6))
        self.master_prompt_box = ctk.CTkTextbox(content_frame, height=180)
        self.master_prompt_box.grid(row=13, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 12))
        ctk.CTkLabel(content_frame, text="标题库").grid(row=14, column=0, sticky="w", padx=16, pady=(0, 6))
        self.title_library_box = ctk.CTkTextbox(content_frame, height=160)
        self.title_library_box.grid(row=15, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 14))
        self.master_prompt_box.bind("<KeyRelease>", lambda *_: self._mark_prompt_form_dirty(), add="+")
        self.title_library_box.bind("<KeyRelease>", lambda *_: self._mark_prompt_form_dirty(), add="+")

        audience_frame = ctk.CTkFrame(tab)
        audience_frame.grid(row=3, column=0, sticky="ew", padx=8, pady=(8, 16))
        audience_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(audience_frame, text="受众截图自动识别", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 12)
        )
        bar = ctk.CTkFrame(audience_frame, fg_color="transparent")
        bar.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 8))
        ctk.CTkButton(bar, text="选择截图", command=self._choose_audience_image).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="粘贴截图", command=self._paste_audience_image).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="重新识别", command=self._reanalyze_audience_image).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="清空截图", command=self._clear_audience_image).pack(side="left", padx=6)
        self.audience_result_box = ctk.CTkTextbox(audience_frame, height=140)
        self.audience_result_box.grid(row=2, column=0, sticky="ew", padx=16, pady=(0, 14))

    def _build_log_tab(self) -> None:
        tab = self.tabview.tab("日志")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(1, weight=1)
        bar = ctk.CTkFrame(tab, fg_color="transparent")
        bar.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        ctk.CTkButton(bar, text="清空日志", command=self._clear_logs).pack(side="left", padx=6)
        self.log_box = ctk.CTkTextbox(tab)
        self.log_box.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))

    def _entry_row(
        self,
        parent: ctk.CTkFrame,
        row: int,
        label: str,
        variable: ctk.StringVar,
        *,
        values: list[str] | None = None,
        show: str | None = None,
        entry_key: str | None = None,
    ) -> None:
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=16, pady=(0, 6))
        if values:
            widget = ctk.CTkOptionMenu(parent, variable=variable, values=values)
        else:
            widget = ctk.CTkEntry(parent, textvariable=variable, show=show or "")
        widget.grid(row=row, column=1, columnspan=3, sticky="ew", padx=16, pady=(0, 12))
        if entry_key:
            self._runtime_path_widgets[entry_key] = widget

    def _range_row(
        self,
        parent: ctk.CTkFrame,
        row: int,
        label: str,
        min_var: ctk.StringVar,
        max_var: ctk.StringVar,
    ) -> None:
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=16, pady=(0, 6))
        ctk.CTkEntry(parent, textvariable=min_var, placeholder_text="最小值").grid(
            row=row, column=1, sticky="ew", padx=(16, 8), pady=(0, 12)
        )
        ctk.CTkLabel(parent, text="到").grid(row=row, column=2, sticky="ew", padx=4, pady=(0, 12))
        ctk.CTkEntry(parent, textvariable=max_var, placeholder_text="最大值").grid(
            row=row, column=3, sticky="ew", padx=(8, 16), pady=(0, 12)
        )

    def _bind_scroll_frame_wheel(self, scroll_frame: ctk.CTkScrollableFrame, *widgets: ctk.CTkBaseClass) -> None:
        canvas = getattr(scroll_frame, "_parent_canvas", None)
        if canvas is None:
            return

        try:
            canvas.configure(yscrollincrement=24)
        except Exception:
            pass

        def _on_mousewheel(event: Any) -> str | None:
            delta = 0
            if getattr(event, "delta", 0):
                delta = -int(event.delta / 120) if event.delta else 0
            elif getattr(event, "num", None) == 4:
                delta = -1
            elif getattr(event, "num", None) == 5:
                delta = 1
            if delta:
                canvas.yview_scroll(delta, "units")
                return "break"
            return None

        def _bind_tree(widget: Any) -> None:
            if isinstance(widget, ctk.CTkTextbox):
                return
            for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
                widget.bind(sequence, _on_mousewheel, add="+")
            for attr_name in ("_entry", "_text_label", "_canvas", "_dropdown_menu", "_scrollbar"):
                inner = getattr(widget, attr_name, None)
                if inner is None:
                    continue
                for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
                    try:
                        inner.bind(sequence, _on_mousewheel, add="+")
                    except Exception:
                        pass
            for child in widget.winfo_children():
                _bind_tree(child)

        for widget in widgets:
            _bind_tree(widget)

    def _runtime_field_text(self, key: str, variable: ctk.StringVar) -> str:
        widget = self._runtime_path_widgets.get(key)
        if widget is not None:
            try:
                value = str(widget.get()).strip()
                if value:
                    return value
            except Exception:
                pass
        return str(variable.get()).strip()

    def _collect_visual_settings(self) -> dict[str, Any]:
        return {
            "preset": self.visual_preset_var.get().strip() or "none",
            "spectrum": _bool_from_yes_no(self.visual_spectrum_var.get()),
            "timeline": _bool_from_yes_no(self.visual_timeline_var.get()),
            "letterbox": _bool_from_yes_no(self.visual_letterbox_var.get()),
            "zoom": self.visual_zoom_var.get().strip() or "normal",
            "style": self.visual_style_var.get().strip() or "bar",
            "color_spectrum": self.visual_color_spectrum_var.get().strip() or "WhiteGold",
            "color_timeline": self.visual_color_timeline_var.get().strip() or "WhiteGold",
            "spectrum_y": self.visual_spectrum_y_var.get().strip() or "530",
            "spectrum_x": self.visual_spectrum_x_var.get().strip() or "-1",
            "spectrum_w": _compose_range_value(
                self.visual_spectrum_w_min_var.get(),
                self.visual_spectrum_w_max_var.get(),
                "1200",
            ),
            "film_grain": _bool_from_yes_no(self.visual_film_grain_var.get()),
            "grain_strength": _compose_range_value(
                self.visual_grain_strength_min_var.get(),
                self.visual_grain_strength_max_var.get(),
                "15",
            ),
            "vignette": _bool_from_yes_no(self.visual_vignette_var.get()),
            "color_tint": self.visual_tint_var.get().strip() or "none",
            "soft_focus": _bool_from_yes_no(self.visual_soft_focus_var.get()),
            "soft_focus_sigma": _compose_range_value(
                self.visual_soft_focus_sigma_min_var.get(),
                self.visual_soft_focus_sigma_max_var.get(),
                "1.5",
            ),
            "particle": self.visual_particle_var.get().strip() or "none",
            "particle_opacity": _compose_range_value(
                self.visual_particle_opacity_min_var.get(),
                self.visual_particle_opacity_max_var.get(),
                "0.6",
            ),
            "particle_speed": _compose_range_value(
                self.visual_particle_speed_min_var.get(),
                self.visual_particle_speed_max_var.get(),
                "1.0",
            ),
            "bass_pulse": _bool_from_yes_no(self.visual_bass_pulse_var.get()),
            "bass_pulse_scale": _compose_range_value(
                self.visual_bass_pulse_scale_min_var.get(),
                self.visual_bass_pulse_scale_max_var.get(),
                "0.03",
            ),
            "bass_pulse_brightness": _compose_range_value(
                self.visual_bass_pulse_brightness_min_var.get(),
                self.visual_bass_pulse_brightness_max_var.get(),
                "0.04",
            ),
            "text": self.visual_text_var.get(),
            "text_font": self.visual_text_font_var.get().strip() or "default",
            "text_pos": self.visual_text_pos_var.get().strip() or "center",
            "text_size": _compose_range_value(
                self.visual_text_size_min_var.get(),
                self.visual_text_size_max_var.get(),
                "60",
            ),
            "text_style": self.visual_text_style_var.get().strip() or "Classic",
        }

    def _save_visual_settings(self) -> None:
        config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
        config["visual_settings"] = self._collect_visual_settings()
        self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
        self._save_state()
        self._log("[Visual] Saved advanced visual settings")

    def _apply_visual_preset_mega_bass(self) -> None:
        self.visual_preset_var.set("mega_bass")
        self.visual_spectrum_var.set("yes")
        self.visual_timeline_var.set("no")
        self.visual_letterbox_var.set("no")
        self.visual_zoom_var.set("random")
        self.visual_style_var.set("random")
        self.visual_color_spectrum_var.set("random")
        self.visual_color_timeline_var.set("random")
        self.visual_film_grain_var.set("yes")
        self.visual_grain_strength_min_var.set("8")
        self.visual_grain_strength_max_var.set("14")
        self.visual_vignette_var.set("yes")
        self.visual_tint_var.set("random")
        self.visual_soft_focus_var.set("no")
        self.visual_soft_focus_sigma_min_var.set("1.2")
        self.visual_soft_focus_sigma_max_var.set("1.8")
        self.visual_particle_var.set("random")
        self.visual_particle_opacity_min_var.set("0.22")
        self.visual_particle_opacity_max_var.set("0.42")
        self.visual_particle_speed_min_var.set("0.95")
        self.visual_particle_speed_max_var.set("1.45")
        self.visual_bass_pulse_var.set("yes")
        self.visual_bass_pulse_scale_min_var.set("0.018")
        self.visual_bass_pulse_scale_max_var.set("0.036")
        self.visual_bass_pulse_brightness_min_var.set("0.02")
        self.visual_bass_pulse_brightness_max_var.set("0.06")
        if not self.visual_text_var.get().strip():
            self.visual_text_var.set("MEGA BASS")
        self.visual_text_font_var.set("random")
        self.visual_text_pos_var.set("random")
        self.visual_text_size_min_var.set("96")
        self.visual_text_size_max_var.set("136")
        self.visual_text_style_var.set("random")
        self._save_visual_settings()
        self._log("[Visual] Applied MEGA BASS preset")

    def _load_state(self) -> dict[str, Any]:
        if STATE_FILE.exists():
            try:
                return json.loads(STATE_FILE.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_state(self) -> None:
        state = {
            "run_metadata": bool(self.run_metadata_var.get()),
            "run_render": bool(self.run_render_var.get()),
            "run_upload": bool(self.run_upload_var.get()),
            "date_mmdd": self.date_var.get(),
            "simulate_seconds": self.simulate_seconds_var.get(),
            "randomize_effects": False,
            "visual_preset": self.visual_preset_var.get(),
            "visual_spectrum": self.visual_spectrum_var.get(),
            "visual_timeline": self.visual_timeline_var.get(),
            "visual_letterbox": self.visual_letterbox_var.get(),
            "visual_zoom": self.visual_zoom_var.get(),
            "visual_style": self.visual_style_var.get(),
            "visual_color_spectrum": self.visual_color_spectrum_var.get(),
            "visual_color_timeline": self.visual_color_timeline_var.get(),
            "visual_spectrum_y": self.visual_spectrum_y_var.get(),
            "visual_spectrum_x": self.visual_spectrum_x_var.get(),
            "visual_spectrum_w": _compose_range_value(
                self.visual_spectrum_w_min_var.get(),
                self.visual_spectrum_w_max_var.get(),
                "1200",
            ),
            "visual_spectrum_w_min": self.visual_spectrum_w_min_var.get(),
            "visual_spectrum_w_max": self.visual_spectrum_w_max_var.get(),
            "visual_film_grain": self.visual_film_grain_var.get(),
            "visual_grain_strength": _compose_range_value(
                self.visual_grain_strength_min_var.get(),
                self.visual_grain_strength_max_var.get(),
                "15",
            ),
            "visual_grain_strength_min": self.visual_grain_strength_min_var.get(),
            "visual_grain_strength_max": self.visual_grain_strength_max_var.get(),
            "visual_vignette": self.visual_vignette_var.get(),
            "visual_tint": self.visual_tint_var.get(),
            "visual_soft_focus": self.visual_soft_focus_var.get(),
            "visual_soft_focus_sigma": _compose_range_value(
                self.visual_soft_focus_sigma_min_var.get(),
                self.visual_soft_focus_sigma_max_var.get(),
                "1.5",
            ),
            "visual_soft_focus_sigma_min": self.visual_soft_focus_sigma_min_var.get(),
            "visual_soft_focus_sigma_max": self.visual_soft_focus_sigma_max_var.get(),
            "visual_particle": self.visual_particle_var.get(),
            "visual_particle_opacity": _compose_range_value(
                self.visual_particle_opacity_min_var.get(),
                self.visual_particle_opacity_max_var.get(),
                "0.6",
            ),
            "visual_particle_opacity_min": self.visual_particle_opacity_min_var.get(),
            "visual_particle_opacity_max": self.visual_particle_opacity_max_var.get(),
            "visual_particle_speed": _compose_range_value(
                self.visual_particle_speed_min_var.get(),
                self.visual_particle_speed_max_var.get(),
                "1.0",
            ),
            "visual_particle_speed_min": self.visual_particle_speed_min_var.get(),
            "visual_particle_speed_max": self.visual_particle_speed_max_var.get(),
            "visual_bass_pulse": self.visual_bass_pulse_var.get(),
            "visual_bass_pulse_scale": _compose_range_value(
                self.visual_bass_pulse_scale_min_var.get(),
                self.visual_bass_pulse_scale_max_var.get(),
                "0.03",
            ),
            "visual_bass_pulse_scale_min": self.visual_bass_pulse_scale_min_var.get(),
            "visual_bass_pulse_scale_max": self.visual_bass_pulse_scale_max_var.get(),
            "visual_bass_pulse_brightness": _compose_range_value(
                self.visual_bass_pulse_brightness_min_var.get(),
                self.visual_bass_pulse_brightness_max_var.get(),
                "0.04",
            ),
            "visual_bass_pulse_brightness_min": self.visual_bass_pulse_brightness_min_var.get(),
            "visual_bass_pulse_brightness_max": self.visual_bass_pulse_brightness_max_var.get(),
            "visual_text": self.visual_text_var.get(),
            "visual_text_font": self.visual_text_font_var.get(),
            "visual_text_pos": self.visual_text_pos_var.get(),
            "visual_text_size": _compose_range_value(
                self.visual_text_size_min_var.get(),
                self.visual_text_size_max_var.get(),
                "60",
            ),
            "visual_text_size_min": self.visual_text_size_min_var.get(),
            "visual_text_size_max": self.visual_text_size_max_var.get(),
            "visual_text_style": self.visual_text_style_var.get(),
            "generate_text": bool(self.generate_text_var.get()),
            "generate_thumbnails": bool(self.generate_thumbnails_var.get()),
            "metadata_mode": "prompt_api",
            "current_group": self.current_group_var.get(),
            "source_dir_override": self.source_dir_override_var.get(),
            "add_ypp": self.add_ypp_var.get(),
            "add_quantity": self.add_quantity_var.get(),
            "add_title": self.add_title_var.get(),
            "add_visibility": self.add_visibility_var.get(),
            "add_category": self.add_category_var.get(),
            "add_kids": self.add_kids_var.get(),
            "add_ai": self.add_ai_var.get(),
            "add_notify_subscribers": bool(self.add_notify_var.get()),
            "add_schedule_enabled": bool(self.add_schedule_enabled_var.get()),
            "add_schedule_date": self.add_schedule_date_var.get(),
            "add_schedule_time": self.add_schedule_time_var.get(),
            "add_schedule_timezone": self.add_schedule_timezone_var.get(),
            "add_schedule": self._compose_add_schedule(),
            "default_visibility": self.default_visibility_var.get(),
            "default_category": self.default_category_var.get(),
            "default_kids": self.default_kids_var.get(),
            "default_ai": self.default_ai_var.get(),
            "default_notify_subscribers": bool(self.default_notify_var.get()),
            "schedule_enabled": bool(self.schedule_enabled_var.get()),
            "schedule_date": self.schedule_date_var.get(),
            "schedule_time": self.schedule_time_var.get(),
            "schedule_timezone": self.schedule_timezone_var.get(),
            "schedule_start": self._compose_default_schedule(),
            "schedule_interval": self.schedule_interval_var.get(),
            "upload_auto_close": bool(self.upload_auto_close_var.get()),
            "upload_delay_mode": self.upload_delay_mode_var.get().strip() or "抖动",
            "upload_file_delay_min_ms": self.upload_file_delay_min_ms_var.get().strip() or "0",
            "upload_file_delay_max_ms": self.upload_file_delay_max_ms_var.get().strip() or "0",
            "upload_next_delay_min_ms": self.upload_next_delay_min_ms_var.get().strip() or "0",
            "upload_next_delay_max_ms": self.upload_next_delay_max_ms_var.get().strip() or "0",
            "upload_done_delay_min_ms": self.upload_done_delay_min_ms_var.get().strip() or "0",
            "upload_done_delay_max_ms": self.upload_done_delay_max_ms_var.get().strip() or "0",
            "upload_publish_delay_min_ms": self.upload_publish_delay_min_ms_var.get().strip() or "0",
            "upload_publish_delay_max_ms": self.upload_publish_delay_max_ms_var.get().strip() or "0",
            "upload_click_delay_min_ms": self.upload_click_delay_min_ms_var.get().strip() or "0",
            "upload_click_delay_max_ms": self.upload_click_delay_max_ms_var.get().strip() or "0",
            "prompt_group": self.prompt_group_var.get(),
        }
        STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    def _drain_log_queue(self) -> None:
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_box.insert("end", message + "\n")
            self.log_box.see("end")
            self._update_run_status_from_log(message)
        while True:
            try:
                action = self.ui_action_queue.get_nowait()
            except queue.Empty:
                break
            action()
        self.after(150, self._drain_log_queue)

    def _log(self, message: str) -> None:
        self.log_queue.put(message)

    def _post_ui_action(self, action: Callable[[], None]) -> None:
        self.ui_action_queue.put(action)

    def _clear_logs(self) -> None:
        self.log_box.delete("1.0", "end")

    def _active_upload_processes(self) -> list[subprocess.Popen[str]]:
        processes: list[subprocess.Popen[str]] = []
        if self.worker_process is not None and self.worker_process.poll() is None:
            processes.append(self.worker_process)
        for proc in self.worker_processes:
            if proc.poll() is None:
                processes.append(proc)
        return processes

    def _set_run_paused(self, paused: bool) -> None:
        self._run_paused = bool(paused)
        self.pause_button_text_var.set("继续" if paused else "暂停")
        self._apply_run_status()

    def _toggle_pause_current_task(self) -> None:
        if not self._has_active_background_work():
            messagebox.showinfo("没有运行中的任务", "当前没有可暂停的任务。")
            return

        target_state = not self._run_paused
        try:
            for proc in self._active_upload_processes():
                if target_state:
                    _suspend_windows_process(proc.pid)
                else:
                    _resume_windows_process(proc.pid)
            if self.execution_control:
                if target_state:
                    self.execution_control.request_pause()
                else:
                    self.execution_control.request_resume()
            self._set_run_paused(target_state)
            self._log("[Control] 已暂停当前批次" if target_state else "[Control] 已继续当前批次")
        except Exception as exc:
            messagebox.showerror("暂停失败", str(exc))

    def _cancel_current_task(self) -> None:
        if not self._has_active_background_work():
            messagebox.showinfo("没有运行中的任务", "当前没有可取消的任务。")
            return
        self._cancel_requested = True
        if self.execution_control:
            self.execution_control.request_cancel()
            self.execution_control.request_resume()
        for proc in self._active_upload_processes():
            try:
                proc.terminate()
            except Exception:
                pass
        self._set_run_paused(False)
        self._log("[Control] 已请求取消当前批次")

    def _has_active_background_work(self) -> bool:
        return bool(
            (self.worker_thread and self.worker_thread.is_alive())
            or (self.upload_monitor_thread and self.upload_monitor_thread.is_alive())
            or self.worker_process is not None
            or bool(self.worker_processes)
            or any(thread.is_alive() for thread in self.upload_monitor_threads)
        )

    def _apply_run_status(self) -> None:
        if not self._run_started_at:
            self.run_status_var.set("空闲")
            self.run_phase_var.set("等待任务")
            self.run_detail_var.set("当前没有在运行的任务")
            self.run_progress_var.set("0/0")
            self.run_elapsed_var.set("00:00")
            self.run_eta_var.set("--")
            if not self.run_last_log_var.get().strip():
                self.run_last_log_var.set("最近日志会显示在这里")
            self.run_progress_bar.set(0.0)
            self._run_paused = False
            self.pause_button_text_var.set("暂停")
            return

        elapsed = time.time() - self._run_started_at
        progress = 0.0
        if self._run_total_steps > 0:
            progress = min(1.0, max(0.0, (self._run_completed_steps + self._run_current_ratio) / self._run_total_steps))
        eta = "--"
        if progress > 0:
            remaining = max(0.0, elapsed * (1.0 - progress) / progress)
            eta = _format_runtime_duration(remaining)

        status_text = "已暂停" if self._run_paused else "运行中"
        self.run_status_var.set(f"{status_text} | {self._run_mode_label}")
        phase_text = self._run_phase or "处理中"
        if self._run_paused and not phase_text.startswith("已暂停"):
            phase_text = f"已暂停 | {phase_text}"
        self.run_phase_var.set(phase_text)
        self.run_detail_var.set(self._run_current_item or "等待首条进度")
        self.run_progress_var.set(f"{self._run_completed_steps}/{self._run_total_steps} | {progress * 100:.0f}%")
        self.run_elapsed_var.set(_format_runtime_duration(elapsed))
        self.run_eta_var.set(eta)
        self.run_progress_bar.set(progress)

    def _start_run_tracking(self, mode_label: str, total_items: int, *, include_upload: bool = False) -> None:
        self._run_started_at = time.time()
        self._run_mode_label = mode_label
        self._run_total_items = max(0, int(total_items))
        self._run_include_upload = bool(include_upload)
        self._run_total_steps = self._run_total_items * (2 if include_upload else 1)
        self._run_completed_steps = 0
        self._run_current_ratio = 0.0
        self._run_current_item = f"{self._run_total_items} 个窗口待处理"
        self._run_phase = "准备中"
        self.execution_control = ExecutionControl()
        self._cancel_requested = False
        self._run_paused = False
        self.pause_button_text_var.set("暂停")
        self._run_render_done.clear()
        self._run_upload_done.clear()
        with self._upload_process_lock:
            self._upload_failures = []
        self._run_result_map = {}
        self._run_plan_for_summary = None
        self._run_execution_result = None
        self._run_report_logged = False
        self.run_last_log_var.set("任务已启动，等待第一条日志")
        self._apply_run_status()

    def _task_result_key(self, tag: str, serial: int, slot_index: int = 1, total_slots: int = 1) -> str:
        clean_tag = str(tag or "").strip()
        base = f"{clean_tag}/{int(serial)}"
        if int(total_slots or 1) <= 1:
            return base
        return f"{base}#{int(slot_index or 1):02d}"

    def _parse_task_label(self, label: str) -> tuple[str, int, int, int] | None:
        text = str(label or "").strip()
        if not text or "/" not in text:
            return None
        tag_name, right = text.rsplit("/", 1)
        slot_match = re.fullmatch(r"(\d+)#(\d+)", right)
        if slot_match:
            return tag_name.strip(), int(slot_match.group(1)), int(slot_match.group(2)), 2
        round_match = re.fullmatch(r"(\d+)\[(\d+)/(\d+)\]", right)
        if round_match:
            return tag_name.strip(), int(round_match.group(1)), int(round_match.group(2)), int(round_match.group(3))
        try:
            return tag_name.strip(), int(right), 1, 1
        except ValueError:
            return None

    def _lookup_task_tag(self, serial: int) -> str:
        run_plan = self._run_plan_for_summary
        for task in getattr(run_plan, "tasks", []) or []:
            if int(getattr(task, "serial", -1)) == int(serial):
                return str(getattr(task, "tag", "") or "").strip()
        return ""

    def _prepare_run_result_tracking(self, run_plan: Any) -> None:
        self._run_plan_for_summary = run_plan
        self._run_execution_result = None
        result_map: dict[str, dict[str, dict[str, str]]] = {}
        modules = getattr(run_plan, "modules", None)
        for task in getattr(run_plan, "tasks", []) or []:
            key = self._task_result_key(
                task.tag,
                task.serial,
                getattr(task, "slot_index", 1),
                getattr(task, "total_slots", 1),
            )
            stages: dict[str, dict[str, str]] = {}
            if getattr(modules, "render", False):
                stages["render"] = {"status": "pending", "detail": ""}
            if getattr(modules, "metadata", False):
                stages["metadata"] = {"status": "pending", "detail": ""}
            if getattr(modules, "upload", False):
                stages["upload"] = {"status": "pending", "detail": ""}
            result_map[key] = stages
        self._run_result_map = result_map

    def _mark_run_stage(
        self,
        tag: str,
        serial: int,
        stage: str,
        status: str,
        detail: str = "",
        *,
        slot_index: int = 1,
        total_slots: int = 1,
    ) -> None:
        key = self._task_result_key(tag, serial, slot_index, total_slots)
        entry = self._run_result_map.setdefault(key, {})
        stage_entry = entry.setdefault(stage, {"status": "pending", "detail": ""})
        stage_entry["status"] = status
        stage_entry["detail"] = str(detail or "").strip()

    def _ingest_execution_result(self, execution: Any) -> None:
        self._run_execution_result = execution
        run_plan = getattr(execution, "run_plan", None) if execution else None
        workflow_result = getattr(execution, "workflow_result", None) if execution else None
        if not run_plan:
            return
        modules = getattr(run_plan, "modules", None)
        warning_map: dict[str, str] = {}
        for warning in getattr(workflow_result, "warnings", []) or []:
            match = re.search(r"([^/\s]+)/([0-9]+)\s+文案/封面阶段失败[^:：]*[:：]\s*(.+)", str(warning))
            if match:
                warning_map[self._task_result_key(match.group(1), int(match.group(2)))] = match.group(3).strip()
        item_map = {self._task_result_key(item.tag, item.serial): item for item in (getattr(workflow_result, "items", []) or [])}
        for task in getattr(run_plan, "tasks", []) or []:
            key = self._task_result_key(task.tag, task.serial)
            item = item_map.get(key)
            if getattr(modules, "render", False):
                if item and str(getattr(item, "output_video", "")).strip():
                    self._mark_run_stage(task.tag, task.serial, "render", "success")
                else:
                    self._mark_run_stage(task.tag, task.serial, "render", "failed", "未生成成品视频")
            if getattr(modules, "metadata", False):
                metadata_error = warning_map.get(key)
                if metadata_error:
                    self._mark_run_stage(task.tag, task.serial, "metadata", "failed", metadata_error)
                    if getattr(modules, "upload", False):
                        self._mark_run_stage(task.tag, task.serial, "upload", "skipped", "文案/封面失败后已跳过上传")
                elif item:
                    self._mark_run_stage(task.tag, task.serial, "metadata", "success")
            if getattr(modules, "upload", False) and key not in warning_map and key in item_map:
                current = self._run_result_map.get(key, {}).get("upload", {})
                if current.get("status") == "pending":
                    self._mark_run_stage(task.tag, task.serial, "upload", "running", "等待上传结果")

    def _load_upload_record_result(self, date_mmdd: str, tag: str, serial: int) -> dict[str, Any] | None:
        record_path = SCRIPT_DIR / "upload_records" / str(date_mmdd) / str(tag) / f"channel_{int(serial)}.json"
        if not record_path.exists():
            return None
        try:
            return json.loads(record_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _sync_upload_results_from_records(self) -> None:
        run_plan = self._run_plan_for_summary
        if not run_plan or not getattr(getattr(run_plan, "modules", None), "upload", False):
            return
        date_mmdd = str(getattr(getattr(run_plan, "defaults", None), "date_mmdd", "")).strip()
        for task in getattr(run_plan, "tasks", []) or []:
            record = self._load_upload_record_result(date_mmdd, task.tag, task.serial)
            if not record:
                continue
            if bool(record.get("success")):
                self._mark_run_stage(task.tag, task.serial, "upload", "success", str(record.get("stage") or "上传成功"))
            else:
                detail = str(record.get("failure_reason") or record.get("stage") or "上传失败")
                self._mark_run_stage(task.tag, task.serial, "upload", "failed", detail)

    def _compose_run_completion_report(self, success: bool, summary: str, cancelled: bool = False) -> tuple[str, str]:
        self._sync_upload_results_from_records()
        if not self._run_result_map:
            return summary, summary

        success_lines: list[str] = []
        failed_lines: list[str] = []
        skipped_lines: list[str] = []
        pending_lines: list[str] = []
        stage_labels = {"render": "Render", "metadata": "Metadata/Cover", "upload": "Upload"}

        for key, stages in self._run_result_map.items():
            parts: list[str] = []
            has_failed = False
            has_skipped = False
            has_pending = False

            for stage_name in ("render", "metadata", "upload"):
                stage = stages.get(stage_name)
                if not stage:
                    continue
                status = str(stage.get("status", "pending") or "pending").strip()
                detail = str(stage.get("detail", "") or "").strip()
                label = stage_labels.get(stage_name, stage_name)

                if status == "success":
                    parts.append(f"{label}: success")
                elif status == "failed":
                    has_failed = True
                    parts.append(f"{label}: failed ({detail or 'unknown reason'})")
                elif status == "skipped":
                    has_skipped = True
                    parts.append(f"{label}: skipped ({detail or 'skipped'})")
                else:
                    has_pending = True
                    parts.append(f"{label}: pending ({detail or 'waiting'})")

            line = f"- {key}: " + " | ".join(parts or ["no result"])
            if has_failed:
                failed_lines.append(line)
            elif has_skipped:
                skipped_lines.append(line)
            elif has_pending:
                pending_lines.append(line)
            else:
                success_lines.append(line)

        headline = "Batch cancelled" if cancelled else f"Success {len(success_lines)}, Failed {len(failed_lines)}, Skipped {len(skipped_lines)}"
        lines = ["[Result] Batch execution summary", headline]
        if success_lines:
            lines.append("Success:")
            lines.extend(success_lines)
        if failed_lines:
            lines.append("Failed:")
            lines.extend(failed_lines)
        if skipped_lines:
            lines.append("Skipped:")
            lines.extend(skipped_lines)
        if pending_lines:
            lines.append("Pending:")
            lines.extend(pending_lines)
        if summary and summary not in headline:
            lines.append(f"Note: {summary}")
        report = chr(10).join(lines)
        return headline or summary, report

    def _finish_run_tracking(self, *, success: bool, summary: str, cancelled: bool = False) -> None:
        elapsed = time.time() - self._run_started_at if self._run_started_at else 0.0
        summary_text, full_report = self._compose_run_completion_report(success, summary, cancelled)
        self.run_status_var.set("Cancelled" if cancelled else ("Done" if success else "Failed"))
        self.run_phase_var.set("Finished")
        self.run_detail_var.set(summary_text)
        self.run_progress_var.set(f"{self._run_completed_steps}/{self._run_total_steps}")
        self.run_elapsed_var.set(_format_runtime_duration(elapsed))
        self.run_eta_var.set("00:00" if success or cancelled else "--")
        self.run_progress_bar.set(1.0 if success or cancelled else self.run_progress_bar.get())
        if full_report and not self._run_report_logged:
            self._run_report_logged = True
            self._log(full_report)
        self._run_started_at = None
        self._run_mode_label = ""
        self._run_total_items = 0
        self._run_total_steps = 0
        self._run_completed_steps = 0
        self._run_current_ratio = 0.0
        self._run_current_item = ""
        self._run_phase = "Idle"
        self._run_include_upload = False
        self.execution_control = None
        self._cancel_requested = False
        self._run_paused = False
        self.pause_button_text_var.set("Pause")
        self._run_render_done.clear()
        self._run_upload_done.clear()
        self._run_plan_for_summary = None
        self._run_execution_result = None
        self._run_result_map = {}
        self._run_report_logged = False

    def _tick_run_status(self) -> None:
        if self._run_started_at:
            self._apply_run_status()
        self.after(1000, self._tick_run_status)

    def _run_progress_step_done(self, step_key: str, step_type: str) -> None:
        target_set = self._run_render_done if step_type == "render" else self._run_upload_done
        marker: str | int = step_key if step_type == "render" else int(step_key)
        if marker in target_set:
            return
        target_set.add(marker)
        self._run_completed_steps = min(self._run_total_steps, self._run_completed_steps + 1)
        self._run_current_ratio = 0.0

    def _update_run_status_from_log(self, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        self.run_last_log_var.set(text[:120])
        if not self._run_started_at:
            return
        if text.startswith("[开始]"):
            self._run_phase = text[4:].strip() or "开始"
        elif text.startswith("[Control] Paused"):
            self._run_phase = "已暂停"
        elif text.startswith("[Control] Resumed"):
            self._run_phase = "继续执行"
        elif text.startswith("[Control] 已请求取消") or text.startswith("[取消]"):
            self._run_phase = "取消中"
        elif text.startswith("[计划]"):
            self._run_phase = "生成计划"
        elif text.startswith("[任务]"):
            match = re.search(r"\[任务\]\s+([^/]+)/([0-9]+):", text)
            if match:
                self._run_phase = "渲染"
                self._run_current_item = f"{match.group(1)} / 窗口 {match.group(2)}"
                self._run_current_ratio = 0.0
        elif text.startswith("[渲染]"):
            self._run_phase = "渲染"
            progress_match = re.search(r"进度\s+(\d+)%", text)
            if progress_match:
                self._run_current_ratio = max(0.0, min(1.0, int(progress_match.group(1)) / 100.0))
            done_match = re.search(r"\[渲染\]\s+完成\s+(.+?)\s+\|", text)
            if done_match:
                self._run_progress_step_done(done_match.group(1), "render")
        elif text.startswith("[清单]"):
            self._run_phase = "写入清单"
        elif text.startswith("[模拟]"):
            self._run_phase = "模拟完成"
        elif text.startswith("[上传]"):
            self._run_phase = "上传"

        upload_match = re.match(r"\[Upload ([^\]]+)\]\s+(.*)$", text)
        if upload_match:
            label = upload_match.group(1)
            upload_text = upload_match.group(2).strip()
            if "/" in label:
                tag_name, serial_text = label.rsplit("/", 1)
                try:
                    serial_value = int(serial_text)
                except ValueError:
                    serial_value = None
                if serial_value is not None:
                    fail_match = re.search(r"上传失败\(stage=([^)]+)\)", upload_text)
                    if fail_match:
                        self._mark_run_stage(tag_name, serial_value, "upload", "failed", fail_match.group(1))
                    elif "发布成功" in upload_text or "上传成功" in upload_text:
                        self._mark_run_stage(tag_name, serial_value, "upload", "success", "上传成功")

        if "上传状态检查" in text:
            self._run_phase = "上传检查"
            status_match = re.search(r"\[(\d+)\]", text)
            if status_match:
                self._run_current_item = f"窗口 {status_match.group(1)}"
        if "开始执行标签组" in text:
            self._run_phase = "批量上传"
        serial_match = re.search(r"序号\s+(\d+)", text)
        if serial_match:
            serial_value = int(serial_match.group(1))
            self._run_current_item = f"窗口 {serial_value}"
            fail_match = re.search(r"上传失败\(stage=([^)]+)\)", text)
            if fail_match:
                tag_name = self._lookup_task_tag(serial_value)
                if tag_name:
                    self._mark_run_stage(tag_name, serial_value, "upload", "failed", fail_match.group(1))
        if "发布成功" in text and serial_match:
            serial_value = int(serial_match.group(1))
            tag_name = self._lookup_task_tag(serial_value)
            if tag_name:
                self._mark_run_stage(tag_name, serial_value, "upload", "success", "发布成功")
            self._run_phase = "上传完成"
            self._run_progress_step_done(serial_match.group(1), "upload")
        self._apply_run_status()

    def _current_run_summary(self) -> str:
        parts = [
            f"状态: {self.run_status_var.get()}",
            f"阶段: {self.run_phase_var.get()}",
            f"当前任务: {self.run_detail_var.get()}",
            f"进度: {self.run_progress_var.get()}",
            f"已运行: {self.run_elapsed_var.get()}",
            f"预计剩余: {self.run_eta_var.get()}",
            f"最近日志: {self.run_last_log_var.get()}",
        ]
        if self._has_active_background_work():
            parts.append("说明: 你可以继续切换其他页面查看或修改配置；新的开始任务会等当前流程结束。")
        return "\n".join(parts)

    def _refresh_groups(self) -> None:

        self.group_catalog = get_group_catalog()
        groups = list(self.group_catalog.keys()) or [""]
        for menu in (
            self.current_group_menu,
            self.binding_group_menu,
            self.prompt_group_menu,
        ):
            menu.configure(values=groups)
        if self.current_group_var.get() not in groups:
            self.current_group_var.set(groups[0])
        if self.binding_group_var.get() not in groups:
            self.binding_group_var.set(self.current_group_var.get())
        if self.prompt_group_var.get() not in groups:
            self.prompt_group_var.set(self.current_group_var.get())
        self.binding_folder_var.set(get_group_bindings(self.scheduler_config).get(self.binding_group_var.get(), ""))
        self._refresh_window_buttons()
        self._refresh_bindings_box()

    def _refresh_window_buttons(self) -> None:
        for child in self.window_button_frame.winfo_children():
            child.destroy()
        current_group = self.current_group_var.get()
        windows = self.group_catalog.get(current_group, [])
        if not windows:
            ctk.CTkLabel(self.window_button_frame, text="当前分组没有窗口").pack(padx=12, pady=12)
            return
        ctk.CTkLabel(self.window_button_frame, text="点哪个窗口，就把哪个窗口加入任务区").pack(anchor="w", padx=12, pady=(10, 6))
        grid = ctk.CTkFrame(self.window_button_frame, fg_color="transparent")
        grid.pack(fill="x", padx=8, pady=(0, 10))
        for column in range(WINDOW_BUTTONS_PER_ROW):
            grid.grid_columnconfigure(column, weight=1)
        for index, info in enumerate(windows):
            label = f"{info.serial} {info.channel_name}".strip()
            row_index = index // WINDOW_BUTTONS_PER_ROW
            column_index = index % WINDOW_BUTTONS_PER_ROW
            ctk.CTkButton(
                grid,
                text=label,
                command=lambda current=info: self._add_window_task(current),
            ).grid(row=row_index, column=column_index, sticky="ew", padx=6, pady=6)

    def _refresh_task_tree(self) -> None:
        for item in self.task_tree.get_children():
            self.task_tree.delete(item)
        bindings = get_group_bindings(self.scheduler_config)
        for index, task in enumerate(self.window_tasks):
            source_text = task.source_dir.strip() or bindings.get(task.tag, "")
            self.task_tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    task.tag,
                    task.serial,
                    max(1, int(getattr(task, "quantity", 1) or 1)),
                    _yes_no_from_bool(task.is_ypp),
                    task.visibility,
                    task.category,
                    _yes_no_from_bool(task.made_for_kids),
                    _yes_no_from_bool(task.altered_content),
                    task.scheduled_publish_at,
                    source_text,
                ),
            )
        self._save_state()

    def _refresh_bindings_box(self) -> None:
        self.binding_box.delete("1.0", "end")
        self.binding_box.insert("end", describe_group_bindings(self.scheduler_config))

    def _refresh_prompt_dropdowns(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        api_names = list((self.prompt_config.get("apiPresets") or {}).keys()) or [""]
        content_names = list((self.prompt_config.get("contentTemplates") or {}).keys()) or [""]
        self.api_preset_menu.configure(values=api_names)
        self.content_template_menu.configure(values=content_names)
        if self.api_preset_var.get() not in api_names:
            self.api_preset_var.set(api_names[0])
        if self.content_template_var.get() not in content_names:
            self.content_template_var.set(content_names[0])

    def _pick_source_override(self) -> None:
        selected = filedialog.askdirectory(title="选择新增窗口使用的素材目录")
        if selected:
            self.source_dir_override_var.set(selected)

    def _fill_binding_source(self) -> None:
        bindings = get_group_bindings(self.scheduler_config)
        self.source_dir_override_var.set(bindings.get(self.current_group_var.get(), ""))

    def _sync_schedule_mode_state(self) -> None:
        if self.schedule_enabled_var.get():
            self.default_visibility_var.set("schedule")
        elif self.default_visibility_var.get() == "schedule":
            self.default_visibility_var.set("public")

        if self.add_schedule_enabled_var.get():
            self.add_visibility_var.set("schedule")
        elif self.add_visibility_var.get() == "schedule":
            self.add_visibility_var.set("public")

    def _compose_add_schedule(self) -> str:
        if not bool(self.add_schedule_enabled_var.get()):
            return ""
        return _compose_schedule_text(self.add_schedule_date_var.get(), self.add_schedule_time_var.get())

    def _compose_default_schedule(self) -> str:
        if not bool(self.schedule_enabled_var.get()):
            return ""
        return _compose_schedule_text(self.schedule_date_var.get(), self.schedule_time_var.get())

    def _refresh_schedule_controls(self) -> None:
        add_state = "normal" if bool(self.add_schedule_enabled_var.get()) else "disabled"
        for widget in (
            getattr(self, "add_schedule_date_menu", None),
            getattr(self, "add_schedule_time_menu", None),
            getattr(self, "add_schedule_timezone_menu", None),
        ):
            if widget is not None:
                widget.configure(state=add_state)

        default_state = "normal" if bool(self.schedule_enabled_var.get()) else "disabled"
        for widget in (
            getattr(self, "schedule_date_menu", None),
            getattr(self, "schedule_time_menu", None),
            getattr(self, "schedule_timezone_menu", None),
        ):
            if widget is not None:
                widget.configure(state=default_state)

    def _on_add_visibility_change(self, *_args: object) -> None:
        is_schedule = self.add_visibility_var.get() == "schedule"
        if is_schedule and not self.add_schedule_enabled_var.get():
            self.add_schedule_enabled_var.set(True)
            return
        if not is_schedule and self.add_schedule_enabled_var.get():
            self.add_schedule_enabled_var.set(False)
            return
        self._refresh_schedule_controls()

    def _on_default_visibility_change(self, *_args: object) -> None:
        is_schedule = self.default_visibility_var.get() == "schedule"
        if is_schedule and not self.schedule_enabled_var.get():
            self.schedule_enabled_var.set(True)
            return
        if not is_schedule and self.schedule_enabled_var.get():
            self.schedule_enabled_var.set(False)
            return
        self._refresh_schedule_controls()

    def _on_add_schedule_toggle(self, *_args: object) -> None:
        if self.add_schedule_enabled_var.get() and self.add_visibility_var.get() != "schedule":
            self.add_visibility_var.set("schedule")
            return
        if not self.add_schedule_enabled_var.get() and self.add_visibility_var.get() == "schedule":
            self.add_visibility_var.set("public")
            return
        self._refresh_schedule_controls()

    def _on_default_schedule_toggle(self, *_args: object) -> None:
        if self.schedule_enabled_var.get() and self.default_visibility_var.get() != "schedule":
            self.default_visibility_var.set("schedule")
            return
        if not self.schedule_enabled_var.get() and self.default_visibility_var.get() == "schedule":
            self.default_visibility_var.set("public")
            return
        self._refresh_schedule_controls()

    def _add_window_task(self, info) -> None:
        quantity_text = str(self.add_quantity_var.get() or "1").strip()
        try:
            quantity = max(1, int(quantity_text or "1"))
        except ValueError:
            quantity = 1
            self.add_quantity_var.set("1")
        task = create_task(
            tag=info.tag,
            serial=info.serial,
            quantity=quantity,
            is_ypp=_bool_from_yes_no(self.add_ypp_var.get()) or bool(info.is_ypp),
            title="",
            visibility="schedule" if self.add_schedule_enabled_var.get() else self.add_visibility_var.get(),
            category=self.add_category_var.get(),
            made_for_kids=_bool_from_yes_no(self.add_kids_var.get()),
            altered_content=_bool_from_yes_no(self.add_ai_var.get()),
            notify_subscribers=bool(self.add_notify_var.get()),
            scheduled_publish_at=self._compose_add_schedule(),
            schedule_timezone=self.add_schedule_timezone_var.get() if self.add_schedule_enabled_var.get() else "",
            source_dir=self.source_dir_override_var.get(),
            channel_name=info.channel_name,
        )
        for index, existing in enumerate(self.window_tasks):
            if existing.tag == task.tag and existing.serial == task.serial:
                self.window_tasks[index] = task
                break
        else:
            self.window_tasks.append(task)
        self._refresh_task_tree()
        self._preview_plan()

    def _remove_selected_tasks(self) -> None:
        selected = sorted((int(item) for item in self.task_tree.selection()), reverse=True)
        for index in selected:
            if 0 <= index < len(self.window_tasks):
                self.window_tasks.pop(index)
        self._refresh_task_tree()
        self._preview_plan()

    def _clear_tasks(self) -> None:
        self.window_tasks.clear()
        self._refresh_task_tree()
        self.start_preview.delete("1.0", "end")

    def _pick_binding_folder(self) -> None:
        selected = filedialog.askdirectory(title="选择分组长期绑定目录")
        if selected:
            self.binding_folder_var.set(selected)

    def _legacy__save_paths_shadow(self) -> None:
        config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
        config.update(
            {
                "music_dir": self.music_dir_var.get().strip(),
                "base_image_dir": self.base_image_dir_var.get().strip(),
                "output_root": self.output_root_var.get().strip(),
                "metadata_root": self.metadata_root_var.get().strip(),
                "ffmpeg_bin": self.ffmpeg_var.get().strip() or "ffmpeg",
                "ffmpeg_path": self.ffmpeg_var.get().strip() or "ffmpeg",
                "used_media_root": self.used_media_root_var.get().strip(),
                "render_cleanup_days": int(self.cleanup_days_var.get().strip() or "5"),
            }
        )
        self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
        self._refresh_bindings_box()
        self._log("[路径] 路径配置已保存")

    def _save_binding(self) -> None:
        self.scheduler_config = set_group_binding(self.binding_group_var.get(), self.binding_folder_var.get())
        self._refresh_bindings_box()
        self._log(f"[路径] 已绑定 {self.binding_group_var.get()} -> {self.binding_folder_var.get()}")

    def _remove_binding(self) -> None:
        self.scheduler_config = set_group_binding(self.binding_group_var.get(), "")
        self.binding_folder_var.set("")
        self._refresh_bindings_box()
        self._log(f"[路径] 已移除 {self.binding_group_var.get()} 的绑定")

    def _current_api_form(self) -> dict[str, Any]:
        return {
            "provider": self.provider_var.get(),
            "baseUrl": self.base_url_var.get().strip(),
            "model": self.model_var.get().strip(),
            "apiKey": self.api_key_var.get().strip(),
            "temperature": self.temperature_var.get().strip(),
            "maxTokens": self.max_tokens_var.get().strip(),
            "autoImageEnabled": self.auto_image_var.get().strip(),
            "imageBaseUrl": self.image_base_url_var.get().strip(),
            "imageApiKey": self.image_api_key_var.get().strip(),
            "imageModel": self.image_model_var.get().strip(),
            "imageConcurrency": self.image_concurrency_var.get().strip(),
            "outputLanguage": self.content_language_var.get().strip(),
        }

    def _current_content_form(self) -> dict[str, Any]:
        return {
            "musicGenre": self.music_genre_var.get().strip(),
            "angle": self.angle_var.get().strip(),
            "audience": self.audience_var.get().strip(),
            "outputLanguage": self.content_language_var.get().strip(),
            "titleCount": self.title_count_var.get().strip(),
            "descCount": self.desc_count_var.get().strip(),
            "thumbCount": self.thumb_count_var.get().strip(),
            "titleMin": self.title_min_var.get().strip(),
            "titleMax": self.title_max_var.get().strip(),
            "descLen": self.desc_len_var.get().strip(),
            "tagRange": self.tag_range_var.get().strip(),
            "masterPrompt": self.master_prompt_box.get("1.0", "end").strip(),
            "titleLibrary": self.title_library_box.get("1.0", "end").strip(),
        }

    def _legacy__persist_prompt_form_for_active_tasks_shadow(self) -> None:
        task_tags = sorted({task.tag.strip() for task in self.window_tasks if task.tag.strip()})
        if not task_tags:
            return

        if len(task_tags) == 1:
            target_tag = task_tags[0]
        else:
            target_tag = self.prompt_group_var.get().strip()
            if not target_tag or target_tag not in task_tags:
                self._log("[提示词] 本次包含多个分组，当前表单不会自动覆盖全部分组；将使用各分组已保存绑定")
                return

        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        explicit_save = bool(self.api_save_name_var.get().strip() or self.content_save_name_var.get().strip())
        prompt_group = self.prompt_group_var.get().strip()
        if not explicit_save and (not self.prompt_form_dirty or prompt_group != target_tag):
            self._log(f"[提示词] 运行前直接使用已保存绑定 -> {target_tag}")
            return

        existing_api_name = pick_api_preset_name(self.prompt_config, target_tag)
        existing_content_name = pick_content_template_name(self.prompt_config, target_tag)
        api_name = (
            self.api_save_name_var.get().strip()
            or self.api_preset_var.get().strip()
            or existing_api_name
            or "默认API模板"
        )
        content_name = (
            self.content_save_name_var.get().strip()
            or self.content_template_var.get().strip()
            or existing_content_name
            or "默认内容模板"
        )
        ensure_prompt_presets(
            api_name=api_name,
            api_payload=self._current_api_form(),
            content_name=content_name,
            content_payload=self._current_content_form(),
            tag=target_tag,
            path=PROMPT_STUDIO_FILE,
        )
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._refresh_prompt_dropdowns()
        self.prompt_group_var.set(target_tag)
        self.api_preset_var.set(api_name)
        self.content_template_var.set(content_name)
        self.prompt_form_dirty = False
        self._log(f"[提示词] 运行前已同步当前表单 -> {target_tag} | API={api_name} | 内容模板={content_name}")

    def _persist_prompt_form_for_active_tasks(self) -> None:
        task_tags = sorted({task.tag.strip() for task in self.window_tasks if task.tag.strip()})
        if not task_tags:
            return

        if len(task_tags) == 1:
            target_tag = task_tags[0]
        else:
            target_tag = self.prompt_group_var.get().strip()
            if not target_tag or target_tag not in task_tags:
                self._log("[提示词] 本次包含多个分组，当前表单不会自动覆盖全部分组；将继续使用各分组已保存绑定。")
                return

        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        explicit_save = bool(self.api_save_name_var.get().strip() or self.content_save_name_var.get().strip())
        prompt_group = self.prompt_group_var.get().strip()
        explicit_api_name = find_explicit_api_preset_name(self.prompt_config, target_tag)
        explicit_content_name = find_explicit_content_template_name(self.prompt_config, target_tag)
        existing_api_name = explicit_api_name or pick_api_preset_name(self.prompt_config, target_tag)
        existing_content_name = explicit_content_name or pick_content_template_name(self.prompt_config, target_tag)
        should_force_bind_current_form = (
            len(task_tags) == 1
            and (
                prompt_group == target_tag
                or not explicit_api_name
                or not explicit_content_name
            )
        )
        if not should_force_bind_current_form and not explicit_save and (not self.prompt_form_dirty or prompt_group != target_tag):
            self._log(
                f"[提示词] 运行前直接使用已保存绑定 -> {target_tag} | "
                f"API={existing_api_name} | 内容模板={existing_content_name}"
            )
            return

        api_name = (
            self.api_save_name_var.get().strip()
            or self.api_preset_var.get().strip()
            or existing_api_name
            or "默认API模板"
        )
        content_name = (
            self.content_save_name_var.get().strip()
            or self.content_template_var.get().strip()
            or existing_content_name
            or "默认内容模板"
        )
        ensure_prompt_presets(
            api_name=api_name,
            api_payload=self._current_api_form(),
            content_name=content_name,
            content_payload=self._current_content_form(),
            tag=target_tag,
            path=PROMPT_STUDIO_FILE,
        )
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._refresh_prompt_dropdowns()
        self.prompt_group_var.set(target_tag)
        self.api_preset_var.set(api_name)
        self.content_template_var.set(content_name)
        self.prompt_form_dirty = False
        self._log(
            f"[提示词] 运行前已同步当前表单 -> {target_tag} | "
            f"API={api_name} | 内容模板={content_name}"
        )

    def _load_prompt_for_group(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        tag = self.prompt_group_var.get()
        api_name = pick_api_preset_name(self.prompt_config, tag)
        content_name = pick_content_template_name(self.prompt_config, tag)
        self._loading_prompt_form = True
        self.api_preset_var.set(api_name)
        self.content_template_var.set(content_name)

        api_data = dict((self.prompt_config.get("apiPresets") or {}).get(api_name) or {})
        content_data = dict((self.prompt_config.get("contentTemplates") or {}).get(content_name) or {})

        self.provider_var.set(str(api_data.get("provider") or "openai_compatible"))
        self.base_url_var.set(str(api_data.get("baseUrl") or ""))
        self.model_var.set(str(api_data.get("model") or ""))
        self.api_key_var.set(str(api_data.get("apiKey") or ""))
        self.temperature_var.set(str(api_data.get("temperature") or "0.9"))
        self.max_tokens_var.set(str(api_data.get("maxTokens") or "16000"))
        self.auto_image_var.set(str(api_data.get("autoImageEnabled") or "0"))
        self.image_base_url_var.set(str(api_data.get("imageBaseUrl") or ""))
        self.image_api_key_var.set(str(api_data.get("imageApiKey") or ""))
        self.image_model_var.set(str(api_data.get("imageModel") or ""))
        self.image_concurrency_var.set(str(api_data.get("imageConcurrency") or "3"))

        self.music_genre_var.set(str(content_data.get("musicGenre") or ""))
        self.angle_var.set(str(content_data.get("angle") or ""))
        self.audience_var.set(str(content_data.get("audience") or ""))
        self.content_language_var.set(str(content_data.get("outputLanguage") or "zh-TW"))
        self.title_count_var.set(str(content_data.get("titleCount") or "3"))
        self.desc_count_var.set(str(content_data.get("descCount") or "1"))
        self.thumb_count_var.set(str(content_data.get("thumbCount") or "3"))
        self.title_min_var.set(str(content_data.get("titleMin") or "80"))
        self.title_max_var.set(str(content_data.get("titleMax") or "95"))
        self.desc_len_var.set(str(content_data.get("descLen") or "300"))
        self.tag_range_var.set(str(content_data.get("tagRange") or "10-20"))
        self.master_prompt_box.delete("1.0", "end")
        self.master_prompt_box.insert("1.0", str(content_data.get("masterPrompt") or ""))
        self.title_library_box.delete("1.0", "end")
        self.title_library_box.insert("1.0", str(content_data.get("titleLibrary") or ""))
        self._loading_prompt_form = False
        self.prompt_form_dirty = False

    def _save_api_preset(self) -> None:
        tag = self.prompt_group_var.get()
        name = self.api_save_name_var.get().strip() or self.api_preset_var.get().strip()
        if not name:
            messagebox.showerror("保存失败", "请填写 API 模板名称")
            return
        ensure_prompt_presets(
            api_name=name,
            api_payload=self._current_api_form(),
            content_name=self.content_template_var.get().strip() or "默认内容模板",
            content_payload=self._current_content_form(),
            path=PROMPT_STUDIO_FILE,
        )
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._refresh_prompt_dropdowns()
        self.api_preset_var.set(name)
        self._log(f"[提示词] 已保存 API 模板: {name}")
        if tag:
            self.api_save_name_var.set(name)

    def _save_content_template(self) -> None:
        name = self.content_save_name_var.get().strip() or self.content_template_var.get().strip()
        if not name:
            messagebox.showerror("保存失败", "请填写内容模板名称")
            return
        ensure_prompt_presets(
            api_name=self.api_preset_var.get().strip() or "默认API模板",
            api_payload=self._current_api_form(),
            content_name=name,
            content_payload=self._current_content_form(),
            path=PROMPT_STUDIO_FILE,
        )
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._refresh_prompt_dropdowns()
        self.content_template_var.set(name)
        self._log(f"[提示词] 已保存内容模板: {name}")

    def _bind_group_api(self) -> None:
        tag = self.prompt_group_var.get().strip()
        name = self.api_preset_var.get().strip() or "默认API模板"
        config = load_prompt_studio_config(PROMPT_STUDIO_FILE)
        config.setdefault("tagApiBindings", {})[tag] = name
        save_prompt_studio_config(PROMPT_STUDIO_FILE, config)
        self.prompt_config = config
        self._log(f"[提示词] {self.prompt_group_var.get()} 已绑定 API 模板 {self.api_preset_var.get()}")

    def _bind_group_content(self) -> None:
        tag = self.prompt_group_var.get().strip()
        name = self.content_template_var.get().strip() or "默认内容模板"
        config = load_prompt_studio_config(PROMPT_STUDIO_FILE)
        config.setdefault("tagBindings", {})[tag] = name
        save_prompt_studio_config(PROMPT_STUDIO_FILE, config)
        self.prompt_config = config
        self._log(f"[提示词] {self.prompt_group_var.get()} 已绑定内容模板 {self.content_template_var.get()}")

    def _test_text_api(self) -> None:
        try:
            raw = call_text_model(self._current_api_form(), "只回复 API_TEXT_OK")
            messagebox.showinfo("测试成功", f"文本 API 测试成功:\n{raw[:200]}")
            self._log("[提示词] 文本 API 测试成功")
        except Exception as exc:
            messagebox.showerror("测试失败", str(exc))
            self._log(f"[提示词] 文本 API 测试失败: {exc}")

    def _test_image_api(self) -> None:
        try:
            raw = call_image_model(self._current_api_form(), "A calm blue jazz poster, Use Traditional Chinese text in the image.")
            if not raw.get("data_url"):
                raise ValueError("图片接口没有返回 data_url")
            messagebox.showinfo("测试成功", "图片 API 已返回可用图片数据。")
            self._log("[提示词] 图片 API 测试成功")
        except Exception as exc:
            messagebox.showerror("测试失败", str(exc))
            self._log(f"[提示词] 图片 API 测试失败: {exc}")

    def _analyze_audience(self, data_url: str) -> None:
        try:
            result = analyze_audience_screenshot(self._current_api_form(), data_url)
            summary = str(result.get("summary") or "")
            self.audience_var.set(summary or self.audience_var.get())
            self.audience_result_box.delete("1.0", "end")
            self.audience_result_box.insert("1.0", json.dumps(result, ensure_ascii=False, indent=2))
            self._log("[提示词] 受众截图识别成功")
        except Exception as exc:
            messagebox.showerror("识别失败", str(exc))
            self._log(f"[提示词] 受众截图识别失败: {exc}")

    def _choose_audience_image(self) -> None:
        selected = filedialog.askopenfilename(
            title="选择受众截图",
            filetypes=[("Image", "*.png;*.jpg;*.jpeg;*.webp"), ("All", "*.*")],
        )
        if not selected:
            return
        image = Image.open(selected)
        self._audience_data_url = _to_data_url(image)
        self._analyze_audience(self._audience_data_url)

    def _paste_audience_image(self) -> None:
        content = ImageGrab.grabclipboard()
        if isinstance(content, Image.Image):
            self._audience_data_url = _to_data_url(content)
            self._analyze_audience(self._audience_data_url)
        else:
            messagebox.showerror("粘贴失败", "剪贴板里没有图片")

    def _reanalyze_audience_image(self) -> None:
        if not self._audience_data_url:
            messagebox.showerror("无法识别", "还没有选中或粘贴任何截图")
            return
        self._analyze_audience(self._audience_data_url)

    def _clear_audience_image(self) -> None:
        self._audience_data_url = ""
        self.audience_result_box.delete("1.0", "end")




    def _test_text_api(self) -> None:
        try:
            raw = call_text_model(
                self._current_api_form(),
                '只返回严格 JSON，不要 markdown。{"titles":["1个繁体中文标题，长度20到40字"]}',
            )
            parsed = _parse_json_like(str(raw or "").strip())
            titles = parsed.get("titles") if isinstance(parsed, dict) else None
            if not isinstance(titles, list) or not any(str(item).strip() for item in titles):
                raise ValueError(f"文本接口返回了内容，但不是可用的大模型 JSON 结果: {str(raw)[:200]}")
            messagebox.showinfo("测试成功", f"文本 API 可生成标题:\n{str(titles[0])[:120]}")
            self._log("[提示词] 文本 API 测试成功")
        except Exception as exc:
            messagebox.showerror("测试失败", str(exc))
            self._log(f"[提示词] 文本 API 测试失败: {exc}")

    def _open_current_output(self) -> None:
        if self.window_tasks:
            tag = self.window_tasks[0].tag
            target = Path(self.output_root_var.get()) / f"{self.date_var.get().strip()}_{tag}"
        else:
            target = Path(self.output_root_var.get())
        if target.exists():
            open_path_in_file_manager(target)

    def _parse_upload_delay_range_ms(self, label: str, min_text: str, max_text: str) -> tuple[int, int]:
        raw_min = str(min_text or "").strip() or "0"
        raw_max = str(max_text or "").strip() or raw_min
        try:
            min_ms = max(0, int(raw_min))
            max_ms = max(0, int(raw_max))
        except ValueError as exc:
            raise ValueError(f"{label} 延迟必须是整数毫秒。") from exc
        if max_ms < min_ms:
            raise ValueError(f"{label} 延迟上限不能小于下限。")
        return min_ms, max_ms

    def _collect_upload_delay_settings(self) -> dict[str, Any]:
        mode_label = self.upload_delay_mode_var.get().strip()
        mode = "jitter" if mode_label == "抖动" else "steady"
        generic = self._parse_upload_delay_range_ms(
            "通用上传点击",
            self.upload_click_delay_min_ms_var.get(),
            self.upload_click_delay_max_ms_var.get(),
        )
        return {
            "mode": mode,
            "generic": generic,
            "file": self._parse_upload_delay_range_ms(
                "文件上传",
                self.upload_file_delay_min_ms_var.get(),
                self.upload_file_delay_max_ms_var.get(),
            ),
            "next": self._parse_upload_delay_range_ms(
                "Next",
                self.upload_next_delay_min_ms_var.get(),
                self.upload_next_delay_max_ms_var.get(),
            ),
            "done": self._parse_upload_delay_range_ms(
                "Done",
                self.upload_done_delay_min_ms_var.get(),
                self.upload_done_delay_max_ms_var.get(),
            ),
            "publish": self._parse_upload_delay_range_ms(
                "Publish",
                self.upload_publish_delay_min_ms_var.get(),
                self.upload_publish_delay_max_ms_var.get(),
            ),
        }

    def _append_upload_delay_args(self, cmd: list[str], delay_settings: dict[str, Any] | None) -> None:
        if not delay_settings:
            return
        mode = str(delay_settings.get("mode") or "steady").strip().lower()
        if mode in {"jitter", "steady"}:
            cmd.extend(["--click-delay-mode", mode])

        generic_min, generic_max = delay_settings.get("generic", (0, 0))
        if generic_min > 0 or generic_max > 0:
            cmd.extend(["--click-delay-min-ms", str(generic_min)])
            cmd.extend(["--click-delay-max-ms", str(generic_max)])

        for prefix in ("file", "next", "done", "publish"):
            profile_min, profile_max = delay_settings.get(prefix, (0, 0))
            if profile_min > 0 or profile_max > 0:
                cmd.extend([f"--{prefix}-delay-min-ms", str(profile_min)])
                cmd.extend([f"--{prefix}-delay-max-ms", str(profile_max)])


    def _collect_output_dirs_from_result(self, result) -> dict[str, str]:
        prepared: dict[str, str] = {}
        if not result:
            return prepared
        for item in getattr(result, "items", []) or []:
            output_video = str(getattr(item, "output_video", "") or "").strip()
            tag = str(getattr(item, "tag", "") or "").strip()
            if not output_video or not tag:
                continue
            folder = Path(output_video).parent
            if folder.exists():
                prepared.setdefault(tag, str(folder))
        return prepared

    def _launch_stream_upload_for_task(
        self,
        run_plan,
        task: WindowTask,
        output_dir: Path,
        *,
        retain_days: str,
        auto_close: bool,
        delay_settings: dict[str, Any] | None = None,
    ) -> None:
        current_config = dict(run_plan.config or {})
        single_modules = build_module_selection(metadata=False, render=False, upload=True)
        single_run_plan = build_run_plan(
            tasks=[task],
            defaults=run_plan.defaults,
            modules=single_modules,
            config=current_config,
        )
        plan = deepcopy(single_run_plan.window_plan)
        plan["tasks"] = [task.to_plan_dict(1)]
        plan["groups"] = {task.tag: [int(task.serial)]}
        plan["tags"] = [task.tag]
        plan["default_tag"] = task.tag
        plan["tag_output_dirs"] = {task.tag: str(output_dir)}
        plan_path = save_window_plan(
            plan,
            run_plan.defaults.date_mmdd,
            path=SCRIPT_DIR / "data" / f"window_upload_plan_{run_plan.defaults.date_mmdd}_{task.serial}.json",
        )
        self._log(
            f"[Upload] {task.tag}/{task.serial} 使用单任务上传计划: "
            f"output={output_dir} | metadata={single_run_plan.metadata_root}"
        )
        cmd = [
            sys.executable,
            "-u",
            str(UPLOAD_SCRIPT),
            "--tag",
            task.tag,
            "--date",
            run_plan.defaults.date_mmdd,
            "--channel",
            str(task.serial),
            "--auto-confirm",
            "--window-plan-file",
            str(plan_path),
            "--retain-video-days",
            retain_days,
        ]
        if auto_close:
            cmd.append("--auto-close-browser")
        self._append_upload_delay_args(cmd, delay_settings)

        label = f"{task.tag}/{task.serial}"
        self._log("[Upload] Stream dispatch -> " + " ".join(cmd))
        proc = subprocess.Popen(
            cmd,
            cwd=str(SCRIPT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_subprocess_utf8_env(),
        )
        with self._upload_process_lock:
            self.worker_processes.append(proc)

        def reader() -> None:
            error_text = ""
            completed_ok = False
            try:
                assert proc.stdout is not None
                for line in proc.stdout:
                    self._log(f"[Upload {label}] {line.rstrip()}")
                return_code = proc.wait()
                if return_code != 0 and not self._cancel_requested:
                    error_text = f"{label} exit {return_code}"
                else:
                    completed_ok = not self._cancel_requested
            except Exception as exc:
                if not self._cancel_requested:
                    error_text = f"{label}: {exc}"
            finally:
                with self._upload_process_lock:
                    try:
                        self.worker_processes.remove(proc)
                    except ValueError:
                        pass
                    if error_text:
                        self._upload_failures.append(error_text)
                if completed_ok:
                    self._post_ui_action(lambda serial=task.serial: self._run_progress_step_done(str(serial), "upload"))

        thread = threading.Thread(target=reader, daemon=True)
        with self._upload_process_lock:
            self.upload_monitor_threads.append(thread)
        thread.start()

    def _wait_for_stream_uploads(self) -> list[str]:
        while True:
            with self._upload_process_lock:
                self.upload_monitor_threads = [thread for thread in self.upload_monitor_threads if thread.is_alive()]
                active = [proc for proc in self.worker_processes if proc.poll() is None]
                failures = list(self._upload_failures)
            if not active and not self.upload_monitor_threads:
                return failures
            if self._cancel_requested:
                return failures
            time.sleep(0.5)

    def _run_upload_command(
        self,
        run_plan,
        *,
        detach: bool = False,
        prepared_output_dirs: dict[str, str] | None = None,
        retain_days: str | None = None,
        auto_close: bool | None = None,
        delay_settings: dict[str, Any] | None = None,
    ) -> bool:
        plan = deepcopy(run_plan.window_plan)
        if prepared_output_dirs:
            plan["tag_output_dirs"] = dict(prepared_output_dirs)
        plan_path = save_window_plan(plan, run_plan.defaults.date_mmdd)
        tags, skip_channels = derive_tags_and_skip_channels(plan, lambda tag: get_tag_info(tag) or {})
        retain_days = str(retain_days or "5")
        auto_close = bool(auto_close)
        ordered_targets: list[tuple[str, int]] = []
        seen_targets: set[tuple[str, int]] = set()
        for task in run_plan.tasks:
            clean_tag = str(task.tag or "").strip()
            key = (clean_tag, int(task.serial))
            if not clean_tag or key in seen_targets:
                continue
            seen_targets.add(key)
            ordered_targets.append(key)

        if len(ordered_targets) > 1 and detach:
            processes: list[tuple[str, subprocess.Popen[str]]] = []
            for tag_name, serial in ordered_targets:
                per_cmd = [
                    sys.executable,
                    "-u",
                    str(UPLOAD_SCRIPT),
                    "--tag",
                    tag_name,
                    "--date",
                    run_plan.defaults.date_mmdd,
                    "--channel",
                    str(serial),
                    "--auto-confirm",
                    "--window-plan-file",
                    str(plan_path),
                    "--retain-video-days",
                    retain_days,
                ]
                if auto_close:
                    per_cmd.append("--auto-close-browser")
                self._append_upload_delay_args(per_cmd, delay_settings)
                self._log("[Upload] " + " ".join(per_cmd))
                proc = subprocess.Popen(
                    per_cmd,
                    cwd=str(SCRIPT_DIR),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=_subprocess_utf8_env(),
                )
                processes.append((f"{tag_name}/{serial}", proc))

            self.worker_process = None
            self.upload_monitor_thread = None
            self.worker_processes = [proc for _label, proc in processes]
            self.upload_monitor_threads = []
            self._log(f"[Upload] Parallel upload started for {len(processes)} windows")

            completion_lock = threading.Lock()
            state = {"remaining": len(processes), "failures": []}

            def finalize_if_done() -> None:
                with completion_lock:
                    if state["remaining"] != 0:
                        return
                    failures = list(state["failures"])
                    self.worker_processes = []
                    self.upload_monitor_threads = []
                if self._cancel_requested:
                    self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary="当前批次已取消", cancelled=True))
                    return
                if failures:
                    summary = " | ".join(failures[:3])
                    self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary=summary))
                    self._post_ui_action(lambda: messagebox.showerror("Upload Failed", summary))
                else:
                    self._post_ui_action(lambda: self._finish_run_tracking(success=True, summary="Parallel upload finished"))

            def make_reader(label: str, proc: subprocess.Popen[str]):
                def reader() -> None:
                    error_text = ""
                    try:
                        assert proc.stdout is not None
                        for line in proc.stdout:
                            self._log(f"[Upload {label}] {line.rstrip()}")
                        return_code = proc.wait()
                        if return_code != 0 and not self._cancel_requested:
                            error_text = f"{label} exit {return_code}"
                            if "/" in label:
                                tag_name, serial_text = label.rsplit("/", 1)
                                try:
                                    self._mark_run_stage(tag_name, int(serial_text), "upload", "failed", f"进程退出 {return_code}")
                                except ValueError:
                                    pass
                        elif not self._cancel_requested and "/" in label:
                            tag_name, serial_text = label.rsplit("/", 1)
                            try:
                                current = self._run_result_map.get(label, {}).get("upload", {})
                                if current.get("status") in {"pending", "running"}:
                                    self._mark_run_stage(tag_name, int(serial_text), "upload", "success", "上传成功")
                            except ValueError:
                                pass
                    except Exception as exc:
                        if not self._cancel_requested:
                            error_text = f"{label}: {exc}"
                    finally:
                        with completion_lock:
                            state["remaining"] -= 1
                            try:
                                self.worker_processes.remove(proc)
                            except ValueError:
                                pass
                            if error_text:
                                state["failures"].append(error_text)
                    finalize_if_done()

                return reader

            for label, proc in processes:
                thread = threading.Thread(target=make_reader(label, proc), daemon=True)
                self.upload_monitor_threads.append(thread)
                thread.start()
            return True

        cmd = [
            sys.executable,
            "-u",
            str(UPLOAD_SCRIPT),
            "--tag",
            ",".join(tags),
            "--date",
            run_plan.defaults.date_mmdd,
            "--auto-confirm",
            "--window-plan-file",
            str(plan_path),
            "--retain-video-days",
            retain_days,
        ]
        if auto_close:
            cmd.append("--auto-close-browser")
        self._append_upload_delay_args(cmd, delay_settings)
        if skip_channels:
            cmd.append("--skip-channels=" + ",".join(str(item) for item in skip_channels))
        self._log("[上传] " + " ".join(cmd))
        self.worker_process = subprocess.Popen(
            cmd,
            cwd=str(SCRIPT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_subprocess_utf8_env(),
        )
        if detach:
            self._log("[上传] 已转入后台监控，可继续切换其他页面查看或修改配置")

            def reader() -> None:
                error_text = ""
                return_code = 0
                try:
                    assert self.worker_process is not None
                    assert self.worker_process.stdout is not None
                    for line in self.worker_process.stdout:
                        self._log(line.rstrip())
                    return_code = self.worker_process.wait()
                    if return_code != 0:
                        error_text = f"上传流程退出码 {return_code}"
                except Exception as exc:
                    error_text = str(exc)
                finally:
                    self.worker_process = None
                    self.upload_monitor_thread = None

                if self._cancel_requested:
                    self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary="当前批次已取消", cancelled=True))
                    return
                if error_text:
                    for task in getattr(run_plan, "tasks", []) or []:
                        current = self._run_result_map.get(self._task_result_key(task.tag, task.serial), {}).get("upload", {})
                        if current.get("status") in {"pending", "running"}:
                            self._mark_run_stage(task.tag, task.serial, "upload", "failed", error_text)
                    self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary=error_text))
                    self._post_ui_action(lambda: messagebox.showerror("上传任务失败", error_text))
                else:
                    self._post_ui_action(lambda: self._finish_run_tracking(success=True, summary="上传流程已结束"))

            self.upload_monitor_thread = threading.Thread(target=reader, daemon=True)
            self.upload_monitor_thread.start()
            return True

        try:
            assert self.worker_process.stdout is not None
            for line in self.worker_process.stdout:
                self._log(line.rstrip())
            return_code = self.worker_process.wait()
            if return_code != 0:
                raise RuntimeError(f"上传流程退出码 {return_code}")
        finally:
            self.worker_process = None
        return False

    def _run_background(self, func, *, task_name: str, total_items: int, include_upload: bool = False) -> None:
        if self._has_active_background_work():
            messagebox.showinfo("任务进行中", self._current_run_summary())
            return

        self._start_run_tracking(task_name, total_items, include_upload=include_upload)

        def runner() -> None:
            try:
                deferred_finish = bool(func())
            except WorkflowCancelledError as exc:
                self._log(f"[取消] {exc}")
                self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary=str(exc), cancelled=True))
                return
            except Exception as exc:
                self._log(f"[错误] {exc}")
                self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary=str(exc)))
                self._post_ui_action(lambda: messagebox.showerror("任务失败", str(exc)))
                return
            if deferred_finish:
                return
            if self._cancel_requested:
                self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary="当前批次已取消", cancelled=True))
            else:
                self._post_ui_action(lambda: self._finish_run_tracking(success=True, summary="任务已执行完成"))

        self.worker_thread = threading.Thread(target=runner, daemon=True)
        self.worker_thread.start()

    def _selected_modules(self) -> dict[str, bool]:
        return {
            "metadata": bool(self.run_metadata_var.get()),
            "render": bool(self.run_render_var.get()),
            "upload": bool(self.run_upload_var.get()),
        }

    def _current_module_selection(self):
        selected = self._selected_modules()
        return build_module_selection(
            metadata=selected["metadata"],
            render=selected["render"],
            upload=selected["upload"],
        )

    def _selected_module_labels(self) -> list[str]:
        return self._current_module_selection().labels()

    def _current_runtime_config(self) -> dict[str, Any]:
        try:
            self.update_idletasks()
        except Exception:
            pass
        config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
        if self.scheduler_config:
            config.update(dict(self.scheduler_config))
        config.update(
            {
                "metadata_root": self._runtime_field_text("metadata_root", self.metadata_root_var),
                "music_dir": self._runtime_field_text("music_dir", self.music_dir_var),
                "base_image_dir": self._runtime_field_text("base_image_dir", self.base_image_dir_var),
                "output_root": self._runtime_field_text("output_root", self.output_root_var),
                "ffmpeg_bin": self._runtime_field_text("ffmpeg_bin", self.ffmpeg_var) or "ffmpeg",
                "ffmpeg_path": self._runtime_field_text("ffmpeg_bin", self.ffmpeg_var) or "ffmpeg",
                "used_media_root": self._runtime_field_text("used_media_root", self.used_media_root_var),
                "render_cleanup_days": int(self._runtime_field_text("render_cleanup_days", self.cleanup_days_var) or "5"),
            }
        )
        return config

    def _sync_runtime_paths(self, *, persist: bool = False) -> dict[str, Any]:
        config = self._current_runtime_config()
        normalized = (
            save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
            if persist
            else normalize_scheduler_config(config, SCRIPT_DIR)
        )
        self.scheduler_config = normalized
        return normalized

    def _write_run_snapshot(self, *, config: dict[str, Any], run_plan) -> None:
        payload = {
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config": {
                "metadata_root": str(config.get("metadata_root") or ""),
                "music_dir": str(config.get("music_dir") or ""),
                "base_image_dir": str(config.get("base_image_dir") or ""),
                "output_root": str(config.get("output_root") or ""),
                "used_media_root": str(config.get("used_media_root") or ""),
                "ffmpeg_bin": str(config.get("ffmpeg_bin") or ""),
                "render_cleanup_days": int(config.get("render_cleanup_days") or 0),
            },
            "modules": self._current_module_selection().labels(),
            "tasks": [task.to_plan_dict(index + 1) for index, task in enumerate(run_plan.tasks)],
            "tag_output_dirs": dict(run_plan.window_plan.get("tag_output_dirs") or {}),
            "output_root": str(run_plan.output_root or ""),
            "metadata_root": str(run_plan.metadata_root or ""),
        }
        RUN_SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        RUN_SNAPSHOT_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _runtime_window_tasks(self) -> list[WindowTask]:
        selected_tag = str(self.current_group_var.get() or "").strip()
        live_source_dir = str(self.source_dir_override_var.get() or "").strip()
        tags = [str(task.tag or "").strip() for task in self.window_tasks if str(task.tag or "").strip()]
        unique_tags = list(dict.fromkeys(tags))
        single_tag_mode = len(unique_tags) == 1

        runtime_tasks: list[WindowTask] = []
        for task in self.window_tasks:
            should_override_source = False
            if single_tag_mode:
                should_override_source = True
            elif selected_tag and str(task.tag or "").strip() == selected_tag:
                should_override_source = True

            raw_quantity = max(1, int(getattr(task, "quantity", 1) or 1))
            raw_slot_index = max(1, int(getattr(task, "slot_index", 1) or 1))
            raw_total_slots = max(1, int(getattr(task, "total_slots", 1) or 1))
            raw_round_index = max(1, int(getattr(task, "round_index", 1) or 1))
            already_expanded = raw_total_slots > 1 or raw_slot_index > 1 or raw_round_index > 1
            slot_indexes = [raw_slot_index] if already_expanded else list(range(1, raw_quantity + 1))
            total_slots = raw_total_slots if already_expanded else raw_quantity

            for slot_index in slot_indexes:
                cloned = create_task(
                    tag=task.tag,
                    serial=task.serial,
                    quantity=raw_quantity,
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
                    source_dir=live_source_dir if should_override_source else task.source_dir,
                    channel_name=task.channel_name,
                    slot_index=slot_index,
                    total_slots=total_slots,
                    round_index=raw_round_index if already_expanded else slot_index,
                )
                cloned.tag_list = [str(item).strip() for item in task.tag_list if str(item).strip()]
                cloned.thumbnails = [str(item).strip() for item in task.thumbnails if str(item).strip()]
                cloned.ab_titles = [str(item).strip() for item in task.ab_titles if str(item).strip()]
                runtime_tasks.append(cloned)
        return runtime_tasks

    def _build_current_run_plan(self, *, config: dict[str, Any] | None = None):
        return build_run_plan(
            tasks=self._runtime_window_tasks(),
            defaults=self._collect_defaults(),
            modules=self._current_module_selection(),
            config=config or self._sync_runtime_paths(persist=False),
        )

    def _assert_manifest_ready_for_upload(self, *, manifest_path: Path, task: WindowTask, output_dir: Path) -> None:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"{task.tag}/{task.serial} upload manifest 读取失败: {exc}") from exc

        channels = payload.get("channels") if isinstance(payload, dict) else {}
        channel = channels.get(str(task.serial)) if isinstance(channels, dict) else {}
        if not isinstance(channel, dict):
            raise RuntimeError(f"{task.tag}/{task.serial} upload manifest 缺少当前窗口记录。")

        generation_source = str(channel.get("generation_source") or "").strip().lower()
        api_preset_name = str(channel.get("api_preset_name") or "").strip()
        api_base_url = str(channel.get("api_base_url") or "").strip()
        api_model = str(channel.get("api_model") or "").strip()
        content_template_name = str(channel.get("content_template_name") or "").strip()
        thumbnail_prompt_source = str(channel.get("thumbnail_prompt_source") or "").strip().lower()
        video_name = str(channel.get("video") or "").strip()
        thumbnails = [str(item).strip() for item in (channel.get("thumbnails") or []) if str(item).strip()]

        if generation_source != "api":
            raise RuntimeError(f"{task.tag}/{task.serial} manifest 不是 API 文案结果，已拒绝上传。")
        if not api_preset_name or not api_base_url or not api_model or not content_template_name:
            raise RuntimeError(f"{task.tag}/{task.serial} manifest 缺少 API/模板指纹，已拒绝上传。")
        if not video_name:
            raise RuntimeError(f"{task.tag}/{task.serial} manifest 未写入视频文件名。")

        video_path = output_dir / Path(video_name).name
        if not video_path.exists():
            raise RuntimeError(f"{task.tag}/{task.serial} manifest 指向的视频不存在: {video_path}")
        if self.generate_thumbnails_var.get() and not thumbnails:
            raise RuntimeError(f"{task.tag}/{task.serial} manifest 未写入缩略图路径，已拒绝上传。")
        if self.generate_thumbnails_var.get() and thumbnails and thumbnail_prompt_source != "api_text":
            raise RuntimeError(f"{task.tag}/{task.serial} manifest 未标记 API 缩略图提示词来源，已拒绝上传。")

        self._log(
            f"[Upload] manifest ready -> {task.tag}/{task.serial} | source={generation_source or 'n/a'} | "
            f"preset={api_preset_name or '-'} | model={api_model or '-'} | "
            f"template={content_template_name or '-'} | video={video_path.name}"
        )

    def _build_start_tab(self) -> None:
        tab = self.tabview.tab("快捷开始")
        tab.grid_columnconfigure(0, weight=1)

        task_frame = ctk.CTkFrame(tab)
        task_frame.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        for column in range(6):
            task_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(task_frame, text="本次任务", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", padx=16, pady=(14, 8)
        )
        ctk.CTkLabel(
            task_frame,
            text="勾选今天要执行的模块。只勾一个就单独跑那一个，勾多个就按顺序连续执行。",
            text_color="#b8c1cc",
        ).grid(row=1, column=0, columnspan=6, sticky="w", padx=16, pady=(0, 10))
        ctk.CTkCheckBox(task_frame, text="生成标题/简介/标签/缩略图", variable=self.run_metadata_var).grid(
            row=2, column=0, columnspan=2, sticky="w", padx=(16, 8), pady=(0, 10)
        )
        ctk.CTkCheckBox(task_frame, text="剪辑", variable=self.run_render_var).grid(
            row=2, column=2, columnspan=2, sticky="w", padx=8, pady=(0, 10)
        )
        ctk.CTkCheckBox(task_frame, text="上传", variable=self.run_upload_var).grid(
            row=2, column=4, columnspan=2, sticky="w", padx=8, pady=(0, 10)
        )
        ctk.CTkLabel(task_frame, text="日期").grid(row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 8))
        ctk.CTkEntry(task_frame, textvariable=self.date_var, width=140).grid(
            row=3, column=1, sticky="w", padx=(0, 12), pady=(0, 8)
        )
        ctk.CTkLabel(task_frame, text="模拟时长(秒)").grid(row=3, column=2, sticky="w", padx=(0, 8), pady=(0, 8))
        ctk.CTkEntry(task_frame, textvariable=self.simulate_seconds_var, width=120).grid(
            row=3, column=3, sticky="w", padx=(0, 12), pady=(0, 8)
        )
        ctk.CTkSwitch(task_frame, text="随机视觉特效", variable=self.randomize_effects_var).grid(
            row=3, column=4, columnspan=2, sticky="w", padx=8, pady=(0, 8)
        )
        ctk.CTkButton(
            task_frame,
            text="打开高级视觉",
            command=lambda: self.tabview.set("高级视觉"),
            width=140,
        ).grid(row=4, column=4, columnspan=2, sticky="w", padx=8, pady=(0, 8))

        option_frame = ctk.CTkFrame(tab)
        option_frame.grid(row=1, column=0, sticky="ew", padx=16, pady=8)
        option_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            option_frame,
            text="文案模块只写标题/简介/标签/缩略图到文案输出目录。剪辑模块只生成成品视频。上传模块直接读取上面配置好的目录，如果缺文件会直接报错。",
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=0, column=0, sticky="w", padx=16, pady=16)

        action_frame = ctk.CTkFrame(tab)
        action_frame.grid(row=2, column=0, sticky="ew", padx=16, pady=8)
        for column in range(5):
            action_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkButton(action_frame, text="预览计划", command=self._preview_plan).grid(
            row=0, column=0, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="路径检查", command=self._validate_paths).grid(
            row=0, column=1, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="模拟 1-2 分钟", command=self._start_simulation).grid(
            row=0, column=2, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="开始真实流程", command=self._start_real_flow).grid(
            row=0, column=3, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="打开当前输出目录", command=self._open_current_output).grid(
            row=0, column=4, sticky="ew", padx=12, pady=12
        )

        self.start_preview = ctk.CTkTextbox(tab, height=420)
        self.start_preview.grid(row=3, column=0, sticky="nsew", padx=16, pady=(8, 16))
        tab.grid_rowconfigure(3, weight=1)

    def _build_paths_tab(self) -> None:
        base_tab = self.tabview.tab("路径配置")
        base_tab.grid_columnconfigure(0, weight=1)
        base_tab.grid_rowconfigure(0, weight=1)
        tab = ctk.CTkScrollableFrame(base_tab)
        tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        tab.grid_columnconfigure(0, weight=1)

        path_frame = ctk.CTkFrame(tab)
        path_frame.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 8))
        path_frame.grid_columnconfigure(0, weight=0)
        path_frame.grid_columnconfigure(1, weight=1)
        path_frame.grid_columnconfigure(2, weight=1)
        path_frame.grid_columnconfigure(3, weight=1)
        ctk.CTkLabel(path_frame, text="全局路径", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 12)
        )
        for row, (label, var) in enumerate(
            [
                ("文案输出目录", self.metadata_root_var),
                ("音乐目录", self.music_dir_var),
                ("底图目录", self.base_image_dir_var),
                ("成品视频输出目录", self.output_root_var),
                ("FFmpeg", self.ffmpeg_var),
                ("已用素材目录", self.used_media_root_var),
                ("上传后保留天数", self.cleanup_days_var),
            ],
            start=1,
        ):
            entry_key = {
                1: "metadata_root",
                2: "music_dir",
                3: "base_image_dir",
                4: "output_root",
                5: "ffmpeg_bin",
                6: "used_media_root",
                7: "render_cleanup_days",
            }.get(row)
            self._entry_row(path_frame, row, label, var, entry_key=entry_key)
        ctk.CTkButton(path_frame, text="保存路径配置", command=self._save_paths).grid(
            row=8, column=0, columnspan=4, sticky="w", padx=16, pady=(0, 14)
        )

        binding_frame = ctk.CTkFrame(tab)
        binding_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(8, 16))
        for column in range(4):
            binding_frame.grid_columnconfigure(column, weight=1)
        binding_frame.grid_rowconfigure(6, weight=0)
        ctk.CTkLabel(binding_frame, text="分组素材目录绑定", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkLabel(
            binding_frame,
            text="每个 BitBrowser 分组都可以长期绑定一个素材目录。上传页如果不单独覆盖，就自动使用这里的绑定。",
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=1, column=0, columnspan=4, sticky="w", padx=16, pady=(0, 10))
        ctk.CTkLabel(binding_frame, text="分组").grid(row=2, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        self.binding_group_menu = ctk.CTkOptionMenu(binding_frame, variable=self.binding_group_var, values=[""])
        self.binding_group_menu.grid(row=3, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
        ctk.CTkLabel(binding_frame, text="绑定目录").grid(row=2, column=1, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkEntry(binding_frame, textvariable=self.binding_folder_var).grid(
            row=3, column=1, columnspan=2, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkButton(binding_frame, text="选择文件夹", command=self._pick_binding_folder).grid(
            row=3, column=3, sticky="ew", padx=(8, 16), pady=(0, 12)
        )
        bar = ctk.CTkFrame(binding_frame, fg_color="transparent")
        bar.grid(row=4, column=0, columnspan=4, sticky="ew", padx=10, pady=(0, 8))
        ctk.CTkButton(bar, text="保存绑定", command=self._save_binding).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="删除绑定", command=self._remove_binding).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="刷新分组", command=self._refresh_groups).pack(side="left", padx=6)
        ctk.CTkLabel(binding_frame, text="当前绑定（框内可滚动）").grid(row=5, column=0, sticky="w", padx=16, pady=(0, 6))
        self.binding_box = ctk.CTkTextbox(binding_frame, height=200)
        self.binding_box.grid(row=6, column=0, columnspan=4, sticky="nsew", padx=16, pady=(0, 14))
        self._bind_scroll_frame_wheel(tab, base_tab, tab, path_frame, binding_frame)
        canvas = getattr(tab, "_parent_canvas", None)
        if canvas is not None:
            def _scroll_binding_box(event: Any) -> str | None:
                delta = 0
                if getattr(event, "delta", 0):
                    delta = -int(event.delta / 120) if event.delta else 0
                elif getattr(event, "num", None) == 4:
                    delta = -1
                elif getattr(event, "num", None) == 5:
                    delta = 1
                if delta:
                    canvas.yview_scroll(delta, "units")
                    return "break"
                return None

            for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
                try:
                    self.binding_box.bind(sequence, _scroll_binding_box, add="+")
                except Exception:
                    pass
                try:
                    inner_binding_box = getattr(self.binding_box, "_textbox", None)
                    if inner_binding_box is not None:
                        inner_binding_box.bind(sequence, _scroll_binding_box, add="+")
                except Exception:
                    pass

    def _save_paths(self) -> dict[str, Any]:
        config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
        config.update(
            {
                "metadata_root": self._runtime_field_text("metadata_root", self.metadata_root_var),
                "music_dir": self._runtime_field_text("music_dir", self.music_dir_var),
                "base_image_dir": self._runtime_field_text("base_image_dir", self.base_image_dir_var),
                "output_root": self._runtime_field_text("output_root", self.output_root_var),
                "ffmpeg_bin": self._runtime_field_text("ffmpeg_bin", self.ffmpeg_var) or "ffmpeg",
                "ffmpeg_path": self._runtime_field_text("ffmpeg_bin", self.ffmpeg_var) or "ffmpeg",
                "used_media_root": self._runtime_field_text("used_media_root", self.used_media_root_var),
                "render_cleanup_days": int(self._runtime_field_text("render_cleanup_days", self.cleanup_days_var) or "5"),
            }
        )
        self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
        self.metadata_root_var.set(str(get_metadata_root(self.scheduler_config)))
        self.music_dir_var.set(str(self.scheduler_config.get("music_dir", "")))
        self.base_image_dir_var.set(str(self.scheduler_config.get("base_image_dir", "")))
        self.output_root_var.set(str(self.scheduler_config.get("output_root", "")))
        self.ffmpeg_var.set(str(self.scheduler_config.get("ffmpeg_bin", "ffmpeg")))
        self.used_media_root_var.set(str(self.scheduler_config.get("used_media_root", "")))
        self.cleanup_days_var.set(str(self.scheduler_config.get("render_cleanup_days", 5)))
        self._refresh_bindings_box()
        self._log("[Paths] Saved path config")
        return dict(self.scheduler_config)

    def _collect_defaults(self) -> WorkflowDefaults:
        modules = self._selected_modules()
        return WorkflowDefaults(
            date_mmdd=normalize_mmdd(self.date_var.get().strip() or _today_mmdd()),
            visibility="schedule" if self.schedule_enabled_var.get() else self.default_visibility_var.get(),
            category=self.default_category_var.get(),
            made_for_kids=_bool_from_yes_no(self.default_kids_var.get()),
            altered_content=_bool_from_yes_no(self.default_ai_var.get()),
            notify_subscribers=bool(self.default_notify_var.get()),
            schedule_enabled=bool(self.schedule_enabled_var.get()),
            schedule_start=self._compose_default_schedule(),
            schedule_interval_minutes=int(self.schedule_interval_var.get().strip() or "60"),
            schedule_timezone=self.schedule_timezone_var.get().strip() or SCHEDULE_TIMEZONE_VALUES[0],
            metadata_mode="prompt_api",
            generate_text=bool(modules["metadata"]),
            generate_thumbnails=bool(modules["metadata"]),
            sync_daily_content=bool(modules["metadata"]),
            randomize_effects=False,
            visual_settings=self._collect_visual_settings(),
        )

    def _preview_plan(self) -> None:
        self._save_paths()
        self.start_preview.delete("1.0", "end")
        module_selection = self._current_module_selection()
        if not module_selection.any_selected():
            self.start_preview.insert("1.0", "No module selected. Go to Quick Start and check Metadata / Render / Upload.")
            return
        if not self.window_tasks:
            self.start_preview.insert("1.0", "No window tasks yet. Go to Upload tab and add at least one window.")
            return

        try:
            run_plan = self._build_current_run_plan()
        except Exception as exc:
            self.start_preview.insert("1.0", f"Plan build failed: {exc}")
            return

        lines = preview_run_plan(run_plan)
        report = validate_run_plan(run_plan, log=lambda *_args, **_kwargs: None)
        if report.warnings:
            lines.append("")
            lines.extend(f"Warning: {item}" for item in report.warnings)
        if report.resolved_output_dirs:
            lines.append("")
            lines.append("Upload will use these existing video folders:")
            for tag, folder in report.resolved_output_dirs.items():
                lines.append(f"  - {tag}: {folder}")
        if report.errors:
            lines.append("")
            lines.extend(f"Error: {item}" for item in report.errors)

        self.start_preview.insert("1.0", "\n".join(lines))
        self._save_state()

    def _validate_paths(self) -> None:
        saved_config = self._save_paths()
        module_selection = self._current_module_selection()
        if not module_selection.any_selected():
            messagebox.showerror("Validate Failed", "Select at least one module first.")
            return
        if not self.window_tasks:
            messagebox.showerror("Validate Failed", "Add at least one window task first.")
            return

        run_plan = self._build_current_run_plan(config=saved_config)
        report = validate_run_plan(run_plan, log=self._log)
        text: list[str] = []
        text.extend(f"Ready to upload: {tag} -> {folder}" for tag, folder in report.resolved_output_dirs.items())
        text.extend(f"Warning: {item}" for item in report.warnings)
        text.extend(f"Error: {item}" for item in report.errors)

        if report.errors:
            messagebox.showerror("Validate Failed", "\n".join(text) if text else "Validation failed.")
        else:
            messagebox.showinfo("Validate OK", "\n".join(text) if text else "Validation passed.")
        self._preview_plan()

    def _start_simulation(self) -> None:
        saved_config = self._save_paths()
        module_selection = self._current_module_selection()
        if not module_selection.render:
            messagebox.showerror("Cannot Simulate", "Simulation requires the Render module.")
            return
        if not self.window_tasks:
            messagebox.showerror("Cannot Simulate", "Add at least one window task first.")
            return
        run_plan = self._build_current_run_plan(config=saved_config)
        seconds = int(self.simulate_seconds_var.get().strip() or "90")

        def job() -> None:
            result = execute_simulation_plan(
                run_plan,
                simulate_seconds=seconds,
                control=self.execution_control,
                log=self._log,
            )
            self._log(f"[Simulate] Finished. Generated {len(result.items)} videos")
            self._log(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))

        self._run_background(job, task_name="Simulate Render", total_items=len(self.window_tasks), include_upload=False)

    def _start_real_flow(self) -> None:
        saved_config = self._save_paths()
        self._log(
            "[路径] 本次运行使用: "
            f"metadata={saved_config.get('metadata_root')} | "
            f"music={saved_config.get('music_dir')} | "
            f"image={saved_config.get('base_image_dir')} | "
            f"output={saved_config.get('output_root')}"
        )
        module_selection = self._current_module_selection()
        if not module_selection.any_selected():
            messagebox.showerror("Cannot Start", "Select at least one module first.")
            return
        if not self.window_tasks:
            messagebox.showerror("Cannot Start", "Add at least one window task first.")
            return
        try:
            upload_delay_settings = self._collect_upload_delay_settings()
        except ValueError as exc:
            messagebox.showerror("Upload Delay Invalid", str(exc))
            return
        if module_selection.metadata:
            if not bool(self.generate_text_var.get()):
                self.generate_text_var.set(True)
            if not bool(self.generate_thumbnails_var.get()):
                self.generate_thumbnails_var.set(True)
            self._log("[Metadata] Quick Start 已勾选文案模块，本次强制走 API 生成标题/简介/标签，缩略图优先走图片 API。")
        self._persist_prompt_form_for_active_tasks()
        run_plan = self._build_current_run_plan(config=saved_config)
        self._write_run_snapshot(config=saved_config, run_plan=run_plan)
        self._prepare_run_result_tracking(run_plan)
        task_runtime_rows = [
            {
                "tag": str(task.tag or "").strip(),
                "serial": int(task.serial),
                "source_dir": str(task.source_dir or "").strip(),
                "title": str(task.title or "").strip(),
                "thumbnails": [str(item).strip() for item in task.thumbnails if str(item).strip()],
            }
            for task in run_plan.tasks
        ]
        self._log(f"[Paths] Runtime tasks: {json.dumps(task_runtime_rows, ensure_ascii=False)}")
        self._log(f"[Paths] Resolved tag output dirs: {json.dumps(run_plan.window_plan.get('tag_output_dirs', {}), ensure_ascii=False)}")
        self._log(f"[Paths] Resolved tag metadata dirs: {json.dumps(run_plan.window_plan.get('tag_metadata_dirs', {}), ensure_ascii=False)}")
        upload_runtime = {
            "retain_days": str(self.cleanup_days_var.get().strip() or "5"),
            "auto_close": bool(self.upload_auto_close_var.get()),
            "delay_settings": upload_delay_settings,
        }

        def job() -> bool:
            self._log(
                f"[Paths] metadata={run_plan.metadata_root} | music={run_plan.music_root} | "
                f"image={run_plan.image_root} | output={run_plan.output_root}"
            )
            stream_upload = bool(run_plan.modules.render and run_plan.modules.upload)
            upload_dispatched = False

            def handle_item_ready(task: WindowTask, output_dir: Path, _manifest_path: Path) -> None:
                nonlocal upload_dispatched
                if not stream_upload:
                    return
                self._assert_manifest_ready_for_upload(
                    manifest_path=_manifest_path,
                    task=task,
                    output_dir=output_dir,
                )
                upload_dispatched = True
                self._log(f"[Upload] {task.tag}/{task.serial} 已完成渲染与文案，立即开始上传")
                self._launch_stream_upload_for_task(
                    run_plan,
                    task,
                    output_dir,
                    retain_days=upload_runtime["retain_days"],
                    auto_close=upload_runtime["auto_close"],
                    delay_settings=upload_runtime["delay_settings"],
                )

            execution = execute_run_plan(
                run_plan,
                control=self.execution_control,
                on_item_ready=handle_item_ready if stream_upload else None,
                log=self._log,
            )
            self._ingest_execution_result(execution)

            if stream_upload:
                if upload_dispatched:
                    failures = self._wait_for_stream_uploads()
                    if self._cancel_requested:
                        return False
                    if failures:
                        raise RuntimeError(" | ".join(failures[:3]))
                else:
                    self._log("[Upload] 本批没有可上传的已就绪项目；渲染会继续，失败的视频不会阻断后续任务。")
                return False

            if run_plan.modules.upload:
                self._log("[Start] Upload module")
                return self._run_upload_command(
                    run_plan,
                    detach=True,
                    prepared_output_dirs=execution.prepared_output_dirs,
                    retain_days=upload_runtime["retain_days"],
                    auto_close=upload_runtime["auto_close"],
                    delay_settings=upload_runtime["delay_settings"],
                )

            return False

        task_name = " + ".join(self._selected_module_labels())
        self._run_background(
            job,
            task_name=task_name,
            total_items=len(self.window_tasks),
            include_upload=bool(module_selection.upload),
        )


    def _bind_scroll_frame_wheel(
        self,
        scroll_frame: ctk.CTkScrollableFrame,
        *widgets: ctk.CTkBaseClass,
        include_textboxes: bool = False,
    ) -> None:
        canvas = getattr(scroll_frame, "_parent_canvas", None)
        if canvas is None:
            return

        try:
            canvas.configure(yscrollincrement=24)
        except Exception:
            pass

        def _on_mousewheel(event: Any) -> str | None:
            delta = 0
            if getattr(event, "delta", 0):
                delta = -int(event.delta / 120) if event.delta else 0
            elif getattr(event, "num", None) == 4:
                delta = -1
            elif getattr(event, "num", None) == 5:
                delta = 1
            if delta:
                canvas.yview_scroll(delta, "units")
                return "break"
            return None

        def _bind_tree(widget: Any) -> None:
            if isinstance(widget, ctk.CTkTextbox) and not include_textboxes:
                return
            for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
                try:
                    widget.bind(sequence, _on_mousewheel, add="+")
                except Exception:
                    pass
            for attr_name in ("_entry", "_text_label", "_canvas", "_dropdown_menu", "_scrollbar", "_textbox"):
                inner = getattr(widget, attr_name, None)
                if inner is None:
                    continue
                for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
                    try:
                        inner.bind(sequence, _on_mousewheel, add="+")
                    except Exception:
                        pass
            for child in widget.winfo_children():
                _bind_tree(child)

        for widget in widgets:
            _bind_tree(widget)

    def _build_start_tab(self) -> None:
        base_tab = self.tabview.tab("快捷开始")
        base_tab.grid_columnconfigure(0, weight=1)
        base_tab.grid_rowconfigure(0, weight=1)
        tab = ctk.CTkScrollableFrame(base_tab)
        tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        tab.grid_columnconfigure(0, weight=1)

        task_frame = ctk.CTkFrame(tab)
        task_frame.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 8))
        for column in range(6):
            task_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(task_frame, text="本次任务", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", padx=16, pady=(14, 8)
        )
        ctk.CTkLabel(
            task_frame,
            text="勾选今天要执行的模块。随机与否全部在高级视觉里用 random 控制，这里不再保留全局随机开关。",
            text_color="#b8c1cc",
        ).grid(row=1, column=0, columnspan=6, sticky="w", padx=16, pady=(0, 10))
        ctk.CTkCheckBox(task_frame, text="生成标题/简介/标签/缩略图", variable=self.run_metadata_var).grid(
            row=2, column=0, columnspan=2, sticky="w", padx=(16, 8), pady=(0, 10)
        )
        ctk.CTkCheckBox(task_frame, text="剪辑", variable=self.run_render_var).grid(
            row=2, column=2, columnspan=2, sticky="w", padx=8, pady=(0, 10)
        )
        ctk.CTkCheckBox(task_frame, text="上传", variable=self.run_upload_var).grid(
            row=2, column=4, columnspan=2, sticky="w", padx=8, pady=(0, 10)
        )
        ctk.CTkLabel(task_frame, text="日期").grid(row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 8))
        ctk.CTkEntry(task_frame, textvariable=self.date_var, width=140).grid(
            row=3, column=1, sticky="w", padx=(0, 12), pady=(0, 8)
        )
        ctk.CTkLabel(task_frame, text="模拟时长(秒)").grid(row=3, column=2, sticky="w", padx=(0, 8), pady=(0, 8))
        ctk.CTkEntry(task_frame, textvariable=self.simulate_seconds_var, width=120).grid(
            row=3, column=3, sticky="w", padx=(0, 12), pady=(0, 8)
        )
        ctk.CTkButton(
            task_frame,
            text="打开高级视觉",
            command=lambda: self.tabview.set("高级视觉"),
            width=140,
        ).grid(row=3, column=4, columnspan=2, sticky="w", padx=8, pady=(0, 8))
        ctk.CTkLabel(task_frame, text="上传延迟策略").grid(row=4, column=0, sticky="w", padx=(16, 8), pady=(0, 12))
        ctk.CTkOptionMenu(task_frame, variable=self.upload_delay_mode_var, values=["抖动", "稳态"]).grid(
            row=4, column=1, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkLabel(task_frame, text="文件上传 下限/上限(ms)").grid(row=4, column=2, sticky="w", padx=(0, 8), pady=(0, 12))
        ctk.CTkEntry(task_frame, textvariable=self.upload_file_delay_min_ms_var, width=100).grid(
            row=4, column=3, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkEntry(task_frame, textvariable=self.upload_file_delay_max_ms_var, width=100).grid(
            row=4, column=4, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkLabel(
            task_frame,
            text="抖动=每次在区间内取值；稳态=主点下限、兜底中值、强制上限。",
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=4, column=5, sticky="w", padx=(0, 16), pady=(0, 12))
        ctk.CTkLabel(task_frame, text="Next 下限/上限(ms)").grid(row=5, column=0, sticky="w", padx=(16, 8), pady=(0, 12))
        ctk.CTkEntry(task_frame, textvariable=self.upload_next_delay_min_ms_var, width=100).grid(
            row=5, column=1, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkEntry(task_frame, textvariable=self.upload_next_delay_max_ms_var, width=100).grid(
            row=5, column=2, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkLabel(task_frame, text="Done 下限/上限(ms)").grid(row=5, column=3, sticky="w", padx=(0, 8), pady=(0, 12))
        ctk.CTkEntry(task_frame, textvariable=self.upload_done_delay_min_ms_var, width=100).grid(
            row=5, column=4, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkEntry(task_frame, textvariable=self.upload_done_delay_max_ms_var, width=100).grid(
            row=5, column=5, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkLabel(task_frame, text="Publish 下限/上限(ms)").grid(row=6, column=0, sticky="w", padx=(16, 8), pady=(0, 12))
        ctk.CTkEntry(task_frame, textvariable=self.upload_publish_delay_min_ms_var, width=100).grid(
            row=6, column=1, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkEntry(task_frame, textvariable=self.upload_publish_delay_max_ms_var, width=100).grid(
            row=6, column=2, sticky="w", padx=(0, 12), pady=(0, 12)
        )
        ctk.CTkLabel(
            task_frame,
            text="这些延迟只影响自动上传链路：文件注入、Next、Done/Save、Publish。",
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=6, column=3, columnspan=3, sticky="w", padx=(0, 16), pady=(0, 12))

        option_frame = ctk.CTkFrame(tab)
        option_frame.grid(row=1, column=0, sticky="ew", padx=8, pady=8)
        option_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            option_frame,
            text="文案模块只写标题、简介、标签和缩略图到文案输出目录。剪辑模块只生成成品视频。上传模块直接读取上面配置好的目录，如果缺文件会直接报错。",
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=0, column=0, sticky="w", padx=16, pady=16)

        action_frame = ctk.CTkFrame(tab)
        action_frame.grid(row=2, column=0, sticky="ew", padx=8, pady=8)
        for column in range(5):
            action_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkButton(action_frame, text="预览计划", command=self._preview_plan).grid(
            row=0, column=0, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="路径检查", command=self._validate_paths).grid(
            row=0, column=1, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="模拟 1-2 分钟", command=self._start_simulation).grid(
            row=0, column=2, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="开始真实流程", command=self._start_real_flow).grid(
            row=0, column=3, sticky="ew", padx=12, pady=12
        )
        ctk.CTkButton(action_frame, text="打开当前输出目录", command=self._open_current_output).grid(
            row=0, column=4, sticky="ew", padx=12, pady=12
        )

        self.start_preview = ctk.CTkTextbox(tab, height=420)
        self.start_preview.grid(row=3, column=0, sticky="nsew", padx=8, pady=(8, 16))
        self._bind_scroll_frame_wheel(tab, base_tab, tab, task_frame, option_frame, action_frame, self.start_preview, include_textboxes=True)

    def _build_paths_tab(self) -> None:
        base_tab = self.tabview.tab("路径配置")
        base_tab.grid_columnconfigure(0, weight=1)
        base_tab.grid_rowconfigure(0, weight=1)
        tab = ctk.CTkScrollableFrame(base_tab)
        tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        tab.grid_columnconfigure(0, weight=1)

        path_frame = ctk.CTkFrame(tab)
        path_frame.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 8))
        path_frame.grid_columnconfigure(0, weight=0)
        path_frame.grid_columnconfigure(1, weight=1)
        path_frame.grid_columnconfigure(2, weight=1)
        path_frame.grid_columnconfigure(3, weight=1)
        ctk.CTkLabel(path_frame, text="全局路径", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 12)
        )
        for row, (label, var) in enumerate(
            [
                ("文案输出目录", self.metadata_root_var),
                ("音乐目录", self.music_dir_var),
                ("底图目录", self.base_image_dir_var),
                ("成品视频输出目录", self.output_root_var),
                ("FFmpeg", self.ffmpeg_var),
                ("已用素材目录", self.used_media_root_var),
                ("上传后保留天数", self.cleanup_days_var),
            ],
            start=1,
        ):
            entry_key = {
                1: "metadata_root",
                2: "music_dir",
                3: "base_image_dir",
                4: "output_root",
                5: "ffmpeg_bin",
                6: "used_media_root",
                7: "render_cleanup_days",
            }.get(row)
            self._entry_row(path_frame, row, label, var, entry_key=entry_key)
        ctk.CTkButton(path_frame, text="保存路径配置", command=self._save_paths).grid(
            row=8, column=0, columnspan=4, sticky="w", padx=16, pady=(0, 14)
        )

        binding_frame = ctk.CTkFrame(tab)
        binding_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(8, 16))
        for column in range(4):
            binding_frame.grid_columnconfigure(column, weight=1)
        binding_frame.grid_rowconfigure(6, weight=0)
        ctk.CTkLabel(binding_frame, text="分组素材目录绑定", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkLabel(
            binding_frame,
            text="每个 BitBrowser 分组都可以长期绑定一个素材目录。上传页如果不单独覆盖，就自动使用这里的绑定。",
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=1, column=0, columnspan=4, sticky="w", padx=16, pady=(0, 10))
        ctk.CTkLabel(binding_frame, text="分组").grid(row=2, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        self.binding_group_menu = ctk.CTkOptionMenu(binding_frame, variable=self.binding_group_var, values=[""])
        self.binding_group_menu.grid(row=3, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
        ctk.CTkLabel(binding_frame, text="绑定目录").grid(row=2, column=1, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkEntry(binding_frame, textvariable=self.binding_folder_var).grid(
            row=3, column=1, columnspan=2, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkButton(binding_frame, text="选择文件夹", command=self._pick_binding_folder).grid(
            row=3, column=3, sticky="ew", padx=(8, 16), pady=(0, 12)
        )
        bar = ctk.CTkFrame(binding_frame, fg_color="transparent")
        bar.grid(row=4, column=0, columnspan=4, sticky="ew", padx=10, pady=(0, 8))
        ctk.CTkButton(bar, text="保存绑定", command=self._save_binding).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="删除绑定", command=self._remove_binding).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="刷新分组", command=self._refresh_groups).pack(side="left", padx=6)
        ctk.CTkLabel(binding_frame, text="当前绑定（这里也支持滚轮直接翻到底）").grid(
            row=5, column=0, sticky="w", padx=16, pady=(0, 6)
        )
        self.binding_box = ctk.CTkTextbox(binding_frame, height=140)
        self.binding_box.grid(row=6, column=0, columnspan=4, sticky="nsew", padx=16, pady=(0, 14))
        self._bind_scroll_frame_wheel(
            tab,
            base_tab,
            tab,
            path_frame,
            binding_frame,
            self.binding_box,
            include_textboxes=True,
        )

    def _on_close(self) -> None:
        self._save_state()
        self.destroy()


def _patched_refresh_task_tree(self: DashboardApp) -> None:
    for item in self.task_tree.get_children():
        self.task_tree.delete(item)
    bindings = get_group_bindings(self.scheduler_config)
    for index, task in enumerate(self.window_tasks):
        source_text = str(task.source_dir or "").strip() or bindings.get(task.tag, "")
        self.task_tree.insert(
            "",
            "end",
            iid=str(index),
            values=(
                task.tag,
                task.serial,
                max(1, int(getattr(task, "quantity", 1) or 1)),
                _yes_no_from_bool(task.is_ypp),
                task.visibility,
                task.category,
                _yes_no_from_bool(task.made_for_kids),
                _yes_no_from_bool(task.altered_content),
                task.scheduled_publish_at,
                source_text,
            ),
        )
    self._save_state()


def _patched_add_window_task(self: DashboardApp, info: Any) -> None:
    quantity_text = str(self.add_quantity_var.get() or "1").strip()
    try:
        quantity = max(1, int(quantity_text or "1"))
    except ValueError:
        quantity = 1
        self.add_quantity_var.set("1")
    task = create_task(
        tag=info.tag,
        serial=info.serial,
        quantity=quantity,
        is_ypp=_bool_from_yes_no(self.add_ypp_var.get()) or bool(info.is_ypp),
        title=self.add_title_var.get(),
        visibility="schedule" if self.add_schedule_enabled_var.get() else self.add_visibility_var.get(),
        category=self.add_category_var.get(),
        made_for_kids=_bool_from_yes_no(self.add_kids_var.get()),
        altered_content=_bool_from_yes_no(self.add_ai_var.get()),
        notify_subscribers=bool(self.add_notify_var.get()),
        scheduled_publish_at=self._compose_add_schedule(),
        schedule_timezone=self.add_schedule_timezone_var.get() if self.add_schedule_enabled_var.get() else "",
        source_dir=self.source_dir_override_var.get(),
        channel_name=info.channel_name,
    )
    for index, existing in enumerate(self.window_tasks):
        if existing.tag == task.tag and existing.serial == task.serial:
            self.window_tasks[index] = task
            break
    else:
        self.window_tasks.append(task)
    self._refresh_task_tree()
    self._preview_plan()


def _patched_runtime_window_tasks(self: DashboardApp) -> list[WindowTask]:
    selected_tag = str(self.current_group_var.get() or "").strip()
    live_source_dir = str(self.source_dir_override_var.get() or "").strip()
    tags = [str(task.tag or "").strip() for task in self.window_tasks if str(task.tag or "").strip()]
    unique_tags = list(dict.fromkeys(tags))
    single_tag_mode = len(unique_tags) == 1

    runtime_tasks: list[WindowTask] = []
    for task in self.window_tasks:
        should_override_source = False
        if single_tag_mode:
            should_override_source = True
        elif selected_tag and str(task.tag or "").strip() == selected_tag:
            should_override_source = True

        raw_quantity = max(1, int(getattr(task, "quantity", 1) or 1))
        raw_slot_index = max(1, int(getattr(task, "slot_index", 1) or 1))
        raw_total_slots = max(1, int(getattr(task, "total_slots", 1) or 1))
        raw_round_index = max(1, int(getattr(task, "round_index", 1) or 1))
        already_expanded = raw_total_slots > 1 or raw_slot_index > 1 or raw_round_index > 1
        slot_indexes = [raw_slot_index] if already_expanded else list(range(1, raw_quantity + 1))
        total_slots = raw_total_slots if already_expanded else raw_quantity

        for slot_index in slot_indexes:
            cloned = create_task(
                tag=task.tag,
                serial=task.serial,
                quantity=raw_quantity,
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
                source_dir=live_source_dir if should_override_source else task.source_dir,
                channel_name=task.channel_name,
                slot_index=slot_index,
                total_slots=total_slots,
                round_index=raw_round_index if already_expanded else slot_index,
            )
            cloned.tag_list = [str(item).strip() for item in task.tag_list if str(item).strip()]
            cloned.thumbnails = [str(item).strip() for item in task.thumbnails if str(item).strip()]
            cloned.ab_titles = [str(item).strip() for item in task.ab_titles if str(item).strip()]
            runtime_tasks.append(cloned)
    return runtime_tasks


def _patched_run_progress_step_done(self: DashboardApp, step_key: str, step_type: str) -> None:
    target_set = self._run_render_done if step_type == "render" else self._run_upload_done
    marker = str(step_key)
    if marker in target_set:
        return
    target_set.add(marker)
    self._run_completed_steps = min(self._run_total_steps, self._run_completed_steps + 1)
    self._run_current_ratio = 0.0


def _patched_ingest_execution_result(self: DashboardApp, execution: Any) -> None:
    self._run_execution_result = execution
    run_plan = getattr(execution, "run_plan", None) if execution else None
    workflow_result = getattr(execution, "workflow_result", None) if execution else None
    if not run_plan:
        return
    modules = getattr(run_plan, "modules", None)
    warning_lines = [str(warning or "").strip() for warning in (getattr(workflow_result, "warnings", []) or [])]
    warning_map: dict[str, str] = {}
    for task in getattr(run_plan, "tasks", []) or []:
        task_key = self._task_result_key(task.tag, task.serial, getattr(task, "slot_index", 1), getattr(task, "total_slots", 1))
        runtime_label = f"{task.tag}/{task_round_label(task)}"
        for warning in warning_lines:
            if runtime_label in warning or task_runtime_key(task) in warning:
                detail = warning.split(":", 1)[-1].strip() if ":" in warning else warning
                warning_map[task_key] = detail
                break
    item_map = {
        self._task_result_key(
            item.tag,
            item.serial,
            getattr(item, "slot_index", 1),
            getattr(item, "total_slots", 1),
        ): item
        for item in (getattr(workflow_result, "items", []) or [])
    }
    for task in getattr(run_plan, "tasks", []) or []:
        slot_index = getattr(task, "slot_index", 1)
        total_slots = getattr(task, "total_slots", 1)
        key = self._task_result_key(task.tag, task.serial, slot_index, total_slots)
        item = item_map.get(key)
        if getattr(modules, "render", False):
            if item and str(getattr(item, "output_video", "")).strip():
                self._mark_run_stage(task.tag, task.serial, "render", "success", slot_index=slot_index, total_slots=total_slots)
            else:
                self._mark_run_stage(task.tag, task.serial, "render", "failed", "no rendered video", slot_index=slot_index, total_slots=total_slots)
            self._run_progress_step_done(key, "render")
        elif getattr(modules, "metadata", False):
            self._run_progress_step_done(key, "render")
        if getattr(modules, "metadata", False):
            metadata_error = warning_map.get(key)
            if metadata_error:
                self._mark_run_stage(task.tag, task.serial, "metadata", "failed", metadata_error, slot_index=slot_index, total_slots=total_slots)
                if getattr(modules, "upload", False):
                    self._mark_run_stage(task.tag, task.serial, "upload", "skipped", "metadata failed", slot_index=slot_index, total_slots=total_slots)
            elif item:
                self._mark_run_stage(task.tag, task.serial, "metadata", "success", slot_index=slot_index, total_slots=total_slots)
        if getattr(modules, "upload", False) and key not in warning_map and key in item_map:
            current = self._run_result_map.get(key, {}).get("upload", {})
            if current.get("status") == "pending":
                self._mark_run_stage(task.tag, task.serial, "upload", "running", "waiting upload result", slot_index=slot_index, total_slots=total_slots)


def _patched_sync_upload_results_from_records(self: DashboardApp) -> None:
    run_plan = self._run_plan_for_summary
    if not run_plan or not getattr(getattr(run_plan, "modules", None), "upload", False):
        return
    date_mmdd = str(getattr(getattr(run_plan, "defaults", None), "date_mmdd", "")).strip()
    for task in getattr(run_plan, "tasks", []) or []:
        if int(getattr(task, "total_slots", 1) or 1) > 1:
            continue
        record = self._load_upload_record_result(date_mmdd, task.tag, task.serial)
        if not record:
            continue
        if bool(record.get("success")):
            self._mark_run_stage(task.tag, task.serial, "upload", "success", str(record.get("stage") or "upload success"))
        else:
            detail = str(record.get("failure_reason") or record.get("stage") or "upload failed")
            self._mark_run_stage(task.tag, task.serial, "upload", "failed", detail)


def _patched_update_run_status_from_log(self: DashboardApp, message: str) -> None:
    text = str(message or "").strip()
    if not text:
        return
    self.run_last_log_var.set(text[:120])
    if not self._run_started_at:
        return
    if text.startswith("[任务]"):
        match = re.search(r"\[任务\]\s+([^/]+)/([0-9]+(?:\[[0-9]+/[0-9]+\])?):", text)
        if match:
            self._run_phase = "render"
            self._run_current_item = f"{match.group(1)} / 窗口 {match.group(2)}"
            self._run_current_ratio = 0.0
    elif text.startswith("[渲染]"):
        self._run_phase = "render"
        progress_match = re.search(r"进度\s+(\d+)%", text)
        if progress_match:
            self._run_current_ratio = max(0.0, min(1.0, int(progress_match.group(1)) / 100.0))
    elif text.startswith("[清单]"):
        self._run_phase = "manifest"
    elif text.startswith("[上传]"):
        self._run_phase = "upload"
    elif text.startswith("[Round]"):
        self._run_phase = "round dispatch"

    upload_match = re.match(r"\[Upload ([^\]]+)\]\s+(.*)$", text)
    if upload_match:
        label = upload_match.group(1)
        upload_text = upload_match.group(2).strip()
        parsed = self._parse_task_label(label)
        if parsed:
            tag_name, serial_value, slot_index, total_slots = parsed
            fail_match = re.search(r"上传失败\(stage=([^)]+)\)", upload_text)
            if fail_match:
                self._mark_run_stage(tag_name, serial_value, "upload", "failed", fail_match.group(1), slot_index=slot_index, total_slots=total_slots)
                self._run_progress_step_done(label, "upload")
            elif "发布成功" in upload_text or "上传成功" in upload_text:
                self._mark_run_stage(tag_name, serial_value, "upload", "success", "upload success", slot_index=slot_index, total_slots=total_slots)
                self._run_progress_step_done(label, "upload")
                self._run_phase = "upload done"
    self._apply_run_status()


def _patched_launch_stream_upload_for_task(
    self: DashboardApp,
    run_plan,
    task: WindowTask,
    output_dir: Path,
    *,
    retain_days: str,
    auto_close: bool,
    delay_settings: dict[str, Any] | None = None,
) -> None:
    current_config = dict(run_plan.config or {})
    single_modules = build_module_selection(metadata=False, render=False, upload=True)
    single_run_plan = build_run_plan(
        tasks=[task],
        defaults=run_plan.defaults,
        modules=single_modules,
        config=current_config,
    )
    plan = deepcopy(single_run_plan.window_plan)
    plan["tasks"] = [task.to_plan_dict(1)]
    plan["groups"] = {task.tag: [int(task.serial)]}
    plan["tags"] = [task.tag]
    plan["default_tag"] = task.tag
    plan["tag_output_dirs"] = {task.tag: str(output_dir)}
    slot_suffix = f"_{int(getattr(task, 'slot_index', 1)):02d}" if int(getattr(task, "total_slots", 1) or 1) > 1 else ""
    plan_path = save_window_plan(
        plan,
        run_plan.defaults.date_mmdd,
        path=SCRIPT_DIR / "data" / f"window_upload_plan_{run_plan.defaults.date_mmdd}_{task.serial}{slot_suffix}.json",
    )
    runtime_key = task_runtime_key(task)
    self._mark_run_stage(
        task.tag,
        task.serial,
        "upload",
        "running",
        "dispatching upload process",
        slot_index=getattr(task, "slot_index", 1),
        total_slots=getattr(task, "total_slots", 1),
    )
    self._log(
        f"[Upload] {runtime_key} 使用单任务上传计划 | output={output_dir} | metadata={single_run_plan.metadata_root}"
    )
    cmd = [
        sys.executable,
        "-u",
        str(UPLOAD_SCRIPT),
        "--tag",
        task.tag,
        "--date",
        run_plan.defaults.date_mmdd,
        "--channel",
        str(task.serial),
        "--auto-confirm",
        "--window-plan-file",
        str(plan_path),
        "--retain-video-days",
        retain_days,
    ]
    if auto_close:
        cmd.append("--auto-close-browser")
    self._append_upload_delay_args(cmd, delay_settings)

    label = runtime_key
    self._log("[Upload] Stream dispatch -> " + " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        cwd=str(SCRIPT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_subprocess_utf8_env(),
    )
    with self._upload_process_lock:
        self.worker_processes.append(proc)

    def reader() -> None:
        error_text = ""
        completed_ok = False
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                self._log(f"[Upload {label}] {line.rstrip()}")
            return_code = proc.wait()
            if return_code != 0 and not self._cancel_requested:
                error_text = f"{label} exit {return_code}"
            else:
                completed_ok = not self._cancel_requested
        except Exception as exc:
            if not self._cancel_requested:
                error_text = f"{label}: {exc}"
        finally:
            with self._upload_process_lock:
                try:
                    self.worker_processes.remove(proc)
                except ValueError:
                    pass
                if error_text:
                    self._upload_failures.append(error_text)
            if error_text:
                self._post_ui_action(
                    lambda detail=error_text, task=task: (
                        self._mark_run_stage(
                            task.tag,
                            task.serial,
                            "upload",
                            "failed",
                            detail,
                            slot_index=getattr(task, "slot_index", 1),
                            total_slots=getattr(task, "total_slots", 1),
                        ),
                        self._run_progress_step_done(task_runtime_key(task), "upload"),
                    )
                )
            elif completed_ok:
                self._post_ui_action(
                    lambda task=task: (
                        self._mark_run_stage(
                            task.tag,
                            task.serial,
                            "upload",
                            "success",
                            "upload success",
                            slot_index=getattr(task, "slot_index", 1),
                            total_slots=getattr(task, "total_slots", 1),
                        ),
                        self._run_progress_step_done(task_runtime_key(task), "upload"),
                    )
                )

    thread = threading.Thread(target=reader, daemon=True)
    with self._upload_process_lock:
        self.upload_monitor_threads.append(thread)
    thread.start()


def _patched_dispatch_upload_round(
    self: DashboardApp,
    run_plan,
    *,
    retain_days: str,
    auto_close: bool,
    delay_settings: dict[str, Any] | None = None,
) -> list[str]:
    with self._upload_process_lock:
        self._upload_failures = []
    launched = 0
    output_dirs = dict(run_plan.window_plan.get("tag_output_dirs") or {})
    for task in run_plan.tasks:
        output_dir = Path(output_dirs.get(task.tag) or run_plan.output_root)
        manifest_path = output_dir / "upload_manifest.json"
        self._assert_manifest_ready_for_upload(manifest_path=manifest_path, task=task, output_dir=output_dir)
        self._log(f"[Upload] Round dispatch -> {task_runtime_key(task)} | output={output_dir}")
        self._launch_stream_upload_for_task(
            run_plan,
            task,
            output_dir,
            retain_days=retain_days,
            auto_close=auto_close,
            delay_settings=delay_settings,
        )
        launched += 1
    if launched == 0:
        return []
    return self._wait_for_stream_uploads()


def _patched_start_real_flow(self: DashboardApp) -> None:
    saved_config = self._save_paths()
    self._log(
        "[Paths] 本次运行使用: "
        f"metadata={saved_config.get('metadata_root')} | "
        f"music={saved_config.get('music_dir')} | "
        f"image={saved_config.get('base_image_dir')} | "
        f"output={saved_config.get('output_root')}"
    )
    module_selection = self._current_module_selection()
    if not module_selection.any_selected():
        messagebox.showerror("Cannot Start", "Select at least one module first.")
        return
    if not self.window_tasks:
        messagebox.showerror("Cannot Start", "Add at least one window task first.")
        return
    try:
        upload_delay_settings = self._collect_upload_delay_settings()
    except ValueError as exc:
        messagebox.showerror("Upload Delay Invalid", str(exc))
        return
    if module_selection.metadata:
        if not bool(self.generate_text_var.get()):
            self.generate_text_var.set(True)
        if not bool(self.generate_thumbnails_var.get()):
            self.generate_thumbnails_var.set(True)
        self._log("[Metadata] Quick Start 已勾选文案模块，本次强制走 API 生成标题/简介/标签，缩略图优先走图片 API。")
    self._persist_prompt_form_for_active_tasks()
    full_run_plan = self._build_current_run_plan(config=saved_config)
    self._write_run_snapshot(config=saved_config, run_plan=full_run_plan)
    self._prepare_run_result_tracking(full_run_plan)
    task_runtime_rows = [
        {
            "tag": str(task.tag or "").strip(),
            "serial": int(task.serial),
            "quantity": max(1, int(getattr(task, "quantity", 1) or 1)),
            "slot_index": int(getattr(task, "slot_index", 1) or 1),
            "total_slots": int(getattr(task, "total_slots", 1) or 1),
            "round_index": int(getattr(task, "round_index", 1) or 1),
            "source_dir": str(task.source_dir or "").strip(),
            "title": str(task.title or "").strip(),
            "thumbnails": [str(item).strip() for item in task.thumbnails if str(item).strip()],
        }
        for task in full_run_plan.tasks
    ]
    self._log(f"[Paths] Runtime tasks: {json.dumps(task_runtime_rows, ensure_ascii=False)}")
    self._log(f"[Paths] Resolved tag output dirs: {json.dumps(full_run_plan.window_plan.get('tag_output_dirs', {}), ensure_ascii=False)}")
    self._log(f"[Paths] Resolved tag metadata dirs: {json.dumps(full_run_plan.window_plan.get('tag_metadata_dirs', {}), ensure_ascii=False)}")

    round_groups: dict[int, list[WindowTask]] = {}
    for task in full_run_plan.tasks:
        round_groups.setdefault(int(getattr(task, "round_index", 1) or 1), []).append(task)
    ordered_rounds = [round_groups[idx] for idx in sorted(round_groups)]
    upload_runtime = {
        "retain_days": str(self.cleanup_days_var.get().strip() or "5"),
        "auto_close": bool(self.upload_auto_close_var.get()),
        "delay_settings": upload_delay_settings,
    }

    def job() -> bool:
        self._log(
            f"[Paths] metadata={full_run_plan.metadata_root} | music={full_run_plan.music_root} | "
            f"image={full_run_plan.image_root} | output={full_run_plan.output_root}"
        )
        seen_failures: set[str] = set()

        def collect_round_failures(round_tasks: list[WindowTask]) -> list[str]:
            failures: list[str] = []
            for round_task in round_tasks:
                key = self._task_result_key(
                    round_task.tag,
                    round_task.serial,
                    getattr(round_task, "slot_index", 1),
                    getattr(round_task, "total_slots", 1),
                )
                stages = self._run_result_map.get(key, {})
                for stage_name in ("render", "metadata", "upload"):
                    stage = stages.get(stage_name) or {}
                    if str(stage.get("status") or "") == "failed":
                        detail = str(stage.get("detail") or stage_name).strip()
                        failure_key = f"{key}:{stage_name}:{detail}"
                        if failure_key not in seen_failures:
                            seen_failures.add(failure_key)
                            failures.append(f"{key} {stage_name} failed ({detail})")
                        break
            return failures

        for round_number, round_tasks in enumerate(ordered_rounds, 1):
            if self._cancel_requested:
                return False
            round_labels = ", ".join(task_runtime_key(task) for task in round_tasks)
            self._log(f"[Round] {round_number}/{len(ordered_rounds)} -> {round_labels}")
            round_plan = build_run_plan(
                tasks=round_tasks,
                defaults=full_run_plan.defaults,
                modules=full_run_plan.modules,
                config=saved_config,
            )
            stream_upload = bool(round_plan.modules.upload and (round_plan.modules.render or round_plan.modules.metadata))
            upload_dispatched = False
            with self._upload_process_lock:
                self._upload_failures = []

            def handle_item_ready(task: WindowTask, output_dir: Path, manifest_path: Path) -> None:
                nonlocal upload_dispatched
                if not stream_upload:
                    return
                self._assert_manifest_ready_for_upload(manifest_path=manifest_path, task=task, output_dir=output_dir)
                upload_dispatched = True
                self._log(f"[Upload] Round {round_number}: {task_runtime_key(task)} 已就绪，立即上传")
                self._launch_stream_upload_for_task(
                    round_plan,
                    task,
                    output_dir,
                    retain_days=upload_runtime["retain_days"],
                    auto_close=upload_runtime["auto_close"],
                    delay_settings=upload_runtime["delay_settings"],
                )

            if round_plan.modules.render or round_plan.modules.metadata:
                execution = execute_run_plan(
                    round_plan,
                    control=self.execution_control,
                    on_item_ready=handle_item_ready if stream_upload else None,
                    log=self._log,
                )
                self._ingest_execution_result(execution)
                if stream_upload:
                    if upload_dispatched:
                        failures = self._wait_for_stream_uploads()
                        if self._cancel_requested:
                            return False
                        for failure in failures:
                            failure_key = f"{round_number}:{failure}"
                            if failure_key not in seen_failures:
                                seen_failures.add(failure_key)
                    else:
                        self._log(f"[Round] {round_number}: no ready uploads in this round")
                round_failures = collect_round_failures(round_tasks)
                if round_failures:
                    self._log("[Round] Failures -> " + " | ".join(round_failures))
                continue

            if round_plan.modules.upload:
                self._log(f"[Start] Upload round {round_number}")
                failures = self._dispatch_upload_round(
                    round_plan,
                    retain_days=upload_runtime["retain_days"],
                    auto_close=upload_runtime["auto_close"],
                    delay_settings=upload_runtime["delay_settings"],
                )
                if self._cancel_requested:
                    return False
                for failure in failures:
                    failure_key = f"{round_number}:{failure}"
                    if failure_key not in seen_failures:
                        seen_failures.add(failure_key)
                round_failures = collect_round_failures(round_tasks)
                if round_failures:
                    self._log("[Round] Failures -> " + " | ".join(round_failures))

        if self._cancel_requested:
            return False
        failures = sorted(seen_failures)
        if failures:
            raise RuntimeError(" | ".join(item.split(":", 1)[-1] for item in failures[:3]))
        return False

    task_name = " + ".join(self._selected_module_labels())
    self._run_background(
        job,
        task_name=task_name,
        total_items=len(full_run_plan.tasks),
        include_upload=bool(module_selection.upload),
    )


DashboardApp._refresh_task_tree = _patched_refresh_task_tree
DashboardApp._add_window_task = _patched_add_window_task
DashboardApp._runtime_window_tasks = _patched_runtime_window_tasks
DashboardApp._run_progress_step_done = _patched_run_progress_step_done
DashboardApp._ingest_execution_result = _patched_ingest_execution_result
DashboardApp._sync_upload_results_from_records = _patched_sync_upload_results_from_records
DashboardApp._update_run_status_from_log = _patched_update_run_status_from_log
DashboardApp._launch_stream_upload_for_task = _patched_launch_stream_upload_for_task
DashboardApp._dispatch_upload_round = _patched_dispatch_upload_round
DashboardApp._start_real_flow = _patched_start_real_flow


def main() -> int:
    app = DashboardApp()
    app.mainloop()
    return 0
