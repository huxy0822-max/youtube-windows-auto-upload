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

from content_generation import analyze_audience_screenshot, call_image_model, call_text_model
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
    load_prompt_studio_config,
    pick_api_preset_name,
    pick_content_template_name,
)
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
)

SCRIPT_DIR = Path(__file__).parent
STATE_FILE = SCRIPT_DIR / "dashboard_state.json"
UPLOAD_SCRIPT = SCRIPT_DIR / "batch_upload.py"

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


def _bool_from_yes_no(value: str) -> bool:
    return str(value).strip().lower() == "yes"


def _yes_no_from_bool(value: bool) -> str:
    return "yes" if value else "no"


def _with_random(values: list[str]) -> list[str]:
    result = list(values)
    if RANDOM_OPTION not in result:
        result.append(RANDOM_OPTION)
    return result


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
        self.randomize_effects_var = ctk.BooleanVar(value=bool(state.get("randomize_effects", True)))
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
        self.visual_spectrum_w_var = ctk.StringVar(value=str(state.get("visual_spectrum_w", visual_cfg.get("spectrum_w", 1200))))
        self.visual_film_grain_var = ctk.StringVar(value=str(state.get("visual_film_grain", visual_cfg.get("film_grain", "no"))))
        self.visual_grain_strength_var = ctk.StringVar(value=str(state.get("visual_grain_strength", visual_cfg.get("grain_strength", 15))))
        self.visual_vignette_var = ctk.StringVar(value=str(state.get("visual_vignette", visual_cfg.get("vignette", "no"))))
        self.visual_tint_var = ctk.StringVar(value=str(state.get("visual_tint", visual_cfg.get("color_tint", "none"))))
        self.visual_soft_focus_var = ctk.StringVar(value=str(state.get("visual_soft_focus", visual_cfg.get("soft_focus", "no"))))
        self.visual_soft_focus_sigma_var = ctk.StringVar(value=str(state.get("visual_soft_focus_sigma", visual_cfg.get("soft_focus_sigma", 1.5))))
        self.visual_particle_var = ctk.StringVar(value=str(state.get("visual_particle", visual_cfg.get("particle", "none"))))
        self.visual_particle_opacity_var = ctk.StringVar(value=str(state.get("visual_particle_opacity", visual_cfg.get("particle_opacity", 0.6))))
        self.visual_particle_speed_var = ctk.StringVar(value=str(state.get("visual_particle_speed", visual_cfg.get("particle_speed", 1.0))))
        self.visual_text_var = ctk.StringVar(value=str(state.get("visual_text", visual_cfg.get("text", ""))))
        self.visual_text_font_var = ctk.StringVar(value=str(state.get("visual_text_font", visual_cfg.get("text_font", "default"))))
        self.visual_text_pos_var = ctk.StringVar(value=str(state.get("visual_text_pos", visual_cfg.get("text_pos", "center"))))
        self.visual_text_size_var = ctk.StringVar(value=str(state.get("visual_text_size", visual_cfg.get("text_size", 60))))
        self.visual_text_style_var = ctk.StringVar(value=str(state.get("visual_text_style", visual_cfg.get("text_style", "Classic"))))
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

        self.music_dir_var = ctk.StringVar(value=str(self.scheduler_config.get("music_dir", "")))
        self.base_image_dir_var = ctk.StringVar(value=str(self.scheduler_config.get("base_image_dir", "")))
        self.metadata_root_var = ctk.StringVar(value=str(get_metadata_root(self.scheduler_config)))
        self.output_root_var = ctk.StringVar(value=str(self.scheduler_config.get("output_root", "")))
        self.ffmpeg_var = ctk.StringVar(value=str(self.scheduler_config.get("ffmpeg_bin", "ffmpeg")))
        self.used_media_root_var = ctk.StringVar(value=str(self.scheduler_config.get("used_media_root", "")))
        self.cleanup_days_var = ctk.StringVar(value=str(self.scheduler_config.get("render_cleanup_days", 5)))
        self.binding_group_var = ctk.StringVar(value=current_group)
        self.binding_folder_var = ctk.StringVar(value=get_group_bindings(self.scheduler_config).get(current_group, ""))

        self.prompt_group_var = ctk.StringVar(value=state.get("prompt_group", current_group))
        self.api_preset_var = ctk.StringVar(value="")
        self.content_template_var = ctk.StringVar(value="")
        self.api_save_name_var = ctk.StringVar(value="")
        self.content_save_name_var = ctk.StringVar(value="")
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
        self._run_upload_done: set[int] = set()
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
            text="???????????????????????????????",
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
        ctk.CTkLabel(add_frame, text="加入窗口时 YPP").grid(row=0, column=0, sticky="w", padx=(16, 8), pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_ypp_var, values=YES_NO_VALUES).grid(
            row=1, column=0, sticky="ew", padx=(16, 8), pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="自定义标题").grid(row=0, column=1, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkEntry(add_frame, textvariable=self.add_title_var).grid(
            row=1, column=1, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="可见性").grid(row=0, column=2, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_visibility_var, values=VISIBILITY_VALUES).grid(
            row=1, column=2, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="分类").grid(row=0, column=3, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_category_var, values=CATEGORY_VALUES).grid(
            row=1, column=3, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="儿童内容").grid(row=0, column=4, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_kids_var, values=YES_NO_VALUES).grid(
            row=1, column=4, sticky="ew", padx=8, pady=(0, 14)
        )
        ctk.CTkLabel(add_frame, text="AI 内容").grid(row=0, column=5, sticky="w", padx=8, pady=(14, 6))
        ctk.CTkOptionMenu(add_frame, variable=self.add_ai_var, values=YES_NO_VALUES).grid(
            row=1, column=5, sticky="ew", padx=(8, 16), pady=(0, 14)
        )
        ctk.CTkCheckBox(
            add_frame,
            text="??????",
            variable=self.add_notify_var,
        ).grid(row=2, column=4, sticky="w", padx=8, pady=(0, 6))
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
        for column in range(6):
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
            text="??????",
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

        columns = ("tag", "serial", "ypp", "visibility", "category", "kids", "ai", "schedule", "source")
        self.task_tree = ttk.Treeview(task_frame, columns=columns, show="headings", height=14)
        for key, width in {
            "tag": 180,
            "serial": 80,
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
        ctk.CTkLabel(intro, text="高级视觉控制", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 8)
        )
        ctk.CTkButton(intro, text="保存视觉设置", command=self._save_visual_settings).grid(
            row=0, column=1, sticky="e", padx=16, pady=(14, 8)
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
        self._entry_row(basic, 8, "频谱宽度", self.visual_spectrum_w_var)

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
        self._entry_row(mood, 4, "颗粒强度", self.visual_grain_strength_var)
        self._entry_row(mood, 5, "暗角", self.visual_vignette_var, values=VISUAL_TOGGLE_VALUES)
        self._entry_row(mood, 6, "色调", self.visual_tint_var, values=_with_random(list_tint_names()))
        self._entry_row(mood, 7, "柔焦", self.visual_soft_focus_var, values=VISUAL_TOGGLE_VALUES)
        self._entry_row(mood, 8, "柔焦强度", self.visual_soft_focus_sigma_var)

        overlay = ctk.CTkFrame(tab)
        overlay.grid(row=3, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            overlay.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(overlay, text="贴纸 / 粒子 / 叠字", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._entry_row(overlay, 1, "贴纸 / 粒子", self.visual_particle_var, values=_with_random(list_particle_effects()))
        self._entry_row(overlay, 2, "贴纸透明度", self.visual_particle_opacity_var)
        self._entry_row(overlay, 3, "贴纸速度", self.visual_particle_speed_var)
        self._entry_row(overlay, 4, "叠字内容", self.visual_text_var)
        self._entry_row(overlay, 5, "字体", self.visual_text_font_var, values=_with_random(list_font_names()))
        self._entry_row(overlay, 6, "文字位置", self.visual_text_pos_var, values=_with_random(list_text_positions()))
        self._entry_row(overlay, 7, "文字大小", self.visual_text_size_var)
        self._entry_row(overlay, 8, "文字样式", self.visual_text_style_var, values=_with_random(list_text_styles()))

        help_frame = ctk.CTkFrame(tab)
        help_frame.grid(row=4, column=0, sticky="ew", padx=8, pady=(8, 16))
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
                "可以直接输入区间，例如 1080-1600、6-18、0.35-0.75、0.85-1.15、48-72。\n"
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
                ("音乐目录", self.music_dir_var),
                ("底图目录", self.base_image_dir_var),
                ("输出目录", self.output_root_var),
                ("FFmpeg", self.ffmpeg_var),
                ("已用素材目录", self.used_media_root_var),
                ("上传后保留天数", self.cleanup_days_var),
            ],
            start=1,
        ):
            self._entry_row(path_frame, row, label, var)
        ctk.CTkButton(path_frame, text="保存路径配置", command=self._save_paths).grid(
            row=7, column=0, columnspan=4, sticky="w", padx=16, pady=(0, 14)
        )

        binding_frame = ctk.CTkFrame(tab)
        binding_frame.grid(row=1, column=0, sticky="nsew", padx=16, pady=(8, 16))
        for column in range(4):
            binding_frame.grid_columnconfigure(column, weight=1)
        binding_frame.grid_rowconfigure(5, weight=1)
        ctk.CTkLabel(binding_frame, text="分组绑定素材文件夹", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkLabel(binding_frame, text="分组").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        self.binding_group_menu = ctk.CTkOptionMenu(binding_frame, variable=self.binding_group_var, values=[""])
        self.binding_group_menu.grid(row=2, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
        ctk.CTkLabel(binding_frame, text="绑定目录").grid(row=1, column=1, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkEntry(binding_frame, textvariable=self.binding_folder_var).grid(
            row=2, column=1, columnspan=2, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkButton(binding_frame, text="选择目录", command=self._pick_binding_folder).grid(
            row=2, column=3, sticky="ew", padx=(8, 16), pady=(0, 12)
        )
        bar = ctk.CTkFrame(binding_frame, fg_color="transparent")
        bar.grid(row=3, column=0, columnspan=4, sticky="ew", padx=10, pady=(0, 8))
        ctk.CTkButton(bar, text="保存绑定", command=self._save_binding).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="移除绑定", command=self._remove_binding).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="刷新分组列表", command=self._refresh_groups).pack(side="left", padx=6)
        ctk.CTkLabel(binding_frame, text="当前绑定一览").grid(row=4, column=0, sticky="w", padx=16, pady=(0, 6))
        self.binding_box = ctk.CTkTextbox(binding_frame, height=220)
        self.binding_box.grid(row=5, column=0, columnspan=4, sticky="nsew", padx=16, pady=(0, 14))

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
    ) -> None:
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=16, pady=(0, 6))
        if values:
            widget = ctk.CTkOptionMenu(parent, variable=variable, values=values)
        else:
            widget = ctk.CTkEntry(parent, textvariable=variable, show=show or "")
        widget.grid(row=row, column=1, columnspan=3, sticky="ew", padx=16, pady=(0, 12))

    def _collect_visual_settings(self) -> dict[str, Any]:
        return {
            "spectrum": _bool_from_yes_no(self.visual_spectrum_var.get()),
            "timeline": _bool_from_yes_no(self.visual_timeline_var.get()),
            "letterbox": _bool_from_yes_no(self.visual_letterbox_var.get()),
            "zoom": self.visual_zoom_var.get().strip() or "normal",
            "style": self.visual_style_var.get().strip() or "bar",
            "color_spectrum": self.visual_color_spectrum_var.get().strip() or "WhiteGold",
            "color_timeline": self.visual_color_timeline_var.get().strip() or "WhiteGold",
            "spectrum_y": self.visual_spectrum_y_var.get().strip() or "530",
            "spectrum_x": self.visual_spectrum_x_var.get().strip() or "-1",
            "spectrum_w": self.visual_spectrum_w_var.get().strip() or "1200",
            "film_grain": _bool_from_yes_no(self.visual_film_grain_var.get()),
            "grain_strength": self.visual_grain_strength_var.get().strip() or "15",
            "vignette": _bool_from_yes_no(self.visual_vignette_var.get()),
            "color_tint": self.visual_tint_var.get().strip() or "none",
            "soft_focus": _bool_from_yes_no(self.visual_soft_focus_var.get()),
            "soft_focus_sigma": self.visual_soft_focus_sigma_var.get().strip() or "1.5",
            "particle": self.visual_particle_var.get().strip() or "none",
            "particle_opacity": self.visual_particle_opacity_var.get().strip() or "0.6",
            "particle_speed": self.visual_particle_speed_var.get().strip() or "1.0",
            "text": self.visual_text_var.get(),
            "text_font": self.visual_text_font_var.get().strip() or "default",
            "text_pos": self.visual_text_pos_var.get().strip() or "center",
            "text_size": self.visual_text_size_var.get().strip() or "60",
            "text_style": self.visual_text_style_var.get().strip() or "Classic",
        }

    def _save_visual_settings(self) -> None:
        config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
        config["visual_settings"] = self._collect_visual_settings()
        self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
        self._save_state()
        self._log("[Visual] Saved advanced visual settings")

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
            "randomize_effects": bool(self.randomize_effects_var.get()),
            "visual_spectrum": self.visual_spectrum_var.get(),
            "visual_timeline": self.visual_timeline_var.get(),
            "visual_letterbox": self.visual_letterbox_var.get(),
            "visual_zoom": self.visual_zoom_var.get(),
            "visual_style": self.visual_style_var.get(),
            "visual_color_spectrum": self.visual_color_spectrum_var.get(),
            "visual_color_timeline": self.visual_color_timeline_var.get(),
            "visual_spectrum_y": self.visual_spectrum_y_var.get(),
            "visual_spectrum_x": self.visual_spectrum_x_var.get(),
            "visual_spectrum_w": self.visual_spectrum_w_var.get(),
            "visual_film_grain": self.visual_film_grain_var.get(),
            "visual_grain_strength": self.visual_grain_strength_var.get(),
            "visual_vignette": self.visual_vignette_var.get(),
            "visual_tint": self.visual_tint_var.get(),
            "visual_soft_focus": self.visual_soft_focus_var.get(),
            "visual_soft_focus_sigma": self.visual_soft_focus_sigma_var.get(),
            "visual_particle": self.visual_particle_var.get(),
            "visual_particle_opacity": self.visual_particle_opacity_var.get(),
            "visual_particle_speed": self.visual_particle_speed_var.get(),
            "visual_text": self.visual_text_var.get(),
            "visual_text_font": self.visual_text_font_var.get(),
            "visual_text_pos": self.visual_text_pos_var.get(),
            "visual_text_size": self.visual_text_size_var.get(),
            "visual_text_style": self.visual_text_style_var.get(),
            "generate_text": bool(self.generate_text_var.get()),
            "generate_thumbnails": bool(self.generate_thumbnails_var.get()),
            "metadata_mode": "prompt_api",
            "current_group": self.current_group_var.get(),
            "source_dir_override": self.source_dir_override_var.get(),
            "add_ypp": self.add_ypp_var.get(),
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
        self.run_last_log_var.set("任务已启动，等待第一条日志")
        self._apply_run_status()

    def _finish_run_tracking(self, *, success: bool, summary: str, cancelled: bool = False) -> None:
        elapsed = time.time() - self._run_started_at if self._run_started_at else 0.0
        self.run_status_var.set("已取消" if cancelled else ("已完成" if success else "失败"))
        self.run_phase_var.set("任务结束")
        self.run_detail_var.set(summary)
        self.run_progress_var.set(f"{self._run_completed_steps}/{self._run_total_steps}")
        self.run_elapsed_var.set(_format_runtime_duration(elapsed))
        self.run_eta_var.set("00:00" if success or cancelled else "--")
        self.run_progress_bar.set(1.0 if success else self.run_progress_bar.get())
        self._run_started_at = None
        self._run_mode_label = ""
        self._run_total_items = 0
        self._run_total_steps = 0
        self._run_completed_steps = 0
        self._run_current_ratio = 0.0
        self._run_current_item = ""
        self._run_phase = "空闲"
        self._run_include_upload = False
        self.execution_control = None
        self._cancel_requested = False
        self._run_paused = False
        self.pause_button_text_var.set("暂停")
        self._run_render_done.clear()
        self._run_upload_done.clear()

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
            match = re.search(r"\[任务\]\s+([^/]+)/(\d+):", text)
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

        if "上传状态检查" in text:
            self._run_phase = "上传检查"
            status_match = re.search(r"\[(\d+)\]", text)
            if status_match:
                self._run_current_item = f"窗口 {status_match.group(1)}"
        if "开始执行标签组" in text:
            self._run_phase = "批量上传"
        serial_match = re.search(r"序号\s+(\d+)", text)
        if serial_match:
            self._run_current_item = f"窗口 {serial_match.group(1)}"
        if "发布成功" in text and serial_match:
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
            parts.append("说明: 你可以继续切换其他页面查看或改配置；新的开始任务会等当前流程结束")
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
        task = create_task(
            tag=info.tag,
            serial=info.serial,
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

    def _save_paths(self) -> None:
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

    def _persist_prompt_form_for_active_tasks(self) -> None:
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

        api_name = self.api_save_name_var.get().strip() or self.api_preset_var.get().strip() or "默认API模板"
        content_name = (
            self.content_save_name_var.get().strip()
            or self.content_template_var.get().strip()
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
        self._log(f"[提示词] 运行前已同步当前表单 -> {target_tag} | API={api_name} | 内容模板={content_name}")

    def _load_prompt_for_group(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        tag = self.prompt_group_var.get()
        api_name = pick_api_preset_name(self.prompt_config, tag)
        content_name = pick_content_template_name(self.prompt_config, tag)
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
        ensure_prompt_presets(
            api_name=self.api_preset_var.get().strip() or "默认API模板",
            api_payload=self._current_api_form(),
            content_name=self.content_template_var.get().strip() or "默认内容模板",
            content_payload=self._current_content_form(),
            tag=self.prompt_group_var.get(),
            path=PROMPT_STUDIO_FILE,
        )
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._log(f"[提示词] {self.prompt_group_var.get()} 已绑定 API 模板 {self.api_preset_var.get()}")

    def _bind_group_content(self) -> None:
        ensure_prompt_presets(
            api_name=self.api_preset_var.get().strip() or "默认API模板",
            api_payload=self._current_api_form(),
            content_name=self.content_template_var.get().strip() or "默认内容模板",
            content_payload=self._current_content_form(),
            tag=self.prompt_group_var.get(),
            path=PROMPT_STUDIO_FILE,
        )
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
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




    def _open_current_output(self) -> None:
        if self.window_tasks:
            tag = self.window_tasks[0].tag
            target = Path(self.output_root_var.get()) / f"{self.date_var.get().strip()}_{tag}"
        else:
            target = Path(self.output_root_var.get())
        if target.exists():
            os.startfile(target)


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

    def _launch_stream_upload_for_task(self, run_plan, task: WindowTask, output_dir: Path) -> None:
        plan = deepcopy(run_plan.window_plan)
        plan["tasks"] = [
            item
            for item in list(plan.get("tasks") or [])
            if str(item.get("tag") or "").strip() == task.tag and int(item.get("serial") or 0) == int(task.serial)
        ]
        plan["groups"] = {task.tag: [int(task.serial)]}
        plan["tags"] = [task.tag]
        plan["default_tag"] = task.tag
        plan["tag_output_dirs"] = {task.tag: str(output_dir)}
        plan_path = save_window_plan(
            plan,
            run_plan.defaults.date_mmdd,
            path=SCRIPT_DIR / "data" / f"window_upload_plan_{run_plan.defaults.date_mmdd}_{task.serial}.json",
        )
        retain_days = str(self.cleanup_days_var.get().strip() or "5")
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
        if self.upload_auto_close_var.get():
            cmd.append("--auto-close-browser")

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
    ) -> bool:
        plan = deepcopy(run_plan.window_plan)
        if prepared_output_dirs:
            plan["tag_output_dirs"] = dict(prepared_output_dirs)
        plan_path = save_window_plan(plan, run_plan.defaults.date_mmdd)
        tags, skip_channels = derive_tags_and_skip_channels(plan, lambda tag: get_tag_info(tag) or {})
        retain_days = str(self.cleanup_days_var.get().strip() or "5")
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
                if self.upload_auto_close_var.get():
                    per_cmd.append("--auto-close-browser")
                self._log("[Upload] " + " ".join(per_cmd))
                proc = subprocess.Popen(
                    per_cmd,
                    cwd=str(SCRIPT_DIR),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
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
        if self.upload_auto_close_var.get():
            cmd.append("--auto-close-browser")
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

    def _build_current_run_plan(self):
        return build_run_plan(
            tasks=self.window_tasks,
            defaults=self._collect_defaults(),
            modules=self._current_module_selection(),
            config=load_scheduler_settings(),
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
        tab = self.tabview.tab("路径配置")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(1, weight=1)

        path_frame = ctk.CTkFrame(tab)
        path_frame.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
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
            self._entry_row(path_frame, row, label, var)
        ctk.CTkButton(path_frame, text="保存路径配置", command=self._save_paths).grid(
            row=8, column=0, columnspan=4, sticky="w", padx=16, pady=(0, 14)
        )

        binding_frame = ctk.CTkFrame(tab)
        binding_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(8, 16))
        for column in range(4):
            binding_frame.grid_columnconfigure(column, weight=1)
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
        ctk.CTkLabel(binding_frame, text="当前绑定").grid(row=5, column=0, sticky="w", padx=16, pady=(0, 6))
        self.binding_box = ctk.CTkTextbox(binding_frame, height=220)
        self.binding_box.grid(row=6, column=0, columnspan=4, sticky="nsew", padx=16, pady=(0, 14))

    def _save_paths(self) -> None:
        config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
        config.update(
            {
                "metadata_root": self.metadata_root_var.get().strip(),
                "music_dir": self.music_dir_var.get().strip(),
                "base_image_dir": self.base_image_dir_var.get().strip(),
                "output_root": self.output_root_var.get().strip(),
                "ffmpeg_bin": self.ffmpeg_var.get().strip() or "ffmpeg",
                "ffmpeg_path": self.ffmpeg_var.get().strip() or "ffmpeg",
                "used_media_root": self.used_media_root_var.get().strip(),
                "render_cleanup_days": int(self.cleanup_days_var.get().strip() or "5"),
            }
        )
        self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
        self.metadata_root_var.set(str(get_metadata_root(self.scheduler_config)))
        self._refresh_bindings_box()
        self._log("[Paths] Saved path config")

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
            generate_text=bool(modules["metadata"] and self.generate_text_var.get()),
            generate_thumbnails=bool(modules["metadata"] and self.generate_thumbnails_var.get()),
            sync_daily_content=bool(modules["metadata"]),
            randomize_effects=bool(self.randomize_effects_var.get()),
            visual_settings=self._collect_visual_settings(),
        )

    def _preview_plan(self) -> None:
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
        module_selection = self._current_module_selection()
        if not module_selection.any_selected():
            messagebox.showerror("Validate Failed", "Select at least one module first.")
            return
        if not self.window_tasks:
            messagebox.showerror("Validate Failed", "Add at least one window task first.")
            return

        run_plan = self._build_current_run_plan()
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
        module_selection = self._current_module_selection()
        if not module_selection.render:
            messagebox.showerror("Cannot Simulate", "Simulation requires the Render module.")
            return
        if not self.window_tasks:
            messagebox.showerror("Cannot Simulate", "Add at least one window task first.")
            return

        def job() -> None:
            self._persist_prompt_form_for_active_tasks()
            run_plan = self._build_current_run_plan()
            seconds = int(self.simulate_seconds_var.get().strip() or "90")
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
        module_selection = self._current_module_selection()
        if not module_selection.any_selected():
            messagebox.showerror("Cannot Start", "Select at least one module first.")
            return
        if not self.window_tasks:
            messagebox.showerror("Cannot Start", "Add at least one window task first.")
            return

        def job() -> bool:
            self._persist_prompt_form_for_active_tasks()
            run_plan = self._build_current_run_plan()
            stream_upload = bool(run_plan.modules.render and run_plan.modules.upload)
            upload_dispatched = False

            def handle_item_ready(task: WindowTask, output_dir: Path, _manifest_path: Path) -> None:
                nonlocal upload_dispatched
                if not stream_upload:
                    return
                upload_dispatched = True
                self._log(f"[Upload] {task.tag}/{task.serial} 已完成渲染与文案，立即开始上传")
                self._launch_stream_upload_for_task(run_plan, task, output_dir)

            execution = execute_run_plan(
                run_plan,
                control=self.execution_control,
                on_item_ready=handle_item_ready if stream_upload else None,
                log=self._log,
            )

            if stream_upload and upload_dispatched:
                failures = self._wait_for_stream_uploads()
                if self._cancel_requested:
                    return False
                if failures:
                    raise RuntimeError(" | ".join(failures[:3]))
                return False

            if run_plan.modules.upload:
                self._log("[Start] Upload module")
                return self._run_upload_command(
                    run_plan,
                    detach=True,
                    prepared_output_dirs=execution.prepared_output_dirs,
                )

            return False

        task_name = " + ".join(self._selected_module_labels())
        self._run_background(
            job,
            task_name=task_name,
            total_items=len(self.window_tasks),
            include_upload=bool(module_selection.upload),
        )


    def _on_close(self) -> None:
        self._save_state()
        self.destroy()


def main() -> int:
    app = DashboardApp()
    app.mainloop()
    return 0
