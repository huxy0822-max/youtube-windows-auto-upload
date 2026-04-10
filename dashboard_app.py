# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import base64
from copy import deepcopy
import io
import json
import os
import platform
import queue
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

import customtkinter as ctk
from PIL import Image, ImageGrab
import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, simpledialog

from browser_api import list_browser_envs, set_runtime_provider, get_runtime_provider, probe_browser_providers
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
    default_api_preset,
    default_content_template,
    find_explicit_api_preset_name,
    find_explicit_content_template_name,
    pick_api_preset_name,
    pick_content_template_name,
)
from path_helpers import normalize_scheduler_config, open_path_in_file_manager
from path_templates import (
    DEFAULT_PATH_TEMPLATE_NAME,
    PATH_TEMPLATES_FILE,
    build_runtime_config,
    get_path_template,
    load_path_templates,
    normalize_path_template,
    resolve_source_dir,
    save_path_templates,
)
from run_plan_service import (
    build_module_selection,
    build_run_plan,
    execute_run_plan,
    execute_run_queue,
    execute_simulation_plan,
    preview_run_plan,
    validate_run_plan,
)
from run_queue import GroupJob, RunQueue, UploadDefaults, WindowOverride
from group_upload_workflow import normalize_mmdd
from upload_window_planner import derive_tags_and_skip_channels
from utils import get_tag_info
from workflow_core import (
    CHANNEL_MAPPING_FILE,
    ExecutionControl,
    PROMPT_STUDIO_FILE,
    SCHEDULER_CONFIG_FILE,
    WindowInfo,
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
    save_prompt_settings,
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
UPLOAD_CONFIG_FILE = SCRIPT_DIR / "config" / "upload_config.json"
VISUAL_PRESETS_FILE = SCRIPT_DIR / "config" / "visual_presets.json"

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
WINDOW_SCHEDULE_MODE_LABELS = {
    "不定时": "none",
    "使用默认规则": "default",
    "自定义": "custom",
}
WINDOW_SCHEDULE_MODE_VALUES = list(WINDOW_SCHEDULE_MODE_LABELS.keys())
WINDOW_SCHEDULE_MODE_CHOICES = {value: key for key, value in WINDOW_SCHEDULE_MODE_LABELS.items()}
QUEUE_VIDEOS_PER_WINDOW_VALUES = ["1", "2", "3", "4", "5", "6"]
WINDOW_BUTTONS_PER_ROW = 6
VISUAL_TOGGLE_VALUES = ["yes", "no", "random"]
RANDOM_OPTION = "random"
QUEUE_VISUAL_RANDOM = "随机"
QUEUE_VISUAL_MANUAL = "手动"
VISUAL_PRESET_NONE = "无预设"
VISUAL_PRESET_HINT_TEMPLATE = "当前使用预设: {name} — 取消预设后可手动编辑"


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


def _subprocess_utf8_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def _bool_from_yes_no(value: str) -> bool:
    return str(value).strip().lower() == "yes"


def _visual_toggle_value(value: str) -> bool | str:
    clean = str(value or "").strip().lower()
    if clean == RANDOM_OPTION:
        return RANDOM_OPTION
    return _bool_from_yes_no(clean)


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


def _window_schedule_mode_to_value(choice: str) -> str:
    clean_choice = str(choice or "").strip()
    return WINDOW_SCHEDULE_MODE_LABELS.get(clean_choice, clean_choice if clean_choice in WINDOW_SCHEDULE_MODE_CHOICES else "default")


def _window_schedule_mode_to_choice(value: str) -> str:
    clean_value = str(value or "").strip().lower()
    return WINDOW_SCHEDULE_MODE_CHOICES.get(clean_value, "使用默认规则")


def _resolve_window_schedule_override(
    override: WindowOverride | None,
    defaults: UploadDefaults,
    visibility: str,
) -> tuple[str, str, str, str]:
    clean_visibility = str(visibility or "").strip().lower()
    default_date = str(defaults.schedule_date or "").strip()
    default_time = str(defaults.schedule_time or "").strip()
    if clean_visibility != "schedule":
        return "none", "", "", ""

    schedule_mode = str((override.schedule_mode if override else "") or "").strip().lower()
    override_date = str((override.schedule_date if override else "") or "").strip()
    override_time = str((override.schedule_time if override else "") or "").strip()

    if not schedule_mode and (override_date or override_time):
        schedule_mode = "custom"
    if not schedule_mode:
        schedule_mode = "default"

    if schedule_mode == "none":
        return "none", "", "", ""
    if schedule_mode == "custom":
        date_value = override_date or default_date or _default_schedule_date()
        time_value = override_time or default_time or "06:00"
        return "custom", date_value, time_value, _compose_schedule_text(date_value, time_value)

    date_value = default_date or _default_schedule_date()
    time_value = default_time or "06:00"
    return "default", date_value, time_value, _compose_schedule_text(date_value, time_value)


def _format_runtime_duration(seconds: float | int | None) -> str:
    total = max(0, int(seconds or 0))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _write_json_object(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _int_or_none(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _load_group_specs_from_upload_config(
    upload_config_path: Path = UPLOAD_CONFIG_FILE,
) -> dict[str, dict[str, Any]]:
    raw_config = _load_json_object(upload_config_path)
    raw_groups = raw_config.get("tag_to_project") or {}
    group_specs: dict[str, dict[str, Any]] = {}
    for raw_tag, raw_value in raw_groups.items():
        clean_tag = str(raw_tag or "").strip()
        if not clean_tag or not isinstance(raw_value, dict):
            continue
        ypp_serials = {_int_or_none(item) for item in (raw_value.get("ypp_serials") or [])}
        non_ypp_serials = {_int_or_none(item) for item in (raw_value.get("non_ypp_serials") or [])}
        serials = sorted(
            serial
            for serial in (ypp_serials | non_ypp_serials)
            if serial is not None
        )
        group_specs[clean_tag] = {
            "serials": serials,
            "ypp_serials": {serial for serial in ypp_serials if serial is not None},
        }
    return group_specs


def _load_channel_mapping_lookup(
    channel_mapping_path: Path = CHANNEL_MAPPING_FILE,
) -> tuple[dict[str, dict[str, Any]], dict[int, dict[str, Any]]]:
    raw_mapping = _load_json_object(channel_mapping_path)
    raw_channels = raw_mapping.get("channels") or {}
    by_container: dict[str, dict[str, Any]] = {}
    by_serial: dict[int, dict[str, Any]] = {}
    for container_code, info in raw_channels.items():
        if not isinstance(info, dict):
            continue
        serial = _int_or_none(info.get("serial_number"))
        if serial is None:
            continue
        entry = {
            "serial": serial,
            "tag": str(info.get("tag") or "").strip(),
            "channel_name": str(info.get("channel_name") or "").strip(),
        }
        clean_container = str(container_code or "").strip()
        if clean_container:
            by_container[clean_container] = entry
        by_serial[serial] = entry
    return by_container, by_serial


def _build_group_catalog_from_config(
    upload_config_path: Path = UPLOAD_CONFIG_FILE,
    channel_mapping_path: Path = CHANNEL_MAPPING_FILE,
) -> tuple[dict[str, list[WindowInfo]], Exception | None]:
    group_specs = _load_group_specs_from_upload_config(upload_config_path)
    catalog: dict[str, list[WindowInfo]] = {tag: [] for tag in group_specs}
    if not group_specs:
        return catalog, None

    by_container, by_serial = _load_channel_mapping_lookup(channel_mapping_path)
    serial_to_group: dict[int, str] = {}
    for tag, spec in group_specs.items():
        for serial in spec["serials"]:
            serial_to_group.setdefault(serial, tag)

    browser_error: Exception | None = None
    try:
        live_envs = list_browser_envs(upload_config_path)
    except Exception as exc:
        browser_error = exc
        live_envs = None

    if live_envs is not None:
        for env in live_envs:
            mapped = by_container.get(str(env.get("containerCode") or "").strip())
            serial = mapped.get("serial") if mapped else _int_or_none(env.get("serialNumber"))
            if serial is None:
                continue

            target_tag = serial_to_group.get(serial)
            if not target_tag:
                mapped_tag = str((mapped or {}).get("tag") or "").strip()
                env_tag = str(env.get("tag") or "").strip()
                if mapped_tag in group_specs and serial in group_specs[mapped_tag]["serials"]:
                    target_tag = mapped_tag
                elif env_tag in group_specs and serial in group_specs[env_tag]["serials"]:
                    target_tag = env_tag
            if not target_tag:
                continue

            if any(item.serial == serial for item in catalog[target_tag]):
                continue

            fallback_entry = by_serial.get(serial) or {}
            channel_name = str(
                (mapped or {}).get("channel_name")
                or env.get("name")
                or fallback_entry.get("channel_name")
                or ""
            ).strip()
            catalog[target_tag].append(
                WindowInfo(
                    tag=target_tag,
                    serial=serial,
                    channel_name=channel_name,
                    is_ypp=serial in group_specs[target_tag]["ypp_serials"],
                )
            )

        for rows in catalog.values():
            rows.sort(key=lambda item: item.serial)
        return catalog, None

    for tag, spec in group_specs.items():
        windows: list[WindowInfo] = []
        for serial in spec["serials"]:
            fallback_entry = by_serial.get(serial) or {}
            windows.append(
                WindowInfo(
                    tag=tag,
                    serial=serial,
                    channel_name=str(fallback_entry.get("channel_name") or "").strip(),
                    is_ypp=serial in spec["ypp_serials"],
                )
            )
        catalog[tag] = windows
    return catalog, browser_error


def _build_live_group_catalog_from_browser(
    upload_config_path: Path = UPLOAD_CONFIG_FILE,
    channel_mapping_path: Path = CHANNEL_MAPPING_FILE,
) -> tuple[dict[str, list[WindowInfo]], dict[str, list[str]], Exception | None]:
    group_specs = _load_group_specs_from_upload_config(upload_config_path)
    by_container, by_serial = _load_channel_mapping_lookup(channel_mapping_path)
    try:
        live_envs = list_browser_envs()
    except Exception as exc:
        fallback_catalog, _ = _build_group_catalog_from_config(upload_config_path, channel_mapping_path)
        fallback_groups = {
            tag: [str(int(item.serial)) for item in rows]
            for tag, rows in fallback_catalog.items()
        }
        return fallback_catalog, fallback_groups, exc

    catalog: dict[str, list[WindowInfo]] = {}
    live_groups: dict[str, list[str]] = {}
    for env in live_envs:
        serial = _int_or_none(
            env.get("seq")
            or env.get("serialNumber")
            or env.get("serial_number")
            or env.get("browserSeq")
        )
        if serial is None:
            continue
        container_code = str(env.get("containerCode") or "").strip()
        mapped = by_container.get(container_code) or {}
        raw_payload = env.get("_raw") if isinstance(env.get("_raw"), dict) else {}
        group_name = str(
            raw_payload.get("groupName")
            or env.get("groupName")
            or env.get("group")
            or env.get("tag")
            or mapped.get("tag")
            or "未分组"
        ).strip() or "未分组"
        channel_name = str(
            env.get("name")
            or env.get("remark")
            or raw_payload.get("browserName")
            or mapped.get("channel_name")
            or (by_serial.get(serial) or {}).get("channel_name")
            or ""
        ).strip()
        ypp_serials = set((group_specs.get(group_name) or {}).get("ypp_serials") or set())
        info = WindowInfo(
            tag=group_name,
            serial=int(serial),
            channel_name=channel_name,
            is_ypp=int(serial) in ypp_serials,
        )
        catalog.setdefault(group_name, [])
        if not any(int(item.serial) == int(serial) for item in catalog[group_name]):
            catalog[group_name].append(info)
        live_groups.setdefault(group_name, [])
        serial_text = str(int(serial))
        if serial_text not in live_groups[group_name]:
            live_groups[group_name].append(serial_text)

    for rows in catalog.values():
        rows.sort(key=lambda item: int(item.serial))
    for group_name, serials in live_groups.items():
        live_groups[group_name] = sorted(serials, key=lambda item: int(item))
        if group_name in catalog:
            catalog[group_name].sort(key=lambda item: int(item.serial))
    if catalog:
        return catalog, live_groups, None
    fallback_catalog, _ = _build_group_catalog_from_config(upload_config_path, channel_mapping_path)
    fallback_groups = {
        tag: [str(int(item.serial)) for item in rows]
        for tag, rows in fallback_catalog.items()
    }
    return fallback_catalog, fallback_groups, RuntimeError("BitBrowser live groups unavailable")


def _load_visual_presets(path: Path = VISUAL_PRESETS_FILE) -> dict[str, dict[str, Any]]:
    raw = _load_json_object(path)
    presets: dict[str, dict[str, Any]] = {}
    for name, payload in raw.items():
        clean_name = str(name or "").strip()
        if clean_name and isinstance(payload, dict):
            presets[clean_name] = dict(payload)
    return presets


def _visual_preset_value_to_choice(value: str) -> str:
    clean_value = str(value or "").strip()
    if not clean_value or clean_value == "none":
        return VISUAL_PRESET_NONE
    return clean_value


def _visual_preset_choice_to_value(choice: str) -> str:
    clean_choice = str(choice or "").strip()
    if not clean_choice or clean_choice == VISUAL_PRESET_NONE:
        return "none"
    return clean_choice


def _visual_preset_menu_values(presets: dict[str, dict[str, Any]]) -> list[str]:
    return [VISUAL_PRESET_NONE, *sorted(presets.keys(), key=str.lower)]


def _bool_to_toggle(value: Any) -> str:
    if str(value or "").strip().lower() == RANDOM_OPTION:
        return RANDOM_OPTION
    return "yes" if bool(value) else "no"


def _visual_mode_to_label(value: str) -> str:
    clean_value = str(value or "").strip()
    if clean_value == "manual":
        return QUEUE_VISUAL_MANUAL
    if not clean_value or clean_value == "random":
        return QUEUE_VISUAL_RANDOM
    return clean_value


def _visual_mode_to_value(label: str) -> str:
    clean_label = str(label or "").strip()
    if clean_label == QUEUE_VISUAL_MANUAL:
        return "manual"
    if not clean_label or clean_label == QUEUE_VISUAL_RANDOM:
        return "random"
    return clean_label


def _suspend_windows_process(pid: int) -> None:
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


def _resume_windows_process(pid: int) -> None:
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
        self._state_lock = threading.RLock()  # 保护共享状态的线程锁
        self._upload_failures: list[str] = []
        self._run_result_map: dict[str, dict[str, dict[str, str]]] = {}
        self._run_plan_for_summary: Any = None
        self._run_execution_result: Any = None
        self._run_report_logged = False
        self._closing = False
        self._audience_data_url: str = ""
        self._state = self._load_state()
        self.window_tasks: list[WindowTask] = []
        self.visual_presets = _load_visual_presets()
        if self._state.pop("window_tasks", None) is not None:
            try:
                STATE_FILE.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as exc:
                print(f"[警告] 清理旧状态写入失败: {exc}")
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
        self.visual_preset_var = ctk.StringVar(
            value=_visual_preset_value_to_choice(str(state.get("visual_preset", visual_cfg.get("preset", "none"))))
        )
        self.visual_preset_hint_var = ctk.StringVar(value="")
        self._visual_setting_widgets: dict[str, list[Any]] = {}
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
        self.run_queue = RunQueue.from_dict(state.get("run_queue") or {})
        self.current_group_var = ctk.StringVar(value=current_group)
        self.source_dir_override_var = ctk.StringVar(value=state.get("source_dir_override", ""))
        self.queue_prompt_template_var = ctk.StringVar(value=str(state.get("queue_prompt_template", "default")))
        self.queue_api_template_var = ctk.StringVar(value=str(state.get("queue_api_template", "default")))
        self.queue_visual_mode_var = ctk.StringVar(
            value=_visual_mode_to_label(str(state.get("queue_visual_mode", "random")))
        )
        selected_serials: set[int] = set()
        for item in state.get("selected_window_serials", []):
            try:
                selected_serials.add(int(item))
            except (TypeError, ValueError):
                continue
        self._selected_window_serials = selected_serials
        self._window_button_widgets: dict[int, Any] = {}
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

        self.music_dir_var = ctk.StringVar(value=str(self.scheduler_config.get("music_dir", "")))
        self.base_image_dir_var = ctk.StringVar(value=str(self.scheduler_config.get("base_image_dir", "")))
        self.metadata_root_var = ctk.StringVar(value=str(get_metadata_root(self.scheduler_config)))
        self.output_root_var = ctk.StringVar(value=str(self.scheduler_config.get("output_root", "")))
        self.ffmpeg_var = ctk.StringVar(value=str(self.scheduler_config.get("ffmpeg_bin", "ffmpeg")))
        self.used_media_root_var = ctk.StringVar(value=str(self.scheduler_config.get("used_media_root", "")))
        self.cleanup_days_var = ctk.StringVar(value=str(self.scheduler_config.get("render_cleanup_days", 0)))
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
        self.current_group_var.trace_add("write", lambda *_: self._on_current_group_change())
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

        queue_frame = ctk.CTkFrame(tab)
        queue_frame.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        queue_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(queue_frame, text="运行队列", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 8)
        )
        ctk.CTkLabel(
            queue_frame,
            text="分组按队列顺序依次执行；每个分组内部仍按当前上传/渲染逻辑处理窗口。",
            text_color="#9fb2c8",
        ).grid(row=1, column=0, sticky="w", padx=16, pady=(0, 10))
        self.queue_list_frame = ctk.CTkScrollableFrame(queue_frame, height=170)
        self.queue_list_frame.grid(row=2, column=0, sticky="ew", padx=16, pady=(0, 14))
        self.queue_list_frame.grid_columnconfigure(0, weight=1)

        add_frame = ctk.CTkFrame(tab)
        add_frame.grid(row=1, column=0, sticky="ew", padx=16, pady=8)
        for column in range(6):
            add_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(add_frame, text="添加分组到队列", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkLabel(add_frame, text="分组").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        self.current_group_menu = ctk.CTkOptionMenu(add_frame, variable=self.current_group_var, values=[""])
        self.current_group_menu.grid(row=2, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
        ctk.CTkButton(add_frame, text="刷新分组", command=self._refresh_groups).grid(
            row=2, column=1, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkLabel(add_frame, text="素材目录").grid(row=1, column=2, sticky="w", padx=8, pady=(0, 6))
        ctk.CTkEntry(add_frame, textvariable=self.source_dir_override_var).grid(
            row=2, column=2, columnspan=3, sticky="ew", padx=8, pady=(0, 12)
        )
        ctk.CTkButton(add_frame, text="选择文件夹", command=self._pick_source_override).grid(
            row=2, column=5, sticky="ew", padx=(8, 16), pady=(0, 12)
        )

        ctk.CTkLabel(add_frame, text="提示词模板").grid(row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
        self.queue_prompt_template_menu = ctk.CTkOptionMenu(
            add_frame,
            variable=self.queue_prompt_template_var,
            values=["default"],
        )
        self.queue_prompt_template_menu.grid(row=4, column=0, columnspan=2, sticky="ew", padx=(16, 8), pady=(0, 12))
        ctk.CTkLabel(add_frame, text="API模板").grid(row=3, column=2, sticky="w", padx=8, pady=(0, 6))
        self.queue_api_template_menu = ctk.CTkOptionMenu(
            add_frame,
            variable=self.queue_api_template_var,
            values=["default"],
        )
        self.queue_api_template_menu.grid(row=4, column=2, columnspan=2, sticky="ew", padx=8, pady=(0, 12))
        ctk.CTkLabel(add_frame, text="视觉模式").grid(row=3, column=4, sticky="w", padx=8, pady=(0, 6))
        self.queue_visual_mode_menu = ctk.CTkOptionMenu(
            add_frame,
            variable=self.queue_visual_mode_var,
            values=[QUEUE_VISUAL_RANDOM, QUEUE_VISUAL_MANUAL, *self.visual_presets.keys()],
        )
        self.queue_visual_mode_menu.grid(row=4, column=4, columnspan=2, sticky="ew", padx=(8, 16), pady=(0, 12))

        window_header = ctk.CTkFrame(add_frame, fg_color="transparent")
        window_header.grid(row=5, column=0, columnspan=6, sticky="ew", padx=10, pady=(0, 4))
        window_header.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(window_header, text="窗口选择").grid(row=0, column=0, sticky="w", padx=6, pady=(0, 4))
        ctk.CTkButton(window_header, text="全选", command=self._select_all_current_group_windows, width=96).grid(
            row=0, column=1, sticky="e", padx=6, pady=(0, 4)
        )
        self.window_button_frame = ctk.CTkFrame(add_frame)
        self.window_button_frame.grid(row=6, column=0, columnspan=6, sticky="ew", padx=16, pady=(0, 10))
        ctk.CTkButton(
            add_frame,
            text="➕ 添加到队列",
            command=self._add_current_group_to_queue,
            height=40,
        ).grid(row=7, column=0, columnspan=6, sticky="ew", padx=16, pady=(0, 14))

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

        control_frame = ctk.CTkFrame(tab)
        control_frame.grid(row=3, column=0, sticky="ew", padx=16, pady=(8, 16))
        for column in range(3):
            control_frame.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(control_frame, text="控制按钮", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=3, sticky="w", padx=16, pady=(14, 12)
        )
        ctk.CTkButton(control_frame, text="▶ 开始运行", command=self._start_real_flow, height=40).grid(
            row=1, column=0, sticky="ew", padx=(16, 8), pady=(0, 14)
        )
        ctk.CTkButton(
            control_frame,
            text="⏸ 暂停",
            textvariable=self.pause_button_text_var,
            command=self._toggle_pause_current_task,
            height=40,
        ).grid(row=1, column=1, sticky="ew", padx=8, pady=(0, 14))
        ctk.CTkButton(
            control_frame,
            text="⏹ 取消当前批次",
            command=self._cancel_current_task,
            height=40,
            fg_color="#7a1f1f",
            hover_color="#932525",
        ).grid(row=1, column=2, sticky="ew", padx=(8, 16), pady=(0, 14))

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
        intro.grid_columnconfigure(3, weight=0)
        ctk.CTkLabel(intro, text="高级视觉控制", font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=16, pady=(14, 8)
        )
        ctk.CTkButton(intro, text="保存视觉设置", command=self._save_visual_settings).grid(
            row=0, column=1, sticky="e", padx=16, pady=(14, 8)
        )
        ctk.CTkButton(intro, text="套用 MEGA BASS 预设", command=self._apply_visual_preset_mega_bass).grid(
            row=0, column=2, sticky="e", padx=(0, 8), pady=(14, 8)
        )
        preset_bar = ctk.CTkFrame(intro, fg_color="transparent")
        preset_bar.grid(row=1, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 6))
        for column in range(4):
            preset_bar.grid_columnconfigure(column, weight=1 if column == 1 else 0)
        ctk.CTkLabel(preset_bar, text="视觉预设").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=0)
        self.visual_preset_menu = ctk.CTkOptionMenu(
            preset_bar,
            variable=self.visual_preset_var,
            values=_visual_preset_menu_values(self.visual_presets),
            command=lambda _value: self._on_visual_preset_change(),
        )
        self.visual_preset_menu.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=0)
        ctk.CTkButton(
            preset_bar,
            text="保存当前为预设",
            command=self._save_current_visual_preset,
        ).grid(row=0, column=2, sticky="ew", padx=(0, 8), pady=0)
        ctk.CTkButton(
            preset_bar,
            text="删除预设",
            command=self._delete_selected_visual_preset,
        ).grid(row=0, column=3, sticky="ew", pady=0)
        self.visual_preset_hint_label = ctk.CTkLabel(
            intro,
            textvariable=self.visual_preset_hint_var,
            text_color="#f7d76a",
            anchor="w",
            justify="left",
        )
        self.visual_preset_hint_label.grid(row=2, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 6))
        self.visual_preset_hint_label.grid_remove()
        ctk.CTkLabel(
            intro,
            text=(
                "这里改的是渲染特效，不影响上传规则。涉及“有没有”的开关仍按你手动勾选执行；"
                "只有你选成 random 的样式、配色、贴纸、字体和数值区间，才会按每个视频单独随机。"
            ),
            text_color="#b8c1cc",
            justify="left",
        ).grid(row=3, column=0, columnspan=4, sticky="w", padx=16, pady=(0, 14))

        basic = ctk.CTkFrame(tab)
        basic.grid(row=1, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            basic.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(basic, text="基础效果", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._remember_visual_widgets(
            "spectrum",
            self._entry_row(basic, 1, "频谱", self.visual_spectrum_var, values=VISUAL_TOGGLE_VALUES),
        )
        self._remember_visual_widgets(
            "timeline",
            self._entry_row(basic, 2, "时间轴", self.visual_timeline_var, values=VISUAL_TOGGLE_VALUES),
        )
        self._remember_visual_widgets(
            "letterbox",
            self._entry_row(basic, 3, "黑边", self.visual_letterbox_var, values=VISUAL_TOGGLE_VALUES),
        )
        self._remember_visual_widgets(
            "zoom",
            self._entry_row(basic, 4, "镜头缩放", self.visual_zoom_var, values=_with_random(list_zoom_modes())),
        )
        self._remember_visual_widgets(
            "style",
            self._entry_row(basic, 5, "频谱样式", self.visual_style_var, values=_with_random(list_effects())),
        )
        self._remember_visual_widgets(
            "spectrum_y",
            self._entry_row(basic, 6, "频谱 Y", self.visual_spectrum_y_var),
        )
        self._remember_visual_widgets(
            "spectrum_x",
            self._entry_row(basic, 7, "频谱 X (-1=居中)", self.visual_spectrum_x_var),
        )
        self._remember_visual_widgets(
            "spectrum_w",
            *self._range_row(basic, 8, "频谱宽度", self.visual_spectrum_w_min_var, self.visual_spectrum_w_max_var),
        )

        mood = ctk.CTkFrame(tab)
        mood.grid(row=2, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            mood.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(mood, text="色彩与氛围", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._remember_visual_widgets(
            "color_spectrum",
            self._entry_row(mood, 1, "频谱配色", self.visual_color_spectrum_var, values=_with_random(list_palette_names())),
        )
        self._remember_visual_widgets(
            "color_timeline",
            self._entry_row(mood, 2, "时间轴配色", self.visual_color_timeline_var, values=_with_random(list_palette_names())),
        )
        self._remember_visual_widgets(
            "film_grain",
            self._entry_row(mood, 3, "胶片颗粒", self.visual_film_grain_var, values=VISUAL_TOGGLE_VALUES),
        )
        self._remember_visual_widgets(
            "grain_strength",
            *self._range_row(
                mood,
                4,
                "颗粒强度",
                self.visual_grain_strength_min_var,
                self.visual_grain_strength_max_var,
            ),
        )
        self._remember_visual_widgets(
            "vignette",
            self._entry_row(mood, 5, "暗角", self.visual_vignette_var, values=VISUAL_TOGGLE_VALUES),
        )
        self._remember_visual_widgets(
            "color_tint",
            self._entry_row(mood, 6, "色调", self.visual_tint_var, values=_with_random(list_tint_names())),
        )
        self._remember_visual_widgets(
            "soft_focus",
            self._entry_row(mood, 7, "柔焦", self.visual_soft_focus_var, values=VISUAL_TOGGLE_VALUES),
        )
        self._remember_visual_widgets(
            "soft_focus_sigma",
            *self._range_row(
                mood,
                8,
                "柔焦强度",
                self.visual_soft_focus_sigma_min_var,
                self.visual_soft_focus_sigma_max_var,
            ),
        )

        preset = ctk.CTkFrame(tab)
        preset.grid(row=3, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            preset.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(preset, text="节奏联动 / 预设", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._remember_visual_widgets(
            "bass_pulse",
            self._entry_row(preset, 1, "低频脉冲", self.visual_bass_pulse_var, values=VISUAL_TOGGLE_VALUES),
        )
        self._remember_visual_widgets(
            "bass_pulse_scale",
            *self._range_row(
                preset,
                2,
                "脉冲缩放",
                self.visual_bass_pulse_scale_min_var,
                self.visual_bass_pulse_scale_max_var,
            ),
        )
        self._remember_visual_widgets(
            "bass_pulse_brightness",
            *self._range_row(
                preset,
                3,
                "脉冲亮度",
                self.visual_bass_pulse_brightness_min_var,
                self.visual_bass_pulse_brightness_max_var,
            ),
        )

        overlay = ctk.CTkFrame(tab)
        overlay.grid(row=4, column=0, sticky="ew", padx=8, pady=8)
        for column in range(4):
            overlay.grid_columnconfigure(column, weight=1)
        ctk.CTkLabel(overlay, text="贴纸 / 粒子 / 叠字", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
        )
        self._remember_visual_widgets(
            "particle",
            self._entry_row(
                overlay,
                1,
                "贴纸 / 粒子",
                self.visual_particle_var,
                values=_with_random_first(list_particle_effects()),
            ),
        )
        self._remember_visual_widgets(
            "particle_opacity",
            *self._range_row(
                overlay,
                2,
                "贴纸透明度",
                self.visual_particle_opacity_min_var,
                self.visual_particle_opacity_max_var,
            ),
        )
        self._remember_visual_widgets(
            "particle_speed",
            *self._range_row(
                overlay,
                3,
                "贴纸速度",
                self.visual_particle_speed_min_var,
                self.visual_particle_speed_max_var,
            ),
        )
        self._remember_visual_widgets(
            "text",
            self._entry_row(overlay, 4, "叠字内容", self.visual_text_var),
        )
        self._remember_visual_widgets(
            "text_font",
            self._entry_row(overlay, 5, "字体", self.visual_text_font_var, values=_with_random(list_font_names())),
        )
        self._remember_visual_widgets(
            "text_pos",
            self._entry_row(overlay, 6, "文字位置", self.visual_text_pos_var, values=_with_random(list_text_positions())),
        )
        self._remember_visual_widgets(
            "text_size",
            *self._range_row(overlay, 7, "文字大小", self.visual_text_size_min_var, self.visual_text_size_max_var),
        )
        self._remember_visual_widgets(
            "text_style",
            self._entry_row(overlay, 8, "文字样式", self.visual_text_style_var, values=_with_random(list_text_styles())),
        )

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
        self._refresh_visual_preset_controls()
        self._on_visual_preset_change()

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
        self.api_preset_menu = ctk.CTkOptionMenu(
            top,
            variable=self.api_preset_var,
            values=[""],
            command=lambda value: self._load_api_preset_into_form(value),
        )
        self.api_preset_menu.grid(row=2, column=1, sticky="ew", padx=8, pady=(0, 12))
        ctk.CTkLabel(top, text="内容模板").grid(row=1, column=2, sticky="w", padx=8, pady=(0, 6))
        self.content_template_menu = ctk.CTkOptionMenu(
            top,
            variable=self.content_template_var,
            values=[""],
            command=lambda value: self._load_content_template_into_form(value),
        )
        self.content_template_menu.grid(row=2, column=2, sticky="ew", padx=8, pady=(0, 12))
        ctk.CTkButton(top, text="载入当前模板", command=self._load_selected_prompt_templates).grid(
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
    ) -> Any:
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=16, pady=(0, 6))
        if values:
            widget = ctk.CTkOptionMenu(parent, variable=variable, values=values)
        else:
            widget = ctk.CTkEntry(parent, textvariable=variable, show=show or "")
        widget.grid(row=row, column=1, columnspan=3, sticky="ew", padx=16, pady=(0, 12))
        if entry_key:
            self._runtime_path_widgets[entry_key] = widget
        return widget

    def _range_row(
        self,
        parent: ctk.CTkFrame,
        row: int,
        label: str,
        min_var: ctk.StringVar,
        max_var: ctk.StringVar,
    ) -> tuple[Any, Any]:
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=16, pady=(0, 6))
        min_widget = ctk.CTkEntry(parent, textvariable=min_var, placeholder_text="最小值")
        min_widget.grid(
            row=row, column=1, sticky="ew", padx=(16, 8), pady=(0, 12)
        )
        ctk.CTkLabel(parent, text="到").grid(row=row, column=2, sticky="ew", padx=4, pady=(0, 12))
        max_widget = ctk.CTkEntry(parent, textvariable=max_var, placeholder_text="最大值")
        max_widget.grid(
            row=row, column=3, sticky="ew", padx=(8, 16), pady=(0, 12)
        )
        return min_widget, max_widget

    def _remember_visual_widgets(self, key: str, *widgets: Any) -> None:
        remembered = [widget for widget in widgets if widget is not None]
        if remembered:
            self._visual_setting_widgets[key] = remembered

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
            "preset": _visual_preset_choice_to_value(self.visual_preset_var.get()),
            "spectrum": _visual_toggle_value(self.visual_spectrum_var.get()),
            "timeline": _visual_toggle_value(self.visual_timeline_var.get()),
            "letterbox": _visual_toggle_value(self.visual_letterbox_var.get()),
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
            "film_grain": _visual_toggle_value(self.visual_film_grain_var.get()),
            "grain_strength": _compose_range_value(
                self.visual_grain_strength_min_var.get(),
                self.visual_grain_strength_max_var.get(),
                "15",
            ),
            "vignette": _visual_toggle_value(self.visual_vignette_var.get()),
            "color_tint": self.visual_tint_var.get().strip() or "none",
            "soft_focus": _visual_toggle_value(self.visual_soft_focus_var.get()),
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
            "bass_pulse": _visual_toggle_value(self.visual_bass_pulse_var.get()),
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

    def _visual_manual_control_values(self) -> dict[str, Any]:
        return self._collect_visual_settings()

    def _serialize_visual_preset(self, name: str) -> dict[str, Any]:
        settings = self._visual_manual_control_values()
        return {
            "description": f"自定义视觉预设 — {name}",
            "spectrum": str(settings.get("style") or "bar"),
            "spectrum_y": str(settings.get("spectrum_y") or "530"),
            "spectrum_x": str(settings.get("spectrum_x") or "-1"),
            "spectrum_w": str(settings.get("spectrum_w") or "1200"),
            "timeline": "on" if bool(settings.get("timeline")) else "off",
            "letterbox": "on" if bool(settings.get("letterbox")) else "off",
            "zoom": str(settings.get("zoom") or "normal"),
            "tint": str(settings.get("color_tint") or "none"),
            "particles": "bass_pulse" if bool(settings.get("bass_pulse")) else str(settings.get("particle") or ""),
            "sticker": str(settings.get("text") or ""),
            "color_spectrum": str(settings.get("color_spectrum") or "WhiteGold"),
            "color_timeline": str(settings.get("color_timeline") or "WhiteGold"),
            "film_grain": _bool_to_toggle(settings.get("film_grain")),
            "grain_strength": str(settings.get("grain_strength") or "15"),
            "vignette": _bool_to_toggle(settings.get("vignette")),
            "soft_focus": _bool_to_toggle(settings.get("soft_focus")),
            "soft_focus_sigma": str(settings.get("soft_focus_sigma") or "1.5"),
            "bass_pulse": _bool_to_toggle(settings.get("bass_pulse")),
            "bass_pulse_scale": str(settings.get("bass_pulse_scale") or "0.03"),
            "bass_pulse_brightness": str(settings.get("bass_pulse_brightness") or "0.04"),
            "text_font": str(settings.get("text_font") or "default"),
            "text_pos": str(settings.get("text_pos") or "center"),
            "text_size": str(settings.get("text_size") or "60"),
            "text_style": str(settings.get("text_style") or "Classic"),
        }

    def _visual_preset_to_settings(self, preset_name: str) -> dict[str, Any]:
        payload = dict(self.visual_presets.get(preset_name) or {})
        particle_value = str(payload.get("particles") or payload.get("particle") or "none").strip()
        bass_pulse_enabled = str(payload.get("bass_pulse") or "").strip().lower() in {"on", "yes", "true"}
        if particle_value == "bass_pulse":
            bass_pulse_enabled = True
            particle_value = "none"
        return {
            "preset": preset_name,
            "spectrum": True,
            "timeline": str(payload.get("timeline") or "on").strip().lower() not in {"off", "no", "false"},
            "letterbox": str(payload.get("letterbox") or "off").strip().lower() in {"on", "yes", "true"},
            "zoom": str(payload.get("zoom") or "normal").strip() or "normal",
            "style": str(payload.get("style") or payload.get("spectrum") or "bar").strip() or "bar",
            "color_spectrum": str(payload.get("color_spectrum") or payload.get("tint") or "WhiteGold").strip() or "WhiteGold",
            "color_timeline": str(payload.get("color_timeline") or payload.get("tint") or "WhiteGold").strip() or "WhiteGold",
            "spectrum_y": str(payload.get("spectrum_y") or "530").strip() or "530",
            "spectrum_x": str(payload.get("spectrum_x") or "-1").strip() or "-1",
            "spectrum_w": str(payload.get("spectrum_w") or "1200").strip() or "1200",
            "film_grain": str(payload.get("film_grain") or "off").strip().lower() in {"on", "yes", "true"},
            "grain_strength": str(payload.get("grain_strength") or "15").strip() or "15",
            "vignette": str(payload.get("vignette") or "off").strip().lower() in {"on", "yes", "true"},
            "color_tint": str(payload.get("tint") or payload.get("color_tint") or "none").strip() or "none",
            "soft_focus": str(payload.get("soft_focus") or "off").strip().lower() in {"on", "yes", "true"},
            "soft_focus_sigma": str(payload.get("soft_focus_sigma") or "1.5").strip() or "1.5",
            "particle": particle_value or "none",
            "particle_opacity": str(payload.get("particle_opacity") or "0.6").strip() or "0.6",
            "particle_speed": str(payload.get("particle_speed") or "1.0").strip() or "1.0",
            "bass_pulse": bass_pulse_enabled,
            "bass_pulse_scale": str(payload.get("bass_pulse_scale") or "0.03").strip() or "0.03",
            "bass_pulse_brightness": str(payload.get("bass_pulse_brightness") or "0.04").strip() or "0.04",
            "text": str(payload.get("sticker") or payload.get("text") or "").strip(),
            "text_font": str(payload.get("text_font") or "default").strip() or "default",
            "text_pos": str(payload.get("text_pos") or "center").strip() or "center",
            "text_size": str(payload.get("text_size") or "60").strip() or "60",
            "text_style": str(payload.get("text_style") or "Classic").strip() or "Classic",
            "description": str(payload.get("description") or "").strip(),
        }

    def _apply_visual_settings_to_form(self, settings: dict[str, Any], *, preset_choice: str | None = None) -> None:
        if preset_choice is not None:
            self.visual_preset_var.set(preset_choice)
        self.visual_spectrum_var.set(_bool_to_toggle(settings.get("spectrum", True)))
        self.visual_timeline_var.set(_bool_to_toggle(settings.get("timeline", True)))
        self.visual_letterbox_var.set(_bool_to_toggle(settings.get("letterbox", False)))
        self.visual_zoom_var.set(str(settings.get("zoom") or "normal"))
        self.visual_style_var.set(str(settings.get("style") or "bar"))
        self.visual_color_spectrum_var.set(str(settings.get("color_spectrum") or "WhiteGold"))
        self.visual_color_timeline_var.set(str(settings.get("color_timeline") or "WhiteGold"))
        self.visual_spectrum_y_var.set(str(settings.get("spectrum_y") or "530"))
        self.visual_spectrum_x_var.set(str(settings.get("spectrum_x") or "-1"))
        spectrum_w_min, spectrum_w_max = _split_range_value(settings.get("spectrum_w", "1200"), 1200, 1200)
        self.visual_spectrum_w_min_var.set(spectrum_w_min)
        self.visual_spectrum_w_max_var.set(spectrum_w_max)
        self.visual_film_grain_var.set(_bool_to_toggle(settings.get("film_grain", False)))
        grain_min, grain_max = _split_range_value(settings.get("grain_strength", "15"), 15, 15)
        self.visual_grain_strength_min_var.set(grain_min)
        self.visual_grain_strength_max_var.set(grain_max)
        self.visual_vignette_var.set(_bool_to_toggle(settings.get("vignette", False)))
        self.visual_tint_var.set(str(settings.get("color_tint") or "none"))
        self.visual_soft_focus_var.set(_bool_to_toggle(settings.get("soft_focus", False)))
        soft_min, soft_max = _split_range_value(settings.get("soft_focus_sigma", "1.5"), 1.5, 1.5)
        self.visual_soft_focus_sigma_min_var.set(soft_min)
        self.visual_soft_focus_sigma_max_var.set(soft_max)
        self.visual_particle_var.set(str(settings.get("particle") or "none"))
        particle_opacity_min, particle_opacity_max = _split_range_value(settings.get("particle_opacity", "0.6"), 0.6, 0.6)
        self.visual_particle_opacity_min_var.set(particle_opacity_min)
        self.visual_particle_opacity_max_var.set(particle_opacity_max)
        particle_speed_min, particle_speed_max = _split_range_value(settings.get("particle_speed", "1.0"), 1.0, 1.0)
        self.visual_particle_speed_min_var.set(particle_speed_min)
        self.visual_particle_speed_max_var.set(particle_speed_max)
        self.visual_bass_pulse_var.set(_bool_to_toggle(settings.get("bass_pulse", False)))
        bass_scale_min, bass_scale_max = _split_range_value(settings.get("bass_pulse_scale", "0.03"), 0.03, 0.03)
        self.visual_bass_pulse_scale_min_var.set(bass_scale_min)
        self.visual_bass_pulse_scale_max_var.set(bass_scale_max)
        bass_brightness_min, bass_brightness_max = _split_range_value(settings.get("bass_pulse_brightness", "0.04"), 0.04, 0.04)
        self.visual_bass_pulse_brightness_min_var.set(bass_brightness_min)
        self.visual_bass_pulse_brightness_max_var.set(bass_brightness_max)
        self.visual_text_var.set(str(settings.get("text") or ""))
        self.visual_text_font_var.set(str(settings.get("text_font") or "default"))
        self.visual_text_pos_var.set(str(settings.get("text_pos") or "center"))
        text_size_min, text_size_max = _split_range_value(settings.get("text_size", "60"), 60, 60)
        self.visual_text_size_min_var.set(text_size_min)
        self.visual_text_size_max_var.set(text_size_max)
        self.visual_text_style_var.set(str(settings.get("text_style") or "Classic"))

    def _set_visual_fields_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        for widgets in self._visual_setting_widgets.values():
            for widget in widgets:
                try:
                    widget.configure(state=state)
                except Exception:
                    pass

    def _refresh_visual_preset_controls(self) -> None:
        preset_values = _visual_preset_menu_values(self.visual_presets)
        if hasattr(self, "visual_preset_menu"):
            self.visual_preset_menu.configure(values=preset_values)
        queue_values = [QUEUE_VISUAL_RANDOM, QUEUE_VISUAL_MANUAL, *sorted(self.visual_presets.keys(), key=str.lower)]
        if hasattr(self, "queue_visual_mode_menu"):
            self.queue_visual_mode_menu.configure(values=queue_values)
        current_choice = self.visual_preset_var.get()
        if current_choice not in preset_values:
            self.visual_preset_var.set(VISUAL_PRESET_NONE)
        if hasattr(self, "queue_visual_mode_var") and self.queue_visual_mode_var.get() not in queue_values:
            self.queue_visual_mode_var.set(QUEUE_VISUAL_RANDOM)

    def _on_visual_preset_change(self, *_args: Any) -> None:
        preset_name = _visual_preset_choice_to_value(self.visual_preset_var.get())
        if preset_name == "none":
            self._set_visual_fields_enabled(True)
            self.visual_preset_hint_var.set("")
            if hasattr(self, "visual_preset_hint_label"):
                self.visual_preset_hint_label.grid_remove()
            return
        settings = self._visual_preset_to_settings(preset_name)
        self._apply_visual_settings_to_form(settings, preset_choice=_visual_preset_value_to_choice(preset_name))
        self._set_visual_fields_enabled(False)
        self.visual_preset_hint_var.set(VISUAL_PRESET_HINT_TEMPLATE.format(name=preset_name))
        if hasattr(self, "visual_preset_hint_label"):
            self.visual_preset_hint_label.grid()

    def _save_current_visual_preset(self) -> None:
        name = simpledialog.askstring("保存视觉预设", "请输入预设名称：", parent=self)
        clean_name = str(name or "").strip()
        if not clean_name:
            return
        presets = dict(self.visual_presets)
        presets[clean_name] = self._serialize_visual_preset(clean_name)
        _write_json_object(VISUAL_PRESETS_FILE, presets)
        self.visual_presets = _load_visual_presets()
        self._refresh_visual_preset_controls()
        self.visual_preset_var.set(_visual_preset_value_to_choice(clean_name))
        self._on_visual_preset_change()
        self._log(f"[Visual] 已保存视觉预设 {clean_name}")

    def _delete_selected_visual_preset(self) -> None:
        preset_name = _visual_preset_choice_to_value(self.visual_preset_var.get())
        if preset_name == "none":
            messagebox.showinfo("删除视觉预设", "当前未选择可删除的预设。")
            return
        presets = dict(self.visual_presets)
        if preset_name not in presets:
            self.visual_preset_var.set(VISUAL_PRESET_NONE)
            self._on_visual_preset_change()
            return
        del presets[preset_name]
        _write_json_object(VISUAL_PRESETS_FILE, presets)
        self.visual_presets = _load_visual_presets()
        self.visual_preset_var.set(VISUAL_PRESET_NONE)
        self._refresh_visual_preset_controls()
        self._on_visual_preset_change()
        self._log(f"[Visual] 已删除视觉预设 {preset_name}")

    def _save_visual_settings(self) -> None:
        config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
        config["visual_settings"] = self._collect_visual_settings()
        self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
        self._save_state()
        self._log("[Visual] Saved advanced visual settings")

    def _apply_visual_preset_mega_bass(self) -> None:
        self.visual_preset_var.set(_visual_preset_value_to_choice("MegaBass"))
        self._on_visual_preset_change()
        self._save_visual_settings()
        self._log("[Visual] Applied MegaBass preset")

    def _load_state(self) -> dict[str, Any]:
        if STATE_FILE.exists():
            try:
                return json.loads(STATE_FILE.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"[警告] 加载状态文件失败，使用默认值: {exc}")
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
            "visual_preset": _visual_preset_choice_to_value(self.visual_preset_var.get()),
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
            "queue_prompt_template": self.queue_prompt_template_var.get(),
            "queue_api_template": self.queue_api_template_var.get(),
            "queue_visual_mode": _visual_mode_to_value(self.queue_visual_mode_var.get()),
            "selected_window_serials": sorted(self._selected_window_serials),
            "run_queue": self.run_queue.to_dict(),
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
            "prompt_group": self.prompt_group_var.get(),
        }
        # 原子写入：先写临时文件，再原子替换
        temp_fd, temp_path = tempfile.mkstemp(
            dir=str(STATE_FILE.parent), suffix=".tmp"
        )
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            Path(temp_path).replace(STATE_FILE)
        except Exception:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
            raise

    def _drain_log_queue(self) -> None:
        # 批量处理日志 — 一次插入多条，减少GUI刷新次数
        batch: list[str] = []
        for _ in range(200):  # 每tick最多处理200条
            try:
                batch.append(self.log_queue.get_nowait())
            except queue.Empty:
                break
        if batch:
            self.log_box.insert("end", "\n".join(batch) + "\n")
            self.log_box.see("end")
            # 只对最后几条做状态解析（性能优化）
            for msg in batch[-10:]:
                self._update_run_status_from_log(msg)
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

    def _stream_process_output(self, proc: subprocess.Popen[str], label: str = "") -> None:
        """统一读取子进程输出"""
        prefix = f"[{label}] " if label else ""
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                self._log(f"{prefix}{line.rstrip()}")
        except Exception as e:
            self._log(f"{prefix}读取输出异常: {e}")

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
        with self._state_lock:
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
        with self._state_lock:
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
        with self._state_lock:
            key = self._task_result_key(tag, serial, slot_index, total_slots)
            entry = self._run_result_map.setdefault(key, {})
            stage_entry = entry.setdefault(stage, {"status": "pending", "detail": ""})
            previous_status = str(stage_entry.get("status", "pending") or "pending").strip()
            previous_detail = str(stage_entry.get("detail", "") or "").strip()
            stage_entry["status"] = status
            stage_entry["detail"] = str(detail or "").strip()
            if status in {"failed", "skipped"} and (status != previous_status or stage_entry["detail"] != previous_detail):
                stage_label = {"render": "剪辑", "metadata": "文案/封面", "upload": "上传"}.get(stage, stage)
                prefix = "失败任务" if status == "failed" else "跳过任务"
                reason = stage_entry["detail"] or ("已失败" if status == "failed" else "已跳过")
                self._log(f"[{prefix}] {key} -> {stage_label}: {reason}")

    def _failure_snapshot_text(self, limit: int = 6) -> str:
        stage_labels = {"render": "剪辑", "metadata": "文案/封面", "upload": "上传"}
        failed: list[str] = []
        with self._state_lock:
            snapshot = dict(self._run_result_map)
        for key, stages in snapshot.items():
            for stage_name in ("render", "metadata", "upload"):
                stage = stages.get(stage_name)
                if not stage:
                    continue
                status = str(stage.get("status", "pending") or "pending").strip()
                if status != "failed":
                    continue
                detail = str(stage.get("detail", "") or "").strip() or "未知原因"
                failed.append(f"{key} -> {stage_labels.get(stage_name, stage_name)}: {detail}")
                break
        if not failed:
            return ""
        if len(failed) > limit:
            shown = failed[:limit]
            shown.append(f"... 还有 {len(failed) - limit} 条失败")
            failed = shown
        return " | ".join(failed)

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

        headline = "已取消" if cancelled else f"成功 {len(success_lines)}，失败 {len(failed_lines)}，跳过 {len(skipped_lines)}"
        lines = ["[结果总结]", headline]
        if success_lines:
            lines.append("成功任务：")
            lines.extend(success_lines)
        if failed_lines:
            lines.append("失败任务：")
            lines.extend(failed_lines)
        if skipped_lines:
            lines.append("跳过任务：")
            lines.extend(skipped_lines)
        if pending_lines:
            lines.append("未完成任务：")
            lines.extend(pending_lines)
        if summary and summary not in headline:
            lines.append(f"补充说明：{summary}")
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
        with self._state_lock:
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
        failure_snapshot = self._failure_snapshot_text()
        if failure_snapshot:
            parts.append(f"失败清单: {failure_snapshot}")
        if self._has_active_background_work():
            parts.append("说明: 你可以继续切换其他页面查看或修改配置；新的开始任务会等当前流程结束。")
        return "\n".join(parts)

    def _module_names_for_new_job(self) -> list[str]:
        selected = [name for name, enabled in self._selected_modules().items() if enabled]
        return selected or ["metadata", "upload"]

    def _current_upload_defaults_model(self) -> UploadDefaults:
        timezone_text = str(self.schedule_timezone_var.get() or SCHEDULE_TIMEZONE_VALUES[0]).strip()
        timezone_name = timezone_text.split(" ", 1)[0] if " " in timezone_text else timezone_text
        return UploadDefaults(
            visibility="schedule" if self.schedule_enabled_var.get() else self.default_visibility_var.get(),
            category=self.default_category_var.get(),
            is_for_kids=_bool_from_yes_no(self.default_kids_var.get()),
            ai_content=self.default_ai_var.get(),
            altered_content=self.default_ai_var.get(),
            notify_subscribers=bool(self.default_notify_var.get()),
            schedule_date=self.schedule_date_var.get() if self.schedule_enabled_var.get() else None,
            schedule_time=self.schedule_time_var.get() if self.schedule_enabled_var.get() else None,
            timezone=timezone_name or "Asia/Taipei",
            auto_close_after=bool(self.upload_auto_close_var.get()),
        )

    def _effective_run_queue_jobs(self) -> list[GroupJob]:
        effective_jobs: list[GroupJob] = []
        current_defaults = self._current_upload_defaults_model()
        for raw_job in self.run_queue.jobs:
            job = GroupJob.from_dict(raw_job.to_dict())
            job.upload_defaults = UploadDefaults.from_dict(current_defaults.to_dict())
            if not job.modules:
                job.modules = self._module_names_for_new_job()
            effective_jobs.append(job)
        return effective_jobs

    def _find_window_info(self, tag: str, serial: int) -> WindowInfo:
        for info in self.group_catalog.get(str(tag or "").strip(), []):
            if int(info.serial) == int(serial):
                return info
        fallback_by_serial = _load_channel_mapping_lookup()[1]
        fallback_entry = fallback_by_serial.get(int(serial)) or {}
        return WindowInfo(
            tag=str(tag or "").strip(),
            serial=int(serial),
            channel_name=str(fallback_entry.get("channel_name") or "").strip(),
            is_ypp=False,
        )

    def _resolve_job_visual_settings(self, job: GroupJob) -> dict[str, Any]:
        base_settings = dict(self._collect_visual_settings())
        visual_mode = str(job.visual_mode or "random").strip() or "random"
        if visual_mode == "manual":
            if job.visual_settings:
                base_settings.update(dict(job.visual_settings))
            base_settings["preset"] = "none"
            base_settings["visual_mode"] = "manual"
            return base_settings
        if visual_mode == "random":
            base_settings["preset"] = "none"
            base_settings["visual_mode"] = "random"
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
                base_settings[key] = "random"
            return base_settings
        preset_settings = self._visual_preset_to_settings(visual_mode)
        if job.visual_settings:
            preset_settings.update(dict(job.visual_settings))
        preset_settings["preset"] = visual_mode
        preset_settings["visual_mode"] = visual_mode
        return preset_settings

    def _build_window_tasks_from_job(self, job: GroupJob) -> list[WindowTask]:
        upload_defaults = job.upload_defaults
        schedule_text = _compose_schedule_text(
            str(upload_defaults.schedule_date or "").strip(),
            str(upload_defaults.schedule_time or "").strip(),
        )
        notify_subscribers = bool(self.default_notify_var.get())
        tasks: list[WindowTask] = []
        seen_serials: set[int] = set()
        for raw_serial in job.window_serials:
            serial = int(raw_serial)
            if serial in seen_serials:
                continue
            seen_serials.add(serial)
            info = self._find_window_info(job.group_tag, serial)
            tasks.append(
                create_task(
                    tag=job.group_tag,
                    serial=serial,
                    quantity=1,
                    is_ypp=bool(info.is_ypp),
                    title="",
                    visibility=str(upload_defaults.visibility or "private").strip() or "private",
                    category=str(upload_defaults.category or "Music").strip() or "Music",
                    made_for_kids=bool(upload_defaults.is_for_kids),
                    altered_content=_bool_from_yes_no(upload_defaults.ai_content or upload_defaults.altered_content),
                    notify_subscribers=notify_subscribers,
                    scheduled_publish_at=schedule_text,
                    schedule_timezone=str(upload_defaults.timezone or "").strip(),
                    source_dir=str(job.source_dir or "").strip(),
                    channel_name=info.channel_name,
                )
            )
        return tasks

    def _sync_window_tasks_from_queue(self) -> None:
        tasks: list[WindowTask] = []
        for job in self._effective_run_queue_jobs():
            tasks.extend(self._build_window_tasks_from_job(job))
        self.window_tasks = tasks

    def _queue_template_defaults_for_group(self, tag: str) -> tuple[str, str]:
        prompt_config = self.prompt_config or {}
        api_names = list((prompt_config.get("apiPresets") or {}).keys()) or ["default"]
        content_names = list((prompt_config.get("contentTemplates") or {}).keys()) or ["default"]
        api_name = (
            find_explicit_api_preset_name(prompt_config, tag)
            or pick_api_preset_name(prompt_config, tag)
            or api_names[0]
        )
        content_name = (
            find_explicit_content_template_name(prompt_config, tag)
            or pick_content_template_name(prompt_config, tag)
            or content_names[0]
        )
        if api_name not in api_names:
            api_name = api_names[0]
        if content_name not in content_names:
            content_name = content_names[0]
        return api_name, content_name

    def _apply_current_group_context(self, *, preserve_selection: bool) -> None:
        current_group = str(self.current_group_var.get() or "").strip()
        bindings = get_group_bindings(self.scheduler_config)
        self.source_dir_override_var.set(bindings.get(current_group, ""))
        api_name, content_name = self._queue_template_defaults_for_group(current_group)
        self.queue_api_template_var.set(api_name)
        self.queue_prompt_template_var.set(content_name)
        valid_serials = {int(info.serial) for info in self.group_catalog.get(current_group, [])}
        if preserve_selection:
            self._selected_window_serials = {
                serial for serial in self._selected_window_serials if serial in valid_serials
            }
        else:
            self._selected_window_serials = set()
        self._refresh_window_buttons()

    def _on_current_group_change(self) -> None:
        try:
            self._apply_current_group_context(preserve_selection=False)
            self._save_state()
        except Exception as exc:
            print(f"[Dashboard] _on_current_group_change error: {exc}")

    def _refresh_queue_display(self) -> None:
        if not hasattr(self, "queue_list_frame"):
            return
        for child in self.queue_list_frame.winfo_children():
            child.destroy()
        if self.run_queue.is_empty():
            ctk.CTkLabel(
                self.queue_list_frame,
                text="队列为空，请在下方添加分组任务",
                text_color="#9fb2c8",
            ).pack(anchor="w", padx=10, pady=12)
            return
        for summary in self.run_queue.get_summary():
            row = ctk.CTkFrame(self.queue_list_frame)
            row.pack(fill="x", padx=4, pady=4)
            row.grid_columnconfigure(1, weight=1)
            ctk.CTkButton(
                row,
                text="✕",
                width=40,
                command=lambda index=summary["index"]: self._remove_queue_job(index),
                fg_color="#7a1f1f",
                hover_color="#932525",
            ).grid(row=0, column=0, sticky="w", padx=(10, 8), pady=8)
            source_dir = str(summary.get("source_dir") or "").strip() or "(未设置，运行时按绑定目录解析)"
            ctk.CTkLabel(
                row,
                text=(
                    f"{summary['group_tag']} ({summary['window_count']}窗口) | "
                    f"模板:{summary['prompt_template']} | 目录:{source_dir}"
                ),
                anchor="w",
                justify="left",
            ).grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=8)

    def _remove_queue_job(self, index: int) -> None:
        try:
            self.run_queue.remove_job(index)
        except IndexError:
            return
        self._refresh_task_tree()
        self._preview_plan()

    def _select_all_current_group_windows(self) -> None:
        current_group = str(self.current_group_var.get() or "").strip()
        self._selected_window_serials = {
            int(info.serial) for info in self.group_catalog.get(current_group, [])
        }
        self._refresh_window_buttons()
        self._save_state()

    def _toggle_window_selection(self, serial: int) -> None:
        clean_serial = int(serial)
        if clean_serial in self._selected_window_serials:
            self._selected_window_serials.remove(clean_serial)
        else:
            self._selected_window_serials.add(clean_serial)
        self._refresh_window_buttons()
        self._save_state()

    def _add_current_group_to_queue(self) -> None:
        current_group = str(self.current_group_var.get() or "").strip()
        if not current_group:
            messagebox.showerror("无法加入队列", "请先选择一个分组。")
            return
        selected_serials = sorted(self._selected_window_serials)
        if not selected_serials:
            messagebox.showerror("无法加入队列", "请先选择至少一个窗口。")
            return
        visual_mode = _visual_mode_to_value(self.queue_visual_mode_var.get())
        visual_settings: dict[str, Any] | None = None
        if visual_mode == "manual":
            visual_settings = dict(self._collect_visual_settings())
        elif visual_mode not in {"random", "manual"}:
            visual_settings = dict(self.visual_presets.get(visual_mode) or {})
        job = GroupJob(
            group_tag=current_group,
            window_serials=selected_serials,
            source_dir=str(self.source_dir_override_var.get() or "").strip(),
            prompt_template=str(self.queue_prompt_template_var.get() or "default").strip() or "default",
            api_template=str(self.queue_api_template_var.get() or "default").strip() or "default",
            visual_mode=visual_mode,
            visual_settings=visual_settings,
            upload_defaults=self._current_upload_defaults_model(),
            modules=self._module_names_for_new_job(),
        )
        self.run_queue.add_job(job)
        self._selected_window_serials = set()
        self._refresh_task_tree()
        self._preview_plan()

    def _refresh_groups(self) -> None:
        self.group_catalog, browser_error = _build_group_catalog_from_config()
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
        self._apply_current_group_context(preserve_selection=True)
        self._refresh_bindings_box()
        self._refresh_queue_display()
        self._save_state()
        if browser_error is not None:
            self._log(f"[分组] BitBrowser 环境列表读取失败，已回退到 upload_config/channel_mapping: {browser_error}")

    def _refresh_window_buttons(self) -> None:
        if not hasattr(self, "window_button_frame"):
            return
        for child in self.window_button_frame.winfo_children():
            child.destroy()
        self._window_button_widgets = {}
        current_group = str(self.current_group_var.get() or "").strip()
        windows = self.group_catalog.get(current_group, [])
        if not windows:
            ctk.CTkLabel(self.window_button_frame, text="当前分组没有窗口").pack(padx=12, pady=12)
            return
        selected_count = len(self._selected_window_serials)
        ctk.CTkLabel(
            self.window_button_frame,
            text=f"点击切换选中状态，当前已选 {selected_count} 个窗口",
        ).pack(anchor="w", padx=12, pady=(10, 6))
        grid = ctk.CTkFrame(self.window_button_frame, fg_color="transparent")
        grid.pack(fill="x", padx=8, pady=(0, 10))
        for column in range(WINDOW_BUTTONS_PER_ROW):
            grid.grid_columnconfigure(column, weight=1)
        for index, info in enumerate(windows):
            row_index = index // WINDOW_BUTTONS_PER_ROW
            column_index = index % WINDOW_BUTTONS_PER_ROW
            selected = int(info.serial) in self._selected_window_serials
            label = f"[{info.serial}] {info.channel_name}".strip()
            button_kwargs = {
                "text": label,
                "command": lambda serial=info.serial: self._toggle_window_selection(serial),
            }
            if selected:
                button_kwargs["fg_color"] = "#2563eb"
                button_kwargs["hover_color"] = "#1d4ed8"
            button = ctk.CTkButton(grid, **button_kwargs)
            button.grid(row=row_index, column=column_index, sticky="ew", padx=6, pady=6)
            self._window_button_widgets[int(info.serial)] = button

    def _refresh_task_tree(self) -> None:
        self._sync_window_tasks_from_queue()
        self._refresh_queue_display()
        self._save_state()

    def _refresh_bindings_box(self) -> None:
        self.binding_box.delete("1.0", "end")
        self.binding_box.insert("end", describe_group_bindings(self.scheduler_config))

    def _refresh_prompt_dropdowns(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        api_names = list((self.prompt_config.get("apiPresets") or {}).keys()) or [""]
        content_names = list((self.prompt_config.get("contentTemplates") or {}).keys()) or [""]
        queue_api_values = api_names if any(api_names) else ["default"]
        queue_content_values = content_names if any(content_names) else ["default"]
        self.api_preset_menu.configure(values=api_names)
        self.content_template_menu.configure(values=content_names)
        if hasattr(self, "queue_api_template_menu"):
            self.queue_api_template_menu.configure(values=queue_api_values)
        if hasattr(self, "queue_prompt_template_menu"):
            self.queue_prompt_template_menu.configure(values=queue_content_values)
        if self.api_preset_var.get() not in api_names:
            self.api_preset_var.set(api_names[0])
        if self.content_template_var.get() not in content_names:
            self.content_template_var.set(content_names[0])
        queue_api_name, queue_content_name = self._queue_template_defaults_for_group(self.current_group_var.get())
        if self.queue_api_template_var.get() not in queue_api_values:
            self.queue_api_template_var.set(queue_api_name if queue_api_name in queue_api_values else queue_api_values[0])
        if self.queue_prompt_template_var.get() not in queue_content_values:
            self.queue_prompt_template_var.set(
                queue_content_name if queue_content_name in queue_content_values else queue_content_values[0]
            )

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
                "render_cleanup_days": int(self.cleanup_days_var.get().strip() or "0"),
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

    def _resolve_api_preset_name(self, requested: str = "") -> str:
        api_presets = self.prompt_config.get("apiPresets") or {}
        clean = str(requested or "").strip()
        if clean in api_presets:
            return clean
        fallback = pick_api_preset_name(self.prompt_config, self.prompt_group_var.get())
        return fallback if fallback in api_presets else next(iter(api_presets), "")

    def _resolve_content_template_name(self, requested: str = "") -> str:
        content_templates = self.prompt_config.get("contentTemplates") or {}
        clean = str(requested or "").strip()
        if clean in content_templates:
            return clean
        fallback = pick_content_template_name(self.prompt_config, self.prompt_group_var.get())
        return fallback if fallback in content_templates else next(iter(content_templates), "")

    def _load_api_preset_into_form(self, name: str | None = None, *, reload_config: bool = True) -> None:
        if reload_config:
            self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        api_name = self._resolve_api_preset_name(str(name or self.api_preset_var.get() or ""))
        api_data = dict((self.prompt_config.get("apiPresets") or {}).get(api_name) or {})
        was_loading = bool(getattr(self, "_loading_prompt_form", False))
        self._loading_prompt_form = True
        try:
            if api_name:
                self.api_preset_var.set(api_name)
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
        finally:
            self._loading_prompt_form = was_loading
        if not was_loading:
            self.prompt_form_dirty = False

    def _load_content_template_into_form(self, name: str | None = None, *, reload_config: bool = True) -> None:
        if reload_config:
            self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        content_name = self._resolve_content_template_name(str(name or self.content_template_var.get() or ""))
        content_data = dict((self.prompt_config.get("contentTemplates") or {}).get(content_name) or {})
        was_loading = bool(getattr(self, "_loading_prompt_form", False))
        self._loading_prompt_form = True
        try:
            if content_name:
                self.content_template_var.set(content_name)
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
        finally:
            self._loading_prompt_form = was_loading
        if not was_loading:
            self.prompt_form_dirty = False

    def _load_prompt_for_group(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        tag = self.prompt_group_var.get()
        api_name = pick_api_preset_name(self.prompt_config, tag)
        content_name = pick_content_template_name(self.prompt_config, tag)
        self._loading_prompt_form = True
        try:
            self._load_api_preset_into_form(api_name, reload_config=False)
            self._load_content_template_into_form(content_name, reload_config=False)
        finally:
            self._loading_prompt_form = False
        self.prompt_form_dirty = False

    def _load_selected_prompt_templates(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._loading_prompt_form = True
        try:
            self._load_api_preset_into_form(self.api_preset_var.get(), reload_config=False)
            self._load_content_template_into_form(self.content_template_var.get(), reload_config=False)
        finally:
            self._loading_prompt_form = False
        self.prompt_form_dirty = False
        self._log(
            f"[提示词] 已载入当前模板: API={self.api_preset_var.get()} | "
            f"内容模板={self.content_template_var.get()}"
        )

    def _save_api_preset(self) -> None:
        tag = self.prompt_group_var.get()
        name = self.api_save_name_var.get().strip() or self.api_preset_var.get().strip()
        if not name:
            messagebox.showerror("保存失败", "请填写 API 模板名称")
            return
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        api_value = default_api_preset(name)
        api_value.update(self._current_api_form())
        api_value["name"] = name
        self.prompt_config.setdefault("apiPresets", {})[name] = api_value
        save_prompt_settings(self.prompt_config, PROMPT_STUDIO_FILE)
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
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        content_value = default_content_template(name)
        content_value.update(self._current_content_form())
        content_value["name"] = name
        self.prompt_config.setdefault("contentTemplates", {})[name] = content_value
        save_prompt_settings(self.prompt_config, PROMPT_STUDIO_FILE)
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._refresh_prompt_dropdowns()
        self.content_template_var.set(name)
        self._log(f"[提示词] 已保存内容模板: {name}")

    def _bind_group_api(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        tag = self.prompt_group_var.get().strip()
        api_name = self.api_preset_var.get().strip()
        if not tag or api_name not in (self.prompt_config.get("apiPresets") or {}):
            messagebox.showerror("绑定失败", "请选择分组和已保存的 API 模板")
            return
        self.prompt_config.setdefault("tagApiBindings", {})[tag] = api_name
        save_prompt_settings(self.prompt_config, PROMPT_STUDIO_FILE)
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._log(f"[提示词] {tag} 已绑定 API 模板 {api_name}")

    def _bind_group_content(self) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        tag = self.prompt_group_var.get().strip()
        content_name = self.content_template_var.get().strip()
        if not tag or content_name not in (self.prompt_config.get("contentTemplates") or {}):
            messagebox.showerror("绑定失败", "请选择分组和已保存的内容模板")
            return
        self.prompt_config.setdefault("tagBindings", {})[tag] = content_name
        save_prompt_settings(self.prompt_config, PROMPT_STUDIO_FILE)
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        self._log(f"[提示词] {tag} 已绑定内容模板 {content_name}")

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
                self._stream_process_output(proc, f"Upload {label}")
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
    ) -> bool:
        plan = deepcopy(run_plan.window_plan)
        if prepared_output_dirs:
            plan["tag_output_dirs"] = dict(prepared_output_dirs)
        plan_path = save_window_plan(plan, run_plan.defaults.date_mmdd)
        tags, skip_channels = derive_tags_and_skip_channels(plan, lambda tag: get_tag_info(tag) or {})
        retain_days = str(retain_days or "5")
        auto_close = bool(auto_close)
        task_output_dirs = dict(plan.get("task_output_dirs") or {})
        ordered_targets: list[WindowTask] = []
        seen_targets: set[str] = set()
        for task in run_plan.tasks:
            runtime_key = task_runtime_key(task)
            if runtime_key in seen_targets:
                continue
            seen_targets.add(runtime_key)
            ordered_targets.append(task)

        if len(ordered_targets) > 1 and detach:
            processes: list[tuple[str, subprocess.Popen[str]]] = []
            for task in ordered_targets:
                runtime_key = task_runtime_key(task)
                output_dir = Path(
                    task_output_dirs.get(runtime_key)
                    or plan.get("tag_output_dirs", {}).get(task.tag)
                    or run_plan.output_root
                )
                single_modules = build_module_selection(metadata=False, render=False, upload=True)
                single_run_plan = build_run_plan(
                    tasks=[task],
                    defaults=run_plan.defaults,
                    modules=single_modules,
                    config=dict(run_plan.config or {}),
                )
                single_plan = deepcopy(single_run_plan.window_plan)
                single_plan["tasks"] = [task.to_plan_dict(1)]
                single_plan["groups"] = {task.tag: [int(task.serial)]}
                single_plan["tags"] = [task.tag]
                single_plan["default_tag"] = task.tag
                single_plan["tag_output_dirs"] = {task.tag: str(output_dir)}
                single_plan["task_output_dirs"] = {runtime_key: str(output_dir)}
                slot_suffix = (
                    f"_{int(getattr(task, 'slot_index', 1)):02d}"
                    if int(getattr(task, "total_slots", 1) or 1) > 1
                    else ""
                )
                single_plan_path = save_window_plan(
                    single_plan,
                    run_plan.defaults.date_mmdd,
                    path=SCRIPT_DIR
                    / "data"
                    / f"window_upload_plan_{run_plan.defaults.date_mmdd}_{task.serial}{slot_suffix}.json",
                )
                per_cmd = [
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
                    str(single_plan_path),
                    "--retain-video-days",
                    retain_days,
                ]
                if auto_close:
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
                    env=_subprocess_utf8_env(),
                )
                processes.append((runtime_key, proc))

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
                        self._stream_process_output(proc, f"Upload {label}")
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
                "render_cleanup_days": int(self._runtime_field_text("render_cleanup_days", self.cleanup_days_var) or "0"),
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
        runtime_tasks: list[WindowTask] = []
        for task in self.window_tasks:
            cloned = create_task(
                tag=task.tag,
                serial=task.serial,
                quantity=task.quantity,
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
                slot_index=getattr(task, "slot_index", 1),
                total_slots=getattr(task, "total_slots", 1),
                round_index=getattr(task, "round_index", 1),
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

    def _module_selection_for_job(self, job: GroupJob):
        flags = {"metadata": False, "render": False, "upload": False}
        for module_name in job.modules:
            clean_name = str(module_name or "").strip()
            if clean_name in flags:
                flags[clean_name] = True
        if not any(flags.values()):
            flags = self._selected_modules()
        return build_module_selection(
            metadata=flags["metadata"],
            render=flags["render"],
            upload=flags["upload"],
        )

    def _workflow_defaults_for_job(self, job: GroupJob) -> WorkflowDefaults:
        upload_defaults = job.upload_defaults
        schedule_text = _compose_schedule_text(
            str(upload_defaults.schedule_date or "").strip(),
            str(upload_defaults.schedule_time or "").strip(),
        )
        module_selection = self._module_selection_for_job(job)
        return WorkflowDefaults(
            date_mmdd=normalize_mmdd(self.date_var.get().strip() or _today_mmdd()),
            visibility=str(upload_defaults.visibility or "private").strip() or "private",
            category=str(upload_defaults.category or "Music").strip() or "Music",
            made_for_kids=bool(upload_defaults.is_for_kids),
            altered_content=_bool_from_yes_no(upload_defaults.ai_content or upload_defaults.altered_content),
            notify_subscribers=bool(self.default_notify_var.get()),
            schedule_enabled=bool(schedule_text and str(upload_defaults.visibility or "").strip() == "schedule"),
            schedule_start=schedule_text,
            schedule_interval_minutes=int(self.schedule_interval_var.get().strip() or "60"),
            schedule_timezone=str(upload_defaults.timezone or "Asia/Taipei").strip() or "Asia/Taipei",
            metadata_mode="prompt_api",
            generate_text=bool(module_selection.metadata),
            generate_thumbnails=bool(module_selection.metadata),
            sync_daily_content=bool(module_selection.metadata),
            randomize_effects=False,
            visual_settings=self._resolve_job_visual_settings(job),
        )

    def _apply_job_prompt_bindings(self, job: GroupJob) -> None:
        self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        api_presets = self.prompt_config.get("apiPresets") or {}
        content_templates = self.prompt_config.get("contentTemplates") or {}
        api_name, content_name = self._queue_template_defaults_for_group(job.group_tag)
        requested_api = str(job.api_template or getattr(job, "api_preset", "") or "").strip()
        requested_content = str(job.prompt_template or "").strip()
        if requested_api and requested_api != "default" and requested_api in api_presets:
            api_name = requested_api
        if requested_content and requested_content != "default" and requested_content in content_templates:
            content_name = requested_content
        api_payload = dict(api_presets.get(api_name) or {})
        content_payload = dict(content_templates.get(content_name) or {})
        if api_payload or content_payload:
            ensure_prompt_presets(
                api_name=api_name,
                api_payload=api_payload,
                content_name=content_name,
                content_payload=content_payload,
                tag=job.group_tag,
                path=PROMPT_STUDIO_FILE,
            )
            self.prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)

    def _build_run_plan_for_job(self, job: GroupJob, *, config: dict[str, Any] | None = None):
        runtime_config = config or self._sync_runtime_paths(persist=False)
        tasks = self._build_window_tasks_from_job(job)
        return build_run_plan(
            tasks=tasks,
            defaults=self._workflow_defaults_for_job(job),
            modules=self._module_selection_for_job(job),
            config=runtime_config,
        )

    def _build_tracking_plan_for_queue(self, *, config: dict[str, Any] | None = None):
        runtime_config = config or self._sync_runtime_paths(persist=False)
        effective_jobs = self._effective_run_queue_jobs()
        tracking_tasks: list[WindowTask] = []
        combined_flags = {"metadata": False, "render": False, "upload": False}
        for job in effective_jobs:
            tracking_tasks.extend(self._build_window_tasks_from_job(job))
            for module_name in job.modules:
                clean_name = str(module_name or "").strip()
                if clean_name in combined_flags:
                    combined_flags[clean_name] = True
        modules = build_module_selection(
            metadata=combined_flags["metadata"],
            render=combined_flags["render"],
            upload=combined_flags["upload"],
        )
        defaults = self._collect_defaults()
        if effective_jobs:
            defaults.visual_settings = self._resolve_job_visual_settings(effective_jobs[0])
        return build_run_plan(
            tasks=tracking_tasks,
            defaults=defaults,
            modules=modules,
            config=runtime_config,
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
                "render_cleanup_days": int(self._runtime_field_text("render_cleanup_days", self.cleanup_days_var) or "0"),
            }
        )
        self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
        self.metadata_root_var.set(str(get_metadata_root(self.scheduler_config)))
        self.music_dir_var.set(str(self.scheduler_config.get("music_dir", "")))
        self.base_image_dir_var.set(str(self.scheduler_config.get("base_image_dir", "")))
        self.output_root_var.set(str(self.scheduler_config.get("output_root", "")))
        self.ffmpeg_var.set(str(self.scheduler_config.get("ffmpeg_bin", "ffmpeg")))
        self.used_media_root_var.set(str(self.scheduler_config.get("used_media_root", "")))
        self.cleanup_days_var.set(str(self.scheduler_config.get("render_cleanup_days", 0)))
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
                )

            return False

        task_name = " + ".join(self._selected_module_labels())
        self._run_background(
            job,
            task_name=task_name,
            total_items=len(self.window_tasks),
            include_upload=bool(module_selection.upload),
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

        option_frame = ctk.CTkFrame(tab)
        option_frame.grid(row=1, column=0, sticky="ew", padx=16, pady=8)
        option_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            option_frame,
            text="文案模块只写标题、简介、标签和缩略图到文案输出目录。剪辑模块只生成成品视频。上传模块直接读取上面配置好的目录，如果缺文件会直接报错。",
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
        self._closing = True
        self._save_state()
        # 清理上传监控线程
        for thread in getattr(self, 'upload_monitor_threads', []):
            if thread.is_alive():
                thread.join(timeout=2)
        # 终止所有子进程
        for proc in getattr(self, 'worker_processes', []):
            try:
                proc.terminate()
            except Exception:
                pass
        self.destroy()


def _patched_refresh_task_tree(self: DashboardApp) -> None:
    self._sync_window_tasks_from_queue()
    self._refresh_queue_display()
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
    runtime_tasks: list[WindowTask] = []
    for task in self.window_tasks:
        cloned = create_task(
            tag=task.tag,
            serial=task.serial,
            quantity=getattr(task, "quantity", 1),
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
            slot_index=getattr(task, "slot_index", 1),
            total_slots=getattr(task, "total_slots", 1),
            round_index=getattr(task, "round_index", 1),
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
) -> None:
    current_config = dict(run_plan.config or {})
    single_modules = build_module_selection(metadata=False, render=False, upload=True)
    single_run_plan = build_run_plan(
        tasks=[task],
        defaults=run_plan.defaults,
        modules=single_modules,
        config=current_config,
    )
    runtime_key = task_runtime_key(task)
    plan = deepcopy(single_run_plan.window_plan)
    plan["tasks"] = [task.to_plan_dict(1)]
    plan["groups"] = {task.tag: [int(task.serial)]}
    plan["tags"] = [task.tag]
    plan["default_tag"] = task.tag
    plan["tag_output_dirs"] = {task.tag: str(output_dir)}
    plan["task_output_dirs"] = {runtime_key: str(output_dir)}
    slot_suffix = f"_{int(getattr(task, 'slot_index', 1)):02d}" if int(getattr(task, "total_slots", 1) or 1) > 1 else ""
    plan_path = save_window_plan(
        plan,
        run_plan.defaults.date_mmdd,
        path=SCRIPT_DIR / "data" / f"window_upload_plan_{run_plan.defaults.date_mmdd}_{task.serial}{slot_suffix}.json",
    )
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
            self._stream_process_output(proc, f"Upload {label}")
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
) -> list[str]:
        with self._upload_process_lock:
            self._upload_failures = []
        launched = 0
        output_dirs = dict(run_plan.window_plan.get("tag_output_dirs") or {})
        task_output_dirs = dict(run_plan.window_plan.get("task_output_dirs") or {})
        for task in run_plan.tasks:
            runtime_key = task_runtime_key(task)
            output_dir = Path(task_output_dirs.get(runtime_key) or output_dirs.get(task.tag) or run_plan.output_root)
            manifest_path = output_dir / "upload_manifest.json"
            self._assert_manifest_ready_for_upload(manifest_path=manifest_path, task=task, output_dir=output_dir)
            self._log(f"[Upload] Round dispatch -> {runtime_key} | output={output_dir}")
            self._launch_stream_upload_for_task(
                run_plan,
                task,
                output_dir,
                retain_days=retain_days,
                auto_close=auto_close,
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
    if self.run_queue.is_empty():
        messagebox.showerror("Cannot Start", "Run queue is empty. Add at least one group job first.")
        return
    self._refresh_task_tree()
    if not self.window_tasks:
        messagebox.showerror("Cannot Start", "Run queue has no valid window tasks.")
        return
    effective_jobs = self._effective_run_queue_jobs()
    tracking_plan = self._build_tracking_plan_for_queue(config=saved_config)
    if not tracking_plan.modules.any_selected():
        messagebox.showerror("Cannot Start", "Select at least one module before adding jobs to the queue.")
        return
    if tracking_plan.modules.metadata:
        if not bool(self.generate_text_var.get()):
            self.generate_text_var.set(True)
        if not bool(self.generate_thumbnails_var.get()):
            self.generate_thumbnails_var.set(True)
        self._log("[Metadata] Quick Start 已勾选文案模块，本次强制走 API 生成标题/简介/标签，缩略图优先走图片 API。")
    self._persist_prompt_form_for_active_tasks()
    self._write_run_snapshot(config=saved_config, run_plan=tracking_plan)
    self._prepare_run_result_tracking(tracking_plan)
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
        for task in tracking_plan.tasks
    ]
    self._log(f"[Paths] Runtime tasks: {json.dumps(task_runtime_rows, ensure_ascii=False)}")
    self._log(f"[Paths] Resolved tag output dirs: {json.dumps(tracking_plan.window_plan.get('tag_output_dirs', {}), ensure_ascii=False)}")
    self._log(f"[Paths] Resolved tag metadata dirs: {json.dumps(tracking_plan.window_plan.get('tag_metadata_dirs', {}), ensure_ascii=False)}")

    def job() -> bool:
        self._log(
            f"[Paths] metadata={tracking_plan.metadata_root} | music={tracking_plan.music_root} | "
            f"image={tracking_plan.image_root} | output={tracking_plan.output_root}"
        )
        seen_failures: set[str] = set()

        def collect_job_failures(job_run_plan) -> list[str]:
            failures: list[str] = []
            for round_task in getattr(job_run_plan, "tasks", []) or []:
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

        for queue_index, queue_job in enumerate(effective_jobs, 1):
            if self._cancel_requested:
                return False
            self._apply_job_prompt_bindings(queue_job)
            job_run_plan = self._build_run_plan_for_job(queue_job, config=saved_config)
            self._log(
                f"[Queue] {queue_index}/{len(effective_jobs)} -> {queue_job.group_tag} | "
                f"windows={queue_job.window_serials} | prompt={queue_job.prompt_template} | "
                f"api={queue_job.api_template} | visual={queue_job.visual_mode}"
            )
            stream_upload = bool(
                job_run_plan.modules.upload and (job_run_plan.modules.render or job_run_plan.modules.metadata)
            )
            upload_dispatched = False
            with self._upload_process_lock:
                self._upload_failures = []
            upload_runtime = {
                "retain_days": str(self.cleanup_days_var.get().strip() or "5"),
                "auto_close": bool(queue_job.upload_defaults.auto_close_after),
            }

            def handle_item_ready(task: WindowTask, output_dir: Path, manifest_path: Path) -> None:
                nonlocal upload_dispatched
                if not stream_upload:
                    return
                self._assert_manifest_ready_for_upload(manifest_path=manifest_path, task=task, output_dir=output_dir)
                upload_dispatched = True
                self._log(
                    f"[Upload] Queue {queue_index}/{len(effective_jobs)}: "
                    f"{task_runtime_key(task)} 已就绪，立即上传"
                )
                self._launch_stream_upload_for_task(
                    job_run_plan,
                    task,
                    output_dir,
                    retain_days=upload_runtime["retain_days"],
                    auto_close=upload_runtime["auto_close"],
                )

            if job_run_plan.modules.render or job_run_plan.modules.metadata:
                execution = execute_run_plan(
                    job_run_plan,
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
                            failure_key = f"{queue_index}:{failure}"
                            if failure_key not in seen_failures:
                                seen_failures.add(failure_key)
                    else:
                        self._log(f"[Queue] {queue_index}/{len(effective_jobs)}: no ready uploads in this group")
                job_failures = collect_job_failures(job_run_plan)
                if job_failures:
                    self._log("[Queue] Failures -> " + " | ".join(job_failures))
                continue

            if job_run_plan.modules.upload:
                self._log(f"[Queue] {queue_index}/{len(effective_jobs)} 开始上传分组 {queue_job.group_tag}")
                failures = self._dispatch_upload_round(
                    job_run_plan,
                    retain_days=upload_runtime["retain_days"],
                    auto_close=upload_runtime["auto_close"],
                )
                if self._cancel_requested:
                    return False
                for failure in failures:
                    failure_key = f"{queue_index}:{failure}"
                    if failure_key not in seen_failures:
                        seen_failures.add(failure_key)
                job_failures = collect_job_failures(job_run_plan)
                if job_failures:
                    self._log("[Queue] Failures -> " + " | ".join(job_failures))

        if self._cancel_requested:
            return False
        failures = sorted(seen_failures)
        if failures:
            raise RuntimeError(" | ".join(item.split(":", 1)[-1] for item in failures[:3]))
        return False

    task_name = " + ".join(tracking_plan.modules.labels()) or "RunQueue"
    self._run_background(
        job,
        task_name=task_name,
        total_items=len(tracking_plan.tasks),
        include_upload=bool(tracking_plan.modules.upload),
    )


def _patched_start_real_flow(self: DashboardApp) -> None:
    saved_config = self._save_paths()
    self._log(
        "[Paths] 本次运行使用: "
        f"metadata={saved_config.get('metadata_root')} | "
        f"music={saved_config.get('music_dir')} | "
        f"image={saved_config.get('base_image_dir')} | "
        f"output={saved_config.get('output_root')}"
    )
    if self.run_queue.is_empty():
        messagebox.showerror("Cannot Start", "Run queue is empty. Add at least one group job first.")
        return
    self._refresh_task_tree()
    if not self.window_tasks:
        messagebox.showerror("Cannot Start", "Run queue has no valid window tasks.")
        return

    queue_defaults = self._current_upload_defaults_model()
    tracking_plan = self._build_tracking_plan_for_queue(config=saved_config)
    if not tracking_plan.modules.any_selected():
        messagebox.showerror("Cannot Start", "Select at least one module before adding jobs to the queue.")
        return
    if tracking_plan.modules.metadata:
        if not bool(self.generate_text_var.get()):
            self.generate_text_var.set(True)
        if not bool(self.generate_thumbnails_var.get()):
            self.generate_thumbnails_var.set(True)
        self._log("[Metadata] Quick Start 已选中文案模块，本次强制走 API 生成标题/简介/标签。")

    self._persist_prompt_form_for_active_tasks()
    self._write_run_snapshot(config=saved_config, run_plan=tracking_plan)
    self._prepare_run_result_tracking(tracking_plan)

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
        for task in tracking_plan.tasks
    ]
    self._log(f"[Paths] Runtime tasks: {json.dumps(task_runtime_rows, ensure_ascii=False)}")
    self._log(f"[Paths] Resolved tag output dirs: {json.dumps(tracking_plan.window_plan.get('tag_output_dirs', {}), ensure_ascii=False)}")
    self._log(f"[Paths] Resolved tag metadata dirs: {json.dumps(tracking_plan.window_plan.get('tag_metadata_dirs', {}), ensure_ascii=False)}")

    def handle_queue_progress(event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip()
        if not event_type:
            return
        if event_type == "log":
            message = str(event.get("message") or "").strip()
            if message:
                self._log(message)
            return

        group_tag = str(event.get("group_tag") or "").strip()
        label = str(event.get("label") or "").strip()
        serial_value = int(event.get("serial") or 0)

        if event_type == "job_started":
            self._run_phase = f"queue {event.get('job_index', 0)}/{event.get('job_total', 0)}"
            self._run_current_item = f"{group_tag} | {int(event.get('window_count') or 0)}窗口"
            self._run_current_ratio = 0.0
            self._log(
                f"[Queue] {event.get('job_index', 0)}/{event.get('job_total', 0)} -> "
                f"{group_tag} | windows={event.get('window_serials', [])}"
            )
            return
        if event_type == "prepare_started":
            self._run_phase = "prepare"
            self._run_current_item = f"{group_tag} | generating metadata/render"
            self._run_current_ratio = 0.0
            return
        if event_type == "prepare_finished":
            self._run_phase = "prepare done"
            self._run_current_item = f"{group_tag} | ready for upload"
            self._run_current_ratio = 0.0
            return
        if event_type == "window_started":
            if label and not bool(event.get("has_prepare_step", True)):
                self._run_progress_step_done(label, "render")
            if group_tag and serial_value:
                self._mark_run_stage(group_tag, serial_value, "upload", "running", "uploading")
            self._run_phase = "upload"
            self._run_current_item = f"{group_tag} / 窗口 {serial_value}"
            self._run_current_ratio = 0.0
            return
        if event_type == "window_finished":
            detail = str(event.get("detail") or event.get("stage") or "").strip()
            if group_tag and serial_value:
                self._mark_run_stage(
                    group_tag,
                    serial_value,
                    "upload",
                    "success" if bool(event.get("success")) else "failed",
                    detail,
                )
            if label:
                self._run_progress_step_done(label, "upload")
            self._run_phase = "upload done" if bool(event.get("success")) else "upload failed"
            self._run_current_item = f"{group_tag} / 窗口 {serial_value} | {detail or 'done'}"
            self._run_current_ratio = 0.0
            return
        if event_type == "group_finished":
            self._run_phase = "group done"
            self._run_current_item = (
                f"{group_tag} | success={int(event.get('success_count') or 0)} | "
                f"failed={int(event.get('failed_count') or 0)}"
            )
            return
        if event_type == "job_error":
            self._run_phase = "job error"
            self._run_current_item = f"{group_tag} | {str(event.get('detail') or '').strip()}"

    def job() -> bool:
        self._log(
            f"[Paths] metadata={tracking_plan.metadata_root} | music={tracking_plan.music_root} | "
            f"image={tracking_plan.image_root} | output={tracking_plan.output_root}"
        )
        queue_results = asyncio.run(
            execute_run_queue(
                self.run_queue,
                queue_defaults,
                control=self.execution_control,
                before_job_callback=self._apply_job_prompt_bindings,
                build_run_plan_for_job=lambda queue_job: self._build_run_plan_for_job(queue_job, config=saved_config),
                execution_result_callback=lambda _job, execution: self._ingest_execution_result(execution),
                progress_callback=handle_queue_progress,
                log=self._log,
            )
        )
        if self._cancel_requested:
            return False
        failures: list[str] = []
        for job_result in queue_results:
            for item in job_result.get("results", []) or []:
                if bool(item.get("success")):
                    continue
                detail = str(item.get("detail") or item.get("stage") or "upload failed").strip()
                if detail and detail not in failures:
                    failures.append(detail)
        if failures:
            raise RuntimeError(" | ".join(failures[:3]))
        return False

    task_name = " + ".join(tracking_plan.modules.labels()) or "RunQueue"
    self._run_background(
        job,
        task_name=task_name,
        total_items=len(tracking_plan.tasks),
        include_upload=bool(tracking_plan.modules.upload and (tracking_plan.modules.metadata or tracking_plan.modules.render)),
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


QUEUE_VISUAL_RANDOM = "随机"
QUEUE_VISUAL_MANUAL = "手动"
VISUAL_PRESET_NONE = "无预设"
VISUAL_PRESET_HINT_TEMPLATE = "当前使用预设: {name} - 取消预设后可手动编辑"

_ORIGINAL_BUILD_VARIABLES = DashboardApp._build_variables
_ORIGINAL_BIND_VARIABLE_EVENTS = DashboardApp._bind_variable_events
_ORIGINAL_SAVE_STATE = DashboardApp._save_state
_ORIGINAL_DASHBOARD_INIT = DashboardApp.__init__
_ORIGINAL_DASHBOARD_DESTROY = DashboardApp.destroy


def _dashboard_cjk_font_family() -> str:
    system = platform.system()
    if system == "Windows":
        return "Microsoft YaHei UI"
    if system == "Darwin":
        return "PingFang SC"
    return "Noto Sans CJK SC"


def _dashboard_theme_font(size: int = 12, *, weight: str = "normal") -> ctk.CTkFont:
    return ctk.CTkFont(family=_dashboard_cjk_font_family(), size=size, weight=weight)


def _prime_dashboard_cjk_theme() -> str:
    family = _dashboard_cjk_font_family()
    try:
        theme = getattr(ctk.ThemeManager, "theme", None)
        if isinstance(theme, dict):
            font_settings = theme.setdefault("CTkFont", {})
            if isinstance(font_settings, dict):
                font_settings["family"] = family
    except Exception:
        pass
    return family


def _patched_apply_cjk_font_to_widgets_v2(self: DashboardApp, widget: tk.Misc | None = None) -> None:
    target = widget or self
    family = str(getattr(self, "_cjk_font_family", "") or _dashboard_cjk_font_family())
    base_font = ctk.CTkFont(family=family, size=12)
    for child in target.winfo_children():
        try:
            if isinstance(child, tk.Listbox):
                child.configure(font=(family, 11))
            else:
                widget_type = child.__class__.__name__
                if widget_type in {"CTkOptionMenu", "CTkComboBox"}:
                    child.configure(font=base_font)
                    try:
                        child.configure(dropdown_font=base_font)
                    except Exception:
                        pass
                elif widget_type in {"CTkButton", "CTkCheckBox", "CTkEntry", "CTkSegmentedButton", "CTkLabel"}:
                    child.configure(font=base_font)
        except Exception:
            pass
        self._apply_cjk_font_to_widgets(child)


def _patched_setup_cjk_font_v2(self: DashboardApp) -> None:
    family = _prime_dashboard_cjk_theme()
    self._cjk_font_family = family
    try:
        self.option_add("*Font", f"{{{family}}} 11")
    except Exception:
        pass
    for font_name in (
        "TkDefaultFont",
        "TkTextFont",
        "TkMenuFont",
        "TkHeadingFont",
        "TkCaptionFont",
        "TkTooltipFont",
        "TkFixedFont",
        "TkIconFont",
    ):
        try:
            tkfont.nametofont(font_name).configure(family=family, size=11)
        except Exception:
            continue
    self._apply_cjk_font_to_widgets()


def _patched_dashboard_init_v2(self: DashboardApp) -> None:
    self._cjk_font_family = _prime_dashboard_cjk_theme()
    _ORIGINAL_DASHBOARD_INIT(self)
    self._setup_cjk_font()


def _patched_dashboard_destroy_v2(self: DashboardApp) -> None:
    try:
        pending_ids = self.tk.call("after", "info")
    except Exception:
        pending_ids = ()
    if isinstance(pending_ids, str):
        pending_items = [pending_ids] if pending_ids else []
    else:
        pending_items = list(pending_ids)
    for after_id in pending_items:
        try:
            self.after_cancel(after_id)
        except Exception:
            continue
    try:
        _ORIGINAL_DASHBOARD_DESTROY(self)
    except Exception:
        pass


def _dashboard_parse_serials(raw_value: str) -> list[int]:
    serials: list[int] = []
    seen: set[int] = set()
    for token in re.split(r"[\s,，/|]+", str(raw_value or "").strip()):
        clean = str(token or "").strip()
        if not clean:
            continue
        try:
            serial = int(clean)
        except ValueError:
            continue
        if serial in seen:
            continue
        seen.add(serial)
        serials.append(serial)
    return serials


def _dashboard_sorted_template_names(templates: dict[str, dict[str, Any]]) -> list[str]:
    names = [str(name).strip() for name in templates.keys() if str(name).strip()]
    return sorted(names, key=lambda item: (item != DEFAULT_PATH_TEMPLATE_NAME, item.lower())) or [DEFAULT_PATH_TEMPLATE_NAME]


def _dashboard_option_row(
    parent: ctk.CTkFrame,
    row: int,
    label: str,
    variable: ctk.StringVar,
    *,
    values: list[str] | None = None,
    button_text: str | None = None,
    button_command: Callable[[], None] | None = None,
) -> Any:
    parent.grid_columnconfigure(1, weight=1)
    ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=(16, 8), pady=(0, 8))
    if values:
        widget = ctk.CTkOptionMenu(
            parent,
            variable=variable,
            values=values,
            font=_dashboard_theme_font(),
            dropdown_font=_dashboard_theme_font(),
        )
    else:
        widget = ctk.CTkEntry(parent, textvariable=variable)
    widget.grid(row=row, column=1, sticky="ew", padx=8, pady=(0, 8))
    if button_text and button_command:
        ctk.CTkButton(parent, text=button_text, command=button_command, width=110).grid(
            row=row, column=2, sticky="e", padx=(8, 16), pady=(0, 8)
        )
    return widget


def _dashboard_range_row(
    parent: ctk.CTkFrame,
    row: int,
    label: str,
    min_var: ctk.StringVar,
    max_var: ctk.StringVar,
) -> tuple[Any, Any]:
    parent.grid_columnconfigure(1, weight=1)
    parent.grid_columnconfigure(3, weight=1)
    ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=(16, 8), pady=(0, 8))
    min_widget = ctk.CTkEntry(parent, textvariable=min_var, placeholder_text="最小值")
    min_widget.grid(row=row, column=1, sticky="ew", padx=(8, 6), pady=(0, 8))
    ctk.CTkLabel(parent, text="到").grid(row=row, column=2, sticky="ew", padx=4, pady=(0, 8))
    max_widget = ctk.CTkEntry(parent, textvariable=max_var, placeholder_text="最大值")
    max_widget.grid(row=row, column=3, sticky="ew", padx=(6, 16), pady=(0, 8))
    return min_widget, max_widget


def _patched_build_variables_v2(self: DashboardApp) -> None:
    _ORIGINAL_BUILD_VARIABLES(self)
    state = self._state
    self.title("YouTube 自动化统一控制台")
    self.run_metadata_var.set(True)
    self.run_render_var.set(True)
    self.run_upload_var.set(True)
    self.path_templates = load_path_templates()
    self._live_groups: dict[str, list[str]] = {}
    self.queue_path_template_var = ctk.StringVar(
        value=str(state.get("queue_path_template") or DEFAULT_PATH_TEMPLATE_NAME).strip() or DEFAULT_PATH_TEMPLATE_NAME
    )
    self.queue_windows_var = ctk.StringVar(value=str(state.get("queue_windows") or "").strip())
    self.queue_videos_per_window_var = ctk.StringVar(value=str(state.get("queue_videos_per_window") or "1").strip() or "1")
    # 浏览器提供者选择: auto / hubstudio / bitbrowser
    _saved_provider = str(state.get("browser_provider") or "auto").strip().lower() or "auto"
    if _saved_provider not in {"auto", "hubstudio", "bitbrowser"}:
        _saved_provider = "auto"
    if _saved_provider == "hubstudio":
        try:
            probes = probe_browser_providers()
            if probes.get("bitbrowser") and not probes.get("hubstudio"):
                _saved_provider = "bitbrowser"
                state["browser_provider"] = _saved_provider
        except Exception:
            pass
    self.browser_provider_var = ctk.StringVar(value=_saved_provider)
    # 启动时立即应用 provider 设置
    set_runtime_provider(_saved_provider if _saved_provider != "auto" else None)
    self._step_generate_var = ctk.BooleanVar(value=bool(state.get("step_generate", True)))
    self._step_render_var = ctk.BooleanVar(value=bool(state.get("step_render", True)))
    self._step_upload_var = ctk.BooleanVar(value=bool(state.get("step_upload", True)))
    self._default_rules_expanded = bool(state.get("default_rules_expanded", False))
    self.default_rules_toggle_text_var = ctk.StringVar(value="")
    self.path_template_name_var = ctk.StringVar(value="")
    self.path_template_description_var = ctk.StringVar(value="")
    self.path_template_source_root_var = ctk.StringVar(value="")
    self.path_template_copywriting_output_var = ctk.StringVar(value="")
    self.path_template_thumbnail_output_var = ctk.StringVar(value="")
    self.path_template_render_output_var = ctk.StringVar(value="")
    self.path_template_used_materials_var = ctk.StringVar(value="")
    self.path_template_used_videos_var = ctk.StringVar(value="")
    self.path_template_auto_delete_days_var = ctk.StringVar(value="0")
    self._path_template_editor_selection = str(
        state.get("path_template_editor_selection") or DEFAULT_PATH_TEMPLATE_NAME
    ).strip() or DEFAULT_PATH_TEMPLATE_NAME
    self.path_template_listbox: tk.Listbox | None = None
    self.default_rules_body: Any = None
    self.default_schedule_details_frame: Any = None
    self.default_schedule_hint_var = ctk.StringVar(
        value="💡 示例：首个 15:00，间隔 60 分钟 -> 15:00, 16:00, 17:00 ..."
    )
    self.queue_list_frame: Any = None
    self.pause_button = None
    self.cancel_button = None
    self.run_status_var.set("空闲")
    self.run_phase_var.set("等待任务")
    self.run_detail_var.set("当前没有在运行的任务")
    self.run_last_log_var.set("最近日志会显示在这里")
    self.pause_button_text_var.set("暂停")


def _patched_bind_variable_events_v2(self: DashboardApp) -> None:
    _ORIGINAL_BIND_VARIABLE_EVENTS(self)
    self.queue_path_template_var.trace_add("write", lambda *_: self._on_queue_path_template_change())
    for variable in (
        self._step_generate_var,
        self._step_render_var,
        self._step_upload_var,
        self.queue_videos_per_window_var,
    ):
        variable.trace_add("write", lambda *_: self._save_state())


def _patched_save_state_v2(self: DashboardApp) -> None:
    """防抖版 save_state — 300ms 内多次调用只执行一次磁盘写入"""
    if not hasattr(self, "_save_state_pending"):
        self._save_state_pending = None

    if self._save_state_pending is not None:
        try:
            self.after_cancel(self._save_state_pending)
        except Exception:
            pass
        self._save_state_pending = None

    def _do_save() -> None:
        self._save_state_pending = None
        _ORIGINAL_SAVE_STATE(self)
        payload = {}
        if STATE_FILE.exists():
            try:
                payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload.update(
            {
                "queue_path_template": str(self.queue_path_template_var.get() or "").strip(),
                "queue_windows": str(self.queue_windows_var.get() or "").strip(),
                "queue_videos_per_window": str(self.queue_videos_per_window_var.get() or "1").strip() or "1",
                "step_generate": bool(self._step_generate_var.get()),
                "step_render": bool(self._step_render_var.get()),
                "step_upload": bool(self._step_upload_var.get()),
                "default_rules_expanded": bool(self._default_rules_expanded),
                "path_template_editor_selection": str(self._path_template_editor_selection or "").strip(),
                "browser_provider": str(self.browser_provider_var.get() or "auto").strip().lower(),
            }
        )
        STATE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    self._save_state_pending = self.after(300, _do_save)


def _patched_refresh_bindings_box_v2(self: DashboardApp) -> None:
    if hasattr(self, "binding_box") and self.binding_box is not None:
        try:
            self.binding_box.delete("1.0", "end")
            self.binding_box.insert("end", describe_group_bindings(self.scheduler_config))
        except Exception:
            pass


def _patched_module_names_for_new_job_v2(self: DashboardApp) -> list[str]:
    modules: list[str] = []
    if self._step_generate_var.get():
        modules.append("metadata")
    if self._step_render_var.get():
        modules.append("render")
    if self._step_upload_var.get():
        modules.append("upload")
    if modules:
        return modules
    self._step_generate_var.set(True)
    self._step_render_var.set(True)
    self._step_upload_var.set(True)
    return ["metadata", "render", "upload"]


def _patched_apply_run_status_v2(self: DashboardApp) -> None:
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


def _patched_start_run_tracking_v2(
    self: DashboardApp,
    mode_label: str,
    total_items: int,
    *,
    include_upload: bool = False,
) -> None:
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
    with self._state_lock:
        self._run_result_map = {}
    self._run_plan_for_summary = None
    self._run_execution_result = None
    self._run_report_logged = False
    self.run_last_log_var.set("任务已启动，等待第一条日志")
    self._apply_run_status()


def _patched_finish_run_tracking_v2(
    self: DashboardApp,
    *,
    success: bool,
    summary: str,
    cancelled: bool = False,
) -> None:
    elapsed = time.time() - self._run_started_at if self._run_started_at else 0.0
    summary_text, full_report = self._compose_run_completion_report(success, summary, cancelled)
    self.run_status_var.set("已取消" if cancelled else ("完成" if success else "失败"))
    self.run_phase_var.set("已结束")
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
    self._run_phase = "空闲"
    self._run_include_upload = False
    self.execution_control = None
    self._cancel_requested = False
    self._run_paused = False
    self.pause_button_text_var.set("暂停")
    self._run_render_done.clear()
    self._run_upload_done.clear()
    self._run_plan_for_summary = None
    self._run_execution_result = None
    with self._state_lock:
        self._run_result_map = {}
    self._run_report_logged = False


def _patched_build_layout_v2(self: DashboardApp) -> None:
    self.title("YouTube 自动化统一控制台")
    self.grid_columnconfigure(0, weight=1)
    self.grid_rowconfigure(1, weight=1)

    header = ctk.CTkFrame(self, corner_radius=18)
    header.grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 8))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        header,
        text="YouTube 自动化统一控制台",
        font=ctk.CTkFont(size=32, weight="bold"),
    ).grid(row=0, column=0, sticky="w", padx=20, pady=(18, 6))
    ctk.CTkLabel(
        header,
        text="上传页负责排队与运行，提示词 / 路径模板 / 高级视觉分别管理各自模板。",
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
    ctk.CTkLabel(status_frame, textvariable=self.run_status_var).grid(row=0, column=1, sticky="w", padx=8, pady=(12, 4))
    ctk.CTkLabel(status_frame, text="阶段").grid(row=1, column=0, sticky="w", padx=14, pady=(0, 4))
    ctk.CTkLabel(status_frame, textvariable=self.run_phase_var).grid(
        row=1, column=1, columnspan=3, sticky="w", padx=8, pady=(0, 4)
    )
    ctk.CTkLabel(status_frame, text="当前任务").grid(row=2, column=0, sticky="w", padx=14, pady=(0, 4))
    ctk.CTkLabel(status_frame, textvariable=self.run_detail_var).grid(
        row=2, column=1, columnspan=3, sticky="w", padx=8, pady=(0, 4)
    )
    ctk.CTkLabel(status_frame, text="进度").grid(row=3, column=0, sticky="w", padx=14, pady=(0, 4))
    ctk.CTkLabel(status_frame, textvariable=self.run_progress_var).grid(row=3, column=1, sticky="w", padx=8, pady=(0, 4))
    ctk.CTkLabel(status_frame, text="已运行").grid(row=3, column=2, sticky="w", padx=8, pady=(0, 4))
    ctk.CTkLabel(status_frame, textvariable=self.run_elapsed_var).grid(row=3, column=3, sticky="w", padx=8, pady=(0, 4))
    ctk.CTkLabel(status_frame, text="预计剩余").grid(row=4, column=0, sticky="w", padx=14, pady=(0, 4))
    ctk.CTkLabel(status_frame, textvariable=self.run_eta_var).grid(row=4, column=1, sticky="w", padx=8, pady=(0, 4))
    ctk.CTkLabel(status_frame, text="最近日志").grid(row=4, column=2, sticky="w", padx=8, pady=(0, 4))
    ctk.CTkLabel(status_frame, textvariable=self.run_last_log_var).grid(
        row=4, column=3, sticky="w", padx=8, pady=(0, 4)
    )
    self.run_progress_bar = ctk.CTkProgressBar(status_frame)
    self.run_progress_bar.grid(row=5, column=0, columnspan=4, sticky="ew", padx=14, pady=(6, 12))
    self.run_progress_bar.set(0.0)

    self.tabview = ctk.CTkTabview(self, corner_radius=18)
    self.tabview.grid(row=1, column=0, sticky="nsew", padx=18, pady=(0, 18))
    for name in ("上传", "提示词", "路径模板", "高级视觉", "日志"):
        self.tabview.add(name)

    self._build_upload_tab()
    self._build_prompt_tab()
    self._build_paths_tab()
    self._build_visual_tab()
    self._build_log_tab()
    self.tabview.set("上传")


def _patched_build_upload_tab_v2(self: DashboardApp) -> None:
    base_tab = self.tabview.tab("上传")
    base_tab.grid_columnconfigure(0, weight=1)
    base_tab.grid_rowconfigure(0, weight=1)
    tab = ctk.CTkScrollableFrame(base_tab)
    tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
    tab.grid_columnconfigure(0, weight=1)

    add_frame = ctk.CTkFrame(tab)
    add_frame.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 8))
    for column in range(4):
        add_frame.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(add_frame, text="添加分组到队列", font=ctk.CTkFont(size=24, weight="bold")).grid(
        row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
    )
    ctk.CTkLabel(add_frame, text="分组").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
    ctk.CTkLabel(add_frame, text="窗口").grid(row=1, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(add_frame, text="素材目录").grid(row=1, column=2, sticky="w", padx=8, pady=(0, 6))
    self.current_group_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.current_group_var,
        values=[""],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.current_group_menu.grid(row=2, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
    ctk.CTkEntry(add_frame, textvariable=self.queue_windows_var).grid(row=2, column=1, sticky="ew", padx=8, pady=(0, 12))
    source_bar = ctk.CTkFrame(add_frame, fg_color="transparent")
    source_bar.grid(row=2, column=2, columnspan=2, sticky="ew", padx=(8, 16), pady=(0, 12))
    source_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkEntry(source_bar, textvariable=self.source_dir_override_var).grid(
        row=0, column=0, sticky="ew", padx=(0, 8), pady=0
    )
    ctk.CTkButton(source_bar, text="选择文件夹", command=lambda: self._browse_directory_var(self.source_dir_override_var, "选择素材目录")).grid(
        row=0, column=1, sticky="e", pady=0
    )

    ctk.CTkButton(add_frame, text="刷新分组", command=self._refresh_groups).grid(
        row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 12)
    )
    ctk.CTkLabel(add_frame, text="提示词模板").grid(row=4, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
    ctk.CTkLabel(add_frame, text="视觉模板").grid(row=4, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(add_frame, text="路径模板").grid(row=4, column=2, sticky="w", padx=8, pady=(0, 6))
    self.queue_prompt_template_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_prompt_template_var,
        values=["default"],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_prompt_template_menu.grid(row=5, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
    self.queue_visual_mode_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_visual_mode_var,
        values=[QUEUE_VISUAL_RANDOM, QUEUE_VISUAL_MANUAL, *sorted(self.visual_presets.keys(), key=str.lower)],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_visual_mode_menu.grid(row=5, column=1, sticky="ew", padx=8, pady=(0, 12))
    self.queue_path_template_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_path_template_var,
        values=[""],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_path_template_menu.grid(row=5, column=2, sticky="ew", padx=8, pady=(0, 12))
    steps_frame = ctk.CTkFrame(add_frame, fg_color="transparent")
    steps_frame.grid(row=6, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 12))
    for column in range(4):
        steps_frame.grid_columnconfigure(column, weight=1 if column else 0)
    ctk.CTkLabel(steps_frame, text="执行步骤:").grid(row=0, column=0, sticky="w", padx=(0, 12), pady=0)
    ctk.CTkCheckBox(
        steps_frame,
        text="生成文案(标题/简介/缩略图)",
        variable=self._step_generate_var,
    ).grid(row=0, column=1, sticky="w", padx=(0, 12), pady=0)
    ctk.CTkCheckBox(
        steps_frame,
        text="剪辑视频",
        variable=self._step_render_var,
    ).grid(row=0, column=2, sticky="w", padx=(0, 12), pady=0)
    ctk.CTkCheckBox(
        steps_frame,
        text="上传YouTube",
        variable=self._step_upload_var,
    ).grid(row=0, column=3, sticky="w", pady=0)
    ctk.CTkButton(add_frame, text="+ 添加到队列", command=self._add_current_group_to_queue, height=38).grid(
        row=7, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 12)
    )

    table_frame = ctk.CTkFrame(tab)
    table_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=8)
    table_frame.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(table_frame, text="运行队列", font=ctk.CTkFont(size=24, weight="bold")).grid(
        row=0, column=0, sticky="w", padx=16, pady=(14, 10)
    )
    header_row = ctk.CTkFrame(table_frame, fg_color="transparent")
    header_row.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 6))
    for column, title in enumerate(["序号", "分组", "窗口", "提示词模板", "视觉模板", "路径模板", "操作"]):
        header_row.grid_columnconfigure(column, weight=1 if column in {1, 2, 3, 4, 5} else 0)
        ctk.CTkLabel(header_row, text=title, font=ctk.CTkFont(weight="bold"), anchor="w").grid(
            row=0, column=column, sticky="w", padx=6, pady=(0, 4)
        )
    self.queue_list_frame = ctk.CTkScrollableFrame(table_frame, height=250)
    self.queue_list_frame.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 14))
    self.queue_list_frame.grid_columnconfigure(0, weight=1)

    default_frame = ctk.CTkFrame(tab)
    default_frame.grid(row=2, column=0, sticky="ew", padx=8, pady=8)
    default_frame.grid_columnconfigure(0, weight=1)
    toggle_bar = ctk.CTkFrame(default_frame, fg_color="transparent")
    toggle_bar.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 6))
    toggle_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkButton(
        toggle_bar,
        textvariable=self.default_rules_toggle_text_var,
        command=self._toggle_default_rules_panel,
        anchor="w",
        fg_color="transparent",
        hover_color="#24324a",
    ).grid(row=0, column=0, sticky="ew")
    self.default_rules_body = ctk.CTkFrame(default_frame, fg_color="transparent")
    self.default_rules_body.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
    for column in range(4):
        self.default_rules_body.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(self.default_rules_body, text="可见性").grid(row=0, column=0, sticky="w", padx=(8, 8), pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="分类").grid(row=0, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="儿童内容").grid(row=0, column=2, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="AI 内容").grid(row=0, column=3, sticky="w", padx=(8, 16), pady=(0, 6))
    ctk.CTkOptionMenu(self.default_rules_body, variable=self.default_visibility_var, values=VISIBILITY_VALUES).grid(
        row=1, column=0, sticky="ew", padx=(8, 8), pady=(0, 12)
    )
    ctk.CTkOptionMenu(self.default_rules_body, variable=self.default_category_var, values=CATEGORY_VALUES).grid(
        row=1, column=1, sticky="ew", padx=8, pady=(0, 12)
    )
    ctk.CTkOptionMenu(self.default_rules_body, variable=self.default_kids_var, values=YES_NO_VALUES).grid(
        row=1, column=2, sticky="ew", padx=8, pady=(0, 12)
    )
    ctk.CTkOptionMenu(self.default_rules_body, variable=self.default_ai_var, values=YES_NO_VALUES).grid(
        row=1, column=3, sticky="ew", padx=(8, 16), pady=(0, 12)
    )
    self.schedule_enabled_checkbox = ctk.CTkCheckBox(
        self.default_rules_body,
        text="启用定时发布",
        variable=self.schedule_enabled_var,
    )
    self.schedule_enabled_checkbox.grid(row=2, column=0, sticky="w", padx=(8, 8), pady=(0, 8))
    ctk.CTkCheckBox(
        self.default_rules_body,
        text="上传完成后自动关闭窗口",
        variable=self.upload_auto_close_var,
    ).grid(row=2, column=1, columnspan=2, sticky="w", padx=8, pady=(0, 8))
    ctk.CTkLabel(self.default_rules_body, text="间隔(分钟)").grid(row=2, column=3, sticky="w", padx=(8, 16), pady=(0, 6))
    ctk.CTkEntry(self.default_rules_body, textvariable=self.schedule_interval_var).grid(
        row=3, column=3, sticky="ew", padx=(8, 16), pady=(0, 12)
    )
    ctk.CTkLabel(self.default_rules_body, text="发布日期").grid(row=4, column=0, sticky="w", padx=(8, 8), pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="发布时间").grid(row=4, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="时区").grid(row=4, column=2, sticky="w", padx=8, pady=(0, 6))
    self.schedule_date_menu = ctk.CTkOptionMenu(self.default_rules_body, variable=self.schedule_date_var, values=_schedule_date_values())
    self.schedule_date_menu.grid(row=5, column=0, sticky="ew", padx=(8, 8), pady=(0, 12))
    self.schedule_time_menu = ctk.CTkOptionMenu(self.default_rules_body, variable=self.schedule_time_var, values=_schedule_time_values())
    self.schedule_time_menu.grid(row=5, column=1, sticky="ew", padx=8, pady=(0, 12))
    self.schedule_timezone_menu = ctk.CTkOptionMenu(
        self.default_rules_body,
        variable=self.schedule_timezone_var,
        values=SCHEDULE_TIMEZONE_VALUES,
    )
    self.schedule_timezone_menu.grid(row=5, column=2, sticky="ew", padx=8, pady=(0, 12))

    control_frame = ctk.CTkFrame(tab)
    control_frame.grid(row=3, column=0, sticky="ew", padx=8, pady=(8, 16))
    for column in range(3):
        control_frame.grid_columnconfigure(column, weight=1)
    ctk.CTkButton(control_frame, text="▶ 开始运行", command=self._start_real_flow, height=40).grid(
        row=0, column=0, sticky="ew", padx=(16, 8), pady=14
    )
    self.pause_button = ctk.CTkButton(
        control_frame,
        textvariable=self.pause_button_text_var,
        command=self._toggle_pause_current_task,
        height=40,
    )
    self.pause_button.grid(row=0, column=1, sticky="ew", padx=8, pady=14)
    self.cancel_button = ctk.CTkButton(
        control_frame,
        text="⏹ 取消当前批次",
        command=self._cancel_current_task,
        height=40,
        fg_color="#7a1f1f",
        hover_color="#932525",
    )
    self.cancel_button.grid(row=0, column=2, sticky="ew", padx=(8, 16), pady=14)

    self._refresh_default_rules_panel()
    self._bind_scroll_frame_wheel(tab, base_tab, tab, add_frame, table_frame, default_frame, control_frame)


def _patched_build_prompt_tab_v2(self: DashboardApp) -> None:
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
    ctk.CTkLabel(top, text="提示词模板入口", font=ctk.CTkFont(size=24, weight="bold")).grid(
        row=0, column=0, columnspan=6, sticky="w", padx=16, pady=(14, 12)
    )
    ctk.CTkLabel(top, text="分组").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
    self.prompt_group_menu = ctk.CTkOptionMenu(top, variable=self.prompt_group_var, values=[""])
    self.prompt_group_menu.grid(row=2, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
    ctk.CTkLabel(top, text="API 模板").grid(row=1, column=1, sticky="w", padx=8, pady=(0, 6))
    self.api_preset_menu = ctk.CTkOptionMenu(
        top,
        variable=self.api_preset_var,
        values=[""],
        command=lambda value: self._load_api_preset_into_form(value),
    )
    self.api_preset_menu.grid(row=2, column=1, sticky="ew", padx=8, pady=(0, 12))
    ctk.CTkLabel(top, text="内容模板").grid(row=1, column=2, sticky="w", padx=8, pady=(0, 6))
    self.content_template_menu = ctk.CTkOptionMenu(
        top,
        variable=self.content_template_var,
        values=[""],
        command=lambda value: self._load_content_template_into_form(value),
    )
    self.content_template_menu.grid(row=2, column=2, sticky="ew", padx=8, pady=(0, 12))
    ctk.CTkButton(top, text="载入当前模板", command=self._load_selected_prompt_templates).grid(
        row=2, column=3, sticky="ew", padx=8, pady=(0, 12)
    )
    ctk.CTkButton(top, text="绑定分组到 API 模板", command=self._bind_group_api).grid(
        row=2, column=4, sticky="ew", padx=8, pady=(0, 12)
    )
    ctk.CTkButton(top, text="绑定分组到内容模板", command=self._bind_group_content).grid(
        row=2, column=5, sticky="ew", padx=(8, 16), pady=(0, 12)
    )
    ctk.CTkLabel(top, text="API 模板另存为").grid(row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
    ctk.CTkEntry(top, textvariable=self.api_save_name_var).grid(row=4, column=0, sticky="ew", padx=(16, 8), pady=(0, 14))
    ctk.CTkLabel(top, text="内容模板另存为").grid(row=3, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkEntry(top, textvariable=self.content_save_name_var).grid(row=4, column=1, sticky="ew", padx=8, pady=(0, 14))
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
    self._entry_row(content_frame, 5, "标题数量", self.title_count_var)
    self._entry_row(content_frame, 6, "简介数量", self.desc_count_var)
    self._entry_row(content_frame, 7, "缩略图数量", self.thumb_count_var)
    self._entry_row(content_frame, 8, "标题最少字数", self.title_min_var)
    self._entry_row(content_frame, 9, "标题最多字数", self.title_max_var)
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
    self._bind_scroll_frame_wheel(tab, base_tab, tab, top, api_frame, content_frame, audience_frame, include_textboxes=True)


def _patched_build_paths_tab_v2(self: DashboardApp) -> None:
    base_tab = self.tabview.tab("路径模板")
    base_tab.grid_columnconfigure(0, weight=1)
    base_tab.grid_rowconfigure(0, weight=1)

    container = ctk.CTkFrame(base_tab)
    container.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
    container.grid_columnconfigure(0, weight=0, minsize=240)
    container.grid_columnconfigure(1, weight=1)
    container.grid_rowconfigure(0, weight=1)

    left = ctk.CTkFrame(container)
    left.grid(row=0, column=0, sticky="nsw", padx=(0, 10), pady=0)
    left.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(left, text="路径模板列表", font=ctk.CTkFont(size=22, weight="bold")).grid(
        row=0, column=0, sticky="w", padx=16, pady=(14, 8)
    )
    list_frame = ctk.CTkFrame(left)
    list_frame.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 14))
    list_frame.grid_columnconfigure(0, weight=1)
    list_frame.grid_rowconfigure(0, weight=1)
    self.path_template_listbox = tk.Listbox(
        list_frame,
        exportselection=False,
        activestyle="none",
        font=("Microsoft YaHei UI", 11),
        height=16,
    )
    self.path_template_listbox.grid(row=0, column=0, sticky="nsew")
    list_scrollbar = tk.Scrollbar(list_frame, orient="vertical", command=self.path_template_listbox.yview)
    list_scrollbar.grid(row=0, column=1, sticky="ns")
    self.path_template_listbox.configure(yscrollcommand=list_scrollbar.set)
    self.path_template_listbox.bind("<<ListboxSelect>>", self._on_path_template_listbox_select)

    right = ctk.CTkScrollableFrame(container)
    right.grid(row=0, column=1, sticky="nsew", padx=0, pady=0)
    right.grid_columnconfigure(0, weight=1)
    editor = ctk.CTkFrame(right)
    editor.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
    editor.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(editor, text="路径模板编辑", font=ctk.CTkFont(size=24, weight="bold")).grid(
        row=0, column=0, sticky="w", padx=16, pady=(14, 12)
    )
    _dashboard_option_row(editor, 1, "模板名称", self.path_template_name_var)
    _dashboard_option_row(editor, 2, "描述", self.path_template_description_var)
    _dashboard_option_row(
        editor,
        3,
        "素材根目录",
        self.path_template_source_root_var,
        button_text="选择文件夹",
        button_command=lambda: self._browse_directory_var(self.path_template_source_root_var, "选择素材根目录"),
    )
    _dashboard_option_row(
        editor,
        4,
        "文案输出目录",
        self.path_template_copywriting_output_var,
        button_text="选择文件夹",
        button_command=lambda: self._browse_directory_var(self.path_template_copywriting_output_var, "选择文案输出目录"),
    )
    _dashboard_option_row(
        editor,
        5,
        "缩略图输出目录",
        self.path_template_thumbnail_output_var,
        button_text="选择文件夹",
        button_command=lambda: self._browse_directory_var(self.path_template_thumbnail_output_var, "选择缩略图输出目录"),
    )
    _dashboard_option_row(
        editor,
        6,
        "渲染输出目录",
        self.path_template_render_output_var,
        button_text="选择文件夹",
        button_command=lambda: self._browse_directory_var(self.path_template_render_output_var, "选择渲染输出目录"),
    )
    _dashboard_option_row(
        editor,
        7,
        "已用素材归档目录",
        self.path_template_used_materials_var,
        button_text="选择文件夹",
        button_command=lambda: self._browse_directory_var(self.path_template_used_materials_var, "选择已用素材归档目录"),
    )
    _dashboard_option_row(
        editor,
        8,
        "已用视频归档目录",
        self.path_template_used_videos_var,
        button_text="选择文件夹",
        button_command=lambda: self._browse_directory_var(self.path_template_used_videos_var, "选择已用视频归档目录"),
    )
    _dashboard_option_row(editor, 9, "视频自动删除天数", self.path_template_auto_delete_days_var)
    button_bar = ctk.CTkFrame(editor, fg_color="transparent")
    button_bar.grid(row=10, column=0, sticky="ew", padx=12, pady=(0, 14))
    ctk.CTkButton(button_bar, text="保存", command=self._save_current_path_template).pack(side="left", padx=6)
    ctk.CTkButton(button_bar, text="删除", command=self._delete_current_path_template).pack(side="left", padx=6)
    ctk.CTkButton(button_bar, text="新建", command=self._new_path_template).pack(side="left", padx=6)

    self._refresh_path_template_controls()


def _patched_build_visual_tab_v2(self: DashboardApp) -> None:
    base_tab = self.tabview.tab("高级视觉")
    base_tab.grid_columnconfigure(0, weight=1)
    base_tab.grid_rowconfigure(0, weight=1)
    tab = ctk.CTkScrollableFrame(base_tab)
    tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
    tab.grid_columnconfigure(0, weight=1)
    self._visual_setting_widgets = {}

    intro = ctk.CTkFrame(tab)
    intro.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
    intro.grid_columnconfigure(0, weight=1)
    intro.grid_columnconfigure(1, weight=0)
    intro.grid_columnconfigure(2, weight=0)
    intro.grid_columnconfigure(3, weight=0)
    ctk.CTkLabel(intro, text="高级视觉控制", font=ctk.CTkFont(size=24, weight="bold")).grid(
        row=0, column=0, sticky="w", padx=16, pady=(14, 8)
    )
    ctk.CTkButton(intro, text="保存视觉设置", command=self._save_visual_settings).grid(
        row=0, column=1, sticky="e", padx=16, pady=(14, 8)
    )
    ctk.CTkButton(intro, text="套用 MegaBass", command=self._apply_visual_preset_mega_bass).grid(
        row=0, column=2, sticky="e", padx=(0, 8), pady=(14, 8)
    )
    preset_bar = ctk.CTkFrame(intro, fg_color="transparent")
    preset_bar.grid(row=1, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 6))
    for column in range(4):
        preset_bar.grid_columnconfigure(column, weight=1 if column == 1 else 0)
    ctk.CTkLabel(preset_bar, text="视觉预设").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=0)
    self.visual_preset_menu = ctk.CTkOptionMenu(
        preset_bar,
        variable=self.visual_preset_var,
        values=_visual_preset_menu_values(self.visual_presets),
        command=lambda _value: self._on_visual_preset_change(),
    )
    self.visual_preset_menu.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=0)
    ctk.CTkButton(preset_bar, text="保存当前为预设", command=self._save_current_visual_preset).grid(
        row=0, column=2, sticky="ew", padx=(0, 8), pady=0
    )
    ctk.CTkButton(preset_bar, text="删除预设", command=self._delete_selected_visual_preset).grid(
        row=0, column=3, sticky="ew", pady=0
    )
    self.visual_preset_hint_label = ctk.CTkLabel(
        intro,
        textvariable=self.visual_preset_hint_var,
        text_color="#f7d76a",
        anchor="w",
        justify="left",
    )
    self.visual_preset_hint_label.grid(row=2, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 6))
    self.visual_preset_hint_label.grid_remove()

    basic = ctk.CTkFrame(tab)
    basic.grid(row=1, column=0, sticky="ew", padx=8, pady=8)
    for column in range(4):
        basic.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(basic, text="基础效果", font=ctk.CTkFont(size=22, weight="bold")).grid(
        row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
    )
    self._remember_visual_widgets("spectrum", _dashboard_option_row(basic, 1, "频谱", self.visual_spectrum_var, values=VISUAL_TOGGLE_VALUES))
    self._remember_visual_widgets("timeline", _dashboard_option_row(basic, 2, "时间轴", self.visual_timeline_var, values=VISUAL_TOGGLE_VALUES))
    self._remember_visual_widgets("letterbox", _dashboard_option_row(basic, 3, "黑边", self.visual_letterbox_var, values=VISUAL_TOGGLE_VALUES))
    self._remember_visual_widgets("zoom", _dashboard_option_row(basic, 4, "镜头缩放", self.visual_zoom_var, values=_with_random(list_zoom_modes())))
    self._remember_visual_widgets("style", _dashboard_option_row(basic, 5, "频谱样式", self.visual_style_var, values=_with_random(list_effects())))
    self._remember_visual_widgets("spectrum_y", _dashboard_option_row(basic, 6, "频谱 Y", self.visual_spectrum_y_var))
    self._remember_visual_widgets("spectrum_x", _dashboard_option_row(basic, 7, "频谱 X (-1=居中)", self.visual_spectrum_x_var))
    self._remember_visual_widgets("spectrum_w", *_dashboard_range_row(basic, 8, "频谱宽度", self.visual_spectrum_w_min_var, self.visual_spectrum_w_max_var))

    mood = ctk.CTkFrame(tab)
    mood.grid(row=2, column=0, sticky="ew", padx=8, pady=8)
    for column in range(4):
        mood.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(mood, text="色彩与氛围", font=ctk.CTkFont(size=22, weight="bold")).grid(
        row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
    )
    self._remember_visual_widgets("color_spectrum", _dashboard_option_row(mood, 1, "频谱配色", self.visual_color_spectrum_var, values=_with_random(list_palette_names())))
    self._remember_visual_widgets("color_timeline", _dashboard_option_row(mood, 2, "时间轴配色", self.visual_color_timeline_var, values=_with_random(list_palette_names())))
    self._remember_visual_widgets("film_grain", _dashboard_option_row(mood, 3, "胶片颗粒", self.visual_film_grain_var, values=VISUAL_TOGGLE_VALUES))
    self._remember_visual_widgets("grain_strength", *_dashboard_range_row(mood, 4, "颗粒强度", self.visual_grain_strength_min_var, self.visual_grain_strength_max_var))
    self._remember_visual_widgets("vignette", _dashboard_option_row(mood, 5, "暗角", self.visual_vignette_var, values=VISUAL_TOGGLE_VALUES))
    self._remember_visual_widgets("color_tint", _dashboard_option_row(mood, 6, "色调", self.visual_tint_var, values=_with_random(list_tint_names())))
    self._remember_visual_widgets("soft_focus", _dashboard_option_row(mood, 7, "柔焦", self.visual_soft_focus_var, values=VISUAL_TOGGLE_VALUES))
    self._remember_visual_widgets("soft_focus_sigma", *_dashboard_range_row(mood, 8, "柔焦强度", self.visual_soft_focus_sigma_min_var, self.visual_soft_focus_sigma_max_var))

    pulse = ctk.CTkFrame(tab)
    pulse.grid(row=3, column=0, sticky="ew", padx=8, pady=8)
    for column in range(4):
        pulse.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(pulse, text="节奏联动", font=ctk.CTkFont(size=22, weight="bold")).grid(
        row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
    )
    self._remember_visual_widgets("bass_pulse", _dashboard_option_row(pulse, 1, "低频脉冲", self.visual_bass_pulse_var, values=VISUAL_TOGGLE_VALUES))
    self._remember_visual_widgets("bass_pulse_scale", *_dashboard_range_row(pulse, 2, "脉冲缩放", self.visual_bass_pulse_scale_min_var, self.visual_bass_pulse_scale_max_var))
    self._remember_visual_widgets(
        "bass_pulse_brightness",
        *_dashboard_range_row(pulse, 3, "脉冲亮度", self.visual_bass_pulse_brightness_min_var, self.visual_bass_pulse_brightness_max_var),
    )

    overlay = ctk.CTkFrame(tab)
    overlay.grid(row=4, column=0, sticky="ew", padx=8, pady=8)
    for column in range(4):
        overlay.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(overlay, text="粒子与叠字", font=ctk.CTkFont(size=22, weight="bold")).grid(
        row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 12)
    )
    self._remember_visual_widgets("particle", _dashboard_option_row(overlay, 1, "粒子效果", self.visual_particle_var, values=_with_random_first(list_particle_effects())))
    self._remember_visual_widgets("particle_opacity", *_dashboard_range_row(overlay, 2, "粒子透明度", self.visual_particle_opacity_min_var, self.visual_particle_opacity_max_var))
    self._remember_visual_widgets("particle_speed", *_dashboard_range_row(overlay, 3, "粒子速度", self.visual_particle_speed_min_var, self.visual_particle_speed_max_var))
    self._remember_visual_widgets("text", _dashboard_option_row(overlay, 4, "叠字内容", self.visual_text_var))
    self._remember_visual_widgets("text_font", _dashboard_option_row(overlay, 5, "字体", self.visual_text_font_var, values=_with_random(list_font_names())))
    self._remember_visual_widgets("text_pos", _dashboard_option_row(overlay, 6, "文字位置", self.visual_text_pos_var, values=_with_random(list_text_positions())))
    self._remember_visual_widgets("text_size", *_dashboard_range_row(overlay, 7, "文字大小", self.visual_text_size_min_var, self.visual_text_size_max_var))
    self._remember_visual_widgets("text_style", _dashboard_option_row(overlay, 8, "文字样式", self.visual_text_style_var, values=_with_random(list_text_styles())))

    self._refresh_visual_preset_controls()
    self._on_visual_preset_change()
    self._bind_scroll_frame_wheel(tab, base_tab, tab, intro, basic, mood, pulse, overlay)


def _patched_build_log_tab_v2(self: DashboardApp) -> None:
    tab = self.tabview.tab("日志")
    tab.grid_columnconfigure(0, weight=1)
    tab.grid_rowconfigure(1, weight=1)
    bar = ctk.CTkFrame(tab, fg_color="transparent")
    bar.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
    ctk.CTkButton(bar, text="清空日志", command=self._clear_logs).pack(side="left", padx=6)
    self.log_box = ctk.CTkTextbox(tab)
    self.log_box.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))


def _patched_refresh_default_rules_panel_v2(self: DashboardApp) -> None:
    expanded = bool(self._default_rules_expanded)
    self.default_rules_toggle_text_var.set("▼ 默认规则（点击折叠）" if expanded else "▶ 默认规则（点击展开）")
    if self.default_rules_body is None:
        return
    if expanded:
        self.default_rules_body.grid()
    else:
        self.default_rules_body.grid_remove()
    self._refresh_schedule_controls()


def _patched_toggle_default_rules_panel_v2(self: DashboardApp) -> None:
    self._default_rules_expanded = not bool(self._default_rules_expanded)
    self._refresh_default_rules_panel()
    self._save_state()


def _patched_browse_directory_var_v2(self: DashboardApp, variable: ctk.StringVar, title: str) -> None:
    selected = filedialog.askdirectory(title=title)
    if selected:
        variable.set(selected)


def _patched_refresh_path_template_controls_v2(self: DashboardApp) -> None:
    self.path_templates = load_path_templates()
    names = _dashboard_sorted_template_names(self.path_templates)
    if hasattr(self, "queue_path_template_menu"):
        self.queue_path_template_menu.configure(values=names)
    selected_name = str(self._path_template_editor_selection or "").strip()
    if selected_name not in names:
        selected_name = names[0]
    self._path_template_editor_selection = selected_name

    if self.path_template_listbox is not None:
        self.path_template_listbox.delete(0, "end")
        for name in names:
            self.path_template_listbox.insert("end", name)
        try:
            selected_index = names.index(selected_name)
        except ValueError:
            selected_index = 0
        self.path_template_listbox.selection_clear(0, "end")
        self.path_template_listbox.selection_set(selected_index)
        self.path_template_listbox.activate(selected_index)
        self.path_template_listbox.see(selected_index)

    self._load_path_template_into_editor(selected_name)
    current_queue_template = str(self.queue_path_template_var.get() or "").strip()
    if current_queue_template not in names:
        self.queue_path_template_var.set(selected_name)


def _patched_load_path_template_into_editor_v2(self: DashboardApp, name: str) -> None:
    clean_name, template = get_path_template(name, templates=self.path_templates)
    self._path_template_editor_selection = clean_name
    self.path_template_name_var.set(clean_name)
    self.path_template_description_var.set(str(template.get("description") or "").strip())
    self.path_template_source_root_var.set(str(template.get("source_root") or "").strip())
    self.path_template_copywriting_output_var.set(str(template.get("copywriting_output") or "").strip())
    self.path_template_thumbnail_output_var.set(str(template.get("thumbnail_output") or "").strip())
    self.path_template_render_output_var.set(str(template.get("render_output") or "").strip())
    self.path_template_used_materials_var.set(str(template.get("used_materials_dir") or "").strip())
    self.path_template_used_videos_var.set(str(template.get("used_videos_dir") or "").strip())
    self.path_template_auto_delete_days_var.set(str(template.get("auto_delete_days") or 0))


def _patched_on_path_template_listbox_select_v2(self: DashboardApp, _event: Any | None = None) -> None:
    if self.path_template_listbox is None:
        return
    selection = self.path_template_listbox.curselection()
    if not selection:
        return
    name = str(self.path_template_listbox.get(selection[0]) or "").strip()
    if not name:
        return
    self._load_path_template_into_editor(name)
    self._save_state()


def _patched_new_path_template_v2(self: DashboardApp) -> None:
    self._path_template_editor_selection = ""
    self.path_template_name_var.set("")
    self.path_template_description_var.set("")
    self.path_template_source_root_var.set("")
    self.path_template_copywriting_output_var.set("")
    self.path_template_thumbnail_output_var.set("")
    self.path_template_render_output_var.set("")
    self.path_template_used_materials_var.set("")
    self.path_template_used_videos_var.set("")
    self.path_template_auto_delete_days_var.set("0")
    if self.path_template_listbox is not None:
        self.path_template_listbox.selection_clear(0, "end")


def _patched_save_current_path_template_v2(self: DashboardApp) -> None:
    name = str(self.path_template_name_var.get() or "").strip()
    if not name:
        messagebox.showerror("保存路径模板失败", "模板名称不能为空。")
        return
    payload = normalize_path_template(
        name,
        {
            "description": self.path_template_description_var.get(),
            "source_root": self.path_template_source_root_var.get(),
            "copywriting_output": self.path_template_copywriting_output_var.get(),
            "thumbnail_output": self.path_template_thumbnail_output_var.get(),
            "render_output": self.path_template_render_output_var.get(),
            "used_materials_dir": self.path_template_used_materials_var.get(),
            "used_videos_dir": self.path_template_used_videos_var.get(),
            "auto_delete_days": self.path_template_auto_delete_days_var.get(),
        },
    )
    templates = dict(load_path_templates())
    templates[name] = payload
    self.path_templates = save_path_templates(templates)
    self._path_template_editor_selection = name
    self._refresh_path_template_controls()
    self.queue_path_template_var.set(name)
    self._apply_current_group_context(preserve_selection=True)
    self._log(f"[路径模板] 已保存: {name}")
    self._save_state()


def _patched_delete_current_path_template_v2(self: DashboardApp) -> None:
    name = str(self.path_template_name_var.get() or "").strip()
    if not name:
        return
    if name == DEFAULT_PATH_TEMPLATE_NAME:
        messagebox.showinfo("删除路径模板", "默认路径不能删除。")
        return
    templates = dict(load_path_templates())
    if name not in templates:
        return
    del templates[name]
    self.path_templates = save_path_templates(templates)
    self._path_template_editor_selection = DEFAULT_PATH_TEMPLATE_NAME
    self._refresh_path_template_controls()
    self.queue_path_template_var.set(DEFAULT_PATH_TEMPLATE_NAME)
    self._apply_current_group_context(preserve_selection=True)
    self._log(f"[路径模板] 已删除: {name}")
    self._save_state()


def _patched_on_queue_path_template_change_v2(self: DashboardApp) -> None:
    self._apply_queue_path_template_selection()
    self._save_state()


def _patched_apply_queue_path_template_selection_v1(self: DashboardApp) -> None:
    current_group = str(self.current_group_var.get() or "").strip()
    selected_name = str(self.queue_path_template_var.get() or "").strip()
    path_name, path_template = get_path_template(selected_name, templates=self.path_templates)
    if path_name != selected_name:
        self.queue_path_template_var.set(path_name)
        return
    resolved_source = resolve_source_dir(
        path_template,
        group_tag=current_group,
        fallback=self.source_dir_override_var.get(),
    )
    self.source_dir_override_var.set(resolved_source)


def _patched_apply_current_group_context_v2(self: DashboardApp, *, preserve_selection: bool) -> None:
    current_group = str(self.current_group_var.get() or "").strip()
    available_serials = [int(info.serial) for info in self.group_catalog.get(current_group, [])]
    valid_serials = set(available_serials)
    if preserve_selection:
        existing = [serial for serial in _dashboard_parse_serials(self.queue_windows_var.get()) if serial in valid_serials]
        serials = existing or available_serials
    else:
        serials = available_serials
    self.queue_windows_var.set(", ".join(str(serial) for serial in serials))

    path_name, path_template = get_path_template(self.queue_path_template_var.get(), templates=self.path_templates)
    if path_name != self.queue_path_template_var.get():
        self.queue_path_template_var.set(path_name)
    resolved_source = resolve_source_dir(path_template, group_tag=current_group, fallback=self.source_dir_override_var.get())
    self.source_dir_override_var.set(resolved_source)

    api_name, content_name = self._queue_template_defaults_for_group(current_group)
    self.queue_api_template_var.set(api_name)
    self.queue_prompt_template_var.set(content_name)


def _patched_refresh_groups_v2(self: DashboardApp) -> None:
    self.group_catalog, browser_error = _build_group_catalog_from_config()
    groups = list(self.group_catalog.keys()) or [""]
    for attr_name in ("current_group_menu", "prompt_group_menu"):
        menu = getattr(self, attr_name, None)
        if menu is not None:
            menu.configure(values=groups)
    if self.current_group_var.get() not in groups:
        self.current_group_var.set(groups[0])
    if self.prompt_group_var.get() not in groups:
        self.prompt_group_var.set(self.current_group_var.get())
    self._apply_current_group_context(preserve_selection=True)
    self._refresh_queue_display()
    self._save_state()
    if browser_error is not None:
        self._log(f"[分组] BitBrowser 环境读取失败，已回退到 upload_config/channel_mapping: {browser_error}")


def _patched_remove_queue_job_v2(self: DashboardApp, index: int) -> None:
    try:
        self.run_queue.remove_job(index)
    except IndexError:
        return
    self._refresh_task_tree()


def _patched_refresh_queue_display_v2(self: DashboardApp) -> None:
    if self.queue_list_frame is None:
        return
    for child in self.queue_list_frame.winfo_children():
        child.destroy()
    if self.run_queue.is_empty():
        ctk.CTkLabel(self.queue_list_frame, text="队列为空，请在上方添加分组任务", text_color="#9fb2c8").pack(
            anchor="w",
            padx=12,
            pady=12,
        )
        return
    for summary in self.run_queue.get_summary():
        row = ctk.CTkFrame(self.queue_list_frame)
        row.pack(fill="x", padx=4, pady=4)
        for column in range(7):
            row.grid_columnconfigure(column, weight=1 if column in {1, 2, 3, 4, 5} else 0)
        ctk.CTkLabel(row, text=str(summary["index"] + 1), anchor="w").grid(row=0, column=0, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["group_tag"], anchor="w").grid(row=0, column=1, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["window_serials_text"], anchor="w").grid(row=0, column=2, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["prompt_template"], anchor="w").grid(row=0, column=3, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["visual_mode"], anchor="w").grid(row=0, column=4, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["path_template"], anchor="w").grid(row=0, column=5, sticky="w", padx=6, pady=8)
        action_bar = ctk.CTkFrame(row, fg_color="transparent")
        action_bar.grid(row=0, column=6, sticky="e", padx=6, pady=8)
        ctk.CTkButton(action_bar, text="设置", width=72, command=lambda index=summary["index"]: self._open_window_override_dialog(index)).pack(side="left", padx=(0, 6))
        ctk.CTkButton(
            action_bar,
            text="删除",
            width=72,
            command=lambda index=summary["index"]: self._remove_queue_job(index),
            fg_color="#7a1f1f",
            hover_color="#932525",
        ).pack(side="left")


def _patched_add_current_group_to_queue_v2(self: DashboardApp) -> None:
    current_group = str(self.current_group_var.get() or "").strip()
    if not current_group:
        messagebox.showerror("无法加入队列", "请先选择一个分组。")
        return
    selected_serials = _dashboard_parse_serials(self.queue_windows_var.get())
    if not selected_serials:
        messagebox.showerror("无法加入队列", "请先填写至少一个窗口号。")
        return

    visual_mode = _visual_mode_to_value(self.queue_visual_mode_var.get())
    visual_settings: dict[str, Any] | None = None
    if visual_mode == "manual":
        visual_settings = dict(self._collect_visual_settings())
    elif visual_mode not in {"random", "manual"}:
        visual_settings = dict(self.visual_presets.get(visual_mode) or {})

    path_name, path_template = get_path_template(self.queue_path_template_var.get(), templates=self.path_templates)
    resolved_source = str(self.source_dir_override_var.get() or "").strip() or resolve_source_dir(path_template, group_tag=current_group)
    selected_modules = self._module_names_for_new_job()
    selected_steps = []
    if "metadata" in selected_modules:
        selected_steps.append("generate")
    if "render" in selected_modules:
        selected_steps.append("render")
    if "upload" in selected_modules:
        selected_steps.append("upload")
    job = GroupJob(
        group_tag=current_group,
        window_serials=selected_serials,
        source_dir=resolved_source,
        prompt_template=str(self.queue_prompt_template_var.get() or "default").strip() or "default",
        api_template=str(self.queue_api_template_var.get() or "default").strip() or "default",
        api_preset=str(self.queue_api_template_var.get() or "default").strip() or "default",
        visual_mode=visual_mode,
        visual_settings=visual_settings,
        path_template=path_name,
        upload_defaults=self._current_upload_defaults_model(),
        steps=selected_steps,
        modules=selected_modules,
    )
    self.run_queue.add_job(job)
    self._apply_current_group_context(preserve_selection=False)
    self._refresh_task_tree()


def _patched_window_default_values_v2(self: DashboardApp, job: GroupJob, serial: int) -> dict[str, str]:
    info = self._find_window_info(job.group_tag, serial)
    defaults = self._current_upload_defaults_model()
    override = job.get_window_override(serial)
    visibility = str((override.visibility if override and override.visibility else defaults.visibility) or "private").strip() or "private"
    category = str((override.category if override and override.category else defaults.category) or "Music").strip() or "Music"
    kids = str((override.kids_content if override and override.kids_content else _yes_no_from_bool(defaults.is_for_kids)) or "no").strip() or "no"
    ai = str((override.ai_content if override and override.ai_content else (defaults.ai_content or defaults.altered_content or "yes")) or "yes").strip() or "yes"
    schedule_time = str((override.schedule_time if override and override.schedule_time else defaults.schedule_time or "")).strip()
    ypp = str((override.ypp if override and override.ypp else _yes_no_from_bool(bool(info.is_ypp))) or "no").strip() or "no"
    return {
        "ypp": ypp,
        "visibility": visibility,
        "category": category,
        "kids_content": kids,
        "ai_content": ai,
        "schedule_time": schedule_time,
    }


def _patched_open_window_override_dialog_v2(self: DashboardApp, index: int) -> None:
    if index < 0 or index >= len(self.run_queue.jobs):
        return
    job = self.run_queue.jobs[index]
    dialog = ctk.CTkToplevel(self)
    dialog.title(f"单独设置 - {job.group_tag}")
    dialog.geometry("960x440")
    dialog.transient(self)
    dialog.grab_set()
    dialog.grid_columnconfigure(0, weight=1)
    dialog.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(
        dialog,
        text=f"分组: {job.group_tag}  |  窗口: {', '.join(str(item) for item in job.window_serials)}",
        font=ctk.CTkFont(size=18, weight="bold"),
    ).grid(row=0, column=0, sticky="w", padx=16, pady=(16, 10))

    table = ctk.CTkScrollableFrame(dialog)
    table.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))
    for column in range(7):
        table.grid_columnconfigure(column, weight=1)

    headers = ["窗口", "YPP", "可见性", "分类", "儿童内容", "AI 内容", "定时发布"]
    for column, title in enumerate(headers):
        ctk.CTkLabel(table, text=title, font=ctk.CTkFont(weight="bold")).grid(row=0, column=column, sticky="w", padx=6, pady=(4, 8))

    row_vars: dict[int, dict[str, ctk.StringVar]] = {}
    default_cache = {serial: self._window_default_values(job, serial) for serial in job.window_serials}
    schedule_values = ["", *_schedule_time_values()]
    for row_index, serial in enumerate(job.window_serials, start=1):
        defaults = default_cache[serial]
        ctk.CTkLabel(table, text=str(serial)).grid(row=row_index, column=0, sticky="w", padx=6, pady=6)
        vars_for_row = {
            "ypp": ctk.StringVar(value=defaults["ypp"]),
            "visibility": ctk.StringVar(value=defaults["visibility"]),
            "category": ctk.StringVar(value=defaults["category"]),
            "kids_content": ctk.StringVar(value=defaults["kids_content"]),
            "ai_content": ctk.StringVar(value=defaults["ai_content"]),
            "schedule_time": ctk.StringVar(value=defaults["schedule_time"]),
        }
        row_vars[serial] = vars_for_row
        ctk.CTkOptionMenu(table, variable=vars_for_row["ypp"], values=YES_NO_VALUES).grid(row=row_index, column=1, sticky="ew", padx=6, pady=6)
        ctk.CTkOptionMenu(table, variable=vars_for_row["visibility"], values=VISIBILITY_VALUES).grid(row=row_index, column=2, sticky="ew", padx=6, pady=6)
        ctk.CTkOptionMenu(table, variable=vars_for_row["category"], values=CATEGORY_VALUES).grid(row=row_index, column=3, sticky="ew", padx=6, pady=6)
        ctk.CTkOptionMenu(table, variable=vars_for_row["kids_content"], values=YES_NO_VALUES).grid(row=row_index, column=4, sticky="ew", padx=6, pady=6)
        ctk.CTkOptionMenu(table, variable=vars_for_row["ai_content"], values=YES_NO_VALUES).grid(row=row_index, column=5, sticky="ew", padx=6, pady=6)
        ctk.CTkOptionMenu(table, variable=vars_for_row["schedule_time"], values=schedule_values).grid(row=row_index, column=6, sticky="ew", padx=6, pady=6)

    def reset_to_defaults() -> None:
        for serial, vars_for_row in row_vars.items():
            defaults = default_cache[serial]
            for key, var in vars_for_row.items():
                var.set(defaults[key])

    def save_dialog() -> None:
        job.clear_window_overrides()
        for serial, vars_for_row in row_vars.items():
            defaults = default_cache[serial]
            override = WindowOverride(
                serial=int(serial),
                ypp="" if vars_for_row["ypp"].get() == defaults["ypp"] else vars_for_row["ypp"].get(),
                visibility="" if vars_for_row["visibility"].get() == defaults["visibility"] else vars_for_row["visibility"].get(),
                category="" if vars_for_row["category"].get() == defaults["category"] else vars_for_row["category"].get(),
                kids_content="" if vars_for_row["kids_content"].get() == defaults["kids_content"] else vars_for_row["kids_content"].get(),
                ai_content="" if vars_for_row["ai_content"].get() == defaults["ai_content"] else vars_for_row["ai_content"].get(),
                schedule_time="" if vars_for_row["schedule_time"].get() == defaults["schedule_time"] else vars_for_row["schedule_time"].get(),
            )
            job.set_window_override(override)
        dialog.destroy()
        self._refresh_task_tree()

    button_bar = ctk.CTkFrame(dialog, fg_color="transparent")
    button_bar.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 16))
    ctk.CTkButton(button_bar, text="全部使用默认值", command=reset_to_defaults).pack(side="left", padx=6)
    ctk.CTkButton(button_bar, text="确定", command=save_dialog).pack(side="left", padx=6)
    ctk.CTkButton(button_bar, text="取消", command=dialog.destroy).pack(side="left", padx=6)


def _patched_build_window_tasks_from_job_v2(self: DashboardApp, job: GroupJob) -> list[WindowTask]:
    upload_defaults = UploadDefaults.from_dict(self._current_upload_defaults_model().to_dict())
    if job.upload_defaults:
        upload_defaults = UploadDefaults.from_dict(job.upload_defaults.to_dict())
    notify_subscribers = bool(self.default_notify_var.get())
    tasks: list[WindowTask] = []
    seen_serials: set[int] = set()
    for raw_serial in job.window_serials:
        serial = int(raw_serial)
        if serial in seen_serials:
            continue
        seen_serials.add(serial)
        info = self._find_window_info(job.group_tag, serial)
        override = job.get_window_override(serial)
        visibility = str((override.visibility if override and override.visibility else upload_defaults.visibility) or "private").strip() or "private"
        category = str((override.category if override and override.category else upload_defaults.category) or "Music").strip() or "Music"
        kids_value = override.kids_content if override and override.kids_content else _yes_no_from_bool(upload_defaults.is_for_kids)
        ai_value = override.ai_content if override and override.ai_content else (upload_defaults.ai_content or upload_defaults.altered_content or "yes")
        schedule_time = override.schedule_time if override and override.schedule_time else str(upload_defaults.schedule_time or "").strip()
        schedule_text = ""
        if visibility == "schedule":
            schedule_text = _compose_schedule_text(str(upload_defaults.schedule_date or "").strip(), schedule_time)
        tasks.append(
            create_task(
                tag=job.group_tag,
                serial=serial,
                quantity=1,
                is_ypp=_bool_from_yes_no(override.ypp) if override and str(override.ypp or "").strip() else bool(info.is_ypp),
                title="",
                visibility=visibility,
                category=category,
                made_for_kids=_bool_from_yes_no(kids_value),
                altered_content=_bool_from_yes_no(ai_value),
                notify_subscribers=notify_subscribers,
                scheduled_publish_at=schedule_text,
                schedule_timezone=str(upload_defaults.timezone or "").strip() if visibility == "schedule" else "",
                source_dir=str(job.source_dir or "").strip(),
                channel_name=info.channel_name,
            )
        )
    return tasks


def _patched_runtime_config_for_job_v2(self: DashboardApp, job: GroupJob) -> dict[str, Any]:
    base_config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
    template_name, template = get_path_template(job.path_template, templates=self.path_templates)
    return build_runtime_config(base_config, template, template_name=template_name, source_dir=str(job.source_dir or "").strip())


def _patched_build_run_plan_for_job_v2(self: DashboardApp, job: GroupJob, *, config: dict[str, Any] | None = None):
    runtime_config = config or self._runtime_config_for_job(job)
    tasks = self._build_window_tasks_from_job(job)
    return build_run_plan(
        tasks=tasks,
        defaults=self._workflow_defaults_for_job(job),
        modules=self._module_selection_for_job(job),
        config=runtime_config,
    )


def _patched_build_tracking_plan_for_queue_v2(self: DashboardApp, *, config: dict[str, Any] | None = None):
    effective_jobs = self._effective_run_queue_jobs()
    runtime_config = config or (self._runtime_config_for_job(effective_jobs[0]) if effective_jobs else self._sync_runtime_paths(persist=False))
    tracking_tasks: list[WindowTask] = []
    combined_flags = {"metadata": False, "render": False, "upload": False}
    for job in effective_jobs:
        tracking_tasks.extend(self._build_window_tasks_from_job(job))
        for module_name in job.modules:
            clean_name = str(module_name or "").strip()
            if clean_name in combined_flags:
                combined_flags[clean_name] = True
    modules = build_module_selection(
        metadata=combined_flags["metadata"],
        render=combined_flags["render"],
        upload=combined_flags["upload"],
    )
    defaults = self._collect_defaults()
    if effective_jobs:
        defaults.visual_settings = self._resolve_job_visual_settings(effective_jobs[0])
    return build_run_plan(tasks=tracking_tasks, defaults=defaults, modules=modules, config=runtime_config)


def _patched_preview_plan_v2(self: DashboardApp) -> None:
    self._save_state()
    preview_widget = getattr(self, "start_preview", None)
    if preview_widget is None:
        return
    preview_widget.delete("1.0", "end")
    if not self.window_tasks:
        preview_widget.insert("1.0", "暂无队列任务。")
        return
    try:
        run_plan = self._build_tracking_plan_for_queue()
    except Exception as exc:
        preview_widget.insert("1.0", f"计划构建失败: {exc}")
        return
    preview_widget.insert("1.0", "\n".join(preview_run_plan(run_plan)))


def _patched_on_visual_preset_change_v2(self: DashboardApp, *_args: Any) -> None:
    preset_name = _visual_preset_choice_to_value(self.visual_preset_var.get())
    if preset_name == "none":
        self._set_visual_fields_enabled(True)
        self.visual_preset_hint_var.set("")
        if hasattr(self, "visual_preset_hint_label"):
            self.visual_preset_hint_label.grid_remove()
        return
    settings = self._visual_preset_to_settings(preset_name)
    self._apply_visual_settings_to_form(settings, preset_choice=_visual_preset_value_to_choice(preset_name))
    self._set_visual_fields_enabled(False)
    self.visual_preset_hint_var.set(VISUAL_PRESET_HINT_TEMPLATE.format(name=preset_name))
    if hasattr(self, "visual_preset_hint_label"):
        self.visual_preset_hint_label.grid()


def _patched_save_current_visual_preset_v2(self: DashboardApp) -> None:
    name = simpledialog.askstring("保存视觉预设", "请输入预设名称：", parent=self)
    clean_name = str(name or "").strip()
    if not clean_name:
        return
    presets = dict(self.visual_presets)
    presets[clean_name] = self._serialize_visual_preset(clean_name)
    _write_json_object(VISUAL_PRESETS_FILE, presets)
    self.visual_presets = _load_visual_presets()
    self._refresh_visual_preset_controls()
    self.visual_preset_var.set(_visual_preset_value_to_choice(clean_name))
    self._on_visual_preset_change()
    self._log(f"[视觉] 已保存预设: {clean_name}")


def _patched_delete_selected_visual_preset_v2(self: DashboardApp) -> None:
    preset_name = _visual_preset_choice_to_value(self.visual_preset_var.get())
    if preset_name == "none":
        messagebox.showinfo("删除视觉预设", "当前没有选中可删除的预设。")
        return
    presets = dict(self.visual_presets)
    if preset_name not in presets:
        self.visual_preset_var.set(VISUAL_PRESET_NONE)
        self._on_visual_preset_change()
        return
    del presets[preset_name]
    _write_json_object(VISUAL_PRESETS_FILE, presets)
    self.visual_presets = _load_visual_presets()
    self.visual_preset_var.set(VISUAL_PRESET_NONE)
    self._refresh_visual_preset_controls()
    self._on_visual_preset_change()
    self._log(f"[视觉] 已删除预设: {preset_name}")


def _patched_save_visual_settings_v2(self: DashboardApp) -> None:
    config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
    config["visual_settings"] = self._collect_visual_settings()
    self.scheduler_config = save_scheduler_settings(config, SCHEDULER_CONFIG_FILE)
    self._save_state()
    self._log("[视觉] 已保存高级视觉设置")


def _patched_apply_visual_preset_mega_bass_v2(self: DashboardApp) -> None:
    self.visual_preset_var.set(_visual_preset_value_to_choice("MegaBass"))
    self._on_visual_preset_change()
    self._save_visual_settings()
    self._log("[视觉] 已套用 MegaBass")


def _patched_start_real_flow_v2(self: DashboardApp) -> None:
    if self.run_queue.is_empty():
        messagebox.showerror("无法开始运行", "队列为空，请先添加至少一个分组任务。")
        return
    self._refresh_task_tree()
    if not self.window_tasks:
        messagebox.showerror("无法开始运行", "队列中没有有效的窗口任务。")
        return

    queue_defaults = self._current_upload_defaults_model()
    effective_jobs = self._effective_run_queue_jobs()
    tracking_plan = self._build_tracking_plan_for_queue()
    runtime_config = self._runtime_config_for_job(effective_jobs[0])
    self._persist_prompt_form_for_active_tasks()
    self._write_run_snapshot(config=runtime_config, run_plan=tracking_plan)
    self._prepare_run_result_tracking(tracking_plan)

    def handle_queue_progress(event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip()
        if not event_type:
            return
        if event_type == "log":
            message = str(event.get("message") or "").strip()
            if message:
                self._log(message)
            return

        group_tag = str(event.get("group_tag") or "").strip()
        label = str(event.get("label") or "").strip()
        serial_value = int(event.get("serial") or 0)

        if event_type == "job_started":
            self._run_phase = f"队列 {event.get('job_index', 0)}/{event.get('job_total', 0)}"
            self._run_current_item = f"{group_tag} | {int(event.get('window_count') or 0)} 个窗口"
            self._run_current_ratio = 0.0
            self._log(f"[队列] {event.get('job_index', 0)}/{event.get('job_total', 0)} -> {group_tag} | windows={event.get('window_serials', [])}")
            return
        if event_type == "prepare_started":
            self._run_phase = "生成文案 / 渲染"
            self._run_current_item = f"{group_tag} | 准备上传素材"
            self._run_current_ratio = 0.0
            return
        if event_type == "prepare_finished":
            self._run_phase = "准备完成"
            self._run_current_item = f"{group_tag} | 等待上传"
            self._run_current_ratio = 0.0
            return
        if event_type == "window_started":
            if label and not bool(event.get("has_prepare_step", True)):
                self._run_progress_step_done(label, "render")
            if group_tag and serial_value:
                self._mark_run_stage(group_tag, serial_value, "upload", "running", "上传中")
            self._run_phase = "上传"
            self._run_current_item = f"{group_tag} / 窗口 {serial_value}"
            self._run_current_ratio = 0.0
            return
        if event_type == "window_finished":
            detail = str(event.get("detail") or event.get("stage") or "").strip()
            if group_tag and serial_value:
                self._mark_run_stage(group_tag, serial_value, "upload", "success" if bool(event.get("success")) else "failed", detail)
            if label:
                self._run_progress_step_done(label, "upload")
            self._run_phase = "上传完成" if bool(event.get("success")) else "上传失败"
            self._run_current_item = f"{group_tag} / 窗口 {serial_value} | {detail or '完成'}"
            self._run_current_ratio = 0.0
            return
        if event_type == "group_finished":
            self._run_phase = "分组完成"
            self._run_current_item = f"{group_tag} | success={int(event.get('success_count') or 0)} | failed={int(event.get('failed_count') or 0)}"
            return
        if event_type == "job_error":
            self._run_phase = "分组失败"
            self._run_current_item = f"{group_tag} | {str(event.get('detail') or '').strip()}"

    def job() -> bool:
        queue_results = asyncio.run(
            execute_run_queue(
                self.run_queue,
                queue_defaults,
                control=self.execution_control,
                before_job_callback=self._apply_job_prompt_bindings,
                build_run_plan_for_job=lambda queue_job: self._build_run_plan_for_job(queue_job),
                execution_result_callback=lambda _job, execution: self._ingest_execution_result(execution),
                progress_callback=handle_queue_progress,
                log=self._log,
            )
        )
        if self._cancel_requested:
            return False
        failures: list[str] = []
        for job_result in queue_results:
            for item in job_result.get("results", []) or []:
                if bool(item.get("success")):
                    continue
                detail = str(item.get("detail") or item.get("stage") or "upload failed").strip()
                if detail and detail not in failures:
                    failures.append(detail)
        if failures:
            raise RuntimeError(" | ".join(failures[:3]))
        return False

    task_name = " + ".join(tracking_plan.modules.labels()) or "RunQueue"
    self._run_background(
        job,
        task_name=task_name,
        total_items=len(tracking_plan.tasks),
        include_upload=bool(tracking_plan.modules.upload and (tracking_plan.modules.metadata or tracking_plan.modules.render)),
    )


def _patched_run_background_v2(self: DashboardApp, func, *, task_name: str, total_items: int, include_upload: bool = False) -> None:
    if self._has_active_background_work():
        messagebox.showinfo("任务进行中", self._current_run_summary())
        return

    self._start_run_tracking(task_name, total_items, include_upload=include_upload)

    def runner() -> None:
        try:
            deferred_finish = bool(func())
        except WorkflowCancelledError as exc:
            error_text = str(exc)
            self._log(f"[取消] {error_text}")
            self._post_ui_action(lambda text=error_text: self._finish_run_tracking(success=False, summary=text, cancelled=True))
            return
        except Exception as exc:
            error_text = str(exc)
            self._log(f"[错误] {error_text}")
            self._post_ui_action(lambda text=error_text: self._finish_run_tracking(success=False, summary=text))
            self._post_ui_action(lambda text=error_text: messagebox.showerror("任务失败", text))
            return
        if deferred_finish:
            return
        if self._cancel_requested:
            self._post_ui_action(lambda: self._finish_run_tracking(success=False, summary="当前批次已取消", cancelled=True))
        else:
            self._post_ui_action(lambda: self._finish_run_tracking(success=True, summary="任务已执行完成"))

    self.worker_thread = threading.Thread(target=runner, daemon=True)
    self.worker_thread.start()


def _patched_refresh_schedule_controls_v3(self: DashboardApp) -> None:
    add_state = "normal" if bool(self.add_schedule_enabled_var.get()) else "disabled"
    for widget in (
        getattr(self, "add_schedule_date_menu", None),
        getattr(self, "add_schedule_time_menu", None),
        getattr(self, "add_schedule_timezone_menu", None),
    ):
        if widget is not None:
            widget.configure(state=add_state)

    default_enabled = bool(self.schedule_enabled_var.get())
    default_state = "normal" if default_enabled else "disabled"
    for widget in (
        getattr(self, "schedule_date_menu", None),
        getattr(self, "schedule_time_menu", None),
        getattr(self, "schedule_timezone_menu", None),
        getattr(self, "schedule_interval_entry", None),
    ):
        if widget is not None:
            widget.configure(state=default_state)

    details_frame = getattr(self, "default_schedule_details_frame", None)
    if details_frame is not None:
        if default_enabled:
            details_frame.grid()
        else:
            details_frame.grid_remove()


def _patched_build_upload_tab_v3(self: DashboardApp) -> None:
    base_tab = self.tabview.tab("上传")
    base_tab.grid_columnconfigure(0, weight=1)
    base_tab.grid_rowconfigure(0, weight=1)
    tab = ctk.CTkScrollableFrame(base_tab)
    tab.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
    tab.grid_columnconfigure(0, weight=1)

    # ── 浏览器提供者选择条 ──
    provider_frame = ctk.CTkFrame(tab)
    provider_frame.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
    provider_frame.grid_columnconfigure(1, weight=1)
    ctk.CTkLabel(provider_frame, text="浏览器管理器:").grid(row=0, column=0, sticky="w", padx=(16, 8), pady=8)
    self.browser_provider_menu = ctk.CTkOptionMenu(
        provider_frame,
        variable=self.browser_provider_var,
        values=["auto", "hubstudio", "bitbrowser"],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
        command=lambda _val: self._on_browser_provider_change(),
        width=180,
    )
    self.browser_provider_menu.grid(row=0, column=1, sticky="w", padx=0, pady=8)
    self._browser_status_label = ctk.CTkLabel(provider_frame, text="", text_color="#9fb2c8")
    self._browser_status_label.grid(row=0, column=2, sticky="w", padx=(16, 16), pady=8)

    add_frame = ctk.CTkFrame(tab)
    add_frame.grid(row=1, column=0, sticky="ew", padx=8, pady=(4, 8))
    for column in range(5):
        add_frame.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(add_frame, text="添加分组到队列", font=ctk.CTkFont(size=24, weight="bold")).grid(
        row=0, column=0, columnspan=5, sticky="w", padx=16, pady=(14, 12)
    )
    ctk.CTkLabel(add_frame, text="分组").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
    ctk.CTkLabel(add_frame, text="窗口").grid(row=1, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(add_frame, text="素材目录").grid(row=1, column=2, sticky="w", padx=8, pady=(0, 6))
    self.current_group_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.current_group_var,
        values=[""],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.current_group_menu.grid(row=2, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
    ctk.CTkEntry(add_frame, textvariable=self.queue_windows_var).grid(row=2, column=1, sticky="ew", padx=8, pady=(0, 12))
    source_bar = ctk.CTkFrame(add_frame, fg_color="transparent")
    source_bar.grid(row=2, column=2, columnspan=2, sticky="ew", padx=(8, 16), pady=(0, 12))
    source_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkEntry(source_bar, textvariable=self.source_dir_override_var).grid(
        row=0, column=0, sticky="ew", padx=(0, 8), pady=0
    )
    ctk.CTkButton(
        source_bar,
        text="选择文件夹",
        command=lambda: self._browse_directory_var(self.source_dir_override_var, "选择素材目录"),
    ).grid(row=0, column=1, sticky="e", pady=0)

    ctk.CTkButton(add_frame, text="刷新分组", command=self._refresh_groups).grid(
        row=3, column=0, sticky="w", padx=(16, 8), pady=(0, 12)
    )
    ctk.CTkLabel(add_frame, text="API模板").grid(row=4, column=0, sticky="w", padx=(16, 8), pady=(0, 6))
    ctk.CTkLabel(add_frame, text="提示词模板").grid(row=4, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(add_frame, text="视觉模板").grid(row=4, column=2, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(add_frame, text="路径模板").grid(row=4, column=3, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(add_frame, text="每窗口上传数量").grid(row=4, column=4, sticky="w", padx=(8, 16), pady=(0, 6))
    self.queue_api_template_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_api_template_var,
        values=["default"],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_api_template_menu.grid(row=5, column=0, sticky="ew", padx=(16, 8), pady=(0, 12))
    self.queue_prompt_template_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_prompt_template_var,
        values=["default"],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_prompt_template_menu.grid(row=5, column=1, sticky="ew", padx=8, pady=(0, 12))
    self.queue_visual_mode_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_visual_mode_var,
        values=[QUEUE_VISUAL_RANDOM, QUEUE_VISUAL_MANUAL, *sorted(self.visual_presets.keys(), key=str.lower)],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_visual_mode_menu.grid(row=5, column=2, sticky="ew", padx=8, pady=(0, 12))
    self.queue_path_template_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_path_template_var,
        values=[""],
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_path_template_menu.grid(row=5, column=3, sticky="ew", padx=8, pady=(0, 12))
    self.queue_videos_per_window_menu = ctk.CTkOptionMenu(
        add_frame,
        variable=self.queue_videos_per_window_var,
        values=QUEUE_VIDEOS_PER_WINDOW_VALUES,
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.queue_videos_per_window_menu.grid(row=5, column=4, sticky="ew", padx=(8, 16), pady=(0, 12))

    steps_frame = ctk.CTkFrame(add_frame, fg_color="transparent")
    steps_frame.grid(row=6, column=0, columnspan=5, sticky="ew", padx=16, pady=(0, 12))
    for column in range(4):
        steps_frame.grid_columnconfigure(column, weight=1 if column else 0)
    ctk.CTkLabel(steps_frame, text="执行步骤:").grid(row=0, column=0, sticky="w", padx=(0, 12), pady=0)
    ctk.CTkCheckBox(
        steps_frame,
        text="生成文案(标题/简介/缩略图)",
        variable=self._step_generate_var,
    ).grid(row=0, column=1, sticky="w", padx=(0, 12), pady=0)
    ctk.CTkCheckBox(
        steps_frame,
        text="剪辑视频",
        variable=self._step_render_var,
    ).grid(row=0, column=2, sticky="w", padx=(0, 12), pady=0)
    ctk.CTkCheckBox(
        steps_frame,
        text="上传YouTube",
        variable=self._step_upload_var,
    ).grid(row=0, column=3, sticky="w", pady=0)
    ctk.CTkButton(add_frame, text="+ 添加到队列", command=self._add_current_group_to_queue, height=38).grid(
        row=7, column=0, columnspan=5, sticky="ew", padx=16, pady=(0, 12)
    )

    table_frame = ctk.CTkFrame(tab)
    table_frame.grid(row=2, column=0, sticky="nsew", padx=8, pady=8)
    table_frame.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(table_frame, text="运行队列", font=ctk.CTkFont(size=24, weight="bold")).grid(
        row=0, column=0, sticky="w", padx=16, pady=(14, 10)
    )
    header_row = ctk.CTkFrame(table_frame, fg_color="transparent")
    header_row.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 6))
    for column, title in enumerate(["序号", "分组", "窗口", "浏览器", "每窗口数量", "API模板", "提示词模板", "视觉模板", "路径模板", "操作"]):
        header_row.grid_columnconfigure(column, weight=1 if column in {1, 2, 3, 4, 5, 6, 7, 8} else 0)
        ctk.CTkLabel(header_row, text=title, font=ctk.CTkFont(weight="bold"), anchor="w").grid(
            row=0, column=column, sticky="w", padx=6, pady=(0, 4)
        )
    self.queue_list_frame = ctk.CTkScrollableFrame(table_frame, height=250)
    self.queue_list_frame.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 14))
    self.queue_list_frame.grid_columnconfigure(0, weight=1)

    default_frame = ctk.CTkFrame(tab)
    default_frame.grid(row=3, column=0, sticky="ew", padx=8, pady=8)
    default_frame.grid_columnconfigure(0, weight=1)
    toggle_bar = ctk.CTkFrame(default_frame, fg_color="transparent")
    toggle_bar.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 6))
    toggle_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkButton(
        toggle_bar,
        textvariable=self.default_rules_toggle_text_var,
        command=self._toggle_default_rules_panel,
        anchor="w",
        fg_color="transparent",
        hover_color="#24324a",
    ).grid(row=0, column=0, sticky="ew")
    self.default_rules_body = ctk.CTkFrame(default_frame, fg_color="transparent")
    self.default_rules_body.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
    for column in range(4):
        self.default_rules_body.grid_columnconfigure(column, weight=1)
    ctk.CTkLabel(self.default_rules_body, text="可见性").grid(row=0, column=0, sticky="w", padx=(8, 8), pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="分类").grid(row=0, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="儿童内容").grid(row=0, column=2, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_rules_body, text="AI 内容").grid(row=0, column=3, sticky="w", padx=(8, 16), pady=(0, 6))
    ctk.CTkOptionMenu(
        self.default_rules_body,
        variable=self.default_visibility_var,
        values=VISIBILITY_VALUES,
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    ).grid(row=1, column=0, sticky="ew", padx=(8, 8), pady=(0, 12))
    ctk.CTkOptionMenu(
        self.default_rules_body,
        variable=self.default_category_var,
        values=CATEGORY_VALUES,
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    ).grid(row=1, column=1, sticky="ew", padx=8, pady=(0, 12))
    ctk.CTkOptionMenu(
        self.default_rules_body,
        variable=self.default_kids_var,
        values=YES_NO_VALUES,
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    ).grid(row=1, column=2, sticky="ew", padx=8, pady=(0, 12))
    ctk.CTkOptionMenu(
        self.default_rules_body,
        variable=self.default_ai_var,
        values=YES_NO_VALUES,
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    ).grid(row=1, column=3, sticky="ew", padx=(8, 16), pady=(0, 12))
    self.schedule_enabled_checkbox = ctk.CTkCheckBox(
        self.default_rules_body,
        text="启用定时发布",
        variable=self.schedule_enabled_var,
    )
    self.schedule_enabled_checkbox.grid(row=2, column=0, sticky="w", padx=(8, 8), pady=(0, 8))
    ctk.CTkCheckBox(
        self.default_rules_body,
        text="上传完成后自动关闭窗口",
        variable=self.upload_auto_close_var,
    ).grid(row=2, column=1, columnspan=3, sticky="w", padx=8, pady=(0, 8))
    self.default_schedule_details_frame = ctk.CTkFrame(self.default_rules_body, fg_color="transparent")
    self.default_schedule_details_frame.grid(row=3, column=0, columnspan=4, sticky="ew", padx=8, pady=(0, 4))
    for column in range(7):
        self.default_schedule_details_frame.grid_columnconfigure(column, weight=1 if column in {1, 3, 5} else 0)
    ctk.CTkLabel(self.default_schedule_details_frame, text="首个发布时间:").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 6))
    ctk.CTkLabel(self.default_schedule_details_frame, text="日期").grid(row=0, column=1, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_schedule_details_frame, text="时间").grid(row=0, column=3, sticky="w", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_schedule_details_frame, text="时区").grid(row=0, column=5, sticky="w", padx=8, pady=(0, 6))
    self.schedule_date_menu = ctk.CTkOptionMenu(
        self.default_schedule_details_frame,
        variable=self.schedule_date_var,
        values=_schedule_date_values(),
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.schedule_date_menu.grid(row=1, column=1, sticky="ew", padx=8, pady=(0, 10))
    self.schedule_time_menu = ctk.CTkOptionMenu(
        self.default_schedule_details_frame,
        variable=self.schedule_time_var,
        values=_schedule_time_values(),
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.schedule_time_menu.grid(row=1, column=3, sticky="ew", padx=8, pady=(0, 10))
    self.schedule_timezone_menu = ctk.CTkOptionMenu(
        self.default_schedule_details_frame,
        variable=self.schedule_timezone_var,
        values=SCHEDULE_TIMEZONE_VALUES,
        font=_dashboard_theme_font(),
        dropdown_font=_dashboard_theme_font(),
    )
    self.schedule_timezone_menu.grid(row=1, column=5, sticky="ew", padx=8, pady=(0, 10))
    ctk.CTkLabel(self.default_schedule_details_frame, text="后续间隔:").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(0, 6))
    self.schedule_interval_entry = ctk.CTkEntry(self.default_schedule_details_frame, textvariable=self.schedule_interval_var)
    self.schedule_interval_entry.grid(row=2, column=1, sticky="ew", padx=8, pady=(0, 6))
    ctk.CTkLabel(self.default_schedule_details_frame, text="分钟（多个视频依次间隔发布）").grid(
        row=2, column=2, columnspan=5, sticky="w", padx=8, pady=(0, 6)
    )
    self.default_schedule_hint_label = ctk.CTkLabel(
        self.default_schedule_details_frame,
        textvariable=self.default_schedule_hint_var,
        text_color="#9fb2c8",
        justify="left",
    )
    self.default_schedule_hint_label.grid(row=3, column=0, columnspan=7, sticky="w", padx=(0, 8), pady=(0, 10))

    control_frame = ctk.CTkFrame(tab)
    control_frame.grid(row=3, column=0, sticky="ew", padx=8, pady=(8, 16))
    for column in range(3):
        control_frame.grid_columnconfigure(column, weight=1)
    ctk.CTkButton(control_frame, text="▶ 开始运行", command=self._start_real_flow, height=40).grid(
        row=0, column=0, sticky="ew", padx=(16, 8), pady=14
    )
    self.pause_button = ctk.CTkButton(
        control_frame,
        textvariable=self.pause_button_text_var,
        command=self._toggle_pause_current_task,
        height=40,
    )
    self.pause_button.grid(row=0, column=1, sticky="ew", padx=8, pady=14)
    self.cancel_button = ctk.CTkButton(
        control_frame,
        text="⏹ 取消当前批次",
        command=self._cancel_current_task,
        height=40,
        fg_color="#7a1f1f",
        hover_color="#932525",
    )
    self.cancel_button.grid(row=0, column=2, sticky="ew", padx=(8, 16), pady=14)

    self._refresh_default_rules_panel()
    self._bind_scroll_frame_wheel(tab, base_tab, tab, add_frame, table_frame, default_frame, control_frame)


def _patched_apply_current_group_context_v3(self: DashboardApp, *, preserve_selection: bool) -> None:
    current_group = str(self.current_group_var.get() or "").strip()
    live_serials = [
        int(str(serial).strip())
        for serial in (getattr(self, "_live_groups", {}) or {}).get(current_group, [])
        if str(serial).strip().isdigit()
    ]
    available_serials = live_serials or [int(info.serial) for info in self.group_catalog.get(current_group, [])]
    valid_serials = set(available_serials)
    if preserve_selection:
        existing = [serial for serial in _dashboard_parse_serials(self.queue_windows_var.get()) if serial in valid_serials]
        serials = existing or available_serials
    else:
        serials = available_serials
    self.queue_windows_var.set(", ".join(str(serial) for serial in serials))

    self._apply_queue_path_template_selection()

    # 只有在分组真正切换时才自动设置API/提示词模板（不是路径模板变化触发的）
    prev_group = getattr(self, "_last_applied_group", None)
    if not preserve_selection or prev_group != current_group:
        api_name, content_name = self._queue_template_defaults_for_group(current_group)
        self.queue_api_template_var.set(api_name)
        self.queue_prompt_template_var.set(content_name)
    self._last_applied_group = current_group


def _on_browser_provider_change_impl(self: DashboardApp) -> None:
    """处理浏览器管理器切换 — 更新 runtime provider 并刷新分组"""
    choice = str(self.browser_provider_var.get() or "auto").strip().lower()
    set_runtime_provider(choice if choice != "auto" else None)
    self._save_state()
    self._log(f"[浏览器] 切换到: {choice}")
    # 同步更新 upload_config.json
    try:
        cfg_path = UPLOAD_CONFIG_FILE
        cfg_data: dict = {}
        if cfg_path.exists():
            cfg_data = json.loads(cfg_path.read_text(encoding="utf-8"))
        if choice == "auto":
            cfg_data.pop("browser_provider", None)
            cfg_data.pop("browser_api", None)
        else:
            from browser_api import DEFAULT_BROWSER_SETTINGS
            cfg_data["browser_provider"] = choice
            defaults = DEFAULT_BROWSER_SETTINGS.get(choice, {})
            cfg_data["browser_api"] = {
                "provider": choice,
                "base_url": defaults.get("base_url", ""),
                "list_endpoint": defaults.get("list_endpoint", ""),
                "open_endpoint": defaults.get("open_endpoint", ""),
                "stop_endpoint": defaults.get("stop_endpoint", ""),
                "list_payload": defaults.get("list_payload", {}),
                "open_payload": defaults.get("open_payload", {}),
                "stop_payload": defaults.get("stop_payload", {}),
                "open_payload_id_key": defaults.get("open_payload_id_key", ""),
                "stop_payload_id_key": defaults.get("stop_payload_id_key", ""),
            }
        cfg_path.write_text(json.dumps(cfg_data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        self._log(f"[浏览器] 保存配置失败: {exc}")

    # 后台探测可用性并更新状态标签
    import threading as _thr

    def _probe_and_refresh():
        try:
            status_parts: list[str] = []
            probes = probe_browser_providers()
            for name, alive in probes.items():
                status_parts.append(f"{name}: {'在线' if alive else '离线'}")
            status_text = "  |  ".join(status_parts)
        except Exception:
            status_text = ""
        try:
            self.after(0, lambda: _update_status(status_text))
        except RuntimeError:
            pass

    def _update_status(text: str):
        if getattr(self, "_closing", False):
            return
        try:
            if not self.winfo_exists():
                return
        except Exception:
            return
        label = getattr(self, "_browser_status_label", None)
        if label is not None:
            label.configure(text=text)
        self._refresh_groups()

    _thr.Thread(target=_probe_and_refresh, daemon=True).start()


def _patched_refresh_groups_v3(self: DashboardApp) -> None:
    """刷新分组 — 在后台线程查询浏览器管理器 API，避免阻塞UI"""

    def _bg_query():
        try:
            return _build_live_group_catalog_from_browser()
        except Exception as exc:
            return {}, {}, str(exc)

    def _apply_result(result):
        if getattr(self, "_closing", False):
            return
        try:
            if not self.winfo_exists():
                return
        except Exception:
            return
        group_catalog, live_groups, browser_error = result
        self.group_catalog = group_catalog
        self._live_groups = live_groups
        groups = list(self.group_catalog.keys()) or list(self._live_groups.keys()) or [""]
        for attr_name in ("current_group_menu", "prompt_group_menu"):
            menu = getattr(self, attr_name, None)
            if menu is not None:
                menu.configure(values=groups)
        if self.current_group_var.get() not in groups:
            self.current_group_var.set(groups[0])
        if self.prompt_group_var.get() not in groups:
            self.prompt_group_var.set(self.current_group_var.get())
        self._apply_current_group_context(preserve_selection=True)
        self._refresh_queue_display()
        self._save_state()
        if browser_error is not None:
            provider_name = get_runtime_provider() or "auto"
            self._log(f"[分组] 浏览器管理器({provider_name})实时读取失败，已回退到配置文件: {browser_error}")

    import threading as _thr

    def _worker():
        result = _bg_query()
        try:
            self.after(0, lambda: _apply_result(result))
        except RuntimeError:
            # tkinter 主循环未运行时（如测试环境），仅更新数据不操作GUI
            group_catalog, live_groups, _ = result
            self.group_catalog = group_catalog
            self._live_groups = live_groups

    _thr.Thread(target=_worker, daemon=True).start()


def _patched_refresh_queue_display_v3(self: DashboardApp) -> None:
    if self.queue_list_frame is None:
        return
    for child in self.queue_list_frame.winfo_children():
        child.destroy()
    if self.run_queue.is_empty():
        ctk.CTkLabel(self.queue_list_frame, text="队列为空，请在上方添加分组任务", text_color="#9fb2c8").pack(
            anchor="w",
            padx=12,
            pady=12,
        )
        return
    for summary in self.run_queue.get_summary():
        row = ctk.CTkFrame(self.queue_list_frame)
        row.pack(fill="x", padx=4, pady=4)
        for column in range(10):
            row.grid_columnconfigure(column, weight=1 if column in {1, 2, 3, 4, 5, 6, 7, 8} else 0)
        ctk.CTkLabel(row, text=str(summary["index"] + 1), anchor="w").grid(row=0, column=0, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["group_tag"], anchor="w").grid(row=0, column=1, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["window_serials_text"], anchor="w").grid(row=0, column=2, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary.get("browser_provider", "auto"), anchor="w").grid(row=0, column=3, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=str(summary["videos_per_window"]), anchor="w").grid(row=0, column=4, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["api_template"], anchor="w").grid(row=0, column=5, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["prompt_template"], anchor="w").grid(row=0, column=6, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["visual_mode"], anchor="w").grid(row=0, column=7, sticky="w", padx=6, pady=8)
        ctk.CTkLabel(row, text=summary["path_template"], anchor="w").grid(row=0, column=8, sticky="w", padx=6, pady=8)
        action_bar = ctk.CTkFrame(row, fg_color="transparent")
        action_bar.grid(row=0, column=9, sticky="e", padx=6, pady=8)
        ctk.CTkButton(action_bar, text="设置", width=72, command=lambda index=summary["index"]: self._open_window_override_dialog(index)).pack(side="left", padx=(0, 6))
        ctk.CTkButton(
            action_bar,
            text="删除",
            width=72,
            command=lambda index=summary["index"]: self._remove_queue_job(index),
            fg_color="#7a1f1f",
            hover_color="#932525",
        ).pack(side="left")


def _patched_add_current_group_to_queue_v3(self: DashboardApp) -> None:
    current_group = str(self.current_group_var.get() or "").strip()
    if not current_group:
        messagebox.showerror("无法加入队列", "请先选择一个分组。")
        return
    selected_serials = _dashboard_parse_serials(self.queue_windows_var.get())
    if not selected_serials:
        messagebox.showerror("无法加入队列", "请先填写至少一个窗口号。")
        return

    visual_mode = _visual_mode_to_value(self.queue_visual_mode_var.get())
    visual_settings: dict[str, Any] | None = None
    if visual_mode == "manual":
        visual_settings = dict(self._collect_visual_settings())
    elif visual_mode not in {"random", "manual"}:
        visual_settings = dict(self.visual_presets.get(visual_mode) or {})

    path_name, path_template = get_path_template(self.queue_path_template_var.get(), templates=self.path_templates)
    resolved_source = str(self.source_dir_override_var.get() or "").strip() or resolve_source_dir(path_template, group_tag=current_group)
    selected_modules = self._module_names_for_new_job()
    selected_steps: list[str] = []
    if "metadata" in selected_modules:
        selected_steps.append("generate")
    if "render" in selected_modules:
        selected_steps.append("render")
    if "upload" in selected_modules:
        selected_steps.append("upload")
    try:
        videos_per_window = max(1, int(str(self.queue_videos_per_window_var.get() or "1").strip() or "1"))
    except ValueError:
        videos_per_window = 1
    self.queue_videos_per_window_var.set(str(videos_per_window))
    browser_provider = str(self.browser_provider_var.get() or "auto").strip().lower() or "auto"
    job = GroupJob(
        group_tag=current_group,
        window_serials=selected_serials,
        source_dir=resolved_source,
        prompt_template=str(self.queue_prompt_template_var.get() or "default").strip() or "default",
        api_template=str(self.queue_api_template_var.get() or "default").strip() or "default",
        visual_mode=visual_mode,
        visual_settings=visual_settings,
        path_template=path_name,
        videos_per_window=videos_per_window,
        upload_defaults=self._current_upload_defaults_model(),
        steps=selected_steps,
        modules=selected_modules,
        browser_provider=browser_provider,
    )
    self.run_queue.add_job(job)
    self._apply_current_group_context(preserve_selection=False)
    self._refresh_task_tree()


def _patched_window_default_values_v3(self: DashboardApp, job: GroupJob, serial: int) -> dict[str, str]:
    info = self._find_window_info(job.group_tag, serial)
    defaults = self._current_upload_defaults_model()
    override = job.get_window_override(serial)
    visibility = str((override.visibility if override and override.visibility else defaults.visibility) or "private").strip() or "private"
    category = str((override.category if override and override.category else defaults.category) or "Music").strip() or "Music"
    kids = str((override.kids_content if override and override.kids_content else _yes_no_from_bool(defaults.is_for_kids)) or "no").strip() or "no"
    ai = str((override.ai_content if override and override.ai_content else (defaults.ai_content or defaults.altered_content or "yes")) or "yes").strip() or "yes"
    notify = str(
        (override.notify_subscribers if override and override.notify_subscribers else _yes_no_from_bool(bool(defaults.notify_subscribers)))
        or "no"
    ).strip() or "no"
    schedule_mode, schedule_date, schedule_time, _ = _resolve_window_schedule_override(override, defaults, visibility)
    ypp = str((override.ypp if override and override.ypp else _yes_no_from_bool(bool(info.is_ypp))) or "no").strip() or "no"
    return {
        "ypp": ypp,
        "visibility": visibility,
        "category": category,
        "kids_content": kids,
        "ai_content": ai,
        "notify_subscribers": notify,
        "schedule_mode": schedule_mode,
        "schedule_date": schedule_date or str(defaults.schedule_date or "").strip() or _default_schedule_date(),
        "schedule_time": schedule_time or str(defaults.schedule_time or "").strip() or "06:00",
    }


def _patched_open_window_override_dialog_v3(self: DashboardApp, index: int) -> None:
    if index < 0 or index >= len(self.run_queue.jobs):
        return
    job = self.run_queue.jobs[index]
    dialog = ctk.CTkToplevel(self)
    dialog.title(f"单独设置 - {job.group_tag}")
    dialog.geometry("1320x500")
    dialog.transient(self)
    dialog.grab_set()
    dialog.grid_columnconfigure(0, weight=1)
    dialog.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(
        dialog,
        text=f"分组: {job.group_tag}  |  窗口: {', '.join(str(item) for item in job.window_serials)}",
        font=ctk.CTkFont(size=18, weight="bold"),
    ).grid(row=0, column=0, sticky="w", padx=16, pady=(16, 10))

    table = ctk.CTkScrollableFrame(dialog)
    table.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))
    for column in range(10):
        table.grid_columnconfigure(column, weight=1)
    headers = ["窗口", "YPP", "可见性", "分类", "儿童内容", "AI 内容", "通知订阅者", "定时发布", "日期", "时间"]
    for column, title in enumerate(headers):
        ctk.CTkLabel(table, text=title, font=ctk.CTkFont(weight="bold")).grid(row=0, column=column, sticky="w", padx=6, pady=(4, 8))

    row_contexts: dict[int, dict[str, Any]] = {}
    default_cache = {serial: self._window_default_values(job, serial) for serial in job.window_serials}

    def refresh_row(serial: int) -> None:
        context = row_contexts[serial]
        if context["updating"]:
            return
        context["updating"] = True
        try:
            visibility = str(context["vars"]["visibility"].get() or "").strip()
            mode_value = _window_schedule_mode_to_value(context["vars"]["schedule_mode"].get())
            default_visibility = str(default_cache[serial]["visibility"] or "private").strip() or "private"
            fallback_visibility = default_visibility if default_visibility != "schedule" else "private"

            if visibility == "schedule" and mode_value == "none":
                context["vars"]["schedule_mode"].set(_window_schedule_mode_to_choice("default"))
                mode_value = "default"
            elif visibility != "schedule" and mode_value != "none":
                context["vars"]["schedule_mode"].set(_window_schedule_mode_to_choice("none"))
                mode_value = "none"

            if mode_value in {"default", "custom"} and visibility != "schedule":
                context["vars"]["visibility"].set("schedule")
                visibility = "schedule"
            elif mode_value == "none" and visibility == "schedule":
                context["vars"]["visibility"].set(fallback_visibility)
                visibility = fallback_visibility

            if mode_value == "custom":
                if not str(context["vars"]["schedule_date"].get() or "").strip():
                    context["vars"]["schedule_date"].set(default_cache[serial]["schedule_date"])
                if not str(context["vars"]["schedule_time"].get() or "").strip():
                    context["vars"]["schedule_time"].set(default_cache[serial]["schedule_time"])

            widget_state = "normal" if mode_value == "custom" and visibility == "schedule" else "disabled"
            context["date_widget"].configure(state=widget_state)
            context["time_widget"].configure(state=widget_state)
        finally:
            context["updating"] = False

    for row_index, serial in enumerate(job.window_serials, start=1):
        defaults = default_cache[serial]
        ctk.CTkLabel(table, text=str(serial)).grid(row=row_index, column=0, sticky="w", padx=6, pady=6)
        vars_for_row = {
            "ypp": ctk.StringVar(value=defaults["ypp"]),
            "visibility": ctk.StringVar(value=defaults["visibility"]),
            "category": ctk.StringVar(value=defaults["category"]),
            "kids_content": ctk.StringVar(value=defaults["kids_content"]),
            "ai_content": ctk.StringVar(value=defaults["ai_content"]),
            "notify_subscribers": ctk.StringVar(value=defaults["notify_subscribers"]),
            "schedule_mode": ctk.StringVar(value=_window_schedule_mode_to_choice(defaults["schedule_mode"])),
            "schedule_date": ctk.StringVar(value=defaults["schedule_date"]),
            "schedule_time": ctk.StringVar(value=defaults["schedule_time"]),
        }
        widgets = [
            ctk.CTkOptionMenu(table, variable=vars_for_row["ypp"], values=YES_NO_VALUES, font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["visibility"], values=VISIBILITY_VALUES, font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["category"], values=CATEGORY_VALUES, font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["kids_content"], values=YES_NO_VALUES, font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["ai_content"], values=YES_NO_VALUES, font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["notify_subscribers"], values=YES_NO_VALUES, font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["schedule_mode"], values=WINDOW_SCHEDULE_MODE_VALUES, font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["schedule_date"], values=_schedule_date_values(), font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
            ctk.CTkOptionMenu(table, variable=vars_for_row["schedule_time"], values=_schedule_time_values(), font=_dashboard_theme_font(), dropdown_font=_dashboard_theme_font()),
        ]
        for column, widget in enumerate(widgets, start=1):
            widget.grid(row=row_index, column=column, sticky="ew", padx=6, pady=6)
        row_contexts[serial] = {
            "vars": vars_for_row,
            "date_widget": widgets[7],
            "time_widget": widgets[8],
            "updating": False,
        }
        vars_for_row["visibility"].trace_add("write", lambda *_args, serial=serial: refresh_row(serial))
        vars_for_row["schedule_mode"].trace_add("write", lambda *_args, serial=serial: refresh_row(serial))
        refresh_row(serial)

    def reset_to_defaults() -> None:
        for serial, context in row_contexts.items():
            defaults = default_cache[serial]
            context["vars"]["ypp"].set(defaults["ypp"])
            context["vars"]["visibility"].set(defaults["visibility"])
            context["vars"]["category"].set(defaults["category"])
            context["vars"]["kids_content"].set(defaults["kids_content"])
            context["vars"]["ai_content"].set(defaults["ai_content"])
            context["vars"]["notify_subscribers"].set(defaults["notify_subscribers"])
            context["vars"]["schedule_mode"].set(_window_schedule_mode_to_choice(defaults["schedule_mode"]))
            context["vars"]["schedule_date"].set(defaults["schedule_date"])
            context["vars"]["schedule_time"].set(defaults["schedule_time"])
            refresh_row(serial)

    def save_dialog() -> None:
        job.clear_window_overrides()
        for serial, context in row_contexts.items():
            defaults = default_cache[serial]
            vars_for_row = context["vars"]
            visibility = str(vars_for_row["visibility"].get() or "").strip()
            schedule_mode = _window_schedule_mode_to_value(vars_for_row["schedule_mode"].get())
            schedule_date = str(vars_for_row["schedule_date"].get() or "").strip()
            schedule_time = str(vars_for_row["schedule_time"].get() or "").strip()
            if visibility != "schedule":
                schedule_mode = "none"
                schedule_date = ""
                schedule_time = ""
            elif schedule_mode != "custom":
                schedule_date = ""
                schedule_time = ""
            override = WindowOverride(
                serial=int(serial),
                ypp="" if vars_for_row["ypp"].get() == defaults["ypp"] else vars_for_row["ypp"].get(),
                visibility="" if visibility == defaults["visibility"] else visibility,
                category="" if vars_for_row["category"].get() == defaults["category"] else vars_for_row["category"].get(),
                kids_content="" if vars_for_row["kids_content"].get() == defaults["kids_content"] else vars_for_row["kids_content"].get(),
                ai_content="" if vars_for_row["ai_content"].get() == defaults["ai_content"] else vars_for_row["ai_content"].get(),
                notify_subscribers="" if vars_for_row["notify_subscribers"].get() == defaults["notify_subscribers"] else vars_for_row["notify_subscribers"].get(),
                schedule_mode="" if schedule_mode == defaults["schedule_mode"] else schedule_mode,
                schedule_date="" if schedule_date == defaults["schedule_date"] else schedule_date,
                schedule_time="" if schedule_time == defaults["schedule_time"] else schedule_time,
            )
            job.set_window_override(override)
        dialog.destroy()
        self._refresh_task_tree()

    button_bar = ctk.CTkFrame(dialog, fg_color="transparent")
    button_bar.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 16))
    ctk.CTkButton(button_bar, text="全部使用默认值", command=reset_to_defaults).pack(side="left", padx=6)
    ctk.CTkButton(button_bar, text="确定", command=save_dialog).pack(side="left", padx=6)
    ctk.CTkButton(button_bar, text="取消", command=dialog.destroy).pack(side="left", padx=6)


def _patched_build_window_tasks_from_job_v3(self: DashboardApp, job: GroupJob) -> list[WindowTask]:
    upload_defaults = UploadDefaults.from_dict(self._current_upload_defaults_model().to_dict())
    if job.upload_defaults:
        upload_defaults = UploadDefaults.from_dict(job.upload_defaults.to_dict())
    notify_subscribers = bool(upload_defaults.notify_subscribers)
    tasks: list[WindowTask] = []
    seen_serials: set[int] = set()
    quantity = max(1, int(job.videos_per_window or 1))
    for raw_serial in job.window_serials:
        serial = int(raw_serial)
        if serial in seen_serials:
            continue
        seen_serials.add(serial)
        info = self._find_window_info(job.group_tag, serial)
        override = job.get_window_override(serial)
        visibility = str((override.visibility if override and override.visibility else upload_defaults.visibility) or "private").strip() or "private"
        category = str((override.category if override and override.category else upload_defaults.category) or "Music").strip() or "Music"
        kids_value = override.kids_content if override and override.kids_content else _yes_no_from_bool(upload_defaults.is_for_kids)
        ai_value = override.ai_content if override and override.ai_content else (upload_defaults.ai_content or upload_defaults.altered_content or "yes")
        notify_value = override.notify_subscribers if override and override.notify_subscribers else _yes_no_from_bool(notify_subscribers)
        _schedule_mode, _schedule_date, _schedule_time, schedule_text = _resolve_window_schedule_override(override, upload_defaults, visibility)
        tasks.append(
            create_task(
                tag=job.group_tag,
                serial=serial,
                quantity=quantity,
                is_ypp=_bool_from_yes_no(override.ypp) if override and str(override.ypp or "").strip() else bool(info.is_ypp),
                title="",
                visibility=visibility,
                category=category,
                made_for_kids=_bool_from_yes_no(kids_value),
                altered_content=_bool_from_yes_no(ai_value),
                notify_subscribers=_bool_from_yes_no(notify_value),
                scheduled_publish_at=schedule_text,
                schedule_timezone=str(upload_defaults.timezone or "").strip() if schedule_text else "",
                source_dir=str(job.source_dir or "").strip(),
                channel_name=info.channel_name,
            )
        )
    return tasks


def _patched_start_real_flow_v3(self: DashboardApp) -> None:
    if self.run_queue.is_empty():
        messagebox.showerror("无法开始运行", "队列为空，请先添加至少一个分组任务。")
        return
    self._refresh_task_tree()
    if not self.window_tasks:
        messagebox.showerror("无法开始运行", "队列中没有有效的窗口任务。")
        return

    queue_defaults = self._current_upload_defaults_model()
    effective_jobs = self._effective_run_queue_jobs()
    tracking_plan = self._build_tracking_plan_for_queue()
    runtime_config = self._runtime_config_for_job(effective_jobs[0])
    self._persist_prompt_form_for_active_tasks()
    self._write_run_snapshot(config=runtime_config, run_plan=tracking_plan)
    self._prepare_run_result_tracking(tracking_plan)

    def handle_queue_progress(event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip()
        if not event_type:
            return
        if event_type == "log":
            message = str(event.get("message") or "").strip()
            if message:
                self._log(message)
            return

        group_tag = str(event.get("group_tag") or "").strip()
        label = str(event.get("label") or "").strip()
        serial_value = int(event.get("serial") or 0)
        slot_index = int(event.get("slot_index") or 1)
        total_slots = int(event.get("total_slots") or 1)

        if event_type == "job_started":
            self._run_phase = f"队列 {event.get('job_index', 0)}/{event.get('job_total', 0)}"
            self._run_current_item = f"{group_tag} | {int(event.get('window_count') or 0)} 个窗口"
            self._run_current_ratio = 0.0
            self._log(f"[队列] {event.get('job_index', 0)}/{event.get('job_total', 0)} -> {group_tag} | windows={event.get('window_serials', [])}")
            return
        if event_type == "prepare_started":
            self._run_phase = "生成文案 / 渲染"
            self._run_current_item = f"{group_tag} | 准备上传素材"
            self._run_current_ratio = 0.0
            return
        if event_type == "prepare_finished":
            self._run_phase = "准备完成"
            self._run_current_item = f"{group_tag} | 等待上传"
            self._run_current_ratio = 0.0
            return
        if event_type == "window_started":
            if label and not bool(event.get("has_prepare_step", True)):
                self._run_progress_step_done(label, "render")
            if group_tag and serial_value:
                self._mark_run_stage(
                    group_tag,
                    serial_value,
                    "upload",
                    "running",
                    "上传中",
                    slot_index=slot_index,
                    total_slots=total_slots,
                )
            slot_suffix = f" [{slot_index}/{total_slots}]" if total_slots > 1 else ""
            self._run_phase = "上传"
            self._run_current_item = f"{group_tag} / 窗口 {serial_value}{slot_suffix}"
            self._run_current_ratio = 0.0
            return
        if event_type == "window_finished":
            detail = str(event.get("detail") or event.get("stage") or "").strip()
            if group_tag and serial_value:
                self._mark_run_stage(
                    group_tag,
                    serial_value,
                    "upload",
                    "success" if bool(event.get("success")) else "failed",
                    detail,
                    slot_index=slot_index,
                    total_slots=total_slots,
                )
            if label:
                self._run_progress_step_done(label, "upload")
            slot_suffix = f" [{slot_index}/{total_slots}]" if total_slots > 1 else ""
            self._run_phase = "上传完成" if bool(event.get("success")) else "上传失败"
            self._run_current_item = f"{group_tag} / 窗口 {serial_value}{slot_suffix} | {detail or '完成'}"
            self._run_current_ratio = 0.0
            return
        if event_type == "group_finished":
            self._run_phase = "分组完成"
            self._run_current_item = f"{group_tag} | success={int(event.get('success_count') or 0)} | failed={int(event.get('failed_count') or 0)}"
            return
        if event_type == "job_error":
            self._run_phase = "分组失败"
            self._run_current_item = f"{group_tag} | {str(event.get('detail') or '').strip()}"

    def job() -> bool:
        queue_results = asyncio.run(
            execute_run_queue(
                self.run_queue,
                queue_defaults,
                control=self.execution_control,
                before_job_callback=self._apply_job_prompt_bindings,
                build_run_plan_for_job=lambda queue_job: self._build_run_plan_for_job(queue_job),
                execution_result_callback=lambda _job, execution: self._ingest_execution_result(execution),
                progress_callback=handle_queue_progress,
                log=self._log,
            )
        )
        if self._cancel_requested:
            return False
        failures: list[str] = []
        for job_result in queue_results:
            for item in job_result.get("results", []) or []:
                if bool(item.get("success")):
                    continue
                detail = str(item.get("detail") or item.get("stage") or "upload failed").strip()
                if detail and detail not in failures:
                    failures.append(detail)
        if failures:
            raise RuntimeError(" | ".join(failures[:3]))
        return False

    task_name = " + ".join(tracking_plan.modules.labels()) or "RunQueue"
    self._run_background(
        job,
        task_name=task_name,
        total_items=len(tracking_plan.tasks),
        include_upload=bool(tracking_plan.modules.upload and (tracking_plan.modules.metadata or tracking_plan.modules.render)),
    )


DashboardApp.__init__ = _patched_dashboard_init_v2
DashboardApp.destroy = _patched_dashboard_destroy_v2
DashboardApp._setup_cjk_font = _patched_setup_cjk_font_v2
DashboardApp._apply_cjk_font_to_widgets = _patched_apply_cjk_font_to_widgets_v2
DashboardApp._build_variables = _patched_build_variables_v2
DashboardApp._bind_variable_events = _patched_bind_variable_events_v2
DashboardApp._save_state = _patched_save_state_v2
DashboardApp._refresh_bindings_box = _patched_refresh_bindings_box_v2
DashboardApp._module_names_for_new_job = _patched_module_names_for_new_job_v2
DashboardApp._run_background = _patched_run_background_v2
DashboardApp._apply_run_status = _patched_apply_run_status_v2
DashboardApp._start_run_tracking = _patched_start_run_tracking_v2
DashboardApp._finish_run_tracking = _patched_finish_run_tracking_v2
DashboardApp._build_layout = _patched_build_layout_v2
DashboardApp._build_upload_tab = _patched_build_upload_tab_v3
DashboardApp._build_prompt_tab = _patched_build_prompt_tab_v2
DashboardApp._build_paths_tab = _patched_build_paths_tab_v2
DashboardApp._build_visual_tab = _patched_build_visual_tab_v2
DashboardApp._build_log_tab = _patched_build_log_tab_v2
DashboardApp._refresh_default_rules_panel = _patched_refresh_default_rules_panel_v2
DashboardApp._refresh_schedule_controls = _patched_refresh_schedule_controls_v3
DashboardApp._toggle_default_rules_panel = _patched_toggle_default_rules_panel_v2
DashboardApp._browse_directory_var = _patched_browse_directory_var_v2
DashboardApp._refresh_path_template_controls = _patched_refresh_path_template_controls_v2
DashboardApp._load_path_template_into_editor = _patched_load_path_template_into_editor_v2
DashboardApp._on_path_template_listbox_select = _patched_on_path_template_listbox_select_v2
DashboardApp._new_path_template = _patched_new_path_template_v2
DashboardApp._save_current_path_template = _patched_save_current_path_template_v2
DashboardApp._delete_current_path_template = _patched_delete_current_path_template_v2
DashboardApp._apply_queue_path_template_selection = _patched_apply_queue_path_template_selection_v1
DashboardApp._on_queue_path_template_change = _patched_on_queue_path_template_change_v2
DashboardApp._apply_current_group_context = _patched_apply_current_group_context_v3
DashboardApp._refresh_groups = _patched_refresh_groups_v3
DashboardApp._on_browser_provider_change = _on_browser_provider_change_impl
DashboardApp._remove_queue_job = _patched_remove_queue_job_v2
DashboardApp._refresh_queue_display = _patched_refresh_queue_display_v3
DashboardApp._add_current_group_to_queue = _patched_add_current_group_to_queue_v3
DashboardApp._window_default_values = _patched_window_default_values_v3
DashboardApp._open_window_override_dialog = _patched_open_window_override_dialog_v3
DashboardApp._build_window_tasks_from_job = _patched_build_window_tasks_from_job_v3
DashboardApp._runtime_config_for_job = _patched_runtime_config_for_job_v2
DashboardApp._build_run_plan_for_job = _patched_build_run_plan_for_job_v2
DashboardApp._build_tracking_plan_for_queue = _patched_build_tracking_plan_for_queue_v2
DashboardApp._preview_plan = _patched_preview_plan_v2
DashboardApp._on_visual_preset_change = _patched_on_visual_preset_change_v2
DashboardApp._save_current_visual_preset = _patched_save_current_visual_preset_v2
DashboardApp._delete_selected_visual_preset = _patched_delete_selected_visual_preset_v2
DashboardApp._save_visual_settings = _patched_save_visual_settings_v2
DashboardApp._apply_visual_preset_mega_bass = _patched_apply_visual_preset_mega_bass_v2
DashboardApp._start_real_flow = _patched_start_real_flow_v3


def main() -> int:
    app = DashboardApp()
    app.mainloop()
    return 0
