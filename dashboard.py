#!/usr/bin/env python3
"""
统一控制台。

目标：
1. 把常用的渲染调度、上传、路径配置放到一个入口里。
2. 默认使用“全随机视觉”策略，降低同质化风险。
3. 保留旧版高级入口，避免一次性重写全部 GUI 逻辑。
"""

from __future__ import annotations

import base64
import io
import json
import mimetypes
import os
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import List

import customtkinter as ctk
from PIL import Image, ImageGrab
from tkinter import filedialog, messagebox

from content_generation import analyze_audience_screenshot
from group_upload_workflow import (
    load_upload_batch_settings,
    parse_serials_text,
    prepare_group_upload_batch,
    prepare_window_task_upload_batch,
    save_upload_batch_settings,
)
from path_helpers import default_scheduler_config, normalize_scheduler_config
from prompt_studio import (
    build_site_preview,
    clone_json,
    default_api_preset,
    default_content_template,
    guess_cover_names,
    load_generation_map as load_generation_map_file,
    load_prompt_studio_config,
    pick_content_template_name,
    render_master_prompt,
    save_generation_map as save_generation_map_file,
    save_prompt_studio_config,
    sync_manifest_from_generation_map,
)
from upload_window_planner import (
    SCOPE_MANUAL,
    SCOPE_MULTI_GROUP,
    SCOPE_SAME_GROUP,
    build_window_upload_plan,
    derive_tags_and_skip_channels,
    parse_manual_tasks,
    save_window_upload_plan,
)
from utils import get_all_tags, get_tag_info


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

SCRIPT_DIR = Path(__file__).parent
SCHEDULER_SCRIPT = SCRIPT_DIR / "daily_scheduler.py"
UPLOAD_SCRIPT = SCRIPT_DIR / "batch_upload.py"
BULK_UPLOAD_SCRIPT = SCRIPT_DIR / "bulk_upload.py"
GROUP_UPLOAD_SCRIPT = SCRIPT_DIR / "group_upload_batch.py"
LEGACY_RENDERER = SCRIPT_DIR / "app.py"
LEGACY_SCHEDULER = SCRIPT_DIR / "scheduler_gui.py"
SCHEDULER_CONFIG_FILE = SCRIPT_DIR / "scheduler_config.json"
DASHBOARD_STATE_FILE = SCRIPT_DIR / "dashboard_state.json"
PROMPT_STUDIO_FILE = SCRIPT_DIR / "config" / "prompt_studio.json"
CHANNEL_MAPPING_FILE = SCRIPT_DIR / "config" / "channel_mapping.json"
UPLOAD_BATCH_SETTINGS_FILE = SCRIPT_DIR / "config" / "upload_batch_settings.json"

COLOR_MAP = {
    "随机": "random",
    "白金色": "WhiteGold",
    "铂金色": "Platinum",
    "珍珠白": "Pearl",
    "象牙白": "Ivory",
    "银色": "Silver",
    "香槟色": "Champagne",
    "玫瑰金": "RoseGold",
    "鼠尾草绿": "SageGreen",
    "雾蓝色": "DustyBlue",
    "冷蓝色": "CoolBlue",
    "午夜蓝": "MidnightBlue",
    "酒红色": "Burgundy",
    "日落色": "Sunset",
}
ZOOM_MAP = {"随机": "random", "慢": "slow", "中": "normal", "快": "fast", "关闭": "off"}
STYLE_MAP = {"随机": "random", "柱状": "bar", "镜像柱状": "bar_mirror", "波浪": "wave", "环形": "circular"}
LETTERBOX_MAP = {"随机": "random", "关闭": "false", "开启": "true"}
TINT_MAP = {
    "随机": "random",
    "无": "none",
    "暖色": "warm",
    "冷色": "cool",
    "复古": "vintage",
    "深蓝夜晚": "blue_night",
    "金色": "golden",
    "森林绿调": "forest",
}
PARTICLE_MAP = {
    "随机": "random",
    "无": "none",
    "浮尘/光斑": "dust_bokeh",
    "萤火虫": "fireflies",
    "飘雪": "snow",
    "雨丝": "rain",
}
TEXT_POS_MAP = {
    "底部居中": "bottom_center",
    "左下角": "bottom_left",
    "顶部居中": "top_center",
    "正中央": "center",
}
TEXT_STYLE_MAP = {
    "随机": "random",
    "经典": "Classic",
    "发光": "Glow",
    "霓虹": "Neon",
    "粗体": "Bold",
    "方框": "Box",
}

PROMPT_PROVIDER_VALUES = ["openai_compatible", "anthropic", "gemini"]
PROMPT_LANGUAGE_VALUES = ["zh-TW", "zh-CN", "en-US", "ja-JP", "ko-KR"]
PROMPT_COUNT_VALUES = [str(i) for i in range(1, 11)]
PROMPT_AUTO_IMAGE_VALUES = ["0", "1"]
GROUP_UPLOAD_MODE_VALUES = ["site_api", "legacy"]
GROUP_UPLOAD_VISIBILITY_VALUES = ["public", "private", "unlisted", "schedule"]
GROUP_UPLOAD_CATEGORY_VALUES = [
    "Music",
    "People & Blogs",
    "Education",
    "Entertainment",
    "News & Politics",
    "Gaming",
    "Sports",
    "Travel & Events",
]
TASK_MODE_VALUES = ["upload_only", "render_only", "render_and_upload"]
UPLOAD_SOURCE_MODE_VALUES = ["render_output", "ready_folder"]
UPLOAD_METADATA_MODE_VALUES = ["prompt_api", "daily_content"]
BOOL_OVERRIDE_VALUES = ["yes", "no"]

UPLOAD_ENTRY_MODE_LABELS = {
    "window_plan": "按窗口任务上传",
    "single_channel": "单频道上传",
    "batch_tags": "多赛道任务清单",
    "group_folder": "同分组现成视频",
}
UPLOAD_ENTRY_MODE_TABS = {
    "window_plan": "上传",
    "single_channel": "上传",
    "batch_tags": "上传",
    "group_folder": "上传",
}
WINDOW_SCOPE_LABELS = {
    SCOPE_SAME_GROUP: "同一分组的一批窗口",
    SCOPE_MULTI_GROUP: "多个分组，各自列窗口",
    SCOPE_MANUAL: "逐窗口单独配置",
}


def normalize_upload_entry_mode(value: str) -> str:
    aliases = {
        "window_plan": "window_plan",
        "window": "window_plan",
        "窗口": "window_plan",
        "窗口任务": "window_plan",
        "按窗口任务上传": "window_plan",
        "single_channel": "single_channel",
        "single": "single_channel",
        "单频道": "single_channel",
        "单频道上传": "single_channel",
        "batch_tags": "batch_tags",
        "bulk_tags": "batch_tags",
        "多赛道": "batch_tags",
        "多赛道任务清单": "batch_tags",
        "group_folder": "group_folder",
        "grouped_folder": "group_folder",
        "分组批量上传": "group_folder",
        "同分组现成视频": "group_folder",
    }
    return aliases.get(str(value or "").strip(), "window_plan")


def normalize_yes_no_choice(value: str, fallback: str = "no") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"yes", "true", "1"}:
        return "yes"
    if normalized in {"no", "false", "0"}:
        return "no"
    return fallback


def normalize_choice(value: str, allowed: list[str], fallback: str) -> str:
    current = str(value or "").strip()
    return current if current in allowed else fallback


def load_scheduler_config() -> dict:
    if SCHEDULER_CONFIG_FILE.exists():
        try:
            with open(SCHEDULER_CONFIG_FILE, "r", encoding="utf-8") as f:
                return normalize_scheduler_config(json.load(f), SCRIPT_DIR)
        except Exception:
            pass
    return default_scheduler_config(SCRIPT_DIR)


def save_scheduler_config(cfg: dict) -> None:
    with open(SCHEDULER_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def load_dashboard_state() -> dict:
    defaults = {
        "date": datetime.now().strftime("%m%d"),
        "tag": "",
        "upload_tag": "",
        "generation_tag": "",
        "generation_channel": "",
        "prompt_tag": "",
        "batch_tags_text": "",
        "upload_entry_mode": "window_plan",
        "today_task_mode": "render_and_upload",
        "upload_source_mode": "render_output",
        "upload_metadata_mode": "prompt_api",
        "upload_fill_text": True,
        "upload_fill_thumbnails": True,
        "upload_sync_daily_content": True,
        "upload_picker_tag": "",
        "upload_window_ypp_override": "no",
        "upload_window_title_override": "",
        "upload_window_visibility_override": "public",
        "upload_window_category_override": "Music",
        "upload_window_made_for_kids_override": "no",
        "upload_window_altered_content_override": "yes",
        "song_count": "1",
        "channel": "",
        "window_scope_mode": SCOPE_SAME_GROUP,
        "same_group_serials_text": "",
        "multi_group_plan_text": "",
        "manual_window_plan_text": "",
        "audio_workers": 2,
        "video_workers": 2,
        "render_enabled": True,
        "randomize_effects": True,
        "auto_upload": True,
        "auto_close_browser": True,
        "auto_sync_manifest": True,
        "spectrum": True,
        "timeline": True,
        "letterbox": "随机",
        "zoom": "随机",
        "color_spectrum": "随机",
        "color_timeline": "随机",
        "style": "随机",
        "particle": "随机",
        "particle_opacity": "0.55",
        "film_grain": True,
        "grain_strength": "10",
        "vignette": False,
        "soft_focus": False,
        "soft_focus_sigma": "1.2",
        "color_tint": "随机",
        "text": "",
        "text_pos": "底部居中",
        "text_size": "60",
        "text_style": "随机",
        "api_preset_name": "默认API模板",
        "content_template_name": "默认内容模板",
        "prompt_audience_shot_path": "",
        "prompt_audience_parsed_text": "",
    }
    if DASHBOARD_STATE_FILE.exists():
        try:
            with open(DASHBOARD_STATE_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            defaults.update(loaded)
        except Exception:
            pass
    return defaults


def save_dashboard_state(state: dict) -> None:
    with open(DASHBOARD_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def normalize_mmdd(text: str) -> str:
    raw = str(text).strip()
    if "." in raw:
        month, day = raw.split(".", 1)
        return f"{int(month):02d}{int(day):02d}"
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 3:
        return digits.zfill(4)
    if len(digits) == 4:
        return digits
    raise ValueError("日期格式应为 MMDD 或 M.DD，例如 0312 / 3.12")


def open_folder(path: str) -> None:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    os.startfile(str(target))


def resolve_local_path(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else (SCRIPT_DIR / path)


def open_target(path: str | Path) -> None:
    target = Path(path)
    if target.suffix:
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_text("", encoding="utf-8")
    else:
        target.mkdir(parents=True, exist_ok=True)
    os.startfile(str(target))


def load_channel_name_map() -> dict[str, str]:
    if not CHANNEL_MAPPING_FILE.exists():
        return {}
    try:
        with open(CHANNEL_MAPPING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    result: dict[str, str] = {}
    for info in data.get("channels", {}).values():
        serial = info.get("serial_number")
        name = str(info.get("channel_name") or "").strip()
        if serial is not None:
            result[str(serial)] = name
    return result


class CommandCenterApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("YouTube 自动化统一控制台")
        self.geometry("1180x860")
        self.minsize(1060, 760)

        self.scheduler_cfg = load_scheduler_config()
        self.prompt_cfg = load_prompt_studio_config(PROMPT_STUDIO_FILE)
        self.upload_batch_cfg = load_upload_batch_settings(UPLOAD_BATCH_SETTINGS_FILE)
        self.channel_name_map = load_channel_name_map()
        self.ui_state = load_dashboard_state()
        self.process: subprocess.Popen | None = None
        self.log_thread: threading.Thread | None = None
        self.manual_widgets: List[ctk.CTkBaseClass] = []
        self.api_preset_menu: ctk.CTkOptionMenu | None = None
        self.content_template_menu: ctk.CTkOptionMenu | None = None
        self.prompt_master_box: ctk.CTkTextbox | None = None
        self.prompt_title_library_box: ctk.CTkTextbox | None = None
        self.prompt_preview_box: ctk.CTkTextbox | None = None
        self.prompt_audience_parsed_box: ctk.CTkTextbox | None = None
        self.prompt_audience_preview_label: ctk.CTkLabel | None = None
        self.prompt_audience_preview_image: ctk.CTkImage | None = None
        self.prompt_audience_image_data_url: str | None = None
        self.prompt_audience_analysis_busy = False
        self._syncing_task_mode = False
        self.quick_basic_card: ctk.CTkFrame | None = None
        self.gen_title_box: ctk.CTkTextbox | None = None
        self.gen_desc_box: ctk.CTkTextbox | None = None
        self.gen_covers_box: ctk.CTkTextbox | None = None
        self.gen_ab_titles_box: ctk.CTkTextbox | None = None
        self.batch_tags_box: ctk.CTkTextbox | None = None
        self.batch_path_preview_box: ctk.CTkTextbox | None = None
        self.multi_group_plan_box: ctk.CTkTextbox | None = None
        self.manual_window_plan_box: ctk.CTkTextbox | None = None
        self.window_plan_preview_box: ctk.CTkTextbox | None = None
        self.upload_window_buttons_frame: ctk.CTkScrollableFrame | None = None
        self.upload_tag_menu: ctk.CTkOptionMenu | None = None
        self.channel_menu: ctk.CTkOptionMenu | None = None
        self.upload_picker_tag_menu: ctk.CTkOptionMenu | None = None
        self.group_upload_tag_menu: ctk.CTkOptionMenu | None = None
        self.group_upload_preview_box: ctk.CTkTextbox | None = None

        self._build_variables()
        self._reset_upload_window_overrides()
        self._sync_task_mode_to_module_flags(initial=True)
        self._build_ui()
        self._load_tags()
        self._sync_upload_channels()
        self._sync_generation_channels()
        self._refresh_preset_menus()
        self._load_api_preset()
        self._load_content_template()
        self._restore_prompt_audience_state()
        self._load_generation_entry(silent=True)
        self._refresh_batch_path_preview()
        self._preview_window_plan(show_error=False)
        self._refresh_quick_upload_mode_summary()
        self._apply_randomize_state()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_variables(self) -> None:
        self.date_var = ctk.StringVar(value=self.ui_state["date"])
        self.tag_var = ctk.StringVar(value=self.ui_state["tag"])
        self.upload_tag_var = ctk.StringVar(value=self.ui_state["upload_tag"] or self.ui_state["tag"])
        self.generation_tag_var = ctk.StringVar(value=self.ui_state["generation_tag"] or self.ui_state["upload_tag"] or self.ui_state["tag"])
        self.prompt_tag_var = ctk.StringVar(value=self.ui_state["prompt_tag"] or self.ui_state["tag"])
        self.song_count_var = ctk.StringVar(value=self.ui_state["song_count"])
        self.today_task_mode_var = ctk.StringVar(value=self.ui_state.get("today_task_mode", "render_and_upload"))
        self.upload_source_mode_var = ctk.StringVar(value=self.ui_state.get("upload_source_mode", "render_output"))
        self.upload_metadata_mode_var = ctk.StringVar(value=self.ui_state.get("upload_metadata_mode", "prompt_api"))
        self.upload_entry_mode_var = ctk.StringVar(
            value=normalize_upload_entry_mode(self.ui_state.get("upload_entry_mode", "window_plan"))
        )
        self.channel_var = ctk.StringVar(value=self.ui_state["channel"])
        self.window_scope_mode_var = ctk.StringVar(value=self.ui_state.get("window_scope_mode", SCOPE_SAME_GROUP))
        self.same_group_serials_var = ctk.StringVar(value=self.ui_state.get("same_group_serials_text", ""))
        self.upload_picker_tag_var = ctk.StringVar(
            value=self.ui_state.get("upload_picker_tag") or self.ui_state["upload_tag"] or self.ui_state["tag"]
        )
        self.upload_window_ypp_override_var = ctk.StringVar(
            value=normalize_yes_no_choice(self.ui_state.get("upload_window_ypp_override", "no"), "no")
        )
        self.upload_window_title_override_var = ctk.StringVar(value=self.ui_state.get("upload_window_title_override", ""))
        self.upload_window_visibility_override_var = ctk.StringVar(
            value=normalize_choice(self.ui_state.get("upload_window_visibility_override", "public"), GROUP_UPLOAD_VISIBILITY_VALUES, "public")
        )
        self.upload_window_category_override_var = ctk.StringVar(
            value=normalize_choice(self.ui_state.get("upload_window_category_override", "Music"), GROUP_UPLOAD_CATEGORY_VALUES, "Music")
        )
        self.upload_window_made_for_kids_override_var = ctk.StringVar(
            value=normalize_yes_no_choice(self.ui_state.get("upload_window_made_for_kids_override", "no"), "no")
        )
        self.upload_window_altered_content_override_var = ctk.StringVar(
            value=normalize_yes_no_choice(self.ui_state.get("upload_window_altered_content_override", "yes"), "yes")
        )
        self.generation_channel_var = ctk.StringVar(value=self.ui_state["generation_channel"])
        self.audio_workers_var = ctk.IntVar(value=int(self.ui_state["audio_workers"]))
        self.video_workers_var = ctk.IntVar(value=int(self.ui_state["video_workers"]))
        self.render_enabled_var = ctk.BooleanVar(value=bool(self.ui_state["render_enabled"]))
        self.randomize_effects_var = ctk.BooleanVar(value=bool(self.ui_state["randomize_effects"]))
        self.auto_upload_var = ctk.BooleanVar(value=bool(self.ui_state["auto_upload"]))
        self.auto_close_browser_var = ctk.BooleanVar(value=bool(self.ui_state["auto_close_browser"]))
        self.auto_sync_manifest_var = ctk.BooleanVar(value=bool(self.ui_state["auto_sync_manifest"]))
        self.spectrum_var = ctk.BooleanVar(value=bool(self.ui_state["spectrum"]))
        self.timeline_var = ctk.BooleanVar(value=bool(self.ui_state["timeline"]))
        self.letterbox_var = ctk.StringVar(value=self.ui_state["letterbox"])
        self.zoom_var = ctk.StringVar(value=self.ui_state["zoom"])
        self.color_spectrum_var = ctk.StringVar(value=self.ui_state["color_spectrum"])
        self.color_timeline_var = ctk.StringVar(value=self.ui_state["color_timeline"])
        self.style_var = ctk.StringVar(value=self.ui_state["style"])
        self.particle_var = ctk.StringVar(value=self.ui_state["particle"])
        self.particle_opacity_var = ctk.StringVar(value=self.ui_state["particle_opacity"])
        self.film_grain_var = ctk.BooleanVar(value=bool(self.ui_state["film_grain"]))
        self.grain_strength_var = ctk.StringVar(value=self.ui_state["grain_strength"])
        self.vignette_var = ctk.BooleanVar(value=bool(self.ui_state["vignette"]))
        self.soft_focus_var = ctk.BooleanVar(value=bool(self.ui_state["soft_focus"]))
        self.soft_focus_sigma_var = ctk.StringVar(value=self.ui_state["soft_focus_sigma"])
        self.color_tint_var = ctk.StringVar(value=self.ui_state["color_tint"])
        self.text_var = ctk.StringVar(value=self.ui_state["text"])
        self.text_pos_var = ctk.StringVar(value=self.ui_state["text_pos"])
        self.text_size_var = ctk.StringVar(value=self.ui_state["text_size"])
        self.text_style_var = ctk.StringVar(value=self.ui_state["text_style"])
        self.music_dir_var = ctk.StringVar(value=self.scheduler_cfg.get("music_dir", ""))
        self.image_dir_var = ctk.StringVar(value=self.scheduler_cfg.get("base_image_dir", ""))
        self.output_dir_var = ctk.StringVar(value=self.scheduler_cfg.get("output_root", ""))
        self.ffmpeg_var = ctk.StringVar(value=self.scheduler_cfg.get("ffmpeg_bin") or "ffmpeg")
        self.status_var = ctk.StringVar(value="就绪")
        self.prompt_status_var = ctk.StringVar(value="提示词模板未修改")
        self.generation_status_var = ctk.StringVar(value="等待加载 generation_map")
        self.api_preset_name_var = ctk.StringVar(value=self.ui_state["api_preset_name"])
        self.content_template_name_var = ctk.StringVar(value=self.ui_state["content_template_name"])
        self.api_provider_var = ctk.StringVar(value="")
        self.api_key_var = ctk.StringVar(value="")
        self.api_base_url_var = ctk.StringVar(value="")
        self.api_model_var = ctk.StringVar(value="")
        self.api_temperature_var = ctk.StringVar(value="0.9")
        self.api_max_tokens_var = ctk.StringVar(value="16000")
        self.api_auto_image_var = ctk.StringVar(value="0")
        self.image_base_url_var = ctk.StringVar(value="")
        self.image_api_key_var = ctk.StringVar(value="")
        self.image_model_var = ctk.StringVar(value="")
        self.image_concurrency_var = ctk.StringVar(value="3")
        self.prompt_audience_shot_path_var = ctk.StringVar(value=self.ui_state.get("prompt_audience_shot_path", ""))
        self.content_music_genre_var = ctk.StringVar(value="")
        self.content_angle_var = ctk.StringVar(value="")
        self.content_audience_var = ctk.StringVar(value="")
        self.content_output_language_var = ctk.StringVar(value="zh-TW")
        self.content_title_count_var = ctk.StringVar(value="3")
        self.content_desc_count_var = ctk.StringVar(value="1")
        self.content_thumb_count_var = ctk.StringVar(value="3")
        self.content_title_min_var = ctk.StringVar(value="80")
        self.content_title_max_var = ctk.StringVar(value="95")
        self.content_desc_len_var = ctk.StringVar(value="300")
        self.content_tag_range_var = ctk.StringVar(value="10-20")
        self.generation_is_ypp_var = ctk.BooleanVar(value=False)
        self.generation_set_var = ctk.StringVar(value="1")
        self.batch_status_var = ctk.StringVar(value="未设置多赛道任务")
        self.group_upload_tag_var = ctk.StringVar(value=self.upload_batch_cfg.get("tag") or self.ui_state["upload_tag"] or self.ui_state["tag"])
        self.group_upload_source_dir_var = ctk.StringVar(value=self.upload_batch_cfg.get("source_video_dir", ""))
        self.group_upload_thumb_dir_var = ctk.StringVar(value=self.upload_batch_cfg.get("source_thumbnail_dir", ""))
        self.group_upload_serials_var = ctk.StringVar(value=self.upload_batch_cfg.get("selected_serials_text", ""))
        self.group_upload_mode_var = ctk.StringVar(value=self.upload_batch_cfg.get("generation_mode", "site_api"))
        self.group_upload_visibility_var = ctk.StringVar(value=self.upload_batch_cfg.get("visibility", "public"))
        self.group_upload_category_var = ctk.StringVar(value=self.upload_batch_cfg.get("category", "Music"))
        self.group_upload_made_for_kids_var = ctk.BooleanVar(value=bool(self.upload_batch_cfg.get("made_for_kids", False)))
        self.group_upload_altered_content_var = ctk.BooleanVar(value=bool(self.upload_batch_cfg.get("altered_content", True)))
        self.group_upload_schedule_enabled_var = ctk.BooleanVar(value=bool(self.upload_batch_cfg.get("schedule_enabled", False)))
        self.group_upload_schedule_start_var = ctk.StringVar(value=self.upload_batch_cfg.get("schedule_start", ""))
        self.group_upload_schedule_interval_var = ctk.StringVar(value=str(self.upload_batch_cfg.get("schedule_interval_minutes", 60)))
        self.group_upload_status_var = ctk.StringVar(value="等待载入分组批量上传计划")
        self.window_plan_status_var = ctk.StringVar(value="等待设置今日上传窗口")
        self.quick_upload_mode_status_var = ctk.StringVar(value="当前上传方式：按窗口任务上传")
        self.upload_fill_text_var = ctk.BooleanVar(value=bool(self.ui_state.get("upload_fill_text", True)))
        self.upload_fill_thumbnails_var = ctk.BooleanVar(value=bool(self.ui_state.get("upload_fill_thumbnails", True)))
        self.upload_sync_daily_content_var = ctk.BooleanVar(value=bool(self.ui_state.get("upload_sync_daily_content", True)))

        for traced_var in (
            self.date_var,
            self.tag_var,
            self.music_dir_var,
            self.image_dir_var,
            self.output_dir_var,
        ):
            traced_var.trace_add("write", self._handle_batch_preview_change)

        for traced_var in (
            self.today_task_mode_var,
            self.upload_source_mode_var,
            self.upload_metadata_mode_var,
            self.date_var,
            self.tag_var,
            self.upload_tag_var,
            self.upload_picker_tag_var,
            self.channel_var,
            self.window_scope_mode_var,
            self.same_group_serials_var,
            self.render_enabled_var,
            self.auto_upload_var,
            self.upload_entry_mode_var,
            self.upload_fill_text_var,
            self.upload_fill_thumbnails_var,
            self.upload_sync_daily_content_var,
            self.group_upload_tag_var,
            self.group_upload_source_dir_var,
            self.group_upload_serials_var,
            self.group_upload_visibility_var,
            self.group_upload_category_var,
            self.group_upload_made_for_kids_var,
            self.group_upload_altered_content_var,
            self.group_upload_schedule_enabled_var,
            self.group_upload_schedule_start_var,
            self.group_upload_schedule_interval_var,
        ):
            traced_var.trace_add("write", self._handle_quick_mode_change)

    def _build_ui(self) -> None:
        top = ctk.CTkFrame(self, corner_radius=16)
        top.pack(fill="x", padx=18, pady=(18, 10))
        ctk.CTkLabel(top, text="YouTube 自动化统一控制台", font=ctk.CTkFont(size=28, weight="bold")).pack(anchor="w", padx=18, pady=(14, 4))
        ctk.CTkLabel(
            top,
            text="以后常用流程直接从这里进。默认走简化前台，复杂功能保留在高级区和旧版入口里。",
            text_color="#9aa0aa",
        ).pack(anchor="w", padx=18, pady=(0, 14))

        body = ctk.CTkFrame(self, corner_radius=16)
        body.pack(fill="both", expand=True, padx=18, pady=(0, 18))

        self.tabs = ctk.CTkTabview(body)
        self.tabs.pack(fill="both", expand=True, padx=12, pady=12)
        self.tabs.add("快捷开始")
        self.tabs.add("上传")
        self.tabs.add("提示词")
        self.tabs.add("当日内容")
        self.tabs.add("高级视觉")
        self.tabs.add("路径配置")
        self.tabs.add("日志")

        self._build_quick_tab(self.tabs.tab("快捷开始"))
        self._build_upload_tab(self.tabs.tab("上传"))
        self._build_prompt_tab(self.tabs.tab("提示词"))
        self._build_generation_tab(self.tabs.tab("当日内容"))
        self._build_advanced_tab(self.tabs.tab("高级视觉"))
        self._build_paths_tab(self.tabs.tab("路径配置"))
        self._build_log_tab(self.tabs.tab("日志"))

    def _sync_task_mode_to_module_flags(self, *, initial: bool = False) -> None:
        if self._syncing_task_mode:
            return
        mode = str(self.today_task_mode_var.get() or "render_and_upload").strip()
        desired = {
            "upload_only": (False, True),
            "render_only": (True, False),
            "render_and_upload": (True, True),
        }.get(mode, (True, True))
        self._syncing_task_mode = True
        try:
            if bool(self.render_enabled_var.get()) != desired[0]:
                self.render_enabled_var.set(desired[0])
            if bool(self.auto_upload_var.get()) != desired[1]:
                self.auto_upload_var.set(desired[1])
            if mode != "upload_only" and str(self.upload_source_mode_var.get() or "").strip() == "ready_folder":
                self.upload_source_mode_var.set("render_output")
        finally:
            self._syncing_task_mode = False
        if not initial:
            self._refresh_quick_upload_mode_summary()

    def _task_mode_label(self) -> str:
        return {
            "upload_only": "本日只上传",
            "render_only": "本日只剪辑",
            "render_and_upload": "本日剪辑并上传",
        }.get(str(self.today_task_mode_var.get() or "").strip(), "本日剪辑并上传")

    def _current_upload_entry_mode(self) -> str:
        source_mode = str(self.upload_source_mode_var.get() or "render_output").strip()
        normalized = "group_folder" if source_mode == "ready_folder" else "window_plan"
        if self.upload_entry_mode_var.get() != normalized:
            self.upload_entry_mode_var.set(normalized)
        return normalized

    def _bool_override_token(self, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized == "yes":
            return "true"
        if normalized == "no":
            return "false"
        return ""

    def _current_upload_tasks(self) -> list[dict]:
        try:
            return parse_manual_tasks(self._textbox_get(self.manual_window_plan_box))
        except Exception:
            return []

    def _window_picker_defaults(self) -> dict[str, str]:
        return {
            "visibility": self.group_upload_visibility_var.get().strip() or "public",
            "category": self.group_upload_category_var.get().strip() or "Music",
            "made_for_kids": "yes" if bool(self.group_upload_made_for_kids_var.get()) else "no",
            "altered_content": "yes" if bool(self.group_upload_altered_content_var.get()) else "no",
        }

    def _reset_upload_window_overrides(self) -> None:
        defaults = self._window_picker_defaults()
        self.upload_window_ypp_override_var.set("no")
        self.upload_window_title_override_var.set("")
        self.upload_window_visibility_override_var.set(defaults["visibility"])
        self.upload_window_category_override_var.set(defaults["category"])
        self.upload_window_made_for_kids_override_var.set(defaults["made_for_kids"])
        self.upload_window_altered_content_override_var.set(defaults["altered_content"])

    def _selected_serials_for_picker_tag(self, tag: str) -> set[int]:
        selected: set[int] = set()
        for task in self._current_upload_tasks():
            if str(task.get("tag") or "").strip() == str(tag).strip():
                selected.add(int(task.get("serial") or 0))
        return selected

    def _window_button_label(self, tag: str, serial: int) -> str:
        name = self.channel_name_map.get(str(serial), "")
        ypp_serials = set((get_tag_info(tag) or {}).get("ypp_serials", []))
        ypp_mark = " YPP" if serial in ypp_serials else ""
        return f"{serial}{ypp_mark}" + (f"\n{name}" if name else "")

    def _refresh_upload_window_buttons(self) -> None:
        if self.upload_window_buttons_frame is None:
            return
        for child in self.upload_window_buttons_frame.winfo_children():
            child.destroy()
        tag = self.upload_picker_tag_var.get().strip() or self.upload_tag_var.get().strip() or self.tag_var.get().strip()
        info = get_tag_info(tag) if tag else None
        if not info:
            ctk.CTkLabel(self.upload_window_buttons_frame, text="先选一个分组，下面才会出现该组窗口。", text_color="#9aa0aa").pack(anchor="w", padx=8, pady=8)
            return
        selected_serials = self._selected_serials_for_picker_tag(tag)
        row = ctk.CTkFrame(self.upload_window_buttons_frame, fg_color="transparent")
        row.pack(fill="x", padx=6, pady=6)
        wrap_count = 0
        for serial in info.get("all_serials", []):
            if wrap_count and wrap_count % 5 == 0:
                row = ctk.CTkFrame(self.upload_window_buttons_frame, fg_color="transparent")
                row.pack(fill="x", padx=6, pady=6)
            fg_color = "#2563eb" if serial not in selected_serials else "#16a34a"
            ctk.CTkButton(
                row,
                text=self._window_button_label(tag, serial),
                width=140,
                height=52,
                fg_color=fg_color,
                command=lambda current_serial=serial, current_tag=tag: self._append_window_task(current_tag, current_serial),
            ).pack(side="left", padx=(0, 8))
            wrap_count += 1

    def _build_window_task_line(self, tag: str, serial: int) -> str:
        defaults = self._window_picker_defaults()
        tokens = [str(serial), tag]
        ypp_token = self._bool_override_token(self.upload_window_ypp_override_var.get())
        if ypp_token:
            tokens.append(f"is_ypp={ypp_token}")
        title = self.upload_window_title_override_var.get().strip()
        if title:
            tokens.append(f"title={title}")
        visibility = normalize_choice(self.upload_window_visibility_override_var.get(), GROUP_UPLOAD_VISIBILITY_VALUES, defaults["visibility"])
        if visibility and visibility != defaults["visibility"]:
            tokens.append(f"visibility={visibility}")
        category = normalize_choice(self.upload_window_category_override_var.get(), GROUP_UPLOAD_CATEGORY_VALUES, defaults["category"])
        if category and category != defaults["category"]:
            tokens.append(f"category={category}")
        kids_choice = normalize_yes_no_choice(self.upload_window_made_for_kids_override_var.get(), defaults["made_for_kids"])
        kids_token = self._bool_override_token(kids_choice)
        if kids_token and kids_choice != defaults["made_for_kids"]:
            tokens.append(f"made_for_kids={kids_token}")
        altered_choice = normalize_yes_no_choice(self.upload_window_altered_content_override_var.get(), defaults["altered_content"])
        altered_token = self._bool_override_token(altered_choice)
        if altered_token and altered_choice != defaults["altered_content"]:
            tokens.append(f"altered_content={altered_token}")
        return " | ".join(tokens)

    def _append_window_task(self, tag: str, serial: int, *, reset_picker: bool = True) -> None:
        line = self._build_window_task_line(tag, serial)
        existing_lines = [item.strip() for item in self._textbox_get(self.manual_window_plan_box).splitlines() if item.strip()]
        prefix = f"{serial} | {tag}"
        existing_lines = [item for item in existing_lines if not item.startswith(prefix)]
        existing_lines.append(line)
        self._textbox_set(self.manual_window_plan_box, "\n".join(existing_lines))
        self.window_scope_mode_var.set(SCOPE_MANUAL)
        self.window_plan_status_var.set(f"已加入窗口任务：{tag} / {serial}")
        self._preview_window_plan(show_error=False)
        self._refresh_upload_window_buttons()
        if reset_picker:
            self._reset_upload_window_overrides()

    def _append_picker_group_windows(self) -> None:
        tag = self.upload_picker_tag_var.get().strip()
        info = get_tag_info(tag) if tag else None
        if not info:
            messagebox.showwarning("缺少分组", "请先选择一个分组，再加入该组窗口。")
            return
        for serial in info.get("all_serials", []):
            self._append_window_task(tag, serial, reset_picker=False)
        self._reset_upload_window_overrides()

    def _remove_picker_group_windows(self) -> None:
        tag = self.upload_picker_tag_var.get().strip()
        if not tag:
            return
        lines = [item.strip() for item in self._textbox_get(self.manual_window_plan_box).splitlines() if item.strip()]
        kept = [item for item in lines if not item.startswith(f"{tag}") and f"| {tag}" not in item]
        self._textbox_set(self.manual_window_plan_box, "\n".join(kept))
        self._preview_window_plan(show_error=False)
        self._refresh_upload_window_buttons()

    def _build_quick_tab(self, parent) -> None:
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=8, pady=8)

        task_mode = self._section_card(
            scroll,
            "步骤 1：本次要进行哪些任务",
            "先决定今天是只上传、只剪辑，还是剪辑完继续上传。这里的“上传”包含标题、简介、标签、缩略图等整条流程。",
        )
        task_row = ctk.CTkFrame(task_mode, fg_color="transparent")
        task_row.pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkRadioButton(task_row, text="本日只上传", variable=self.today_task_mode_var, value="upload_only", command=self._sync_task_mode_to_module_flags).pack(side="left", padx=(0, 20))
        ctk.CTkRadioButton(task_row, text="本日只剪辑", variable=self.today_task_mode_var, value="render_only", command=self._sync_task_mode_to_module_flags).pack(side="left", padx=(0, 20))
        ctk.CTkRadioButton(task_row, text="本日剪辑并上传", variable=self.today_task_mode_var, value="render_and_upload", command=self._sync_task_mode_to_module_flags).pack(side="left", padx=(0, 20))
        ctk.CTkSwitch(task_row, text="全随机视觉（推荐）", variable=self.randomize_effects_var, command=self._apply_randomize_state).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(task_row, text="上传后自动关闭浏览器", variable=self.auto_close_browser_var).pack(side="left")

        hidden_scope = ctk.CTkFrame(scroll, fg_color="transparent", width=1, height=1)
        self.quick_basic_card = hidden_scope
        self.tag_menu = self._labeled_option(
            hidden_scope,
            "主分组",
            self.tag_var,
            ["加载中..."],
            width=220,
            command=lambda _: self._sync_tag_to_upload(),
        )

        workflow = self._section_card(
            scroll,
            "步骤 2：先去上传页把今天的范围填好",
            "今天要剪哪些、传哪些、传哪个分组哪个窗口，都统一在“上传”页配置。快捷开始这里只负责看状态和一键执行。",
        )
        workflow_row = ctk.CTkFrame(workflow, fg_color="transparent")
        workflow_row.pack(fill="x", padx=14, pady=(0, 8))
        ctk.CTkSwitch(workflow_row, text="保存后同步 upload_manifest", variable=self.auto_sync_manifest_var).pack(side="left", padx=(0, 16))
        ctk.CTkButton(workflow_row, text="打开上传页", width=120, fg_color="#2563eb", command=lambda: self.tabs.set("上传")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(workflow_row, text="打开提示词页", width=120, fg_color="#334155", command=lambda: self.tabs.set("提示词")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(workflow_row, text="打开当日内容页", width=120, fg_color="#334155", command=lambda: self.tabs.set("当日内容")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(workflow_row, text="保存工作台", width=120, fg_color="#475569", command=self._save_workspace_state).pack(side="left", padx=(0, 10))
        ctk.CTkLabel(
            workflow,
            textvariable=self.quick_upload_mode_status_var,
            text_color="#9aa0aa",
            wraplength=940,
            justify="left",
        ).pack(anchor="w", padx=14, pady=(0, 12))

        render = self._section_card(
            scroll,
            "步骤 3：渲染并发和节奏",
            "如果今天要制作视频，这里只保留最常用的几个杠杆。分组和日期直接读“上传”页，复杂特效再去“高级视觉”。",
        )
        render_row = ctk.CTkFrame(render, fg_color="transparent")
        render_row.pack(fill="x", padx=14, pady=(0, 12))
        ctk.CTkLabel(render_row, text="音频并行", width=90).pack(side="left")
        ctk.CTkSlider(render_row, from_=1, to=8, variable=self.audio_workers_var, number_of_steps=7, width=220).pack(side="left", padx=(0, 20))
        ctk.CTkLabel(render_row, text="视频并行", width=90).pack(side="left")
        ctk.CTkSlider(render_row, from_=1, to=8, variable=self.video_workers_var, number_of_steps=7, width=220).pack(side="left")
        render_row2 = ctk.CTkFrame(render, fg_color="transparent")
        render_row2.pack(fill="x", padx=14, pady=(0, 12))
        self._labeled_entry(render_row2, "每赛道曲数", self.song_count_var, width=110, placeholder="1")
        ctk.CTkButton(
            render_row2,
            text="打开输出目录",
            width=120,
            fg_color="#475569",
            command=lambda: open_folder(self.output_dir_var.get()),
        ).pack(side="left", padx=(0, 10), pady=(20, 0))

        batch = self._section_card(
            scroll,
            "步骤 4：如果今天有多个赛道，就在这里填任务清单",
            "只有今天要批量渲染多个赛道时，这块才是必填。其他模式可以先留空。",
        )
        self.batch_tags_box = ctk.CTkTextbox(batch, height=96)
        self.batch_tags_box.pack(fill="x", padx=14, pady=(0, 8))
        self.batch_tags_box.bind("<KeyRelease>", lambda *_: (self._refresh_batch_path_preview(), self._refresh_quick_upload_mode_summary()))
        if self.ui_state.get("batch_tags_text"):
            self.batch_tags_box.insert("1.0", self.ui_state["batch_tags_text"])
        batch_actions = ctk.CTkFrame(batch, fg_color="transparent")
        batch_actions.pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkButton(batch_actions, text="加入当前 tag", width=110, fg_color="#334155", command=self._append_current_tag_to_batch).pack(side="left", padx=(0, 8))
        ctk.CTkButton(batch_actions, text="整理去重", width=100, fg_color="#334155", command=self._normalize_batch_tags_box).pack(side="left", padx=(0, 8))
        ctk.CTkButton(batch_actions, text="批量建目录", width=100, fg_color="#475569", command=self._prepare_batch_directories).pack(side="left", padx=(0, 8))
        ctk.CTkButton(batch_actions, text="打开多赛道目录", width=120, fg_color="#475569", command=self._open_batch_directories).pack(side="left", padx=(0, 8))
        ctk.CTkLabel(batch_actions, textvariable=self.batch_status_var, text_color="#9aa0aa").pack(side="right")

        links = self._section_card(
            scroll,
            "步骤 5：启动前，先检查内容和素材路径",
            "通常顺序是：先去提示词页看模板，再去当日内容页看标题/简介/封面，最后确认路径配置。",
        )
        links_row = ctk.CTkFrame(links, fg_color="transparent")
        links_row.pack(fill="x", padx=14, pady=(0, 12))
        ctk.CTkButton(links_row, text="打开提示词页", width=120, fg_color="#334155", command=lambda: self.tabs.set("提示词")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(links_row, text="打开当日内容页", width=120, fg_color="#334155", command=lambda: self.tabs.set("当日内容")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(links_row, text="打开路径配置", width=120, fg_color="#334155", command=lambda: self.tabs.set("路径配置")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(links_row, text="打开当前上传设置", width=140, fg_color="#475569", command=self._open_active_upload_settings_tab).pack(side="left")

        actions = self._section_card(
            scroll,
            "步骤 6：开始执行",
            "“开始当前流程”会按你上面的任务模式去跑；不想全跑，就用下面的单独按钮。",
        )
        ctk.CTkButton(actions, text="开始当前流程", height=42, command=self._run_current_flow).pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkButton(actions, text="仅跑当前上传方式", height=40, fg_color="#334155", command=self._run_selected_upload_mode).pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkButton(actions, text="仅渲染", height=40, fg_color="#475569", command=lambda: self._run_scheduler(render_only=True)).pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkButton(actions, text="打开旧版高级调度器", height=38, fg_color="#334155", command=lambda: self._launch_tool(LEGACY_SCHEDULER)).pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkButton(actions, text="打开旧版渲染工作站", height=38, fg_color="#334155", command=lambda: self._launch_tool(LEGACY_RENDERER)).pack(fill="x", padx=14, pady=(0, 14))

        status = self._section_card(scroll, "当前状态")
        ctk.CTkLabel(status, textvariable=self.status_var, text_color="#a3e635").pack(anchor="w", padx=14, pady=(0, 14))

    def _build_upload_tab(self, parent) -> None:
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=8, pady=8)
        hidden_state = ctk.CTkFrame(scroll, fg_color="transparent", width=1, height=1)
        self.upload_tag_menu = ctk.CTkOptionMenu(hidden_state, variable=self.upload_tag_var, values=[""])
        self.channel_menu = ctk.CTkOptionMenu(hidden_state, variable=self.channel_var, values=[""])

        source = self._section_card(
            scroll,
            "步骤 1：先定上传来源和文案来源",
            "上传页就是唯一入口。只传 1 个窗口就是单个上传，传多个窗口就是批量上传，不再拆两个 tab。",
        )
        source_row = ctk.CTkFrame(source, fg_color="transparent")
        source_row.pack(fill="x", padx=14, pady=(0, 8))
        ctk.CTkRadioButton(source_row, text="上传渲染产物", variable=self.upload_source_mode_var, value="render_output").pack(side="left", padx=(0, 20))
        ctk.CTkRadioButton(source_row, text="上传现成视频文件夹", variable=self.upload_source_mode_var, value="ready_folder").pack(side="left", padx=(0, 20))
        ctk.CTkButton(source_row, text="打开提示词页", width=120, fg_color="#334155", command=lambda: self.tabs.set("提示词")).pack(side="left", padx=(16, 8))
        ctk.CTkButton(source_row, text="打开当日内容页", width=120, fg_color="#334155", command=lambda: self.tabs.set("当日内容")).pack(side="left")

        source_row2 = ctk.CTkFrame(source, fg_color="transparent")
        source_row2.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_option(source_row2, "文案来源", self.upload_metadata_mode_var, UPLOAD_METADATA_MODE_VALUES, width=150)
        ctk.CTkSwitch(source_row2, text="生成标题/简介/标签", variable=self.upload_fill_text_var).pack(side="left", padx=(0, 16), pady=(20, 0))
        ctk.CTkSwitch(source_row2, text="处理缩略图", variable=self.upload_fill_thumbnails_var).pack(side="left", padx=(0, 16), pady=(20, 0))
        ctk.CTkSwitch(source_row2, text="同步到当日内容", variable=self.upload_sync_daily_content_var).pack(side="left", padx=(0, 16), pady=(20, 0))

        source_row3 = ctk.CTkFrame(source, fg_color="transparent")
        source_row3.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_entry(source_row3, "现成视频文件夹", self.group_upload_source_dir_var, placeholder="只在“上传现成视频文件夹”时需要", expand=True)
        ctk.CTkButton(source_row3, text="选择视频目录", width=110, fg_color="#334155", command=lambda: self._pick_directory_for_var(self.group_upload_source_dir_var)).pack(side="left", padx=(0, 10), pady=(20, 0))

        source_row4 = ctk.CTkFrame(source, fg_color="transparent")
        source_row4.pack(fill="x", padx=14, pady=(0, 12))
        self._labeled_entry(source_row4, "现成缩略图文件夹", self.group_upload_thumb_dir_var, placeholder="可选，不填就自动处理", expand=True)
        ctk.CTkButton(source_row4, text="选择缩略图目录", width=120, fg_color="#334155", command=lambda: self._pick_directory_for_var(self.group_upload_thumb_dir_var)).pack(side="left", padx=(0, 10), pady=(20, 0))
        ctk.CTkLabel(source, text="说明：上传渲染产物时，上面两个目录可以留空。", text_color="#9aa0aa").pack(anchor="w", padx=14, pady=(0, 12))

        defaults = self._section_card(
            scroll,
            "步骤 2：统一默认规则",
            "这里是所有窗口的通用默认值。某个窗口需要单独改，再在下面“加入窗口时”那一排临时改。",
        )
        default_row = ctk.CTkFrame(defaults, fg_color="transparent")
        default_row.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_entry(default_row, "日期", self.date_var, width=120, placeholder="MMDD / 3.12")
        self._labeled_option(default_row, "可见性", self.group_upload_visibility_var, GROUP_UPLOAD_VISIBILITY_VALUES, width=150)
        self._labeled_option(default_row, "分类", self.group_upload_category_var, GROUP_UPLOAD_CATEGORY_VALUES, width=180)
        ctk.CTkSwitch(default_row, text="儿童内容", variable=self.group_upload_made_for_kids_var).pack(side="left", padx=(0, 16), pady=(20, 0))
        ctk.CTkSwitch(default_row, text="AI/合成内容", variable=self.group_upload_altered_content_var).pack(side="left", padx=(0, 16), pady=(20, 0))
        ctk.CTkSwitch(default_row, text="启用定时发布", variable=self.group_upload_schedule_enabled_var).pack(side="left", padx=(0, 16), pady=(20, 0))

        default_row2 = ctk.CTkFrame(defaults, fg_color="transparent")
        default_row2.pack(fill="x", padx=14, pady=(0, 12))
        self._labeled_entry(default_row2, "开始时间", self.group_upload_schedule_start_var, width=220, placeholder="2026-03-13 21:00")
        self._labeled_entry(default_row2, "间隔(分钟)", self.group_upload_schedule_interval_var, width=120, placeholder="60")
        ctk.CTkButton(default_row2, text="恢复加入窗口默认值", width=150, fg_color="#475569", command=self._reset_upload_window_overrides).pack(side="left", padx=(0, 10), pady=(20, 0))

        planner = self._section_card(
            scroll,
            "步骤 3：把今天要上传的窗口加进来",
            "先选分组，再点窗口。系统会自动写入下面唯一的任务区。只写 1 行就是单个上传，写多行就是批量上传。",
        )
        picker_row = ctk.CTkFrame(planner, fg_color="transparent")
        picker_row.pack(fill="x", padx=14, pady=(0, 8))
        self.upload_picker_tag_menu = self._labeled_option(
            picker_row,
            "当前分组",
            self.upload_picker_tag_var,
            ["加载中..."],
            width=220,
            command=lambda _: self._refresh_upload_window_buttons(),
        )
        ctk.CTkButton(picker_row, text="加入本组全部窗口", width=130, fg_color="#334155", command=self._append_picker_group_windows).pack(side="left", padx=(0, 8), pady=(20, 0))
        ctk.CTkButton(picker_row, text="移除本组窗口", width=130, fg_color="#475569", command=self._remove_picker_group_windows).pack(side="left", padx=(0, 8), pady=(20, 0))
        ctk.CTkButton(picker_row, text="刷新窗口按钮", width=120, fg_color="#475569", command=self._refresh_upload_window_buttons).pack(side="left", pady=(20, 0))

        picker_help = ctk.CTkFrame(planner, fg_color="transparent")
        picker_help.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_option(picker_help, "加入窗口时 YPP", self.upload_window_ypp_override_var, BOOL_OVERRIDE_VALUES, width=110)
        self._labeled_entry(picker_help, "自定义标题", self.upload_window_title_override_var, width=220, placeholder="可留空")
        self._labeled_option(picker_help, "可见性", self.upload_window_visibility_override_var, GROUP_UPLOAD_VISIBILITY_VALUES, width=140)
        self._labeled_option(picker_help, "分类", self.upload_window_category_override_var, GROUP_UPLOAD_CATEGORY_VALUES, width=170)
        self._labeled_option(picker_help, "儿童内容", self.upload_window_made_for_kids_override_var, BOOL_OVERRIDE_VALUES, width=110)
        self._labeled_option(picker_help, "AI内容", self.upload_window_altered_content_override_var, BOOL_OVERRIDE_VALUES, width=110)

        self.upload_window_buttons_frame = ctk.CTkScrollableFrame(planner, height=180, fg_color="#10151d")
        self.upload_window_buttons_frame.pack(fill="both", expand=True, padx=14, pady=(0, 8))

        manual_card = ctk.CTkFrame(planner, fg_color="transparent")
        manual_card.pack(fill="x", padx=14, pady=(0, 8))
        ctk.CTkLabel(manual_card, text="窗口任务列表（唯一输入区，系统会自动写入，你也可以手改）", text_color="#9aa0aa").pack(anchor="w", pady=(0, 4))
        self.manual_window_plan_box = ctk.CTkTextbox(manual_card, height=170)
        self.manual_window_plan_box.pack(fill="x")
        self.manual_window_plan_box.bind("<KeyRelease>", lambda *_: (self._preview_window_plan(show_error=False), self._refresh_upload_window_buttons()))
        if self.ui_state.get("manual_window_plan_text"):
            self.manual_window_plan_box.insert("1.0", self.ui_state["manual_window_plan_text"])
        self.multi_group_plan_box = None
        ctk.CTkLabel(
            planner,
            text="示例：\n90 | 面壁者\n91 | 面壁者 | title=自定义标题\n95 | 芝加哥蓝调 | is_ypp=true | visibility=private",
            text_color="#9aa0aa",
            justify="left",
        ).pack(anchor="w", padx=14, pady=(0, 12))

        preview = self._section_card(
            scroll,
            "步骤 4：预览并执行",
            "先看一眼今天到底会传哪些窗口，再正式开始上传。",
        )
        action_row = ctk.CTkFrame(preview, fg_color="transparent")
        action_row.pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkButton(action_row, text="预览今日上传计划", width=150, fg_color="#334155", command=self._preview_current_upload_plan).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="开始上传", width=170, command=self._run_selected_upload_mode).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="打开上传记录目录", width=140, fg_color="#475569", command=lambda: open_folder(str(SCRIPT_DIR / "upload_records"))).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="打开计划文件目录", width=140, fg_color="#475569", command=lambda: open_folder(str(self._window_plan_path().parent))).pack(side="left")

        self.window_plan_preview_box = ctk.CTkTextbox(preview, height=240)
        self.window_plan_preview_box.pack(fill="both", expand=True, padx=14, pady=(0, 8))
        ctk.CTkLabel(preview, textvariable=self.window_plan_status_var, text_color="#a3e635").pack(anchor="w", padx=14, pady=(0, 12))

        self.window_scope_mode_var.set(SCOPE_MANUAL)
        self._refresh_upload_window_buttons()

    def _build_group_upload_tab(self, parent) -> None:
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=8, pady=8)

        source = self._section_card(
            scroll,
            "步骤 1：选择同一分组下要上传的现成视频",
            "适合这种场景：一个文件夹里已经有多条成品视频，你要按顺序发到同一个比特浏览器分组下的多个频道。",
        )
        row1 = ctk.CTkFrame(source, fg_color="transparent")
        row1.pack(fill="x", padx=14, pady=(0, 8))
        self.group_upload_tag_menu = self._labeled_option(row1, "目标分组", self.group_upload_tag_var, ["加载中..."], width=220)
        self._labeled_entry(row1, "日期", self.date_var, width=120, placeholder="MMDD / 3.12")
        self._labeled_entry(row1, "现成视频文件夹", self.group_upload_source_dir_var, placeholder="放成品视频的文件夹", expand=True)
        choose_video = ctk.CTkFrame(row1, fg_color="transparent")
        choose_video.pack(side="left", pady=(20, 0))
        ctk.CTkButton(choose_video, text="选择视频目录", width=110, fg_color="#334155", command=lambda: self._pick_directory_for_var(self.group_upload_source_dir_var)).pack()

        row2 = ctk.CTkFrame(source, fg_color="transparent")
        row2.pack(fill="x", padx=14, pady=(0, 12))
        self._labeled_entry(row2, "现成缩略图文件夹", self.group_upload_thumb_dir_var, placeholder="可选，不填就自动处理", expand=True)
        choose_thumb = ctk.CTkFrame(row2, fg_color="transparent")
        choose_thumb.pack(side="left", padx=(0, 10), pady=(20, 0))
        ctk.CTkButton(choose_thumb, text="选择缩略图目录", width=120, fg_color="#334155", command=lambda: self._pick_directory_for_var(self.group_upload_thumb_dir_var)).pack()
        self._labeled_entry(row2, "限定频道序号", self.group_upload_serials_var, width=260, placeholder="可选，如 90,94,95")

        policy = self._section_card(
            scroll,
            "步骤 2：决定文案生成和发布规则",
            "这里决定标题简介缩略图走哪套生成逻辑，也决定可见性、分类、AI 标记和定时发布时间。",
        )
        row3 = ctk.CTkFrame(policy, fg_color="transparent")
        row3.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_option(row3, "生成模式", self.group_upload_mode_var, GROUP_UPLOAD_MODE_VALUES, width=170)
        self._labeled_option(row3, "可见性", self.group_upload_visibility_var, GROUP_UPLOAD_VISIBILITY_VALUES, width=160)
        self._labeled_option(row3, "分类", self.group_upload_category_var, GROUP_UPLOAD_CATEGORY_VALUES, width=180)
        ctk.CTkLabel(row3, textvariable=self.group_upload_status_var, text_color="#9aa0aa", wraplength=340, justify="left").pack(side="left", fill="x", expand=True, pady=(20, 0))

        row4 = ctk.CTkFrame(policy, fg_color="transparent")
        row4.pack(fill="x", padx=14, pady=(0, 8))
        ctk.CTkSwitch(row4, text="儿童内容", variable=self.group_upload_made_for_kids_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row4, text="AI / 合成内容", variable=self.group_upload_altered_content_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row4, text="启用定时发布", variable=self.group_upload_schedule_enabled_var).pack(side="left", padx=(0, 16))

        row5 = ctk.CTkFrame(policy, fg_color="transparent")
        row5.pack(fill="x", padx=14, pady=(0, 12))
        self._labeled_entry(row5, "开始发布时间", self.group_upload_schedule_start_var, width=180, placeholder="YYYY-MM-DD HH:MM")
        self._labeled_entry(row5, "间隔分钟", self.group_upload_schedule_interval_var, width=120, placeholder="60")
        ctk.CTkLabel(
            policy,
            text="说明：`site_api` = 走你网页版那套 API；`legacy` = 走原先 generation_map 逻辑。默认建议 `site_api`。",
            text_color="#9aa0aa",
            wraplength=940,
            justify="left",
        ).pack(anchor="w", padx=14, pady=(0, 12))

        actions = self._section_card(
            scroll,
            "步骤 3：先预览，再上传",
            "先看计划，再决定是 dry-run 测试，还是直接开始正式上传。",
        )
        action_row = ctk.CTkFrame(actions, fg_color="transparent")
        action_row.pack(fill="x", padx=14, pady=(0, 14))
        ctk.CTkButton(action_row, text="保存本页设置", width=110, fg_color="#475569", command=self._save_group_upload_settings).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="预览并准备上传计划", width=150, command=self._preview_group_upload_plan).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="Dry-run 测试上传", width=130, fg_color="#334155", command=lambda: self._run_group_upload(dry_run=True)).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="开始分组批量上传", width=150, fg_color="#2563eb", command=self._run_group_upload).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="打开 staging 输出目录", width=140, fg_color="#334155", command=self._open_group_upload_output_dir).pack(side="left")

        preview = self._section_card(scroll, "上传计划预览", "这里会显示视频如何分配到频道，以及即将写入的 manifest 路径。")
        self.group_upload_preview_box = ctk.CTkTextbox(preview, height=340)
        self.group_upload_preview_box.pack(fill="both", expand=True, padx=14, pady=(0, 14))

    def _build_prompt_tab(self, parent) -> None:
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=8, pady=8)

        choose = self._section_card(
            scroll,
            "步骤 1：先选分组和要套用的模板",
            "这一块是模板入口。先选分组，再选 API 模板和内容模板；如果你要让某个分组长期绑定某套风格，也在这里做。",
        )
        top = ctk.CTkFrame(choose, fg_color="transparent")
        top.pack(fill="x", padx=14, pady=(0, 12))
        self.prompt_tag_menu = self._labeled_option(top, "分组", self.prompt_tag_var, ["加载中..."], width=180, command=lambda _: self._apply_bound_content_template())
        self.api_preset_menu = self._labeled_option(top, "API 模板", self.api_preset_name_var, ["默认API模板"], width=200, command=lambda _: self._load_api_preset())
        self.content_template_menu = self._labeled_option(top, "内容模板", self.content_template_name_var, ["默认内容模板"], width=220, command=lambda _: self._load_content_template())
        choose_buttons = ctk.CTkFrame(top, fg_color="transparent")
        choose_buttons.pack(side="left", pady=(20, 0))
        ctk.CTkButton(choose_buttons, text="绑定当前分组到内容模板", width=150, command=self._bind_tag_to_content_template).pack(side="left", padx=(0, 8))
        ctk.CTkButton(choose_buttons, text="打开配置文件", width=110, fg_color="#334155", command=lambda: open_target(PROMPT_STUDIO_FILE)).pack(side="left")

        api = self._section_card(
            scroll,
            "步骤 2：文本 / 图片 API 模板",
            "这里专门放模型和接口。之前那些没有标签的输入框都补回字段名了，后面改模型只看这一块。",
        )
        api_row1 = ctk.CTkFrame(api, fg_color="transparent")
        api_row1.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_option(api_row1, "文本 Provider", self.api_provider_var, PROMPT_PROVIDER_VALUES, width=170)
        self._labeled_entry(api_row1, "文本 Base URL", self.api_base_url_var, placeholder="https://...", expand=True)
        self._labeled_entry(api_row1, "文本 Model", self.api_model_var, width=220, placeholder="deepseek-chat")

        api_row2 = ctk.CTkFrame(api, fg_color="transparent")
        api_row2.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_entry(api_row2, "文本 API Key", self.api_key_var, placeholder="sk-...", expand=True)
        self._labeled_entry(api_row2, "Temperature", self.api_temperature_var, width=100, placeholder="0.9")
        self._labeled_entry(api_row2, "Max Tokens", self.api_max_tokens_var, width=120, placeholder="16000")
        self._labeled_option(api_row2, "自动出图", self.api_auto_image_var, PROMPT_AUTO_IMAGE_VALUES, width=120)

        api_row3 = ctk.CTkFrame(api, fg_color="transparent")
        api_row3.pack(fill="x", padx=14, pady=(0, 12))
        self._labeled_entry(api_row3, "图片 Base URL", self.image_base_url_var, placeholder="https://...", expand=True)
        self._labeled_entry(api_row3, "图片 API Key", self.image_api_key_var, placeholder="可留空", expand=True)
        self._labeled_entry(api_row3, "图片 Model", self.image_model_var, width=220, placeholder="gemini-3-pro-image-preview")
        self._labeled_entry(api_row3, "图片并发", self.image_concurrency_var, width=100, placeholder="3")

        content = self._section_card(
            scroll,
            "步骤 3：内容模板（你网页那套字段）",
            "这块负责告诉模型：你做什么风格、打给谁看、生成多少标题和缩略图，以及输出语言是什么。",
        )
        content_row1 = ctk.CTkFrame(content, fg_color="transparent")
        content_row1.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_entry(content_row1, "音乐类型 / 风格", self.content_music_genre_var, placeholder="如：芝加哥慢蓝调", expand=True)
        self._labeled_entry(content_row1, "切入角度", self.content_angle_var, placeholder="如：烟嗓女伶爵士", expand=True)
        self._labeled_entry(content_row1, "目标受众", self.content_audience_var, placeholder="如：50岁以上台湾男性", expand=True)

        content_row2 = ctk.CTkFrame(content, fg_color="transparent")
        content_row2.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_option(content_row2, "输出语言", self.content_output_language_var, PROMPT_LANGUAGE_VALUES, width=120)
        self._labeled_option(content_row2, "标题数", self.content_title_count_var, PROMPT_COUNT_VALUES, width=90)
        self._labeled_option(content_row2, "简介数", self.content_desc_count_var, PROMPT_COUNT_VALUES, width=90)
        self._labeled_option(content_row2, "缩略图数", self.content_thumb_count_var, PROMPT_COUNT_VALUES, width=90)
        self._labeled_entry(content_row2, "标题最小字数", self.content_title_min_var, width=100, placeholder="80")
        self._labeled_entry(content_row2, "标题最大字数", self.content_title_max_var, width=100, placeholder="95")
        self._labeled_entry(content_row2, "简介字数", self.content_desc_len_var, width=100, placeholder="300")
        self._labeled_entry(content_row2, "标签数量区间", self.content_tag_range_var, width=120, placeholder="10-20")

        audience_card = self._section_card(
            scroll,
            "步骤 4：受众截图自动识别",
            "把 YouTube 后台受众截图放进来后，会调用当前 API 模板对应的模型识别年龄、性别、地区和设备，并自动同步到上面的“目标受众”。",
        )
        shot_row = ctk.CTkFrame(audience_card, fg_color="transparent")
        shot_row.pack(fill="x", padx=14, pady=(0, 8))
        self._labeled_entry(
            shot_row,
            "受众截图文件",
            self.prompt_audience_shot_path_var,
            placeholder="可直接选择文件，或用下方按钮粘贴剪贴板截图",
            expand=True,
        )
        shot_buttons = ctk.CTkFrame(shot_row, fg_color="transparent")
        shot_buttons.pack(side="left", pady=(20, 0))
        ctk.CTkButton(shot_buttons, text="选择截图", width=100, command=self._pick_prompt_audience_shot).pack(side="left", padx=(0, 8))
        ctk.CTkButton(shot_buttons, text="粘贴截图", width=100, fg_color="#334155", command=self._paste_prompt_audience_shot_from_clipboard).pack(side="left", padx=(0, 8))
        ctk.CTkButton(shot_buttons, text="重新识别", width=100, fg_color="#475569", command=self._analyze_prompt_audience_shot).pack(side="left", padx=(0, 8))
        ctk.CTkButton(shot_buttons, text="清空截图", width=100, fg_color="#475569", command=self._clear_prompt_audience_shot).pack(side="left")

        shot_body = ctk.CTkFrame(audience_card, fg_color="transparent")
        shot_body.pack(fill="both", expand=True, padx=14, pady=(0, 12))
        preview_frame = ctk.CTkFrame(shot_body)
        preview_frame.pack(side="left", fill="both", expand=True, padx=(0, 8))
        ctk.CTkLabel(preview_frame, text="截图预览", text_color="#9aa0aa").pack(anchor="w", padx=12, pady=(12, 6))
        self.prompt_audience_preview_label = ctk.CTkLabel(
            preview_frame,
            text="未选择截图",
            width=360,
            height=220,
            fg_color="#111827",
            corner_radius=10,
        )
        self.prompt_audience_preview_label.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        parsed_frame = ctk.CTkFrame(shot_body)
        parsed_frame.pack(side="left", fill="both", expand=True, padx=(8, 0))
        ctk.CTkLabel(parsed_frame, text="截图解析结果（会自动同步到“目标受众”）", text_color="#9aa0aa").pack(anchor="w", padx=12, pady=(12, 6))
        self.prompt_audience_parsed_box = ctk.CTkTextbox(parsed_frame, height=220)
        self.prompt_audience_parsed_box.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self._textbox_set(self.prompt_audience_parsed_box, "等待截图后自动解析目标人群...")

        prompt_body = self._section_card(
            scroll,
            "步骤 5：主提示词和标题库",
            "这里就是你最关心的提示词正文。以后改生成方向，优先改这里，而不是去猜接口参数。",
        )
        ctk.CTkLabel(prompt_body, text="主提示词", text_color="#9aa0aa").pack(anchor="w", padx=14, pady=(0, 6))
        self.prompt_master_box = ctk.CTkTextbox(prompt_body, height=220)
        self.prompt_master_box.pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkLabel(prompt_body, text="对标标题库 / 参考爆款标题", text_color="#9aa0aa").pack(anchor="w", padx=14, pady=(0, 6))
        self.prompt_title_library_box = ctk.CTkTextbox(prompt_body, height=140)
        self.prompt_title_library_box.pack(fill="x", padx=14, pady=(0, 14))

        actions = self._section_card(
            scroll,
            "步骤 6：保存模板并看最终送模版的内容",
            "改完模板后先保存，再点预览。这样你能直接看到发给模型前的拼装结果。",
        )
        action_row = ctk.CTkFrame(actions, fg_color="transparent")
        action_row.pack(fill="x", padx=14, pady=(0, 12))
        ctk.CTkButton(action_row, text="保存 API 模板", command=self._save_api_preset).pack(side="left", padx=(0, 10))
        ctk.CTkButton(action_row, text="保存内容模板", command=self._save_content_template).pack(side="left", padx=(0, 10))
        ctk.CTkButton(action_row, text="预览当前主提示词", fg_color="#334155", command=self._preview_prompt_bundle).pack(side="left", padx=(0, 10))
        ctk.CTkLabel(action_row, textvariable=self.prompt_status_var, text_color="#9aa0aa").pack(side="right")

        preview = self._section_card(scroll, "步骤 7：送给模型前的预览", "这里显示最终拼装后的 Prompt，方便你确认变量、风格和语气有没有跑偏。")
        self.prompt_preview_box = ctk.CTkTextbox(preview, height=260)
        self.prompt_preview_box.pack(fill="both", expand=True, padx=14, pady=(0, 14))

    def _build_generation_tab(self, parent) -> None:
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=8, pady=8)

        header = ctk.CTkFrame(scroll)
        header.pack(fill="x", padx=10, pady=(10, 8))
        ctk.CTkLabel(header, text="当日标题 / 简介 / 封面", font=ctk.CTkFont(size=22, weight="bold")).pack(anchor="w", padx=14, pady=(12, 4))
        ctk.CTkLabel(
            header,
            text="这里直接改 workspace/base_image/<tag>/generation_map.json。保存后可顺手同步 upload_manifest，避免上传时还吃旧标题。",
            text_color="#9aa0aa",
        ).pack(anchor="w", padx=14, pady=(0, 12))

        top = ctk.CTkFrame(scroll, fg_color="transparent")
        top.pack(fill="x", padx=10, pady=(0, 8))
        self.generation_tag_menu = ctk.CTkOptionMenu(top, variable=self.generation_tag_var, values=["加载中..."], width=220, command=lambda _: self._sync_generation_channels())
        self.generation_tag_menu.pack(side="left", padx=(0, 10))
        ctk.CTkEntry(top, textvariable=self.date_var, width=120, placeholder_text="MMDD / 3.12").pack(side="left", padx=(0, 10))
        self.generation_channel_menu = ctk.CTkOptionMenu(top, variable=self.generation_channel_var, values=[""], width=220)
        self.generation_channel_menu.pack(side="left", padx=(0, 10))
        ctk.CTkButton(top, text="加载当前频道", width=110, command=self._load_generation_entry).pack(side="left", padx=(0, 10))
        ctk.CTkButton(top, text="打开底图目录", width=110, fg_color="#334155", command=self._open_generation_tag_dir).pack(side="left")

        meta = ctk.CTkFrame(scroll, fg_color="transparent")
        meta.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkSwitch(meta, text="该频道视为 YPP", variable=self.generation_is_ypp_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(meta, text="保存后自动同步 manifest", variable=self.auto_sync_manifest_var).pack(side="left", padx=(0, 16))
        ctk.CTkEntry(meta, textvariable=self.generation_set_var, width=100, placeholder_text="套数").pack(side="left", padx=(0, 16))
        ctk.CTkLabel(meta, textvariable=self.generation_status_var, text_color="#9aa0aa", wraplength=560, justify="left").pack(side="left", fill="x", expand=True)

        title_frame = ctk.CTkFrame(scroll)
        title_frame.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkLabel(title_frame, text="标题", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 6))
        self.gen_title_box = ctk.CTkTextbox(title_frame, height=90)
        self.gen_title_box.pack(fill="x", padx=14, pady=(0, 14))

        desc_frame = ctk.CTkFrame(scroll)
        desc_frame.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkLabel(desc_frame, text="简介", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 6))
        self.gen_desc_box = ctk.CTkTextbox(desc_frame, height=180)
        self.gen_desc_box.pack(fill="x", padx=14, pady=(0, 14))

        side = ctk.CTkFrame(scroll)
        side.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkLabel(side, text="封面文件名（每行一个）", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 6))
        self.gen_covers_box = ctk.CTkTextbox(side, height=100)
        self.gen_covers_box.pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkLabel(side, text="AB 标题（每行一个）", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(0, 6))
        self.gen_ab_titles_box = ctk.CTkTextbox(side, height=100)
        self.gen_ab_titles_box.pack(fill="x", padx=14, pady=(0, 14))

        actions = ctk.CTkFrame(scroll)
        actions.pack(fill="x", padx=10, pady=(0, 12))
        ctk.CTkButton(actions, text="自动匹配封面文件", command=self._guess_generation_covers).pack(side="left", padx=(0, 10), pady=12)
        ctk.CTkButton(actions, text="保存当前频道", command=self._save_generation_entry).pack(side="left", padx=(0, 10), pady=12)
        ctk.CTkButton(actions, text="仅同步 manifest", fg_color="#334155", command=self._sync_current_manifest_only).pack(side="left", padx=(0, 10), pady=12)
        ctk.CTkButton(actions, text="把当前内容带去提示词预览", fg_color="#475569", command=self._push_generation_to_prompt_preview).pack(side="left", padx=(0, 10), pady=12)

    def _build_advanced_tab(self, parent) -> None:
        tip = ctk.CTkLabel(
            parent,
            text="这里只有你想手动控视觉风格时才需要动。开启“全随机视觉”后，这里的参数会暂时不参与渲染。",
            text_color="#9aa0aa",
        )
        tip.pack(anchor="w", padx=14, pady=(12, 8))

        grid = ctk.CTkFrame(parent, fg_color="transparent")
        grid.pack(fill="both", expand=True, padx=10, pady=8)
        grid.grid_columnconfigure((0, 1), weight=1)

        self._manual_field(grid, 0, 0, "频谱", ctk.CTkSwitch, variable=self.spectrum_var, text="")
        self._manual_field(grid, 0, 1, "时间轴", ctk.CTkSwitch, variable=self.timeline_var, text="")
        self._manual_field(grid, 1, 0, "黑边", ctk.CTkOptionMenu, variable=self.letterbox_var, values=list(LETTERBOX_MAP.keys()))
        self._manual_field(grid, 1, 1, "缩放", ctk.CTkOptionMenu, variable=self.zoom_var, values=list(ZOOM_MAP.keys()))
        self._manual_field(grid, 2, 0, "频谱色", ctk.CTkOptionMenu, variable=self.color_spectrum_var, values=list(COLOR_MAP.keys()))
        self._manual_field(grid, 2, 1, "时间轴色", ctk.CTkOptionMenu, variable=self.color_timeline_var, values=list(COLOR_MAP.keys()))
        self._manual_field(grid, 3, 0, "样式", ctk.CTkOptionMenu, variable=self.style_var, values=list(STYLE_MAP.keys()))
        self._manual_field(grid, 3, 1, "粒子", ctk.CTkOptionMenu, variable=self.particle_var, values=list(PARTICLE_MAP.keys()))
        self._manual_field(grid, 4, 0, "粒子透明度", ctk.CTkEntry, textvariable=self.particle_opacity_var)
        self._manual_field(grid, 4, 1, "色调", ctk.CTkOptionMenu, variable=self.color_tint_var, values=list(TINT_MAP.keys()))
        self._manual_field(grid, 5, 0, "噪点", ctk.CTkSwitch, variable=self.film_grain_var, text="")
        self._manual_field(grid, 5, 1, "噪点强度", ctk.CTkEntry, textvariable=self.grain_strength_var)
        self._manual_field(grid, 6, 0, "暗角", ctk.CTkSwitch, variable=self.vignette_var, text="")
        self._manual_field(grid, 6, 1, "轻模糊", ctk.CTkSwitch, variable=self.soft_focus_var, text="")
        self._manual_field(grid, 7, 0, "模糊强度", ctk.CTkEntry, textvariable=self.soft_focus_sigma_var)
        self._manual_field(grid, 7, 1, "文字内容", ctk.CTkEntry, textvariable=self.text_var)
        self._manual_field(grid, 8, 0, "文字位置", ctk.CTkOptionMenu, variable=self.text_pos_var, values=list(TEXT_POS_MAP.keys()))
        self._manual_field(grid, 8, 1, "文字样式", ctk.CTkOptionMenu, variable=self.text_style_var, values=list(TEXT_STYLE_MAP.keys()))
        self._manual_field(grid, 9, 0, "文字大小", ctk.CTkEntry, textvariable=self.text_size_var)

    def _build_paths_tab(self, parent) -> None:
        info = ctk.CTkLabel(parent, text="这里是统一工作路径。以后如果你换盘符或目录，只改这里。", text_color="#9aa0aa")
        info.pack(anchor="w", padx=14, pady=(12, 8))
        self._path_row(parent, "音乐目录", self.music_dir_var)
        self._path_row(parent, "底图目录", self.image_dir_var)
        self._path_row(parent, "输出目录", self.output_dir_var)
        self._path_row(parent, "FFmpeg", self.ffmpeg_var)
        ctk.CTkButton(parent, text="保存路径配置", command=self._save_scheduler_paths).pack(anchor="w", padx=14, pady=12)
        ctk.CTkButton(parent, text="打开 prompt_studio.json", fg_color="#334155", command=lambda: open_target(PROMPT_STUDIO_FILE)).pack(anchor="w", padx=14, pady=(0, 8))
        ctk.CTkButton(parent, text="打开 config 文件夹", fg_color="#334155", command=lambda: open_target(SCRIPT_DIR / "config")).pack(anchor="w", padx=14)

        batch = ctk.CTkFrame(parent)
        batch.pack(fill="both", expand=True, padx=14, pady=(14, 0))
        ctk.CTkLabel(batch, text="多赛道路径预览", font=ctk.CTkFont(size=17, weight="bold")).pack(anchor="w", padx=12, pady=(12, 6))
        ctk.CTkLabel(
            batch,
            text="这里按“快捷开始”的多赛道任务清单预览每个赛道的音乐目录 / 底图目录 / 输出目录。",
            text_color="#9aa0aa",
        ).pack(anchor="w", padx=12, pady=(0, 8))
        actions = ctk.CTkFrame(batch, fg_color="transparent")
        actions.pack(fill="x", padx=12, pady=(0, 8))
        ctk.CTkButton(actions, text="刷新预览", width=90, fg_color="#334155", command=self._refresh_batch_path_preview).pack(side="left", padx=(0, 8))
        ctk.CTkButton(actions, text="批量建目录", width=90, fg_color="#475569", command=self._prepare_batch_directories).pack(side="left", padx=(0, 8))
        ctk.CTkButton(actions, text="打开所有目录", width=100, fg_color="#475569", command=self._open_batch_directories).pack(side="left")
        self.batch_path_preview_box = ctk.CTkTextbox(batch, height=220)
        self.batch_path_preview_box.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def _build_log_tab(self, parent) -> None:
        toolbar = ctk.CTkFrame(parent, fg_color="transparent")
        toolbar.pack(fill="x", padx=10, pady=(10, 8))
        ctk.CTkButton(toolbar, text="清空日志", width=100, command=self._clear_log).pack(side="left", padx=(0, 8))
        ctk.CTkButton(toolbar, text="停止当前任务", width=120, fg_color="#7f1d1d", command=self._stop_process).pack(side="left")
        self.log_box = ctk.CTkTextbox(parent, font=ctk.CTkFont(family="Consolas", size=13))
        self.log_box.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self._append_log("统一控制台已启动。")

    def _section_card(self, parent, title: str, desc: str | None = None) -> ctk.CTkFrame:
        card = ctk.CTkFrame(parent)
        card.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkLabel(card, text=title, font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 4))
        if desc:
            ctk.CTkLabel(
                card,
                text=desc,
                text_color="#9aa0aa",
                wraplength=940,
                justify="left",
            ).pack(anchor="w", padx=14, pady=(0, 10))
        return card

    def _labeled_entry(
        self,
        parent,
        label: str,
        variable,
        *,
        width: int | None = None,
        placeholder: str = "",
        expand: bool = False,
    ) -> ctk.CTkEntry:
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(side="left", fill="x" if expand else "none", expand=expand, padx=(0, 10))
        ctk.CTkLabel(frame, text=label, text_color="#9aa0aa").pack(anchor="w", pady=(0, 4))
        kwargs = {"textvariable": variable, "placeholder_text": placeholder}
        if width is not None:
            kwargs["width"] = width
        widget = ctk.CTkEntry(frame, **kwargs)
        widget.pack(fill="x")
        return widget

    def _labeled_option(
        self,
        parent,
        label: str,
        variable,
        values: list[str],
        *,
        width: int | None = None,
        command=None,
        expand: bool = False,
    ) -> ctk.CTkOptionMenu:
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(side="left", fill="x" if expand else "none", expand=expand, padx=(0, 10))
        ctk.CTkLabel(frame, text=label, text_color="#9aa0aa").pack(anchor="w", pady=(0, 4))
        kwargs = {"variable": variable, "values": values}
        if width is not None:
            kwargs["width"] = width
        if command is not None:
            kwargs["command"] = command
        widget = ctk.CTkOptionMenu(frame, **kwargs)
        widget.pack(fill="x")
        return widget

    def _manual_field(self, parent, row: int, col: int, label: str, widget_cls, **kwargs):
        frame = ctk.CTkFrame(parent)
        frame.grid(row=row, column=col, sticky="ew", padx=8, pady=6)
        ctk.CTkLabel(frame, text=label).pack(anchor="w", padx=12, pady=(10, 4))
        widget = widget_cls(frame, **kwargs)
        widget.pack(fill="x", padx=12, pady=(0, 10))
        self.manual_widgets.append(widget)
        return widget

    def _path_row(self, parent, label: str, variable) -> None:
        row = ctk.CTkFrame(parent)
        row.pack(fill="x", padx=14, pady=6)
        ctk.CTkLabel(row, text=label, width=90).pack(side="left", padx=(12, 8), pady=10)
        ctk.CTkEntry(row, textvariable=variable).pack(side="left", fill="x", expand=True, padx=(0, 12), pady=10)

    def _pick_directory_for_var(self, variable) -> None:
        selected = filedialog.askdirectory()
        if selected:
            variable.set(selected)

    def _pick_prompt_audience_shot(self) -> None:
        selected = filedialog.askopenfilename(
            title="选择 YouTube 受众截图",
            filetypes=[
                ("图片文件", "*.png *.jpg *.jpeg *.webp"),
                ("PNG", "*.png"),
                ("JPEG", "*.jpg *.jpeg"),
                ("WebP", "*.webp"),
                ("所有文件", "*.*"),
            ],
        )
        if selected:
            self._load_prompt_audience_shot_from_path(Path(selected))

    def _paste_prompt_audience_shot_from_clipboard(self) -> None:
        try:
            grabbed = ImageGrab.grabclipboard()
        except Exception as exc:
            messagebox.showerror("剪贴板读取失败", str(exc))
            return

        if isinstance(grabbed, list):
            for item in grabbed:
                candidate = Path(str(item))
                if candidate.exists() and candidate.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
                    self._load_prompt_audience_shot_from_path(candidate)
                    return
            messagebox.showwarning("剪贴板没有图片", "剪贴板里有文件，但不是可用的图片截图。")
            return

        if isinstance(grabbed, Image.Image):
            self._apply_prompt_audience_image(grabbed.copy(), image_data_url=self._image_to_data_url(grabbed), display_path="")
            return

        messagebox.showwarning("剪贴板没有图片", "当前剪贴板里没有检测到图片截图。")

    def _clear_prompt_audience_shot(self) -> None:
        self.prompt_audience_shot_path_var.set("")
        self.prompt_audience_image_data_url = None
        self.prompt_audience_preview_image = None
        if self.prompt_audience_preview_label is not None:
            self.prompt_audience_preview_label.configure(image=None, text="未选择截图")
        self._textbox_set(self.prompt_audience_parsed_box, "等待截图后自动解析目标人群...")
        self.prompt_status_var.set("已清空受众截图")

    def _restore_prompt_audience_state(self) -> None:
        parsed_text = str(self.ui_state.get("prompt_audience_parsed_text") or "").strip()
        if parsed_text:
            self._textbox_set(self.prompt_audience_parsed_box, parsed_text)
        shot_path = str(self.ui_state.get("prompt_audience_shot_path") or "").strip()
        if shot_path and Path(shot_path).exists():
            try:
                self._load_prompt_audience_shot_from_path(Path(shot_path), auto_analyze=False, update_status=False)
            except Exception:
                self.prompt_audience_shot_path_var.set("")

    def _load_prompt_audience_shot_from_path(
        self,
        path: Path,
        *,
        auto_analyze: bool = True,
        update_status: bool = True,
    ) -> None:
        target = Path(path)
        if not target.exists():
            raise FileNotFoundError(f"截图文件不存在: {target}")
        with Image.open(target) as image:
            loaded = image.copy()
        self._apply_prompt_audience_image(
            loaded,
            image_data_url=self._image_file_to_data_url(target),
            display_path=str(target),
            auto_analyze=auto_analyze,
            update_status=update_status,
        )

    def _apply_prompt_audience_image(
        self,
        image: Image.Image,
        *,
        image_data_url: str,
        display_path: str,
        auto_analyze: bool = True,
        update_status: bool = True,
    ) -> None:
        self.prompt_audience_image_data_url = image_data_url
        self.prompt_audience_shot_path_var.set(display_path)
        self._set_prompt_audience_preview(image)
        if update_status:
            self.prompt_status_var.set("受众截图已加载" + ("，正在识别..." if auto_analyze else ""))
        if auto_analyze:
            self._textbox_set(self.prompt_audience_parsed_box, "正在自动识别截图中的年龄 / 性别 / 地区 / 设备...")
            self._analyze_prompt_audience_shot()

    def _set_prompt_audience_preview(self, image: Image.Image | None) -> None:
        if self.prompt_audience_preview_label is None:
            return
        if image is None:
            self.prompt_audience_preview_image = None
            self.prompt_audience_preview_label.configure(image=None, text="未选择截图")
            return
        preview = image.copy()
        preview.thumbnail((360, 220))
        self.prompt_audience_preview_image = ctk.CTkImage(light_image=preview, dark_image=preview, size=preview.size)
        self.prompt_audience_preview_label.configure(image=self.prompt_audience_preview_image, text="")

    def _image_file_to_data_url(self, path: Path) -> str:
        mime_type = mimetypes.guess_type(str(path))[0] or "image/png"
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"

    def _image_to_data_url(self, image: Image.Image) -> str:
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
        return f"data:image/png;base64,{encoded}"

    def _analyze_prompt_audience_shot(self) -> None:
        if self.prompt_audience_analysis_busy:
            return
        if not self.prompt_audience_image_data_url:
            messagebox.showwarning("缺少截图", "请先选择或粘贴一张 YouTube 后台受众截图。")
            return
        api_preset = self._current_api_preset()
        if not api_preset.get("baseUrl") or not api_preset.get("apiKey") or not api_preset.get("model"):
            tip = "自动识别已跳过：请先配置可用的 Base URL / API Key / 模型。"
            self._textbox_set(self.prompt_audience_parsed_box, tip)
            self.prompt_status_var.set(tip)
            return

        self.prompt_audience_analysis_busy = True
        self._textbox_set(self.prompt_audience_parsed_box, "正在自动识别截图中的年龄 / 性别 / 地区 / 设备...")
        self.prompt_status_var.set("正在识别受众截图...")
        image_data_url = self.prompt_audience_image_data_url
        preset = clone_json(api_preset)

        def worker() -> None:
            try:
                result = analyze_audience_screenshot(preset, image_data_url)
            except Exception as exc:
                self.after(0, lambda: self._finish_prompt_audience_analysis(error=str(exc)))
                return
            self.after(0, lambda: self._finish_prompt_audience_analysis(result=result))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_prompt_audience_analysis(self, *, result: dict | None = None, error: str = "") -> None:
        self.prompt_audience_analysis_busy = False
        if error:
            self._textbox_set(self.prompt_audience_parsed_box, f"自动识别失败：{error}")
            self.prompt_status_var.set("受众截图识别失败")
            self._append_log(f"[提示词] 受众截图识别失败: {error}")
            return

        formatted_text = str((result or {}).get("formatted_text") or "").strip()
        audience_summary = str((result or {}).get("audience_summary") or "").strip()
        self._textbox_set(self.prompt_audience_parsed_box, formatted_text or "未识别到可用受众数据")
        if audience_summary:
            self.content_audience_var.set(audience_summary)
            self.prompt_status_var.set("已自动识别受众截图，并同步到目标受众")
            self._append_log(f"[提示词] 已自动识别受众截图: {audience_summary}")
        else:
            self.prompt_status_var.set("截图已识别，但没有提取到可用的目标受众摘要")

    def _collect_group_upload_settings(self) -> dict:
        visibility = self.group_upload_visibility_var.get().strip() or "public"
        if self.group_upload_schedule_enabled_var.get():
            visibility = "schedule"
        try:
            interval_minutes = int(self.group_upload_schedule_interval_var.get().strip() or "60")
        except Exception:
            interval_minutes = 60
        return {
            "version": 1,
            "tag": self.group_upload_tag_var.get().strip(),
            "source_video_dir": self.group_upload_source_dir_var.get().strip(),
            "source_thumbnail_dir": self.group_upload_thumb_dir_var.get().strip(),
            "selected_serials_text": self.group_upload_serials_var.get().strip(),
            "generation_mode": self.group_upload_mode_var.get().strip() or "site_api",
            "visibility": visibility,
            "category": self.group_upload_category_var.get().strip() or "Music",
            "made_for_kids": bool(self.group_upload_made_for_kids_var.get()),
            "altered_content": bool(self.group_upload_altered_content_var.get()),
            "schedule_enabled": bool(self.group_upload_schedule_enabled_var.get()),
            "schedule_start": self.group_upload_schedule_start_var.get().strip(),
            "schedule_interval_minutes": interval_minutes,
        }

    def _window_plan_path(self) -> Path:
        raw_date = str(self.date_var.get()).strip() or "draft"
        try:
            raw_date = normalize_mmdd(raw_date)
        except Exception:
            raw_date = "".join(ch for ch in raw_date if ch.isalnum()) or "draft"
        return SCRIPT_DIR / "data" / f"window_upload_plan_{raw_date}.json"

    def _collect_window_plan_defaults(self) -> tuple[dict, str, int]:
        settings = self._collect_group_upload_settings()
        schedule_requested = settings["visibility"] == "schedule" or settings["schedule_enabled"]
        default_upload_options = {
            "visibility": "schedule" if schedule_requested else settings["visibility"],
            "category": settings["category"],
            "made_for_kids": settings["made_for_kids"],
            "altered_content": settings["altered_content"],
        }
        schedule_start = settings["schedule_start"] if schedule_requested else ""
        schedule_interval = int(settings["schedule_interval_minutes"])
        return default_upload_options, schedule_start, schedule_interval

    def _build_window_plan(self) -> dict:
        default_upload_options, schedule_start, schedule_interval = self._collect_window_plan_defaults()
        if str(default_upload_options.get("visibility") or "").strip().lower() == "schedule" and not schedule_start:
            raise ValueError("已启用定时发布，但还没填写开始时间。")
        default_tag = self.upload_picker_tag_var.get().strip() or self.upload_tag_var.get().strip() or self.tag_var.get().strip()
        if default_tag == "全部标签":
            default_tag = ""
        scope_mode = SCOPE_MANUAL if self._textbox_get(self.manual_window_plan_box) else (self.window_scope_mode_var.get().strip() or SCOPE_SAME_GROUP)
        plan = build_window_upload_plan(
            scope_mode=scope_mode,
            default_tag=default_tag,
            same_group_serials_text=self.same_group_serials_var.get().strip(),
            multi_group_text=self._textbox_get(self.multi_group_plan_box),
            manual_text=self._textbox_get(self.manual_window_plan_box),
            default_upload_options=default_upload_options,
            schedule_start=schedule_start,
            schedule_interval_minutes=schedule_interval,
        )
        if not plan.get("tasks"):
            raise ValueError("请先设置今天要上传的窗口。")
        return plan

    def _scope_mode_label(self, mode: str | None = None) -> str:
        return WINDOW_SCOPE_LABELS.get(mode or self.window_scope_mode_var.get().strip(), "未设置")

    def _preview_window_plan(self, *, show_error: bool = True) -> dict | None:
        try:
            plan = self._build_window_plan()
            plan_path = save_window_upload_plan(self._window_plan_path(), plan)
        except Exception as e:
            self.window_plan_status_var.set(f"窗口任务未就绪：{e}")
            if self.window_plan_preview_box is not None:
                self._textbox_set(
                    self.window_plan_preview_box,
                    "先把今天要上传的窗口加进任务区，然后再预览。\n\n"
                    "示例：\n"
                    "90 | 面壁者\n"
                    "91 | 面壁者 | title=自定义标题\n"
                    "95 | 芝加哥蓝调 | is_ypp=true | visibility=private",
                )
            if show_error:
                messagebox.showerror("窗口任务错误", str(e))
            return None

        lines = [
            f"模式: {self._scope_mode_label(plan.get('scope_mode'))}",
            f"计划文件: {plan_path}",
            "",
            *plan.get("preview_lines", []),
        ]
        warnings = plan.get("warnings", [])
        if warnings:
            lines.extend(["", "警告:"])
            lines.extend(f"- {item}" for item in warnings)
        if self.window_plan_preview_box is not None:
            self._textbox_set(self.window_plan_preview_box, "\n".join(lines).strip())
        self.window_plan_status_var.set(f"已整理 {len(plan.get('tasks', []))} 个窗口任务")
        return plan

    def _prepare_window_plan_for_run(self) -> tuple[dict, Path, list[str], list[int]]:
        plan = self._preview_window_plan(show_error=False)
        if not plan:
            raise ValueError(self.window_plan_status_var.get())
        plan_path = save_window_upload_plan(self._window_plan_path(), plan)
        tags, skip_channels = derive_tags_and_skip_channels(plan, lambda tag: get_tag_info(tag) or {})
        if not tags:
            raise ValueError("当前窗口任务没有解析出任何分组。")
        return plan, plan_path, tags, skip_channels

    def _preview_ready_folder_upload_plan(self) -> dict:
        plan, _plan_path, _tags, _skip_channels = self._prepare_window_plan_for_run()
        source_dir = self.group_upload_source_dir_var.get().strip()
        if not source_dir:
            raise ValueError("请先填写现成视频文件夹")
        prepared = prepare_window_task_upload_batch(
            script_dir=SCRIPT_DIR,
            scheduler_config_path=SCHEDULER_CONFIG_FILE,
            prompt_studio_path=PROMPT_STUDIO_FILE,
            channel_mapping_path=CHANNEL_MAPPING_FILE,
            window_plan=plan,
            date_value=self.date_var.get().strip(),
            source_video_dir=Path(source_dir),
            thumbnail_dir=Path(self.group_upload_thumb_dir_var.get().strip()) if self.group_upload_thumb_dir_var.get().strip() else None,
            metadata_mode=self.upload_metadata_mode_var.get().strip() or "prompt_api",
            fill_title_desc_tags=bool(self.upload_fill_text_var.get()),
            fill_thumbnails=bool(self.upload_fill_thumbnails_var.get()),
            sync_daily_content=bool(self.upload_sync_daily_content_var.get()),
        )
        lines = [
            f"来源: 现成视频文件夹",
            f"tags: {', '.join(prepared.get('tags', []))}",
            f"assigned: {prepared.get('assigned_count', 0)}",
            "",
            *prepared.get("preview_lines", []),
        ]
        manifest_paths = prepared.get("manifest_paths", {})
        if manifest_paths:
            lines.extend(["", "manifest:"])
            lines.extend(f"- {tag}: {path}" for tag, path in manifest_paths.items())
        warnings = prepared.get("warnings", [])
        if warnings:
            lines.extend(["", "warnings:"])
            lines.extend(f"- {item}" for item in warnings)
        self._textbox_set(self.window_plan_preview_box, "\n".join(lines).strip())
        self.window_plan_status_var.set(f"已准备现成视频上传计划：{prepared.get('assigned_count', 0)} 个窗口")
        return prepared

    def _preview_current_upload_plan(self) -> None:
        try:
            if self._current_upload_entry_mode() == "group_folder":
                self._preview_ready_folder_upload_plan()
            else:
                self._preview_window_plan()
        except Exception as e:
            messagebox.showerror("预览失败", str(e))

    def _build_ready_folder_upload_cmd(self) -> list[str]:
        _plan, plan_path, tags, _skip_channels = self._prepare_window_plan_for_run()
        source_dir = self.group_upload_source_dir_var.get().strip()
        if not source_dir:
            raise ValueError("请先填写现成视频文件夹")
        primary_tag = tags[0] if tags else (self.upload_tag_var.get().strip() or self.tag_var.get().strip())
        args = [
            sys.executable,
            str(GROUP_UPLOAD_SCRIPT),
            "--tag",
            primary_tag,
            "--date",
            normalize_mmdd(self.date_var.get()),
            "--source-dir",
            source_dir,
            "--window-plan-file",
            str(plan_path),
            "--metadata-mode",
            self.upload_metadata_mode_var.get().strip() or "prompt_api",
            "--auto-confirm",
        ]
        if self.group_upload_thumb_dir_var.get().strip():
            args.extend(["--thumb-dir", self.group_upload_thumb_dir_var.get().strip()])
        if self.upload_fill_text_var.get():
            args.append("--fill-text")
        else:
            args.append("--no-fill-text")
        if self.upload_fill_thumbnails_var.get():
            args.append("--fill-thumbnails")
        else:
            args.append("--no-fill-thumbnails")
        if self.upload_sync_daily_content_var.get():
            args.append("--sync-daily-content")
        else:
            args.append("--no-sync-daily-content")
        if self.auto_close_browser_var.get():
            args.append("--auto-close-browser")
        return args

    def _run_ready_folder_upload(self) -> None:
        try:
            cmd = self._build_ready_folder_upload_cmd()
        except Exception as e:
            messagebox.showerror("参数错误", str(e))
            return
        self._run_process(cmd, job_name="现成视频批量上传")

    def _save_group_upload_settings(self) -> None:
        settings = self._collect_group_upload_settings()
        save_upload_batch_settings(UPLOAD_BATCH_SETTINGS_FILE, settings)
        self.group_upload_status_var.set("分组批量上传设置已保存")
        self._append_log("[分组批量上传] 已保存设置")

    def _group_upload_output_dir(self) -> Path:
        date_mmdd = normalize_mmdd(self.date_var.get())
        tag = self.group_upload_tag_var.get().strip()
        return resolve_local_path(self.output_dir_var.get().strip() or "workspace/AutoTask") / f"{date_mmdd}_{tag}"

    def _open_group_upload_output_dir(self) -> None:
        try:
            open_target(self._group_upload_output_dir())
        except Exception as e:
            messagebox.showerror("打开失败", str(e))

    def _preview_group_upload_plan(self) -> None:
        try:
            settings = self._collect_group_upload_settings()
            save_upload_batch_settings(UPLOAD_BATCH_SETTINGS_FILE, settings)
            prepared = prepare_group_upload_batch(
                script_dir=SCRIPT_DIR,
                scheduler_config_path=SCHEDULER_CONFIG_FILE,
                prompt_studio_path=PROMPT_STUDIO_FILE,
                channel_mapping_path=CHANNEL_MAPPING_FILE,
                tag=self.group_upload_tag_var.get().strip(),
                date_value=self.date_var.get().strip(),
                source_video_dir=Path(self.group_upload_source_dir_var.get().strip()),
                thumbnail_dir=Path(self.group_upload_thumb_dir_var.get().strip()) if self.group_upload_thumb_dir_var.get().strip() else None,
                selected_serials=parse_serials_text(self.group_upload_serials_var.get().strip()),
                generation_mode=settings["generation_mode"],
                visibility=settings["visibility"],
                category=settings["category"],
                made_for_kids=settings["made_for_kids"],
                altered_content=settings["altered_content"],
                schedule_enabled=settings["schedule_enabled"],
                schedule_start=settings["schedule_start"],
                schedule_interval_minutes=settings["schedule_interval_minutes"],
            )
        except Exception as e:
            messagebox.showerror("生成计划失败", str(e))
            return

        lines = [
            f"tag={prepared['tag']}",
            f"date={prepared['date']}",
            f"manifest={prepared['manifest_path']}",
            f"assigned={prepared['assigned_count']}",
            "",
        ]
        lines.extend(prepared["preview_lines"])
        if prepared["warnings"]:
            lines.append("")
            lines.append("Warnings:")
            lines.extend([f"- {item}" for item in prepared["warnings"]])
        self._textbox_set(self.group_upload_preview_box, "\n".join(lines).strip())
        self.group_upload_status_var.set(f"已准备 {prepared['assigned_count']} 个频道的上传计划")
        self._append_log(f"[分组批量上传] 已生成计划: {prepared['manifest_path']}")

    def _build_group_upload_cmd(self, *, dry_run: bool = False) -> list[str]:
        settings = self._collect_group_upload_settings()
        tag = self.group_upload_tag_var.get().strip()
        source_dir = self.group_upload_source_dir_var.get().strip()
        if not tag:
            raise ValueError("请先选择一个上传分组")
        if not source_dir:
            raise ValueError("请先填写现成视频文件夹")

        args = [
            sys.executable,
            str(GROUP_UPLOAD_SCRIPT),
            "--tag",
            tag,
            "--date",
            normalize_mmdd(self.date_var.get()),
            "--source-dir",
            source_dir,
            "--generation-mode",
            settings["generation_mode"],
            "--visibility",
            settings["visibility"],
            "--category",
            settings["category"],
            "--schedule-interval-minutes",
            str(settings["schedule_interval_minutes"]),
            "--auto-confirm",
        ]
        if self.group_upload_thumb_dir_var.get().strip():
            args.extend(["--thumb-dir", self.group_upload_thumb_dir_var.get().strip()])
        if self.group_upload_serials_var.get().strip():
            args.extend(["--serials", self.group_upload_serials_var.get().strip()])
        if settings["schedule_enabled"] and settings["schedule_start"]:
            args.extend(["--schedule-start", settings["schedule_start"]])
        if settings["made_for_kids"]:
            args.append("--made-for-kids")
        else:
            args.append("--not-made-for-kids")
        if settings["altered_content"]:
            args.append("--altered-content-yes")
        else:
            args.append("--altered-content-no")
        if self.auto_close_browser_var.get():
            args.append("--auto-close-browser")
        if dry_run:
            args.append("--dry-run")
        return args

    def _run_group_upload(self, *, dry_run: bool = False) -> None:
        try:
            save_upload_batch_settings(UPLOAD_BATCH_SETTINGS_FILE, self._collect_group_upload_settings())
            cmd = self._build_group_upload_cmd(dry_run=dry_run)
        except Exception as e:
            messagebox.showerror("参数错误", str(e))
            return
        self._run_process(cmd, job_name="分组批量上传")

    def _load_tags(self) -> None:
        tags = get_all_tags()
        values = ["全部标签", *tags] if tags else ["全部标签"]
        self.tag_menu.configure(values=values)
        self.upload_tag_menu.configure(values=tags or [""])
        if self.upload_picker_tag_menu is not None:
            self.upload_picker_tag_menu.configure(values=tags or [""])
        if hasattr(self, "generation_tag_menu") and self.generation_tag_menu is not None:
            self.generation_tag_menu.configure(values=tags or [""])
        if hasattr(self, "prompt_tag_menu") and self.prompt_tag_menu is not None:
            self.prompt_tag_menu.configure(values=tags or [""])
        if hasattr(self, "group_upload_tag_menu") and self.group_upload_tag_menu is not None:
            self.group_upload_tag_menu.configure(values=tags or [""])
        if self.tag_var.get() not in values:
            self.tag_var.set(values[1] if len(values) > 1 else values[0])
        if self.upload_tag_var.get() not in (tags or [""]):
            self.upload_tag_var.set(self.tag_var.get() if self.tag_var.get() != "全部标签" else (tags[0] if tags else ""))
        if self.generation_tag_var.get() not in (tags or [""]):
            self.generation_tag_var.set(self.upload_tag_var.get() or (tags[0] if tags else ""))
        if self.prompt_tag_var.get() not in (tags or [""]):
            self.prompt_tag_var.set(self.tag_var.get() if self.tag_var.get() != "全部标签" else (tags[0] if tags else ""))
        if self.group_upload_tag_var.get() not in (tags or [""]):
            self.group_upload_tag_var.set(self.upload_tag_var.get() or (tags[0] if tags else ""))
        if self.upload_picker_tag_var.get() not in (tags or [""]):
            self.upload_picker_tag_var.set(self.upload_tag_var.get() or (tags[0] if tags else ""))
        self._sync_upload_channels()
        self._sync_generation_channels()
        self._refresh_upload_window_buttons()

    def _sync_tag_to_upload(self) -> None:
        if self.tag_var.get() and self.tag_var.get() != "全部标签":
            self.upload_tag_var.set(self.tag_var.get())
            self.upload_picker_tag_var.set(self.tag_var.get())
            self.generation_tag_var.set(self.tag_var.get())
            self.prompt_tag_var.set(self.tag_var.get())
            self.group_upload_tag_var.set(self.tag_var.get())
            self._sync_upload_channels()
            self._sync_generation_channels()
            self._refresh_upload_window_buttons()

    def _sync_upload_channels(self) -> None:
        tag = self.upload_tag_var.get().strip()
        info = get_tag_info(tag) if tag else None
        channels = [str(x) for x in info.get("all_serials", [])] if info else []
        self.channel_menu.configure(values=channels or [""])
        if self.channel_var.get() not in channels:
            self.channel_var.set(channels[0] if channels else "")
        if tag and tag != self.tag_var.get().strip():
            self.tag_var.set(tag)
        if not self.upload_picker_tag_var.get().strip():
            self.upload_picker_tag_var.set(tag)
        self._refresh_upload_window_buttons()

    def _sync_generation_channels(self) -> None:
        tag = self.generation_tag_var.get().strip()
        info = get_tag_info(tag) if tag else None
        channels = [str(x) for x in info.get("all_serials", [])] if info else []
        tag_dir = resolve_local_path(self.scheduler_cfg.get("base_image_dir", "workspace/base_image")) / tag
        generation_map_path = tag_dir / "generation_map.json"
        generation_map = load_generation_map_file(generation_map_path)
        for serial in generation_map.get("channels", {}).keys():
            if str(serial) not in channels:
                channels.append(str(serial))
        channels = sorted(channels, key=lambda item: int(item)) if channels else []
        self.generation_channel_menu.configure(values=channels or [""])
        if self.generation_channel_var.get() not in channels:
            self.generation_channel_var.set(channels[0] if channels else "")

    def _apply_randomize_state(self) -> None:
        state = "disabled" if self.randomize_effects_var.get() else "normal"
        for widget in self.manual_widgets:
            widget.configure(state=state)

    def _handle_batch_preview_change(self, *_args) -> None:
        self._refresh_batch_path_preview()

    def _handle_quick_mode_change(self, *_args) -> None:
        self._sync_task_mode_to_module_flags(initial=True)
        self._preview_window_plan(show_error=False)
        self._refresh_upload_window_buttons()
        self._refresh_quick_upload_mode_summary()

    def _current_upload_entry_mode(self) -> str:
        source_mode = str(self.upload_source_mode_var.get() or "render_output").strip()
        normalized = "group_folder" if source_mode == "ready_folder" else "window_plan"
        if normalized != self.upload_entry_mode_var.get():
            self.upload_entry_mode_var.set(normalized)
        return normalized

    def _upload_mode_label(self, mode: str | None = None) -> str:
        return UPLOAD_ENTRY_MODE_LABELS.get(mode or self._current_upload_entry_mode(), "单频道上传")

    def _open_active_upload_settings_tab(self) -> None:
        self.tabs.set("上传")

    def _run_selected_upload_mode(self) -> None:
        if self._current_upload_entry_mode() == "group_folder":
            self._run_ready_folder_upload()
            return
        self._run_window_plan_upload()

    def _refresh_quick_upload_mode_summary(self) -> None:
        task_mode = self._task_mode_label()
        source_mode = "现成视频文件夹" if self._current_upload_entry_mode() == "group_folder" else "渲染产物目录"
        metadata_mode = "提示词/API" if self.upload_metadata_mode_var.get().strip() == "prompt_api" else "当日内容"
        try:
            plan = self._build_window_plan()
            groups = plan.get("groups", {})
            task_count = len(plan.get("tasks", []))
            if len(groups) <= 2:
                group_text = "；".join(f"{tag}: {', '.join(str(x) for x in serials)}" for tag, serials in groups.items())
            else:
                group_text = f"{len(groups)} 个分组 / {task_count} 个窗口"
        except Exception as e:
            group_text = f"窗口任务未完成：{e}"

        extra_lines = [
            f"本次任务：{task_mode}",
            f"上传来源：{source_mode}",
            f"文案来源：{metadata_mode}",
            f"窗口概览：{group_text}",
            "任务范围统一以“上传”页里的窗口任务和日期为准。",
        ]
        if self._current_upload_entry_mode() == "group_folder":
            extra_lines.append(f"现成视频目录：{self.group_upload_source_dir_var.get().strip() or '未设置'}")
        if self.today_task_mode_var.get() == "render_only":
            extra_lines.append("当前只会执行剪辑，不会执行上传。")
        elif self.today_task_mode_var.get() == "upload_only":
            extra_lines.append("当前只会执行上传，不会重新剪辑。")
        else:
            extra_lines.append("当前会先剪辑，再按上传页里的窗口任务继续上传。")
        self.quick_upload_mode_status_var.set("\n".join(extra_lines))

    def _parse_tag_lines(self, raw: str) -> list[str]:
        seen = set()
        tags: list[str] = []
        all_known = set(get_all_tags())
        normalized = raw.replace("，", ",").replace("；", ",").replace(";", ",")
        for line in normalized.splitlines():
            for chunk in line.split(","):
                tag = chunk.strip()
                if not tag or tag in seen:
                    continue
                if all_known and tag not in all_known:
                    continue
                seen.add(tag)
                tags.append(tag)
        return tags

    def _batch_tags(self) -> list[str]:
        return self._parse_tag_lines(self._textbox_get(self.batch_tags_box))

    def _append_current_tag_to_batch(self) -> None:
        current = self._primary_tag()
        if not current or current == "全部标签":
            return
        tags = self._batch_tags()
        if current not in tags:
            tags.append(current)
        self._textbox_set(self.batch_tags_box, "\n".join(tags))
        self.batch_status_var.set(f"已加入当前 tag：{current}")
        self._refresh_batch_path_preview()

    def _normalize_batch_tags_box(self) -> None:
        tags = self._batch_tags()
        self._textbox_set(self.batch_tags_box, "\n".join(tags))
        self.batch_status_var.set(f"已整理 {len(tags)} 个赛道")
        self._refresh_batch_path_preview()

    def _effective_tags(self) -> list[str]:
        tags = self._batch_tags()
        if tags:
            return tags
        for current in (
            self.upload_tag_var.get().strip(),
            self.upload_picker_tag_var.get().strip(),
            self.tag_var.get().strip(),
        ):
            if current and current != "全部标签":
                return [current]
        return []

    def _primary_tag(self) -> str:
        for candidate in (
            self.upload_tag_var.get().strip(),
            self.upload_picker_tag_var.get().strip(),
            self.tag_var.get().strip(),
            self.group_upload_tag_var.get().strip(),
        ):
            if candidate and candidate != "全部标签":
                return candidate
        return ""

    def _scheduler_tags_for_current_mode(self) -> list[str]:
        if self._current_upload_entry_mode() == "window_plan":
            try:
                _plan, _path, tags, _skip_channels = self._prepare_window_plan_for_run()
            except Exception:
                return []
            return tags
        if self._current_upload_entry_mode() == "single_channel":
            tag = self._primary_tag()
            return [tag] if tag else []
        return self._effective_tags()

    def _prepare_batch_directories(self) -> None:
        try:
            date_mmdd = normalize_mmdd(self.date_var.get())
        except Exception as e:
            messagebox.showerror("日期错误", str(e))
            return

        tags = self._effective_tags()
        if not tags:
            messagebox.showwarning("没有赛道", "请先在多赛道任务清单里填写 tag，或先选一个当前 tag。")
            return

        music_root = resolve_local_path(self.music_dir_var.get().strip() or "workspace/music")
        image_root = resolve_local_path(self.image_dir_var.get().strip() or "workspace/base_image")
        output_root = resolve_local_path(self.output_dir_var.get().strip() or "workspace/AutoTask")
        for tag in tags:
            (music_root / tag).mkdir(parents=True, exist_ok=True)
            (image_root / tag).mkdir(parents=True, exist_ok=True)
            (output_root / f"{date_mmdd}_{tag}").mkdir(parents=True, exist_ok=True)

        self.batch_status_var.set(f"已准备 {len(tags)} 个赛道目录")
        self._append_log(f"[多赛道] 已准备目录: {', '.join(tags)}")
        self._refresh_batch_path_preview()

    def _open_batch_directories(self) -> None:
        try:
            date_mmdd = normalize_mmdd(self.date_var.get())
        except Exception:
            date_mmdd = self.date_var.get().strip()

        tags = self._effective_tags()
        if not tags:
            messagebox.showwarning("没有赛道", "请先在多赛道任务清单里填写 tag，或先选一个当前 tag。")
            return

        music_root = resolve_local_path(self.music_dir_var.get().strip() or "workspace/music")
        image_root = resolve_local_path(self.image_dir_var.get().strip() or "workspace/base_image")
        output_root = resolve_local_path(self.output_dir_var.get().strip() or "workspace/AutoTask")
        for tag in tags:
            open_target(music_root / tag)
            open_target(image_root / tag)
            open_target(output_root / f"{date_mmdd}_{tag}")
        self.batch_status_var.set(f"已打开 {len(tags)} 个赛道目录")

    def _refresh_batch_path_preview(self) -> None:
        tags = self._effective_tags()
        if not self.batch_path_preview_box:
            return
        if not tags:
            self._textbox_set(self.batch_path_preview_box, "暂无多赛道任务。你可以在“快捷开始”页的任务清单里每行填一个 tag。")
            return
        try:
            date_mmdd = normalize_mmdd(self.date_var.get())
        except Exception:
            date_mmdd = self.date_var.get().strip()
        music_root = resolve_local_path(self.music_dir_var.get().strip() or "workspace/music")
        image_root = resolve_local_path(self.image_dir_var.get().strip() or "workspace/base_image")
        output_root = resolve_local_path(self.output_dir_var.get().strip() or "workspace/AutoTask")
        lines = []
        for tag in tags:
            lines.append(f"[{tag}]")
            lines.append(f"  music : {music_root / tag}")
            lines.append(f"  image : {image_root / tag}")
            lines.append(f"  output: {output_root / f'{date_mmdd}_{tag}'}")
            lines.append("")
        self._textbox_set(self.batch_path_preview_box, "\n".join(lines).strip())

    def _build_scheduler_cmd(self, *, render_only: bool) -> list[str]:
        date_mmdd = normalize_mmdd(self.date_var.get())
        mode = self._current_upload_entry_mode()
        if mode == "window_plan" and (render_only or not self.auto_upload_var.get()):
            primary_tag = self._primary_tag()
            tags = [primary_tag] if primary_tag else []
        else:
            tags = self._scheduler_tags_for_current_mode()
        args = [
            sys.executable,
            str(SCHEDULER_SCRIPT),
            "--standard",
            date_mmdd,
            f"--song-count={self.song_count_var.get().strip() or '1'}",
        ]
        if tags:
            args.append("--tags=" + ",".join(tags))
        if mode == "window_plan" and self.auto_upload_var.get() and not render_only:
            _plan, plan_path, _tags, skip_channels = self._prepare_window_plan_for_run()
            args.append("--window-plan-file=" + str(plan_path))
            if skip_channels:
                args.append("--skip-channels=" + ",".join(str(item) for item in skip_channels))
        elif mode == "single_channel" and self.auto_upload_var.get() and not render_only:
            tag = self.upload_tag_var.get().strip() or self._primary_tag()
            channel = self.channel_var.get().strip()
            info = get_tag_info(tag) if tag else None
            all_serials = [str(item) for item in (info.get("all_serials", []) if info else [])]
            if channel and channel in all_serials:
                skip_channels = [item for item in all_serials if item != channel]
                if skip_channels:
                    args.append("--skip-channels=" + ",".join(skip_channels))
        if render_only or not self.auto_upload_var.get():
            args.append("--render-only")
        if self.randomize_effects_var.get():
            args.append("--randomize-effects")
        else:
            if not self.spectrum_var.get():
                args.append("--no-spectrum")
            if not self.timeline_var.get():
                args.append("--no-timeline")
            args.append(f"--letterbox={LETTERBOX_MAP[self.letterbox_var.get()]}")
            args.append(f"--zoom={ZOOM_MAP[self.zoom_var.get()]}")
            args.append(f"--color-spectrum={COLOR_MAP[self.color_spectrum_var.get()]}")
            args.append(f"--color-timeline={COLOR_MAP[self.color_timeline_var.get()]}")
            args.append(f"--style={STYLE_MAP[self.style_var.get()]}")
            particle = PARTICLE_MAP[self.particle_var.get()]
            if particle != "none":
                args.append(f"--particle={particle}")
                args.append(f"--particle-opacity={self.particle_opacity_var.get().strip() or '0.55'}")
            if self.film_grain_var.get():
                args.append(f"--film-grain={self.grain_strength_var.get().strip() or '10'}")
            if self.vignette_var.get():
                args.append("--vignette")
            if self.soft_focus_var.get():
                args.append(f"--soft-focus={self.soft_focus_sigma_var.get().strip() or '1.2'}")
            tint = TINT_MAP[self.color_tint_var.get()]
            if tint != "none":
                args.append(f"--color-tint={tint}")
            text = self.text_var.get().strip()
            if text:
                args.append(f"--text={text}")
                args.append(f"--text-pos={TEXT_POS_MAP[self.text_pos_var.get()]}")
                args.append(f"--text-size={self.text_size_var.get().strip() or '60'}")
                args.append(f"--text-style={TEXT_STYLE_MAP[self.text_style_var.get()]}")
        if not self.auto_close_browser_var.get():
            args.append("--keep-upload-browser-open")
        return args

    def _build_bulk_upload_cmd(self) -> list[str]:
        date_mmdd = normalize_mmdd(self.date_var.get())
        tags = self._effective_tags()
        if not tags:
            raise ValueError("请先在多赛道任务清单填写至少一个 tag。")
        args = [
            sys.executable,
            str(BULK_UPLOAD_SCRIPT),
            "--date",
            date_mmdd,
            "--tags",
            ",".join(tags),
            "--auto-confirm",
        ]
        if self.auto_close_browser_var.get():
            args.append("--auto-close-browser")
        return args

    def _build_window_plan_upload_cmd(self) -> list[str]:
        date_mmdd = normalize_mmdd(self.date_var.get())
        _plan, plan_path, tags, skip_channels = self._prepare_window_plan_for_run()
        args = [
            sys.executable,
            str(UPLOAD_SCRIPT),
            "--tag",
            ",".join(tags),
            "--date",
            date_mmdd,
            "--auto-confirm",
            "--window-plan-file",
            str(plan_path),
        ]
        if skip_channels:
            args.append("--skip-channels=" + ",".join(str(item) for item in skip_channels))
        if self.auto_close_browser_var.get():
            args.append("--auto-close-browser")
        return args

    def _build_upload_cmd(self) -> list[str]:
        date_mmdd = normalize_mmdd(self.date_var.get())
        tag = self.upload_tag_var.get().strip()
        if not tag:
            raise ValueError("请先选择上传标签")
        channel = self.channel_var.get().strip()
        args = [
            sys.executable,
            str(UPLOAD_SCRIPT),
            "--tag",
            tag,
            "--date",
            date_mmdd,
            "--auto-confirm",
        ]
        if channel:
            args.extend(["--channel", channel])
        if self.auto_close_browser_var.get():
            args.append("--auto-close-browser")
        return args

    def _save_scheduler_paths(self) -> None:
        cfg = self.scheduler_cfg.copy()
        cfg["music_dir"] = self.music_dir_var.get().strip()
        cfg["base_image_dir"] = self.image_dir_var.get().strip()
        cfg["output_root"] = self.output_dir_var.get().strip()
        cfg["ffmpeg_bin"] = self.ffmpeg_var.get().strip() or "ffmpeg"
        cfg["ffmpeg_path"] = cfg["ffmpeg_bin"]
        save_scheduler_config(cfg)
        self.scheduler_cfg = load_scheduler_config()
        self.status_var.set("路径配置已保存")
        self._append_log("已保存路径配置。")
        self._refresh_batch_path_preview()

    def _collect_state(self) -> dict:
        return {
            "date": self.date_var.get(),
            "tag": self.tag_var.get(),
            "upload_tag": self.upload_tag_var.get(),
            "generation_tag": self.generation_tag_var.get(),
            "generation_channel": self.generation_channel_var.get(),
            "prompt_tag": self.prompt_tag_var.get(),
            "batch_tags_text": self._textbox_get(self.batch_tags_box),
            "upload_entry_mode": self._current_upload_entry_mode(),
            "today_task_mode": self.today_task_mode_var.get(),
            "upload_source_mode": self.upload_source_mode_var.get(),
            "upload_metadata_mode": self.upload_metadata_mode_var.get(),
            "upload_fill_text": bool(self.upload_fill_text_var.get()),
            "upload_fill_thumbnails": bool(self.upload_fill_thumbnails_var.get()),
            "upload_sync_daily_content": bool(self.upload_sync_daily_content_var.get()),
            "song_count": self.song_count_var.get(),
            "channel": self.channel_var.get(),
            "window_scope_mode": self.window_scope_mode_var.get(),
            "same_group_serials_text": self.same_group_serials_var.get(),
            "multi_group_plan_text": self._textbox_get(self.multi_group_plan_box),
            "manual_window_plan_text": self._textbox_get(self.manual_window_plan_box),
            "upload_picker_tag": self.upload_picker_tag_var.get(),
            "upload_window_ypp_override": self.upload_window_ypp_override_var.get(),
            "upload_window_title_override": self.upload_window_title_override_var.get(),
            "upload_window_visibility_override": self.upload_window_visibility_override_var.get(),
            "upload_window_category_override": self.upload_window_category_override_var.get(),
            "upload_window_made_for_kids_override": self.upload_window_made_for_kids_override_var.get(),
            "upload_window_altered_content_override": self.upload_window_altered_content_override_var.get(),
            "audio_workers": int(self.audio_workers_var.get()),
            "video_workers": int(self.video_workers_var.get()),
            "render_enabled": bool(self.render_enabled_var.get()),
            "randomize_effects": bool(self.randomize_effects_var.get()),
            "auto_upload": bool(self.auto_upload_var.get()),
            "auto_close_browser": bool(self.auto_close_browser_var.get()),
            "auto_sync_manifest": bool(self.auto_sync_manifest_var.get()),
            "spectrum": bool(self.spectrum_var.get()),
            "timeline": bool(self.timeline_var.get()),
            "letterbox": self.letterbox_var.get(),
            "zoom": self.zoom_var.get(),
            "color_spectrum": self.color_spectrum_var.get(),
            "color_timeline": self.color_timeline_var.get(),
            "style": self.style_var.get(),
            "particle": self.particle_var.get(),
            "particle_opacity": self.particle_opacity_var.get(),
            "film_grain": bool(self.film_grain_var.get()),
            "grain_strength": self.grain_strength_var.get(),
            "vignette": bool(self.vignette_var.get()),
            "soft_focus": bool(self.soft_focus_var.get()),
            "soft_focus_sigma": self.soft_focus_sigma_var.get(),
            "color_tint": self.color_tint_var.get(),
            "text": self.text_var.get(),
            "text_pos": self.text_pos_var.get(),
            "text_size": self.text_size_var.get(),
            "text_style": self.text_style_var.get(),
            "api_preset_name": self.api_preset_name_var.get(),
            "content_template_name": self.content_template_name_var.get(),
            "prompt_audience_shot_path": self.prompt_audience_shot_path_var.get().strip(),
            "prompt_audience_parsed_text": self._textbox_get(self.prompt_audience_parsed_box),
        }

    def _textbox_get(self, box: ctk.CTkTextbox | None) -> str:
        if box is None:
            return ""
        return box.get("1.0", "end").strip()

    def _textbox_set(self, box: ctk.CTkTextbox | None, text: str) -> None:
        if box is None:
            return
        box.delete("1.0", "end")
        if text:
            box.insert("1.0", text)

    def _save_workspace_state(self) -> None:
        save_dashboard_state(self._collect_state())
        save_upload_batch_settings(UPLOAD_BATCH_SETTINGS_FILE, self._collect_group_upload_settings())
        self.status_var.set("当前工作台配置已保存")
        self._append_log("已保存当前工作台配置。")

    def _run_current_flow(self) -> None:
        mode = self._current_upload_entry_mode()
        if self.render_enabled_var.get():
            if self.auto_upload_var.get() and mode == "group_folder":
                messagebox.showwarning(
                    "流程冲突",
                    "“上传现成视频文件夹”只适合上传已经做好的视频，不参与渲染。\n请关闭“启用渲染模块”，或把上传来源改成“上传渲染产物”。",
                )
                return
            self._run_scheduler(render_only=not self.auto_upload_var.get())
            return
        if self.auto_upload_var.get():
            self._run_selected_upload_mode()
            return
        messagebox.showwarning("没有启用模块", "请至少开启一个模块：渲染 或 上传。")

    def _refresh_preset_menus(self) -> None:
        api_names = sorted(self.prompt_cfg.get("apiPresets", {}).keys()) or [default_api_preset()["name"]]
        content_names = sorted(self.prompt_cfg.get("contentTemplates", {}).keys()) or [default_content_template()["name"]]
        if self.api_preset_menu is not None:
            self.api_preset_menu.configure(values=api_names)
        if self.content_template_menu is not None:
            self.content_template_menu.configure(values=content_names)
        if self.api_preset_name_var.get() not in api_names:
            self.api_preset_name_var.set(api_names[0])
        if self.content_template_name_var.get() not in content_names:
            self.content_template_name_var.set(pick_content_template_name(self.prompt_cfg, self.prompt_tag_var.get().strip()))

    def _apply_bound_content_template(self) -> None:
        target_name = pick_content_template_name(self.prompt_cfg, self.prompt_tag_var.get().strip())
        if target_name in self.prompt_cfg.get("contentTemplates", {}):
            self.content_template_name_var.set(target_name)
            self._load_content_template()

    def _current_api_preset(self) -> dict:
        return {
            "templateType": "api",
            "name": self.api_preset_name_var.get().strip() or "默认API模板",
            "provider": self.api_provider_var.get().strip(),
            "apiKey": self.api_key_var.get().strip(),
            "baseUrl": self.api_base_url_var.get().strip(),
            "model": self.api_model_var.get().strip(),
            "temperature": self.api_temperature_var.get().strip(),
            "maxTokens": self.api_max_tokens_var.get().strip(),
            "autoImageEnabled": self.api_auto_image_var.get().strip(),
            "imageBaseUrl": self.image_base_url_var.get().strip(),
            "imageApiKey": self.image_api_key_var.get().strip(),
            "imageModel": self.image_model_var.get().strip(),
            "imageConcurrency": self.image_concurrency_var.get().strip(),
            "outputLanguage": self.content_output_language_var.get().strip(),
        }

    def _current_content_template(self) -> dict:
        return {
            "templateType": "content",
            "name": self.content_template_name_var.get().strip() or "默认内容模板",
            "musicGenre": self.content_music_genre_var.get().strip(),
            "angle": self.content_angle_var.get().strip(),
            "audience": self.content_audience_var.get().strip(),
            "outputLanguage": self.content_output_language_var.get().strip(),
            "titleCount": self.content_title_count_var.get().strip(),
            "descCount": self.content_desc_count_var.get().strip(),
            "thumbCount": self.content_thumb_count_var.get().strip(),
            "titleMin": self.content_title_min_var.get().strip(),
            "titleMax": self.content_title_max_var.get().strip(),
            "descLen": self.content_desc_len_var.get().strip(),
            "tagRange": self.content_tag_range_var.get().strip(),
            "masterPrompt": self._textbox_get(self.prompt_master_box),
            "titleLibrary": self._textbox_get(self.prompt_title_library_box),
        }

    def _load_api_preset(self) -> None:
        preset_name = self.api_preset_name_var.get().strip()
        preset = clone_json(self.prompt_cfg.get("apiPresets", {}).get(preset_name, default_api_preset(preset_name or "默认API模板")))
        self.api_preset_name_var.set(preset.get("name", "默认API模板"))
        self.api_provider_var.set(preset.get("provider", "openai_compatible"))
        self.api_key_var.set(preset.get("apiKey", ""))
        self.api_base_url_var.set(preset.get("baseUrl", ""))
        self.api_model_var.set(preset.get("model", ""))
        self.api_temperature_var.set(str(preset.get("temperature", "0.9")))
        self.api_max_tokens_var.set(str(preset.get("maxTokens", "16000")))
        self.api_auto_image_var.set(str(preset.get("autoImageEnabled", "0")))
        self.image_base_url_var.set(preset.get("imageBaseUrl", ""))
        self.image_api_key_var.set(preset.get("imageApiKey", ""))
        self.image_model_var.set(preset.get("imageModel", ""))
        self.image_concurrency_var.set(str(preset.get("imageConcurrency", "3")))
        self.prompt_status_var.set(f"已加载 API 模板：{self.api_preset_name_var.get()}")

    def _load_content_template(self) -> None:
        template_name = self.content_template_name_var.get().strip()
        template = clone_json(self.prompt_cfg.get("contentTemplates", {}).get(template_name, default_content_template(template_name or "默认内容模板")))
        self.content_template_name_var.set(template.get("name", "默认内容模板"))
        self.content_music_genre_var.set(template.get("musicGenre", ""))
        self.content_angle_var.set(template.get("angle", ""))
        self.content_audience_var.set(template.get("audience", ""))
        self.content_output_language_var.set(template.get("outputLanguage", "zh-TW"))
        self.content_title_count_var.set(str(template.get("titleCount", "3")))
        self.content_desc_count_var.set(str(template.get("descCount", "1")))
        self.content_thumb_count_var.set(str(template.get("thumbCount", "3")))
        self.content_title_min_var.set(str(template.get("titleMin", "80")))
        self.content_title_max_var.set(str(template.get("titleMax", "95")))
        self.content_desc_len_var.set(str(template.get("descLen", "300")))
        self.content_tag_range_var.set(str(template.get("tagRange", "10-20")))
        self._textbox_set(self.prompt_master_box, template.get("masterPrompt", ""))
        self._textbox_set(self.prompt_title_library_box, template.get("titleLibrary", ""))
        self.prompt_status_var.set(f"已加载内容模板：{self.content_template_name_var.get()}")

    def _save_api_preset(self) -> None:
        preset = self._current_api_preset()
        self.prompt_cfg.setdefault("apiPresets", {})
        self.prompt_cfg["apiPresets"][preset["name"]] = preset
        save_prompt_studio_config(PROMPT_STUDIO_FILE, self.prompt_cfg)
        self._refresh_preset_menus()
        self.api_preset_name_var.set(preset["name"])
        self.prompt_status_var.set(f"已保存 API 模板：{preset['name']}")
        self._append_log(f"[提示词] 已保存 API 模板：{preset['name']}")

    def _save_content_template(self) -> None:
        template = self._current_content_template()
        self.prompt_cfg.setdefault("contentTemplates", {})
        self.prompt_cfg["contentTemplates"][template["name"]] = template
        save_prompt_studio_config(PROMPT_STUDIO_FILE, self.prompt_cfg)
        self._refresh_preset_menus()
        self.content_template_name_var.set(template["name"])
        self.prompt_status_var.set(f"已保存内容模板：{template['name']}")
        self._append_log(f"[提示词] 已保存内容模板：{template['name']}")

    def _bind_tag_to_content_template(self) -> None:
        tag = self.prompt_tag_var.get().strip()
        template_name = self.content_template_name_var.get().strip()
        if not tag:
            messagebox.showerror("缺少标签", "请先选择一个分组标签。")
            return
        if not template_name:
            messagebox.showerror("缺少模板", "请先选择一个内容模板。")
            return
        self.prompt_cfg.setdefault("tagBindings", {})
        self.prompt_cfg["tagBindings"][tag] = template_name
        save_prompt_studio_config(PROMPT_STUDIO_FILE, self.prompt_cfg)
        self.prompt_status_var.set(f"已绑定：{tag} -> {template_name}")
        self._append_log(f"[提示词] 已绑定 {tag} -> {template_name}")

    def _preview_prompt_bundle(self) -> None:
        preview = build_site_preview(self._current_content_template(), self._current_api_preset())
        self._textbox_set(self.prompt_preview_box, preview)
        self.prompt_status_var.set("已刷新主提示词预览")

    def _generation_tag_dir(self) -> Path:
        return resolve_local_path(self.scheduler_cfg.get("base_image_dir", "workspace/base_image")) / self.generation_tag_var.get().strip()

    def _generation_map_path(self) -> Path:
        return self._generation_tag_dir() / "generation_map.json"

    def _output_root_path(self) -> Path:
        return resolve_local_path(self.scheduler_cfg.get("output_root", "workspace/AutoTask"))

    def _open_generation_tag_dir(self) -> None:
        open_target(self._generation_tag_dir())

    def _load_generation_entry(self, silent: bool = False) -> None:
        try:
            date_mmdd = normalize_mmdd(self.date_var.get())
        except Exception as e:
            if not silent:
                messagebox.showerror("日期错误", str(e))
            return
        tag = self.generation_tag_var.get().strip()
        channel = self.generation_channel_var.get().strip()
        if not tag or not channel:
            return
        generation_map = load_generation_map_file(self._generation_map_path())
        channel_info = generation_map.setdefault("channels", {}).setdefault(channel, {"is_ypp": False, "days": {}})
        day_info = channel_info.get("days", {}).get(date_mmdd, {})
        self.generation_is_ypp_var.set(bool(channel_info.get("is_ypp", False)))
        self.generation_set_var.set(str(day_info.get("set", 1)))
        self._textbox_set(self.gen_title_box, day_info.get("title", ""))
        self._textbox_set(self.gen_desc_box, day_info.get("description", ""))
        covers = day_info.get("covers", []) or guess_cover_names(self._generation_tag_dir(), date_mmdd, int(channel))
        self._textbox_set(self.gen_covers_box, "\n".join(covers))
        self._textbox_set(self.gen_ab_titles_box, "\n".join(day_info.get("ab_titles", [])))
        channel_name = self.channel_name_map.get(channel, "")
        self.generation_status_var.set(f"{tag} / {channel}{(' / ' + channel_name) if channel_name else ''} / {date_mmdd}")
        if not silent:
            self._append_log(f"[generation_map] 已加载 {tag} / {channel} / {date_mmdd}")

    def _guess_generation_covers(self) -> None:
        try:
            date_mmdd = normalize_mmdd(self.date_var.get())
        except Exception as e:
            messagebox.showerror("日期错误", str(e))
            return
        tag = self.generation_tag_var.get().strip()
        channel = self.generation_channel_var.get().strip()
        if not tag or not channel:
            return
        covers = guess_cover_names(self._generation_tag_dir(), date_mmdd, int(channel))
        self._textbox_set(self.gen_covers_box, "\n".join(covers))
        self.generation_status_var.set(f"已自动匹配封面：{', '.join(covers) if covers else '未找到'}")

    def _save_generation_entry(self) -> None:
        try:
            date_mmdd = normalize_mmdd(self.date_var.get())
        except Exception as e:
            messagebox.showerror("日期错误", str(e))
            return
        tag = self.generation_tag_var.get().strip()
        channel = self.generation_channel_var.get().strip()
        if not tag or not channel:
            messagebox.showerror("缺少目标", "请先选择分组和频道。")
            return

        generation_map = load_generation_map_file(self._generation_map_path())
        channel_info = generation_map.setdefault("channels", {}).setdefault(channel, {"is_ypp": False, "days": {}})
        channel_info["is_ypp"] = bool(self.generation_is_ypp_var.get())
        channel_info.setdefault("days", {})
        channel_info["days"][date_mmdd] = {
            "title": self._textbox_get(self.gen_title_box),
            "description": self._textbox_get(self.gen_desc_box),
            "covers": [line.strip() for line in self._textbox_get(self.gen_covers_box).splitlines() if line.strip()],
            "ab_titles": [line.strip() for line in self._textbox_get(self.gen_ab_titles_box).splitlines() if line.strip()],
            "set": int(self.generation_set_var.get().strip() or "1"),
        }
        save_generation_map_file(self._generation_map_path(), generation_map)
        self.generation_status_var.set(f"已保存 generation_map：{self._generation_map_path()}")
        self._append_log(f"[generation_map] 已保存 {tag} / {channel} / {date_mmdd}")
        if self.auto_sync_manifest_var.get():
            self._sync_current_manifest_only()

    def _sync_current_manifest_only(self) -> None:
        try:
            date_mmdd = normalize_mmdd(self.date_var.get())
        except Exception as e:
            messagebox.showerror("日期错误", str(e))
            return
        tag = self.generation_tag_var.get().strip()
        if not tag:
            messagebox.showerror("缺少标签", "请先选择分组。")
            return
        generation_map = load_generation_map_file(self._generation_map_path())
        manifest_path, count = sync_manifest_from_generation_map(
            generation_map,
            self._generation_tag_dir(),
            self._output_root_path(),
            tag,
            date_mmdd,
        )
        self.generation_status_var.set(f"已同步 manifest：{manifest_path}（{count} 个频道）")
        self._append_log(f"[manifest] 已同步 {manifest_path}")

    def _push_generation_to_prompt_preview(self) -> None:
        self.prompt_tag_var.set(self.generation_tag_var.get())
        bound_template = pick_content_template_name(self.prompt_cfg, self.prompt_tag_var.get().strip())
        if bound_template in self.prompt_cfg.get("contentTemplates", {}):
            self.content_template_name_var.set(bound_template)
            self._load_content_template()
        preview_text = build_site_preview(self._current_content_template(), self._current_api_preset())
        title = self._textbox_get(self.gen_title_box)
        desc = self._textbox_get(self.gen_desc_box)
        self._textbox_set(
            self.prompt_preview_box,
            preview_text + f"\n\n=== 当前 generation_map 内容 ===\n标题: {title}\n\n简介:\n{desc}",
        )
        self.tabs.set("提示词")
        self.prompt_status_var.set("已把当前 generation_map 内容带到提示词预览")

    def _run_scheduler(self, *, render_only: bool) -> None:
        try:
            cmd = self._build_scheduler_cmd(render_only=render_only)
        except Exception as e:
            messagebox.showerror("参数错误", str(e))
            return
        self._run_process(cmd, job_name="渲染调度")

    def _run_upload_only(self) -> None:
        try:
            cmd = self._build_upload_cmd()
        except Exception as e:
            messagebox.showerror("参数错误", str(e))
            return
        self._run_process(cmd, job_name="单频道上传")

    def _run_window_plan_upload(self) -> None:
        try:
            cmd = self._build_window_plan_upload_cmd()
        except Exception as e:
            messagebox.showerror("参数错误", str(e))
            return
        self._run_process(cmd, job_name="窗口任务上传")

    def _run_bulk_upload(self) -> None:
        try:
            cmd = self._build_bulk_upload_cmd()
        except Exception as e:
            messagebox.showerror("参数错误", str(e))
            return
        self._run_process(cmd, job_name="多赛道批量上传")

    def _run_process(self, cmd: list[str], *, job_name: str) -> None:
        if self.process and self.process.poll() is None:
            messagebox.showwarning("任务进行中", "当前已经有任务在跑，先停止或等它结束。")
            return

        save_dashboard_state(self._collect_state())
        env = os.environ.copy()
        env["AUDIO_WORKERS"] = str(int(self.audio_workers_var.get()))
        env["VIDEO_WORKERS"] = str(int(self.video_workers_var.get()))

        self.tabs.set("日志")
        self.status_var.set(f"{job_name} 已启动")
        self._append_log("")
        self._append_log(f"[启动] {job_name}")
        self._append_log(" ".join(cmd))

        try:
            self.process = subprocess.Popen(
                cmd,
                cwd=str(SCRIPT_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
            )
        except Exception as e:
            self.status_var.set(f"{job_name} 启动失败")
            self._append_log(f"[错误] 启动失败: {e}")
            return

        def reader():
            assert self.process is not None
            for line in self.process.stdout:
                self.after(0, self._append_log, line.rstrip())
            code = self.process.wait()
            self.after(0, self._process_finished, job_name, code)

        self.log_thread = threading.Thread(target=reader, daemon=True)
        self.log_thread.start()

    def _process_finished(self, job_name: str, code: int) -> None:
        self.status_var.set(f"{job_name} 已结束 (exit={code})")
        self._append_log(f"[结束] {job_name} exit={code}")
        self.process = None

    def _stop_process(self) -> None:
        if not self.process or self.process.poll() is not None:
            self._append_log("当前没有运行中的任务。")
            return
        self.process.terminate()
        self.status_var.set("已请求停止当前任务")
        self._append_log("[操作] 已请求停止当前任务")

    def _append_log(self, text: str) -> None:
        self.log_box.insert("end", text + "\n")
        self.log_box.see("end")

    def _clear_log(self) -> None:
        self.log_box.delete("1.0", "end")

    def _launch_tool(self, path: Path) -> None:
        try:
            subprocess.Popen([sys.executable, str(path)], cwd=str(SCRIPT_DIR))
            self._append_log(f"[工具] 已打开 {path.name}")
        except Exception as e:
            messagebox.showerror("启动失败", str(e))

    def _on_close(self) -> None:
        save_dashboard_state(self._collect_state())
        save_upload_batch_settings(UPLOAD_BATCH_SETTINGS_FILE, self._collect_group_upload_settings())
        self.destroy()


def main():
    app = CommandCenterApp()
    app.mainloop()


if __name__ == "__main__":
    main()
