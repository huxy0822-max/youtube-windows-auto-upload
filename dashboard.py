#!/usr/bin/env python3
"""
统一控制台。

目标：
1. 把常用的渲染调度、上传、路径配置放到一个入口里。
2. 默认使用“全随机视觉”策略，降低同质化风险。
3. 保留旧版高级入口，避免一次性重写全部 GUI 逻辑。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import List

import customtkinter as ctk
from tkinter import filedialog, messagebox

from group_upload_workflow import (
    load_upload_batch_settings,
    parse_serials_text,
    prepare_group_upload_batch,
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
        "song_count": "1",
        "channel": "",
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
        self.gen_title_box: ctk.CTkTextbox | None = None
        self.gen_desc_box: ctk.CTkTextbox | None = None
        self.gen_covers_box: ctk.CTkTextbox | None = None
        self.gen_ab_titles_box: ctk.CTkTextbox | None = None
        self.batch_tags_box: ctk.CTkTextbox | None = None
        self.batch_path_preview_box: ctk.CTkTextbox | None = None
        self.group_upload_tag_menu: ctk.CTkOptionMenu | None = None
        self.group_upload_preview_box: ctk.CTkTextbox | None = None

        self._build_variables()
        self._build_ui()
        self._load_tags()
        self._sync_upload_channels()
        self._sync_generation_channels()
        self._refresh_preset_menus()
        self._load_api_preset()
        self._load_content_template()
        self._load_generation_entry(silent=True)
        self._refresh_batch_path_preview()
        self._apply_randomize_state()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_variables(self) -> None:
        self.date_var = ctk.StringVar(value=self.ui_state["date"])
        self.tag_var = ctk.StringVar(value=self.ui_state["tag"])
        self.upload_tag_var = ctk.StringVar(value=self.ui_state["upload_tag"] or self.ui_state["tag"])
        self.generation_tag_var = ctk.StringVar(value=self.ui_state["generation_tag"] or self.ui_state["upload_tag"] or self.ui_state["tag"])
        self.prompt_tag_var = ctk.StringVar(value=self.ui_state["prompt_tag"] or self.ui_state["tag"])
        self.song_count_var = ctk.StringVar(value=self.ui_state["song_count"])
        self.channel_var = ctk.StringVar(value=self.ui_state["channel"])
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

        for traced_var in (
            self.date_var,
            self.tag_var,
            self.music_dir_var,
            self.image_dir_var,
            self.output_dir_var,
        ):
            traced_var.trace_add("write", lambda *_: self._refresh_batch_path_preview())

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
        self.tabs.add("分组批量上传")
        self.tabs.add("提示词")
        self.tabs.add("当日内容")
        self.tabs.add("高级视觉")
        self.tabs.add("路径配置")
        self.tabs.add("日志")

        self._build_quick_tab(self.tabs.tab("快捷开始"))
        self._build_upload_tab(self.tabs.tab("上传"))
        self._build_group_upload_tab(self.tabs.tab("分组批量上传"))
        self._build_prompt_tab(self.tabs.tab("提示词"))
        self._build_generation_tab(self.tabs.tab("当日内容"))
        self._build_advanced_tab(self.tabs.tab("高级视觉"))
        self._build_paths_tab(self.tabs.tab("路径配置"))
        self._build_log_tab(self.tabs.tab("日志"))

    def _build_quick_tab(self, parent) -> None:
        row1 = ctk.CTkFrame(parent, fg_color="transparent")
        row1.pack(fill="x", padx=10, pady=(10, 8))
        self.tag_menu = ctk.CTkOptionMenu(row1, variable=self.tag_var, values=["加载中..."], width=220, command=lambda _: self._sync_tag_to_upload())
        self.tag_menu.pack(side="left", padx=(0, 10))
        ctk.CTkEntry(row1, textvariable=self.date_var, width=120, placeholder_text="MMDD / 3.12").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(row1, textvariable=self.song_count_var, width=90, placeholder_text="母带歌曲数").pack(side="left", padx=(0, 10))
        ctk.CTkButton(row1, text="刷新标签", width=100, command=self._load_tags).pack(side="left", padx=(0, 10))
        ctk.CTkButton(row1, text="打开输出目录", width=120, command=lambda: open_folder(self.output_dir_var.get())).pack(side="right")

        row2 = ctk.CTkFrame(parent, fg_color="transparent")
        row2.pack(fill="x", padx=10, pady=8)
        ctk.CTkSwitch(row2, text="启用渲染模块", variable=self.render_enabled_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row2, text="启用上传模块", variable=self.auto_upload_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row2, text="全随机视觉（推荐）", variable=self.randomize_effects_var, command=self._apply_randomize_state).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row2, text="上传后自动关闭浏览器", variable=self.auto_close_browser_var).pack(side="left", padx=(0, 16))

        row2b = ctk.CTkFrame(parent, fg_color="transparent")
        row2b.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkSwitch(row2b, text="保存后同步 upload_manifest", variable=self.auto_sync_manifest_var).pack(side="left", padx=(0, 16))
        ctk.CTkButton(row2b, text="打开提示词页", width=120, fg_color="#334155", command=lambda: self.tabs.set("提示词")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(row2b, text="打开当日内容页", width=120, fg_color="#334155", command=lambda: self.tabs.set("当日内容")).pack(side="left", padx=(0, 10))
        ctk.CTkButton(row2b, text="保存工作台", width=120, fg_color="#475569", command=self._save_workspace_state).pack(side="left", padx=(0, 10))

        row3 = ctk.CTkFrame(parent, fg_color="transparent")
        row3.pack(fill="x", padx=10, pady=8)
        ctk.CTkLabel(row3, text="音频并行").pack(side="left")
        ctk.CTkSlider(row3, from_=1, to=8, variable=self.audio_workers_var, number_of_steps=7, width=180).pack(side="left", padx=8)
        ctk.CTkLabel(row3, textvariable=ctk.StringVar(value="")).pack_forget()
        ctk.CTkLabel(row3, text="视频并行").pack(side="left", padx=(16, 0))
        ctk.CTkSlider(row3, from_=1, to=8, variable=self.video_workers_var, number_of_steps=7, width=180).pack(side="left", padx=8)

        batch = ctk.CTkFrame(parent)
        batch.pack(fill="x", padx=10, pady=(4, 10))
        ctk.CTkLabel(batch, text="多赛道任务清单（每行一个 tag）", font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=12, pady=(12, 6))
        self.batch_tags_box = ctk.CTkTextbox(batch, height=90)
        self.batch_tags_box.pack(fill="x", padx=12, pady=(0, 8))
        if self.ui_state.get("batch_tags_text"):
            self.batch_tags_box.insert("1.0", self.ui_state["batch_tags_text"])
        batch_actions = ctk.CTkFrame(batch, fg_color="transparent")
        batch_actions.pack(fill="x", padx=12, pady=(0, 10))
        ctk.CTkButton(batch_actions, text="加入当前 tag", width=110, fg_color="#334155", command=self._append_current_tag_to_batch).pack(side="left", padx=(0, 8))
        ctk.CTkButton(batch_actions, text="整理去重", width=100, fg_color="#334155", command=self._normalize_batch_tags_box).pack(side="left", padx=(0, 8))
        ctk.CTkButton(batch_actions, text="批量建目录", width=100, fg_color="#475569", command=self._prepare_batch_directories).pack(side="left", padx=(0, 8))
        ctk.CTkButton(batch_actions, text="打开多赛道目录", width=120, fg_color="#475569", command=self._open_batch_directories).pack(side="left", padx=(0, 8))
        ctk.CTkLabel(batch_actions, textvariable=self.batch_status_var, text_color="#9aa0aa").pack(side="right")

        note = ctk.CTkLabel(
            parent,
            text="日常推荐：先配置提示词模板，再检查当天标题/简介/封面，最后回这里执行当前流程。",
            text_color="#9aa0aa",
        )
        note.pack(anchor="w", padx=14, pady=(4, 12))

        actions = ctk.CTkFrame(parent, fg_color="transparent")
        actions.pack(fill="x", padx=10, pady=(0, 12))
        ctk.CTkButton(actions, text="开始当前流程", height=42, command=self._run_current_flow).pack(fill="x", pady=(0, 10))
        ctk.CTkButton(actions, text="仅渲染", height=40, fg_color="#475569", command=lambda: self._run_scheduler(render_only=True)).pack(fill="x", pady=(0, 10))
        ctk.CTkButton(actions, text="打开旧版高级调度器", height=38, fg_color="#334155", command=lambda: self._launch_tool(LEGACY_SCHEDULER)).pack(fill="x", pady=(0, 10))
        ctk.CTkButton(actions, text="打开旧版渲染工作站", height=38, fg_color="#334155", command=lambda: self._launch_tool(LEGACY_RENDERER)).pack(fill="x")

        status = ctk.CTkFrame(parent)
        status.pack(fill="x", padx=10, pady=(6, 10))
        ctk.CTkLabel(status, text="当前状态").pack(anchor="w", padx=12, pady=(10, 2))
        ctk.CTkLabel(status, textvariable=self.status_var, text_color="#a3e635").pack(anchor="w", padx=12, pady=(0, 10))

    def _build_upload_tab(self, parent) -> None:
        top = ctk.CTkFrame(parent, fg_color="transparent")
        top.pack(fill="x", padx=10, pady=(12, 8))
        self.upload_tag_menu = ctk.CTkOptionMenu(top, variable=self.upload_tag_var, values=["加载中..."], width=220, command=lambda _: self._sync_upload_channels())
        self.upload_tag_menu.pack(side="left", padx=(0, 10))
        ctk.CTkEntry(top, textvariable=self.date_var, width=120, placeholder_text="MMDD / 3.12").pack(side="left", padx=(0, 10))
        self.channel_menu = ctk.CTkOptionMenu(top, variable=self.channel_var, values=[""], width=220)
        self.channel_menu.pack(side="left", padx=(0, 10))
        ctk.CTkButton(top, text="刷新频道", width=100, command=self._sync_upload_channels).pack(side="left")

        ctk.CTkLabel(
            parent,
            text="单频道上传建议从这里跑。多频道批量仍建议先用上面的流水线。",
            text_color="#9aa0aa",
        ).pack(anchor="w", padx=14, pady=(0, 10))

        ctk.CTkButton(parent, text="开始单频道上传", height=42, command=self._run_upload_only).pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkButton(parent, text="打开上传记录目录", height=38, fg_color="#475569", command=lambda: open_folder(str(SCRIPT_DIR / "upload_records"))).pack(fill="x", padx=10)
        ctk.CTkLabel(
            parent,
            text="如果今天要连跑多个赛道，直接在“快捷开始”的多赛道任务清单里填 tag，然后回来点下面这个按钮。",
            text_color="#9aa0aa",
        ).pack(anchor="w", padx=14, pady=(12, 8))
        ctk.CTkButton(parent, text="按多赛道任务清单批量上传", height=42, fg_color="#334155", command=self._run_bulk_upload).pack(fill="x", padx=10)
        ctk.CTkButton(parent, text="打开“分组批量上传”专用页", height=38, fg_color="#475569", command=lambda: self.tabs.set("分组批量上传")).pack(fill="x", padx=10, pady=(10, 0))

    def _build_group_upload_tab(self, parent) -> None:
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=8, pady=8)

        header = ctk.CTkFrame(scroll)
        header.pack(fill="x", padx=10, pady=(10, 8))
        ctk.CTkLabel(header, text="分组批量上传", font=ctk.CTkFont(size=22, weight="bold")).pack(anchor="w", padx=14, pady=(12, 4))
        ctk.CTkLabel(
            header,
            text="适合这种场景：一个文件夹里有多条现成视频，你要把它们按顺序发到同一个比特浏览器分组下的多个频道。",
            text_color="#9aa0aa",
            wraplength=900,
            justify="left",
        ).pack(anchor="w", padx=14, pady=(0, 12))

        row1 = ctk.CTkFrame(scroll, fg_color="transparent")
        row1.pack(fill="x", padx=10, pady=(0, 8))
        self.group_upload_tag_menu = ctk.CTkOptionMenu(row1, variable=self.group_upload_tag_var, values=["加载中..."], width=220)
        self.group_upload_tag_menu.pack(side="left", padx=(0, 10))
        ctk.CTkEntry(row1, textvariable=self.date_var, width=120, placeholder_text="MMDD / 3.12").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(row1, textvariable=self.group_upload_source_dir_var, placeholder_text="现成视频文件夹").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkButton(row1, text="选择视频目录", width=110, fg_color="#334155", command=lambda: self._pick_directory_for_var(self.group_upload_source_dir_var)).pack(side="left")

        row2 = ctk.CTkFrame(scroll, fg_color="transparent")
        row2.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkEntry(row2, textvariable=self.group_upload_thumb_dir_var, placeholder_text="可选：现成缩略图文件夹").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkButton(row2, text="选择缩略图目录", width=120, fg_color="#334155", command=lambda: self._pick_directory_for_var(self.group_upload_thumb_dir_var)).pack(side="left", padx=(0, 10))
        ctk.CTkEntry(row2, textvariable=self.group_upload_serials_var, width=280, placeholder_text="可选：限定频道序号，如 90,94,95").pack(side="left")

        row3 = ctk.CTkFrame(scroll, fg_color="transparent")
        row3.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkOptionMenu(row3, variable=self.group_upload_mode_var, values=["site_api", "legacy"], width=170).pack(side="left", padx=(0, 10))
        ctk.CTkOptionMenu(row3, variable=self.group_upload_visibility_var, values=["public", "private", "unlisted", "schedule"], width=160).pack(side="left", padx=(0, 10))
        ctk.CTkOptionMenu(
            row3,
            variable=self.group_upload_category_var,
            values=["Music", "People & Blogs", "Education", "Entertainment", "News & Politics", "Gaming", "Sports", "Travel & Events"],
            width=180,
        ).pack(side="left", padx=(0, 10))
        ctk.CTkLabel(row3, textvariable=self.group_upload_status_var, text_color="#9aa0aa", wraplength=340, justify="left").pack(side="left", fill="x", expand=True)

        row4 = ctk.CTkFrame(scroll, fg_color="transparent")
        row4.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkSwitch(row4, text="儿童内容", variable=self.group_upload_made_for_kids_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row4, text="AI / 合成内容", variable=self.group_upload_altered_content_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row4, text="启用定时发布", variable=self.group_upload_schedule_enabled_var).pack(side="left", padx=(0, 16))
        ctk.CTkEntry(row4, textvariable=self.group_upload_schedule_start_var, width=180, placeholder_text="YYYY-MM-DD HH:MM").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(row4, textvariable=self.group_upload_schedule_interval_var, width=100, placeholder_text="间隔分钟").pack(side="left")

        hint = ctk.CTkLabel(
            scroll,
            text="生成模式：`site_api` = 走你网页版那套 API；`legacy` = 走原先 generation_map 逻辑。默认建议 `site_api`。",
            text_color="#9aa0aa",
            wraplength=960,
            justify="left",
        )
        hint.pack(anchor="w", padx=14, pady=(0, 10))

        actions = ctk.CTkFrame(scroll)
        actions.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkButton(actions, text="保存本页设置", width=110, fg_color="#475569", command=self._save_group_upload_settings).pack(side="left", padx=(0, 8), pady=12)
        ctk.CTkButton(actions, text="预览并准备上传计划", width=150, command=self._preview_group_upload_plan).pack(side="left", padx=(0, 8), pady=12)
        ctk.CTkButton(actions, text="Dry-run 测试上传", width=130, fg_color="#334155", command=lambda: self._run_group_upload(dry_run=True)).pack(side="left", padx=(0, 8), pady=12)
        ctk.CTkButton(actions, text="开始分组批量上传", width=150, fg_color="#2563eb", command=self._run_group_upload).pack(side="left", padx=(0, 8), pady=12)
        ctk.CTkButton(actions, text="打开 staging 输出目录", width=140, fg_color="#334155", command=self._open_group_upload_output_dir).pack(side="left", pady=12)

        preview = ctk.CTkFrame(scroll)
        preview.pack(fill="both", expand=True, padx=10, pady=(0, 12))
        ctk.CTkLabel(preview, text="上传计划预览", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 8))
        self.group_upload_preview_box = ctk.CTkTextbox(preview, height=320)
        self.group_upload_preview_box.pack(fill="both", expand=True, padx=14, pady=(0, 14))

    def _build_prompt_tab(self, parent) -> None:
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=8, pady=8)

        header = ctk.CTkFrame(scroll)
        header.pack(fill="x", padx=10, pady=(10, 8))
        ctk.CTkLabel(header, text="提示词 / API 模板", font=ctk.CTkFont(size=22, weight="bold")).pack(anchor="w", padx=14, pady=(12, 4))
        ctk.CTkLabel(
            header,
            text="字段参考你那个网页版站点：文本模型、图片模型、主提示词、标题库、标题/简介/缩略图数量都在这里。",
            text_color="#9aa0aa",
        ).pack(anchor="w", padx=14, pady=(0, 12))

        top = ctk.CTkFrame(scroll, fg_color="transparent")
        top.pack(fill="x", padx=10, pady=(0, 8))
        self.prompt_tag_menu = ctk.CTkOptionMenu(top, variable=self.prompt_tag_var, values=["加载中..."], width=180, command=lambda _: self._apply_bound_content_template())
        self.prompt_tag_menu.pack(side="left", padx=(0, 10))
        self.api_preset_menu = ctk.CTkOptionMenu(top, variable=self.api_preset_name_var, values=["默认API模板"], width=200, command=lambda _: self._load_api_preset())
        self.api_preset_menu.pack(side="left", padx=(0, 10))
        self.content_template_menu = ctk.CTkOptionMenu(top, variable=self.content_template_name_var, values=["默认内容模板"], width=220, command=lambda _: self._load_content_template())
        self.content_template_menu.pack(side="left", padx=(0, 10))
        ctk.CTkButton(top, text="绑定当前分组到内容模板", width=150, command=self._bind_tag_to_content_template).pack(side="left", padx=(0, 10))
        ctk.CTkButton(top, text="打开配置文件", width=110, fg_color="#334155", command=lambda: open_target(PROMPT_STUDIO_FILE)).pack(side="left")

        api = ctk.CTkFrame(scroll)
        api.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkLabel(api, text="文本 / 图片 API 模板", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 8))
        api_row1 = ctk.CTkFrame(api, fg_color="transparent")
        api_row1.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkEntry(api_row1, textvariable=self.api_provider_var, width=160, placeholder_text="provider").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(api_row1, textvariable=self.api_base_url_var, placeholder_text="text base url").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkEntry(api_row1, textvariable=self.api_model_var, width=220, placeholder_text="text model").pack(side="left")

        api_row2 = ctk.CTkFrame(api, fg_color="transparent")
        api_row2.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkEntry(api_row2, textvariable=self.api_key_var, placeholder_text="text api key").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkEntry(api_row2, textvariable=self.api_temperature_var, width=90, placeholder_text="temp").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(api_row2, textvariable=self.api_max_tokens_var, width=120, placeholder_text="max tokens").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(api_row2, textvariable=self.api_auto_image_var, width=120, placeholder_text="auto image 0/1").pack(side="left")

        api_row3 = ctk.CTkFrame(api, fg_color="transparent")
        api_row3.pack(fill="x", padx=10, pady=(0, 12))
        ctk.CTkEntry(api_row3, textvariable=self.image_base_url_var, placeholder_text="image base url").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkEntry(api_row3, textvariable=self.image_api_key_var, placeholder_text="image api key").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkEntry(api_row3, textvariable=self.image_model_var, width=220, placeholder_text="image model").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(api_row3, textvariable=self.image_concurrency_var, width=90, placeholder_text="并发").pack(side="left")

        content = ctk.CTkFrame(scroll)
        content.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkLabel(content, text="内容模板（网页版字段）", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 8))
        content_row1 = ctk.CTkFrame(content, fg_color="transparent")
        content_row1.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkEntry(content_row1, textvariable=self.content_music_genre_var, placeholder_text="内容/音乐类型").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkEntry(content_row1, textvariable=self.content_angle_var, placeholder_text="切入角度").pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkEntry(content_row1, textvariable=self.content_audience_var, placeholder_text="目标群体").pack(side="left", fill="x", expand=True)

        content_row2 = ctk.CTkFrame(content, fg_color="transparent")
        content_row2.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkEntry(content_row2, textvariable=self.content_output_language_var, width=120, placeholder_text="语言").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(content_row2, textvariable=self.content_title_count_var, width=90, placeholder_text="标题数").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(content_row2, textvariable=self.content_desc_count_var, width=90, placeholder_text="简介数").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(content_row2, textvariable=self.content_thumb_count_var, width=90, placeholder_text="缩略图数").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(content_row2, textvariable=self.content_title_min_var, width=90, placeholder_text="标题最小").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(content_row2, textvariable=self.content_title_max_var, width=90, placeholder_text="标题最大").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(content_row2, textvariable=self.content_desc_len_var, width=100, placeholder_text="简介字数").pack(side="left", padx=(0, 10))
        ctk.CTkEntry(content_row2, textvariable=self.content_tag_range_var, width=120, placeholder_text="标签区间").pack(side="left")

        ctk.CTkLabel(content, text="主提示词（可直接参考网页版）").pack(anchor="w", padx=14, pady=(0, 6))
        self.prompt_master_box = ctk.CTkTextbox(content, height=160)
        self.prompt_master_box.pack(fill="x", padx=14, pady=(0, 10))
        ctk.CTkLabel(content, text="对标标题库").pack(anchor="w", padx=14, pady=(0, 6))
        self.prompt_title_library_box = ctk.CTkTextbox(content, height=120)
        self.prompt_title_library_box.pack(fill="x", padx=14, pady=(0, 12))

        actions = ctk.CTkFrame(scroll)
        actions.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkButton(actions, text="保存 API 模板", command=self._save_api_preset).pack(side="left", padx=(0, 10), pady=12)
        ctk.CTkButton(actions, text="保存内容模板", command=self._save_content_template).pack(side="left", padx=(0, 10), pady=12)
        ctk.CTkButton(actions, text="预览当前主提示词", fg_color="#334155", command=self._preview_prompt_bundle).pack(side="left", padx=(0, 10), pady=12)
        ctk.CTkLabel(actions, textvariable=self.prompt_status_var, text_color="#9aa0aa").pack(side="right", padx=12)

        preview = ctk.CTkFrame(scroll)
        preview.pack(fill="both", expand=True, padx=10, pady=(0, 12))
        ctk.CTkLabel(preview, text="送给模型前的预览", font=ctk.CTkFont(size=18, weight="bold")).pack(anchor="w", padx=14, pady=(12, 8))
        self.prompt_preview_box = ctk.CTkTextbox(preview, height=240)
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
        self._sync_upload_channels()
        self._sync_generation_channels()

    def _sync_tag_to_upload(self) -> None:
        if self.tag_var.get() and self.tag_var.get() != "全部标签":
            self.upload_tag_var.set(self.tag_var.get())
            self.generation_tag_var.set(self.tag_var.get())
            self.prompt_tag_var.set(self.tag_var.get())
            self._sync_upload_channels()
            self._sync_generation_channels()

    def _sync_upload_channels(self) -> None:
        tag = self.upload_tag_var.get().strip()
        info = get_tag_info(tag) if tag else None
        channels = [str(x) for x in info.get("all_serials", [])] if info else []
        self.channel_menu.configure(values=channels or [""])
        if self.channel_var.get() not in channels:
            self.channel_var.set(channels[0] if channels else "")

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
        current = self.tag_var.get().strip()
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
        current = self.tag_var.get().strip()
        if current and current != "全部标签":
            return [current]
        return []

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
        tags = self._effective_tags()
        args = [
            sys.executable,
            str(SCHEDULER_SCRIPT),
            "--standard",
            date_mmdd,
            f"--song-count={self.song_count_var.get().strip() or '1'}",
        ]
        if tags:
            args.append("--tags=" + ",".join(tags))
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
            "song_count": self.song_count_var.get(),
            "channel": self.channel_var.get(),
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
        if self.render_enabled_var.get():
            self._run_scheduler(render_only=not self.auto_upload_var.get())
            return
        if self.auto_upload_var.get():
            if len(self._effective_tags()) > 1:
                self._run_bulk_upload()
                return
            self._run_upload_only()
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
