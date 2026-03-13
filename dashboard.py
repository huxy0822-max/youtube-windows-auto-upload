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
from tkinter import messagebox

from path_helpers import default_scheduler_config, normalize_scheduler_config
from utils import get_all_tags, get_tag_info


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

SCRIPT_DIR = Path(__file__).parent
SCHEDULER_SCRIPT = SCRIPT_DIR / "daily_scheduler.py"
UPLOAD_SCRIPT = SCRIPT_DIR / "batch_upload.py"
LEGACY_RENDERER = SCRIPT_DIR / "app.py"
LEGACY_SCHEDULER = SCRIPT_DIR / "scheduler_gui.py"
SCHEDULER_CONFIG_FILE = SCRIPT_DIR / "scheduler_config.json"
DASHBOARD_STATE_FILE = SCRIPT_DIR / "dashboard_state.json"

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
        "song_count": "1",
        "channel": "",
        "audio_workers": 2,
        "video_workers": 2,
        "randomize_effects": True,
        "auto_upload": True,
        "auto_close_browser": True,
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


class CommandCenterApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("YouTube 自动化统一控制台")
        self.geometry("1180x860")
        self.minsize(1060, 760)

        self.scheduler_cfg = load_scheduler_config()
        self.ui_state = load_dashboard_state()
        self.process: subprocess.Popen | None = None
        self.log_thread: threading.Thread | None = None
        self.manual_widgets: List[ctk.CTkBaseClass] = []

        self._build_variables()
        self._build_ui()
        self._load_tags()
        self._sync_upload_channels()
        self._apply_randomize_state()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_variables(self) -> None:
        self.date_var = ctk.StringVar(value=self.ui_state["date"])
        self.tag_var = ctk.StringVar(value=self.ui_state["tag"])
        self.upload_tag_var = ctk.StringVar(value=self.ui_state["upload_tag"] or self.ui_state["tag"])
        self.song_count_var = ctk.StringVar(value=self.ui_state["song_count"])
        self.channel_var = ctk.StringVar(value=self.ui_state["channel"])
        self.audio_workers_var = ctk.IntVar(value=int(self.ui_state["audio_workers"]))
        self.video_workers_var = ctk.IntVar(value=int(self.ui_state["video_workers"]))
        self.randomize_effects_var = ctk.BooleanVar(value=bool(self.ui_state["randomize_effects"]))
        self.auto_upload_var = ctk.BooleanVar(value=bool(self.ui_state["auto_upload"]))
        self.auto_close_browser_var = ctk.BooleanVar(value=bool(self.ui_state["auto_close_browser"]))
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
        self.tabs.add("高级视觉")
        self.tabs.add("路径配置")
        self.tabs.add("日志")

        self._build_quick_tab(self.tabs.tab("快捷开始"))
        self._build_upload_tab(self.tabs.tab("上传"))
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
        ctk.CTkSwitch(row2, text="全随机视觉（推荐）", variable=self.randomize_effects_var, command=self._apply_randomize_state).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row2, text="渲染后自动上传", variable=self.auto_upload_var).pack(side="left", padx=(0, 16))
        ctk.CTkSwitch(row2, text="上传后自动关闭浏览器", variable=self.auto_close_browser_var).pack(side="left", padx=(0, 16))

        row3 = ctk.CTkFrame(parent, fg_color="transparent")
        row3.pack(fill="x", padx=10, pady=8)
        ctk.CTkLabel(row3, text="音频并行").pack(side="left")
        ctk.CTkSlider(row3, from_=1, to=8, variable=self.audio_workers_var, number_of_steps=7, width=180).pack(side="left", padx=8)
        ctk.CTkLabel(row3, textvariable=ctk.StringVar(value="")).pack_forget()
        ctk.CTkLabel(row3, text="视频并行").pack(side="left", padx=(16, 0))
        ctk.CTkSlider(row3, from_=1, to=8, variable=self.video_workers_var, number_of_steps=7, width=180).pack(side="left", padx=8)

        note = ctk.CTkLabel(
            parent,
            text="日常推荐：选标签和日期后，直接点“开始流水线”。如果只想先出片不上传，就点“仅渲染”。",
            text_color="#9aa0aa",
        )
        note.pack(anchor="w", padx=14, pady=(4, 12))

        actions = ctk.CTkFrame(parent, fg_color="transparent")
        actions.pack(fill="x", padx=10, pady=(0, 12))
        ctk.CTkButton(actions, text="开始流水线", height=42, command=lambda: self._run_scheduler(render_only=False)).pack(fill="x", pady=(0, 10))
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

    def _load_tags(self) -> None:
        tags = get_all_tags()
        values = ["全部标签", *tags] if tags else ["全部标签"]
        self.tag_menu.configure(values=values)
        self.upload_tag_menu.configure(values=tags or [""])
        if self.tag_var.get() not in values:
            self.tag_var.set(values[1] if len(values) > 1 else values[0])
        if self.upload_tag_var.get() not in (tags or [""]):
            self.upload_tag_var.set(self.tag_var.get() if self.tag_var.get() != "全部标签" else (tags[0] if tags else ""))
        self._sync_upload_channels()

    def _sync_tag_to_upload(self) -> None:
        if self.tag_var.get() and self.tag_var.get() != "全部标签":
            self.upload_tag_var.set(self.tag_var.get())
            self._sync_upload_channels()

    def _sync_upload_channels(self) -> None:
        tag = self.upload_tag_var.get().strip()
        info = get_tag_info(tag) if tag else None
        channels = [str(x) for x in info.get("all_serials", [])] if info else []
        self.channel_menu.configure(values=channels or [""])
        if self.channel_var.get() not in channels:
            self.channel_var.set(channels[0] if channels else "")

    def _apply_randomize_state(self) -> None:
        state = "disabled" if self.randomize_effects_var.get() else "normal"
        for widget in self.manual_widgets:
            widget.configure(state=state)

    def _build_scheduler_cmd(self, *, render_only: bool) -> list[str]:
        date_mmdd = normalize_mmdd(self.date_var.get())
        tag = self.tag_var.get().strip()
        args = [
            sys.executable,
            str(SCHEDULER_SCRIPT),
            "--standard",
            date_mmdd,
            f"--song-count={self.song_count_var.get().strip() or '1'}",
        ]
        if tag and tag != "全部标签":
            args.append(f"--tags={tag}")
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

    def _collect_state(self) -> dict:
        return {
            "date": self.date_var.get(),
            "tag": self.tag_var.get(),
            "upload_tag": self.upload_tag_var.get(),
            "song_count": self.song_count_var.get(),
            "channel": self.channel_var.get(),
            "audio_workers": int(self.audio_workers_var.get()),
            "video_workers": int(self.video_workers_var.get()),
            "randomize_effects": bool(self.randomize_effects_var.get()),
            "auto_upload": bool(self.auto_upload_var.get()),
            "auto_close_browser": bool(self.auto_close_browser_var.get()),
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
        }

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
        self.destroy()


def main():
    app = CommandCenterApp()
    app.mainloop()


if __name__ == "__main__":
    main()
