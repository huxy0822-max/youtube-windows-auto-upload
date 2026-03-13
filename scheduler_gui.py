#!/usr/bin/env python3
"""
调度器 GUI 壳层。

这个文件是 `daily_scheduler.py` 的图形化启动器，负责：
- 让用户通过窗口设置并行数、特效和运行参数。
- 保存 `scheduler_config.json`。
- 以跨平台方式拉起调度脚本。
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import subprocess
import sys
import json
import os
import platform as pf
import threading
import ssl
import urllib.request
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from path_helpers import default_scheduler_config, normalize_scheduler_config

IS_WINDOWS = pf.system() == "Windows"

# ============ 路径 ============
if getattr(sys, 'frozen', False):
    SCRIPT_DIR = Path(sys.executable).parent
else:
    SCRIPT_DIR = Path(__file__).parent
SCHEDULER_SCRIPT = SCRIPT_DIR / "daily_scheduler.py"
HISTORY_FILE = SCRIPT_DIR / "render_history.json"
GUI_CONFIG_FILE = SCRIPT_DIR / "scheduler_config.json"

def load_gui_config() -> dict:
    if GUI_CONFIG_FILE.exists():
        try:
            with open(GUI_CONFIG_FILE, 'r', encoding='utf-8') as f:
                return normalize_scheduler_config(json.load(f), SCRIPT_DIR)
        except:
            pass
    return default_scheduler_config(SCRIPT_DIR)

def save_gui_config(cfg: dict):
    with open(GUI_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

# ============ 中英文映射（界面显示中文，内部传递英文）============
COLOR_MAP = {
    "随机": "random",
    "白金色": "WhiteGold", "铂金色": "Platinum", "珍珠白": "Pearl",
    "象牙白": "Ivory", "银色": "Silver", "香槟色": "Champagne",
    "玫瑰金": "RoseGold", "鼠尾草绿": "SageGreen", "雾蓝色": "DustyBlue",
    "冷蓝色": "CoolBlue", "午夜蓝": "MidnightBlue", "酒红色": "Burgundy",
    "日落色": "Sunset"
}
COLOR_DISPLAY = list(COLOR_MAP.keys())
COLOR_REVERSE = {v: k for k, v in COLOR_MAP.items()}

ZOOM_MAP = {"慢": "slow", "中": "normal", "快": "fast", "关闭": "off"}
ZOOM_DISPLAY = list(ZOOM_MAP.keys())
ZOOM_REVERSE = {v: k for k, v in ZOOM_MAP.items()}

STYLE_MAP = {"柱状": "bar", "镜像柱状": "bar_mirror", "波浪": "wave", "环形": "circular", "随机": "random"}
STYLE_DISPLAY = list(STYLE_MAP.keys())
STYLE_REVERSE = {v: k for k, v in STYLE_MAP.items()}

TEXT_POS_MAP = {"左下角": "bottom_left", "底部居中": "bottom_center", "顶部居中": "top_center", "正中央": "center"}
TEXT_POS_DISPLAY = list(TEXT_POS_MAP.keys())
TEXT_POS_REVERSE = {v: k for k, v in TEXT_POS_MAP.items()}

LETTERBOX_MAP = {"关闭": "Off", "开启": "On", "随机": "Random"}
LETTERBOX_DISPLAY = list(LETTERBOX_MAP.keys())
LETTERBOX_REVERSE = {v: k for k, v in LETTERBOX_MAP.items()}

TEXT_STYLE_MAP = {"经典": "Classic", "发光": "Glow", "霓虹": "Neon", "粗体": "Bold", "方框": "Box"}
TEXT_STYLE_DISPLAY = list(TEXT_STYLE_MAP.keys())
TEXT_STYLE_REVERSE = {v: k for k, v in TEXT_STYLE_MAP.items()}

TINT_MAP = {"无": "none", "暖色": "warm", "冷色": "cool", "复古": "vintage", "深蓝夜晚": "blue_night", "金色": "golden", "森林绿调": "forest", "随机": "random"}
PARTICLE_MAP = {"无": "none", "飘雪": "snow", "浮尘/光斑": "dust_bokeh", "萤火虫": "fireflies", "雨丝": "rain", "随机": "random"}
FONT_MAP_GUI = {"系统默认": "default", "宋体": "songti", "黑体": "heiti", "手写": "handwrite", "楷书": "edu_kaishu", "教育宋体": "edu_songti"}


def load_history():
    """加载渲染历史"""
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return []


class SchedulerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("🎬 Daily Scheduler 配置面板")
        self.root.configure(bg="#1e1e2e")
        self.gui_cfg = load_gui_config()
        
        # 窗口居中 & 放大
        w, h = 1000, 920
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = (sw - w) // 2
        y = (sh - h) // 2
        self.root.geometry(f"{w}x{h}+{x}+{y}")
        self.root.resizable(True, True)
        
        # 主题色
        self.bg = "#1e1e2e"
        self.fg = "#cdd6f4"
        self.accent = "#89b4fa"
        self.green = "#a6e3a1"
        self.red = "#f38ba8"
        self.surface = "#313244"
        self.overlay = "#45475a"
        
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TScale", background=self.bg, troughcolor=self.surface)
        style.configure("TCombobox", fieldbackground=self.surface, background=self.surface)
        
        self._build_ui()
    
    def _build_ui(self):
        # ========== 按钮区 (固定底部) ==========
        btn_frame = tk.Frame(self.root, bg=self.bg)
        btn_frame.pack(side="bottom", fill="x", padx=20, pady=15)
        
        # 5秒预览按钮 & 状态标签
        self.preview_btn = tk.Button(btn_frame, text="👁️  5秒预览效果", command=self._preview,
                                font=("SF Pro Display", 14, "bold"),
                                bg="#a6e3a1", fg="#1e1e2e", activebackground="#94e2d5",
                                relief="flat", bd=0, padx=20, pady=8, cursor="hand2")
        self.preview_btn.pack(fill="x", pady=(0, 5))
        
        # 状态提示标签 (替代弹窗)
        self.status_label = tk.Label(btn_frame, text="准备就绪", bg=self.bg, fg="#6c7086",
                                     font=("SF Pro Text", 10))
        self.status_label.pack(anchor="c", pady=(0, 8))
        
        start_btn = tk.Button(btn_frame, text="▶️  开始渲染", command=self._start,
                              font=("SF Pro Display", 16, "bold"),
                              bg="#89b4fa", fg="#1e1e2e", activebackground="#74c7ec",
                              relief="flat", bd=0, padx=30, pady=10, cursor="hand2")
        start_btn.pack(fill="x")

        # ========== 滚动区域 (Main Content) ==========
        main_container = tk.Frame(self.root, bg=self.bg)
        main_container.pack(fill="both", expand=True)
        
        canvas = tk.Canvas(main_container, bg=self.bg, highlightthickness=0)
        scrollbar = ttk.Scrollbar(main_container, orient="vertical", command=canvas.yview)
        
        self.scroll_frame = tk.Frame(canvas, bg=self.bg)
        self.scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=self.scroll_frame, anchor="nw", width=980) # Width matches slightly less than window width
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 鼠标滚轮支持 (Mac/Win兼容)
        def _on_mousewheel(event):
            if sys.platform == "darwin":
                # Mac delta is usually small integers, scroll 'units' directly
                canvas.yview_scroll(int(-1 * event.delta), "units")
            else:
                # Windows/Linux delta is usually multiples of 120
                canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
                
        # Bind to canvas and all layout
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # Linux uses Button-4 and Button-5
        if sys.platform == "linux":
            canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
            canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # ========== 标题 ==========
        title_frame = tk.Frame(self.scroll_frame, bg=self.bg) # Use scroll_frame
        title_frame.pack(fill="x", padx=20, pady=(15, 5))
        
        tk.Label(title_frame, text="🎬 Daily Scheduler", font=("SF Pro Display", 22, "bold"),
                 bg=self.bg, fg=self.fg).pack(side="left")
        
        # ========== 文件夹配置 (隐藏，从 scheduler_config.json 自动读取) ==========
        # 变量仍然需要初始化，供预览和保存使用
        self.music_dir_var = tk.StringVar(value=self.gui_cfg.get("music_dir", ""))
        self.image_dir_var = tk.StringVar(value=self.gui_cfg.get("base_image_dir", ""))
        self.output_dir_var = tk.StringVar(value=self.gui_cfg.get("output_root", ""))
        self.ffmpeg_var = tk.StringVar(value=self.gui_cfg.get("ffmpeg_bin") or self.gui_cfg.get("ffmpeg_path") or self._detect_ffmpeg_path())
        self.ffmpeg_status = tk.Label(self.scroll_frame, text="", bg=self.bg)  # 隐藏的占位
        
        # 日期也自动填入今天，不显示
        self.date_var = tk.StringVar(value=datetime.now().strftime("%m%d"))
        
        # ========== 并行数 ==========
        para_frame = self._section("⚡ 并行数")
        
        # 音频并行
        af = tk.Frame(para_frame, bg=self.bg)
        af.pack(fill="x", pady=2)
        tk.Label(af, text="🎵 音频合成:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 13), width=12, anchor="w").pack(side="left")
        self.audio_workers_var = tk.IntVar(value=2)
        audio_scale = tk.Scale(af, from_=1, to=16, orient="horizontal",
                               variable=self.audio_workers_var, length=250,
                               bg=self.bg, fg=self.accent, troughcolor=self.surface,
                               highlightthickness=0, font=("SF Mono", 11))
        audio_scale.pack(side="left", padx=10)
        
        # 视频并行
        vf = tk.Frame(para_frame, bg=self.bg)
        vf.pack(fill="x", pady=2)
        tk.Label(vf, text="🎬 视频渲染:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 13), width=12, anchor="w").pack(side="left")
        self.video_workers_var = tk.IntVar(value=2)
        video_scale = tk.Scale(vf, from_=1, to=16, orient="horizontal",
                               variable=self.video_workers_var, length=250,
                               bg=self.bg, fg=self.accent, troughcolor=self.surface,
                               highlightthickness=0, font=("SF Mono", 11))
        video_scale.pack(side="left", padx=10)
        
        # 歌曲数量
        sf = tk.Frame(para_frame, bg=self.bg)
        sf.pack(fill="x", pady=2)
        tk.Label(sf, text="🎶 歌曲数量:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 13), width=12, anchor="w").pack(side="left")
        self.song_count_var = tk.IntVar(value=20)
        song_scale = tk.Scale(sf, from_=3, to=50, orient="horizontal",
                              variable=self.song_count_var, length=250,
                              bg=self.bg, fg=self.accent, troughcolor=self.surface,
                              highlightthickness=0, font=("SF Mono", 11))
        song_scale.pack(side="left", padx=10)
        tk.Label(sf, text="首/母带", bg=self.bg, fg="#6c7086",
                 font=("SF Pro Text", 11)).pack(side="left")
        
        # ========== 特效开关 ==========
        fx_frame = self._section("🎨 视觉特效")
        
        # Row 1: 开关
        checks_row = tk.Frame(fx_frame, bg=self.bg)
        checks_row.pack(fill="x", pady=5)
        
        self.spectrum_var = tk.BooleanVar(value=True)
        self.timeline_var = tk.BooleanVar(value=True)
        
        # Checkbuttons for Spectrum and Timeline
        for var, label, emoji in [
            (self.spectrum_var, "频谱动画", "📊"),
            (self.timeline_var, "时间轴", "⏱️"),
        ]:
            cb = tk.Checkbutton(checks_row, text=f"{emoji} {label}", variable=var,
                                bg=self.bg, fg=self.fg, selectcolor=self.surface,
                                activebackground=self.bg, activeforeground=self.green,
                                font=("SF Pro Text", 13))
            cb.pack(side="left", padx=(0, 20))
            
        # Letterbox Combobox
        tk.Label(checks_row, text="🖼️ 黑边:", bg=self.bg, fg=self.fg, font=("SF Pro Text", 13)).pack(side="left", padx=(0,5))
        self.letterbox_var = tk.StringVar(value="随机")
        ttk.Combobox(checks_row, textvariable=self.letterbox_var, values=LETTERBOX_DISPLAY, 
                     width=8, state="readonly").pack(side="left")
        
        # Row 2: 频谱配色 + 时间轴配色
        color_row = tk.Frame(fx_frame, bg=self.bg)
        color_row.pack(fill="x", pady=4)
        
        tk.Label(color_row, text="🎵 频谱色:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 13)).pack(side="left", padx=(0, 5))
        self.color_spectrum_var = tk.StringVar(value="随机")
        ttk.Combobox(color_row, textvariable=self.color_spectrum_var,
                     values=COLOR_DISPLAY, width=12, state="readonly").pack(side="left", padx=(0, 15))
        
        # Visuals Frame for timeline color, style, zoom, and spectrum position
        viz_frame = tk.Frame(fx_frame, bg=self.bg)
        viz_frame.pack(fill="x", pady=4)

        tk.Label(viz_frame, text="⏳ 进度色:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 13)).grid(row=0, column=2, sticky="e", padx=5)
        self.color_timeline_var = tk.StringVar(value="随机")
        ttk.Combobox(viz_frame, textvariable=self.color_timeline_var,
                     values=COLOR_DISPLAY, width=12, state="readonly").grid(row=0, column=3, sticky="w", padx=5)
        
        # Row 2: 样式 + 缩放
        self.style_var = tk.StringVar(value="柱状")
        self.zoom_var = tk.StringVar(value="慢")
        
        tk.Label(viz_frame, text="🎨 样式:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 13)).grid(row=1, column=0, sticky="e", padx=5, pady=5)
        style_cb = ttk.Combobox(viz_frame, textvariable=self.style_var, values=STYLE_DISPLAY, width=10, state="readonly")
        style_cb.grid(row=1, column=1, sticky="w", padx=5)
        
        tk.Label(viz_frame, text="🔍 缩放:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 13)).grid(row=1, column=2, sticky="e", padx=5)
        ttk.Combobox(viz_frame, textvariable=self.zoom_var, values=ZOOM_DISPLAY, width=8, state="readonly").grid(row=1, column=3, sticky="w", padx=5)
        
        # 频谱位置控制 (拖动条)
        pos_frame = tk.LabelFrame(viz_frame, text="频谱位置调整", bg=self.bg, fg=self.fg, bd=1,
                                  font=("SF Pro Text", 12), relief="flat", padx=5, pady=5)
        pos_frame.grid(row=2, column=0, columnspan=4, sticky="ew", padx=5, pady=10)
        
        # X轴 (左右) -1表示自动居中
        tk.Label(pos_frame, text="↔️ 左右:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 12)).grid(row=0, column=0, padx=5, sticky="w")
        self.spectrum_x_var = tk.IntVar(value=-1)
        self.scale_x = tk.Scale(pos_frame, variable=self.spectrum_x_var, from_=-1, to=1500, orient="horizontal", 
                                bg=self.bg, fg=self.accent, troughcolor=self.surface, highlightthickness=0, length=200,
                                font=("SF Mono", 10), label="(-1=居中)")
        self.scale_x.grid(row=0, column=1, sticky="ew", padx=5)
        
        # Y轴 (上下)
        tk.Label(pos_frame, text="↕️ 上下:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 12)).grid(row=0, column=2, padx=5, sticky="w")
        self.spectrum_y_var = tk.IntVar(value=530)
        # Allow -1 for Random
        self.scale_y = tk.Scale(pos_frame, variable=self.spectrum_y_var, from_=-1, to=900, orient="horizontal",
                                bg=self.bg, fg=self.accent, troughcolor=self.surface, highlightthickness=0, length=200,
                                font=("SF Mono", 10), label="(0-900, -1=随机)")
        self.scale_y.grid(row=0, column=3, sticky="ew", padx=5)
        
        # 宽度 (Width)
        tk.Label(pos_frame, text="📏 宽度:", bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 12)).grid(row=1, column=0, padx=5, sticky="w", pady=5)
        self.spectrum_w_var = tk.IntVar(value=800)
        self.scale_w = tk.Scale(pos_frame, variable=self.spectrum_w_var, from_=-1, to=1920, orient="horizontal",
                                bg=self.bg, fg=self.accent, troughcolor=self.surface, highlightthickness=0, length=200,
                                font=("SF Mono", 10), label="(0-1920, -1=随机)")
        self.scale_w.grid(row=1, column=1, sticky="ew", padx=5, pady=5)
        
        # ============ 文字叠加 ============
        text_frame = self._section("✏️ 文字叠加")
        
        # 第一行：文字输入 + 位置
        text_row1 = tk.Frame(text_frame, bg=self.bg)
        text_row1.pack(fill="x", pady=5)
        
        tk.Label(text_row1, text="文字:", bg=self.bg, fg=self.fg, font=("SF Pro Text", 13)).pack(side="left", padx=5)
        self.text_var = tk.StringVar(value="")
        tk.Entry(text_row1, textvariable=self.text_var, width=30, 
                 font=("SF Pro Text", 12), bg=self.surface, fg=self.fg, insertbackground=self.fg).pack(side="left", padx=5)
        
        tk.Label(text_row1, text="位置:", bg=self.bg, fg=self.fg, font=("SF Pro Text", 13)).pack(side="left", padx=10)
        self.text_pos_var = tk.StringVar(value="正中央")
        ttk.Combobox(text_row1, textvariable=self.text_pos_var, 
                     values=TEXT_POS_DISPLAY, width=10, state="readonly").pack(side="left")

        # 第二行：文字大小 + 样式
        text_row2 = tk.Frame(text_frame, bg=self.bg)
        text_row2.pack(fill="x", pady=2)
        
        tk.Label(text_row2, text="大小:", bg=self.bg, fg=self.fg, font=("SF Pro Text", 13)).pack(side="left", padx=5)
        self.text_size_var = tk.IntVar(value=60)
        tk.Scale(text_row2, variable=self.text_size_var, from_=20, to=200, orient="horizontal",
                 bg=self.bg, fg=self.accent, troughcolor=self.surface, highlightthickness=0, length=120,
                 font=("SF Mono", 10)).pack(side="left", padx=5)

        tk.Label(text_row2, text="风格:", bg=self.bg, fg=self.fg, font=("SF Pro Text", 13)).pack(side="left", padx=5)
        self.text_style_var = tk.StringVar(value="经典")
        ttk.Combobox(text_row2, textvariable=self.text_style_var, values=TEXT_STYLE_DISPLAY, 
                     width=8, state="readonly").pack(side="left", padx=5)
        
        tk.Label(text_frame, text="💡 留空=不显示频道名。支持中文", 
                 bg=self.bg, fg="#6c7086", font=("SF Pro Text", 10)).pack(anchor="w", padx=10, pady=(5,0))

        # ============ 画面增强 ============
        enh_frame = self._section("🎛️ 画面增强")
        
        # 噪点
        row_grain = tk.Frame(enh_frame, bg=self.bg)
        row_grain.pack(fill="x", pady=2)
        self.film_grain_var = tk.BooleanVar(value=False)
        tk.Checkbutton(row_grain, text="荧幕噪点", variable=self.film_grain_var, bg=self.bg, fg=self.fg, selectcolor=self.surface).pack(side="left")
        tk.Label(row_grain, text="强度:", bg=self.bg, fg=self.fg).pack(side="left", padx=(10, 2))
        self.grain_strength_var = tk.IntVar(value=50)
        tk.Scale(row_grain, variable=self.grain_strength_var, from_=10, to=100, orient="horizontal", bg=self.bg, fg=self.accent, troughcolor=self.surface, highlightthickness=0, length=100).pack(side="left")
        
        # 暗角 & 柔焦
        row_vig = tk.Frame(enh_frame, bg=self.bg)
        row_vig.pack(fill="x", pady=2)
        self.vignette_var = tk.BooleanVar(value=False)
        tk.Checkbutton(row_vig, text="暗角效果", variable=self.vignette_var, bg=self.bg, fg=self.fg, selectcolor=self.surface).pack(side="left")
        
        self.soft_focus_var = tk.BooleanVar(value=False)
        tk.Checkbutton(row_vig, text="轻微模糊", variable=self.soft_focus_var, bg=self.bg, fg=self.fg, selectcolor=self.surface).pack(side="left", padx=(10, 0))
        tk.Label(row_vig, text="强度:", bg=self.bg, fg=self.fg).pack(side="left", padx=(10, 2))
        self.soft_focus_sigma_var = tk.DoubleVar(value=1.5)
        tk.Scale(row_vig, variable=self.soft_focus_sigma_var, from_=0.5, to=5.0, resolution=0.1, orient="horizontal", bg=self.bg, fg=self.accent, troughcolor=self.surface, highlightthickness=0, length=100).pack(side="left")
        
        # 色调 & 粒子
        row_color = tk.Frame(enh_frame, bg=self.bg)
        row_color.pack(fill="x", pady=2)
        tk.Label(row_color, text="色调:", bg=self.bg, fg=self.fg).pack(side="left")
        
        self.color_tint_var = tk.StringVar(value="无")
        ttk.Combobox(row_color, textvariable=self.color_tint_var, values=list(TINT_MAP.keys()), width=8, state="readonly").pack(side="left", padx=5)
        
        tk.Label(row_color, text="粒子:", bg=self.bg, fg=self.fg).pack(side="left", padx=(10, 2))
        self.particle_var = tk.StringVar(value="无")
        ttk.Combobox(row_color, textvariable=self.particle_var, values=list(PARTICLE_MAP.keys()), width=10, state="readonly").pack(side="left", padx=5)
        
        tk.Label(row_color, text="不透明度:", bg=self.bg, fg=self.fg).pack(side="left", padx=(10, 2))
        self.particle_opacity_var = tk.DoubleVar(value=0.6)
        tk.Scale(row_color, variable=self.particle_opacity_var, from_=0.1, to=1.0, resolution=0.1, orient="horizontal", bg=self.bg, fg=self.accent, troughcolor=self.surface, highlightthickness=0, length=80).pack(side="left")
        
        # 字体
        row_font = tk.Frame(enh_frame, bg=self.bg)
        row_font.pack(fill="x", pady=2)
        tk.Label(row_font, text="字体:", bg=self.bg, fg=self.fg).pack(side="left")
        self.text_font_var = tk.StringVar(value="系统默认")
        ttk.Combobox(row_font, textvariable=self.text_font_var, values=list(FONT_MAP_GUI.keys()), width=10, state="readonly").pack(side="left", padx=5)

        # ============ 选项 & 按钮 ============
        opt_frame = self._section("⚙️ 选项")
        self.dryrun_var = tk.BooleanVar(value=False) # Renamed from dryrun_var to dry_run_var in diff, but keeping original name for consistency with other parts of the code.
        tk.Checkbutton(opt_frame, text="🧪 Dry Run (只扫描，不渲染)", variable=self.dryrun_var,
                       bg=self.bg, fg=self.fg, selectcolor=self.surface,
                       activebackground=self.bg, activeforeground=self.green,
                       font=("SF Pro Text", 13)).pack(anchor="w")
        
        self.auto_upload_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opt_frame, text="📤 渲染后自动上传 (Pipeline 模式)", variable=self.auto_upload_var,
                       bg=self.bg, fg=self.fg, selectcolor=self.surface,
                       activebackground=self.bg, activeforeground=self.green,
                       font=("SF Pro Text", 13)).pack(anchor="w", pady=(5, 0))
        
        # ========== 历史记录 ==========
        hist_frame = self._section("📊 渲染历史 (最近 5 次)")
        
        self.history_text = tk.Text(hist_frame, height=8, bg=self.surface, fg=self.fg,
                                     font=("SF Mono", 11), relief="flat", bd=5,
                                     insertbackground=self.fg)
        self.history_text.pack(fill="x")
        self._refresh_history()
    
    def _folder_row(self, parent, label, var, config_key):
        """创建文件夹选择行"""
        row = tk.Frame(parent, bg=self.bg)
        row.pack(fill="x", pady=2)
        tk.Label(row, text=label, bg=self.bg, fg=self.fg,
                 font=("SF Pro Text", 12), width=10, anchor="w").pack(side="left")
        tk.Entry(row, textvariable=var, width=35,
                 font=("SF Pro Text", 11), bg=self.surface, fg=self.fg,
                 insertbackground=self.fg, relief="flat", bd=3).pack(side="left", padx=5)
        tk.Button(row, text="选择", command=lambda: self._browse_folder(var, config_key),
                  bg=self.surface, fg=self.fg, relief="flat", font=("SF Pro Text", 10),
                  padx=8, cursor="hand2").pack(side="left", padx=2)
    
    def _browse_folder(self, var, config_key):
        folder = filedialog.askdirectory(
            title="选择文件夹",
            initialdir=var.get() or str(Path.home() / "Downloads")
        )
        if folder:
            var.set(folder)
            self.gui_cfg[config_key] = folder
            save_gui_config(self.gui_cfg)
    
    def _detect_ffmpeg_path(self):
        """自动检测 FFmpeg"""
        for name in ["ffmpeg", "ffmpeg.exe"]:
            for d in [SCRIPT_DIR / "tools" / "ffmpeg" / "bin", SCRIPT_DIR / "tools", SCRIPT_DIR]:
                p = d / name
                if p.exists():
                    return str(p)
        # 检查 PATH
        try:
            result = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=3)
            if result.returncode == 0:
                return "ffmpeg"
        except:
            pass
        return ""
    
    def _detect_ffmpeg_btn(self):
        path = self._detect_ffmpeg_path()
        if path:
            self.ffmpeg_var.set(path)
            self.ffmpeg_status.config(text="✅ 已检测到", fg=self.green)
            self.gui_cfg["ffmpeg_bin"] = path
            self.gui_cfg["ffmpeg_path"] = path  # 兼容两个 key
            save_gui_config(self.gui_cfg)
        else:
            self.ffmpeg_status.config(text="❌ 未找到", fg=self.red)
    
    def _download_ffmpeg(self):
        """自动下载 FFmpeg"""
        if pf.system() == "Darwin":
            messagebox.showinfo("macOS 用户", "请在终端运行: brew install ffmpeg")
            return
        
        self.ffmpeg_status.config(text="📥 下载中...", fg=self.accent)
        self.root.update()
        
        def worker():
            try:
                tools_dir = SCRIPT_DIR / "tools"
                tools_dir.mkdir(parents=True, exist_ok=True)
                zip_path = tools_dir / "ffmpeg.zip"
                url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
                
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                with urllib.request.urlopen(url, context=ctx) as resp, open(zip_path, 'wb') as f:
                    shutil.copyfileobj(resp, f)
                
                ffmpeg_dir = tools_dir / "ffmpeg"
                ffmpeg_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(str(zip_path), 'r') as zf:
                    zf.extractall(str(tools_dir / "ffmpeg_temp"))
                
                temp_dir = tools_dir / "ffmpeg_temp"
                for item in temp_dir.iterdir():
                    if item.is_dir():
                        for sub in item.iterdir():
                            dest = ffmpeg_dir / sub.name
                            if sub.is_dir():
                                if dest.exists(): shutil.rmtree(dest)
                                shutil.copytree(sub, dest)
                            else:
                                shutil.copy2(sub, dest)
                        break
                
                shutil.rmtree(temp_dir, ignore_errors=True)
                zip_path.unlink(missing_ok=True)
                
                self.root.after(0, self._detect_ffmpeg_btn)
                self.root.after(0, lambda: self.ffmpeg_status.config(text="✅ 下载完成!", fg=self.green))
            except Exception as e:
                self.root.after(0, lambda: self.ffmpeg_status.config(text=f"❌ 失败: {str(e)[:30]}", fg=self.red))
        
        threading.Thread(target=worker, daemon=True).start()
    
    def _section(self, title):
        """创建一个带标题的区块"""
        container = tk.Frame(self.scroll_frame, bg=self.bg)
        container.pack(fill="x", padx=20, pady=(10, 0))
        
        tk.Label(container, text=title, font=("SF Pro Display", 14, "bold"),
                 bg=self.bg, fg=self.accent).pack(anchor="w", pady=(0, 5))
        
        inner = tk.Frame(container, bg=self.bg)
        inner.pack(fill="x", padx=10)
        return inner
    
    def _refresh_history(self):
        """刷新历史记录显示"""
        self.history_text.config(state="normal")
        self.history_text.delete("1.0", "end")
        
        history = load_history()
        if not history:
            self.history_text.insert("end", "  暂无渲染记录\n  运行一次后这里会显示耗时对比")
        else:
            # 表头
            header = f"{'日期':<12} {'标签数':>5} {'视频数':>5} {'并行':>4} {'总耗时':>10}\n"
            self.history_text.insert("end", header)
            self.history_text.insert("end", "─" * 48 + "\n")
            
            for entry in history[-5:]:
                dt = entry.get("datetime", "")[:16]
                tags = entry.get("tag_count", "?")
                vids = entry.get("video_count", "?")
                workers = entry.get("video_workers", "?")
                elapsed = entry.get("total_minutes", 0)
                
                line = f"{dt:<12} {tags:>5} {vids:>5} {workers:>4} {elapsed:>8.1f}min\n"
                self.history_text.insert("end", line)
        
        self.history_text.config(state="disabled")
    
    def _preview(self):
        """生成5秒预览视频 (内置，不依赖外部脚本)"""
        import time as _time
        import random
        
        # 1. 获取参数 (中文 → 英文)
        params = {
            "spectrum": self.spectrum_var.get(),
            "timeline": self.timeline_var.get(),
            "letterbox": {"关闭": False, "开启": True, "随机": "random"}.get(self.letterbox_var.get(), False),
            "zoom": ZOOM_MAP.get(self.zoom_var.get(), "normal"),
            "color_spectrum": COLOR_MAP.get(self.color_spectrum_var.get(), "WhiteGold"),
            "color_timeline": COLOR_MAP.get(self.color_timeline_var.get(), "WhiteGold"),
            "spectrum_y": int(self.spectrum_y_var.get()),
            "spectrum_x": int(self.spectrum_x_var.get()),
            "spectrum_w": int(self.spectrum_w_var.get()),
            "style": STYLE_MAP.get(self.style_var.get(), "bar"),
            "text": self.text_var.get().strip(),
            "text_pos": TEXT_POS_MAP.get(self.text_pos_var.get(), "center"),
            "text_size": int(self.text_size_var.get()),
            "text_style": TEXT_STYLE_MAP.get(self.text_style_var.get(), "Classic"),
            "film_grain": getattr(self, 'film_grain_var', tk.BooleanVar(value=False)).get(),
            "grain_strength": getattr(self, 'grain_strength_var', tk.IntVar(value=15)).get(),
            "vignette": getattr(self, 'vignette_var', tk.BooleanVar(value=False)).get(),
            "soft_focus": getattr(self, 'soft_focus_var', tk.BooleanVar(value=False)).get(),
            "soft_focus_sigma": getattr(self, 'soft_focus_sigma_var', tk.DoubleVar(value=1.5)).get(),
            "color_tint": TINT_MAP.get(getattr(self, 'color_tint_var', tk.StringVar(value="无")).get(), "none"),
            "particle": PARTICLE_MAP.get(getattr(self, 'particle_var', tk.StringVar(value="无")).get(), "none"),
            "particle_opacity": getattr(self, 'particle_opacity_var', tk.DoubleVar(value=0.6)).get(),
            "text_font": FONT_MAP_GUI.get(getattr(self, 'text_font_var', tk.StringVar(value="系统默认")).get(), "default")
        }
        
        # 2. 检查 FFmpeg
        ffmpeg_bin = self.ffmpeg_var.get() or self._detect_ffmpeg_path()
        if not ffmpeg_bin:
            messagebox.showerror("缺少 FFmpeg", "请先检测或下载 FFmpeg")
            return
        
        # 3. 更新UI
        self.preview_btn.config(state="disabled", text="⏳ 生成中...", bg="#45475a")
        self.status_label.config(text="正在调用 FFmpeg 生成预览，请稍候...", fg=self.accent)
        self.root.update()
        
        def preview_worker():
            try:
                # 4. 找素材 — 先用 GUI 选的文件夹，再 fallback 到默认位置
                img_dir = self.image_dir_var.get()
                music_dir = self.music_dir_var.get()
                home = Path.home()
                
                # 找图片 (排除封面图 cover)
                img_candidates = []
                search_dirs = []
                if img_dir and Path(img_dir).exists():
                    search_dirs.append(Path(img_dir))
                search_dirs.extend([
                    home / "Downloads" / "base image",
                    home / "Downloads" / "base_image",
                ])
                for d in search_dirs:
                    if d.exists():
                        for ext in ["png", "jpg", "jpeg", "webp"]:
                            for f in d.rglob(f"*.{ext}"):
                                # 排除封面图 (与 daily_scheduler 的 find_images_by_date 保持一致)
                                if 'cover' not in f.stem.lower():
                                    img_candidates.append(f)
                        if img_candidates:
                            break
                
                # 找音乐
                audio_candidates = []
                search_dirs = []
                if music_dir and Path(music_dir).exists():
                    search_dirs.append(Path(music_dir))
                search_dirs.extend([
                    home / "Downloads" / "Suno Downloads",
                    home / "Downloads" / "suno downloads",
                ])
                for d in search_dirs:
                    if d.exists():
                        for ext in ["mp3", "wav", "m4a"]:
                            audio_candidates.extend(d.rglob(f"*.{ext}"))
                        if audio_candidates:
                            break
                
                if not img_candidates:
                    self._preview_done(False, "未找到图片文件，请先选择底图目录")
                    return
                if not audio_candidates:
                    self._preview_done(False, "未找到音频文件，请先选择音乐目录")
                    return
                
                # 随机选取（或最新的）
                img_file = str(random.choice(img_candidates))
                audio_file = str(random.choice(audio_candidates))
                
                # 5. 生成效果
                try:
                    # PyInstaller 打包时 effects_library 在 _MEIPASS 目录
                    if getattr(sys, 'frozen', False):
                        sys.path.insert(0, str(Path(sys._MEIPASS)))
                    else:
                        sys.path.insert(0, str(SCRIPT_DIR))
                    import effects_library
                    import importlib
                    importlib.reload(effects_library)  # 确保使用最新代码
                    preview_dur = 5.0
                    
                    # 调试: 打印文字参数
                    print(f"[预览] 文字=\"{params.get('text', '')}\", 粒子={params.get('particle', 'none')}")
                    
                    filter_str, desc, extra_inputs = effects_library.get_effect(
                        preview_dur, **params
                    )
                    
                    # 调试: 检查 drawtext 是否在 filter 中
                    has_drawtext = "drawtext" in filter_str
                    print(f"[预览] filter 包含 drawtext: {has_drawtext}")
                    if has_drawtext:
                        for part in filter_str.split(";"):
                            if "drawtext" in part:
                                print(f"[预览] drawtext: {part[:150]}...")
                except Exception as e:
                    self._preview_done(False, f"效果生成失败: {e}")
                    return
                
                # 6. FFmpeg 渲染
                # 预览文件放在项目内 _previews/ 目录，方便管理
                preview_dir = SCRIPT_DIR / "_previews"
                preview_dir.mkdir(exist_ok=True)
                
                # 自动清理: 删除超过 24 小时的旧预览
                import time as _time_module
                now_ts = _time_module.time()
                for old_file in preview_dir.glob("preview_*.mp4"):
                    try:
                        age_hours = (now_ts - old_file.stat().st_mtime) / 3600
                        if age_hours > 24:
                            old_file.unlink()
                    except:
                        pass
                
                output_path = str(preview_dir / f"preview_{int(_time.time())}.mp4")
                
                cmd = [
                    ffmpeg_bin, "-y", "-v", "error",
                    "-loop", "1", "-i", img_file,
                    "-i", audio_file,
                ]
                if extra_inputs:
                    cmd.extend(extra_inputs)
                cmd.extend([
                    "-filter_complex", filter_str,
                    "-map", "[outv]", "-map", "1:a",
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
                    "-c:a", "aac", "-b:a", "128k",
                    "-t", str(preview_dur),
                    "-shortest",
                    output_path
                ])
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                
                if result.returncode != 0:
                    err = result.stderr[-200:] if result.stderr else "未知错误"
                    self._preview_done(False, f"FFmpeg 失败: {err}")
                    return
                
                # 7. 自动打开播放
                if pf.system() == "Darwin":
                    subprocess.run(["open", output_path])
                elif pf.system() == "Windows":
                    os.startfile(output_path)
                else:
                    subprocess.run(["xdg-open", output_path])
                
                self._preview_done(True, "✅ 预览已生成 (视频窗口已弹出)")
                
            except subprocess.TimeoutExpired:
                self._preview_done(False, "生成超时 (超过60秒)")
            except Exception as e:
                self._preview_done(False, str(e))
        
        threading.Thread(target=preview_worker, daemon=True).start()
    
    def _preview_done(self, success, msg):
        """预览结束的回调 (必须在主线程更新 UI)"""
        def update_ui():
            self.preview_btn.config(state="normal", text="👁️  5秒预览效果", bg="#a6e3a1")
            
            if success:
                self.status_label.config(text=msg, fg=self.green)
            else:
                self.status_label.config(text=f"❌ {msg}", fg="#f38ba8")
                messagebox.showerror("预览出错", msg) # 只有出错才弹窗
        
        self.root.after(0, update_ui)
    
    def _build_args(self):
        """构建命令参数"""
        date = self.date_var.get().strip()
        if not date or not date.isdigit() or len(date) != 4:
            messagebox.showerror("错误", "日期格式不正确，请输入 MMDD (如 0218)")
            return None
        
        args = [date]
        
        if not self.spectrum_var.get():
            args.append("--no-spectrum")
        if not self.timeline_var.get():
            args.append("--no-timeline")
            
        lb_val = LETTERBOX_MAP.get(self.letterbox_var.get(), "Off")
        if lb_val == "On":
            args.append("--letterbox=true")
        elif lb_val == "Random":
            args.append("--letterbox=random")
        
        args.append(f"--zoom={ZOOM_MAP.get(self.zoom_var.get(), 'normal')}")
        args.append(f"--color-spectrum={COLOR_MAP.get(self.color_spectrum_var.get(), 'WhiteGold')}")
        args.append(f"--color-timeline={COLOR_MAP.get(self.color_timeline_var.get(), 'WhiteGold')}")
        args.append(f"--style={STYLE_MAP.get(self.style_var.get(), 'bar')}")
        args.append(f"--spectrum-y={self.spectrum_y_var.get()}")
        args.append(f"--spectrum-x={self.spectrum_x_var.get()}")
        args.append(f"--spectrum-w={self.spectrum_w_var.get()}")
        
        if hasattr(self, 'film_grain_var') and self.film_grain_var.get():
            args.append(f"--film-grain={self.grain_strength_var.get()}")
        if hasattr(self, 'vignette_var') and self.vignette_var.get():
            args.append("--vignette")
        if hasattr(self, 'soft_focus_var') and self.soft_focus_var.get():
            args.append(f"--soft-focus={self.soft_focus_sigma_var.get()}")
            
        if hasattr(self, 'color_tint_var'):
            ct = TINT_MAP.get(self.color_tint_var.get(), "none")
            if ct != "none": args.append(f"--color-tint={ct}")
            
        if hasattr(self, 'particle_var'):
            pt = PARTICLE_MAP.get(self.particle_var.get(), "none")
            if pt != "none":
                args.append(f"--particle={pt}")
                args.append(f"--particle-opacity={self.particle_opacity_var.get()}")
        
        text_val = self.text_var.get().strip()
        if text_val:
            args.append(f"--text={text_val}")
            args.append(f"--text-pos={TEXT_POS_MAP.get(self.text_pos_var.get(), 'center')}")
            args.append(f"--text-size={self.text_size_var.get()}")
            args.append(f"--text-style={TEXT_STYLE_MAP.get(self.text_style_var.get(), 'Classic')}")
                
        if hasattr(self, 'text_font_var'):
            tf = FONT_MAP_GUI.get(self.text_font_var.get(), "default")
            if tf != "default":
                args.append(f"--text-font={tf}")
        
        if self.dryrun_var.get():
            args.append("--dry-run")
        if not self.auto_upload_var.get():
            args.append("--render-only")
        
        args.append(f"--song-count={self.song_count_var.get()}")
        return args

    def _save_current_config(self):
        """保存当前界面配置"""
        self.gui_cfg["music_dir"] = self.music_dir_var.get()
        self.gui_cfg["base_image_dir"] = self.image_dir_var.get()
        self.gui_cfg["output_root"] = self.output_dir_var.get()
        ffmpeg = self.ffmpeg_var.get()
        if ffmpeg:
            self.gui_cfg["ffmpeg_bin"] = ffmpeg
            self.gui_cfg["ffmpeg_path"] = ffmpeg
        save_gui_config(self.gui_cfg)

    def _launch_process(self, args):
        """跨平台启动渲染进程"""
        import tempfile
        IS_FROZEN = getattr(sys, 'frozen', False)
        
        if pf.system() == "Darwin":
            import shlex
            cmd = [sys.executable, str(SCHEDULER_SCRIPT)] + args
            cmd_str = " ".join(shlex.quote(c) for c in cmd)
            print(f"\n🚀 启动命令: {cmd_str}")
            
            launch_script = Path(tempfile.gettempdir()) / "scheduler_launch.sh"
            with open(launch_script, "w") as f:
                f.write("#!/bin/bash\n")
                f.write(f"cd {shlex.quote(str(SCRIPT_DIR))}\n")
                f.write(f"export AUDIO_WORKERS={self.audio_workers_var.get()}\n")
                f.write(f"export VIDEO_WORKERS={self.video_workers_var.get()}\n")
                f.write(f"{cmd_str}\n")
                f.write("echo ''\n")
                f.write("echo '按任意键关闭窗口...'\n")
                f.write("read -n 1\n")
            os.chmod(launch_script, 0o755)
            
            try:
                subprocess.Popen(["open", "-a", "Terminal", str(launch_script)])
                self.root.after(3000, self._refresh_history)
            except Exception as e:
                messagebox.showerror("启动失败", str(e))
        else:
            if IS_FROZEN:
                scheduler_exe = SCRIPT_DIR / "DailyScheduler.exe"
                if scheduler_exe.exists():
                    cmd = [str(scheduler_exe)] + args
                else:
                    messagebox.showerror("缺少文件", f"找不到 DailyScheduler.exe\n\n请确保 DailyScheduler.exe 和 VideoRenderer.exe\n放在同一个文件夹下\n\n查找路径: {scheduler_exe}")
                    return
            else:
                cmd = [sys.executable, str(SCHEDULER_SCRIPT)] + args
            
            cmd_str = " ".join(f'"{c}"' if " " in c else c for c in cmd)
            print(f"\n🚀 启动命令: {cmd_str}")
            
            launch_script = Path(tempfile.gettempdir()) / "scheduler_launch.bat"
            with open(launch_script, "w", encoding="utf-8") as f:
                f.write("@echo off\n")
                f.write("chcp 65001 >nul\n")
                f.write(f'cd /d "{SCRIPT_DIR}"\n')
                f.write(f"set AUDIO_WORKERS={self.audio_workers_var.get()}\n")
                f.write(f"set VIDEO_WORKERS={self.video_workers_var.get()}\n")
                f.write(f"{cmd_str}\n")
                f.write("echo.\n")
                f.write("echo 按任意键关闭窗口...\n")
                f.write("pause >nul\n")
            
            try:
                subprocess.Popen(["cmd", "/c", "start", "", str(launch_script)])
                self.root.after(3000, self._refresh_history)
            except Exception as e:
                messagebox.showerror("启动失败", str(e))

    def _start(self):
        """构建命令并启动"""
        args = self._build_args()
        if args is None:
            return
            
        self._save_current_config()
        self._launch_process(args)

def main():
    root = tk.Tk()
    app = SchedulerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
