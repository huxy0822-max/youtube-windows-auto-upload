#!/usr/bin/env python3
"""
主界面入口。

这个文件负责启动桌面版“视频渲染工作站”GUI，主要做三件事：
1. 读取/保存本地渲染配置。
2. 调用 `render_engine.py` 做资源扫描、预览和正式渲染。
3. 提供给非命令行用户一个可视化操作面板。
"""

import os
import sys
import json
import platform
import threading
import subprocess
import urllib.request
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from tkinter import filedialog, messagebox
import tkinter as tk
import ssl
from PIL import Image, ImageTk # Requires Pillow (comes with customtkinter usually)
import time

# ============ 依赖检查 ============
try:
    import customtkinter as ctk
except ImportError:
    print("正在安装界面依赖 customtkinter...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "customtkinter"])
    import customtkinter as ctk

# ============ 路径设置 ============
if getattr(sys, "frozen", False):
    APP_BASE_DIR = Path(sys.executable).parent
else:
    APP_BASE_DIR = Path(__file__).parent

# 导入渲染引擎
sys.path.insert(0, str(APP_BASE_DIR))
from render_engine import (
    detect_ffmpeg,
    detect_best_codec,
    scan_resources,
    run_full_pipeline,
    render_video, # Added for preview
    DEFAULT_TARGET_DURATION,
    DEFAULT_MASTER_COUNT,
    DEFAULT_AUDIO_WORKERS,
    DEFAULT_VIDEO_WORKERS,
)
from effects_library import PALETTES, ZOOM_SPEEDS, get_effect # Added get_effect

# ============ 常量 ============
APP_NAME = "🎬 YouTube 视频渲染工作站"
APP_VERSION = "2.0.0 (Stable)"
CONFIG_FILE = APP_BASE_DIR / "config.json"

# CustomTkinter 主题
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


# ============ 配置管理 ============
def load_app_config() -> dict:
    """加载用户配置"""
    default = {
        "music_dir": "",
        "image_dir": "",
        "output_dir": "",
        "ffmpeg_path": "",
        "target_duration": DEFAULT_TARGET_DURATION,
        "master_count": DEFAULT_MASTER_COUNT,
        "audio_workers": DEFAULT_AUDIO_WORKERS,
        "video_workers": DEFAULT_VIDEO_WORKERS,
        "first_run": True,
        # 特效配置
        "fx_spectrum": 1,
        "fx_timeline": 1,
        "fx_letterbox": 0,
        "fx_zoom": "normal",
        "fx_color_spectrum": "WhiteGold",
        "fx_color_timeline": "WhiteGold",
        "fx_spectrum_y": 530,
        "cpu_mode": 0
    }
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            default.update(saved)
        except:
            pass
    return default


def save_app_config(cfg: dict):
    """保存用户配置"""
    cfg["first_run"] = False
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def parse_color_hex(ffmpeg_color_str):
    """解析 FFmpeg 颜色字符串为 Hex (优化版: 更接近实际渲染效果)"""
    # 特殊映射: 让预览色更接近视频里的发光效果
    # 注意: FFmpeg 里的 WhiteGold 频谱其实是 white|#FFD700 (白芯金边)
    # 这里我们返回一个元组 (main_color, glow_color)
    if not ffmpeg_color_str: return ("#FFFFFF", "#FFFFFF")

    # 1. 处理 "color|color" 格式
    if "|" in ffmpeg_color_str:
        parts = ffmpeg_color_str.split("|")
        c_main = parts[0] # 内芯
        c_glow = parts[1] # 外发光
    else:
        c_main = ffmpeg_color_str
        c_glow = ffmpeg_color_str # 单色

    # 2. 清理透明度 @
    if "@" in c_main: c_main = c_main.split("@")[0]
    if "@" in c_glow: c_glow = c_glow.split("@")[0]
    
    # 3. 颜色名映射 (标准 HTML 颜色)
    COLOR_MAP = {
        "white": "#FFFFFF", "black": "#000000", "gray": "#808080",
        "red": "#FF0000", "blue": "#0000FF", "green": "#008000",
        "gold": "#FFD700", "orange": "#FFA500", "purple": "#800080"
    }

    def to_hex(c):
        if c.lower() in COLOR_MAP: return COLOR_MAP[c.lower()]
        if c.startswith("&H"): return "#FFFFFF" # ASS format fallback
        if c.startswith("#"): return c
        return "#FFFFFF" # default

    return (to_hex(c_main), to_hex(c_glow))


# ============ 主应用 ============
class VideoRendererApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        self.config = load_app_config()
        self.rendering = False
        self.preview_image_path = None # 当前预览用的底图路径
        
        # 窗口设置
        self.title(f"{APP_NAME} v{APP_VERSION}")
        self.geometry("900x720")
        self.minsize(800, 600)
        
        # 自动检测 FFmpeg
        if not self.config["ffmpeg_path"]:
            detected = detect_ffmpeg()
            if detected:
                self.config["ffmpeg_path"] = detected
        
        # 构建界面
        self._build_ui()
        
        # 首次运行提示
        if self.config["first_run"]:
            self.after(500, self.show_welcome)
    
    def _build_ui(self):
        """构建主界面"""
        
        # ===== 顶部标题 =====
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.pack(fill="x", padx=20, pady=(15, 5))
        
        ctk.CTkLabel(
            header, text="🎬 YouTube 视频渲染工作站 (稳定版)",
            font=ctk.CTkFont(size=22, weight="bold")
        ).pack(side="left")
        
        ctk.CTkLabel(
            header, text=f"v{APP_VERSION} | {platform.system()}",
            font=ctk.CTkFont(size=12), text_color="gray"
        ).pack(side="right")
        
        # ===== 配置区域 =====
        config_frame = ctk.CTkFrame(self)
        config_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(
            config_frame, text="📁 文件夹配置",
            font=ctk.CTkFont(size=16, weight="bold")
        ).pack(anchor="w", padx=15, pady=(10, 5))
        
        # --- 音乐目录 ---
        self.music_var = ctk.StringVar(value=self.config.get("music_dir", ""))
        self._make_folder_row(config_frame, "🎵 音乐目录:", self.music_var, "music_dir",
                              "子文件夹名 = 标签名 (如 大提琴/、钢琴/)")
        
        # --- 底图目录 ---
        self.image_var = ctk.StringVar(value=self.config.get("image_dir", ""))
        self._make_folder_row(config_frame, "🖼️ 底图目录:", self.image_var, "image_dir",
                              "子文件夹名与音乐一致，图片命名: {日期}_{序号}.png")
        
        # --- 输出目录 ---
        self.output_var = ctk.StringVar(value=self.config.get("output_dir", ""))
        self._make_folder_row(config_frame, "📂 输出目录:", self.output_var, "output_dir",
                              "渲染完成的视频存放位置")
        
        # --- FFmpeg 路径 ---
        self.ffmpeg_var = ctk.StringVar(value=self.config.get("ffmpeg_path", ""))
        ffmpeg_row = ctk.CTkFrame(config_frame, fg_color="transparent")
        ffmpeg_row.pack(fill="x", padx=15, pady=2)
        ctk.CTkLabel(ffmpeg_row, text="⚙️ FFmpeg:", width=100, anchor="w").pack(side="left")
        ctk.CTkEntry(ffmpeg_row, textvariable=self.ffmpeg_var, width=400).pack(side="left", padx=5)
        ctk.CTkButton(ffmpeg_row, text="检测", width=60, command=self._detect_ffmpeg).pack(side="left", padx=2)
        
        self.ffmpeg_dl_btn = ctk.CTkButton(
            ffmpeg_row, text="自动下载", width=80,
            fg_color="#e67e22", hover_color="#d35400",
            command=self._download_ffmpeg
        )
        self.ffmpeg_dl_btn.pack(side="left", padx=2)
        
        # FFmpeg 状态
        self.ffmpeg_status = ctk.CTkLabel(ffmpeg_row, text="", font=ctk.CTkFont(size=11))
        self.ffmpeg_status.pack(side="left", padx=5)
        
        # 留一点底部间距
        ctk.CTkFrame(config_frame, height=8, fg_color="transparent").pack()
        
        # ===== 渲染参数区 =====
        params_frame = ctk.CTkFrame(self)
        params_frame.pack(fill="x", padx=20, pady=5)
        
        ctk.CTkLabel(
            params_frame, text="⚙️ 渲染参数",
            font=ctk.CTkFont(size=16, weight="bold")
        ).pack(anchor="w", padx=15, pady=(10, 5))
        
        params_inner = ctk.CTkFrame(params_frame, fg_color="transparent")
        params_inner.pack(fill="x", padx=15, pady=(0, 10))
        
        # 第一行参数
        row1 = ctk.CTkFrame(params_inner, fg_color="transparent")
        row1.pack(fill="x", pady=2)
        
        # 日期
        ctk.CTkLabel(row1, text="📅 日期:", anchor="w").pack(side="left")
        self.date_var = ctk.StringVar(value=datetime.now().strftime("%m%d"))
        ctk.CTkEntry(row1, textvariable=self.date_var, width=80).pack(side="left", padx=(5, 20))
        
        # 时长 - 改为直接输入文本框，但增加按钮方便设置
        ctk.CTkLabel(row1, text="⏱️ 时长(分钟):", anchor="w").pack(side="left")
        self.duration_var = ctk.StringVar(value=str(self.config["target_duration"] // 60))
        ctk.CTkEntry(row1, textvariable=self.duration_var, width=60).pack(side="left", padx=(5, 5))
        
        # 测试按钮
        ctk.CTkButton(row1, text="1分钟测试", width=80, fg_color="#6c757d", 
                      command=lambda: self.duration_var.set("1")).pack(side="left", padx=5)

        # 母带数
        ctk.CTkLabel(row1, text="🎵 母带数:", anchor="w").pack(side="left", padx=(15, 0))
        self.master_var = ctk.StringVar(value=str(self.config["master_count"]))
        ctk.CTkEntry(row1, textvariable=self.master_var, width=50).pack(side="left", padx=(5, 20))
        
        # 第二行参数
        row2 = ctk.CTkFrame(params_inner, fg_color="transparent")
        row2.pack(fill="x", pady=2)
        
        ctk.CTkLabel(row2, text="🔧 音频并行:", anchor="w").pack(side="left")
        self.aw_var = ctk.StringVar(value=str(self.config["audio_workers"]))
        ctk.CTkEntry(row2, textvariable=self.aw_var, width=50).pack(side="left", padx=(5, 20))
        
        ctk.CTkLabel(row2, text="🔧 视频并行:", anchor="w").pack(side="left")
        self.vw_var = ctk.StringVar(value=str(self.config["video_workers"]))
        ctk.CTkEntry(row2, textvariable=self.vw_var, width=50).pack(side="left", padx=(5, 20))
        
        # ===== 特效配置区 (Split into Left: Controls, Right: Preview) =====
        fx_main = ctk.CTkFrame(self)
        fx_main.pack(fill="x", padx=20, pady=5)
        
        ctk.CTkLabel(
            fx_main, text="🎨 特效设置与排版预览", font=ctk.CTkFont(size=16, weight="bold")
        ).pack(anchor="w", padx=15, pady=(10, 5))
        
        # Split into Left (Controls) and Right (Canvas)
        content_frame = ctk.CTkFrame(fx_main, fg_color="transparent")
        content_frame.pack(fill="both", expand=True, padx=15, pady=(0, 10))
        
        # --- Left: Controls ---
        left_panel = ctk.CTkFrame(content_frame, fg_color="transparent")
        left_panel.pack(side="left", fill="y", expand=True)

        # Row 1: Checkboxes & Zoom
        row1 = ctk.CTkFrame(left_panel, fg_color="transparent")
        row1.pack(fill="x", pady=5)
        
        self.fx_spectrum = ctk.IntVar(value=self.config.get("fx_spectrum", 1))
        ctk.CTkCheckBox(row1, text="频谱", variable=self.fx_spectrum).pack(side="left", padx=5)
        
        self.fx_timeline = ctk.IntVar(value=self.config.get("fx_timeline", 1))
        ctk.CTkCheckBox(row1, text="时间轴", variable=self.fx_timeline).pack(side="left", padx=5)
        
        self.fx_letterbox = ctk.IntVar(value=self.config.get("fx_letterbox", 0))
        ctk.CTkCheckBox(row1, text="黑边", variable=self.fx_letterbox, width=60).pack(side="left", padx=5)
        
        ctk.CTkLabel(row1, text="缩放:").pack(side="left", padx=(10, 2))
        self.fx_zoom = ctk.StringVar(value=self.config.get("fx_zoom", "normal"))
        ctk.CTkOptionMenu(row1, values=list(ZOOM_SPEEDS.keys()), variable=self.fx_zoom, width=80).pack(side="left")

        # Style Selector
        ctk.CTkLabel(row1, text="样式:").pack(side="left", padx=(10, 2))
        self.fx_style = ctk.StringVar(value=self.config.get("fx_style", "bar"))
        ctk.CTkOptionMenu(row1, values=["bar", "wave", "circular"], variable=self.fx_style, width=80).pack(side="left")

        # Row 2: Colors & Position
        row2 = ctk.CTkFrame(left_panel, fg_color="transparent")
        row2.pack(fill="x", pady=5)
        
        colors = ["WhiteGold", "CoolBlue", "RoseGold", "Champagne"] + [k for k in PALETTES.keys() if k not in ["WhiteGold", "CoolBlue", "RoseGold", "Champagne"]] + ["random"]
        
        # Spectrum Color
        ctk.CTkLabel(row2, text="🎵 频谱色:").grid(row=0, column=0, padx=5, pady=2, sticky="e")
        self.fx_color_s = ctk.StringVar(value=self.config.get("fx_color_spectrum", "WhiteGold"))
        ctk.CTkOptionMenu(row2, values=colors, variable=self.fx_color_s, width=110).grid(row=0, column=1, padx=2, pady=2)

        # Timeline Color
        ctk.CTkLabel(row2, text="⏳ 进度色:").grid(row=1, column=0, padx=5, pady=2, sticky="e")
        self.fx_color_t = ctk.StringVar(value=self.config.get("fx_color_timeline", "WhiteGold"))
        ctk.CTkOptionMenu(row2, values=colors, variable=self.fx_color_t, width=110).grid(row=1, column=1, padx=2, pady=2)
        
        # Y Position
        ctk.CTkLabel(row2, text="↕️ 频谱Y:").grid(row=2, column=0, padx=5, pady=2, sticky="e")
        self.fx_spec_y = ctk.StringVar(value=str(self.config.get("fx_spectrum_y", 530)))
        ent = ctk.CTkEntry(row2, textvariable=self.fx_spec_y, width=60)
        ent.grid(row=2, column=1, padx=2, pady=2, sticky="w")
        # ent.bind("<KeyRelease>", lambda e: self._update_preview()) # Live update removed

        # CPU Mode
        self.cpu_mode = ctk.IntVar(value=self.config.get("cpu_mode", 0))
        ctk.CTkCheckBox(left_panel, text="强制 CPU (兼容模式)", variable=self.cpu_mode).pack(anchor="w", padx=10, pady=10)

        # --- Right: Preview Controls (Replaced Canvas) ---
        right_panel = ctk.CTkFrame(content_frame, fg_color="#2b2b2b", corner_radius=5) 
        right_panel.pack(side="right", padx=10, pady=5, fill="both", expand=True)
        
        ctk.CTkLabel(right_panel, text="由于静态模拟不准确，建议使用\n5秒实机渲染来确认效果。",
                     text_color="gray", font=("Arial", 12)).pack(pady=(20, 10))
        
        # 5s Preview Button
        self.preview_btn = ctk.CTkButton(
            right_panel, text="🎬 生成 5秒 预览视频", height=40, 
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#6f42c1", hover_color="#5a32a3",
            command=self._generate_preview_video
        )
        self.preview_btn.pack(pady=10)

        # Remove init call to _update_preview since canvas is gone
        # self.after(100, self._update_preview)

    def _generate_preview_video(self):
        """生成5秒真机预览"""
        if self.rendering:
            messagebox.showwarning("忙碌", "正在渲染任务中...")
            return
            
        # 1. Check data
        music_dir = self.music_var.get()
        image_dir = self.image_var.get()
        date_str = self.date_var.get().strip()
        ffmpeg_path = self.ffmpeg_var.get()
        
        if not ffmpeg_path:
            messagebox.showerror("错误", "请先设置 FFmpeg 路径")
            return
            
        # 2. Rescan briefly to find ONE valid pair
        # We don't want to rely on previous scan if dir changed.
        # But full scan is slow. Let's try to find just the first valid project.
        if not music_dir or not image_dir:
            messagebox.showerror("错误", "请设置音乐和图片目录")
            return
            
        self.preview_btn.configure(state="disabled", text="正在生成...")
        self._log("🎥 正在生成 5秒 预览视频...")
        
        def preview_worker():
            try:
                self.after(0, self._log, "🔍 正在扫描第一组资源...")
                projects = scan_resources(music_dir, image_dir, date_str)
                valid_proj = None
                for p in projects:
                    if p["images"] and p["music"]:
                        valid_proj = p
                        break
                
                if not valid_proj:
                    self.after(0, lambda: messagebox.showwarning("资源不足", "未找到成对的 图片+音乐，无法生成预览"))
                    return

                self.after(0, self._log, f"✅ 选中资源: {valid_proj['tag']}")
                
                # Prepare paths
                img_path = Path(valid_proj["images"][0])
                audio_path = Path(valid_proj["music"][0])
                
                # Settings
                dur = 5 # 5 seconds
                
                # Filter
                try:
                    style_val = self.fx_style.get()
                except:
                    style_val = "bar"
                    
                fx_filter, _, extra_inputs = get_effect(
                    dur,
                    spectrum=bool(self.fx_spectrum.get()),
                    timeline=bool(self.fx_timeline.get()),
                    letterbox=bool(self.fx_letterbox.get()),
                    zoom=self.fx_zoom.get(),
                    color_spectrum=self.fx_color_s.get(),
                    color_timeline=self.fx_color_t.get(),
                    spectrum_y=int(self.fx_spec_y.get()),
                    style=style_val
                )
                
                # Codec
                self.after(0, self._log, "⚙️ 正在检测编码器...")
                if self.cpu_mode.get():
                     # Force CPU preset ultrafast
                    codec = ["-c:v", "libx264", "-preset", "ultrafast", "-crf", "28"] 
                else:
                    # 如果之前已经检测过，直接用之前的配置
                    # 这里为了安全，我们用一个比较通用的 GPU 配置或者重新快速检测
                    # detect_best_codec 比较快，但为了防卡死，我们包裹一下
                    try:
                        codec = detect_best_codec(ffmpeg_path)
                        # 为了预览速度，我们可以强制把 preset 改快一点 (如果是 h264_nvenc)
                        new_codec = []
                        for i, arg in enumerate(codec):
                            if arg == "-preset":
                                new_codec.append("-preset")
                                new_codec.append("p1") # fast preset for nvenc
                                # skip next arg in loop? loop logic is simple copy so...
                                # tricky to modify list in place reliably without assuming structure.
                                # Let's just use what detect_best_codec returned, usually it's fine.
                            else:
                                if i > 0 and codec[i-1] == "-preset": continue
                                new_codec.append(arg)
                        # actually rely on detect logic.
                    except Exception as e:
                        print(f"Codec detect failed: {e}")
                        codec = ["-c:v", "libx264", "-preset", "ultrafast", "-crf", "28"]

                # Output file
                preview_file = APP_BASE_DIR / "preview_temp.mp4"
                if preview_file.exists():
                    try:
                        os.remove(preview_file)
                    except:
                        pass # might be open
                    
                # Render
                self.after(0, self._log, "🚀 开始 FFmpeg 渲染 (5秒)...")
                
                res = render_video(
                    img_path, audio_path, preview_file,
                    fx_filter, codec, dur, ffmpeg_path, extra_inputs=extra_inputs
                )
                
                if res["success"]:
                    self.after(0, self._log, f"✅ 预览成功! 正在打开播放...")
                    # Open it
                    try:
                        if platform.system() == "Windows":
                            os.startfile(preview_file)
                        elif platform.system() == "Darwin":
                            subprocess.call(["open", preview_file])
                        else:
                            subprocess.call(["xdg-open", preview_file])
                    except Exception as e:
                         self.after(0, self._log, f"⚠️ 无法自动播放: {e}")
                else:
                     self.after(0, self._log, f"❌ 预览渲染失败: {res.get('error')}")
                     self.after(0, lambda: messagebox.showerror("预览失败", res.get('error')))
                     
            except Exception as e:
                self.after(0, self._log, f"❌ 预览线程异常: {e}")
                import traceback
                traceback.print_exc()

            finally:
                self.after(0, lambda: self.preview_btn.configure(state="normal", text="🎬 生成 5秒 预览视频"))

        threading.Thread(target=preview_worker, daemon=True).start()

        # ===== 操作按钮区 =====
        btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        btn_frame.pack(fill="x", padx=20, pady=5)
        
        self.scan_btn = ctk.CTkButton(
            btn_frame, text="🔍 扫描资源", width=140, height=40,
            font=ctk.CTkFont(size=14), command=self._scan_resources
        )
        self.scan_btn.pack(side="left", padx=5)
        
        self.render_btn = ctk.CTkButton(
            btn_frame, text="🚀 开始渲染", width=180, height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#28a745", hover_color="#218838",
            command=self._start_render
        )
        self.render_btn.pack(side="left", padx=5)
        
        self.stop_btn = ctk.CTkButton(
            btn_frame, text="⏹️ 停止", width=100, height=40,
            fg_color="#dc3545", hover_color="#c82333",
            command=self._stop_render, state="disabled"
        )
        self.stop_btn.pack(side="left", padx=5)
        
        ctk.CTkButton(
            btn_frame, text="📂 打开输出", width=120, height=40,
            fg_color="#6c757d", hover_color="#5a6268",
            command=self._open_output
        ).pack(side="right", padx=5)
        
        # ===== 进度条 =====
        self.progress = ctk.CTkProgressBar(self, width=860)
        self.progress.pack(padx=20, pady=(5, 0))
        self.progress.set(0)
        
        self.progress_label = ctk.CTkLabel(self, text="就绪", font=ctk.CTkFont(size=12))
        self.progress_label.pack(padx=20, pady=(0, 5))
        
        # ===== 日志区 =====
        log_frame = ctk.CTkFrame(self)
        log_frame.pack(fill="both", expand=True, padx=20, pady=(0, 15))
        
        ctk.CTkLabel(
            log_frame, text="📋 运行日志",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=(8, 2))
        
        self.log_text = ctk.CTkTextbox(log_frame, font=ctk.CTkFont(size=12, family="Courier"))
        self.log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # 初始日志
        self._log("🎬 YouTube 视频渲染工作站已启动 (稳定版 v2.0)")
        self._log(f"📍 系统: {platform.system()} {platform.machine()}")
        if self.ffmpeg_var.get():
            self._log(f"✅ FFmpeg: {self.ffmpeg_var.get()}")
            self._check_codec()
        else:
            self._log("⚠️ 未检测到 FFmpeg，请手动指定路径或安装 FFmpeg")
    
    def _make_folder_row(self, parent, label, var, config_key, hint=""):
        """创建文件夹选择行"""
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", padx=15, pady=2)
        
        ctk.CTkLabel(row, text=label, width=100, anchor="w").pack(side="left")
        entry = ctk.CTkEntry(row, textvariable=var, width=400, placeholder_text=hint)
        entry.pack(side="left", padx=5)
        ctk.CTkButton(
            row, text="选择", width=60,
            command=lambda: self._browse_folder(var, config_key)
        ).pack(side="left", padx=2)
        
        if hint:
            ctk.CTkLabel(
                row, text=hint, font=ctk.CTkFont(size=10), text_color="gray"
            ).pack(side="left", padx=10)
    
    def _browse_folder(self, var, config_key):
        """打开文件夹选择对话框"""
        folder = filedialog.askdirectory(title="选择文件夹")
        if folder:
            var.set(folder)
            self.config[config_key] = folder
            save_app_config(self.config)
            self._log(f"📁 已设置 {config_key}: {folder}")
    
    def _detect_ffmpeg(self):
        """检测 FFmpeg"""
        detected = detect_ffmpeg()
        if detected:
            self.ffmpeg_var.set(detected)
            self.config["ffmpeg_path"] = detected
            save_app_config(self.config)
            self.ffmpeg_status.configure(text="✅ 已检测到", text_color="green")
            self._log(f"✅ FFmpeg 检测成功: {detected}")
            self._check_codec()
        else:
            self.ffmpeg_status.configure(text="❌ 未找到", text_color="red")
            self._log("❌ 未找到 FFmpeg")
            self._log("   👆 点击 [自动下载] 按钮可以一键安装 FFmpeg")
            self._log("   或手动安装: Windows → gyan.dev/ffmpeg | macOS → brew install ffmpeg")
    
    def _download_ffmpeg(self):
        """自动下载 FFmpeg 到本地 tools/ 目录"""
        system = platform.system()
        tools_dir = APP_BASE_DIR / "tools"
        ffmpeg_dir = tools_dir / "ffmpeg"
        
        if system == "Windows":
            url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
            ext = "ffmpeg.exe"
        elif system == "Darwin":
            # macOS: 建议用 brew
            answer = messagebox.askyesno(
                "macOS 用户",
                "macOS 推荐使用 Homebrew 安装 FFmpeg:\n\n"
                "在终端运行: brew install ffmpeg\n\n"
                "确定要尝试自动下载吗？"
            )
            if not answer:
                return
            url = "https://evermeet.cx/ffmpeg/getrelease/zip"
            ext = "ffmpeg"
        else:
            messagebox.showinfo("提示", "Linux 请运行: sudo apt install ffmpeg")
            return
        
        self.ffmpeg_dl_btn.configure(state="disabled", text="下载中...")
        self._log(f"📥 开始下载 FFmpeg... (可能需要几分钟)")
        
        def download_worker():
            try:
                tools_dir.mkdir(parents=True, exist_ok=True)
                zip_path = tools_dir / "ffmpeg_download.zip"
                
                # 下载
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE

                with urllib.request.urlopen(url, context=ctx) as response, open(zip_path, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                
                self.after(0, self._log, "📦 下载完成，正在解压...")
                
                # 解压
                ffmpeg_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(str(zip_path), 'r') as zf:
                    zf.extractall(str(tools_dir / "ffmpeg_temp"))
                
                # 移动文件 (GitHub版解压后有子目录)
                temp_dir = tools_dir / "ffmpeg_temp"
                for item in temp_dir.iterdir():
                    if item.is_dir():
                        # 移动子目录内容到 ffmpeg_dir
                        for sub in item.iterdir():
                            dest = ffmpeg_dir / sub.name
                            if sub.is_dir():
                                if dest.exists():
                                    shutil.rmtree(dest)
                                shutil.copytree(sub, dest)
                            else:
                                shutil.copy2(sub, dest)
                        break
                    elif item.name.startswith("ffmpeg"):
                        # macOS: 直接是 ffmpeg 可执行文件
                        bin_dir = ffmpeg_dir / "bin"
                        bin_dir.mkdir(exist_ok=True)
                        shutil.copy2(item, bin_dir / item.name)
                        os.chmod(str(bin_dir / item.name), 0o755)
                
                # 清理
                shutil.rmtree(temp_dir, ignore_errors=True)
                zip_path.unlink(missing_ok=True)
                
                # 检测
                self.after(0, self._detect_ffmpeg)
                self.after(0, self._log, "✅ FFmpeg 安装成功！")
                
            except Exception as e:
                self.after(0, self._log, f"❌ FFmpeg 下载失败: {e}")
                self.after(0, self._log, "   请手动下载: https://www.gyan.dev/ffmpeg/builds/")
            finally:
                self.after(0, lambda: self.ffmpeg_dl_btn.configure(state="normal", text="自动下载"))
        
        threading.Thread(target=download_worker, daemon=True).start()
    
    def _check_codec(self):
        """检测编码器"""
        ffmpeg = self.ffmpeg_var.get()
        if ffmpeg:
            self.after(100, lambda: self._log("⏳ 正在自动检测显卡兼容性..."))
            
            def check_in_bg():
                codec = detect_best_codec(ffmpeg)
                codec_name = codec[1] if len(codec) > 1 else "libx264 (CPU)"
                # 回到主线程更新 UI
                self.after(0, self._log, f"🎬 最佳编码器: {codec_name}")
                if "libx264" in str(codec):
                    self.after(0, self._log, "   (未检测到可用显卡加速，将使用 CPU 渲染，稳定性最高)")
                else:
                    self.after(0, self._log, "   (检测到可用显卡加速，将尝试使用 GPU 提升速度)")

            threading.Thread(target=check_in_bg, daemon=True).start()
    
    def _scan_resources(self):
        """扫描资源"""
        music_dir = self.music_var.get()
        image_dir = self.image_var.get()
        date_str = self.date_var.get().strip()
        
        if not music_dir:
            messagebox.showwarning("提示", "请先设置音乐目录")
            return
        if not image_dir:
            messagebox.showwarning("提示", "请先设置底图目录")
            return
        if not date_str or len(date_str) != 4:
            messagebox.showwarning("提示", "日期格式应为 4 位数字 (如 0215)")
            return
        
        self._log(f"\n🔍 扫描资源 (日期: {date_str})...")
        
        projects = scan_resources(music_dir, image_dir, date_str)
        
        if not projects:
            self._log("❌ 未找到任何资源。请检查文件夹匹配：")
            self._log(f"   音乐: .../{music_dir}/[标签名]")
            self._log(f"   图片: .../{image_dir}/[标签名] (必须与音乐文件夹同名)")
            return
        
        total_images = 0
        total_music = 0
        
        # Reset preview image
        self.preview_image_path = None
        
        for proj in projects:
            tag = proj["tag"]
            img_count = len(proj["images"])
            music_count = len(proj["music"])
            total_images += img_count
            total_music += music_count
            
            # Pick the first available image as preview
            if img_count > 0 and self.preview_image_path is None:
                self.preview_image_path = proj["images"][0]
            
            if img_count > 0 and music_count > 0:
                containers = [str(c) for c in proj["containers"]]
                self._log(f"  ✅ {tag}: {img_count} 张底图, {music_count} 首音乐")
            elif img_count > 0:
                self._log(f"  ⚠️ {tag}: {img_count} 张底图, 但无音乐")
            else:
                pass # 忽略完全不匹配的
        
        if self.preview_image_path:
            self._log(f"🖼️ 已加载预览图: {Path(self.preview_image_path).name}")
            self._update_preview()
            
        self._log(f"\n📊 总计: {total_images} 张底图, {total_music} 首音乐")
    
    def _start_render(self):
        """开始渲染 (在子线程中执行)"""
        # 验证输入
        music_dir = self.music_var.get()
        image_dir = self.image_var.get()
        output_dir = self.output_var.get()
        ffmpeg = self.ffmpeg_var.get()
        date_str = self.date_var.get().strip()
        
        errors = []
        if not music_dir or not Path(music_dir).exists():
            errors.append("音乐目录无效")
        if not image_dir or not Path(image_dir).exists():
            errors.append("底图目录无效")
        if not output_dir:
            errors.append("请设置输出目录")
        if not ffmpeg:
            errors.append("请先检测或设置 FFmpeg 路径")
        if not date_str or len(date_str) != 4:
            errors.append("日期格式应为 4 位数字 (如 0215)")
        
        if errors:
            messagebox.showerror("配置错误", "\n".join(errors))
            return
        
        # 保存配置
        self.config["music_dir"] = music_dir
        self.config["image_dir"] = image_dir
        self.config["output_dir"] = output_dir
        self.config["ffmpeg_path"] = ffmpeg
        
        try:
            self.config["target_duration"] = int(float(self.duration_var.get()) * 60)
            self.config["master_count"] = int(self.master_var.get())
            self.config["audio_workers"] = int(self.aw_var.get())
            self.config["video_workers"] = int(self.vw_var.get())
            
            # 保存特效配置
            self.config["fx_spectrum"] = self.fx_spectrum.get()
            self.config["fx_timeline"] = self.fx_timeline.get()
            self.config["fx_letterbox"] = self.fx_letterbox.get()
            self.config["fx_zoom"] = self.fx_zoom.get()
            self.config["fx_color_spectrum"] = self.fx_color_s.get()
            self.config["fx_color_timeline"] = self.fx_color_t.get()
            self.config["fx_spectrum_y"] = int(self.fx_spec_y.get())
            self.config["fx_style"] = self.fx_style.get()
            self.config["cpu_mode"] = self.cpu_mode.get()
        except ValueError:
            messagebox.showerror("参数错误", "数字参数输入有误 (如 Y 坐标、时长)")
            return
        
        save_app_config(self.config)
        
        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # UI 状态
        self.rendering = True
        self.render_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.scan_btn.configure(state="disabled")
        self.progress.set(0)
        self.progress_label.configure(text="正在渲染...")
        
        self._log(f"\n{'🚀' * 20}")
        if self.config["cpu_mode"]:
            self._log(f"  开始渲染流水线 (强制 CPU 兼容模式)")
        else:
            self._log(f"  开始渲染流水线 (GPU 优先模式)")
        self._log(f"{'🚀' * 20}\n")
        
        # 子线程执行渲染
        thread = threading.Thread(target=self._render_worker, daemon=True)
        thread.start()
    
    def _render_worker(self):
        """渲染工作线程"""
        try:
            result = run_full_pipeline(
                music_dir=self.config["music_dir"],
                image_dir=self.config["image_dir"],
                output_dir=self.config["output_dir"],
                date_str=self.date_var.get().strip(),
                target_duration=self.config["target_duration"],
                master_count=self.config["master_count"],
                audio_workers=self.config["audio_workers"],
                video_workers=self.config["video_workers"],
                ffmpeg_path=self.config["ffmpeg_path"],
                log_callback=lambda msg: self.after(0, self._log, msg),
                progress_callback=lambda cur, total, msg: self.after(
                    0, self._update_progress, cur, total, msg
                ),
                # 传递特效参数
                fx_spectrum=bool(self.config["fx_spectrum"]),
                fx_timeline=bool(self.config["fx_timeline"]),
                fx_letterbox=bool(self.config["fx_letterbox"]),
                fx_zoom=self.config["fx_zoom"],
                # 传递新参数
                fx_color_spectrum=self.config.get("fx_color_spectrum", "WhiteGold"),
                fx_color_timeline=self.config.get("fx_color_timeline", "WhiteGold"),
                fx_spectrum_y=self.config.get("fx_spectrum_y", 530),
                fx_style=self.config.get("fx_style", "bar"),
                # 传递 CPU 模式
                cpu_mode=bool(self.config.get("cpu_mode", 0))
            )
            
            if result.get("success"):
                rendered = result.get("rendered", 0)
                self.after(0, self._log, f"\n🎉 渲染完成! 成功: {rendered} 个视频")
                self.after(0, self._update_progress, 1, 1, f"✅ 完成! 共 {rendered} 个视频")
            else:
                self.after(0, self._log, f"\n❌ 渲染失败: {result.get('error', '未知错误')}")
                self.after(0, self._update_progress, 0, 1, "❌ 失败")
        
        except Exception as e:
            self.after(0, self._log, f"\n❌ 渲染异常: {e}")
            import traceback
            self.after(0, self._log, traceback.format_exc())
        
        finally:
            self.after(0, self._render_finished)
    
    def _render_finished(self):
        """渲染完成，恢复 UI"""
        self.rendering = False
        self.render_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.scan_btn.configure(state="normal")
    
    def _stop_render(self):
        """停止渲染"""
        self.rendering = False
        self._log("⏹️ 正在停止... (当前任务会完成后停止)")
        self.stop_btn.configure(state="disabled")
    
    def _update_progress(self, current, total, msg=""):
        """更新进度条"""
        if total > 0:
            self.progress.set(current / total)
        self.progress_label.configure(text=msg)
    
    def _open_output(self):
        """打开输出目录"""
        out = self.output_var.get()
        if not out or not Path(out).exists():
            messagebox.showinfo("提示", "输出目录不存在，请先渲染视频")
            return
        
        if platform.system() == "Darwin":
            subprocess.call(["open", out])
        elif platform.system() == "Windows":
            os.startfile(out)
        else:
            subprocess.call(["xdg-open", out])
    
    def _log(self, msg):
        """添加日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert("end", f"[{timestamp}] {msg}\n")
        self.log_text.see("end")
    
    def show_welcome(self):
        """首次运行欢迎向导"""
        welcome = ctk.CTkToplevel(self)
        welcome.title("👋 欢迎使用")
        welcome.geometry("650x600")
        welcome.transient(self)
        welcome.grab_set()
        
        # 居中显示
        welcome.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() - 650) // 2
        y = self.winfo_y() + (self.winfo_height() - 600) // 2
        welcome.geometry(f"+{x}+{y}")
        
        # 内容
        ctk.CTkLabel(
            welcome, text="🎬 欢迎使用 YouTube 视频渲染工作站！",
            font=ctk.CTkFont(size=20, weight="bold")
        ).pack(pady=(20, 10))
        
        info_frame = ctk.CTkFrame(welcome)
        info_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        guide_text = """
🎬 视频渲染工作站使用指南:

1️⃣ 安装 FFmpeg (核心组件)
   • 点击主界面「自动检测」旁的 [自动下载] 按钮，它会自动帮您装好。
   • 装好后才能开始干活哦！

2️⃣ 准备素材 (划重点！)
   🎵 音乐目录: 
      D:\\MyMusic\\
         ├── 钢琴曲\\     ← (标签文件夹)
         │   └── song1.mp3
         └── 古典乐\\
             └── song2.mp3

   🖼️ 底图目录: (子文件夹名必须和音乐一样)
      D:\\MyImages\\
         ├── 钢琴曲\\     ← (一定要一样!)
         │   └── 0215_1.png
         └── 古典乐\\
             └── 0215_2.png

3️⃣ 开始渲染
   • 填入今天的日期 (如 0215)
   • 建议先用 [1分钟测试] 按钮试试水
   • 点击 [开始渲染]，如果不成功，日志里会有提示。

💡 常见问题:
   • 扫描不到图片？检查文件夹名字是不是写错了。
   • 渲染失败？无需担心，程序现在会自动尝试修复 (GPU -> CPU)。
"""
        
        textbox = ctk.CTkTextbox(info_frame, font=ctk.CTkFont(size=13))
        textbox.pack(fill="both", expand=True, padx=10, pady=10)
        textbox.insert("1.0", guide_text)
        textbox.configure(state="disabled")
        
        ctk.CTkButton(
            welcome, text="✅ 我知道了", width=200, height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=lambda: self._close_welcome(welcome)
        ).pack(pady=(5, 20))
    
    def _close_welcome(self, window):
        """关闭欢迎窗口并保存"""
        self.config["first_run"] = False
        save_app_config(self.config)
        window.destroy()


# ============ 入口 ============
def main():
    app = VideoRendererApp()
    app.mainloop()


if __name__ == "__main__":
    main()
