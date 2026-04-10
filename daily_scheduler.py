#!/usr/bin/env python3
"""
批量调度入口。

这个文件负责把“底图 + 音乐 + 特效参数”串成整条渲染流水线，并在可选情况下
调用外部上传脚本。它是当前仓库里最接近“自动化总控”的脚本。

注意：
- 这里的上传步骤只是“调用外部 batch_upload.py”。
- 朋友教程中提到的完整上传项目并不在当前仓库中。
- 这次整理后，路径会优先解析到当前仓库内的 `config/`，不再默认写死 macOS 目录。
"""

from __future__ import annotations

import logging
import os
import sys
import json
import random
import re
import time
import argparse
import subprocess
import threading
import ctypes
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import platform
import numpy as np
from group_upload_workflow import prepare_window_task_upload_batch
from path_helpers import (
    companion_local_config,
    default_scheduler_config,
    load_json_with_local_override,
    normalize_scheduler_config,
    open_path_in_file_manager,
    resolve_upload_script,
)

logger = logging.getLogger(__name__)

# ============ 平台检测 ============
IS_WINDOWS = platform.system() == "Windows"
IS_MAC = platform.system() == "Darwin"

# Windows 控制台默认可能是 GBK，含 emoji 输出会抛 UnicodeEncodeError。
if IS_WINDOWS:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# PyInstaller 打包兼容
if getattr(sys, 'frozen', False):
    _SCRIPT_DIR = Path(sys.executable).parent  # exe 所在目录 (配置/数据)
    _BUNDLE_DIR = Path(sys._MEIPASS)           # 解包目录 (模块导入)
else:
    _SCRIPT_DIR = Path(__file__).parent
    _BUNDLE_DIR = _SCRIPT_DIR

# ============ 路径配置 (支持 scheduler_config.json 覆盖) ============
def _default_platform_config() -> dict:
    defaults = default_scheduler_config(_SCRIPT_DIR)
    if IS_MAC:
        for candidate in ("/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
            if Path(candidate).exists():
                defaults["ffmpeg_bin"] = candidate
                defaults["ffmpeg_path"] = candidate
                break
    return defaults


def _load_platform_config():
    """加载平台配置。优先读取 scheduler_config.json，否则使用平台默认值。"""
    config_file = _SCRIPT_DIR / "scheduler_config.json"
    if config_file.exists() or companion_local_config(config_file).exists():
        return normalize_scheduler_config(load_json_with_local_override(config_file, {}), _SCRIPT_DIR)
    if IS_MAC:
        return normalize_scheduler_config(_default_platform_config(), _SCRIPT_DIR)
    # Windows/Linux 首次运行: GUI 向导让用户选择文件夹
    return _run_setup_wizard(config_file)

def _run_setup_wizard(config_file: Path) -> dict:
    """首次运行配置向导 - 弹窗让用户选择文件夹，全程鼠标操作。"""
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except ImportError:
        # 无 GUI 环境 (如服务器)，使用默认值
        defaults = default_scheduler_config(_SCRIPT_DIR)
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(defaults, f, ensure_ascii=False, indent=2)
        print(f"📝 已生成默认配置: {config_file}")
        return defaults

    root = tk.Tk()
    root.withdraw()  # 隐藏主窗口

    messagebox.showinfo(
        "🎬 首次运行配置",
        "欢迎使用 Daily Scheduler！\n\n"
        "首次运行需要设置以下文件夹路径：\n"
        "  1. 底图文件夹 (base image)\n"
        "  2. 音乐文件夹 (Suno Downloads)\n"
        "  3. 输出文件夹 (视频输出位置)\n"
        "  4. 上传配置文件 (upload_config.json)\n\n"
        "接下来会依次弹出文件夹选择窗口，\n"
        "请逐一选择对应的文件夹。"
    )

    # 1. 选择底图文件夹
    base_image_dir = filedialog.askdirectory(
        title="📁 第1步: 选择【底图文件夹】(存放各环境底图的文件夹)",
        initialdir=str(Path.home() / "Downloads")
    )
    if not base_image_dir:
        base_image_dir = str((Path(_SCRIPT_DIR) / "workspace" / "base_image").resolve(strict=False))
        Path(base_image_dir).mkdir(parents=True, exist_ok=True)

    # 2. 选择音乐文件夹
    music_dir = filedialog.askdirectory(
        title="🎵 第2步: 选择【音乐文件夹】(存放各环境音乐的文件夹)",
        initialdir=str(Path.home() / "Downloads")
    )
    if not music_dir:
        music_dir = str((Path(_SCRIPT_DIR) / "workspace" / "music").resolve(strict=False))
        Path(music_dir).mkdir(parents=True, exist_ok=True)

    # 3. 选择输出文件夹
    output_root = filedialog.askdirectory(
        title="📂 第3步: 选择【输出文件夹】(渲染好的视频存放位置)",
        initialdir=str(Path.home() / "Downloads")
    )
    if not output_root:
        output_root = str((Path(_SCRIPT_DIR) / "workspace" / "AutoTask").resolve(strict=False))
        Path(output_root).mkdir(parents=True, exist_ok=True)

    # 4. 选择上传配置文件
    upload_config = filedialog.askopenfilename(
        title="📋 第4步: 选择【上传配置文件】(upload_config.json)",
        initialdir=str(_SCRIPT_DIR),
        filetypes=[("JSON 文件", "*.json"), ("所有文件", "*.*")]
    )
    if not upload_config:
        upload_config = str((Path(_SCRIPT_DIR) / "config" / "upload_config.json").resolve(strict=False))
        Path(upload_config).parent.mkdir(parents=True, exist_ok=True)

    config = {
        "base_image_dir": base_image_dir,
        "music_dir": music_dir,
        "output_root": output_root,
        "upload_config": upload_config,
        "ffmpeg_bin": "ffmpeg",
    }

    # 保存配置
    target_config = companion_local_config(config_file)
    with open(target_config, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    messagebox.showinfo(
        "✅ 配置完成",
        f"配置已保存到:\n{target_config}\n\n"
        f"底图文件夹: {base_image_dir}\n"
        f"音乐文件夹: {music_dir}\n"
        f"输出文件夹: {output_root}\n"
        f"上传配置: {upload_config}\n\n"
        "如需修改，可以删除上述 JSON 文件后重新运行，\n"
        "或直接编辑该文件。"
    )

    root.destroy()
    return normalize_scheduler_config(config, _SCRIPT_DIR)

def _find_ffmpeg():
    """自动查找 FFmpeg 可执行文件"""
    # 同时支持 ffmpeg_bin 和 ffmpeg_path (兼容 GUI 保存的 key)
    cfg_path = _platform_cfg.get("ffmpeg_bin") or _platform_cfg.get("ffmpeg_path") or "ffmpeg"
    if os.path.isabs(cfg_path) and Path(cfg_path).exists():
        return cfg_path
    if IS_MAC:
        for p in ["/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg"]:
            if Path(p).exists():
                return p
    if IS_WINDOWS:
        local = _SCRIPT_DIR / "tools" / "ffmpeg" / "bin" / "ffmpeg.exe"
        if local.exists():
            return str(local)
    return cfg_path if cfg_path else "ffmpeg"

_platform_cfg = _load_platform_config()
BASE_IMAGE_DIR = Path(_platform_cfg.get("base_image_dir", _SCRIPT_DIR / "base_image"))
MUSIC_DIR = Path(_platform_cfg.get("music_dir", _SCRIPT_DIR / "music"))
OUTPUT_ROOT = Path(_platform_cfg.get("output_root", _SCRIPT_DIR / "output"))
CONFIG_PATH = Path(_platform_cfg.get("upload_config", _SCRIPT_DIR / "config" / "upload_config.json"))
FFMPEG_BIN = _find_ffmpeg()

# 素材盘里偶尔会出现繁简体目录名混用，这里做最小别名兼容。
TAG_DIR_ALIASES = {
    "竖琴": ["豎琴"],
    "豎琴": ["竖琴"],
}

# ============ 上传脚本路径 ============
UPLOAD_SCRIPT = resolve_upload_script(_SCRIPT_DIR)

# ============ 渲染配置 ============
def _default_worker_count(kind: str) -> int:
    cores = max(1, os.cpu_count() or 4)
    if IS_MAC:
        # VideoToolbox 编码很快，但滤镜链仍吃 CPU/GPU 内存带宽。Mac 上过高并发会互相抢资源，
        # 反而让单条视频变慢，所以默认收敛到更稳的队列宽度；仍允许环境变量手动覆盖。
        if kind == "video":
            return max(1, min(3, cores // 3 or 1))
        return max(2, min(6, cores // 2 or 2))
    return 10 if kind == "video" else 16


AUDIO_WORKERS = int(os.environ.get("AUDIO_WORKERS", _default_worker_count("audio")))  # 音频合成并行数
VIDEO_WORKERS = int(os.environ.get("VIDEO_WORKERS", _default_worker_count("video")))   # 视频渲染并行数
SONG_COUNT = 20               # 每个母带默认用多少首歌
MASTER_COUNT_PER_TAG = 5      # 每个环境默认生成多少个随机母带
HISTORY_FILE = _SCRIPT_DIR / "render_history.json"


@lru_cache(maxsize=8)
def _ffmpeg_has_encoder(encoder_name: str) -> bool:
    try:
        result = subprocess.run(
            [FFMPEG_BIN, "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )
    except Exception:
        return False
    blob = f"{result.stdout}\n{result.stderr}".lower()
    needle = str(encoder_name or "").strip().lower()
    return bool(needle) and needle in blob


@lru_cache(maxsize=1)
def _windows_has_amf_runtime() -> bool:
    if not IS_WINDOWS:
        return False
    try:
        ctypes.WinDLL("amfrt64.dll")
        return True
    except Exception:
        return False


@lru_cache(maxsize=1)
def _windows_has_nvenc_runtime() -> bool:
    if not IS_WINDOWS or not _ffmpeg_has_encoder("h264_nvenc"):
        return False
    try:
        probe = subprocess.run(
            ["nvidia-smi", "-L"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
        if probe.returncode != 0:
            return False
    except Exception:
        return False

    probe_path = _SCRIPT_DIR / "_tmp_nvenc_probe.mp4"
    try:
        result = subprocess.run(
            [
                FFMPEG_BIN,
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=black:s=1280x720:d=0.1",
                "-frames:v",
                "1",
                "-c:v",
                "h264_nvenc",
                str(probe_path),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
        )
        return result.returncode == 0 and probe_path.exists() and probe_path.stat().st_size > 0
    except Exception:
        return False
    finally:
        try:
            probe_path.unlink(missing_ok=True)
        except Exception:
            pass

# ============ FFmpeg 编码参数 (平台自适应) ============
if IS_MAC:
    VIDEO_CODEC = "h264_videotoolbox"
    VIDEO_BITRATE = "8000k"
    VIDEO_SPATIAL_AQ = False
    _CODEC_EXTRA_ARGS = [
        "-realtime", "1",
        "-prio_speed", "1",
        "-power_efficient", "1",
        "-allow_sw", "1",
        "-profile:v", "high",
        "-maxrate", "8000k",
        "-bufsize", "16000k",
    ]
elif IS_WINDOWS and _windows_has_nvenc_runtime():
    VIDEO_CODEC = "h264_nvenc"
    VIDEO_BITRATE = "8000k"
    VIDEO_SPATIAL_AQ = False
    _CODEC_EXTRA_ARGS = [
        "-preset", "p4",
        "-tune", "hq",
        "-rc", "vbr",
        "-maxrate", "8000k",
        "-bufsize", "16000k",
    ]
elif IS_WINDOWS and _ffmpeg_has_encoder("h264_amf") and _windows_has_amf_runtime():
    VIDEO_CODEC = "h264_amf"
    VIDEO_BITRATE = "8000k"
    VIDEO_SPATIAL_AQ = False
    _CODEC_EXTRA_ARGS = [
        "-usage", "transcoding",
        "-quality", "balanced",
        "-profile:v", "high",
        "-rc", "cbr",
        "-maxrate", "8000k",
        "-bufsize", "16000k",
    ]
else:
    VIDEO_CODEC = "libx264"           # CPU 编码 (最大兼容性)
    VIDEO_BITRATE = "8000k"
    VIDEO_SPATIAL_AQ = False
    _CODEC_EXTRA_ARGS = ['-preset', 'veryfast', '-maxrate', '8000k', '-bufsize', '16000k']
AUDIO_BITRATE = "320k"
AUDIO_SAMPLERATE = "44100"

# ============ 特效库导入 ============
sys.path.insert(0, str(_BUNDLE_DIR))
from effects_library import (
    PALETTES,
    ZOOM_SPEEDS,
    get_effect,
    list_effects,
    list_font_names,
    list_mega_bass_font_names,
    list_mega_bass_palette_names,
    list_mega_bass_particle_effects,
    list_mega_bass_style_variants,
    list_palette_names,
    list_particle_effects,
    list_text_positions,
    list_text_styles,
    list_tint_names,
    list_zoom_modes,
)

# ============ 完成标记系统 ============
# 用 .done 标记文件代替文件大小启发式判断，彻底避免渲染中断后误跳过不完整文件

def mark_complete(filepath: Path, duration: float = 0):
    """写入完成标记（同时保存时长信息）"""
    marker = Path(str(filepath) + ".done")
    data = f"{datetime.now().isoformat()}|dur={duration:.2f}"
    marker.write_text(data, encoding='utf-8')

def read_done_duration(filepath: Path) -> float:
    """从 .done 标记文件中读取保存的时长，失败返回 0"""
    marker = Path(str(filepath) + ".done")
    try:
        text = marker.read_text(encoding='utf-8').strip()
        if '|dur=' in text:
            return float(text.split('|dur=')[1])
    except Exception as e:
        logger.debug(f"读取 .done 标记文件失败 {marker}: {e}")
    return 0.0

def is_complete(filepath: Path) -> bool:
    """检查文件是否存在且有完成标记"""
    if not filepath.exists():
        return False
    marker = Path(str(filepath) + ".done")
    return marker.exists()

def clean_incomplete(filepath: Path):
    """清理不完整的文件（存在但无标记）"""
    if filepath.exists() and not is_complete(filepath):
        size_mb = filepath.stat().st_size / 1024 / 1024
        print(f"  🗑️ 删除不完整文件: {filepath.name} ({size_mb:.0f}MB)")
        filepath.unlink(missing_ok=True)

# ============ 工具函数 ============

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print(f"❌ 配置文件不存在: {CONFIG_PATH}")
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_all_tags(config: dict) -> list:
    return list(config.get("tag_to_project", {}).keys())


def _dir_has_direct_files(directory: Path, suffixes: tuple[str, ...]) -> bool:
    if not directory.exists() or not directory.is_dir():
        return False
    for item in directory.iterdir():
        if item.is_file() and item.suffix.lower() in suffixes and not item.name.startswith('.'):
            return True
    return False

def find_images_by_date(tag: str, date_str: str) -> list:
    tag_dir = BASE_IMAGE_DIR / tag
    if not tag_dir.exists():
        return []
    images = []
    for f in tag_dir.iterdir():
        if f.is_file() and f.suffix.lower() in ['.png', '.jpg', '.jpeg', '.mp4']:
            # 必须过滤掉 cover 封面图，只保留底图
            if f.stem.startswith(f"{date_str}_") and 'cover' not in f.stem:
                images.append(f)
    images.sort(key=lambda x: x.stem)
    return images

def find_images_simple(tag: str) -> list:
    """简单模式：扫描 tag 目录下所有图片（不要求日期命名）"""
    tag_dir = BASE_IMAGE_DIR / tag
    if not tag_dir.exists():
        return []
    images = []
    for f in tag_dir.iterdir():
        if f.is_file() and f.suffix.lower() in ['.png', '.jpg', '.jpeg']:
            # 排除封面图
            if 'cover' not in f.stem.lower():
                images.append(f)
    images.sort(key=lambda x: x.stem)
    return images

def extract_container(filename: str) -> int:
    match = re.match(r'\d{4}_(\d+)', Path(filename).stem)
    return int(match.group(1)) if match else None

def get_audio_duration(filepath) -> float:
    """获取音频时长 (100% 可靠版本: ffmpeg 全解码)"""
    # 优先从 .done 标记读取缓存的精确时长
    cached = read_done_duration(Path(filepath))
    if cached > 0:
        return cached

    # 方法 1: ffprobe 快速读取元数据
    try:
        ffmpeg_path = Path(FFMPEG_BIN)
        ffprobe_name = ffmpeg_path.name.replace("ffmpeg", "ffprobe")
        if IS_WINDOWS and not ffprobe_name.endswith(".exe"):
            ffprobe_name += ".exe"
        ffprobe_bin = str(ffmpeg_path.parent / ffprobe_name)

        cmd = [
            ffprobe_bin, '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            str(filepath)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
        out_val = result.stdout.strip()
        if out_val and out_val != 'N/A':
            dur = float(out_val)
            if dur > 0:
                return dur
    except Exception as e:
        logger.debug(f"ffprobe 获取时长失败，尝试 ffmpeg 全解码: {e}")

    # 方法 2: ffmpeg 全解码 (最可靠，对任何格式都准确)
    try:
        cmd = [FFMPEG_BIN, '-i', str(filepath), '-f', 'null', '-']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)

        # 从 stderr 最后的 time= 中获取实际解码时长
        for line in reversed(result.stderr.split('\n')):
            if 'time=' in line:
                parts = line.split('time=')[1].split(' ')[0].strip()
                h, m, s = parts.split(':')
                dur = float(h) * 3600 + float(m) * 60 + float(s)
                if dur > 0:
                    return dur

        # 从 Duration: 行获取
        for line in result.stderr.split('\n'):
            if 'Duration:' in line and 'N/A' not in line:
                parts = line.split('Duration:')[1].split(',')[0].strip()
                h, m, s = parts.split(':')
                dur = float(h) * 3600 + float(m) * 60 + float(s)
                if dur > 0:
                    return dur
    except Exception as e:
        logger.warning(f"ffmpeg 全解码获取时长失败 {Path(filepath).name}: {e}")

    print(f"  ⚠️ 无法获取时长 {Path(filepath).name}, 默认180s")
    return 180.0

def find_all_music(tag: str) -> list:
    candidate_names = [tag] + TAG_DIR_ALIASES.get(tag, [])
    mp3s = []
    seen = set()
    for name in candidate_names:
        tag_dir = MUSIC_DIR / name
        if not tag_dir.exists():
            continue
        for root, _, files in os.walk(tag_dir):
            for f in files:
                if f.lower().endswith('.mp3') and not f.startswith('.'):
                    p = Path(root) / f
                    if p not in seen:
                        seen.add(p)
                        mp3s.append(p)
    return mp3s

# ============ 核心任务逻辑 ============

def build_master_audio_task(tag: str, mp3_pool: list, output_path: Path, task_id: int, song_count: int = 20) -> dict:
    """
    具体的母带合成任务函数 (Thread Safe)
    song_count: 每个母带使用的歌曲数量
    """
    if output_path.exists() and output_path.stat().st_size > 100_000:
        if is_complete(output_path):
            dur = get_audio_duration(output_path)
            return {"success": True, "tag": tag, "id": task_id, "path": output_path, "dur": dur, "msg": "已存在"}
        else:
            size_mb = output_path.stat().st_size / 1024 / 1024
            print(f"  ⚠️ [{tag}] 母带 #{task_id} 存在但无完成标记 ({size_mb:.0f}MB)，删除后重新生成")
            output_path.unlink(missing_ok=True)

    start_time = time.time()
    
    # 随机打乱并选取指定数量的歌曲
    shuffled = mp3_pool.copy()
    random.shuffle(shuffled)
    
    # 按歌曲数量选取
    actual_count = min(song_count, len(shuffled))
    selected = shuffled[:actual_count]
    
    # 计算总时长
    total_dur = 0.0
    for mp3 in selected:
        total_dur += get_audio_duration(mp3)
    
    print(f"  📊 [{tag}] 母带 #{task_id}: 选了 {len(selected)}/{len(mp3_pool)} 首歌 (目标{song_count}首), 预计时长 {total_dur:.0f}s ({total_dur/60:.1f}min)")
            
    # 写列表文件 (用 PID 避免文件名冲突)
    # Windows FFmpeg 无法处理中文路径，用临时符号链接/硬链接绕过
    list_file = output_path.parent / f"temp_concat_{task_id}_{os.getpid()}.txt"
    temp_links = []
    
    with open(list_file, 'w', encoding='utf-8') as f:
        for i, mp3 in enumerate(selected):
            mp3_path = str(mp3)
            # 检查是否包含非 ASCII 字符（中文等）
            try:
                mp3_path.encode('ascii')
                is_ascii = True
            except UnicodeEncodeError:
                is_ascii = False
            
            if IS_WINDOWS and not is_ascii:
                # 创建临时硬链接到纯英文路径
                import tempfile
                temp_dir = Path(tempfile.gettempdir()) / "ffmpeg_audio"
                temp_dir.mkdir(parents=True, exist_ok=True)
                ext = Path(mp3).suffix
                link_path = temp_dir / f"audio_{task_id}_{i}{ext}"
                try:
                    if link_path.exists():
                        link_path.unlink()
                    # 优先硬链接（不占空间），失败则复制
                    try:
                        os.link(str(mp3), str(link_path))
                    except (OSError, NotImplementedError):
                        import shutil
                        shutil.copy2(str(mp3), str(link_path))
                    temp_links.append(link_path)
                    safe = str(link_path).replace("\\", "/")
                except Exception:
                    safe = mp3_path.replace("\\", "/").replace("'", "'\\''")
            else:
                safe = mp3_path.replace("\\", "/").replace("'", "'\\''")
            
            f.write(f"file '{safe}'\n")

    cmd = [
        FFMPEG_BIN, '-y', '-hide_banner', '-loglevel', 'error',
        '-f', 'concat', '-safe', '0', '-i', str(list_file),
        '-c:a', 'aac', '-b:a', AUDIO_BITRATE, '-ar', AUDIO_SAMPLERATE,
        str(output_path)
    ]
    
    try:
        subprocess.run(cmd, check=True)
        # 删除临时文件
        list_file.unlink(missing_ok=True)
        for lnk in temp_links:
            try: lnk.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"清理临时链接失败: {e}")
        actual_dur = get_audio_duration(output_path)
        elapsed = time.time() - start_time
        print(f"  ✅ [{tag}] 母带 #{task_id} 完成: 实际时长 {actual_dur:.0f}s ({actual_dur/60:.1f}min), 耗时 {elapsed:.1f}s")
        mark_complete(output_path, duration=actual_dur)  # 写入完成标记 + 精确时长
        return {
            "success": True, 
            "tag": tag, 
            "id": task_id, 
            "path": output_path, 
            "dur": actual_dur, 
            "time": elapsed
        }
    except Exception as e:
        return {"success": False, "tag": tag, "id": task_id, "error": str(e)}

def _safe_path_for_ffmpeg(filepath, label="file"):
    """Windows FFmpeg 中文路径保护：创建临时硬链接"""
    filepath = Path(filepath)
    if not IS_WINDOWS:
        return str(filepath), None
    try:
        str(filepath).encode('ascii')
        return str(filepath), None  # 纯英文路径，无需处理
    except UnicodeEncodeError:
        pass
    import tempfile
    temp_dir = Path(tempfile.gettempdir()) / "ffmpeg_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    ext = filepath.suffix
    link_path = temp_dir / f"{label}_{os.getpid()}_{id(filepath)}{ext}"
    try:
        if link_path.exists():
            link_path.unlink()
        try:
            os.link(str(filepath), str(link_path))
        except (OSError, NotImplementedError):
            import shutil
            shutil.copy2(str(filepath), str(link_path))
        return str(link_path), link_path
    except Exception:
        return str(filepath), None

def render_video_task(tag: str, image_path, audio_path, output_path, filter_complex, extra_inputs=None) -> dict:
    """
    具体的视频渲染任务函数
    """
    # Windows 中文路径保护
    safe_image, tmp_img = _safe_path_for_ffmpeg(image_path, "img")
    safe_audio, tmp_aud = _safe_path_for_ffmpeg(audio_path, "aud")
    
    cmd = [
        FFMPEG_BIN, '-y', '-hide_banner', '-loglevel', 'error',
        '-loop', '1', '-r', '25', '-i', safe_image,
        '-i', safe_audio,
    ]
    if extra_inputs:
        cmd.extend(extra_inputs)
        
    cmd.extend([
        '-filter_complex', filter_complex,
        '-map', '[outv]', '-map', '1:a',
        '-c:v', VIDEO_CODEC, '-b:v', VIDEO_BITRATE,
    ] + _CODEC_EXTRA_ARGS + [
        '-pix_fmt', 'yuv420p', '-movflags', '+faststart',
        '-color_range', 'pc',
        '-aspect', '16:9',
        '-c:a', 'copy',
        '-shortest',
        str(output_path)
    ])

    start = time.time()
    try:
        subprocess.run(cmd, check=True, timeout=7200) # 2小时超时
        # 清理临时文件
        for tmp in [tmp_img, tmp_aud]:
            if tmp:
                try: tmp.unlink(missing_ok=True)
                except Exception as e:
                    logger.warning(f"清理临时文件失败: {e}")
        
        # 冷却: 让 CPU 短暂喘息，防止持续满载导致热功耗过高
        # 特别是在多并行时，这能给电池充电的机会
        time.sleep(5) 
        
        elapsed = time.time() - start
        mark_complete(output_path)  # 写入完成标记
        return {
            "success": True,
            "tag": tag,
            "file": Path(output_path).name,
            "time": elapsed
        }
    except Exception as e:
        return {
            "success": False,
            "tag": tag,
            "file": Path(output_path).name,
            "error": str(e)
        }

# ============ 主程序 ============

def phase0_cleanup(target_date: str):
    """Phase 0: 清理已上传的旧文件 (今天之前)"""
    print(f"\n{'🗑️' * 20}")
    print("  Phase 0: 清理已上传的旧文件 (今天之前)")
    print(f"{'🗑️' * 20}")
    
    cleaned_files = 0
    cleaned_size = 0
    cleaned_masters = 0
    
    if OUTPUT_ROOT.exists() and '--no-clean' not in sys.argv:
        for folder in sorted(OUTPUT_ROOT.iterdir()):
            if not folder.is_dir() or folder.name.startswith('.'):
                continue
            
            # 跳过今天的文件夹 (文件夹名格式: {MMDD}_{tag})
            if folder.name.startswith(target_date):
                continue
            
            # 必须有 upload_report.json 才清理 (确认已上传完成)
            report_file = folder / "upload_report.json"
            if not report_file.exists():
                print(f"  ⏭️  {folder.name}: 无上传报告，保留")
                continue
            
            folder_freed = 0
            
            # 删除 .mp4 视频文件 + .done 标记
            for mp4 in folder.glob("*.mp4"):
                size = mp4.stat().st_size
                mp4.unlink()
                # 同时删除完成标记
                done_marker = Path(str(mp4) + ".done")
                done_marker.unlink(missing_ok=True)
                cleaned_files += 1
                folder_freed += size
            
            # 删除 .masters/ 目录 (隐藏的母带音频，占大量空间)
            masters_dir = folder / ".masters"
            if masters_dir.exists():
                import shutil
                for m4a in masters_dir.iterdir():
                    if m4a.is_file():
                        folder_freed += m4a.stat().st_size
                shutil.rmtree(masters_dir)
                cleaned_masters += 1
            
            cleaned_size += folder_freed
            
            # 如果文件夹只剩 manifest 和 report，也可以选择删除整个文件夹
            remaining = [f for f in folder.iterdir() if not f.name.startswith('.')]
            if len(remaining) <= 2:  # 只剩 manifest + report
                # 保留这些记录文件，不删除文件夹
                pass
        
        if cleaned_files > 0 or cleaned_masters > 0:
            gb = cleaned_size / 1024 / 1024 / 1024
            print(f"  🗑️  清理完成: {cleaned_files} 个视频 + {cleaned_masters} 个母带目录")
            print(f"  💾  释放空间: {gb:.1f} GB")
        else:
            print(f"  ✅ 无需清理 (所有旧文件已清除)")
    elif '--no-clean' in sys.argv:
        print("  ⏭️  跳过清理 (--no-clean)")
    else:
        print("  ℹ️  输出目录不存在，跳过")

# ============ 配置数据类 ============

@dataclass
class RenderOptions:
    target_date: str = ""
    simple_mode: bool = False
    render_only: bool = False
    selected_tags: list[str] | None = None
    upload_auto_close_browser: bool = True
    upload_skip_channels: list[int] | None = None
    upload_window_plan_file: str = ""
    upload_metadata_mode: str = "prompt_api"
    upload_fill_text: bool = True
    upload_fill_thumbnails: bool = True
    upload_sync_daily_content: bool = True
    fx_randomize: bool = False
    fx_spectrum: bool | str = True
    fx_timeline: bool | str = True
    fx_letterbox: bool | str = False
    fx_zoom: str = "normal"
    fx_color_spectrum: str = "random"
    fx_color_timeline: str = "random"
    fx_spectrum_y: int = 530
    fx_spectrum_x: int = -1
    fx_spectrum_w: int = 1200
    fx_style: str = "bar"
    fx_film_grain: bool | str = False
    fx_grain_strength: int = 15
    fx_vignette: bool | str = False
    fx_color_tint: str = "none"
    fx_soft_focus: bool | str = False
    fx_soft_focus_sigma: float = 1.5
    fx_particle: str = "none"
    fx_particle_opacity: float = 0.6
    fx_particle_speed: float = 1.0
    fx_text_font: str = "default"
    fx_text: str = ""
    fx_text_pos: str = "center"
    fx_text_size: int = 60
    fx_text_style: str = "Classic"
    fx_visual_preset: str = "none"
    fx_bass_pulse: bool | str = False
    fx_bass_pulse_scale: float = 0.03
    fx_bass_pulse_brightness: float = 0.04

def parse_arguments() -> RenderOptions:
    global SONG_COUNT
    opts = RenderOptions()
    date_arg = None

    def parse_toggle_value(value: str, *, default: bool) -> bool | str:
        text = str(value or "").strip().lower()
        if text == "random":
            return "random"
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default
    
    for arg in sys.argv[1:]:
        if arg == '--no-spectrum':     opts.fx_spectrum = False
        elif arg.startswith('--spectrum='):
            opts.fx_spectrum = parse_toggle_value(arg.split('=', 1)[1], default=True)
        elif arg == '--no-timeline':  opts.fx_timeline = False
        elif arg.startswith('--timeline='):
            opts.fx_timeline = parse_toggle_value(arg.split('=', 1)[1], default=True)
        elif arg == '--randomize-effects': opts.fx_randomize = True
        elif arg == '--fixed-effects': opts.fx_randomize = False
        elif arg.startswith('--letterbox'):
            if '=' in arg:
                opts.fx_letterbox = parse_toggle_value(arg.split('=', 1)[1], default=False)
            else:
                opts.fx_letterbox = True
        elif arg.startswith('--zoom='):  opts.fx_zoom = arg.split('=')[1]
        elif arg.startswith('--color='):
            c = arg.split('=')[1]
            opts.fx_color_spectrum = c
            opts.fx_color_timeline = c
        elif arg.startswith('--color-spectrum='): opts.fx_color_spectrum = arg.split('=')[1]
        elif arg.startswith('--color-timeline='): opts.fx_color_timeline = arg.split('=')[1]
        elif arg.startswith('--spectrum-y='): opts.fx_spectrum_y = int(arg.split('=')[1])
        elif arg.startswith('--spectrum-x='): opts.fx_spectrum_x = int(arg.split('=')[1])
        elif arg.startswith('--spectrum-w='): opts.fx_spectrum_w = int(arg.split('=')[1])
        elif arg.startswith('--style='): opts.fx_style = arg.split('=')[1]
        elif arg.startswith('--film-grain='):
            value = arg.split('=', 1)[1]
            if value.strip().lower() == "random":
                opts.fx_film_grain = "random"
            elif re.fullmatch(r"\s*(?:0|false|no|off)\s*", value, re.I):
                opts.fx_film_grain = False
            else:
                opts.fx_film_grain = True
                opts.fx_grain_strength = int(value)
        elif arg == '--vignette': opts.fx_vignette = True
        elif arg.startswith('--vignette='): opts.fx_vignette = parse_toggle_value(arg.split('=', 1)[1], default=False)
        elif arg.startswith('--color-tint='): opts.fx_color_tint = arg.split('=')[1]
        elif arg.startswith('--soft-focus='):
            value = arg.split('=', 1)[1]
            if value.strip().lower() == "random":
                opts.fx_soft_focus = "random"
            elif re.fullmatch(r"\s*(?:0|false|no|off)\s*", value, re.I):
                opts.fx_soft_focus = False
            else:
                opts.fx_soft_focus = True
                opts.fx_soft_focus_sigma = float(value)
        elif arg.startswith('--particle='): opts.fx_particle = arg.split('=')[1]
        elif arg.startswith('--particle-opacity='): opts.fx_particle_opacity = float(arg.split('=')[1])
        elif arg.startswith('--song-count='): SONG_COUNT = int(arg.split('=')[1])
        elif arg.startswith('--text-font='): opts.fx_text_font = arg.split('=')[1]
        elif arg.startswith('--text='): opts.fx_text = arg.split('=', 1)[1]
        elif arg.startswith('--text-pos='): opts.fx_text_pos = arg.split('=')[1]
        elif arg.startswith('--text-size='): opts.fx_text_size = int(arg.split('=')[1])
        elif arg.startswith('--text-style='): opts.fx_text_style = arg.split('=')[1]
        elif arg.startswith('--visual-preset='): opts.fx_visual_preset = arg.split('=', 1)[1]
        elif arg == '--bass-pulse': opts.fx_bass_pulse = True
        elif arg.startswith('--bass-pulse='): opts.fx_bass_pulse = parse_toggle_value(arg.split('=', 1)[1], default=False)
        elif arg.startswith('--bass-pulse-scale='): opts.fx_bass_pulse_scale = float(arg.split('=')[1])
        elif arg.startswith('--bass-pulse-brightness='): opts.fx_bass_pulse_brightness = float(arg.split('=')[1])
        elif arg.startswith('--tags=') or arg.startswith('--tag='):
            raw = arg.split('=', 1)[1]
            tags = [t.strip() for t in re.split(r'[，,]', raw) if t.strip()]
            opts.selected_tags = tags or None
        elif arg.startswith('--skip-channels='):
            raw = arg.split('=', 1)[1]
            opts.upload_skip_channels = [int(x.strip()) for x in re.split(r'[，,]', raw) if x.strip().isdigit()]
        elif arg.startswith('--window-plan-file='):
            opts.upload_window_plan_file = arg.split('=', 1)[1].strip()
        elif arg.startswith('--upload-metadata-mode='):
            opts.upload_metadata_mode = arg.split('=', 1)[1].strip() or "prompt_api"
        elif arg == '--upload-fill-text':
            opts.upload_fill_text = True
        elif arg == '--no-upload-fill-text':
            opts.upload_fill_text = False
        elif arg == '--upload-fill-thumbnails':
            opts.upload_fill_thumbnails = True
        elif arg == '--no-upload-fill-thumbnails':
            opts.upload_fill_thumbnails = False
        elif arg == '--upload-sync-daily-content':
            opts.upload_sync_daily_content = True
        elif arg == '--no-upload-sync-daily-content':
            opts.upload_sync_daily_content = False
        elif arg == '--simple':  pass
        elif arg == '--render-only': opts.render_only = True
        elif arg == '--keep-upload-browser-open': opts.upload_auto_close_browser = False
        elif arg == '--auto-close-browser': opts.upload_auto_close_browser = True
        elif not arg.startswith('--') and arg.isdigit():
            date_arg = arg

    # 简单模式检测
    if '--standard' in sys.argv:
        opts.simple_mode = False
    elif '--simple' in sys.argv:
        opts.simple_mode = True
    elif IS_WINDOWS:
        opts.simple_mode = True
        print("\n🟢 Windows 默认简单模式（不需要日期命名规则）")
        print("   如需标准模式，请加 --standard 参数")
    else:
        opts.simple_mode = False
    
    if opts.simple_mode:
        print("\n🟢 简单模式: 扫描所有图片+音乐，自动配固渲染")
        print("   不需要日期格式命名，不需要 generation_map")
        opts.target_date = datetime.now().strftime("%m%d")
    elif not date_arg:
        print("❌ 必须指定日期 (MMDD, e.g. 0214)")
        print("   或使用 --simple 进入简单模式")
        return None
    else:
        opts.target_date = date_arg
        
    return opts


def build_effect_kwargs(opts: RenderOptions, *, rng=None) -> dict:
    rng = rng or random

    def choose_value(value, *, default=None, choices: list | None = None):
        raw = value
        if isinstance(raw, str):
            raw = raw.strip()
            if raw.lower() == "random":
                if choices:
                    return rng.choice(list(choices))
                return default
        return raw if raw not in (None, "") else default

    def choose_flag(value, *, default: bool, probability_true: float) -> bool:
        if isinstance(value, str):
            text = value.strip().lower()
            if text == "random":
                return rng.random() < probability_true
            if text in {"1", "true", "yes", "y", "on"}:
                return True
            if text in {"0", "false", "no", "n", "off"}:
                return False
            return default
        if isinstance(value, bool):
            return value
        return bool(value) if value is not None else default

    def choose_int(value, *, default: int, minimum: int | None = None, maximum: int | None = None):
        raw = value
        if isinstance(raw, str):
            text = raw.strip()
            matched = re.fullmatch(r"\s*(-?\d+)\s*[-,~]\s*(-?\d+)\s*", text)
            if matched:
                left = int(matched.group(1))
                right = int(matched.group(2))
                low, high = sorted((left, right))
                picked = rng.randint(low, high)
                if minimum is not None:
                    picked = max(minimum, picked)
                if maximum is not None:
                    picked = min(maximum, picked)
                return picked
            if text.lower() == "random" and minimum is not None and maximum is not None:
                return rng.randint(minimum, maximum)
            try:
                picked = int(text)
            except Exception:
                picked = default
        else:
            try:
                picked = int(raw)
            except Exception:
                picked = default
        if minimum is not None:
            picked = max(minimum, picked)
        if maximum is not None:
            picked = min(maximum, picked)
        return picked

    def choose_float(value, *, default: float, minimum: float | None = None, maximum: float | None = None, precision: int = 2):
        raw = value
        if isinstance(raw, str):
            text = raw.strip()
            matched = re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*[-,~]\s*(-?\d+(?:\.\d+)?)\s*", text)
            if matched:
                left = float(matched.group(1))
                right = float(matched.group(2))
                low, high = sorted((left, right))
                picked = round(rng.uniform(low, high), precision)
                if minimum is not None:
                    picked = max(minimum, picked)
                if maximum is not None:
                    picked = min(maximum, picked)
                return picked
            if text.lower() == "random" and minimum is not None and maximum is not None:
                return round(rng.uniform(minimum, maximum), precision)
            try:
                picked = float(text)
            except Exception:
                picked = default
        else:
            try:
                picked = float(raw)
            except Exception:
                picked = default
        if minimum is not None:
            picked = max(minimum, picked)
        if maximum is not None:
            picked = min(maximum, picked)
        return picked

    kwargs = {
        "spectrum": choose_flag(opts.fx_spectrum, default=True, probability_true=0.90),
        "timeline": choose_flag(opts.fx_timeline, default=True, probability_true=0.80),
        "letterbox": choose_flag(opts.fx_letterbox, default=False, probability_true=0.35),
        "visual_preset": getattr(opts, "fx_visual_preset", "none"),
        "zoom": choose_value(opts.fx_zoom, default="normal", choices=list_zoom_modes()),
        "color_spectrum": choose_value(opts.fx_color_spectrum, default="WhiteGold", choices=list_palette_names()),
        "color_timeline": choose_value(opts.fx_color_timeline, default="WhiteGold", choices=list_palette_names()),
        "spectrum_y": choose_int(opts.fx_spectrum_y, default=530, minimum=0, maximum=1000),
        "spectrum_x": choose_int(opts.fx_spectrum_x, default=-1, minimum=-1, maximum=1800),
        "spectrum_w": choose_int(opts.fx_spectrum_w, default=1200, minimum=360, maximum=1800),
        "style": choose_value(opts.fx_style, default="bar", choices=list_effects()),
        "text": opts.fx_text,
        "text_pos": choose_value(opts.fx_text_pos, default="center", choices=list_text_positions()),
        "text_size": choose_int(opts.fx_text_size, default=60, minimum=18, maximum=180),
        "text_style": choose_value(opts.fx_text_style, default="Classic", choices=list_text_styles()),
        "film_grain": choose_flag(opts.fx_film_grain, default=False, probability_true=0.35),
        "grain_strength": choose_int(opts.fx_grain_strength, default=15, minimum=0, maximum=60),
        "vignette": choose_flag(opts.fx_vignette, default=False, probability_true=0.35),
        "color_tint": choose_value(opts.fx_color_tint, default="none", choices=list_tint_names()),
        "soft_focus": choose_flag(opts.fx_soft_focus, default=False, probability_true=0.25),
        "soft_focus_sigma": choose_float(opts.fx_soft_focus_sigma, default=1.5, minimum=0.3, maximum=6.0),
        "particle": choose_value(
            opts.fx_particle,
            default="none",
            choices=[item for item in list_particle_effects() if item not in {"none", "random"}],
        ),
        "particle_opacity": choose_float(opts.fx_particle_opacity, default=0.6, minimum=0.0, maximum=1.0),
        "particle_speed": choose_float(opts.fx_particle_speed, default=1.0, minimum=0.2, maximum=3.0),
        "text_font": choose_value(opts.fx_text_font, default="default", choices=list_font_names()),
        "bass_pulse": (
            True
            if str(getattr(opts, "fx_visual_preset", "")) == "mega_bass"
            else choose_flag(getattr(opts, "fx_bass_pulse", False), default=False, probability_true=0.35)
        ),
        "bass_pulse_scale": choose_float(getattr(opts, "fx_bass_pulse_scale", 0.03), default=0.03, minimum=0.0, maximum=0.12, precision=3),
        "bass_pulse_brightness": choose_float(getattr(opts, "fx_bass_pulse_brightness", 0.04), default=0.04, minimum=0.0, maximum=0.12, precision=3),
    }
    if kwargs["visual_preset"] == "mega_bass":
        mega_palettes = list_mega_bass_palette_names() or ["MegaBassPurple", "MegaBassGreen", "MegaBassAmber"]
        mega_styles = list_mega_bass_style_variants() or ["mega_neon_line"]
        mega_particles = list_mega_bass_particle_effects() or [item for item in list_particle_effects() if item not in {"none", "random"}]
        latin_text = all(ord(ch) < 128 for ch in str(kwargs.get("text") or "")) and bool(str(kwargs.get("text") or "").strip())
        mega_fonts = list_mega_bass_font_names() if latin_text else [item for item in list_font_names() if item in {"default", "heiti", "songti"}]
        kwargs["style"] = rng.choice(mega_styles)
        palette_name = rng.choice(mega_palettes)
        kwargs["color_spectrum"] = palette_name
        kwargs["color_timeline"] = palette_name
        kwargs["zoom"] = rng.choice(["slow", "normal", "fast"])
        kwargs["particle"] = rng.choice(mega_particles)
        kwargs["particle_opacity"] = choose_float(opts.fx_particle_opacity, default=0.34, minimum=0.12, maximum=0.65)
        kwargs["particle_speed"] = choose_float(opts.fx_particle_speed, default=1.15, minimum=0.45, maximum=1.85)
        kwargs["text_style"] = rng.choice(["Glow", "Neon", "Bold"])
        kwargs["text_pos"] = rng.choice(["center", "bottom_center", "top_center"])
        kwargs["text_font"] = rng.choice(mega_fonts) if mega_fonts else kwargs["text_font"]
        kwargs["text_size"] = choose_int(opts.fx_text_size, default=96, minimum=62, maximum=144)
        kwargs["spectrum_w"] = rng.randint(980, 1620)
        kwargs["spectrum_y"] = rng.randint(470, 620)
        kwargs["color_tint"] = rng.choice(["none", "blue_night", "cool", "golden"])
    return kwargs


@lru_cache(maxsize=64)
def detect_audio_bpm_profile(audio_path: str) -> tuple[float, float]:
    path = str(audio_path or "").strip()
    if not path:
        return 128.0, 0.0

    cmd = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        path,
        "-t",
        "90",
        "-ac",
        "1",
        "-ar",
        "11025",
        "-f",
        "s16le",
        "-",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, check=True, timeout=120)
        pcm = proc.stdout
        if not pcm:
            return 128.0, 0.0
        samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
        if samples.size < 11025:
            return 128.0, 0.0

        frame_size = 1024
        hop = 512
        frame_count = 1 + max(0, (samples.size - frame_size) // hop)
        if frame_count < 32:
            return 128.0, 0.0

        env = np.empty(frame_count, dtype=np.float32)
        for idx in range(frame_count):
            start = idx * hop
            frame = samples[start : start + frame_size]
            env[idx] = float(np.sqrt(np.mean(frame * frame)))

        smooth = np.convolve(env, np.ones(8, dtype=np.float32) / 8.0, mode="same")
        onset = np.maximum(env - smooth, 0.0)
        onset -= onset.min()
        peak = float(onset.max())
        if peak <= 1e-6:
            return 128.0, 0.0
        onset /= peak

        env_rate = 11025.0 / hop
        min_bpm = 90.0
        max_bpm = 170.0
        min_lag = max(1, int(env_rate * 60.0 / max_bpm))
        max_lag = max(min_lag + 1, int(env_rate * 60.0 / min_bpm))

        centered = onset - float(onset.mean())
        best_lag = min_lag
        best_score = -1.0
        for lag in range(min_lag, min(max_lag, centered.size - 2)):
            score = float(np.dot(centered[:-lag], centered[lag:]))
            if score > best_score:
                best_score = score
                best_lag = lag

        bpm = max(min_bpm, min(max_bpm, 60.0 * env_rate / max(best_lag, 1)))
        first_peak_idx = int(np.argmax(onset))
        first_peak_time = first_peak_idx / env_rate
        phase = (math.pi / 2.0) - (2.0 * math.pi * (bpm / 60.0) * first_peak_time)
        return round(float(bpm), 2), float(phase)
    except Exception as e:
        logger.warning(f"BPM 分析失败，使用默认值 128.0: {e}")
        return 128.0, 0.0

def phase2_build_master_audios(active_projects: list) -> dict:
    """Phase 2: 并行母带合成"""
    print(f"\n{'🎵' * 20}")
    print("  Phase 2: 并行母带合成")
    print(f"{'🎵' * 20}")
    
    audio_jobs = []
    # 存储生成的母带路径: tag -> [output_path1, output_path2...]
    master_map = {} 

    for proj in active_projects:
        tag = proj['tag']
        vid_count = len(proj['images'])
        
        # 视频越少，母带越少，避免浪费。
        needed = min(vid_count, MASTER_COUNT_PER_TAG)
        needed = max(1, needed)
        
        master_map[tag] = []
        
        master_dir = proj['out_dir'] / ".masters"
        master_dir.mkdir(exist_ok=True)
        
        print(f"  • {tag}: 计划生成 {needed} 个随机母带")
        
        for i in range(needed):
            p = master_dir / f"master_{i+1}.m4a"
            master_map[tag].append(p)
            audio_jobs.append({
                "tag": tag,
                "pool": proj['music'],
                "out": p,
                "id": i+1
            })
            
    print(f"  📋 总计 {len(audio_jobs)} 个母带任务，正在并行处理...")

    # 执行音频任务
    audio_success_count = 0
    audio_start = time.time()
    
    with ThreadPoolExecutor(max_workers=AUDIO_WORKERS) as executor:
        futures = {
            executor.submit(build_master_audio_task, j['tag'], j['pool'], j['out'], j['id'], SONG_COUNT): j 
            for j in audio_jobs
        }
        
        for i, future in enumerate(as_completed(futures)):
            res = future.result()
            progress = f"[{i+1}/{len(audio_jobs)}]"
            
            if res['success']:
                audio_success_count += 1
                msg = res.get('msg', f"耗时 {res.get('time',0):.1f}s")
            else:
                print(f"  {progress} ❌ {res['tag']} #{res['id']}: {res['error']}")

    print(f"  ⏱️  母带阶段完成: {audio_success_count}/{len(audio_jobs)} 成功 (耗时 {time.time()-audio_start:.1f}s)")
    return master_map

def phase3_render_and_upload(active_projects: list, master_map: dict, opts: RenderOptions, pipeline_mode: bool):
    """Phase 3: 逐 Tag 渲染 + 自动上传"""
    print(f"\n{'🚀' * 20}")
    if pipeline_mode:
        print(f"  Phase 3: 逐 Tag 渲染 + 自动上传 (并行数: {VIDEO_WORKERS})")
    else:
        print(f"  Phase 3: 逐 Tag 渲染 (并行数: {VIDEO_WORKERS})")
    print(f"{'🚀' * 20}")
    
    total_success = 0
    total_rendered = 0
    upload_procs = []
    
    if '--dry-run' in sys.argv:
        print("  ⏭️  跳过渲染 (--dry-run 模式)")
        return total_success, total_rendered, upload_procs
        
    for tag_idx, proj in enumerate(active_projects, 1):
        tag = proj['tag']
        out_dir = proj['out_dir']
        masters = master_map.get(tag, [])
        valid_masters = [m for m in masters if m.exists()]
        
        print(f"\n  ── [{tag_idx}/{len(active_projects)}] {tag} ──")
        
        if not valid_masters:
            print(f"  ⚠️ 无可用母带，跳过")
            continue
        
        # 写 manifest (渲染前写，不依赖视频文件是否存在)
        manifest_path = out_dir / "upload_manifest.json"
        map_file = BASE_IMAGE_DIR / tag / "generation_map.json"
        manifest_channels = {}
        
        if opts.simple_mode:
            print(f"  📋 简单模式: 跳过 manifest (无容器信息)")
        elif map_file.exists():
            try:
                with open(map_file, 'r', encoding='utf-8') as f:
                    gen_map = json.load(f)
                
                for container_str, ch_info in gen_map.get("channels", {}).items():
                    container = int(container_str)
                    day_info = ch_info.get("days", {}).get(opts.target_date)
                    if not day_info:
                        continue
                    
                    tag_dir = BASE_IMAGE_DIR / tag
                    covers = [str(tag_dir / c) for c in day_info.get("covers", [])]
                    
                    manifest_channels[str(container)] = {
                        "video": f"{opts.target_date}_{container}.mp4",
                        "title": day_info.get("title", ""),
                        "description": day_info.get("description", ""),
                        "thumbnails": covers,
                        "is_ypp": ch_info.get("is_ypp", False),
                        "ab_titles": ([day_info.get("title", "")] + day_info.get("ab_titles", []))
                                     if ch_info.get("is_ypp", False) and day_info.get("ab_titles")
                                     else day_info.get("ab_titles", []),
                        "set": day_info.get("set", 1),
                    }
                
                print(f"  📋 manifest: {len(manifest_channels)} 个频道 (from generation_map)")
            except Exception as e:
                print(f"  ⚠️ 读取 generation_map 失败: {e}")
        
        if not opts.simple_mode and not manifest_channels:
            for img in proj['images']:
                container = extract_container(img.name)
                if container is None:
                    continue
                manifest_channels[str(container)] = {
                    "video": f"{opts.target_date}_{container}.mp4",
                    "title": "",
                    "description": "",
                    "thumbnails": [],
                    "is_ypp": False,
                    "ab_titles": [],
                    "set": 0,
                }
            if manifest_channels:
                print(f"  📋 manifest: {len(manifest_channels)} 个频道 (fallback)")
        
        if manifest_channels:
            # 【空标题检查】写入前警告
            empty_title_containers = [c for c, info in manifest_channels.items() if not info.get("title")]
            if empty_title_containers:
                print(f"  ⚠️⚠️ 警告: {len(empty_title_containers)} 个频道标题为空! Containers: {empty_title_containers}")
                print(f"  ⚠️⚠️ 这些频道上传时将会失败！请检查 metadata_channels.md 和 generation_map.json")

            manifest = {
                "date": opts.target_date,
                "tag": tag,
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "source": "generation_map.json" if map_file.exists() else "fallback",
                "channels": manifest_channels,
            }
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
        
        # 收集该 Tag 的渲染任务
        tag_video_jobs = []
        tag_skip_count = 0
        for idx, img in enumerate(proj['images'], 1):
            if opts.simple_mode:
                out_file = out_dir / f"{opts.target_date}_{idx:03d}.mp4"
            else:
                container = extract_container(img.name)
                if container is None:
                    continue
                out_file = out_dir / f"{opts.target_date}_{container}.mp4"
            
            if is_complete(out_file):
                tag_skip_count += 1
                continue
            
            # 存在但无标记 = 不完整，清理后重新渲染
            clean_incomplete(out_file)
            
            chosen_master = random.choice(valid_masters)
            master_dur = get_audio_duration(chosen_master)

            effect_seed = "|".join(
                [
                    str(opts.target_date),
                    str(tag),
                    str(container if not opts.simple_mode else idx),
                    img.stem,
                    chosen_master.stem,
                ]
            )
            effect_rng = random.Random(effect_seed)
            effect_kwargs = build_effect_kwargs(opts, rng=effect_rng)
            if effect_kwargs.get("bass_pulse"):
                bpm_value, phase_value = detect_audio_bpm_profile(str(chosen_master))
                effect_kwargs["bass_pulse_bpm"] = bpm_value
                effect_kwargs["bass_pulse_phase"] = phase_value
            filter_str, effect_desc, extra_inputs = get_effect(master_dur, rng=effect_rng, **effect_kwargs)
            
            tag_video_jobs.append({
                "tag": tag,
                "img": img,
                "audio": chosen_master,
                "out": out_file,
                "filter": filter_str,
                "desc": effect_desc,
                "effect_kwargs": effect_kwargs,
                "extra_inputs": extra_inputs
            })
        
        if tag_skip_count > 0:
            print(f"  ♻️ {tag_skip_count} 个视频已存在")
        if tag_video_jobs:
            print(f"  📝 {len(tag_video_jobs)} 个视频待渲染")
        elif tag_skip_count > 0:
            print(f"  ✅ 所有视频已存在，无需渲染")
        
        # 并行渲染该 Tag 的所有视频
        tag_success = 0
        if tag_video_jobs:
            vid_start = time.time()
            progress_stop = threading.Event()
            progress_state = {"done": 0}

            def progress_heartbeat() -> None:
                while not progress_stop.wait(25):
                    elapsed_minutes = (time.time() - vid_start) / 60
                    print(
                        f"  ⏱️ {tag} 渲染中: {progress_state['done']}/{len(tag_video_jobs)} 已完成, "
                        f"已运行 {elapsed_minutes:.1f} 分钟"
                    )
             
            with ThreadPoolExecutor(max_workers=VIDEO_WORKERS) as executor:
                futures = {
                    executor.submit(render_video_task, j['tag'], j['img'], j['audio'], j['out'], j['filter'], extra_inputs=j.get('extra_inputs')): j
                    for j in tag_video_jobs
                }
                heartbeat_thread = threading.Thread(target=progress_heartbeat, daemon=True)
                heartbeat_thread.start()
                 
                for i, future in enumerate(as_completed(futures)):
                    task = futures[future]
                    try:
                        res = future.result()
                    except Exception as e:
                        res = {"success": False, "error": str(e)}
                    
                    prefix = f"[{i+1}/{len(tag_video_jobs)}]"
                    container = extract_container(task['img'].name)
                    progress_state["done"] = i + 1
                    if res['success']:
                        tag_success += 1
                        total_success += 1
                        fx = task.get('effect_kwargs', {})
                        print(
                            f"  {prefix} ✅ {tag}/{container} | {task['desc']} | "
                            f"particle={fx.get('particle')} opacity={fx.get('particle_opacity')} "
                            f"speed={fx.get('particle_speed')} pulse={'on' if fx.get('bass_pulse') else 'off'} "
                            f"bpm={fx.get('bass_pulse_bpm', '-')} | {res['time']:.0f}s"
                        )
                    else:
                        print(f"  {prefix} ❌ {tag}/{container}: {res.get('error')}")
                progress_stop.set()
                heartbeat_thread.join(timeout=1)
            
            total_rendered += len(tag_video_jobs)
            vid_time = time.time() - vid_start
            print(f"  📊 {tag} 渲染完成: {tag_success}/{len(tag_video_jobs)} 成功 (耗时 {vid_time/60:.1f}分钟)")
        
        # ========== 渲染完毕，立即启动上传 (Pipeline 模式) ==========
        has_videos_to_upload = tag_success > 0 or tag_skip_count > 0  # 有新渲染的或已存在的
        
        if pipeline_mode and has_videos_to_upload and not opts.simple_mode:
            upload_log = out_dir / "upload.log"
            print(f"  🚀 启动后台上传: {tag} → {upload_log.name}")
            
            try:
                if opts.upload_window_plan_file:
                    plan_path = Path(opts.upload_window_plan_file)
                    window_plan = json.loads(plan_path.read_text(encoding="utf-8"))
                    prepared = prepare_window_task_upload_batch(
                        script_dir=_SCRIPT_DIR,
                        scheduler_config_path=_SCRIPT_DIR / "scheduler_config.json",
                        prompt_studio_path=_SCRIPT_DIR / "config" / "prompt_studio.json",
                        channel_mapping_path=_SCRIPT_DIR / "config" / "channel_mapping.json",
                        window_plan=window_plan,
                        date_value=opts.target_date,
                        source_video_dir=out_dir,
                        thumbnail_dir=None,
                        metadata_mode=opts.upload_metadata_mode,
                        fill_title_desc_tags=bool(opts.upload_fill_text),
                        fill_thumbnails=bool(opts.upload_fill_thumbnails),
                        sync_daily_content=bool(opts.upload_sync_daily_content),
                    )
                    print(f"  [manifest] rebuilt channels={prepared.get('assigned_count', 0)}")
                    for warning in prepared.get("warnings", []):
                        print(f"  [manifest] {warning}")
                log_f = open(upload_log, 'w', encoding='utf-8')
                upload_cmd = [
                    sys.executable,
                    str(UPLOAD_SCRIPT),
                    "--tag", tag,
                    "--date", f"{int(opts.target_date[:2])}.{int(opts.target_date[2:]):02d}",  # MMDD → M.DD
                    "--auto-confirm"
                ]
                if opts.upload_auto_close_browser:
                    upload_cmd.append("--auto-close-browser")
                if opts.upload_skip_channels:
                    upload_cmd.append("--skip-channels=" + ",".join(str(x) for x in opts.upload_skip_channels))
                if opts.upload_window_plan_file:
                    upload_cmd.append("--window-plan-file=" + opts.upload_window_plan_file)
                upload_proc = subprocess.Popen(
                    upload_cmd,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    cwd=str(UPLOAD_SCRIPT.parent),
                )
                upload_procs.append((tag, upload_proc, upload_log, log_f))
                print(f"  📤 上传进程 PID={upload_proc.pid} 已启动")
            except Exception as e:
                print(f"  ❌ 启动上传进程失败: {e}")
        elif pipeline_mode and opts.simple_mode:
            print(f"  ⏭️  简单模式不支持自动上传")
            
    return total_success, total_rendered, upload_procs


def phase4_wait_uploads(upload_procs: list):
    """Phase 4: 等待所有上传完成"""
    if upload_procs:
        print(f"\n{'📤' * 20}")
        print(f"  Phase 4: 等待 {len(upload_procs)} 个上传进程完成")
        print(f"{'📤' * 20}")
        
        for tag, proc, log_path, log_f in upload_procs:
            print(f"  ⏳ 等待 {tag} 上传 (PID={proc.pid})...")
            proc.wait()
            log_f.close()
            
            if proc.returncode == 0:
                print(f"  ✅ {tag} 上传完成")
            else:
                print(f"  ❌ {tag} 上传异常 (exit code: {proc.returncode})")
                print(f"     查看日志: {log_path}")


def save_render_history(opts: RenderOptions, active_projects: list, total_rendered: int, total_success: int, total_time: float):
    """保存渲染历史供 GUI 显示"""
    try:
        history = []
        if HISTORY_FILE.exists():
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                raw_text = f.read().strip()
            if raw_text:
                loaded = json.loads(raw_text)
                if isinstance(loaded, list):
                    history = loaded
                elif isinstance(loaded, dict):
                    maybe_history = loaded.get("history")
                    history = maybe_history if isinstance(maybe_history, list) else []
            else:
                history = []
        
        history.append({
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date": opts.target_date,
            "tag_count": len(active_projects),
            "tags": [p['tag'] for p in active_projects],
            "video_count": total_rendered,
            "success_count": total_success,
            "audio_workers": AUDIO_WORKERS,
            "video_workers": VIDEO_WORKERS,
            "total_minutes": round(total_time / 60, 1),
            "fx_randomize": opts.fx_randomize,
            "fx_spectrum": opts.fx_spectrum,
            "fx_timeline": opts.fx_timeline,
            "fx_letterbox": opts.fx_letterbox,
            "fx_zoom": opts.fx_zoom,
            "fx_color_spectrum": opts.fx_color_spectrum,
            "fx_color_timeline": opts.fx_color_timeline,
            "fx_spectrum_y": opts.fx_spectrum_y,
            "fx_style": opts.fx_style,
        })
        
        # 只保留最近 50 条
        history = history[-50:]
        
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        print(f"  📊 渲染记录已保存 ({len(history)} 条)")
    except Exception as e:
        print(f"  ⚠️ 保存渲染记录失败: {e}")


def deep_clean_old_images():
    """清理 7 天前的旧底图和封面"""
    print(f"\n  🧹 深度清理: 删除 7 天前的底图和封面...")
    cutoff = datetime.now() - timedelta(days=7)
    old_count = 0
    for tag_dir in BASE_IMAGE_DIR.iterdir():
        if not tag_dir.is_dir():
            continue
        for img in tag_dir.glob("*.png"):
            # 从文件名提取日期 (MMDD_xxx.png)
            match = re.match(r'(\d{4})_', img.name)
            if match:
                try:
                    img_date = datetime.strptime(f"2026{match.group(1)}", "%Y%m%d")
                    if img_date < cutoff:
                        img.unlink()
                        old_count += 1
                except Exception as e:
                    logger.warning(f"清理旧图片失败 {img.name}: {e}")
    if old_count:
        print(f"  🧹 清理了 {old_count} 张旧图片")
    else:
        print(f"  🧹 没有找到 7 天前的旧底图。")


def phase1_scan_resources(opts: RenderOptions, all_tags: list) -> list:
    """Phase 1: 扫描资源 (Base Image & Music)"""
    print(f"\n{'🔍' * 20}")
    print(f"  Phase 1: 扫描资源 (Base Image & Music)")
    print(f"{'🔍' * 20}")
    
    active_projects = [] # [{'tag', 'images':[], 'music':[], 'out_dir':Path}]
    target_date = opts.target_date
    simple_mode = opts.simple_mode
    flat_image_root_hint = len(all_tags) == 1 and _dir_has_direct_files(BASE_IMAGE_DIR, ('.png', '.jpg', '.jpeg', '.mp4'))
    flat_music_root_hint = len(all_tags) == 1 and _dir_has_direct_files(MUSIC_DIR, ('.mp3',))
    
    for i, tag in enumerate(all_tags, 1):
        if simple_mode:
            images = find_images_simple(tag)
        else:
            images = find_images_by_date(tag, target_date)
            
        music = find_all_music(tag)
        
        status_icon = "⚪"
        status_msg = "无底图"
        
        if images:
            if not music:
                status_icon = "⚠️"
                status_msg = "有底图但无音乐 (跳过)"
            else:
                status_icon = "✅"
                status_msg = f"{len(images)} 个视频任务 | {len(music)} 首可用音乐"
                
                out_dir = OUTPUT_ROOT / f"{target_date}_{tag}"
                out_dir.mkdir(parents=True, exist_ok=True)
                
                active_projects.append({
                    "tag": tag,
                    "images": images,
                    "music": music,
                    "out_dir": out_dir
                })
        
        print(f"  {i:2d}. {tag:<15s} {status_icon} {status_msg}")
        tag_image_dir = BASE_IMAGE_DIR / tag
        tag_music_dir = MUSIC_DIR / tag
        if not tag_image_dir.exists() and flat_image_root_hint:
            print(f"      ↳ 检测到底图放在根目录：{BASE_IMAGE_DIR}")
            print(f"        当前标准模式只会扫描：{tag_image_dir}")
        elif not simple_mode and tag_image_dir.exists() and not images:
            raw_images = [f for f in tag_image_dir.iterdir() if f.is_file() and f.suffix.lower() in ['.png', '.jpg', '.jpeg'] and 'cover' not in f.stem.lower()]
            if raw_images:
                print(f"      ↳ 检测到未按日期命名的底图，例如：{raw_images[0].name}")
                print(f"        当前标准模式要求：{target_date}_90.png / {target_date}_91.jpg 这种格式")
        if not tag_music_dir.exists() and flat_music_root_hint:
            print(f"      ↳ 检测到音乐放在根目录：{MUSIC_DIR}")
            print(f"        当前会扫描：{tag_music_dir}")
        
        # ========== 库存检查 (从 base image 目录 + generation_map.json) ==========
        try:
            # 从 generation_map.json 读取库存 (这是最准确的来源)
            map_file = BASE_IMAGE_DIR / tag / "generation_map.json"
            if map_file.exists():
                with open(map_file, 'r', encoding='utf-8') as f:
                    gen_map = json.load(f)
                
                tag_dir = BASE_IMAGE_DIR / tag
                inventory_strs = []
                
                # 分析当前底图覆盖情况
                present_containers = set()
                if images:
                    for img in images:
                        c_id = extract_container(img.name)
                        if c_id: present_containers.add(c_id)
                
                for container_str, ch_info in gen_map.get("channels", {}).items():
                    container = int(container_str)
                    days = ch_info.get("days", {})
                    
                    # 统计: 有多少天的素材已生成
                    days_with_cover = 0
                    days_with_base = 0
                    days_with_title = 0
                    
                    for day_str, day_info in days.items():
                        # 检查封面文件是否实际存在
                        covers = day_info.get("covers", [])
                        if covers:
                            # 检查第一个封面文件是否存在 (代表这天的封面已生成)
                            first_cover = tag_dir / covers[0]
                            if first_cover.exists():
                                days_with_cover += 1
                        
                        # 检查底图
                        base_img = tag_dir / f"{day_str}_{container}.png"
                        if base_img.exists():
                            days_with_base += 1
                        
                        # 检查标题
                        if day_info.get("title"):
                            days_with_title += 1
                    
                    total_days = len(days)
                    
                    # 底图状态 (今天)
                    has_base_today = container in present_containers
                    b_mark = "✅" if has_base_today else "❌"
                    
                    # 封面状态 (所有天)
                    c_mark = "✅" if days_with_cover >= total_days else ("⚠️" if days_with_cover > 0 else "❌")
                    
                    # 标题状态 (所有天)
                    t_mark = "✅" if days_with_title >= total_days else ("⚠️" if days_with_title > 0 else "❌")
                    
                    item_str = f"{container}:{b_mark}底 {c_mark}封面{days_with_cover}/{total_days} {t_mark}标题{days_with_title}/{total_days}"
                    inventory_strs.append(item_str)
                
                if inventory_strs:
                    print(f"      📦 库存: {'  '.join(inventory_strs)}")
                                    
        except Exception as e:
            print(f"      ❌ 库存检查出错: {e}")
        
    if not active_projects:
        print("\n❌ 没有待处理的任务。请先生成今天日期的底图。")
        
    return active_projects

def main():
    global AUDIO_WORKERS, VIDEO_WORKERS
    
    print("\n" + "🎬" * 25)
    print("    中央调度系统 V3 (多母带并行版)")
    print("🎬" * 25)
    print(f"    并行设置: 音频 x{AUDIO_WORKERS} | 视频 x{VIDEO_WORKERS}")
    print(f"    母带设置: 每个母带 {SONG_COUNT} 首歌")
    
    # 1. 初始化
    config = load_config()
    all_tags = get_all_tags(config)
    
    # 简单模式下如果没有 config，从底图文件夹名自动发现 tag
    if not all_tags and IS_WINDOWS:
        if BASE_IMAGE_DIR.exists():
            all_tags = [d.name for d in sorted(BASE_IMAGE_DIR.iterdir()) 
                        if d.is_dir() and not d.name.startswith('.')]
            if all_tags:
                print(f"\\n📂 从底图文件夹自动发现 {len(all_tags)} 个标签:")
                for t in all_tags:
                    print(f"   • {t}")
        if not all_tags:
            print("❌ 底图文件夹里没有任何子文件夹")
            print(f"   请在 {BASE_IMAGE_DIR} 下创建标签文件夹（如「大提琴」）并放入图片")
            return

    opts = parse_arguments()
    if not opts:
        return

    if opts.selected_tags:
        requested_tags = []
        seen = set()
        for tag in opts.selected_tags:
            if tag not in seen:
                requested_tags.append(tag)
                seen.add(tag)
        missing_tags = [tag for tag in requested_tags if tag not in all_tags]
        all_tags = [tag for tag in requested_tags if tag in all_tags]
        if missing_tags:
            print(f"⚠️ 指定标签不在 upload_config 中，已跳过: {', '.join(missing_tags)}")
        if not all_tags:
            print("❌ 指定标签均不可用，退出")
            return
    
    target_date = opts.target_date
    simple_mode = opts.simple_mode
    
    # 显示当前设置
    print(f"🎛️  效果设置:")
    print("   视觉策略: 🎛️ 高级视觉配置（只有选成 random 的字段会按每个视频单独随机）")
    print(f"   频谱: {'✅ 开' if opts.fx_spectrum else '❌ 关'}  |  时间轴: {'✅ 开' if opts.fx_timeline else '❌ 关'}  |  黑边: {'✅ 开' if opts.fx_letterbox else '❌ 关'}")
    print(f"   缩放: {opts.fx_zoom}  |  频谱色: {opts.fx_color_spectrum}  |  时间轴色: {opts.fx_color_timeline}")
    print(f"   样式: {opts.fx_style}  |  频谱Y: {opts.fx_spectrum_y}")
    print(f"   可选: --color=X (同时设置两种颜色)  --color-spectrum=X  --color-timeline=X  --style=bar/wave/circular  --spectrum-y=530")
    if opts.selected_tags:
        print(f"🏷️  指定标签: {', '.join(all_tags)}")
    print(f"🪟 上传后浏览器: {'自动关闭' if opts.upload_auto_close_browser else '保留打开'}")
    if opts.upload_skip_channels:
        print(f"⏭️  跳过频道: {', '.join(str(x) for x in opts.upload_skip_channels)}")
    
    if opts.target_date and not opts.simple_mode:
        print(f"📅 指定日期: {target_date}")
    else:
        print(f"📅 使用今天日期: {target_date}")
        print(f"   (若需指定日期，请运行 python3 daily_scheduler.py 0215)")

    # 1.5 先清理旧文件腾出磁盘空间
    phase0_cleanup(target_date)
    
    # 2. 扫描环境资源
    active_projects = phase1_scan_resources(opts, all_tags)
    if not active_projects:
        return

    if '--dry-run' in sys.argv:
        print("\n🧪 Dry-run: 资源扫描完成，跳过母带合成、渲染和上传")
        return

    # 3. 准备音频生成任务
    master_map = phase2_build_master_audios(active_projects)

    # ============ Pipeline 模式检测 ============
    pipeline_mode = not opts.render_only
    if pipeline_mode and UPLOAD_SCRIPT.exists():
        print(f"\n  🔗 流水线模式: 渲染完一个 Tag 即自动上传")
        print(f"     上传脚本: {UPLOAD_SCRIPT.name}")
    elif pipeline_mode and not UPLOAD_SCRIPT.exists():
        print(f"\n  ⚠️ 上传脚本不存在: {UPLOAD_SCRIPT}")
        print(f"     回退到纯渲染模式")
        pipeline_mode = False
    else:
        print(f"\n  📦 纯渲染模式 (--render-only)")
        pipeline_mode = False
    
    # 4. 按 tag 循环: 写 manifest → 渲染 → 上传
    global_start = time.time()
    
    total_success, total_rendered, upload_procs = phase3_render_and_upload(
        active_projects, master_map, opts, pipeline_mode
    )
    
    # ========== Phase 4: 等待所有上传完成 ==========
    phase4_wait_uploads(upload_procs)
    
    total_time = time.time() - global_start
    
    # 总结
    upload_summary = ""
    upload_ok = sum(1 for _, proc, _, _ in upload_procs if proc.returncode == 0)
    if upload_procs:
        upload_summary = f" | 上传: {upload_ok}/{len(upload_procs)} 完成"
    
    print(f"\n  🏁 全部完成！ 渲染: {total_success}/{total_rendered} 成功{upload_summary}  总耗时: {total_time/60:.1f} 分钟")
    
    # === 保存渲染历史 (供 GUI 显示) ===
    save_render_history(opts, active_projects, total_rendered, total_success, total_time)
    
    # 7. 清理 7 天前的旧底图和封面
    if '--deep-clean' in sys.argv:
        deep_clean_old_images()
    
    # 最终总结
    print(f"\n{'🎉' * 20}")
    if upload_procs:
        print(f"  全部完成！ 渲染 + 上传流水线")
        print(f"  📊 渲染: {total_success}/{total_rendered} 成功")
        print(f"  📤 上传: {upload_ok}/{len(upload_procs)} 完成")
        if upload_ok < len(upload_procs):
            print(f"  ⚠️ 以下 Tag 上传有异常:")
            for tag, proc, log_path, _ in upload_procs:
                if proc.returncode != 0:
                    print(f"     - {tag}: exit code {proc.returncode} → {log_path}")
    else:
        print(f"  全部完成！")
        
    print(f"  输出目录: {OUTPUT_ROOT}")
    if not upload_procs:
        open_path_in_file_manager(OUTPUT_ROOT)

if __name__ == "__main__":
    main()
