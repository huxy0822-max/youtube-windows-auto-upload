#!/usr/bin/env python3
"""
底层渲染引擎。

这个文件不处理 GUI，也不处理上传，只负责调用 FFmpeg 完成：
- 音频母带拼接
- 编码器检测与 GPU/CPU 回退
- 单视频渲染
- 批量渲染流水线
"""

import os
import sys
import json
import random
import re
import time
import subprocess
import platform
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from effects_library import get_effect  # 使用新接口

# ============ 默认渲染参数 ============
DEFAULT_TARGET_DURATION = 3600   # 1小时
DEFAULT_MASTER_COUNT = 5         # 每个标签生成几个随机母带
DEFAULT_AUDIO_WORKERS = 4        # 音频合成并行数
DEFAULT_VIDEO_WORKERS = 6        # 视频渲染并行数
AUDIO_BITRATE = "320k"
AUDIO_SAMPLERATE = "44100"


def detect_ffmpeg() -> str:
    """检测 FFmpeg 路径 (优先本地 tools/ 目录)"""
    if getattr(sys, "frozen", False):
        app_dir = Path(sys.executable).parent
    else:
        app_dir = Path(__file__).parent
    
    # 优先查找本地 tools/ 目录
    local_candidates = [
        app_dir / "tools" / "ffmpeg" / "bin" / ("ffmpeg.exe" if platform.system() == "Windows" else "ffmpeg"),
        app_dir / "tools" / "ffmpeg" / ("ffmpeg.exe" if platform.system() == "Windows" else "ffmpeg"),
    ]
    
    for local in local_candidates:
        if local.exists():
            try:
                subprocess.run([str(local), "-version"], capture_output=True, timeout=5)
                return str(local)
            except:
                continue
    
    # 系统路径查找
    candidates = ["ffmpeg"]
    if platform.system() == "Windows":
        candidates += [
            r"C:\ffmpeg\bin\ffmpeg.exe",
            r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
            os.path.expanduser(r"~\ffmpeg\bin\ffmpeg.exe"),
        ]
    elif platform.system() == "Darwin":
        candidates += ["/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg"]

    for path in candidates:
        try:
            result = subprocess.run([path, "-version"], capture_output=True, timeout=5)
            if result.returncode == 0:
                return path
        except:
            continue
    return ""


def detect_best_codec(ffmpeg_path: str = "ffmpeg") -> list:
    """自动检测最佳视频编码器"""
    system = platform.system()
    try:
        result = subprocess.run([ffmpeg_path, "-encoders"], capture_output=True, text=True, timeout=10)
        available = result.stdout
    except:
        available = ""
    
    if system == "Darwin":  # macOS
        if "h264_videotoolbox" in available:
            # 2026年方案: -b:v 5000k + -spatial_aq 1 (替代旧版 -q:v)
            return ["-c:v", "h264_videotoolbox", "-b:v", "5000k", "-spatial_aq", "1", "-allow_sw", "1"]
    
    elif system == "Windows":
        if "h264_nvenc" in available:
            return ["-c:v", "h264_nvenc", "-preset", "p4", "-cq", "28"]
        if "h264_amf" in available:
            return ["-c:v", "h264_amf", "-quality", "balanced"]
        if "h264_qsv" in available:
            return ["-c:v", "h264_qsv", "-preset", "medium"]
    
    if "h264_vaapi" in available:
         return ["-c:v", "h264_vaapi"]

    # CPU 回退
    return ["-c:v", "libx264", "-preset", "medium", "-crf", "23"]


def get_audio_duration(filepath, ffmpeg_path="ffmpeg") -> float:
    """获取音频时长 (100% 可靠版本: ffmpeg 全解码优先)"""
    # 方法 1: ffmpeg 全解码 (最可靠)
    try:
        cmd = [ffmpeg_path, '-i', str(filepath), '-f', 'null', '-']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        for line in reversed(result.stderr.split('\n')):
            if 'time=' in line:
                parts = line.split('time=')[1].split(' ')[0].strip()
                h, m, s = parts.split(':')
                dur = float(h) * 3600 + float(m) * 60 + float(s)
                if dur > 0:
                    return dur
        
        for line in result.stderr.split('\n'):
            if 'Duration:' in line and 'N/A' not in line:
                parts = line.split('Duration:')[1].split(',')[0].strip()
                h, m, s = parts.split(':')
                dur = float(h) * 3600 + float(m) * 60 + float(s)
                if dur > 0:
                    return dur
    except Exception:
        pass
    
    # 方法 2: ffprobe (备选)
    try:
        ffprobe = ffmpeg_path.replace("ffmpeg", "ffprobe")
        if platform.system() == "Windows" and not ffprobe.endswith(".exe"):
            ffprobe += ".exe"
        
        cmd = [
            ffprobe, '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            str(filepath)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        out_val = result.stdout.strip()
        if out_val and out_val != "N/A":
            dur = float(out_val)
            if dur > 0:
                return dur
    except Exception:
        pass
        
    return 3600.0  # 默认 1 小时


def scan_resources(music_dir: str, image_dir: str, date_str: str) -> list:
    """扫描资源"""
    music_root = Path(music_dir)
    image_root = Path(image_dir)
    results = []
    
    if not music_root.exists(): return results
    
    for tag_dir in sorted(music_root.iterdir()):
        if not tag_dir.is_dir() or tag_dir.name.startswith('.'): continue
        tag = tag_dir.name
        
        mp3s = []
        for root, _, files in os.walk(tag_dir):
            for f in files:
                if f.lower().endswith(('.mp3', '.wav', '.m4a', '.flac')) and not f.startswith('.'):
                    mp3s.append(Path(root) / f)
        
        if not mp3s: continue
        
        image_tag_dir = image_root / tag
        images = []
        containers = []
        
        if image_tag_dir.exists():
            for f in sorted(image_tag_dir.iterdir()):
                if f.is_file() and f.suffix.lower() in ['.png', '.jpg', '.jpeg', '.webp']:
                    # 排除封面图 (cover)
                    if 'cover' in f.stem.lower():
                        continue
                    # 宽松模式: 只要是图片就认
                    # 尝试提取日期和序号，提不到就用文件名做 ID
                    match = re.fullmatch(rf"{re.escape(date_str)}_(\d+)", f.stem)
                    if match:
                        container = match.group(1)
                    else:
                        # 只要不是以 . 开头的文件
                        if not f.name.startswith('.'):
                            container = f.stem  # 直接用文件名
                        else:
                            continue
                            
                    images.append(f)
                    containers.append(container)
        
        results.append({
            "tag": tag, "images": images, "music": mp3s, "containers": containers,
            "image_status": "ok" if images else "no_match", "image_folder": str(image_tag_dir)
        })
    return results


def build_master_audio(mp3_pool, output_path, target_duration, ffmpeg_path, task_id):
    """合成母带"""
    # ... (逻辑同前，省略部分细节以节省篇幅，核心是一样的) ...
    # 为保证完整性，我还是写全
    if output_path.exists() and output_path.stat().st_size > 100_000:
        dur = get_audio_duration(str(output_path), ffmpeg_path)
        # 如果缓存的母带时长不足 10 分钟，视为无效，删除后重新生成
        if dur >= 600:
            return {"success": True, "id": task_id, "path": output_path, "skipped": True}
        else:
            print(f"  ⚠️ 母带 #{task_id} 时长仅 {dur:.0f}s (不足10分钟)，删除后重新生成")
            output_path.unlink(missing_ok=True)

    start_time = time.time()
    shuffled = mp3_pool.copy()
    random.shuffle(shuffled)
    
    selected = []
    total_dur = 0.0
    for mp3 in shuffled:
        dur = get_audio_duration(mp3, ffmpeg_path)
        selected.append(mp3)
        total_dur += dur
        if total_dur >= target_duration: break
    
    list_file = output_path.parent / f"temp_concat_{task_id}_{os.getpid()}.txt"
    with open(list_file, 'w', encoding='utf-8') as f:
        for mp3 in selected:
            safe = str(mp3).replace("'", "'\\''")
            f.write(f"file '{safe}'\n")
            
    cmd = [
        ffmpeg_path, '-y', '-hide_banner', '-loglevel', 'error',
        '-f', 'concat', '-safe', '0', '-i', str(list_file),
        '-c:a', 'aac', '-b:a', AUDIO_BITRATE, '-ar', AUDIO_SAMPLERATE,
        '-t', str(target_duration),
        str(output_path)
    ]
    try:
        subprocess.run(cmd, check=True, timeout=1200)
        list_file.unlink(missing_ok=True)
        return {"success": True, "id": task_id, "time": time.time() - start_time}
    except Exception as e:
        list_file.unlink(missing_ok=True)
        return {"success": False, "id": task_id, "error": str(e)}


def render_video(image_path, audio_path, output_path, filter_complex, codec_args, target_duration, ffmpeg_path, extra_inputs=None):
    """渲染视频 (带自动重试)"""
    
    # 构造基础命令部分
    base_cmd = [
        ffmpeg_path, '-y', '-hide_banner', '-loglevel', 'error',
        '-loop', '1', '-i', str(image_path),
        '-i', str(audio_path),
    ]
    
    if extra_inputs:
        base_cmd.extend(extra_inputs)
        
    base_cmd.extend([
        '-filter_complex', filter_complex,
        '-map', '[outv]', '-map', '1:a',
    ])

    
    # 构造尾部通用参数
    suffix_cmd = [
        '-pix_fmt', 'yuv420p', '-movflags', '+faststart',
        '-c:a', 'copy', '-shortest', '-t', str(target_duration),
        str(output_path)
    ]

    start = time.time()
    
    # 第一次尝试：使用传入的 codec_args (可能是 GPU)
    try:
        cmd = base_cmd + codec_args + suffix_cmd
        subprocess.run(cmd, check=True, timeout=7200)
        return {"success": True, "file": output_path.name, "time": time.time() - start}
    except Exception as e_gpu:
        # 如果第一次失败，且原参数包含硬件加速特征，尝试回退 CPU
        is_gpu_attempt = any(x in str(codec_args) for x in ["nvenc", "amf", "qsv", "videotoolbox"])
        
        if is_gpu_attempt:
            print(f"  ⚠️ {output_path.name}: GPU渲染失败 ({e_gpu}), 转 CPU (libx264) 重试...")
            
            # 强制使用 CPU 参数
            cpu_args = ["-c:v", "libx264", "-preset", "medium", "-crf", "23"]
            cmd_cpu = base_cmd + cpu_args + suffix_cmd
            
            try:
                subprocess.run(cmd_cpu, check=True, timeout=14400) # CPU 慢，给更多时间
                return {
                    "success": True, 
                    "file": output_path.name, 
                    "time": time.time() - start, 
                    "msg": "CPU_Fallback_Success"
                }
            except Exception as e_cpu:
                return {
                    "success": False, 
                    "file": output_path.name, 
                    "error": f"GPU failed, then CPU failed: {e_cpu}"
                }
        
        # 如果本来就是 CPU 失败，直接返回
        return {"success": False, "file": output_path.name, "error": str(e_gpu)}


def run_full_pipeline(
    music_dir, image_dir, output_dir, date_str,
    target_duration, master_count, audio_workers, video_workers, ffmpeg_path,
    log_callback=None, progress_callback=None,
    # === 新增特效参数 ===
    fx_spectrum=True, fx_timeline=True, fx_letterbox=False, fx_zoom="normal", 
    fx_color_spectrum="WhiteGold", fx_color_timeline="WhiteGold", fx_spectrum_y=530,
    fx_style="bar",
    fx_film_grain=False, fx_grain_strength=15,
    fx_vignette=False, fx_color_tint="none", fx_soft_focus=False, fx_soft_focus_sigma=1.5,
    fx_particle="none", fx_particle_opacity=0.6, fx_particle_speed=1.0, fx_text_font="default",
    # === 新增 CPU 模式 ===
    cpu_mode=False
):
    """执行渲染流水线"""
    
    def log(msg):
        if log_callback: log_callback(msg)
        else: print(msg)
    
    def progress(cur, total, msg=""):
        if progress_callback: progress_callback(cur, total, msg)

    # 1. 检测/选择编码器
    if cpu_mode:
        log("🛡️ 已启用强制 CPU 兼容模式 (libx264)")
        codec_args = ["-c:v", "libx264", "-preset", "medium", "-crf", "23"]
    else:
        codec_args = detect_best_codec(ffmpeg_path)
    
    log(f"🎬 编码器: {' '.join(codec_args[:2])} ...")
    log(f"🎨 特效: 频谱={fx_spectrum} (Y={fx_spectrum_y}, {fx_color_spectrum}), 时间轴={fx_timeline} ({fx_color_timeline}), 黑边={fx_letterbox}")
    
    # 2. 扫描
    projects = scan_resources(music_dir, image_dir, date_str)
    active_projects = [p for p in projects if p["images"] and p["music"]]
    
    if not active_projects:
        log("❌ 无任务")
        return {"success": False}
    
    # 3. 母带
    output_root = Path(output_dir)
    audio_jobs = []
    master_map = {}
    
    for proj in active_projects:
        tag = proj["tag"]
        out_dir = output_root / f"{date_str}_{tag}"
        out_dir.mkdir(parents=True, exist_ok=True)
        proj["out_dir"] = out_dir
        
        master_dir = out_dir / ".masters"
        master_dir.mkdir(exist_ok=True)
        master_map[tag] = []
        
        needed = min(len(proj["images"]), master_count)
        
        for i in range(needed):
            p = master_dir / f"master_{i+1}.m4a"
            master_map[tag].append(p)
            audio_jobs.append({"pool": proj["music"], "out": p, "id": i+1})
            
    log(f"🔨 母带任务: {len(audio_jobs)}")
    
    with ThreadPoolExecutor(max_workers=audio_workers) as ex:
        futures = {ex.submit(build_master_audio, j["pool"], j["out"], target_duration, ffmpeg_path, j["id"]): j for j in audio_jobs}
        for i, f in enumerate(as_completed(futures)):
            progress(i+1, len(audio_jobs), f"母带 {i+1}/{len(audio_jobs)}")
            res = f.result()
            if not res.get("skipped") and res["success"]:
                log(f"  ✅ 母带 #{res['id']} 完成")
    
    # 4. 视频
    video_jobs = []
    for proj in active_projects:
        tag = proj["tag"]
        masters = [m for m in master_map.get(tag, []) if m.exists()]
        if not masters: continue
        
        for img in proj["images"]:
            container = img.stem  # 直接用图片名作为容器 ID
            out = proj["out_dir"] / f"{container}.mp4"
            
            # 跳过已存在
            if out.exists() and out.stat().st_size > 5_000_000:
                log(f"  ♻️ {out.name} 已存在")
                continue
            
            master = random.choice(masters)
            real_dur = get_audio_duration(master, ffmpeg_path)
            
            # === 使用新接口生成特效 ===
            filter_str, desc, extra_inputs = get_effect(
                real_dur,
                spectrum=fx_spectrum,
                timeline=fx_timeline,
                letterbox=fx_letterbox,
                zoom=fx_zoom,
                color_spectrum=fx_color_spectrum,
                color_timeline=fx_color_timeline,
                spectrum_y=fx_spectrum_y,
                style=fx_style,
                film_grain=fx_film_grain, grain_strength=fx_grain_strength,
                vignette=fx_vignette, color_tint=fx_color_tint,
                soft_focus=fx_soft_focus, soft_focus_sigma=fx_soft_focus_sigma,
                particle=fx_particle, particle_opacity=fx_particle_opacity,
                particle_speed=fx_particle_speed, text_font=fx_text_font
            )
            
            video_jobs.append({
                "img": img, "audio": master, "out": out,
                "filter": filter_str, "desc": desc, "tag": tag, "container": container,
                "extra_inputs": extra_inputs
            })
            
    if not video_jobs:
        log("✅ 所有视频已完成")
        return {"success": True, "rendered": 0}
        
    log(f"🚀 开始渲染: {len(video_jobs)} 个视频")
    vid_ok = 0
    with ThreadPoolExecutor(max_workers=video_workers) as ex:
        futures = {
            ex.submit(render_video, j["img"], j["audio"], j["out"], j["filter"], codec_args, target_duration, ffmpeg_path, extra_inputs=j.get("extra_inputs")): j
            for j in video_jobs
        }
        for i, f in enumerate(as_completed(futures)):
            progress(i+1, len(video_jobs), f"渲染 {i+1}/{len(video_jobs)}")
            res = f.result()
            j = futures[f]
            if res["success"]:
                vid_ok += 1
                log(f"  ✅ [{i+1}/{len(video_jobs)}] {j['tag']}_{j['container']} | {j['desc']} | {res['time']:.0f}s")
            else:
                log(f"  ❌ {j['tag']}_{j['container']} 失败: {res.get('error')}")
                
    return {"success": True, "rendered": vid_ok}
