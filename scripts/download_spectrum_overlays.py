#!/usr/bin/env python3
"""
download_spectrum_overlays.py
=============================
下载免费音频频谱/可视化叠加素材 (MOV/MP4)

支持的来源：
  - Pixabay (通过 API, 需要免费 API key)
  - Pexels (通过 API, 需要免费 API key)
  - Mixkit (直接下载)
  - Videezy (手动下载链接)
  - Vecteezy (手动下载链接)

用法:
  python download_spectrum_overlays.py [--pixabay-key KEY] [--pexels-key KEY] [--output-dir DIR]

如果没有提供 API key，脚本会跳过对应平台的自动下载，
但仍然会生成一份包含所有手动下载链接的报告文件。
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path
from typing import Optional


# ─── 默认输出目录 ───
DEFAULT_OUTPUT_DIR = Path(r"C:\youtube自动化-claude优化版本\assets\spectrum_overlays")

# ─── 搜索关键词 ───
SEARCH_QUERIES = [
    "audio spectrum",
    "audio visualizer",
    "equalizer",
    "music spectrum",
    "sound wave",
    "audio equalizer",
]

# ─── 手动下载资源清单 ───
MANUAL_RESOURCES = {
    # ── Pixabay 免费视频 (无需登录，浏览器直接下载) ──
    "Pixabay - Audio Spectrum (各种风格)": [
        {
            "title": "Audio Spectrum 频谱可视化",
            "url": "https://pixabay.com/videos/search/audio%20spectrum/",
            "note": "461+ 免费 4K/HD 视频, 黑色背景, 可直接用于 FFmpeg 叠加 (blend mode)",
        },
        {
            "title": "Audio Visualizer 音频可视化",
            "url": "https://pixabay.com/videos/search/audio%20visualizer/",
            "note": "3181+ 免费视频, 多种风格",
        },
        {
            "title": "Equalizer 均衡器动画",
            "url": "https://pixabay.com/videos/search/equalizer/",
            "note": "69+ 免费视频, 条形均衡器风格",
        },
        {
            "title": "Music Equalizer 音乐均衡器",
            "url": "https://pixabay.com/videos/search/music%20equalizer/",
            "note": "992+ 免费视频",
        },
        {
            "title": "Green Screen Audio Spectrum 绿幕频谱",
            "url": "https://pixabay.com/videos/search/green%20screen%20audio%20spectrum/",
            "note": "10929+ 绿幕视频, 可用 FFmpeg chromakey 去绿",
        },
        {
            "title": "Audio Spectrum #163355",
            "url": "https://pixabay.com/videos/audio-spectrum-spectrum-163355/",
            "note": "具体视频 - 频谱可视化动画",
        },
        {
            "title": "Audio Spectrum Visualiser #156055",
            "url": "https://pixabay.com/videos/audio-spectrum-visualiser-156055/",
            "note": "具体视频 - 频谱可视化",
        },
        {
            "title": "Audio Visualizer Music #153124",
            "url": "https://pixabay.com/videos/audio-audio-visualizer-music-153124/",
            "note": "具体视频 - 音频可视化",
        },
        {
            "title": "Music Visualizer #151003",
            "url": "https://pixabay.com/videos/music-visualizer-audio-visualizer-151003/",
            "note": "具体视频 - 音乐可视化",
        },
    ],

    # ── Pexels 免费视频 ──
    "Pexels - Audio Visualizer (免费 4K 视频)": [
        {
            "title": "Audio Visualizer 搜索页",
            "url": "https://www.pexels.com/search/videos/audio%20visualizer/",
            "note": "1610+ 免费 4K/HD 视频, 可直接下载",
        },
        {
            "title": "Equalizer 搜索页",
            "url": "https://www.pexels.com/search/videos/equalizer/",
            "note": "8301+ 免费均衡器视频",
        },
        {
            "title": "Graphic Equalizer 搜索页",
            "url": "https://www.pexels.com/search/videos/graphic%20equalizer/",
            "note": "4927+ 免费图形均衡器视频",
        },
        {
            "title": "Audio Sequence #6892732",
            "url": "https://www.pexels.com/video/tracking-shot-of-an-audio-sequence-6892732/",
            "note": "具体视频 - 音频序列追踪镜头",
        },
    ],

    # ── Mixkit 免费素材 ──
    "Mixkit - 免费视频素材 & AE 模板": [
        {
            "title": "Digital Equalizer Close Up",
            "url": "https://mixkit.co/free-stock-video/digital-equalizer-close-up-9320/",
            "note": "免费高清视频, 无需注册, Mixkit License",
        },
        {
            "title": "Digital Equalizer Playing a Song",
            "url": "https://mixkit.co/free-stock-video/digital-equalizer-playing-a-song-9370/",
            "note": "免费高清视频, 无需注册",
        },
        {
            "title": "After Effects 音频可视化模板 (9个)",
            "url": "https://mixkit.co/free-after-effects-templates/audio-visualizer/",
            "note": "免费 AE 模板, 包含 Modern/Retro/Gradient 等风格",
        },
        {
            "title": "Premiere Pro 音频可视化模板 (3个)",
            "url": "https://mixkit.co/free-premiere-pro-templates/audio-visualizer/",
            "note": "免费 PR 模板",
        },
    ],

    # ── Videezy 免费素材 ──
    "Videezy - 免费频谱视频": [
        {
            "title": "Audio Spectrum 视频合集",
            "url": "https://www.videezy.com/free-video/audio-spectrum",
            "note": "多种数字均衡器动画, MP4/MOV 格式",
        },
        {
            "title": "Audio Equalizer 视频合集",
            "url": "https://www.videezy.com/free-video/audio-equalizer",
            "note": "293 个免费音频均衡器视频",
        },
        {
            "title": "Spectrum 视频合集",
            "url": "https://www.videezy.com/free-video/spectrum",
            "note": "267 个免费频谱视频",
        },
    ],

    # ── Vecteezy 免费素材 ──
    "Vecteezy - 透明背景频谱视频": [
        {
            "title": "Audio Spectrum Transparent 透明背景",
            "url": "https://www.vecteezy.com/free-videos/audio-spectrum-transparent",
            "note": "276+ 透明背景频谱视频, 适合直接叠加",
        },
        {
            "title": "Sound Wave Transparent Background",
            "url": "https://www.vecteezy.com/free-videos/sound-wave-transparent-background",
            "note": "透明背景声波动画",
        },
        {
            "title": "Audio Spectrum Visualizer Green Screen",
            "url": "https://www.vecteezy.com/free-videos/audio-spectrum-visualizer-green-screen",
            "note": "256+ 绿幕频谱视频, 可用 chromakey 处理",
        },
        {
            "title": "Black Screen Audio Spectrum",
            "url": "https://www.vecteezy.com/free-videos/black-screen-audio-spectrum",
            "note": "黑色背景频谱, 可用 blend mode 叠加",
        },
    ],

    # ── 复古/Vintage 风格 ──
    "复古/Vintage 风格频谱模板": [
        {
            "title": "Vintage Audio Visualizer AE 模板 (Avnish Parker)",
            "url": "https://www.avnishparker.com/projects/vintage-audio-visualizer---after-effects-audio-spectrum-template-free-download",
            "note": "免费 AE 模板, 复古风格, 可商用",
        },
        {
            "title": "Digital Base Audio Visualizer - Mixkit AE 模板",
            "url": "https://mixkit.co/free-after-effects-templates/digital-base-audio-visualizer-621/",
            "note": "免费 AE 模板, 数字风格",
        },
    ],

    # ── GitHub 开源工具 (生成自定义频谱) ──
    "GitHub 开源频谱生成工具": [
        {
            "title": "audio-visualizer-python (生成频谱视频)",
            "url": "https://github.com/djfun/audio-visualizer-python",
            "note": "Python GUI 工具, 从音频生成可视化视频, 支持多种组件叠加",
        },
        {
            "title": "Wav2Bar (自定义音频可视化导出)",
            "url": "https://picorims.github.io/wav2bar-website/",
            "note": "免费开源, 自定义音频可视化并导出视频",
        },
        {
            "title": "awesome-audio-visualization (资源合集)",
            "url": "https://github.com/willianjusten/awesome-audio-visualization",
            "note": "音频可视化资源精选列表",
        },
        {
            "title": "SpectrumCpp (Windows 实时频谱叠加)",
            "url": "https://github.com/diqezit/SpectrumCpp",
            "note": "C++ 实时音频频谱可视化, 支持 overlay 模式",
        },
        {
            "title": "Audio Visualizer for OBS (HTML/JS)",
            "url": "https://github.com/wompmacho/audio-visualizer",
            "note": "OBS 浏览器源音频可视化叠加",
        },
        {
            "title": "IconScout - 免费均衡器 Lottie 动画",
            "url": "https://iconscout.com/free-lottie-animations/audio-equalizer",
            "note": "20+ 免费均衡器动画, GIF/MP4/Lottie JSON 格式",
        },
    ],
}


def download_file(url: str, dest_path: Path, headers: Optional[dict] = None) -> bool:
    """下载文件到指定路径"""
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)

        print(f"  下载中: {url[:80]}...")
        with urllib.request.urlopen(req, timeout=60) as response:
            content = response.read()
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(dest_path, "wb") as f:
                f.write(content)
            size_mb = len(content) / (1024 * 1024)
            print(f"  已保存: {dest_path} ({size_mb:.1f} MB)")
            return True
    except Exception as e:
        print(f"  下载失败: {e}")
        return False


def search_pixabay(api_key: str, output_dir: Path) -> list[dict]:
    """通过 Pixabay API 搜索并下载音频频谱视频"""
    results = []
    downloaded = 0
    max_downloads = 20  # 限制下载数量

    for query in SEARCH_QUERIES:
        if downloaded >= max_downloads:
            break

        params = urllib.parse.urlencode({
            "key": api_key,
            "q": query,
            "video_type": "animation",
            "per_page": 10,
            "safesearch": "true",
            "order": "popular",
        })
        url = f"https://pixabay.com/api/videos/?{params}"

        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "Mozilla/5.0")
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())

            hits = data.get("hits", [])
            print(f"\n  Pixabay [{query}]: 找到 {len(hits)} 个视频")

            for hit in hits:
                if downloaded >= max_downloads:
                    break

                video_id = hit.get("id", "unknown")
                tags = hit.get("tags", "")

                # 优先下载 medium 质量 (720p)
                videos = hit.get("videos", {})
                video_url = None
                quality = "medium"

                for q in ["medium", "small", "tiny"]:
                    v = videos.get(q, {})
                    if v.get("url"):
                        video_url = v["url"]
                        quality = q
                        break

                if not video_url:
                    continue

                filename = f"pixabay_{video_id}_{quality}.mp4"
                dest = output_dir / "pixabay" / filename

                info = {
                    "source": "pixabay",
                    "id": video_id,
                    "tags": tags,
                    "url": video_url,
                    "page_url": hit.get("pageURL", ""),
                    "quality": quality,
                    "filename": filename,
                }

                if dest.exists():
                    print(f"  跳过 (已存在): {filename}")
                    info["status"] = "already_exists"
                else:
                    success = download_file(video_url, dest)
                    info["status"] = "downloaded" if success else "failed"
                    if success:
                        downloaded += 1
                    time.sleep(0.5)  # 避免请求过快

                results.append(info)

        except Exception as e:
            print(f"  Pixabay API 错误 [{query}]: {e}")

    return results


def search_pexels(api_key: str, output_dir: Path) -> list[dict]:
    """通过 Pexels API 搜索并下载音频频谱视频"""
    results = []
    downloaded = 0
    max_downloads = 15

    for query in ["audio visualizer", "equalizer animation", "spectrum"]:
        if downloaded >= max_downloads:
            break

        params = urllib.parse.urlencode({
            "query": query,
            "per_page": 10,
            "orientation": "landscape",
        })
        url = f"https://api.pexels.com/videos/search?{params}"

        try:
            req = urllib.request.Request(url)
            req.add_header("Authorization", api_key)
            req.add_header("User-Agent", "Mozilla/5.0")
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())

            videos = data.get("videos", [])
            print(f"\n  Pexels [{query}]: 找到 {len(videos)} 个视频")

            for video in videos:
                if downloaded >= max_downloads:
                    break

                video_id = video.get("id", "unknown")

                # 获取 HD 或 SD 视频文件
                files = video.get("video_files", [])
                best_file = None
                for f in sorted(files, key=lambda x: x.get("height", 0)):
                    h = f.get("height", 0)
                    if 360 <= h <= 1080 and f.get("link"):
                        best_file = f
                        break

                if not best_file:
                    # 取任意可用的
                    for f in files:
                        if f.get("link"):
                            best_file = f
                            break

                if not best_file:
                    continue

                video_url = best_file["link"]
                height = best_file.get("height", "unknown")
                filename = f"pexels_{video_id}_{height}p.mp4"
                dest = output_dir / "pexels" / filename

                info = {
                    "source": "pexels",
                    "id": video_id,
                    "url": video_url,
                    "page_url": video.get("url", ""),
                    "quality": f"{height}p",
                    "filename": filename,
                }

                if dest.exists():
                    print(f"  跳过 (已存在): {filename}")
                    info["status"] = "already_exists"
                else:
                    success = download_file(video_url, dest)
                    info["status"] = "downloaded" if success else "failed"
                    if success:
                        downloaded += 1
                    time.sleep(0.5)

                results.append(info)

        except Exception as e:
            print(f"  Pexels API 错误 [{query}]: {e}")

    return results


def try_download_mixkit(output_dir: Path) -> list[dict]:
    """尝试下载 Mixkit 免费视频素材"""
    results = []
    # Mixkit 的直接下载 URL 模式
    mixkit_videos = [
        {
            "id": "9320",
            "title": "digital-equalizer-close-up",
            "url": "https://assets.mixkit.co/videos/9320/9320-720.mp4",
        },
        {
            "id": "9370",
            "title": "digital-equalizer-playing-a-song",
            "url": "https://assets.mixkit.co/videos/9370/9370-720.mp4",
        },
    ]

    for video in mixkit_videos:
        filename = f"mixkit_{video['title']}_{video['id']}.mp4"
        dest = output_dir / "mixkit" / filename

        info = {
            "source": "mixkit",
            "id": video["id"],
            "title": video["title"],
            "url": video["url"],
            "filename": filename,
        }

        if dest.exists():
            print(f"  跳过 (已存在): {filename}")
            info["status"] = "already_exists"
        else:
            success = download_file(video["url"], dest)
            info["status"] = "downloaded" if success else "failed"
            # 如果 720p 失败，尝试预览版本
            if not success:
                preview_url = f"https://assets.mixkit.co/videos/preview/{video['id']}/mixkit-{video['title']}-{video['id']}-small.mp4"
                info["url"] = preview_url
                success = download_file(preview_url, dest)
                info["status"] = "downloaded" if success else "failed"
            time.sleep(0.5)

        results.append(info)

    return results


def generate_report(output_dir: Path, download_results: list[dict]):
    """生成下载报告和手动下载指南"""
    report_path = output_dir / "下载报告_spectrum_overlays.md"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# 音频频谱/可视化叠加素材下载报告\n\n")
        f.write(f"生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # ── 自动下载结果 ──
        if download_results:
            f.write("## 自动下载结果\n\n")
            downloaded = [r for r in download_results if r.get("status") == "downloaded"]
            failed = [r for r in download_results if r.get("status") == "failed"]
            existed = [r for r in download_results if r.get("status") == "already_exists"]

            f.write(f"- 成功下载: {len(downloaded)} 个\n")
            f.write(f"- 已存在 (跳过): {len(existed)} 个\n")
            f.write(f"- 下载失败: {len(failed)} 个\n\n")

            if downloaded:
                f.write("### 已下载文件\n\n")
                for r in downloaded:
                    f.write(f"- `{r.get('filename', 'unknown')}` ({r['source']})\n")
                f.write("\n")

            if failed:
                f.write("### 下载失败 (需手动下载)\n\n")
                for r in failed:
                    f.write(f"- [{r.get('filename', 'unknown')}]({r.get('page_url', r.get('url', ''))})\n")
                f.write("\n")

        # ── 手动下载资源 ──
        f.write("## 手动下载资源汇总\n\n")
        f.write("以下资源需要在浏览器中手动下载：\n\n")

        for category, items in MANUAL_RESOURCES.items():
            f.write(f"### {category}\n\n")
            for item in items:
                f.write(f"**{item['title']}**\n")
                f.write(f"- 链接: {item['url']}\n")
                f.write(f"- 说明: {item['note']}\n\n")

        # ── FFmpeg 使用指南 ──
        f.write("## FFmpeg 叠加频谱素材使用指南\n\n")

        f.write("### 方法1: 黑色背景素材叠加 (blend mode)\n")
        f.write("```bash\n")
        f.write('ffmpeg -i background.mp4 -i spectrum_overlay.mp4 \\\n')
        f.write('  -filter_complex "[1:v]format=gbrp[ov];[0:v][ov]blend=all_mode=screen[out]" \\\n')
        f.write('  -map "[out]" -map 0:a output.mp4\n')
        f.write("```\n\n")

        f.write("### 方法2: 绿幕素材叠加 (chromakey)\n")
        f.write("```bash\n")
        f.write('ffmpeg -i background.mp4 -i green_screen_spectrum.mp4 \\\n')
        f.write('  -filter_complex "[1:v]chromakey=0x00FF00:0.3:0.1[ov];[0:v][ov]overlay=0:H-h[out]" \\\n')
        f.write('  -map "[out]" -map 0:a output.mp4\n')
        f.write("```\n\n")

        f.write("### 方法3: 透明背景 MOV 素材叠加\n")
        f.write("```bash\n")
        f.write('ffmpeg -i background.mp4 -i spectrum_alpha.mov \\\n')
        f.write('  -filter_complex "[0:v][1:v]overlay=0:H-h[out]" \\\n')
        f.write('  -map "[out]" -map 0:a output.mp4\n')
        f.write("```\n\n")

        f.write("### 方法4: 频谱放在视频底部\n")
        f.write("```bash\n")
        f.write('ffmpeg -i background.mp4 -i spectrum.mp4 \\\n')
        f.write('  -filter_complex "\\\n')
        f.write('    [1:v]scale=iw:200[ov]; \\\n')
        f.write('    [ov]format=gbrp[ov2]; \\\n')
        f.write('    [0:v][ov2]blend=all_mode=screen:shortest=1[out]" \\\n')
        f.write('  -map "[out]" -map 0:a -shortest output.mp4\n')
        f.write("```\n\n")

        f.write("### 推荐下载策略\n\n")
        f.write("1. **Pixabay** - 最推荐, 无需注册即可下载, 黑色背景素材最多\n")
        f.write("2. **Pexels** - 免费 4K 素材, 需注册\n")
        f.write("3. **Mixkit** - 高质量, 无需注册\n")
        f.write("4. **Vecteezy** - 有透明背景的素材, 需注册\n")
        f.write("5. **audio-visualizer-python** - 开源工具, 可从音频自动生成频谱视频\n\n")

    print(f"\n报告已保存: {report_path}")
    return report_path


def generate_ffmpeg_overlay_script(output_dir: Path):
    """生成一个辅助 FFmpeg 叠加脚本"""
    script_path = output_dir / "apply_spectrum_overlay.bat"
    with open(script_path, "w", encoding="utf-8") as f:
        f.write("@echo off\n")
        f.write("REM 使用方法: apply_spectrum_overlay.bat <背景视频> <频谱叠加素材> <输出文件>\n")
        f.write("REM 示例: apply_spectrum_overlay.bat background.mp4 pixabay\\pixabay_163355_medium.mp4 output.mp4\n\n")
        f.write("if \"%~3\"==\"\" (\n")
        f.write("    echo 用法: %~nx0 ^<背景视频^> ^<频谱叠加素材^> ^<输出文件^>\n")
        f.write("    exit /b 1\n)\n\n")
        f.write('ffmpeg -i "%~1" -i "%~2" ^\n')
        f.write('  -filter_complex "[1:v]format=gbrp[ov];[0:v][ov]blend=all_mode=screen[out]" ^\n')
        f.write('  -map "[out]" -map 0:a -shortest "%~3"\n')
        f.write("\necho 完成: %~3\n")
    print(f"FFmpeg 辅助脚本已保存: {script_path}")


def main():
    parser = argparse.ArgumentParser(
        description="下载免费音频频谱/可视化叠加素材"
    )
    parser.add_argument(
        "--pixabay-key",
        help="Pixabay API key (免费注册: https://pixabay.com/api/docs/)",
        default=os.environ.get("PIXABAY_API_KEY", ""),
    )
    parser.add_argument(
        "--pexels-key",
        help="Pexels API key (免费注册: https://www.pexels.com/api/)",
        default=os.environ.get("PEXELS_API_KEY", ""),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"输出目录 (默认: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="仅生成报告, 不下载文件",
    )
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("音频频谱/可视化叠加素材下载器")
    print("=" * 60)
    print(f"输出目录: {output_dir}\n")

    all_results = []

    if args.report_only:
        print("模式: 仅生成报告\n")
    else:
        # ── 1. Mixkit 直接下载 ──
        print("\n[1/3] 尝试下载 Mixkit 素材...")
        mixkit_results = try_download_mixkit(output_dir)
        all_results.extend(mixkit_results)

        # ── 2. Pixabay API ──
        if args.pixabay_key:
            print("\n[2/3] 通过 Pixabay API 搜索并下载...")
            pixabay_results = search_pixabay(args.pixabay_key, output_dir)
            all_results.extend(pixabay_results)
        else:
            print("\n[2/3] 跳过 Pixabay (未提供 API key)")
            print("  提示: 免费注册获取 API key: https://pixabay.com/api/docs/")

        # ── 3. Pexels API ──
        if args.pexels_key:
            print("\n[3/3] 通过 Pexels API 搜索并下载...")
            pexels_results = search_pexels(args.pexels_key, output_dir)
            all_results.extend(pexels_results)
        else:
            print("\n[3/3] 跳过 Pexels (未提供 API key)")
            print("  提示: 免费注册获取 API key: https://www.pexels.com/api/")

    # ── 生成报告 ──
    print("\n" + "=" * 60)
    print("生成下载报告...")
    report_path = generate_report(output_dir, all_results)
    generate_ffmpeg_overlay_script(output_dir)

    # ── 统计 ──
    downloaded = len([r for r in all_results if r.get("status") == "downloaded"])
    failed = len([r for r in all_results if r.get("status") == "failed"])
    existed = len([r for r in all_results if r.get("status") == "already_exists"])

    print("\n" + "=" * 60)
    print("下载完成!")
    print(f"  成功下载: {downloaded} 个文件")
    print(f"  已存在 (跳过): {existed} 个文件")
    print(f"  下载失败: {failed} 个文件")
    print(f"\n报告文件: {report_path}")
    print(f"输出目录: {output_dir}")

    if not args.pixabay_key or not args.pexels_key:
        print("\n提示: 使用 API key 可以自动下载更多素材:")
        if not args.pixabay_key:
            print(f"  --pixabay-key <key>  (注册: https://pixabay.com/api/docs/)")
        if not args.pexels_key:
            print(f"  --pexels-key <key>   (注册: https://www.pexels.com/api/)")

    print("\n手动下载推荐:")
    print("  1. Pixabay: https://pixabay.com/videos/search/audio%20spectrum/")
    print("  2. Pexels: https://www.pexels.com/search/videos/audio%20visualizer/")
    print("  3. Vecteezy: https://www.vecteezy.com/free-videos/audio-spectrum-transparent")
    print("  4. Videezy: https://www.videezy.com/free-video/audio-spectrum")


if __name__ == "__main__":
    main()
