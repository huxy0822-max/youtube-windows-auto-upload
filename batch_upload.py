#!/usr/bin/env python3
"""
YouTube 批量上传脚本
根据标签组自动上传视频到对应的频道

使用方法:
  python3 batch_upload.py --tag 大提琴 --date 1.28
  python3 batch_upload.py --tag 大提琴 --date 1.28 --dry-run  # 只预览不执行
"""

from __future__ import annotations

import asyncio
import argparse
import json
import os
import re
import sys
import time
import random
import subprocess
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import platform
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from browser_api import list_browser_envs, start_browser_debug_port, stop_browser_container
from metadata_service import archive_uploaded_metadata
from path_helpers import normalize_path, resolve_config_file
from upload_window_planner import (
    find_window_task,
    load_window_upload_plan,
    merge_manifest_with_window_task,
)

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
    SCRIPT_DIR = Path(sys.executable).parent
    _BUNDLE_DIR = Path(sys._MEIPASS)
else:
    SCRIPT_DIR = Path(__file__).parent
    _BUNDLE_DIR = SCRIPT_DIR

sys.path.insert(0, str(_BUNDLE_DIR))

# 导入共享配置模块
from utils import (
    parse_metadata as utils_parse_metadata,
    get_thumbnail_by_container,
    get_next_thumbnail_set,
    load_config as utils_load_config,
    get_inventory_status
)

# ============ 配置 ============
CONFIG_PATH = Path(os.environ.get("UPLOAD_CONFIG_PATH", str(resolve_config_file(SCRIPT_DIR, "upload_config.json"))))
CHANNEL_MAPPING_PATH = Path(os.environ.get("CHANNEL_MAPPING_PATH", str(resolve_config_file(SCRIPT_DIR, "channel_mapping.json"))))
# 兼容旧代码常量；实际 API 调用走 browser_api.py
HUBSTUDIO_API = "http://127.0.0.1:6873"
STUDIO_UI_LANGUAGE = "en"
STUDIO_UI_LOCATION = "US"
UPLOAD_MONITOR_POLL_SECONDS = 10
UPLOAD_SAFE_CLOSE_TIMEOUT_SECONDS = 2 * 60 * 60
UPLOAD_SAFE_CLOSE_STABLE_POLLS = 2
TAIL_CLOSE_WATCHER_TIMEOUT_SECONDS = 6 * 60 * 60
NETWORK_RECOVERY_RETRY_WAIT_SECONDS = 6
RETRYABLE_NETWORK_ERROR_MARKERS = (
    "ERR_CONNECTION_CLOSED",
    "ERR_CONNECTION_RESET",
    "ERR_NETWORK_CHANGED",
)
SUPPORTED_VIDEO_EXTENSIONS = (".mp4", ".mov", ".mkv", ".avi", ".m4v", ".webm")

# 详细上传记录目录 / 上传历史目录 (跨平台统一，支持环境变量覆盖)
UPLOAD_RECORDS_DIR = normalize_path(os.environ.get("UPLOAD_RECORDS_DIR", "upload_records"), SCRIPT_DIR)
UPLOAD_HISTORY_PATH = normalize_path(os.environ.get("UPLOAD_HISTORY_PATH", "data/upload_history.json"), SCRIPT_DIR)
UPLOAD_STAGING_ROOT = Path(tempfile.gettempdir()) / "youtube_auto_upload_staging"

# ============ 播放列表名称映射 (方案三: 自动生成) ============
# 特殊映射: tag → 播放列表名称 (如果不在这里, 自动生成 "超好聽的{tag}音樂")
TAG_TO_PLAYLIST = {
    # === 3 保留赛道 ===
    "大提琴": "超好聽的大提琴音樂",
    "竖琴": "超好聽的豎琴音樂",
    "古典吉他": "超好聽的古典吉他音樂",
    # === 9 新赛道 (2026-03-09 更新) ===
    "Lo-Fi嘻哈": "超好聽的Lo-Fi嘻哈音樂",
    "Motown": "超好聽的Motown音樂",
    "三角洲藍調": "超好聽的三角洲藍調音樂",
    "威士忌藍調": "超好聽的威士忌藍調音樂",
    "巴洛克音樂": "超好聽的巴洛克音樂",
    "爵士鋼琴": "超好聽的爵士鋼琴音樂",
    "薩克斯風": "超好聽的薩克斯風音樂",
    "雷鬼": "超好聽的雷鬼音樂",
    "非洲節拍": "超好聽的非洲節拍音樂",
}
HUBSTUDIO_TAG_ALIASES = {
    # HubStudio live 分组名的兼容映射；仓库内部仍使用规范名。
    "Lo-Fi嘻哈": "LoFi嘻哈",
}


def stage_upload_assets(
    *,
    video_path: Path,
    thumbnails: List[Path],
    serial: int,
) -> tuple[Path, List[Path], Path]:
    """复制上传素材到纯 ASCII 临时目录，避免浏览器读取 Unicode 路径失败。"""
    stage_dir = UPLOAD_STAGING_ROOT / f"serial_{int(serial)}" / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    stage_dir.mkdir(parents=True, exist_ok=True)

    staged_video = stage_dir / video_path.name
    shutil.copy2(video_path, staged_video)

    staged_thumbnails: List[Path] = []
    for thumb in thumbnails or []:
        staged_thumb = stage_dir / thumb.name
        shutil.copy2(thumb, staged_thumb)
        staged_thumbnails.append(staged_thumb)

    return staged_video, staged_thumbnails, stage_dir


def get_hubstudio_tag_name(tag: str) -> str:
    return HUBSTUDIO_TAG_ALIASES.get(tag, tag)


def _normalize_tag_for_match(tag: str) -> str:
    text = str(tag or "").strip().replace(" ", "")
    return text.translate(
        str.maketrans(
            {
                "風": "风",
                "樂": "乐",
                "薩": "萨",
                "館": "馆",
                "臺": "台",
                "牆": "墙",
                "試": "试",
                "藍": "蓝",
                "龍": "龙",
            }
        )
    )


def _get_tag_config(config: Dict, tag: str, default: Optional[Dict] = None) -> Dict:
    tag_to_project = config.get("tag_to_project", {}) or {}
    direct = tag_to_project.get(tag)
    if isinstance(direct, dict):
        return direct
    wanted = _normalize_tag_for_match(tag)
    for raw_tag, raw_value in tag_to_project.items():
        if _normalize_tag_for_match(raw_tag) == wanted and isinstance(raw_value, dict):
            return raw_value
    return dict(default or {})


def _iter_window_plan_source_dirs(window_plan: Optional[Dict[str, Any]], tag: str) -> List[Path]:
    if not isinstance(window_plan, dict):
        return []
    wanted = _normalize_tag_for_match(tag)
    seen: set[str] = set()
    paths: list[Path] = []
    for task in window_plan.get("tasks", []) or []:
        if _normalize_tag_for_match(task.get("tag") or "") != wanted:
            continue
        folder_text = str(task.get("source_dir") or "").strip()
        if not folder_text:
            continue
        key = folder_text.lower()
        if key in seen:
            continue
        seen.add(key)
        paths.append(Path(folder_text))
    return paths


def _count_matching_output_videos(folder: Path, date_key: str) -> int:
    if not folder.exists() or not folder.is_dir():
        return 0
    count = 0
    for item in folder.iterdir():
        if not item.is_file() or item.suffix.lower() not in SUPPORTED_VIDEO_EXTENSIONS:
            continue
        if re.match(rf"^{re.escape(date_key)}_(\\d+)\\.[^.]+$", item.name):
            count += 1
    return count


def _resolve_plan_output_dir(window_plan: Optional[Dict[str, Any]], tag: str, date_key: str) -> Optional[Path]:
    if not isinstance(window_plan, dict):
        return None

    plan_tag_output_dirs = window_plan.get("tag_output_dirs", {}) if isinstance(window_plan, dict) else {}
    tag_output_dir_text = str(plan_tag_output_dirs.get(tag) or "").strip()
    if not tag_output_dir_text and isinstance(plan_tag_output_dirs, dict):
        wanted_tag_key = _normalize_tag_for_match(tag)
        for raw_tag, raw_folder in plan_tag_output_dirs.items():
            if _normalize_tag_for_match(raw_tag) == wanted_tag_key:
                tag_output_dir_text = str(raw_folder or "").strip()
                break
    if tag_output_dir_text:
        candidate = Path(tag_output_dir_text)
        if candidate.exists():
            return candidate

    wanted = _normalize_tag_for_match(tag)
    candidates: list[tuple[int, Path]] = []
    for source_dir in _iter_window_plan_source_dirs(window_plan, tag):
        possible_dirs = [source_dir]
        if source_dir.exists() and source_dir.is_dir():
            possible_dirs.extend(item for item in source_dir.iterdir() if item.is_dir())
        for folder in possible_dirs:
            if not folder.exists() or not folder.is_dir():
                continue
            score = 0
            if (folder / "upload_manifest.json").exists():
                score += 100
            match_count = _count_matching_output_videos(folder, date_key)
            if match_count:
                score += match_count * 10
            normalized_name = _normalize_tag_for_match(folder.name)
            if wanted and wanted in normalized_name:
                score += 25
            if folder.name.startswith(f"{date_key}_"):
                score += 20
            if score > 0:
                candidates.append((score, folder))

    if not candidates:
        return None
    candidates.sort(key=lambda item: (-item[0], str(item[1]).lower()))
    return candidates[0][1]


def _iter_window_plan_tasks(window_plan: Optional[Dict[str, Any]], tag: str) -> List[Dict[str, Any]]:
    if not isinstance(window_plan, dict):
        return []
    wanted = _normalize_tag_for_match(tag)
    tasks: List[Dict[str, Any]] = []
    for task in window_plan.get("tasks", []) or []:
        if _normalize_tag_for_match(task.get("tag") or "") != wanted:
            continue
        try:
            serial = int(task.get("serial") or 0)
        except (TypeError, ValueError):
            continue
        if serial <= 0:
            continue
        row = dict(task)
        row["serial"] = serial
        tasks.append(row)
    return tasks


def _resolve_manifest_media_path(folder: Path, value: Any) -> Optional[Path]:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else (folder / path)


def _resolve_video_for_serial(
    *,
    serial: int,
    manifest_channel: Optional[Dict[str, Any]],
    videos: List[Path],
    manifest_dir: Path,
) -> Optional[Path]:
    if isinstance(manifest_channel, dict):
        explicit = _resolve_manifest_media_path(manifest_dir, manifest_channel.get("video"))
        if explicit and explicit.exists():
            return explicit

    for video in videos:
        match = re.match(r"\d{4}_(\d+)\.[^.]+$", video.name)
        if match and int(match.group(1)) == serial:
            return video
    return None


def _resolve_container_for_serial(
    *,
    serial: int,
    tag: str,
    plan_task: Optional[Dict[str, Any]],
    manifest_channel: Optional[Dict[str, Any]],
    live_container: Optional[Dict[str, Any]],
    mapping_registry: Dict[int, Dict[str, Any]],
    serial_to_channel_name: Dict[int, str],
) -> Dict[str, Any]:
    container = dict(live_container or {})
    if not container and serial in mapping_registry:
        container = dict(mapping_registry[serial])

    container_code = str(
        (plan_task or {}).get("container_code")
        or (manifest_channel or {}).get("container_code")
        or container.get("containerCode")
        or ""
    ).strip()
    channel_name = str(
        (manifest_channel or {}).get("channel_name")
        or (plan_task or {}).get("channel_name")
        or container.get("name")
        or serial_to_channel_name.get(serial)
        or ""
    ).strip()

    if not container:
        container = {
            "serialNumber": serial,
            "tag": tag,
            "tagName": tag,
            "remark": "",
        }
    else:
        container.setdefault("serialNumber", serial)
        container.setdefault("tag", tag)
        container.setdefault("tagName", container.get("tag", tag))
        container.setdefault("remark", "")

    if container_code:
        container["containerCode"] = container_code
    if channel_name:
        container["name"] = channel_name
    return container


def _task_is_ypp(
    *,
    serial: int,
    plan_task: Optional[Dict[str, Any]],
    manifest_channel: Optional[Dict[str, Any]],
    live_container: Optional[Dict[str, Any]],
) -> bool:
    if isinstance(plan_task, dict) and "is_ypp" in plan_task:
        return bool(plan_task.get("is_ypp"))
    if isinstance(manifest_channel, dict) and "is_ypp" in manifest_channel:
        return bool(manifest_channel.get("is_ypp"))
    remark = str((live_container or {}).get("remark") or "")
    return "YPP" in remark.upper()

def get_playlist_name(tag: str) -> str:
    """根据标签获取播放列表名称"""
    if tag in TAG_TO_PLAYLIST:
        return TAG_TO_PLAYLIST[tag]
    # 默认生成
    return f"超好聽的{tag}音樂"

# ============ 日志 ============
def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {"INFO": "ℹ️", "OK": "✅", "ERR": "❌", "WARN": "⚠️", "ACT": "🖱️", "WAIT": "⏳"}
    icon = icons.get(level, "")
    print(f"[{timestamp}] {icon} {msg}")


def append_upload_history(history_path: Path, record: Dict[str, Any]) -> bool:
    """并发追加 upload_history.json，避免多进程互相覆盖。"""
    history_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(history_path, "a+", encoding="utf-8") as f:
            if IS_MAC:
                import fcntl

                fcntl.flock(f.fileno(), fcntl.LOCK_EX)

            f.seek(0)
            raw = f.read().strip()
            if raw:
                try:
                    history_data = json.loads(raw)
                except Exception:
                    history_data = {"history": []}
            else:
                history_data = {"history": []}

            if not isinstance(history_data, dict):
                history_data = {"history": []}
            if not isinstance(history_data.get("history"), list):
                history_data["history"] = []

            history_data["history"].append(record)

            f.seek(0)
            f.truncate()
            json.dump(history_data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

            if IS_MAC:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        return True
    except Exception as e:
        log(f"⚠️ 追加 upload_history 失败: {e}", "WARN")
        return False


def make_upload_result(
    success: bool,
    close_browser: bool,
    reason: str,
    stage: str,
    monitor: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """统一上传结果，避免外层仅凭布尔值误关浏览器。"""
    result = {
        "success": success,
        "close_browser": close_browser,
        "reason": reason,
        "stage": stage,
        "monitor": monitor or {},
    }
    if extra:
        result.update(extra)
    return result


def make_batch_result(
    total: int,
    success_count: int,
    failed_count: int,
    pending_count: int = 0,
) -> Dict[str, int]:
    """统一批次结果，供退出码和调度器汇总使用。"""
    return {
        "total": total,
        "success_count": success_count,
        "failed_count": failed_count,
        "pending_count": pending_count,
    }


def with_studio_locale(url: Optional[str] = None) -> str:
    """统一为 YouTube Studio 注入稳定的英文界面参数。"""
    target = url or "https://studio.youtube.com/"
    parsed = urlparse(target)

    if not parsed.scheme:
        parsed = urlparse(f"https://studio.youtube.com{target}")

    if "studio.youtube.com" not in (parsed.netloc or "studio.youtube.com"):
        parsed = urlparse("https://studio.youtube.com/")

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["hl"] = STUDIO_UI_LANGUAGE
    query["gl"] = STUDIO_UI_LOCATION

    normalized = parsed._replace(
        scheme=parsed.scheme or "https",
        netloc=parsed.netloc or "studio.youtube.com",
        path=parsed.path or "/",
        query=urlencode(query),
    )
    return urlunparse(normalized)


def build_direct_upload_url(current_url: Optional[str] = None) -> str:
    """从当前 Studio URL 推导直达上传页。"""
    localized = with_studio_locale(current_url)
    parsed = urlparse(localized)
    channel_match = re.search(r"(/channel/[^/?#]+)", parsed.path or "")
    upload_path = f"{channel_match.group(1)}/videos/upload" if channel_match else "/"

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["d"] = "ud"

    return urlunparse(parsed._replace(path=upload_path, query=urlencode(query)))

# ============ 人性化交互 ============
async def random_delay(min_s=0.5, max_s=1.5, msg=""):
    """人性化随机延迟"""
    delay = random.uniform(min_s, max_s)
    if msg:
        log(f"{msg} ({delay:.1f}s)", "WAIT")
    await asyncio.sleep(delay)

async def clear_blocking_overlays(page, reason: str = ""):
    """清理会拦截点击的 YouTube overlay/backdrop。"""
    try:
        removed = await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                let count = 0;
                document.querySelectorAll("tp-yt-iron-overlay-backdrop").forEach((el) => {
                    if (visible(el) || el.hasAttribute("opened")) {
                        el.style.display = "none";
                        el.style.pointerEvents = "none";
                        count += 1;
                    }
                });
                return count;
            }
            """
        )
        if removed:
            suffix = f" ({reason})" if reason else ""
            log(f"已清理遮罩层 {removed} 个{suffix}", "WARN")
    except Exception:
        pass

async def human_click(page, locator, desc=""):
    """人性化点击 - 随机位置点击"""
    log(f"点击: {desc}", "ACT")
    try:
        await clear_blocking_overlays(page, "pre-click")
        await locator.wait_for(state="visible", timeout=15000)
        box = await locator.bounding_box()
        if box:
            x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
            y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
            await page.mouse.move(x, y)
            await asyncio.sleep(0.15)
            await page.mouse.click(x, y)
        else:
            await locator.click()
        return True
    except Exception as e:
        if "intercepts pointer events" in str(e) or "overlay-backdrop" in str(e):
            try:
                await clear_blocking_overlays(page, "click-retry")
                await locator.click(force=True, timeout=5000)
                return True
            except Exception:
                pass
        log(f"点击失败: {e}", "WARN")
        return False


async def wait_for_upload_details_ready(page, timeout_ms: int = 25000) -> bool:
    """等待上传详情页真正出现。"""
    selectors = [
        "ytcp-social-suggestions-textbox#title-textarea div#textbox",
        "#title-textarea #textbox",
        "ytcp-uploads-dialog ytcp-social-suggestions-textbox div#textbox",
        "ytcp-uploads-dialog #textbox[contenteditable='true']",
        "ytcp-uploads-dialog textarea",
        "ytcp-uploads-dialog input[type='text']",
        "ytcp-video-metadata-editor #title-textarea #textbox",
        "ytcp-video-metadata-editor-sidepanel ytcp-video-metadata-visibility",
        "ytcp-button#save",
        "#save",
    ]
    deadline = time.monotonic() + max(1, timeout_ms) / 1000.0

    while time.monotonic() < deadline:
        for sel in selectors:
            try:
                locator = page.locator(sel).first
                if await locator.count() > 0 and await locator.is_visible():
                    return True
            except Exception:
                continue

        try:
            ready = await page.evaluate(
                """
                () => {
                    const dlg = document.querySelector('ytcp-uploads-dialog');
                    if (dlg) {
                        const text = (dlg.innerText || '').trim();
                        if (/Details|詳情|详细信息|詳細資料/i.test(text)) return true;
                    }
                    const href = window.location.href || '';
                    const bodyText = (document.body && document.body.innerText) ? document.body.innerText : '';
                    const editPage = /studio\\.youtube\\.com\\/video\\//i.test(href);
                    if (!editPage) return false;
                    return (
                        !!document.querySelector('ytcp-video-metadata-visibility') ||
                        !!document.querySelector('ytcp-button#save, #save') ||
                        /Video details|视频详细信息|影片詳細資料|視頻詳細資料/i.test(bodyText)
                    );
                }
                """
            )
            if ready:
                return True
        except Exception:
            pass

        await asyncio.sleep(1)

    return False


async def is_phone_verification_required_for_upload(page) -> bool:
    """识别长视频上传前要求手机验证的账号权限提示。"""
    try:
        return await page.evaluate(
            """
            () => {
                const text = (document.body && document.body.innerText) ? document.body.innerText : '';
                return /upload videos longer than 15 minutes|verify your phone number|上傳超過 15 分鐘的影片|上传超过 15 分钟的视频|驗證你的手機號碼|验证你的手机号码/i.test(text);
            }
            """
        )
    except Exception:
        return False


async def open_direct_upload_page(page) -> bool:
    """Create 按钮缺失时，直接跳到上传页兜底。"""
    target_url = build_direct_upload_url(page.url)
    try:
        log(f"Create 不可用，改走直达上传页: {target_url}", "WARN")
        await page.goto(target_url, wait_until="domcontentloaded", timeout=90000)
        await asyncio.sleep(5)
        file_input = page.locator("input[type='file']").first
        if await file_input.count() > 0:
            return True
        return await wait_for_upload_details_ready(page, timeout_ms=8000)
    except Exception as e:
        log(f"直达上传页兜底失败: {e}", "WARN")
        return False


async def ensure_upload_radio_selected(
    page,
    radio_name: str,
    description: str,
    max_attempts: int = 4,
    allow_dom_fallback: bool = True,
) -> bool:
    """点击上传弹窗中的 radio，并在继续前确认它真的处于选中态。"""

    radio_locator = page.locator(f"tp-yt-paper-radio-button[name='{radio_name}']")

    def _fallback_patterns() -> tuple[list[str], list[str]]:
        normalized = str(radio_name or "").strip().upper()
        mapping: dict[str, tuple[list[str], list[str]]] = {
            "VIDEO_HAS_ALTERED_CONTENT_YES": (
                [r"altered\s+content", r"synthetic", r"ai\s*content", r"合成内容", r"ai内容", r"ai內容"],
                [r"(?:^|\b)yes(?:\b|$)", r"是"],
            ),
            "VIDEO_HAS_ALTERED_CONTENT_NO": (
                [r"altered\s+content", r"synthetic", r"ai\s*content", r"合成内容", r"ai内容", r"ai內容"],
                [r"(?:^|\b)no(?:\b|$)", r"否"],
            ),
            "VIDEO_MADE_FOR_KIDS_MFK": (
                [r"made\s+for\s+kids", r"儿童", r"兒童", r"kid"],
                [r"(?:^|\b)yes(?:\b|$)", r"是"],
            ),
            "VIDEO_MADE_FOR_KIDS_NOT_MFK": (
                [r"made\s+for\s+kids", r"儿童", r"兒童", r"kid"],
                [r"(?:^|\b)no(?:\b|$)", r"否"],
            ),
            "PUBLIC": (
                [r"visibility", r"可见度", r"可見度", r"谁可以观看", r"who\s+can\s+watch"],
                [r"(?:^|\b)public(?:\b|$)", r"(?:^|\b)everyone(?:\b|$)", r"公开", r"公開"],
            ),
            "PRIVATE": (
                [r"visibility", r"可见度", r"可見度", r"谁可以观看", r"who\s+can\s+watch"],
                [r"(?:^|\b)private(?:\b|$)", r"私密"],
            ),
            "UNLISTED": (
                [r"visibility", r"可见度", r"可見度", r"谁可以观看", r"who\s+can\s+watch"],
                [r"(?:^|\b)unlisted(?:\b|$)", r"不公开", r"不公開"],
            ),
            "SCHEDULE": (
                [r"visibility", r"可见度", r"可見度", r"谁可以观看", r"who\s+can\s+watch"],
                [r"(?:^|\b)schedule(?:d)?(?:\b|$)", r"定时", r"定時", r"排程"],
            ),
        }
        return mapping.get(normalized, ([], []))

    async def _dom_locate_and_maybe_click(
        section_patterns: list[str],
        option_patterns: list[str],
        *,
        do_click: bool,
    ) -> Dict[str, Any]:
        if not section_patterns or not option_patterns:
            return {"found": False, "selected": False, "clicked": False, "label": "", "context": ""}
        try:
            return await page.evaluate(
                """
                ({ sectionPatterns, optionPatterns, doClick }) => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                    const attrOf = (el) => [
                        el?.getAttribute?.("aria-label") || "",
                        el?.getAttribute?.("label") || "",
                        el?.getAttribute?.("name") || "",
                        el?.getAttribute?.("title") || "",
                        el?.id || "",
                    ].join(" ").trim();
                    const sectionRe = new RegExp(sectionPatterns.join("|"), "i");
                    const optionRe = new RegExp(optionPatterns.join("|"), "i");
                    const roots = [document];
                    const seenRoots = new Set([document]);
                    const seenHosts = new Set();

                    const isSelected = (el) => {
                        if (!el) return false;
                        if (el instanceof HTMLInputElement && el.type === "radio") {
                            return !!el.checked;
                        }
                        const nodes = [el, el.querySelector?.("[role='radio']"), el.querySelector?.("input[type='radio']")].filter(Boolean);
                        return nodes.some((node) =>
                            node.getAttribute?.("aria-checked") === "true" ||
                            node.hasAttribute?.("checked") ||
                            node.classList?.contains?.("checked") ||
                            node.checked === true
                        );
                    };

                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement)) return false;
                        try {
                            el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
                        } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };

                    const candidates = [];
                    while (roots.length) {
                        const root = roots.shift();
                        const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                        for (const node of allNodes) {
                            if (node && node.shadowRoot && !seenRoots.has(node.shadowRoot)) {
                                seenRoots.add(node.shadowRoot);
                                roots.push(node.shadowRoot);
                            }
                        }
                        const radios = root.querySelectorAll
                            ? root.querySelectorAll("tp-yt-paper-radio-button, [role='radio'], input[type='radio']")
                            : [];
                        for (const raw of radios) {
                            const host =
                                raw.closest?.("tp-yt-paper-radio-button") ||
                                raw.closest?.("[role='radio']") ||
                                raw;
                            if (!host || seenHosts.has(host)) continue;
                            seenHosts.add(host);
                            if (!visible(host) && !visible(raw)) continue;

                            const labelText = [
                                textOf(host.querySelector?.("#radioLabel")),
                                textOf(host.querySelector?.("label")),
                                textOf(host),
                                textOf(host.nextElementSibling),
                                textOf(host.previousElementSibling),
                                attrOf(host),
                                attrOf(raw),
                            ].join(" ").trim();

                            let context = labelText;
                            let current = host.parentElement;
                            for (let depth = 0; current && depth < 6; depth += 1, current = current.parentElement) {
                                context += " " + textOf(current) + " " + attrOf(current);
                            }
                            if (!sectionRe.test(context)) continue;
                            if (!optionRe.test(labelText) && !optionRe.test(context)) continue;

                            const score =
                                (optionRe.test(labelText) ? 10 : 0) +
                                (sectionRe.test(labelText) ? 4 : 0) +
                                (sectionRe.test(context) ? 3 : 0);
                            candidates.push({ host, raw, labelText, context, score });
                        }
                    }

                    if (!candidates.length) {
                        return { found: false, selected: false, clicked: false, label: "", context: "" };
                    }

                    candidates.sort((a, b) => b.score - a.score);
                    const best = candidates[0];
                    let selected = isSelected(best.host) || isSelected(best.raw);
                    let clicked = false;
                    if (!selected && doClick) {
                        const targets = [
                            best.host.querySelector?.("[role='radio']"),
                            best.host.querySelector?.("#radioContainer"),
                            best.host.querySelector?.("#radioLabel"),
                            best.host.querySelector?.("label"),
                            best.host,
                            best.raw,
                        ].filter(Boolean);
                        for (const target of targets) {
                            if (clickEl(target)) {
                                clicked = true;
                                break;
                            }
                        }
                        selected = isSelected(best.host) || isSelected(best.raw);
                    }

                    return {
                        found: true,
                        selected,
                        clicked,
                        label: (best.labelText || "").trim(),
                        context: (best.context || "").trim().slice(0, 240),
                    };
                }
                """,
                {
                    "sectionPatterns": section_patterns,
                    "optionPatterns": option_patterns,
                    "doClick": do_click,
                },
            )
        except Exception:
            return {"found": False, "selected": False, "clicked": False, "label": "", "context": ""}

    async def _is_selected(single_radio) -> bool:
        try:
            return bool(
                await single_radio.evaluate(
                    """
                    (radio) => {
                        if (!radio) return false;
                        const roleRadio = radio.querySelector('[role="radio"]');
                        return (
                            radio.getAttribute('aria-checked') === 'true' ||
                            radio.hasAttribute('checked') ||
                            radio.classList.contains('checked') ||
                            (roleRadio && (
                                roleRadio.getAttribute('aria-checked') === 'true' ||
                                roleRadio.hasAttribute('checked') ||
                                roleRadio.classList.contains('checked')
                            ))
                        );
                    }
                    """
                )
            )
        except Exception:
            return False

    section_patterns, option_patterns = _fallback_patterns()

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"radio-{radio_name}-{attempt}")
        try:
            radio_count = await radio_locator.count()
        except Exception:
            radio_count = 0

        if radio_count == 0:
            log(f"{description} 检查#{attempt}: 未找到 radio(name={radio_name})", "INFO")
            if allow_dom_fallback:
                dom_state = await _dom_locate_and_maybe_click(section_patterns, option_patterns, do_click=False)
                if dom_state.get("selected"):
                    if attempt > 1:
                        log(f"{description} 已确认选中 (dom_probe:{dom_state.get('label', '')})", "OK")
                    return True
                dom_clicked = await _dom_locate_and_maybe_click(section_patterns, option_patterns, do_click=True)
                if dom_clicked.get("clicked"):
                    log(f"{description} DOM 兜底点击: {dom_clicked.get('label', '')}", "INFO")
                    await asyncio.sleep(0.9)
                    dom_verify = await _dom_locate_and_maybe_click(section_patterns, option_patterns, do_click=False)
                    if dom_verify.get("selected"):
                        log(f"{description} 已确认选中 (dom_verify:{dom_verify.get('label', '')})", "OK")
                        return True
            await asyncio.sleep(0.7)
            continue

        visible_indices: List[int] = []
        for idx in range(min(radio_count, 6)):
            try:
                radio = radio_locator.nth(idx)
                if await radio.is_visible():
                    visible_indices.append(idx)
            except Exception:
                continue
        if not visible_indices:
            visible_indices = [0]

        # 先检查当前是否已选中
        selected_before_click = False
        for idx in visible_indices:
            radio = radio_locator.nth(idx)
            if await _is_selected(radio):
                selected_before_click = True
                break
        if selected_before_click:
            if attempt > 1:
                log(f"{description} 已确认选中 (already_selected)", "OK")
            return True

        clicked = False
        for idx in visible_indices:
            radio = radio_locator.nth(idx)
            targets = [
                radio.locator("[role='radio']").first,
                radio.locator("#radioContainer").first,
                radio.locator("#radioLabel").first,
                radio,
            ]
            for target in targets:
                try:
                    if await target.count() == 0:
                        continue
                    await target.scroll_into_view_if_needed()
                    await asyncio.sleep(0.2)
                    clicked = await human_click(page, target, f"{description} (idx={idx})")
                    if not clicked:
                        await target.click(force=True, timeout=3000)
                        clicked = True
                    if clicked:
                        break
                except Exception:
                    continue
            if clicked:
                break

        if not clicked:
            try:
                await radio_locator.first.focus()
                await page.keyboard.press("Space")
                clicked = True
                log(f"{description} 点击成功 (keyboard_space)", "INFO")
            except Exception:
                pass

        await asyncio.sleep(0.9)
        for idx in visible_indices:
            radio = radio_locator.nth(idx)
            if await _is_selected(radio):
                log(f"{description} 已确认选中 (playwright_verify)", "OK")
                return True

        if allow_dom_fallback:
            dom_clicked = await _dom_locate_and_maybe_click(section_patterns, option_patterns, do_click=True)
            if dom_clicked.get("clicked"):
                log(f"{description} DOM 兜底点击: {dom_clicked.get('label', '')}", "INFO")
                await asyncio.sleep(0.9)
                dom_verify = await _dom_locate_and_maybe_click(section_patterns, option_patterns, do_click=False)
                if dom_verify.get("selected"):
                    log(f"{description} 已确认选中 (dom_verify:{dom_verify.get('label', '')})", "OK")
                    return True

        await asyncio.sleep(0.6)

    log(f"{description} 仍未成功选中", "WARN")
    return False


async def set_video_category_music(page, max_attempts: int = 5) -> bool:
    # Keep locator logic focused on the actual Category field.
    async def _strict_read_state() -> Dict[str, Any]:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                const attrText = (el) => [
                    el?.getAttribute?.("aria-label") || "",
                    el?.getAttribute?.("label") || "",
                    el?.getAttribute?.("title") || "",
                    el?.getAttribute?.("name") || "",
                    el?.getAttribute?.("id") || "",
                ].join(" ");
                const catRe = /category|鍒嗛|鍒嗙被/i;
                const badRe = /recording\\s+date|record\\s+date|錄製日期|录制日期/i;
                const musicRe = /(^|\\s)music(\\s|$)|闊虫▊|闊充箰/i;
                const nodes = Array.from(document.querySelectorAll("ytcp-form-select, tp-yt-paper-dropdown-menu"));
                let best = null;
                for (const node of nodes) {
                    if (!visible(node)) continue;
                    const labelText = [
                        attrText(node),
                        textOf(node.querySelector?.("label")),
                        textOf(node.querySelector?.("tp-yt-paper-input-label")),
                        textOf(node.querySelector?.("[slot='label']")),
                    ].join(" ").trim();
                    const wholeText = [labelText, textOf(node)].join(" ").trim();
                    if (!catRe.test(labelText) && !catRe.test(wholeText)) continue;
                    const selectedText =
                        textOf(node.querySelector?.("#label")) ||
                        textOf(node.querySelector?.("ytcp-dropdown-trigger")) ||
                        textOf(node.querySelector?.("[aria-haspopup='listbox']")) ||
                        textOf(node);
                    if (badRe.test((selectedText || "").toLowerCase()) && !catRe.test(labelText)) continue;
                    const score =
                        (catRe.test(labelText) ? 10 : 0) +
                        (catRe.test(wholeText) ? 4 : 0) +
                        (musicRe.test(selectedText.toLowerCase()) || musicRe.test(selectedText) ? 3 : 0);
                    if (!best || score > best.score) {
                        best = { value: selectedText, score };
                    }
                }
                if (!best) {
                    return { found: false, selected: false, value: "", method: "strict_not_found" };
                }
                const value = (best.value || "").trim();
                const selected = musicRe.test(value.toLowerCase()) || musicRe.test(value);
                return { found: true, selected, value, method: selected ? "strict_already_music" : "strict_found" };
            }
            """
        )

    async def _strict_open_dropdown() -> bool:
        selectors = [
            "ytcp-form-select:has-text('Category') ytcp-dropdown-trigger",
            "ytcp-form-select:has-text('鍒嗛') ytcp-dropdown-trigger",
            "ytcp-form-select:has-text('鍒嗙被') ytcp-dropdown-trigger",
            "tp-yt-paper-dropdown-menu:has-text('Category')",
            "tp-yt-paper-dropdown-menu:has-text('鍒嗛')",
            "tp-yt-paper-dropdown-menu:has-text('鍒嗙被')",
        ]
        for sel in selectors:
            try:
                locator = page.locator(sel).first
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                if await human_click(page, locator, f"Category strict dropdown ({sel})"):
                    return True
            except Exception:
                continue
        return False

    async def _strict_set_category_music() -> bool:
        for attempt in range(1, max_attempts + 1):
            await clear_blocking_overlays(page, f"category-strict-{attempt}")
            state = await _strict_read_state()
            log(
                f"Category 严格检查#{attempt}: found={state.get('found')} selected={state.get('selected')} "
                f"method={state.get('method')} value={state.get('value', '')}",
                "INFO",
            )
            if state.get("selected"):
                log(f"Category 已确认是 Music ({state.get('value', '')})", "OK")
                return True
            opened = await _strict_open_dropdown()
            log(f"Category 严格打开下拉#{attempt}: opened={opened}", "INFO")
            if not opened:
                await asyncio.sleep(0.8)
                continue
            await asyncio.sleep(1.2)
            clicked = await _pick_music_option_locator()
            if clicked:
                await asyncio.sleep(1.0)
                verify = await _strict_read_state()
                if verify.get("selected"):
                    log(f"Category 已设置为 Music ({verify.get('value', '')})", "OK")
                    return True
            await asyncio.sleep(0.8)
        log("Category 严格模式仍未成功设置为 Music", "WARN")
        return False

    strict_ok = await _strict_set_category_music()
    if strict_ok:
        return True
    """在上传详情页将 Category 固定为 Music。"""

    async def _read_category_state() -> Dict[str, Any]:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                const attrText = (el) => [
                    el?.getAttribute?.("aria-label") || "",
                    el?.getAttribute?.("label") || "",
                    el?.getAttribute?.("title") || "",
                    el?.getAttribute?.("name") || "",
                    el?.getAttribute?.("id") || "",
                ].join(" ");
                const catRe = /category|分類|分类/i;
                const categoryValueRe = /people\\s*&\\s*blogs|people\\s+and\\s+blogs|music|education|entertainment|news|gaming|sports|travel/i;
                const musicRe = /(^|\\s)music(\\s|$)|音樂|音乐/i;

                const roots = [document];
                const seen = new Set([document]);
                let best = null;

                while (roots.length) {
                    const root = roots.shift();
                    const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                    for (const node of allNodes) {
                        if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                            seen.add(node.shadowRoot);
                            roots.push(node.shadowRoot);
                        }
                    }

                    const candidates = Array.from(
                        root.querySelectorAll
                            ? root.querySelectorAll(
                                [
                                    "ytcp-form-select",
                                    "tp-yt-paper-dropdown-menu",
                                    "ytcp-dropdown-trigger",
                                    "[aria-haspopup='listbox']",
                                    "[role='combobox']",
                                    "[role='button']",
                                ].join(",")
                            )
                            : []
                    ).filter(visible);

                    for (const el of candidates) {
                        const scope =
                            el.closest?.(
                                [
                                    "ytcp-form-select",
                                    "tp-yt-paper-dropdown-menu",
                                    "ytcp-form-input-container",
                                    "ytcp-video-metadata-editor-basics",
                                    "ytcp-video-metadata-editor-advanced",
                                    "ytcp-uploads-dialog",
                                ].join(",")
                            ) ||
                            el.parentElement ||
                            el;
                        const wholeText = [textOf(scope), textOf(el), attrText(el)].join(" ").trim();
                        if (!wholeText) continue;
                        const lower = wholeText.toLowerCase();
                        let score = 0;
                        if (catRe.test(wholeText)) score += 5;
                        if (categoryValueRe.test(lower)) score += 3;
                        if (/people\\s*&\\s*blogs|people\\s+and\\s+blogs/i.test(lower)) score += 4;
                        if (/music/.test(lower)) score += 2;
                        if (score <= 0) continue;

                        const selectedText =
                            textOf(scope.querySelector?.("#label")) ||
                            textOf(scope.querySelector?.("[aria-haspopup='listbox']")) ||
                            textOf(el) ||
                            textOf(scope);

                        if (!best || score > best.score) {
                            best = { score, selectedText };
                        }
                    }
                }

                if (!best) {
                    return { found: false, selected: false, method: "category_not_found", value: "" };
                }

                const value = (best.selectedText || "").trim();
                const selected = musicRe.test(value.toLowerCase()) || musicRe.test(value);
                return {
                    found: true,
                    selected,
                    method: selected ? "already_music" : "category_found",
                    value,
                };
            }
            """
        )

    async def _open_category_dropdown_dom() -> Dict[str, Any]:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                const attrText = (el) => [
                    el?.getAttribute?.("aria-label") || "",
                    el?.getAttribute?.("label") || "",
                    el?.getAttribute?.("title") || "",
                    el?.getAttribute?.("name") || "",
                    el?.getAttribute?.("id") || "",
                ].join(" ");
                const clickEl = (el) => {
                    if (!(el instanceof HTMLElement)) return false;
                    try {
                        el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
                    } catch (_) {}
                    for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                        try {
                            el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                        } catch (_) {}
                    }
                    try {
                        el.click();
                    } catch (_) {}
                    return true;
                };

                const catRe = /category|分類|分类/i;
                const categoryValueRe = /people\\s*&\\s*blogs|people\\s+and\\s+blogs|music|education|entertainment|news|gaming|sports|travel/i;
                const roots = [document];
                const seen = new Set([document]);
                let best = null;

                while (roots.length) {
                    const root = roots.shift();
                    const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                    for (const node of allNodes) {
                        if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                            seen.add(node.shadowRoot);
                            roots.push(node.shadowRoot);
                        }
                    }

                    const candidates = Array.from(
                        root.querySelectorAll
                            ? root.querySelectorAll(
                                [
                                    "ytcp-form-select",
                                    "tp-yt-paper-dropdown-menu",
                                    "ytcp-dropdown-trigger",
                                    "[aria-haspopup='listbox']",
                                    "[role='combobox']",
                                    "[role='button']",
                                ].join(",")
                            )
                            : []
                    ).filter(visible);

                    for (const el of candidates) {
                        const scope =
                            el.closest?.(
                                [
                                    "ytcp-form-select",
                                    "tp-yt-paper-dropdown-menu",
                                    "ytcp-form-input-container",
                                    "ytcp-video-metadata-editor-basics",
                                    "ytcp-video-metadata-editor-advanced",
                                    "ytcp-uploads-dialog",
                                ].join(",")
                            ) ||
                            el.parentElement ||
                            el;
                        const wholeText = [textOf(scope), textOf(el), attrText(el)].join(" ").trim();
                        if (!wholeText) continue;
                        const lower = wholeText.toLowerCase();
                        let score = 0;
                        if (catRe.test(wholeText)) score += 5;
                        if (categoryValueRe.test(lower)) score += 3;
                        if (/people\\s*&\\s*blogs|people\\s+and\\s+blogs/i.test(lower)) score += 4;
                        if (/music/.test(lower)) score += 2;
                        if (score <= 0) continue;

                        const triggerCandidates = [
                            scope.querySelector?.("#trigger"),
                            scope.querySelector?.("ytcp-dropdown-trigger"),
                            scope.querySelector?.("[aria-haspopup='listbox']"),
                            scope.querySelector?.("[role='combobox']"),
                            scope.querySelector?.("[role='button']"),
                            el,
                            scope,
                        ].filter(Boolean);

                        if (!best || score > best.score) {
                            best = {
                                score,
                                value: textOf(scope.querySelector?.("#label")) || textOf(el) || textOf(scope),
                                triggers: triggerCandidates,
                            };
                        }
                    }
                }

                if (!best) return { opened: false, found: false, method: "category_not_found", value: "" };

                for (const trigger of best.triggers) {
                    if (!visible(trigger)) continue;
                    if (clickEl(trigger)) {
                        return { opened: true, found: true, method: "opened_by_dom", value: best.value || "" };
                    }
                }

                return { opened: false, found: true, method: "trigger_not_clickable", value: best.value || "" };
            }
            """
        )

    async def _open_category_dropdown_locator() -> bool:
        selectors = [
            "ytcp-form-select:has-text('Category') ytcp-dropdown-trigger",
            "ytcp-form-select:has-text('分類') ytcp-dropdown-trigger",
            "ytcp-form-select:has-text('分类') ytcp-dropdown-trigger",
            "ytcp-form-select:has-text('People & Blogs') ytcp-dropdown-trigger",
            "ytcp-form-select:has-text('People and Blogs') ytcp-dropdown-trigger",
            "ytcp-form-select:has-text('Music') ytcp-dropdown-trigger",
            "tp-yt-paper-dropdown-menu:has-text('Category')",
            "tp-yt-paper-dropdown-menu:has-text('People & Blogs')",
            "tp-yt-paper-dropdown-menu:has-text('Music')",
            "[aria-haspopup='listbox']",
            "[role='combobox']",
        ]
        for sel in selectors:
            try:
                locator = page.locator(sel).first
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                clicked = await human_click(page, locator, f"Category 下拉 ({sel})")
                if clicked:
                    return True
            except Exception:
                continue
        return False

    async def _pick_music_option_dom() -> Dict[str, Any]:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const musicRe = /(^|\\s)music(\\s|$)|音樂|音乐/i;
                const clickEl = (el) => {
                    if (!el) return false;
                    const target = el instanceof HTMLElement ? el : null;
                    if (!target) return false;
                    try {
                        target.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
                    } catch (_) {}
                    const events = ["pointerdown", "mousedown", "pointerup", "mouseup", "click"];
                    for (const type of events) {
                        try {
                            target.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                        } catch (_) {}
                    }
                    try {
                        target.click();
                    } catch (_) {}
                    return true;
                };
                const roots = [document];
                const seenRoots = new Set([document]);

                while (roots.length) {
                    const root = roots.shift();
                    const nodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                    for (const el of nodes) {
                        if (el && el.shadowRoot && !seenRoots.has(el.shadowRoot)) {
                            seenRoots.add(el.shadowRoot);
                            roots.push(el.shadowRoot);
                        }
                    }

                    const items = Array.from(
                        (root.querySelectorAll
                            ? root.querySelectorAll(
                                "tp-yt-paper-item, ytcp-dropdown-menu tp-yt-paper-item, [role='option'], ytcp-menu-service-item-renderer"
                            )
                            : [])
                    ).filter(visible);

                    for (const item of items) {
                        const text = (item.innerText || item.textContent || "").trim();
                        if (!text) continue;
                        if (musicRe.test(text.toLowerCase()) || musicRe.test(text)) {
                            if (clickEl(item)) {
                                return { clicked: true, text };
                            }
                        }
                    }
                }

                return { clicked: false };
            }
            """
        )

    async def _pick_music_option_locator() -> bool:
        selectors = [
            "tp-yt-paper-item:has-text('Music')",
            "[role='option']:has-text('Music')",
            "ytcp-menu-service-item-renderer:has-text('Music')",
            "tp-yt-paper-item:has-text('音樂')",
            "[role='option']:has-text('音樂')",
            "ytcp-menu-service-item-renderer:has-text('音樂')",
            "tp-yt-paper-item:has-text('音乐')",
            "[role='option']:has-text('音乐')",
            "ytcp-menu-service-item-renderer:has-text('音乐')",
        ]
        for sel in selectors:
            try:
                locator = page.locator(sel).first
                if await locator.count() == 0:
                    continue
                if not await locator.is_visible():
                    continue
                clicked = await human_click(page, locator, f"Category 选项 ({sel})")
                if clicked:
                    return True
                await locator.click(timeout=3000, force=True)
                return True
            except Exception:
                continue
        return False

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"category-{attempt}")
        try:
            await page.mouse.wheel(0, 700)
        except Exception:
            pass
        try:
            state = await _read_category_state()
        except Exception as e:
            state = {"found": False, "selected": False, "method": f"evaluate_error:{e}"}

        log(
            f"Category 检查#{attempt}: found={state.get('found')} selected={state.get('selected')} method={state.get('method')} value={state.get('value', '')}",
            "INFO",
        )

        if state.get("selected"):
            log(f"Category 已确认是 Music ({state.get('value', '')})", "OK")
            return True

        opened = False
        try:
            open_result = await _open_category_dropdown_dom()
            opened = bool(open_result.get("opened"))
            log(
                f"Category 打开下拉#{attempt}: opened={opened} method={open_result.get('method')} value={open_result.get('value', '')}",
                "INFO",
            )
        except Exception as e:
            log(f"Category DOM 打开下拉异常: {e}", "WARN")

        if not opened:
            opened = await _open_category_dropdown_locator()
            if opened:
                log("Category 下拉已通过 Locator 打开", "INFO")

        if opened:
            try:
                await asyncio.sleep(1.2)
                picked = await _pick_music_option_dom()
                clicked = bool(picked.get("clicked"))
                if clicked:
                    log(f"Category 选项点击成功 (DOM): {picked.get('text', '')}", "INFO")
                if not clicked:
                    clicked = await _pick_music_option_locator()
                    if clicked:
                        log("Category 选项点击成功 (Locator)", "INFO")

                if clicked:
                    await asyncio.sleep(1.0)
                    verify = await _read_category_state()
                    if verify.get("selected"):
                        log(f"Category 已设置为 Music ({verify.get('value', '')})", "OK")
                        return True
            except Exception as e:
                log(f"Category 选择流程异常: {e}", "WARN")

        await asyncio.sleep(0.8)

    log("Category 仍未成功设置为 Music", "WARN")
    return False


async def set_video_category_music(page, max_attempts: int = 5) -> bool:
    async def _read_state() -> Dict[str, Any]:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                const roots = [document];
                const seen = new Set([document]);
                const categoryRe = /category|分類|分类/i;
                const musicRe = /(^|\\s)music(\\s|$)|音樂|音乐/i;

                while (roots.length) {
                    const root = roots.shift();
                    const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                    for (const node of allNodes) {
                        if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                            seen.add(node.shadowRoot);
                            roots.push(node.shadowRoot);
                        }
                    }
                    const fields = Array.from(
                        root.querySelectorAll ? root.querySelectorAll("ytcp-form-select, tp-yt-paper-dropdown-menu") : []
                    ).filter(visible);
                    for (const field of fields) {
                        const labelText = [
                            textOf(field.querySelector?.("label")),
                            textOf(field.querySelector?.("tp-yt-paper-input-label")),
                            textOf(field.querySelector?.("[slot='label']")),
                            textOf(field),
                        ].join(" ").trim();
                        if (!categoryRe.test(labelText)) continue;
                        const selectedText =
                            textOf(field.querySelector?.("#label")) ||
                            textOf(field.querySelector?.("ytcp-dropdown-trigger")) ||
                            textOf(field.querySelector?.("[aria-haspopup='listbox']")) ||
                            "";
                        return {
                            found: true,
                            selected: musicRe.test(selectedText.toLowerCase()) || musicRe.test(selectedText),
                            value: selectedText,
                        };
                    }
                }
                return { found: false, selected: false, value: "" };
            }
            """
        )

    async def _open_dropdown() -> bool:
        return bool(
            await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement)) return false;
                        try {
                            el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
                        } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };
                    const roots = [document];
                    const seen = new Set([document]);
                    const categoryRe = /category|分類|分类/i;
                    while (roots.length) {
                        const root = roots.shift();
                        const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                        for (const node of allNodes) {
                            if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                                seen.add(node.shadowRoot);
                                roots.push(node.shadowRoot);
                            }
                        }
                        const fields = Array.from(
                            root.querySelectorAll ? root.querySelectorAll("ytcp-form-select, tp-yt-paper-dropdown-menu") : []
                        ).filter(visible);
                        for (const field of fields) {
                            const labelText = [
                                textOf(field.querySelector?.("label")),
                                textOf(field.querySelector?.("tp-yt-paper-input-label")),
                                textOf(field.querySelector?.("[slot='label']")),
                                textOf(field),
                            ].join(" ").trim();
                            if (!categoryRe.test(labelText)) continue;
                            const trigger =
                                field.querySelector?.("ytcp-dropdown-trigger") ||
                                field.querySelector?.("[aria-haspopup='listbox']") ||
                                field.querySelector?.("[role='button']") ||
                                field;
                            if (clickEl(trigger)) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
                """
            )
        )

    async def _pick_music_option() -> bool:
        selectors = [
            "tp-yt-paper-item:has-text('Music')",
            "[role='option']:has-text('Music')",
            "ytcp-menu-service-item-renderer:has-text('Music')",
            "tp-yt-paper-item:has-text('音樂')",
            "[role='option']:has-text('音樂')",
            "ytcp-menu-service-item-renderer:has-text('音樂')",
            "tp-yt-paper-item:has-text('音乐')",
            "[role='option']:has-text('音乐')",
            "ytcp-menu-service-item-renderer:has-text('音乐')",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).last
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                if await human_click(page, locator, f"Category option ({selector})"):
                    return True
                await locator.click(force=True, timeout=3000)
                return True
            except Exception:
                continue
        return False

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"category-music-{attempt}")
        try:
            await page.mouse.wheel(0, 600)
        except Exception:
            pass
        state = await _read_state()
        log(
            f"Category strict check#{attempt}: found={state.get('found')} selected={state.get('selected')} value={state.get('value', '')}",
            "INFO",
        )
        if state.get("selected"):
            log(f"Category confirmed as Music ({state.get('value', '')})", "OK")
            return True
        opened = await _open_dropdown()
        log(f"Category strict open#{attempt}: opened={opened}", "INFO")
        if not opened:
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(1.0)
        picked = await _pick_music_option()
        log(f"Category strict pick#{attempt}: picked={picked}", "INFO")
        if not picked:
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(1.0)
        verify = await _read_state()
        log(
            f"Category strict verify#{attempt}: found={verify.get('found')} selected={verify.get('selected')} value={verify.get('value', '')}",
            "INFO",
        )
        if verify.get("selected"):
            log(f"Category set to Music ({verify.get('value', '')})", "OK")
            return True

    log("Category still failed to become Music", "WARN")
    return False


async def set_video_category_music(page, max_attempts: int = 6) -> bool:
    async def _read_state() -> Dict[str, Any]:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                const attrOf = (el) => [
                    el?.getAttribute?.("aria-label") || "",
                    el?.getAttribute?.("label") || "",
                    el?.getAttribute?.("title") || "",
                    el?.getAttribute?.("name") || "",
                    el?.id || "",
                ].join(" ").trim();
                const categoryRe = /category|分類|分类/i;
                const excludeRe = /recording\\s*date|錄製日期|录制日期/i;
                const musicRe = /(^|\\s)music(\\s|$)|音樂|音乐/i;
                const roots = [document];
                const seen = new Set([document]);
                const candidates = [];

                while (roots.length) {
                    const root = roots.shift();
                    const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                    for (const node of allNodes) {
                        if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                            seen.add(node.shadowRoot);
                            roots.push(node.shadowRoot);
                        }
                    }
                    const nodes = Array.from(
                        root.querySelectorAll
                            ? root.querySelectorAll(
                                "ytcp-form-select, tp-yt-paper-dropdown-menu, ytcp-dropdown-trigger, [aria-haspopup='listbox'], [role='combobox']"
                            )
                            : []
                    ).filter(visible);
                    for (const node of nodes) {
                        const host =
                            node.closest?.("ytcp-form-select") ||
                            node.closest?.("tp-yt-paper-dropdown-menu") ||
                            node;
                        if (!host) continue;
                        const contextParts = [];
                        let current = host;
                        for (let depth = 0; current && depth < 5; depth += 1, current = current.parentElement) {
                            contextParts.push(textOf(current));
                            contextParts.push(attrOf(current));
                        }
                        const context = contextParts.join(" ").trim();
                        if (!categoryRe.test(context) || excludeRe.test(context)) continue;
                        const trigger =
                            host.querySelector?.("ytcp-dropdown-trigger") ||
                            host.querySelector?.("[aria-haspopup='listbox']") ||
                            host.querySelector?.("[role='combobox']") ||
                            host.querySelector?.("#trigger") ||
                            (node.matches?.("ytcp-dropdown-trigger, [aria-haspopup='listbox'], [role='combobox']") ? node : null) ||
                            host;
                        if (!trigger) continue;
                        const selectedText = [
                            textOf(host.querySelector?.("#label")),
                            textOf(host.querySelector?.("yt-formatted-string#label")),
                            textOf(trigger),
                            textOf(host),
                        ].join(" ").trim();
                        candidates.push({
                            value: selectedText,
                            selected: musicRe.test(selectedText.toLowerCase()) || musicRe.test(selectedText),
                            score:
                                (categoryRe.test(textOf(host.querySelector?.("label")) || "") ? 8 : 0) +
                                (categoryRe.test(context) ? 5 : 0) +
                                (textOf(trigger) ? 1 : 0),
                        });
                    }
                }

                if (!candidates.length) {
                    return { found: false, selected: false, value: "" };
                }
                candidates.sort((a, b) => b.score - a.score);
                return {
                    found: true,
                    selected: !!candidates[0].selected,
                    value: candidates[0].value || "",
                };
            }
            """
        )

    async def _open_dropdown() -> bool:
        return bool(
            await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                    const attrOf = (el) => [
                        el?.getAttribute?.("aria-label") || "",
                        el?.getAttribute?.("label") || "",
                        el?.getAttribute?.("title") || "",
                        el?.getAttribute?.("name") || "",
                        el?.id || "",
                    ].join(" ").trim();
                    const categoryRe = /category|分類|分类/i;
                    const excludeRe = /recording\\s*date|錄製日期|录制日期/i;
                    const roots = [document];
                    const seen = new Set([document]);
                    const candidates = [];
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement)) return false;
                        try { el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" }); } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };

                    while (roots.length) {
                        const root = roots.shift();
                        const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                        for (const node of allNodes) {
                            if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                                seen.add(node.shadowRoot);
                                roots.push(node.shadowRoot);
                            }
                        }
                        const nodes = Array.from(
                            root.querySelectorAll
                                ? root.querySelectorAll(
                                    "ytcp-form-select, tp-yt-paper-dropdown-menu, ytcp-dropdown-trigger, [aria-haspopup='listbox'], [role='combobox']"
                                )
                                : []
                        ).filter(visible);
                        for (const node of nodes) {
                            const host =
                                node.closest?.("ytcp-form-select") ||
                                node.closest?.("tp-yt-paper-dropdown-menu") ||
                                node;
                            if (!host) continue;
                            const contextParts = [];
                            let current = host;
                            for (let depth = 0; current && depth < 5; depth += 1, current = current.parentElement) {
                                contextParts.push(textOf(current));
                                contextParts.push(attrOf(current));
                            }
                            const context = contextParts.join(" ").trim();
                            if (!categoryRe.test(context) || excludeRe.test(context)) continue;
                            const trigger =
                                host.querySelector?.("ytcp-dropdown-trigger") ||
                                host.querySelector?.("[aria-haspopup='listbox']") ||
                                host.querySelector?.("[role='combobox']") ||
                                host.querySelector?.("#trigger") ||
                                (node.matches?.("ytcp-dropdown-trigger, [aria-haspopup='listbox'], [role='combobox']") ? node : null) ||
                                host;
                            if (!trigger || (!visible(trigger) && !visible(host))) continue;
                            candidates.push({
                                trigger,
                                score:
                                    (categoryRe.test(textOf(host.querySelector?.("label")) || "") ? 8 : 0) +
                                    (categoryRe.test(context) ? 5 : 0) +
                                    (textOf(trigger) ? 1 : 0),
                            });
                        }
                    }

                    if (!candidates.length) return false;
                    candidates.sort((a, b) => b.score - a.score);
                    for (const candidate of candidates) {
                        if (clickEl(candidate.trigger)) return true;
                    }
                    return false;
                }
                """
            )
        )

    async def _pick_music_option() -> bool:
        selectors = [
            "tp-yt-paper-item:has-text('Music')",
            "[role='option']:has-text('Music')",
            "[role='menuitemradio']:has-text('Music')",
            "ytcp-menu-service-item-renderer:has-text('Music')",
            "tp-yt-paper-item:has-text('音樂')",
            "[role='option']:has-text('音樂')",
            "[role='menuitemradio']:has-text('音樂')",
            "ytcp-menu-service-item-renderer:has-text('音樂')",
            "tp-yt-paper-item:has-text('音乐')",
            "[role='option']:has-text('音乐')",
            "[role='menuitemradio']:has-text('音乐')",
            "ytcp-menu-service-item-renderer:has-text('音乐')",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).last
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                if await human_click(page, locator, f"Category option ({selector})"):
                    return True
                await locator.click(force=True, timeout=3000)
                return True
            except Exception:
                continue
        try:
            result = await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                    const musicRe = /(^|\\s)music(\\s|$)|音樂|音乐/i;
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement)) return false;
                        try { el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" }); } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };
                    const nodes = Array.from(
                        document.querySelectorAll(
                            "tp-yt-paper-item, [role='option'], [role='menuitemradio'], ytcp-menu-service-item-renderer"
                        )
                    ).filter(visible);
                    for (const node of nodes) {
                        if (!musicRe.test(textOf(node))) continue;
                        if (clickEl(node)) return true;
                    }
                    return false;
                }
                """
            )
            if result:
                return True
        except Exception:
            pass
        return False

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"category-final-{attempt}")
        try:
            await page.mouse.wheel(0, 700)
        except Exception:
            pass
        state = await _read_state()
        log(
            f"Category final check#{attempt}: found={state.get('found')} selected={state.get('selected')} value={state.get('value', '')}",
            "INFO",
        )
        if state.get("selected"):
            log(f"Category confirmed as Music ({state.get('value', '')})", "OK")
            return True
        opened = await _open_dropdown()
        log(f"Category final open#{attempt}: opened={opened}", "INFO")
        if not opened:
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(1.0)
        picked = await _pick_music_option()
        log(f"Category final pick#{attempt}: picked={picked}", "INFO")
        if not picked:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(1.0)
        verify = await _read_state()
        log(
            f"Category final verify#{attempt}: found={verify.get('found')} selected={verify.get('selected')} value={verify.get('value', '')}",
            "INFO",
        )
        if verify.get("selected"):
            log(f"Category set to Music ({verify.get('value', '')})", "OK")
            return True
    log("Category still failed to become Music", "WARN")
    return False


async def set_video_category_music(page, max_attempts: int = 6) -> bool:
    music_re = re.compile(r"(^|\s)music(\s|$)|音樂|音乐", re.I)

    async def _read_direct_state() -> Dict[str, Any]:
        texts: list[str] = []
        found = False
        for selector in ("#category-container", "ytcp-form-select#category", "#category"):
            locator = page.locator(selector).first
            try:
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                found = True
                text = (await locator.inner_text()).strip()
                if text:
                    texts.append(text)
            except Exception:
                continue
        value = " | ".join(texts).strip()
        return {"found": found, "selected": bool(music_re.search(value)), "value": value}

    async def _open_direct_dropdown() -> bool:
        selectors = [
            "#category [aria-haspopup='listbox']",
            "#category-container [aria-haspopup='listbox']",
            "ytcp-form-select#category",
            "#category",
            "#category-container",
        ]
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                await locator.scroll_into_view_if_needed()
                if await human_click(page, locator, f"Category direct ({selector})"):
                    return True
                await locator.click(force=True, timeout=3000)
                return True
            except Exception:
                continue
        return False

    async def _pick_direct_music() -> bool:
        selectors = [
            "tp-yt-paper-item:has-text('Music')",
            "[role='option']:has-text('Music')",
            "[role='menuitemradio']:has-text('Music')",
            "ytcp-menu-service-item-renderer:has-text('Music')",
            "tp-yt-paper-item:has-text('音樂')",
            "[role='option']:has-text('音樂')",
            "[role='menuitemradio']:has-text('音樂')",
            "ytcp-menu-service-item-renderer:has-text('音樂')",
            "tp-yt-paper-item:has-text('音乐')",
            "[role='option']:has-text('音乐')",
            "[role='menuitemradio']:has-text('音乐')",
            "ytcp-menu-service-item-renderer:has-text('音乐')",
        ]
        for selector in selectors:
            locator = page.locator(selector).last
            try:
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                if await human_click(page, locator, f"Category option ({selector})"):
                    return True
                await locator.click(force=True, timeout=3000)
                return True
            except Exception:
                continue
        return False

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"category-direct-{attempt}")
        try:
            category_container = page.locator("#category-container").first
            if await category_container.count() > 0 and await category_container.is_visible():
                await category_container.scroll_into_view_if_needed()
            else:
                await page.mouse.wheel(0, 700)
        except Exception:
            pass

        state = await _read_direct_state()
        log(
            f"Category direct check#{attempt}: found={state.get('found')} selected={state.get('selected')} value={state.get('value', '')}",
            "INFO",
        )
        if state.get("selected"):
            log(f"Category confirmed as Music ({state.get('value', '')})", "OK")
            return True

        opened = await _open_direct_dropdown()
        log(f"Category direct open#{attempt}: opened={opened}", "INFO")
        if not opened:
            await asyncio.sleep(0.8)
            continue

        await asyncio.sleep(0.8)
        picked = await _pick_direct_music()
        log(f"Category direct pick#{attempt}: picked={picked}", "INFO")
        if not picked:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            await asyncio.sleep(0.8)
            continue

        await asyncio.sleep(1.0)
        verify = await _read_direct_state()
        log(
            f"Category direct verify#{attempt}: found={verify.get('found')} selected={verify.get('selected')} value={verify.get('value', '')}",
            "INFO",
        )
        if verify.get("selected"):
            log(f"Category set to Music ({verify.get('value', '')})", "OK")
            return True

    log("Category still failed to become Music", "WARN")
    return False


async def set_video_category(page, category: str) -> bool:
    target = str(category or "").strip()
    if not target:
        return True
    if target.lower() == "music":
        return await set_video_category_music(page)

    dropdown_selectors = [
        "ytcp-form-select:has-text('Category') ytcp-dropdown-trigger",
        "ytcp-form-select:has-text('Category') [aria-haspopup='listbox']",
        "ytcp-form-select #trigger",
    ]
    option_selectors = [
        f"tp-yt-paper-item:has-text('{target}')",
        f"[role='option']:has-text('{target}')",
        f"ytcp-menu-service-item-renderer:has-text('{target}')",
    ]

    for attempt in range(1, 4):
        await clear_blocking_overlays(page, f"generic-category-{attempt}")
        for selector in dropdown_selectors:
            locator = page.locator(selector).first
            try:
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                await human_click(page, locator, f"Category 下拉 ({selector})")
                await asyncio.sleep(0.8)
                break
            except Exception:
                continue

        for selector in option_selectors:
            locator = page.locator(selector).first
            try:
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                clicked = await human_click(page, locator, f"Category 选项 ({target})")
                if not clicked:
                    await locator.click(force=True, timeout=3000)
                await asyncio.sleep(1.0)
                return True
            except Exception:
                continue

    log(f"Category 仍未成功设置为 {target}", "WARN")
    return False


async def set_made_for_kids_setting(page, made_for_kids: bool) -> bool:
    radio_name = "VIDEO_MADE_FOR_KIDS_MFK" if made_for_kids else "VIDEO_MADE_FOR_KIDS_NOT_MFK"
    desc = "Made for kids" if made_for_kids else "Not made for kids"
    return await ensure_upload_radio_selected(page, radio_name, desc, allow_dom_fallback=False)


async def set_altered_content_setting(page, altered_content: bool) -> bool:
    radio_name = "VIDEO_HAS_ALTERED_CONTENT_YES" if altered_content else "VIDEO_HAS_ALTERED_CONTENT_NO"
    desc = f"Altered content = {'Yes' if altered_content else 'No'}"
    return await ensure_upload_radio_selected(page, radio_name, desc, allow_dom_fallback=False)


async def fill_video_tags(page, tags: List[str]) -> bool:
    clean_tags = [str(item).strip() for item in (tags or []) if str(item).strip()]
    if not clean_tags:
        return True

    advanced_ready = await ensure_advanced_settings_open(page)
    if not advanced_ready:
        log("标签填写前未能确认高级设置已展开，继续尝试直接定位标签输入框", "WARN")

    joined = ", ".join(clean_tags[:50])
    tag_selectors = [
        '#tags-input #textbox',
        '#tags-input textarea',
        '#tags-input input[type="text"]',
        'ytcp-video-tags #textbox',
        'ytcp-video-tags textarea',
        'ytcp-video-tags input[type="text"]',
        'input[aria-label*="Tags"]',
        'textarea[aria-label*="Tags"]',
        'input[aria-label*="标签"]',
        'textarea[aria-label*="标签"]',
        'input[placeholder*="标签"]',
        'textarea[placeholder*="标签"]',
    ]

    for selector in tag_selectors:
        locator = page.locator(selector).first
        try:
            if await locator.count() == 0 or not await locator.is_visible():
                continue
            if await human_fill(page, locator, joined, "标签"):
                return True
        except Exception:
            continue

    return await fill_visible_upload_field(page, "tags", joined)


def _format_schedule_strings(schedule_text: str) -> tuple[list[str], list[str]]:
    raw = str(schedule_text or "").strip()
    parsed = None
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            parsed = datetime.strptime(raw, fmt)
            break
        except Exception:
            continue
    if not parsed:
        raise ValueError("定时发布时间格式应为 YYYY-MM-DD HH:MM")

    date_candidates = [
        parsed.strftime("%m/%d/%Y"),
        parsed.strftime("%b %d, %Y"),
    ]
    time_candidates = [
        parsed.strftime("%I:%M %p").lstrip("0"),
        parsed.strftime("%H:%M"),
    ]
    return date_candidates, time_candidates


def _schedule_timezone_candidates(schedule_timezone: str | None) -> list[str]:
    raw = str(schedule_timezone or "").strip()
    if not raw:
        return []

    candidates = [raw]
    lowered = raw.lower()
    if "taipei" in lowered or "+08:00" in lowered or "asia/taipei" in lowered:
        candidates.extend(
            [
                "Asia/Taipei",
                "Taipei",
                "台北",
                "UTC+08:00",
                "GMT+08:00",
                "UTC+8",
                "GMT+8",
                "+08:00",
            ]
        )

    ordered: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        key = item.strip().lower()
        if key and key not in seen:
            ordered.append(item)
            seen.add(key)
    return ordered


async def fill_schedule_timezone(page, schedule_timezone: str | None) -> bool:
    candidates = _schedule_timezone_candidates(schedule_timezone)
    if not candidates:
        return True

    try:
        opened = await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const metaText = (el) => [
                    el.innerText || '',
                    el.textContent || '',
                    el.getAttribute?.('aria-label') || '',
                    el.getAttribute?.('title') || '',
                    el.getAttribute?.('placeholder') || '',
                    el.closest?.('ytcp-datetime-picker, ytcp-form-input-container, tp-yt-paper-dialog')?.innerText || '',
                ].join(' ').toLowerCase();

                const controls = Array.from(
                    document.querySelectorAll('button, [role="button"], tp-yt-paper-button, ytcp-dropdown-trigger')
                ).filter((el) => visible(el) && /timezone|時區|时区/.test(metaText(el)));

                if (!controls.length) {
                    return false;
                }

                const target = controls[0].querySelector?.('button, [role="button"]') || controls[0];
                target.click();
                return true;
            }
            """,
        )
        if not opened:
            return False

        await asyncio.sleep(0.8)
        matched = await page.evaluate(
            """
            ({ candidates }) => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const items = Array.from(
                    document.querySelectorAll('tp-yt-paper-item, [role="option"], [role="menuitem"], [role="menuitemradio"], ytcp-text-menu-item-renderer')
                ).filter(visible);
                const normalizedCandidates = candidates.map((item) => String(item).toLowerCase());

                for (const item of items) {
                    const text = (item.innerText || item.textContent || '').trim();
                    const lowered = text.toLowerCase();
                    if (!lowered) continue;
                    if (normalizedCandidates.some((candidate) => lowered.includes(candidate))) {
                        (item.querySelector('button, [role="option"], [role="menuitem"], [role="menuitemradio"]') || item).click();
                        return text;
                    }
                }
                return '';
            }
            """,
            {"candidates": candidates},
        )
        return bool(matched)
    except Exception as e:
        log(f"定时发布时区填写异常: {e}", "WARN")
        return False


async def fill_schedule_inputs(page, schedule_text: str, schedule_timezone: str | None = None) -> bool:
    date_candidates, time_candidates = _format_schedule_strings(schedule_text)
    try:
        result = await page.evaluate(
            """
            ({ dateCandidates, timeCandidates }) => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );

                const allInputs = Array.from(document.querySelectorAll('input, textarea')).filter(visible);
                const inputMeta = (el) => [
                    el.getAttribute('aria-label') || '',
                    el.getAttribute('placeholder') || '',
                    el.getAttribute('id') || '',
                    el.getAttribute('name') || '',
                    el.closest('tp-yt-paper-input, ytcp-form-input-container, ytcp-datetime-picker')
                        ?.innerText || ''
                ].join(' ').toLowerCase();

                const dateInput = allInputs.find((el) => /date|日期|日期/i.test(inputMeta(el)));
                const timeInput = allInputs.find((el) => /time|时间|時間/i.test(inputMeta(el)));

                const assign = (el, value) => {
                    if (!el) return false;
                    el.focus();
                    const setter = Object.getOwnPropertyDescriptor(el.__proto__, 'value')?.set;
                    if (setter) {
                        setter.call(el, value);
                    } else {
                        el.value = value;
                    }
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
                    el.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }));
                    return true;
                };

                let dateFilled = false;
                for (const value of dateCandidates) {
                    if (assign(dateInput, value)) {
                        dateFilled = true;
                        break;
                    }
                }

                let timeFilled = false;
                for (const value of timeCandidates) {
                    if (assign(timeInput, value)) {
                        timeFilled = true;
                        break;
                    }
                }

                return {
                    dateFilled,
                    timeFilled,
                    dateMeta: dateInput ? inputMeta(dateInput) : '',
                    timeMeta: timeInput ? inputMeta(timeInput) : '',
                };
            }
            """,
            {"dateCandidates": date_candidates, "timeCandidates": time_candidates},
        )
        if result.get("dateFilled") and result.get("timeFilled"):
            await asyncio.sleep(1.0)
            if schedule_timezone:
                timezone_selected = await fill_schedule_timezone(page, schedule_timezone)
                if timezone_selected:
                    log(f"定时发布时区已设置: {schedule_timezone}", "OK")
                else:
                    log("未找到时区下拉，继续使用当前频道默认时区", "WARN")
            return True
    except Exception as e:
        log(f"定时发布时间填写异常: {e}", "WARN")
    return False


async def apply_visibility_settings(
    page,
    visibility: str,
    scheduled_publish_at: str | None = None,
    schedule_timezone: str | None = None,
) -> bool:
    target = str(visibility or "public").strip().lower()
    if target == "public":
        public_radio = page.locator("tp-yt-paper-radio-button[name='PUBLIC']").first
        public_clicked = await human_click(page, public_radio, "Public")
        if not public_clicked:
            public_clicked = await click_visible_upload_dialog_button(
                page,
                "Public",
                text_pattern="^public$|^everyone$|公開|公开",
            )
        if not public_clicked:
            public_clicked = await try_select_public_visibility(page)
        await asyncio.sleep(1.0)
        return await ensure_public_visibility_selected(page)

    radio_map = {
        "private": ("PRIVATE", "Private", "^private$|私密"),
        "unlisted": ("UNLISTED", "Unlisted", "^unlisted$|不公開|不公开"),
        "schedule": ("SCHEDULE", "Schedule", "^schedule$|scheduled|定時|定时|排程"),
    }
    radio_name, desc, pattern = radio_map.get(target, ("PUBLIC", "Public", "^public$|^everyone$|公開|公开"))
    selected = await ensure_upload_radio_selected(page, radio_name, desc, max_attempts=3)
    if not selected:
        locator = page.locator(f"tp-yt-paper-radio-button[name='{radio_name}']").first
        if await locator.count() > 0 and await locator.is_visible():
            selected = await human_click(page, locator, desc)
        if not selected:
            selected = await click_visible_upload_dialog_button(page, desc, text_pattern=pattern)
    await asyncio.sleep(1.0)

    if not selected:
        return False

    if target == "schedule" and scheduled_publish_at:
        return await fill_schedule_inputs(page, scheduled_publish_at, schedule_timezone=schedule_timezone)
    return True


async def ensure_public_visibility_selected(page) -> bool:
    """尽量确认 Visibility 已切到 Public。"""
    for _ in range(3):
        try:
            result = await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0
                    );
                    const roots = Array.from(
                        document.querySelectorAll('ytcp-uploads-dialog, ytcp-dialog, tp-yt-paper-dialog')
                    ).filter(visible);
                    if (!roots.length) roots.push(document);

                    for (const root of roots) {
                        const radios = Array.from(
                            root.querySelectorAll("tp-yt-paper-radio-button[name='PUBLIC']")
                        ).filter((radio) => visible(radio) || visible(radio.querySelector('[role="radio"]')));

                        for (const radio of radios) {
                            const roleRadio = radio.querySelector('[role="radio"]');
                            const isSelected =
                                radio.getAttribute('aria-checked') === 'true' ||
                                radio.hasAttribute('checked') ||
                                radio.classList.contains('checked') ||
                                (roleRadio && roleRadio.getAttribute('aria-checked') === 'true');
                            if (isSelected) return { found: true, selected: true };

                            const candidates = [
                                roleRadio,
                                radio.querySelector('#radioLabel'),
                                radio.querySelector('label'),
                                radio.querySelector('div'),
                                radio,
                            ].filter(Boolean);

                            for (const el of candidates) {
                                if (!visible(el)) continue;
                                try {
                                    el.click();
                                    return { found: true, selected: false };
                                } catch (_) {}
                            }
                        }

                        const publicRe = /^(public|everyone|公開|公开)$/i;
                        const publicNodes = Array.from(
                            root.querySelectorAll('tp-yt-paper-radio-button, tp-yt-paper-item, label, button, div[role="radio"], [role="option"]')
                        ).filter((el) => {
                            const text = (el.innerText || el.textContent || '').trim();
                            return publicRe.test(text) && visible(el);
                        });

                        for (const el of publicNodes) {
                            try {
                                el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
                            } catch (_) {}
                            try {
                                el.click();
                            } catch (_) {}
                        }

                        const selectedByText = publicNodes.some((el) => {
                            const radio = el.closest('tp-yt-paper-radio-button');
                            const roleRadio = radio ? radio.querySelector('[role="radio"]') : el.querySelector('[role="radio"]');
                            return (
                                (radio && (
                                    radio.getAttribute('aria-checked') === 'true' ||
                                    radio.hasAttribute('checked') ||
                                    radio.classList.contains('checked')
                                )) ||
                                (roleRadio && roleRadio.getAttribute('aria-checked') === 'true') ||
                                el.getAttribute('aria-checked') === 'true'
                            );
                        });

                        if (selectedByText) return { found: true, selected: true };
                    }
                    return { found: false, selected: false };
                }
                """
            )
            if result.get("selected"):
                return True
        except Exception:
            pass

        await asyncio.sleep(1)

    try:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const radios = Array.from(
                    document.querySelectorAll("tp-yt-paper-radio-button[name='PUBLIC']")
                ).filter((radio) => visible(radio) || visible(radio.querySelector('[role="radio"]')));

                for (const radio of radios) {
                    const roleRadio = radio.querySelector('[role="radio"]');
                    if (
                        radio.getAttribute('aria-checked') === 'true' ||
                        radio.hasAttribute('checked') ||
                        radio.classList.contains('checked') ||
                        (roleRadio && roleRadio.getAttribute('aria-checked') === 'true')
                    ) {
                        return true;
                    }
                }
                return false;
            }
            """
        )
    except Exception:
        return False


async def try_select_public_visibility(page) -> bool:
    """Visibility 页面兜底：按文案强制点 Public/Everyone。"""
    try:
        result = await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const publicRe = /^(public|everyone|公開|公开)$/i;
                const candidates = Array.from(
                    document.querySelectorAll(
                        "tp-yt-paper-radio-button, tp-yt-paper-item, label, button, div[role='radio'], [role='option'], [aria-label]"
                    )
                ).filter((el) => {
                    const text = (el.innerText || el.textContent || el.getAttribute('aria-label') || '').trim();
                    return publicRe.test(text) && visible(el);
                });

                for (const el of candidates) {
                    const targets = [
                        el.querySelector('[role="radio"]'),
                        el.querySelector('#radioContainer'),
                        el.querySelector('#radioLabel'),
                        el.querySelector('label'),
                        el,
                    ].filter(Boolean);

                    for (const target of targets) {
                        try {
                            target.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
                        } catch (_) {}
                        try {
                            target.click();
                        } catch (_) {}
                    }
                }

                return { clicked: candidates.length > 0, count: candidates.length };
            }
            """
        )
        if result.get("clicked"):
            log(f"Public 文案兜底点击已执行 ({result.get('count', 0)} 个候选)", "OK")
            await asyncio.sleep(1)
            return True
    except Exception as e:
        log(f"Public 文案兜底点击异常: {e}", "WARN")
    return False


async def try_click_publish_button(page) -> bool:
    """外层 host 隐藏时，直接点内部 Publish 按钮。"""
    try:
        result = await page.evaluate(
            """
            () => {
                const dialogs = document.querySelectorAll('ytcp-uploads-dialog, ytcp-dialog, tp-yt-paper-dialog');
                const publishRe = /done|save|publish|schedule|完成|保存|yayınla|發佈|发布|公開|定時|定时|排程/i;
                const visible = (el) => !!el && (el.offsetParent !== null || el.offsetWidth > 0 || el.offsetHeight > 0);

                for (const dialog of dialogs) {
                    if (!visible(dialog)) continue;
                    const buttons = dialog.querySelectorAll('ytcp-button, button');
                    for (const btn of buttons) {
                        const text = (btn.innerText || btn.textContent || '').trim();
                        const id = btn.id || '';
                        const disabled = btn.getAttribute('aria-disabled') === 'true' || btn.disabled === true;
                        if (disabled) continue;
                        if (id !== 'done-button' && !publishRe.test(text)) continue;

                        const target = btn.querySelector('button') || btn;
                        target.click();
                        return { success: true, text: text || id || 'publish_or_schedule' };
                    }
                }
                return { success: false };
            }
            """
        )
        if result.get("success"):
            log(f"Publish DOM 兜底点击成功: {result.get('text', 'publish')}", "OK")
            return True
    except Exception as e:
        log(f"Publish DOM 兜底点击异常: {e}", "WARN")
    return False


async def try_publish_from_video_edit_page(
    page,
    *,
    serial: int,
    visibility: str,
    scheduled_publish_at: str | None = None,
    schedule_timezone: str | None = None,
    debug_port: int | None = None,
    context=None,
) -> Optional[Dict[str, Any]]:
    current_url = str(getattr(page, "url", "") or "")
    try:
        visibility_button = page.locator("ytcp-video-metadata-visibility #select-button").first
        has_visibility_button = await visibility_button.count() > 0 and await visibility_button.is_visible()
    except Exception:
        has_visibility_button = False

    if "/video/" not in current_url and not has_visibility_button:
        return None

    log("检测到 Studio 视频编辑页，改走侧边栏公开流程", "WARN")

    try:
        if not has_visibility_button:
            return make_upload_result(False, True, "编辑页缺少公开范围入口", "edit_page_visibility_missing")

        async def _dialog_open() -> bool:
            try:
                save_btn = page.locator("ytcp-button#save-button, #save-button").first
                if await save_btn.count() > 0 and await save_btn.is_visible():
                    return True
            except Exception:
                pass
            try:
                public_radio = page.locator("tp-yt-paper-radio-button[name='PUBLIC']").first
                if await public_radio.count() > 0 and await public_radio.is_visible():
                    return True
            except Exception:
                pass
            return False

        dialog_opened = await _dialog_open()
        for attempt in range(1, 4):
            if dialog_opened:
                break
            clicked = await human_click(page, visibility_button, f"编辑页公开范围#{attempt}")
            await asyncio.sleep(1.0)
            dialog_opened = await _dialog_open()
            if dialog_opened:
                break
            if not clicked or not dialog_opened:
                await visibility_button.click(force=True, timeout=5000)
                await asyncio.sleep(1.0)
                dialog_opened = await _dialog_open()
            if dialog_opened:
                break
            try:
                await visibility_button.focus()
                await page.keyboard.press("Enter")
            except Exception:
                pass
            await asyncio.sleep(0.8)
            dialog_opened = await _dialog_open()
        if not dialog_opened:
            return make_upload_result(False, True, "编辑页公开范围弹窗未能打开", "edit_page_visibility_dialog_missing")

        log(f"设置可见性 = {visibility}...")
        if str(visibility or "").strip().lower() == "public":
            visibility_ok = await ensure_public_visibility_selected(page)
            if not visibility_ok:
                visibility_ok = await apply_visibility_settings(
                    page,
                    visibility,
                    scheduled_publish_at=scheduled_publish_at,
                    schedule_timezone=schedule_timezone,
                )
        else:
            visibility_ok = await apply_visibility_settings(
                page,
                visibility,
                scheduled_publish_at=scheduled_publish_at,
                schedule_timezone=schedule_timezone,
            )
        if not visibility_ok:
            return make_upload_result(False, True, f"未能确认可见性 = {visibility}", "visibility_selection_failed")

        publish_pattern = "done|save|publish|schedule|完成|保存|yayınla|發佈|发布|公開|定時|定时|排程"
        publish_state = await get_visible_upload_dialog_button_state(
            page,
            button_id="save-button",
            text_pattern=publish_pattern,
        )

        if not publish_state.get("found"):
            log("编辑页未找到最终提交按钮，尝试 DOM 兜底直接点击", "WARN")
            publish_clicked = await try_click_publish_button(page)
            if not publish_clicked:
                return make_upload_result(False, True, "编辑页未找到最终提交按钮", "publish_button_missing")
            await asyncio.sleep(2)
            dialog_result = await handle_publish_anyway_dialog(
                page,
                serial=serial,
                max_wait_seconds=15,
                poll_seconds=1,
            )
            if dialog_result.get("detected") and not dialog_result.get("clicked"):
                log("检测到内容检查提示，但未能自动点击 'Publish anyway'，后续监控会继续重试", "WARN")
            publish_state = {"found": True, "disabled": False, "clicked_via_dom": True}

        if publish_state.get("disabled"):
            top_save = page.locator("ytcp-button#save, #save").first
            try:
                if await top_save.count() > 0 and await top_save.is_visible():
                    top_disabled = ((await top_save.get_attribute("aria-disabled")) or "").lower() == "true"
                    if not top_disabled:
                        log("编辑页先保存详情，等待最终提交按钮可点击", "WARN")
                        clicked = await human_click(page, top_save, "编辑页保存")
                        if not clicked:
                            await top_save.click(force=True, timeout=5000)
                        await asyncio.sleep(2.0)
            except Exception as save_exc:
                log(f"编辑页保存详情失败: {save_exc}", "WARN")

            deadline = time.monotonic() + 2 * 60 * 60
            wait_round = 0
            while time.monotonic() < deadline:
                wait_round += 1
                publish_state = await get_visible_upload_dialog_button_state(
                    page,
                    button_id="save-button",
                    text_pattern=publish_pattern,
                )
                if publish_state.get("found") and not publish_state.get("disabled"):
                    break
                if wait_round == 1 or wait_round % 6 == 0:
                    try:
                        snapshot = await get_best_upload_monitor_snapshot(page, context=context)
                        log(
                            f"编辑页最终提交按钮仍不可点，继续等待: {summarize_upload_monitor(snapshot)}",
                            "WAIT",
                        )
                    except Exception:
                        log("编辑页最终提交按钮仍不可点，继续等待 YouTube 处理/上传", "WAIT")
                await asyncio.sleep(10)
            if publish_state.get("disabled"):
                return make_upload_result(
                    False,
                    False,
                    "编辑页最终提交按钮仍不可点击，继续保留浏览器等待",
                    "publish_pending_monitor",
                    extra={"debug_port": debug_port},
                )

        if not publish_state.get("clicked_via_dom"):
            log("点击最终提交按钮...", "ACT")
            publish_clicked = await click_visible_upload_dialog_button(
                page,
                "Done / Publish / Schedule",
                button_id="save-button",
                text_pattern=publish_pattern,
            )
            if not publish_clicked:
                publish_clicked = await try_click_publish_button(page)
            if not publish_clicked:
                return make_upload_result(False, True, "点击最终提交按钮失败", "publish_click_failed")
            await asyncio.sleep(2)
            dialog_result = await handle_publish_anyway_dialog(
                page,
                serial=serial,
                max_wait_seconds=15,
                poll_seconds=1,
            )
            if dialog_result.get("detected") and not dialog_result.get("clicked"):
                log("检测到内容检查提示，但未能自动点击 'Publish anyway'，后续监控会继续重试", "WARN")
            log("✅ 已点击最终提交按钮!", "OK")

        monitor_result = await wait_for_safe_close_after_publish(page, serial, context=context)
        if not monitor_result.get("confirmed"):
            log("=" * 60)
            log("⚠️ Publish 已点击，但尚未确认上传完成，浏览器将保持打开", "WARN")
            log(f"原因: {monitor_result.get('reason', '未知')}", "WARN")
            log("=" * 60)
            return make_upload_result(
                False,
                False,
                monitor_result.get("reason", "未确认安全关闭状态"),
                "publish_pending_monitor",
                monitor_result.get("snapshot"),
                extra={"debug_port": debug_port},
            )

        final_state = monitor_result.get("snapshot", {}).get("status", "safe_to_close")
        log("=" * 60)
        log(f"✅ 已确认上传进入安全关闭状态: {final_state}", "OK")
        log(f"监控摘要: {monitor_result.get('reason', '')}", "OK")
        log("=" * 60)
        return make_upload_result(
            True,
            True,
            monitor_result.get("reason", "已确认安全关闭"),
            str(final_state),
            monitor_result.get("snapshot"),
        )
    except Exception as exc:
        return make_upload_result(False, True, f"编辑页发布失败: {exc}", "edit_page_publish_failed")


async def wait_for_monetization_section(page, timeout_ms: int = 15000) -> bool:
    """等待 Monetization 页面/区域真正出现。"""
    deadline = time.monotonic() + max(1, timeout_ms) / 1000.0

    while time.monotonic() < deadline:
        try:
            locator = page.locator("ytcp-video-monetization").first
            if await locator.count() > 0 and await locator.is_visible():
                return True
        except Exception:
            pass

        try:
            ready = await page.evaluate(
                """
                () => {
                    const bodyText = (document.body && document.body.innerText) ? document.body.innerText : '';
                    return /Monetization|營利|收益|获利/i.test(bodyText);
                }
                """
            )
            if ready:
                return True
        except Exception:
            pass

        await asyncio.sleep(1)

    return False


async def is_monetization_on(page) -> bool:
    """确认 YPP 收益状态是否已经明确为 On。"""
    try:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const isChecked = (el) => !!el && (
                    el.getAttribute('aria-checked') === 'true' ||
                    el.hasAttribute('checked') ||
                    el.classList.contains('checked') ||
                    el.getAttribute('aria-selected') === 'true'
                );
                const textOf = (el) => (el?.innerText || el?.textContent || el?.getAttribute?.('aria-label') || '').trim();

                const roots = Array.from(document.querySelectorAll('ytcp-video-monetization')).filter(visible);
                if (!roots.length) roots.push(document);

                const onTextRe = /^(on|開啟|开启|启用)$/i;

                for (const root of roots) {
                    const exactOnRadio = Array.from(root.querySelectorAll("tp-yt-paper-radio-button[name='ON']")).find((el) => {
                        const roleRadio = el.querySelector('[role="radio"]');
                        return visible(el) || visible(roleRadio);
                    });
                    if (exactOnRadio) {
                        const roleRadio = exactOnRadio.querySelector('[role="radio"]');
                        if (isChecked(exactOnRadio) || isChecked(roleRadio)) {
                            return true;
                        }
                    }

                    const candidates = Array.from(
                        root.querySelectorAll("tp-yt-paper-radio-button, tp-yt-paper-item, [role='option'], [role='radio'], button, label, div")
                    ).filter((el) => {
                        if (!visible(el)) return false;
                        const text = textOf(el);
                        return onTextRe.test(text);
                    });

                    for (const el of candidates) {
                        const radio = el.matches("tp-yt-paper-radio-button") ? el : el.closest('tp-yt-paper-radio-button');
                        const roleRadio = radio?.querySelector('[role="radio"]') || (el.getAttribute('role') === 'radio' ? el : el.querySelector('[role="radio"]'));
                        if (isChecked(el) || isChecked(radio) || isChecked(roleRadio)) {
                            return true;
                        }
                    }

                    const trigger = root.querySelector("ytcp-dropdown-trigger, .clickable, .m10n-text");
                    const triggerText = textOf(trigger);
                    if (/\bOn\b/i.test(triggerText) && !/\bOff\b/i.test(triggerText)) {
                        return true;
                    }
                }

                return false;
            }
            """
        )
    except Exception:
        return False


async def try_click_monetization_on(page) -> bool:
    """在 Monetization 页面中按文案兜底点击 On。"""
    try:
        result = await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const textOf = (el) => (el?.innerText || el?.textContent || el?.getAttribute?.('aria-label') || '').trim();
                const onTextRe = /^(on|開啟|开启|启用)$/i;
                const roots = Array.from(document.querySelectorAll('ytcp-video-monetization, ytcp-dialog, tp-yt-paper-dialog')).filter(visible);
                if (!roots.length) roots.push(document);

                let clicks = 0;
                for (const root of roots) {
                    const rootText = textOf(root);
                    if ((root.matches('ytcp-dialog, tp-yt-paper-dialog')) && !/Monetization|營利|收益|获利/i.test(rootText)) {
                        continue;
                    }

                    const candidates = Array.from(
                        root.querySelectorAll("tp-yt-paper-radio-button[name='ON'], tp-yt-paper-item, [role='option'], [role='radio'], button, label, div")
                    ).filter((el) => {
                        if (!visible(el)) return false;
                        if (el.matches("tp-yt-paper-radio-button[name='ON']")) return true;
                        const text = textOf(el);
                        return onTextRe.test(text);
                    });

                    for (const el of candidates) {
                        const targets = [
                            el.querySelector('[role="radio"]'),
                            el.querySelector('#radioContainer'),
                            el.querySelector('#radioLabel'),
                            el.querySelector('label'),
                            el,
                        ].filter(Boolean);

                        for (const target of targets) {
                            try {
                                target.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
                            } catch (_) {}
                            try {
                                target.click();
                                clicks += 1;
                            } catch (_) {}
                        }
                    }
                }

                return { clicked: clicks > 0, clicks };
            }
            """
        )
        if result.get("clicked"):
            log(f"Monetization On 文案兜底点击已执行 ({result.get('clicks', 0)} 次)", "OK")
            await asyncio.sleep(1)
            return True
    except Exception as e:
        log(f"Monetization On 文案兜底点击异常: {e}", "WARN")
    return False


async def try_click_monetization_done(page) -> bool:
    """点击 Monetization 面板中的 Done/Save。"""
    done_selectors = [
        'ytcp-video-monetization ytcp-button:has-text("Done")',
        'ytcp-video-monetization ytcp-button:has-text("Save")',
        'ytcp-video-monetization button:has-text("Done")',
        'ytcp-video-monetization button:has-text("Save")',
    ]

    for sel in done_selectors:
        try:
            locator = page.locator(sel).first
            if await locator.count() > 0 and await locator.is_visible():
                return await human_click(page, locator, f"Monetization 完成按钮 ({sel})")
        except Exception:
            continue

    try:
        result = await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const textOf = (el) => (el?.innerText || el?.textContent || '').trim();
                const btnRe = /^(done|save|完成|保存)$/i;
                const roots = Array.from(document.querySelectorAll('ytcp-video-monetization, ytcp-dialog, tp-yt-paper-dialog')).filter(visible);
                if (!roots.length) roots.push(document);

                for (const root of roots) {
                    const rootText = textOf(root);
                    if ((root.matches('ytcp-dialog, tp-yt-paper-dialog')) && !/Monetization|營利|收益|获利/i.test(rootText)) {
                        continue;
                    }

                    const buttons = Array.from(root.querySelectorAll('ytcp-button, button')).filter((el) => {
                        const text = textOf(el);
                        if (!visible(el) || !btnRe.test(text)) return false;
                        return el.getAttribute('aria-disabled') !== 'true' && el.disabled !== true;
                    });

                    for (const btn of buttons) {
                        const target = btn.querySelector('button') || btn;
                        try {
                            target.click();
                            return true;
                        } catch (_) {}
                    }
                }
                return false;
            }
            """
        )
        if result:
            log("Monetization Done/Save DOM 兜底点击成功", "OK")
            await asyncio.sleep(1)
            return True
    except Exception as e:
        log(f"Monetization Done/Save DOM 兜底点击异常: {e}", "WARN")

    return False


async def ensure_monetization_enabled(page) -> bool:
    """YPP 收益必须显式确认 = On，否则不允许继续。"""
    if await is_monetization_on(page):
        log("Monetization 已经是 On", "OK")
        return True

    selectors = [
        "ytcp-video-monetization .clickable",
        ".ytcp-video-monetization .clickable",
        ".m10n-text",
        "ytcp-video-monetization ytcp-dropdown-trigger",
    ]

    for sel in selectors:
        try:
            dropdown = page.locator(sel).first
            if await dropdown.count() > 0 and await dropdown.is_visible():
                await human_click(page, dropdown, f"Monetization 下拉框 ({sel})")
                await asyncio.sleep(1)
                break
        except Exception:
            continue

    on_locator = page.locator("tp-yt-paper-radio-button[name='ON']").first
    try:
        if await on_locator.count() > 0 and await on_locator.is_visible():
            await human_click(page, on_locator, "Monetization On")
            await asyncio.sleep(1)
    except Exception:
        pass

    await try_click_monetization_on(page)
    await asyncio.sleep(1)

    if not await is_monetization_on(page):
        await try_click_monetization_on(page)
        await asyncio.sleep(1)

    if not await is_monetization_on(page):
        return False

    await try_click_monetization_done(page)
    await asyncio.sleep(2)

    return await is_monetization_on(page)


async def _safe_accept_dialog(dialog) -> None:
    try:
        await dialog.accept()
    except Exception:
        pass


async def reload_with_optional_dialog(page, timeout: int = 15000) -> None:
    """刷新页面时安全处理可能出现的 beforeunload 对话框。"""
    try:
        page.once("dialog", lambda dialog: asyncio.create_task(_safe_accept_dialog(dialog)))
    except Exception:
        pass
    await page.reload(timeout=timeout)


async def human_fill(page, locator, text, desc=""):
    """人性化填充 - 直接填充 + 适当延迟"""
    log(f"填写: {desc} ({len(text)} 字符)", "ACT")
    try:
        await clear_blocking_overlays(page, "pre-fill")
        await locator.wait_for(state="visible", timeout=10000)
        await locator.click()
        await asyncio.sleep(random.uniform(0.3, 0.5))
        await page.keyboard.press("Control+a" if IS_WINDOWS else "Meta+a")
        await asyncio.sleep(random.uniform(0.1, 0.2))
        await locator.fill(text)
        await asyncio.sleep(random.uniform(0.5, 1.0))
        return True
    except Exception as e:
        if "intercepts pointer events" in str(e) or "overlay-backdrop" in str(e):
            try:
                await clear_blocking_overlays(page, "fill-retry")
                await locator.click(force=True, timeout=5000)
                await page.keyboard.press("Control+a" if IS_WINDOWS else "Meta+a")
                await locator.fill(text)
                await asyncio.sleep(0.6)
                return True
            except Exception:
                pass
        log(f"填写失败: {e}", "WARN")
        return False


async def fill_visible_upload_field(page, field_name: str, text: str) -> bool:
    """隐藏宿主层命中时，直接填 visible uploads dialog 里的真实输入框。"""
    try:
        result = await page.evaluate(
            """
            ({ fieldName, value }) => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );

                const roots = Array.from(
                    document.querySelectorAll('ytcp-uploads-dialog, ytcp-dialog, tp-yt-paper-dialog')
                ).filter(visible);
                if (!roots.length) roots.push(document);

                const selectorMap = {
                    title: [
                        '#title-textarea #textbox',
                        'ytcp-social-suggestions-textbox#title-textarea #textbox',
                        '#title-textarea [contenteditable="true"]',
                        '#title-textarea textarea',
                        '#title-textarea input[type="text"]',
                        'input[type="text"]',
                    ],
                    description: [
                        '#description-textarea #textbox',
                        'ytcp-social-suggestions-textbox#description-textarea #textbox',
                        '#description-textarea [contenteditable="true"]',
                        '#description-textarea textarea',
                    ],
                    tags: [
                        '#tags-input #textbox',
                        '#tags-input textarea',
                        '#tags-input input[type="text"]',
                        'ytcp-video-tags #textbox',
                        'ytcp-video-tags textarea',
                        'ytcp-video-tags input[type="text"]',
                        'input[aria-label*="Tags"]',
                        'textarea[aria-label*="Tags"]',
                        'input[aria-label*="标签"]',
                        'textarea[aria-label*="标签"]',
                        'input[placeholder*="标签"]',
                        'textarea[placeholder*="标签"]',
                    ],
                };

                const selectors = selectorMap[fieldName] || [];
                for (const root of roots) {
                    for (const selector of selectors) {
                        const nodes = Array.from(root.querySelectorAll(selector)).filter(visible);
                        if (!nodes.length) continue;
                        const el = nodes[0];
                        el.focus();
                        if (el.matches('textarea, input')) {
                            const setter = Object.getOwnPropertyDescriptor(el.__proto__, 'value')?.set;
                            if (setter) {
                                setter.call(el, value);
                            } else {
                                el.value = value;
                            }
                        } else {
                            el.textContent = value;
                        }
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        return {
                            success: true,
                            selector,
                            tag: el.tagName,
                        };
                    }
                }

                return { success: false };
            }
            """,
            {"fieldName": field_name, "value": text},
        )
        if result.get("success"):
            log(
                f"{field_name} DOM 填充成功: {result.get('selector', '')} ({result.get('tag', '')})",
                "OK",
            )
            return True
    except Exception as e:
        log(f"{field_name} DOM 填充异常: {e}", "WARN")
    return False


async def click_visible_upload_dialog_button(
    page,
    desc: str,
    button_id: str = "",
    text_pattern: str = "",
) -> bool:
    """当 Playwright 命中隐藏宿主时，直接点 visible dialog 里的真实按钮。"""
    try:
        result = await page.evaluate(
            """
            ({ buttonId, textPattern }) => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const pattern = textPattern ? new RegExp(textPattern, 'i') : null;
                const roots = Array.from(
                    document.querySelectorAll('ytcp-uploads-dialog, ytcp-dialog, tp-yt-paper-dialog')
                ).filter(visible);
                if (!roots.length) roots.push(document);

                for (const root of roots) {
                    const buttons = root.querySelectorAll('ytcp-button, tp-yt-paper-button, button');
                    for (const btn of buttons) {
                        const target = btn.querySelector('button') || btn;
                        const id = btn.id || target.id || '';
                        const text = (btn.innerText || target.innerText || btn.textContent || target.textContent || '').trim();
                        const disabled =
                            btn.getAttribute('aria-disabled') === 'true' ||
                            target.getAttribute('aria-disabled') === 'true' ||
                            btn.disabled === true ||
                            target.disabled === true;
                        if ((!visible(btn)) && (!visible(target))) continue;
                        if (disabled) continue;
                        if (buttonId && id !== buttonId) continue;
                        if (pattern && !pattern.test(text) && !pattern.test(id)) continue;

                        target.click();
                        return { success: true, text: text || id || buttonId || 'button' };
                    }
                }
                return { success: false };
            }
            """,
            {"buttonId": button_id, "textPattern": text_pattern},
        )
        if result.get("success"):
            log(f"{desc} DOM 兜底点击成功: {result.get('text', '')}", "OK")
            return True
    except Exception as e:
        log(f"{desc} DOM 兜底点击异常: {e}", "WARN")
    return False


async def get_visible_upload_dialog_button_state(
    page,
    button_id: str = "",
    text_pattern: str = "",
) -> Dict[str, Any]:
    """读取 visible dialog 中目标按钮的真实状态，避免被隐藏宿主误导。"""
    try:
        result = await page.evaluate(
            """
            ({ buttonId, textPattern }) => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                const pattern = textPattern ? new RegExp(textPattern, 'i') : null;
                const roots = Array.from(
                    document.querySelectorAll('ytcp-uploads-dialog, ytcp-dialog, tp-yt-paper-dialog')
                ).filter(visible);
                if (!roots.length) roots.push(document);

                for (const root of roots) {
                    const buttons = root.querySelectorAll('ytcp-button, tp-yt-paper-button, button');
                    for (const btn of buttons) {
                        const target = btn.querySelector('button') || btn;
                        const id = btn.id || target.id || '';
                        const text = (btn.innerText || target.innerText || btn.textContent || target.textContent || '').trim();
                        if ((!visible(btn)) && (!visible(target))) continue;
                        if (buttonId && id !== buttonId) continue;
                        if (pattern && !pattern.test(text) && !pattern.test(id)) continue;

                        const disabled =
                            btn.getAttribute('aria-disabled') === 'true' ||
                            target.getAttribute('aria-disabled') === 'true' ||
                            btn.disabled === true ||
                            target.disabled === true;
                        return { found: true, disabled, text, id };
                    }
                }
                return { found: false, disabled: false, text: '', id: '' };
            }
            """,
            {"buttonId": button_id, "textPattern": text_pattern},
        )
        return result if isinstance(result, dict) else {"found": False, "disabled": False}
    except Exception as e:
        log(f"读取按钮状态异常: {e}", "WARN")
        return {"found": False, "disabled": False, "error": str(e)}


async def click_next_button(page, desc: str = "Next", timeout_ms: int = 25000) -> bool:
    """稳健点击上传弹窗中的 Next 按钮（处理隐藏宿主/禁用态/句柄失效）。"""
    deadline = time.monotonic() + max(1, timeout_ms) / 1000.0
    attempt = 0

    async def _is_enabled(next_btn) -> bool:
        try:
            host_disabled = await next_btn.get_attribute("aria-disabled")
            if host_disabled == "true":
                return False
        except Exception:
            pass

        try:
            inner = next_btn.locator("button").first
            if await inner.count() > 0:
                inner_disabled = await inner.get_attribute("aria-disabled")
                if inner_disabled == "true":
                    return False
                dom_disabled = await inner.evaluate("el => el.disabled === true")
                if dom_disabled:
                    return False
        except Exception:
            pass

        return True

    while time.monotonic() < deadline:
        attempt += 1
        await clear_blocking_overlays(page, f"next-{attempt}")
        total_candidates = 0
        next_name_re = re.compile(r"^(next|continue|下一步|继续|繼續|下一個)$", re.IGNORECASE)

        candidate_groups: List[Any] = []
        upload_dialog = page.locator("ytcp-uploads-dialog").first
        try:
            if await upload_dialog.count() > 0:
                candidate_groups.extend(
                    [
                        upload_dialog.locator("ytcp-button#next-button"),
                        upload_dialog.locator("button#next-button"),
                        upload_dialog.locator("button:has-text('Next')"),
                        upload_dialog.locator("tp-yt-paper-button:has-text('Next')"),
                        upload_dialog.get_by_role("button", name=next_name_re),
                    ]
                )
        except Exception:
            pass

        candidate_groups.extend(
            [
                page.locator("ytcp-button#next-button"),
                page.locator("button#next-button"),
                page.locator("button:has-text('Next')"),
                page.locator("tp-yt-paper-button:has-text('Next')"),
                page.get_by_role("button", name=next_name_re),
            ]
        )

        for group in candidate_groups:
            try:
                count = await group.count()
            except Exception:
                continue
            if count <= 0:
                continue
            total_candidates += count

            for idx in range(min(count, 6)):
                next_btn = group.nth(idx)
                try:
                    if not await next_btn.is_visible():
                        continue
                except Exception:
                    continue

                if not await _is_enabled(next_btn):
                    continue

                clicked = await human_click(page, next_btn, f"{desc} (idx={idx})")
                if clicked:
                    return True

                try:
                    await next_btn.scroll_into_view_if_needed()
                    await next_btn.click(timeout=3000, force=True)
                    return True
                except Exception:
                    pass

                try:
                    inner = next_btn.locator("button").first
                    if await inner.count() > 0:
                        await inner.scroll_into_view_if_needed()
                        await inner.click(timeout=3000, force=True)
                        return True
                except Exception:
                    pass

        state = await get_visible_upload_dialog_button_state(
            page,
            button_id="next-button",
            text_pattern="next",
        )
        upload_progress = ""
        if total_candidates == 0:
            try:
                upload_progress = await page.evaluate(
                    """
                    () => {
                        const texts = Array.from(
                            document.querySelectorAll('ytcp-uploads-dialog, ytcp-uploads-dialog *')
                        ).map((el) => (el.innerText || el.textContent || '').trim()).filter(Boolean);
                        const re = /(\\d{1,3})\\s*%\\s*uploaded/i;
                        for (const t of texts) {
                            const m = t.match(re);
                            if (m) return `${m[1]}% uploaded`;
                        }
                        return "";
                    }
                    """
                )
            except Exception:
                upload_progress = ""
        if attempt == 1 or attempt % 5 == 0:
            log(
                f"{desc} 等待可点击: candidates={total_candidates} found={state.get('found')} disabled={state.get('disabled')} text={state.get('text', '')} progress={upload_progress}",
                "INFO",
            )
        if state.get("found") and not state.get("disabled"):
            dom_clicked = await click_visible_upload_dialog_button(
                page,
                desc,
                button_id="next-button",
                text_pattern="next",
            )
            if dom_clicked:
                return True

        await asyncio.sleep(0.8)

    return False


async def scroll_dialog(page, pixels):
    """在对话框内滚动 (验证成功 2026-01-30)
    使用 #scrollable-content 而不是鼠标滚轮，更可靠
    """
    content = page.locator('ytcp-uploads-dialog #scrollable-content').first
    if await content.count() > 0:
        await content.evaluate(f'el => el.scrollTop += {pixels}')
    else:
        # 备用: 用鼠标滚轮
        await page.mouse.wheel(0, pixels)


async def handle_publish_anyway_dialog(
    page,
    serial: Optional[int] = None,
    max_wait_seconds: float = 0.0,
    poll_seconds: float = 1.0,
) -> Dict[str, Any]:
    """检测并处理 Publish 后偶发出现的“仍在检查内容”告警弹窗。"""
    deadline = time.monotonic() + max(0.0, max_wait_seconds)
    last_result: Dict[str, Any] = {
        "detected": False,
        "clicked": False,
        "dialog_text": "",
        "button_text": "",
    }

    while True:
        try:
            result = await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0
                    );

                    const dialogRe = /still checking your video|still checking your content|recommend keeping your content private until checks complete|wait for our checks to finish before publishing|save your video as private or unlisted|checking your video|checking your content|正在检查您的视频|正在检查你的视频|正在检查您的内容|正在检查你的内容|仍在检查您的视频|仍在检查你的视频|仍在检查您的内容|仍在检查你的内容|內容仍在檢查影片|內容仍在檢查視頻|內容仍在檢查|內容還在檢查/i;
                    const publishAnywayRe = /publish anyway|仍要發布|仍要发布|仍要發佈|yine de yayınla|publier quand même|pubblica comunque/i;
                    const dialogs = document.querySelectorAll('ytcp-dialog, tp-yt-paper-dialog');
                    let detectedDialog = null;

                    for (const dialog of dialogs) {
                        if (!visible(dialog)) continue;

                        const dialogText = (dialog.innerText || '').trim();
                        if (!dialogRe.test(dialogText)) continue;
                        detectedDialog = dialogText;

                        const buttonSelectors = [
                            'ytcp-button#secondary-action-button',
                            '#secondary-action-button',
                            'ytcp-button',
                            'button',
                        ];

                        for (const selector of buttonSelectors) {
                            const buttons = dialog.querySelectorAll(selector);
                            for (const btn of buttons) {
                                if (!visible(btn)) continue;
                                const btnText = (btn.innerText || btn.textContent || '').trim();
                                const btnId = btn.id || '';
                                if (!publishAnywayRe.test(btnText) && btnId !== 'secondary-action-button') {
                                    continue;
                                }
                                (btn.querySelector('button') || btn).click();
                                return {
                                    detected: true,
                                    clicked: true,
                                    dialog_text: dialogText.slice(0, 240),
                                    button_text: btnText || btnId || 'Publish anyway',
                                };
                            }
                        }
                    }

                    if (detectedDialog) {
                        return {
                            detected: true,
                            clicked: false,
                            dialog_text: detectedDialog.slice(0, 240),
                            button_text: '',
                        };
                    }

                    return {
                        detected: false,
                        clicked: false,
                        dialog_text: '',
                        button_text: '',
                    };
                }
                """
            )
        except Exception as e:
            return {
                "detected": False,
                "clicked": False,
                "dialog_text": str(e),
                "button_text": "",
            }

        last_result = result if isinstance(result, dict) else last_result

        if last_result.get("clicked"):
            prefix = f"序号 {serial}: " if serial is not None else ""
            button_text = last_result.get("button_text") or "Publish anyway"
            log(f"{prefix}检测到内容检查提示，已自动点击 '{button_text}'", "OK")
            await asyncio.sleep(2)
            return last_result

        if time.monotonic() >= deadline:
            return last_result

        await asyncio.sleep(max(0.2, poll_seconds))


async def get_upload_monitor_snapshot(page) -> Dict[str, Any]:
    """读取 YouTube Studio 当前上传状态。"""
    if page.is_closed():
        return {
            "status": "page_closed",
            "progress_pct": None,
            "progress_text": "",
            "dialog_visible": False,
            "progress_component_present": False,
            "active_uploading": False,
            "active_processing": False,
            "published_confirmed": False,
            "page_url": "",
        }

    snapshot = await page.evaluate(
        """
        () => {
            const result = {
                status: 'unknown',
                progress_pct: null,
                progress_text: '',
                dialog_visible: false,
                progress_component_present: false,
                active_uploading: false,
                active_processing: false,
                active_checking: false,
                checks_complete: false,
                upload_completed: false,
                published_confirmed: false,
                content_row_text: '',
                content_row_pending: false,
                content_row_cancel_available: false,
                content_row_resume_available: false,
                page_url: location.href,
            };

            const dlg = document.querySelector('ytcp-uploads-dialog');
            const prog = document.querySelector('ytcp-video-upload-progress');
            const hover = document.querySelector('ytcp-video-upload-progress-hover');

            if (dlg && (dlg.offsetParent !== null || dlg.offsetWidth > 0 || dlg.offsetHeight > 0)) {
                result.dialog_visible = true;
            }

            let progressText = '';
            if (prog) {
                result.progress_component_present = true;
                progressText = (prog.innerText || '').trim();
            }

            const hoverText = hover ? (hover.innerText || '').trim() : '';
            const dialogText = dlg ? (dlg.innerText || '').trim() : '';
            const bodyText = (document.body && document.body.innerText) ? document.body.innerText : '';
            const combinedText = [progressText, hoverText, dialogText, bodyText].filter(Boolean).join('\\n');

            result.progress_text = [progressText, hoverText].filter(Boolean).join(' | ').slice(0, 300);

            const pctMatch = combinedText.match(/(\\d+)%/);
            if (pctMatch) {
                result.progress_pct = parseInt(pctMatch[1], 10);
            }

            const uploadingRe = /Uploading|Yükleniyor|Téléversement|Caricamento|上[传傳]中|正在上传/i;
            const uploadDoneRe = /Video uploaded|Upload complete|Yükleme tamamlandı|Téléversement terminé|Caricamento completato|视频上传完毕|影片上傳完畢|上[传傳]完成|上传完成/i;
            const processingRe = /Processing video|Processing|İşleniyor|Traitement|Elaborazione|正在处理视频|处理中|處理中/i;
            const checkingRe = /Running checks|Checking|正在检查|正在檢查/i;
            const checksCompleteRe = /Checks complete|No issues found|检查完毕|檢查完畢|未发现任何问题|未發現任何問題/i;
            const publishedRe = /Video published|Video yayınlandı|Vidéo publiée|Video pubblicato|影片已發布|视频已发布/i;
            const pendingRe = /\\bPending\\b|待处理|待處理/i;
            const cancelUploadRe = /Cancel upload|取消上传|取消上傳/i;
            const resumeUploadRe = /Resume upload|恢复上传|恢復上傳|继续上传|繼續上傳/i;
            const rowPublishedRe = /\\bPublished\\b|已发布|已發佈/i;
            const rowScheduledRe = /\\bScheduled\\b|已排程|已定時|定时发布|定時發布/i;
            const rowUploadedRe = /\\bUploaded\\b|已上传|已上傳/i;
            const rowVisibilityRe = /\\b(Public|Private|Unlisted|Scheduled)\\b|公开|公開|私密|不公开|不公開|已排程|已定時/i;

            result.active_processing =
                processingRe.test(combinedText) &&
                !/Processing will start|處理即將開始|处理中即将开始/i.test(combinedText);
            result.active_uploading = uploadingRe.test(combinedText);
            result.active_checking = checkingRe.test(combinedText);
            result.checks_complete = checksCompleteRe.test(combinedText);
            result.upload_completed = uploadDoneRe.test(combinedText);
            result.published_confirmed = publishedRe.test(combinedText);

            if (result.active_uploading) {
                result.status = 'uploading';
            } else if (result.active_checking && !result.checks_complete) {
                result.status = 'checking';
            } else if (result.published_confirmed) {
                result.status = 'published';
            } else if (result.active_processing) {
                result.status = 'processing';
            } else if (result.checks_complete) {
                result.status = 'checks_complete';
            } else if (result.upload_completed) {
                result.status = 'upload_complete';
            }

            const latestVisibleRow = Array.from(document.querySelectorAll('ytcp-video-row'))
                .find((row) => row && (row.offsetParent !== null || row.offsetWidth > 0 || row.offsetHeight > 0));
            const latestRowText = latestVisibleRow ? (latestVisibleRow.innerText || '').trim() : '';
            if (latestRowText) {
                result.content_row_text = latestRowText.replace(/\\s+/g, ' ').slice(0, 300);
                result.content_row_pending = pendingRe.test(latestRowText);
                result.content_row_cancel_available = cancelUploadRe.test(latestRowText);
                result.content_row_resume_available = resumeUploadRe.test(latestRowText);
                if (!result.progress_text) {
                    result.progress_text = result.content_row_text;
                }
            }

            if (result.status === 'unknown' && latestRowText) {
                if (
                    result.content_row_pending &&
                    (result.content_row_cancel_available || result.content_row_resume_available)
                ) {
                    result.active_uploading = true;
                    result.status = 'uploading';
                } else if (result.content_row_pending) {
                    result.active_processing = true;
                    result.status = 'processing';
                } else if (rowPublishedRe.test(latestRowText)) {
                    result.published_confirmed = true;
                    result.status = 'published';
                } else if (rowScheduledRe.test(latestRowText)) {
                    result.upload_completed = true;
                    result.status = 'scheduled';
                } else if (rowUploadedRe.test(latestRowText) || rowVisibilityRe.test(latestRowText)) {
                    result.upload_completed = true;
                    result.status = 'upload_complete';
                }
            }

            return result;
        }
        """
    )
    return snapshot if isinstance(snapshot, dict) else {}


def _upload_monitor_status_rank(snapshot: Dict[str, Any]) -> float:
    status = str(snapshot.get("status") or "unknown").strip().lower()
    priority = {
        "published": 90,
        "scheduled": 80,
        "processing": 70,
        "checks_complete": 65,
        "upload_complete": 60,
        "checking": 50,
        "uploading": 40,
        "unknown": 10,
        "monitor_error": 0,
        "page_closed": -10,
    }
    base = float(priority.get(status, 5))
    pct = snapshot.get("progress_pct")
    if isinstance(pct, (int, float)):
        base += min(max(float(pct), 0.0), 100.0) / 1000.0
    if snapshot.get("published_confirmed"):
        base += 5.0
    if snapshot.get("dialog_visible"):
        base += 0.05
    return base


async def get_best_upload_monitor_snapshot(page, *, context=None) -> Dict[str, Any]:
    """Across reused CDP tabs, pick the most informative Studio upload snapshot."""
    preferred_pages = [page]
    if context is not None:
        for candidate in list(getattr(context, "pages", []) or []):
            if candidate not in preferred_pages:
                preferred_pages.append(candidate)

    best_snapshot: Dict[str, Any] | None = None
    best_rank = float("-inf")
    for candidate in preferred_pages:
        url = (getattr(candidate, "url", "") or "").lower()
        if not url:
            continue
        if "console.bitbrowser.net" in url or url.startswith("chrome-extension://"):
            continue

        try:
            snapshot = await get_upload_monitor_snapshot(candidate)
        except Exception as exc:
            snapshot = {
                "status": "monitor_error",
                "progress_pct": None,
                "progress_text": str(exc),
                "dialog_visible": False,
                "active_uploading": False,
                "active_processing": False,
                "active_checking": False,
                "published_confirmed": False,
                "page_url": getattr(candidate, "url", "") or "",
            }

        if not isinstance(snapshot, dict):
            continue

        snapshot["page_url"] = snapshot.get("page_url") or getattr(candidate, "url", "") or ""
        rank = _upload_monitor_status_rank(snapshot)
        if candidate is page:
            rank += 0.001

        if rank > best_rank:
            best_rank = rank
            best_snapshot = snapshot

    if best_snapshot is not None:
        return best_snapshot
    return await get_upload_monitor_snapshot(page)


def summarize_upload_monitor(snapshot: Dict[str, Any]) -> str:
    """格式化监控摘要，便于写入日志。"""
    status = snapshot.get("status", "unknown")
    pct = snapshot.get("progress_pct")
    pct_text = f"{pct}%" if pct is not None else "N/A"
    dialog_text = "开" if snapshot.get("dialog_visible") else "关"
    progress_text = (snapshot.get("progress_text") or "").strip()
    progress_text = re.sub(r"\s+", " ", progress_text)[:120]

    parts = [f"状态={status}", f"进度={pct_text}", f"对话框={dialog_text}"]
    if progress_text:
        parts.append(f"提示={progress_text}")
    return " | ".join(parts)


def is_safe_to_close_after_publish(snapshot: Dict[str, Any]) -> bool:
    """只在确认上传数据已送达 Studio 后才允许自动关闭。"""
    if not snapshot:
        return False

    if snapshot.get("active_uploading") or snapshot.get("active_checking"):
        return False

    status = snapshot.get("status")
    if status in {"upload_complete", "processing", "published", "checks_complete", "scheduled"}:
        return True

    return False


async def wait_for_safe_close_after_publish(
    page,
    serial: int,
    timeout_seconds: int = UPLOAD_SAFE_CLOSE_TIMEOUT_SECONDS,
    poll_seconds: int = UPLOAD_MONITOR_POLL_SECONDS,
    context=None,
) -> Dict[str, Any]:
    """点击 Publish 后持续监控，只有进入安全状态才允许关浏览器。"""
    started_at = time.monotonic()
    poll_count = 0
    stable_safe_polls = 0
    last_summary = None
    last_snapshot: Dict[str, Any] = {}

    log(
        f"序号 {serial}: Publish 已点击，开始监控 Studio 上传进度，确认安全后再关浏览器",
        "WAIT",
    )

    while True:
        poll_count += 1
        dialog_result = await handle_publish_anyway_dialog(page, serial=serial)
        if dialog_result.get("clicked"):
            stable_safe_polls = 0
        try:
            snapshot = await get_best_upload_monitor_snapshot(page, context=context)
        except Exception as e:
            snapshot = {
                "status": "monitor_error",
                "progress_pct": None,
                "progress_text": str(e),
                "dialog_visible": False,
                "active_uploading": False,
                "active_processing": False,
                "published_confirmed": False,
                "page_url": page.url if not page.is_closed() else "",
            }

        last_snapshot = snapshot
        summary = summarize_upload_monitor(snapshot)
        is_safe = is_safe_to_close_after_publish(snapshot)

        if summary != last_summary or poll_count == 1 or poll_count % 3 == 0:
            log(f"序号 {serial}: 监控#{poll_count} {summary}", "OK" if is_safe else "WAIT")
            last_summary = summary

        if is_safe:
            stable_safe_polls += 1
            if stable_safe_polls >= UPLOAD_SAFE_CLOSE_STABLE_POLLS:
                return {
                    "confirmed": True,
                    "reason": summary,
                    "snapshot": snapshot,
                }
        else:
            stable_safe_polls = 0

        elapsed = time.monotonic() - started_at
        if elapsed >= timeout_seconds:
            return {
                "confirmed": False,
                "reason": (
                    f"等待 {int(timeout_seconds / 60)} 分钟后仍未确认安全关闭状态，"
                    f"最后快照: {summary}"
                ),
                "snapshot": last_snapshot,
            }

        await asyncio.sleep(poll_seconds)

# ============ 配置读取 ============
def load_config():
    """加载配置文件"""
    if not CONFIG_PATH.exists():
        log(f"配置文件不存在: {CONFIG_PATH}", "ERR")
        sys.exit(1)
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# ============ Hubstudio API ============
def get_all_containers() -> List[Dict]:
    """获取全部环境列表（统一走浏览器适配层）。"""
    try:
        containers = list_browser_envs(CONFIG_PATH)
        normalized: List[Dict[str, Any]] = []
        for container in containers:
            item = dict(container)
            # 兼容旧逻辑：原代码大量使用 tagName 字段
            if "tagName" not in item:
                item["tagName"] = item.get("tag", "")
            normalized.append(item)
        return normalized
    except Exception as e:
        log(f"获取环境列表失败: {e}", "ERR")
        return []


def get_containers_by_tag(tag: str) -> List[Dict]:
    """按标签获取环境（兼容 tagName/tag 两种字段）。"""
    containers = get_all_containers()
    hubstudio_tag = get_hubstudio_tag_name(tag)
    wanted = {
        _normalize_tag_for_match(tag),
        _normalize_tag_for_match(hubstudio_tag),
    }
    matched = [
        c for c in containers
        if _normalize_tag_for_match(c.get("tagName", "")) in wanted
        or _normalize_tag_for_match(c.get("tag", "")) in wanted
    ]
    return sorted(matched, key=lambda x: int(x.get("serialNumber", 0) or 0))


def parse_serial_list(raw: Optional[str]) -> List[int]:
    """解析逗号分隔的频道序号列表。"""
    if not raw:
        return []

    serials: List[int] = []
    for token in re.split(r"[，,]", raw):
        token = token.strip()
        if token.isdigit():
            serials.append(int(token))
    return serials


def load_channels_registry(project_folder: Optional[Path]) -> Dict[int, str]:
    """从项目 channels.md 读取 serial -> channel_name。"""
    if not project_folder:
        return {}

    channels_md_path = project_folder / "channels.md"
    if not channels_md_path.exists():
        return {}

    serial_to_channel_name: Dict[int, str] = {}
    try:
        with open(channels_md_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("|") and not line.startswith("| 編號") and not line.startswith("|---"):
                    parts = [p.strip() for p in line.split("|")]
                    if len(parts) >= 5:
                        try:
                            cid = int(parts[3])
                            cname = parts[2]
                            if cname and not cname.startswith("频道"):
                                serial_to_channel_name[cid] = cname
                        except (ValueError, IndexError):
                            pass
    except Exception as e:
        log(f"读取 channels.md 失败: {e}", "WARN")
        return {}

    return serial_to_channel_name


def resolve_containers_for_tag(tag: str, project_folder: Optional[Path]) -> tuple[List[Dict], str, Dict[int, str]]:
    """优先按 HubStudio tag 找环境，找不到则回退到 channels.md 的 serial 列表。"""
    serial_to_channel_name = load_channels_registry(project_folder)

    tagged_containers = get_containers_by_tag(tag)
    if tagged_containers:
        return tagged_containers, "hubstudio_tag", serial_to_channel_name

    if serial_to_channel_name:
        wanted = set(serial_to_channel_name.keys())
        matched = [
            c for c in get_all_containers()
            if int(c.get("serialNumber", 0) or 0) in wanted
        ]
        matched = sorted(matched, key=lambda x: int(x.get("serialNumber", 0) or 0))
        if matched:
            log(
                f"未找到标签为 '{tag}' 的 HubStudio 环境，改用 channels.md 回退匹配: {sorted(wanted)}",
                "WARN",
            )
            return matched, "channels_md_fallback", serial_to_channel_name

    return [], "missing", serial_to_channel_name

def start_browser(container_code: str, max_retries: int = 2) -> Optional[int]:
    """启动浏览器并返回调试端口（带重试机制）"""
    import requests
    
    for attempt in range(max_retries + 1):
        try:
            debug_port = start_browser_debug_port(container_code, CONFIG_PATH)
            if debug_port:
                
                # 验证端口是否可达
                time.sleep(2)
                try:
                    check = requests.get(f"http://127.0.0.1:{debug_port}/json/version", timeout=5)
                    if check.status_code == 200:
                        log(f"浏览器端口 {debug_port} 验证成功", "OK")
                        return debug_port
                except:
                    log(f"端口 {debug_port} 验证失败，浏览器可能还在启动中...", "WARN")
                    time.sleep(3)
                    # 再试一次验证
                    try:
                        check = requests.get(f"http://127.0.0.1:{debug_port}/json/version", timeout=5)
                        if check.status_code == 200:
                            return debug_port
                    except:
                        pass
                
                # 验证失败但 API 返回成功，仍然返回端口
                return debug_port
            else:
                log("启动浏览器失败: 未返回调试端口", "ERR")
                if attempt < max_retries:
                    log(f"等待 5 秒后重试 ({attempt + 1}/{max_retries})...", "WARN")
                    time.sleep(5)
                else:
                    return None
        except Exception as e:
            log(f"启动浏览器异常: {e}", "ERR")
            if attempt < max_retries:
                log(f"等待 5 秒后重试 ({attempt + 1}/{max_retries})...", "WARN")
                time.sleep(5)
            else:
                return None
    
    return None

def stop_browser(container_code: str):
    """通过浏览器适配层关闭浏览器"""
    try:
        stop_browser_container(container_code, CONFIG_PATH)
        log(f"浏览器已关闭: {container_code}", "OK")
    except Exception as e:
        log(f"关闭浏览器异常: {e}", "WARN")


def is_retryable_browser_network_failure(upload_result: Dict[str, Any]) -> bool:
    """识别可通过重启浏览器恢复的代理/网络断线。"""
    if upload_result.get("success"):
        return False
    if not bool(upload_result.get("close_browser", True)):
        return False

    combined = "\n".join(
        [
            str(upload_result.get("stage", "") or ""),
            str(upload_result.get("reason", "") or ""),
        ]
    )
    return any(marker in combined for marker in RETRYABLE_NETWORK_ERROR_MARKERS)


async def upload_single_with_browser_recovery(
    container_code: str,
    serial: int,
    video_path: Path,
    thumbnails: List[Path],
    title: str,
    description: str,
    is_ypp: bool,
    ab_test_titles: Optional[List[str]] = None,
    playlist_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    visibility: str = "public",
    scheduled_publish_at: Optional[str] = None,
    schedule_timezone: Optional[str] = None,
    made_for_kids: bool = False,
    altered_content: bool = True,
    notify_subscribers: bool = False,
    category: str = "Music",
) -> Dict[str, Any]:
    """浏览器/代理断线时，停浏览器后自动重试一次。"""
    attempts = 2
    last_result = make_upload_result(False, True, "未开始上传", "not_started")

    for attempt in range(1, attempts + 1):
        if attempt > 1:
            log(
                f"序号 {serial}: 检测到浏览器/代理断线，停浏览器后重试 ({attempt}/{attempts})",
                "WARN",
            )
            stop_browser(container_code)
            await asyncio.sleep(NETWORK_RECOVERY_RETRY_WAIT_SECONDS)

        last_result = await upload_single(
            container_code=container_code,
            serial=serial,
            video_path=video_path,
            thumbnails=thumbnails,
            title=title,
            description=description,
            is_ypp=is_ypp,
            ab_test_titles=ab_test_titles,
            playlist_name=playlist_name,
            tags=tags,
            visibility=visibility,
            scheduled_publish_at=scheduled_publish_at,
            schedule_timezone=schedule_timezone,
            made_for_kids=made_for_kids,
            altered_content=altered_content,
            notify_subscribers=notify_subscribers,
            category=category,
        )

        if not is_retryable_browser_network_failure(last_result):
            return last_result

    return last_result


def launch_tail_close_watcher(
    serial: int,
    container_code: str,
    debug_port: Optional[int],
    tag: Optional[str] = None,
    date_mmdd: Optional[str] = None,
) -> bool:
    """主监控超时后，起一个独立 watcher 持续盯到安全状态再关窗。"""
    if not debug_port:
        return False

    watcher_script = SCRIPT_DIR / "upload_tail_close_watcher.py"
    if not watcher_script.exists():
        log(f"尾程 watcher 脚本不存在: {watcher_script}", "WARN")
        return False

    try:
        subprocess.Popen(
            [
                sys.executable,
                str(watcher_script),
                "--serial",
                str(serial),
                "--container-code",
                str(container_code),
                "--port",
                str(debug_port),
                "--timeout-seconds",
                str(TAIL_CLOSE_WATCHER_TIMEOUT_SECONDS),
            ]
            + (
                ["--tag", str(tag).strip()]
                if str(tag or "").strip()
                else []
            )
            + (
                ["--date", str(date_mmdd).strip()]
                if str(date_mmdd or "").strip()
                else []
            ),
            cwd=str(SCRIPT_DIR),
            start_new_session=True,
        )
        return True
    except Exception as e:
        log(f"启动尾程 watcher 失败: {e}", "WARN")
        return False

# ============ 自动识别成品文件夹 ============
def auto_detect_videos(config: Dict) -> Dict[str, Dict[str, List[Path]]]:
    """
    自动扫描 AutoTask 文件夹，识别所有待上传的批次
    
    AutoTask 目录结构:
      AutoTask/
        0212_大提琴/
          0212_113.mp4
          0212_114.mp4
        0213_竖琴/
          0213_27.mp4
    
    返回格式: 
    {
        "0212": {
            "大提琴": [视频列表],
        },
        "0213": {
            "竖琴": [视频列表],
        }
    }
    """
    video_folder = Path(config["video_folder"])
    tag_configs = config.get("tag_to_project", {})
    valid_tags = set(tag_configs.keys())
    
    if not video_folder.exists():
        log(f"视频文件夹不存在: {video_folder}", "ERR")
        return {}
    
    grouped = {}  # {date: {tag: [videos]}}
    
    # 子目录匹配模式: {MMDD}_{tag}
    dir_pattern = re.compile(r'^(\d{4})_(.+)$')
    
    for sub_dir in sorted(video_folder.iterdir()):
        if not sub_dir.is_dir():
            continue
        
        dir_match = dir_pattern.match(sub_dir.name)
        if not dir_match:
            continue
        
        date_str = dir_match.group(1)  # 如 "0212"
        tag = dir_match.group(2)       # 如 "大提琴"
        
        # 只处理配置中存在的 tag
        if tag not in valid_tags:
            continue
        
        # 扫描子目录中的视频文件
        videos = sorted([v for v in sub_dir.iterdir() if v.is_file() and v.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS])
        # 排除母带等非视频文件
        videos = [v for v in videos if not v.name.startswith("master_")]
        
        if not videos:
            continue
        
        if date_str not in grouped:
            grouped[date_str] = {}
        grouped[date_str][tag] = videos
    
    # 按日期排序
    return dict(sorted(grouped.items(), key=lambda x: x[0]))


def show_available_uploads(grouped: Dict[str, Dict[str, List[Path]]], config: Dict) -> List[tuple]:
    """
    展示所有可上传的批次，返回用户选择的 [(date, tag), ...]
    """
    if not grouped:
        print("\n❌ 成品文件夹中没有找到待上传的视频")
        return []
    
    print("\n" + "=" * 60)
    print("   📂 AutoTask 扫描结果")
    print("=" * 60 + "\n")
    
    # 收集所有选项
    options = []
    option_idx = 1
    
    for date, tags in grouped.items():
        print(f"📅 日期: {date}")
        print("-" * 40)
        
        for tag, videos in tags.items():
            tag_cfg = _get_tag_config(config, tag, {})
            project_name = tag_cfg.get("project_name", "未配置")
            
            # ========== 动态从 API 获取环境列表 ==========
            tag_containers = get_containers_by_tag(tag)
            ypp_serials = []
            non_ypp_serials = []
            for c in tag_containers:
                serial = c["serialNumber"]
                remark = c.get("remark") or ""
                if "YPP" in remark.upper():
                    ypp_serials.append(serial)
                else:
                    non_ypp_serials.append(serial)
            all_serials = sorted(ypp_serials + non_ypp_serials)
            
            ypp_count = len(ypp_serials)
            non_ypp_count = len(non_ypp_serials)
            total_channels = ypp_count + non_ypp_count
            
            # 检查视频数量是否足够
            video_count = len(videos)
            status = "✅" if video_count >= total_channels else f"⚠️ (需要 {total_channels} 个)"
            
            print(f"  [{option_idx}] 🏷️  {tag}")
            print(f"      项目: {project_name}")
            print(f"      视频: {video_count} 个 {status}")
            print(f"      频道: {total_channels} 个 (YPP: {ypp_count}, 普通: {non_ypp_count})")
            
            # ========== 库存状态展示 (新增) ==========
            if project_name and project_name != "未配置":
                projects_folder = Path(config.get("projects_folder", ""))
                project_path = projects_folder / project_name
                
                if project_path.exists():
                    inventory = get_inventory_status(project_path, all_serials)
                    
                    # 展示表格
                    print()
                    print(f"      📦 库存状态:")
                    print(f"      ┌──────────┬─────────┬─────────┬──────────┐")
                    print(f"      │ Container│ 封面剩余│ 标题剩余│  下一套  │")
                    print(f"      ├──────────┼─────────┼─────────┼──────────┤")
                    
                    has_warning = False
                    for container in all_serials:
                        thumb_info = inventory["thumbnails"].get(container, {"total": 0, "next": None})
                        title_info = inventory["titles"].get(container, {"total": 0})
                        
                        thumb_count = thumb_info["total"]
                        title_count = title_info["total"]
                        next_set = thumb_info.get("next")
                        
                        # 格式化显示
                        if thumb_count == 0:
                            thumb_str = " ❌ 0  "
                            has_warning = True
                        elif thumb_count <= 2:
                            thumb_str = f" ⚠️ {thumb_count}  "
                            has_warning = True
                        else:
                            thumb_str = f"  {thumb_count} 套 "
                        
                        if title_count == 0:
                            title_str = " ❌ 0  "
                        else:
                            title_str = f"  {title_count} 套 "
                        
                        if next_set:
                            next_str = f"  套 {next_set}  "
                        else:
                            next_str = " ❌ 缺货"
                        
                        ypp_mark = "★" if container in ypp_serials else " "
                        print(f"      │{ypp_mark}  {container:>4}   │{thumb_str}│{title_str}│{next_str}│")
                    
                    print(f"      └──────────┴─────────┴─────────┴──────────┘")
                    
                    # 缺货预警
                    if inventory["warnings"]:
                        print()
                        print(f"      ⚠️ 缺货预警:")
                        for warn in inventory["warnings"][:3]:  # 最多显示3条
                            print(f"         - {warn}")
            
            print()
            
            options.append((date, tag, video_count, total_channels))
            option_idx += 1
        
        print()
    
    # 添加"全部"选项
    print(f"  [A] 全部上传 (按顺序执行所有批次)")
    print(f"  [Q] 退出")
    print()
    
    # 用户选择
    while True:
        choice = input("请选择要上传的批次 (输入数字或 A/Q): ").strip().upper()
        
        if choice == 'Q':
            return []
        elif choice == 'A':
            return [(opt[0], opt[1]) for opt in options]
        else:
            try:
                idx = int(choice)
                if 1 <= idx <= len(options):
                    return [(options[idx-1][0], options[idx-1][1])]
                else:
                    print("❌ 无效选项，请重试")
            except ValueError:
                # 支持多选，如 "1,2,3" 或 "1 2 3"
                try:
                    indices = [int(x.strip()) for x in re.split(r'[,\s]+', choice)]
                    selected = []
                    for idx in indices:
                        if 1 <= idx <= len(options):
                            selected.append((options[idx-1][0], options[idx-1][1]))
                    if selected:
                        return selected
                except:
                    pass
                print("❌ 无效输入，请输入数字、A 或 Q")


# ============ 视频文件匹配 ============
def normalize_date_mmdd(date: str) -> str:
    """统一日期格式为 MMDD，兼容 0309 / 3.09 / 3.9。"""
    text = str(date).strip()
    match = re.match(r"^(\d{1,2})\.(\d{1,2})$", text)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        return f"{month:02d}{day:02d}"

    digits = re.sub(r"\D", "", text)
    if len(digits) == 3:
        return digits.zfill(4)
    if len(digits) == 4:
        return digits
    raise ValueError(f"无法识别日期格式: {date}")

def find_videos(video_folder: str, keyword: str, date: str, tag: str = None) -> List[Path]:
    """
    在 AutoTask 文件夹中查找匹配的视频
    
    AutoTask 结构: {video_folder}/{MMDD}_{tag}/{MMDD}_{容器号}.mp4
    
    参数:
        video_folder: AutoTask 根目录
        keyword: video_keyword (如 "古典小提琴")
        date: 日期 (支持 "0212" 或 "2.12" 格式)
        tag: 标签名 (如 "小提琴")，作为 keyword 找不到时的 fallback
    """
    folder = Path(video_folder)
    if not folder.exists():
        log(f"视频文件夹不存在: {folder}", "ERR")
        return []
    
    # 统一日期格式为 MMDD
    date_mmdd = normalize_date_mmdd(date)
    
    # 查找子目录: 优先用 keyword，找不到用 tag 名
    sub_dir = folder / f"{date_mmdd}_{keyword}"
    if not sub_dir.exists() and tag and tag != keyword:
        sub_dir_fallback = folder / f"{date_mmdd}_{tag}"
        if sub_dir_fallback.exists():
            log(f"目录 {date_mmdd}_{keyword} 不存在，使用标签名 {date_mmdd}_{tag}", "WARN")
            sub_dir = sub_dir_fallback
    
    if not sub_dir.exists():
        log(f"视频子目录不存在: {sub_dir}", "WARN")
        return []
    
    # 扫描视频文件
    videos = sorted([v for v in sub_dir.iterdir() if v.is_file() and v.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS])
    # 排除母带
    videos = [v for v in videos if not v.name.startswith("master_")]
    
    return videos

# ============ 封面图匹配 ============
def find_thumbnails(project_folder: str, count: int = 1, container: int = None, set_num: int = None) -> List[Path]:
    """
    从项目文件夹的 images 下获取封面图
    
    参数:
        project_folder: 项目文件夹路径
        count: 需要返回的图片数量
        container: Container ID (新格式)，如果提供则使用精确匹配
        set_num: 套数 (新格式)，如果提供则使用精确匹配
    
    新格式优先: 如果提供了 container，尝试匹配 {container}_{套数}.png
    旧格式兜底: 按顺序返回图片
    """
    project_path = Path(project_folder)
    
    # 新格式: 使用 container 和 set_num 精确匹配
    if container is not None:
        if set_num is not None:
            # 精确匹配特定套数
            thumb = get_thumbnail_by_container(project_path, container, set_num)
            if thumb:
                return [thumb]
        else:
            # 获取该 container 的下一个可用封面
            next_set = get_next_thumbnail_set(project_path, container)
            if next_set:
                thumb = get_thumbnail_by_container(project_path, container, next_set)
                if thumb:
                    return [thumb]
        
        # 新格式匹配失败，尝试旧格式
        log(f"新格式封面未找到 (container={container}, set={set_num})，尝试旧格式", "WARN")
    
    # 旧格式: 按顺序返回图片
    possible_paths = [
        project_path / "images" / "text",
        project_path / "images",
        project_path / "image" / "Text",
        project_path / "image",
    ]
    
    image_folder = None
    for p in possible_paths:
        if p.exists():
            image_folder = p
            break
    
    if not image_folder:
        log(f"封面图文件夹不存在: {project_folder}/images", "WARN")
        return []
    
    # 获取所有图片
    images = []
    for ext in ["*.jpg", "*.jpeg", "*.png", "*.webp"]:
        images.extend(image_folder.glob(ext))
    
    # 排除 used 目录中的
    images = [img for img in images if 'used' not in str(img)]
    
    # 按文件名排序
    images.sort(key=lambda x: x.name)
    
    return images[:count]


def find_thumbnail_for_channel(project_folder: str, channel_index: int, channel_meta: dict) -> List[Path]:
    """
    为特定频道查找封面图
    
    参数:
        project_folder: 项目路径
        channel_index: 频道在列表中的索引 (0-based)
        channel_meta: 从 parse_metadata 获取的频道元数据
    
    返回: 封面图路径列表
    """
    project_path = Path(project_folder)
    container_id = channel_meta.get("container_id")
    
    if container_id:
        # 新格式: 尝试按 container 匹配
        container = int(container_id)
        next_set = get_next_thumbnail_set(project_path, container)
        if next_set:
            thumb = get_thumbnail_by_container(project_path, container, next_set)
            if thumb:
                log(f"找到封面: {thumb.name} (Container {container} Set {next_set})", "OK")
                return [thumb]
    
    # 旧格式: 按顺序获取
    return find_thumbnails(project_folder, count=1)

# ============ 元数据读取 ============
def parse_metadata(project_folder: str) -> List[Dict]:
    """
    解析 metadata_channels.md 文件 (使用 utils.py 的新版本)
    返回每个频道的标题列表和简介
    """
    return utils_parse_metadata(Path(project_folder))

# ============ 文件选择（剪贴板方案）============
def select_file_with_clipboard(file_path: str):
    """使用剪贴板方案选择文件（支持中文路径，跨平台）"""
    log(f"选择文件: {Path(file_path).name}", "ACT")
    
    # 等待对话框打开
    time.sleep(2)
    
    if IS_WINDOWS:
        # Windows: 用 PowerShell 复制路径到剪贴板
        file_path_win = file_path.replace('/', '\\')
        subprocess.run(
            ['powershell', '-Command', f'Set-Clipboard -Value "{file_path_win}"'],
            check=True, timeout=5
        )
        # 用 pyautogui 或 PowerShell 发送键盘命令
        # 文件对话框中: 粘贴路径 + 回车
        subprocess.run(
            ['powershell', '-Command', '''
            Add-Type -AssemblyName System.Windows.Forms
            Start-Sleep -Milliseconds 500
            [System.Windows.Forms.SendKeys]::SendWait("^l")  # Ctrl+L 聚焦地址栏
            Start-Sleep -Milliseconds 300
            [System.Windows.Forms.SendKeys]::SendWait("^v")  # Ctrl+V 粘贴
            Start-Sleep -Milliseconds 500
            [System.Windows.Forms.SendKeys]::SendWait("{ENTER}")
            Start-Sleep -Milliseconds 800
            [System.Windows.Forms.SendKeys]::SendWait("{ENTER}")
            '''],
            check=True, timeout=20
        )
    else:
        # macOS: 用 osascript 复制路径到剪贴板
        copy_script = f'set the clipboard to "{file_path}"'
        subprocess.run(["osascript", "-e", copy_script], check=True, timeout=5)
        
        keyboard_script = '''
        tell application "System Events"
            keystroke "g" using {command down, shift down}
            delay 1.2
            keystroke "v" using {command down}
            delay 0.8
            keystroke return
            delay 1.0
            keystroke return
            delay 0.5
        end tell
        '''
        subprocess.run(["osascript", "-e", keyboard_script], check=True, timeout=20)
    
    log("文件选择完成", "OK")

async def ensure_upload_picker_open(page) -> bool:
    """确保已经进入 Upload videos 选择文件界面。"""
    try:
        if await page.locator("input[type='file']").count() > 0:
            return True
    except Exception:
        pass

    # 1) 若 Create 菜单已展开，先点 Upload videos
    try:
        clicked_upload = await page.evaluate(
            """
            () => {
              const items = Array.from(document.querySelectorAll("tp-yt-paper-item, ytcp-text-menu tp-yt-paper-item"));
              for (const el of items) {
                const t = ((el.innerText || el.textContent || '').trim()).toLowerCase();
                if (t.includes('upload videos') || t.includes('upload video') || t.includes('上傳影片') || t.includes('上传视频')) {
                  el.click();
                  return true;
                }
              }
              return false;
            }
            """
        )
        if clicked_upload:
            await asyncio.sleep(2)
            if await page.locator("input[type='file']").count() > 0:
                return True
    except Exception:
        pass

    # 2) 再尝试点击顶部上传入口
    for sel in ["#upload-icon", "#create-icon", "ytcp-button.ytcpAppHeaderCreateIcon"]:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click(force=True)
                await asyncio.sleep(2)

                try:
                    clicked_upload = await page.evaluate(
                        """
                        () => {
                          const items = Array.from(document.querySelectorAll("tp-yt-paper-item, ytcp-text-menu tp-yt-paper-item"));
                          for (const el of items) {
                            const t = ((el.innerText || el.textContent || '').trim()).toLowerCase();
                            if (t.includes('upload videos') || t.includes('upload video') || t.includes('上傳影片') || t.includes('上传视频')) {
                              el.click();
                              return true;
                            }
                          }
                          return false;
                        }
                        """
                    )
                    if clicked_upload:
                        await asyncio.sleep(2)
                except Exception:
                    pass

                if await page.locator("input[type='file']").count() > 0:
                    return True
        except Exception:
            continue

    # 3) 最后走直达上传页兜底
    return await open_direct_upload_page(page)


async def select_file_with_playwright(page, file_path: str) -> bool:
    """CDP 失败后的回退：用 Playwright 直接设置文件/触发 filechooser。"""
    target = str(Path(file_path))
    await ensure_upload_picker_open(page)

    # 1) 直接找 input[type=file]（包括子 frame）
    selectors = [
        "input[type='file']",
        "ytcp-uploads-file-picker input[type='file']",
        "ytcp-video-upload-progress input[type='file']",
    ]
    frames = page.frames if page.frames else [page.main_frame]
    for frame in frames:
        for selector in selectors:
            try:
                locator = frame.locator(selector)
                count = await locator.count()
            except Exception:
                continue
            if count <= 0:
                continue
            for idx in range(count):
                try:
                    await locator.nth(idx).set_input_files(target)
                    log(f"Playwright 回退成功: {selector}[{idx}] frame={frame.url}", "OK")
                    return True
                except Exception:
                    continue

    # 2) 深度 shadow-root 查找 input[type=file]（主 frame）
    try:
        handle = await page.evaluate_handle(
            """
            () => {
              const queue = [document];
              const visited = new Set();
              while (queue.length) {
                const root = queue.shift();
                if (!root || visited.has(root)) continue;
                visited.add(root);

                if (root.querySelector) {
                  const input = root.querySelector("input[type='file']");
                  if (input) return input;
                }

                if (root.querySelectorAll) {
                  for (const el of root.querySelectorAll("*")) {
                    if (el && el.shadowRoot) queue.push(el.shadowRoot);
                  }
                }
              }
              return null;
            }
            """
        )
        element = handle.as_element() if handle else None
        if element:
            await element.set_input_files(target)
            log("Playwright 深层 DOM 回退成功", "OK")
            try:
                await handle.dispose()
            except Exception:
                pass
            return True
        try:
            await handle.dispose()
        except Exception:
            pass
    except Exception:
        pass

    # 3) filechooser 回退（点击 Select files 按钮）
    chooser_buttons = [
        "#select-files-button",
        "ytcp-button#select-files-button",
        "ytcp-uploads-file-picker #select-files-button",
        "ytcp-uploads-file-picker ytcp-button",
        "tp-yt-paper-button#select-files-button",
    ]
    for selector in chooser_buttons:
        btn = page.locator(selector).first
        try:
            if await btn.count() <= 0 or not await btn.is_visible():
                continue
        except Exception:
            continue

        try:
            async with page.expect_file_chooser(timeout=6000) as fc_info:
                await btn.click(force=True)
            chooser = await fc_info.value
            await chooser.set_files(target)
            log(f"Playwright filechooser 回退成功: {selector}", "OK")
            return True
        except Exception:
            continue

    return False


async def select_file_with_cdp(page, file_path: str) -> bool:
    """使用 CDP DOM.setFileInputFiles 选择文件（后台运行，无需前台窗口）
    
    相比 AppleScript 剪贴板方案的优势：
    - 完全后台运行，不需要浏览器窗口在前台
    - 不依赖 AppleScript / osascript
    - 不受焦点问题影响
    - 跨平台（Windows/macOS 通用）
    
    注意：不需要先点击 Select files 按钮，直接找 file input 设置文件
    """
    log(f"CDP 选择文件: {Path(file_path).name}", "ACT")

    if IS_MAC and any(ord(ch) > 127 for ch in str(file_path)):
        log("macOS 检测到非 ASCII 路径，切换 Playwright 文件注入", "INFO")
        return await select_file_with_playwright(page, file_path)
    
    try:
        await ensure_upload_picker_open(page)
        input_ready = False
        for _ in range(20):
            try:
                if await page.locator("input[type='file']").count() > 0:
                    input_ready = True
                    break
            except Exception:
                pass
            await asyncio.sleep(0.5)
        if not input_ready:
            log("CDP: 上传页 file input 仍未出现，继续尝试深度查询", "WARN")

        cdp = await page.context.new_cdp_session(page)
        
        # 获取 DOM 根节点
        doc = await cdp.send('DOM.getDocument', {'depth': -1, 'pierce': True})
        root_node_id = doc['root']['nodeId']
        
        # 查找 <input type="file"> 元素
        result = await cdp.send('DOM.querySelector', {
            'nodeId': root_node_id,
            'selector': 'input[type="file"]'
        })
        
        node_id = result.get('nodeId', 0)
        if node_id == 0:
            try:
                search = await cdp.send('DOM.performSearch', {'query': 'input[type="file"]'})
                result_count = int(search.get('resultCount', 0) or 0)
                if result_count > 0:
                    matches = await cdp.send(
                        'DOM.getSearchResults',
                        {
                            'searchId': search['searchId'],
                            'fromIndex': 0,
                            'toIndex': min(result_count, 1),
                        },
                    )
                    node_ids = matches.get('nodeIds', []) or []
                    if node_ids:
                        node_id = int(node_ids[0])
                try:
                    await cdp.send('DOM.discardSearchResults', {'searchId': search['searchId']})
                except Exception:
                    pass
            except Exception:
                pass
        if node_id == 0:
            log("CDP: 未找到 file input 元素，切换 Playwright 回退", "WARN")
            return await select_file_with_playwright(page, file_path)
        
        # 设置文件路径（浏览器自己从本地磁盘读取，不经过网络传输）
        await cdp.send('DOM.setFileInputFiles', {
            'nodeId': node_id,
            'files': [str(file_path)]
        })
        
        # 触发 change 事件确保 YouTube 组件响应
        await page.evaluate('''() => {
            const input = document.querySelector("input[type='file']");
            if (input) {
                input.dispatchEvent(new Event("change", { bubbles: true }));
                input.dispatchEvent(new Event("input", { bubbles: true }));
            }
        }''')
        
        log("CDP 文件设置成功", "OK")
        return True
        
    except Exception as e:
        log(f"CDP 文件选择失败: {e}，切换 Playwright 回退", "WARN")
        return await select_file_with_playwright(page, file_path)


async def detect_upload_file_read_error(page) -> Optional[str]:
    try:
        body_text = await page.locator("body").inner_text(timeout=3000)
    except Exception:
        return None

    lowered = body_text.lower()
    markers = [
        "文件不可读",
        "找不到或无法读取你的文件",
        "file is unreadable",
        "can't read your file",
        "cannot read your file",
        "unable to read your file",
    ]
    for marker in markers:
        if marker in lowered:
            return marker
    return None

# ============ 上传状态管理 ============
def get_upload_status(tag: str, date: str, serials: List[int]) -> Dict[int, Dict]:
    """
    读取上传记录，返回每个频道的上传状态
    
    返回: {
        26: {"status": "success", "time": "01:08:37", "title": "xxx..."},
        33: {"status": "failed", "error": "连接错误"},
        39: {"status": "pending"}
    }
    """
    date_key = normalize_date_mmdd(date)
    date_dir = UPLOAD_RECORDS_DIR / date_key / tag
    
    status_map = {}
    for serial in serials:
        record_file = date_dir / f"channel_{serial}.json"
        
        if record_file.exists():
            try:
                with open(record_file, "r", encoding="utf-8") as f:
                    record = json.load(f)
                
                upload_time = record.get("upload_time", "")
                if upload_time:
                    try:
                        dt = datetime.fromisoformat(upload_time)
                        time_str = dt.strftime("%H:%M")
                    except:
                        time_str = "未知"
                else:
                    time_str = "未知"
                
                if record.get("success", False):
                    status_map[serial] = {
                        "status": "success",
                        "time": time_str,
                        "title": record.get("metadata", {}).get("title", "")[:40] + "..."
                    }
                else:
                    status_map[serial] = {
                        "status": "failed",
                        "time": time_str,
                        "error": "上传失败"
                    }
            except Exception as e:
                status_map[serial] = {"status": "error", "error": str(e)}
        else:
            status_map[serial] = {"status": "pending"}
    
    return status_map


def interactive_upload_confirm(tag: str, date: str, all_serials: List[int], ypp_serials: List[int], auto_confirm: bool = False) -> List[int]:
    """
    交互式确认要上传的频道
    
    显示当前状态，让用户确认并选择需要重新上传的
    返回: 最终需要上传的序号列表
    """
    status_map = get_upload_status(tag, date, all_serials)
    
    success_count = sum(1 for s in status_map.values() if s["status"] == "success")
    failed_count = sum(1 for s in status_map.values() if s["status"] == "failed")
    pending_count = sum(1 for s in status_map.values() if s["status"] == "pending")
    
    print(f"\n📋 上传状态检查 [{date} {tag}]")
    print("-" * 50)
    
    for serial in all_serials:
        status = status_map.get(serial, {"status": "pending"})
        is_ypp = "🌟" if serial in ypp_serials else "  "
        
        if status["status"] == "success":
            print(f"  {is_ypp} 序号 {serial}: ✅ 已成功 ({status['time']})")
        elif status["status"] == "failed":
            print(f"  {is_ypp} 序号 {serial}: ❌ 失败 ({status.get('error', '未知')})")
        else:
            print(f"  {is_ypp} 序号 {serial}: ⏳ 待上传")
    
    print("-" * 50)
    print(f"统计: ✅ 成功 {success_count} | ❌ 失败 {failed_count} | ⏳ 待上传 {pending_count}")
    print()
    
    # 默认上传列表：失败的 + 待上传的
    default_upload = [s for s in all_serials if status_map.get(s, {}).get("status") != "success"]
    
    if auto_confirm:
        print("🤖 [自动模式] 跳过强制确认，默认重传失败和待上传的频道")
        return default_upload
    
    if success_count > 0 and (failed_count > 0 or pending_count > 0):
        print(f"📌 默认将跳过 {success_count} 个已成功的，上传 {len(default_upload)} 个")
        print()
        
        # 询问是否有需要强制重传的
        force_input = input("❓ 有需要强制重新上传的吗？输入序号 (如: 26,33) 或直接回车跳过: ").strip()
        
        if force_input:
            try:
                force_serials = [int(x.strip()) for x in force_input.replace("，", ",").split(",") if x.strip().isdigit()]
                # 添加到上传列表（去重）
                for s in force_serials:
                    if s in all_serials and s not in default_upload:
                        default_upload.append(s)
                        print(f"  ➕ 序号 {s} 已加入重新上传列表")
            except:
                pass
        
        # 按原顺序排序
        default_upload = [s for s in all_serials if s in default_upload]
        
    elif success_count == len(all_serials):
        print("🎉 所有频道都已成功上传！")
        print()
        force_input = input("❓ 是否要强制重新上传？输入序号 (如: 26,33) 或直接回车跳过: ").strip()
        
        if force_input:
            if force_input.upper() == "ALL":
                default_upload = all_serials.copy()
                print("  ➕ 全部重新上传")
            else:
                try:
                    force_serials = [int(x.strip()) for x in force_input.replace("，", ",").split(",") if x.strip().isdigit()]
                    default_upload = [s for s in force_serials if s in all_serials]
                except:
                    pass
    
    print()
    if default_upload:
        print(f"✅ 本次将上传 {len(default_upload)} 个频道: {default_upload}")
    else:
        print("ℹ️ 没有需要上传的频道")
    
    return default_upload


# ============ 上传记录 ============
def save_upload_record(
    tag: str,
    date: str,
    serial: int,
    channel_name: str,
    video_path: Path,
    thumbnails: List[Path],
    title: str,
    description: str,
    is_ypp: bool,
    ab_test_titles: List[str] = None,
    success: bool = True
):
    """
    保存上传记录，方便后续对比分析
    
    记录包含:
    - 上传时间
    - 频道序号和名称
    - 视频文件路径
    - 封面图路径
    - 使用的标题和简介
    - A/B 测试标题（如果有）
    """
    try:
        date_key = normalize_date_mmdd(date)
        # 确保目录存在
        UPLOAD_RECORDS_DIR.mkdir(parents=True, exist_ok=True)
        
        # 按日期创建子目录
        date_dir = UPLOAD_RECORDS_DIR / date_key
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # 按标签创建子目录
        tag_dir = date_dir / tag
        tag_dir.mkdir(parents=True, exist_ok=True)
        
        # 记录文件
        record_file = tag_dir / f"channel_{serial}.json"
        
        record = {
            "upload_time": datetime.now().isoformat(),
            "tag": tag,
            "date": date_key,
            "serial": serial,
            "channel_name": channel_name,
            "is_ypp": is_ypp,
            "success": success,
            "video": {
                "path": str(video_path),
                "filename": video_path.name
            },
            "thumbnails": [
                {"path": str(t), "filename": t.name} for t in thumbnails
            ],
            "metadata": {
                "title": title,
                "description": description[:500] + "..." if len(description) > 500 else description,
                "description_full_length": len(description)
            }
        }
        
        if ab_test_titles:
            record["ab_test"] = {
                "enabled": True,
                "titles": ab_test_titles
            }
        
        with open(record_file, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        
        log(f"📝 上传记录已保存: {record_file.relative_to(UPLOAD_RECORDS_DIR)}", "OK")
        
        # 同时更新汇总文件
        summary_file = date_dir / "upload_summary.json"
        if summary_file.exists():
            with open(summary_file, "r", encoding="utf-8") as f:
                summary = json.load(f)
        else:
            summary = {
                "date": date_key,
                "created_at": datetime.now().isoformat(),
                "uploads": []
            }
        
        # 添加/更新记录
        existing = next((u for u in summary["uploads"] if u["serial"] == serial and u["tag"] == tag), None)
        upload_entry = {
            "tag": tag,
            "serial": serial,
            "channel_name": channel_name,
            "title": title[:80] + "..." if len(title) > 80 else title,
            "video": video_path.name,
            "thumbnails": [t.name for t in thumbnails],
            "is_ypp": is_ypp,
            "success": success,
            "upload_time": datetime.now().strftime("%H:%M:%S")
        }
        
        if existing:
            summary["uploads"].remove(existing)
        summary["uploads"].append(upload_entry)
        summary["updated_at"] = datetime.now().isoformat()
        
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
            
    except Exception as e:
        log(f"保存上传记录失败: {e}", "WARN")

# ============ Video Elements: 页面状态清理 ============

async def ensure_ve_page_clean(page) -> bool:
    """
    确保 Video Elements 页面是干净的（无弹窗残留）
    在每个 VE 操作（商品/片尾/卡片）之前调用
    
    清理内容:
    1. 关闭商品对话框 (ytshopping-product-tagging-dialog)
    2. 关闭卡片编辑器 (ytve-info-cards-editor) 
    3. 关闭片尾画面编辑器 (ytve-endscreen-template-picker)
    4. 清除所有 backdrop
    5. 验证 VE 页面按钮可见
    """
    try:
        cleaned = await page.evaluate(r"""
            () => {
                const actions = [];
                
                // 1. 关闭商品对话框
                const productDialog = document.querySelector('ytshopping-product-tagging-dialog');
                if (productDialog && productDialog.offsetWidth > 0) {
                    // 找关闭按钮
                    const closeBtn = productDialog.querySelector('#close-button');
                    if (closeBtn) { closeBtn.click(); actions.push('closed_product_dialog'); }
                    else {
                        // 强制隐藏
                        const pd = productDialog.querySelector('tp-yt-paper-dialog');
                        if (pd && pd.close) { pd.close(); actions.push('force_closed_product'); }
                        productDialog.style.display = 'none';
                        actions.push('hidden_product_dialog');
                    }
                }
                
                // 2. 检查卡片编辑器 - 如果有未保存的,舍弃
                const discardBtn = document.querySelector('#discard-button');
                if (discardBtn && discardBtn.offsetWidth > 0) {
                    // 检查是否在卡片或片尾编辑器中
                    const editor = document.querySelector('ytve-info-cards-editor, ytve-endscreen-editor');
                    if (editor && editor.offsetWidth > 0) {
                        discardBtn.click();
                        actions.push('discarded_editor');
                    }
                }
                
                // 3. 清除所有 backdrop
                const backdrops = document.querySelectorAll('tp-yt-iron-overlay-backdrop');
                for (const b of backdrops) {
                    if (b.style.display !== 'none' && (b.offsetWidth > 0 || b.style.zIndex)) {
                        b.style.display = 'none';
                        actions.push('hidden_backdrop');
                    }
                }
                
                // 4. 处理可能的确认弹窗 ("舍弃更改?")
                const confirmDialogs = document.querySelectorAll('ytcp-confirmation-dialog');
                for (const cd of confirmDialogs) {
                    if (cd.offsetWidth === 0) continue;
                    const btns = cd.querySelectorAll('ytcp-button');
                    for (const btn of btns) {
                        const text = (btn.innerText || '').trim().toLowerCase();
                        if (text.includes('discard') || text.includes('舍弃') || text.includes('放弃')) {
                            btn.click();
                            actions.push('confirmed_discard');
                            break;
                        }
                    }
                }
                
                // 5. 验证 VE 按钮可见
                const productsBtn = document.querySelector('#products-button');
                const cardsBtn = document.querySelector('#cards-button');
                const endscreenBtn = document.querySelector('#endscreens-button');
                
                return {
                    actions,
                    ve_visible: {
                        products: productsBtn ? productsBtn.offsetWidth > 0 : false,
                        cards: cardsBtn ? cardsBtn.offsetWidth > 0 : false,
                        endscreen: endscreenBtn ? endscreenBtn.offsetWidth > 0 : false
                    }
                };
            }
        """)
        
        if cleaned.get('actions'):
            log(f"VE 页面清理: {cleaned['actions']}", "ACT")
            await asyncio.sleep(2)  # 等待关闭动画
            
            # 如果舍弃了编辑器, 可能还有确认弹窗
            if 'discarded_editor' in cleaned['actions']:
                await asyncio.sleep(1)
                await page.evaluate(r"""
                    () => {
                        const dialogs = document.querySelectorAll('ytcp-confirmation-dialog, tp-yt-paper-dialog');
                        for (const d of dialogs) {
                            if (d.offsetWidth === 0) continue;
                            const btns = d.querySelectorAll('ytcp-button');
                            for (const btn of btns) {
                                const text = (btn.innerText || '').trim().toLowerCase();
                                if (text.includes('discard') || text.includes('舍弃')) {
                                    btn.click(); return;
                                }
                            }
                        }
                    }
                """)
                await asyncio.sleep(1)
        
        ve = cleaned.get('ve_visible', {})
        if not any(ve.values()):
            log("VE 页面按钮均不可见, 尝试导航到 VE 步骤...", "WARN")
            await page.evaluate("document.querySelector('#step-badge-2')?.click()")
            await asyncio.sleep(2)
        
        return True
        
    except Exception as e:
        log(f"VE 页面清理异常: {e}", "WARN")
        return False


# ============ Video Elements: 播放列表卡片 ============

async def add_playlist_card(page, playlist_name: str) -> bool:
    """
    在 Video Elements 页面添加播放列表卡片
    验证通过: 2026-02-13
    
    流程: 点击 #cards-button → 选择「播放列表」 → 选择播放列表 → 保存
    """
    if not playlist_name:
        log("未指定播放列表, 跳过卡片", "WARN")
        return False
    
    log(f"添加播放列表卡片: '{playlist_name}'...")
    
    try:
        # Step 1: 点击 Cards 按钮
        cards_btn = page.locator("#cards-button").first
        if await cards_btn.count() == 0:
            log("未找到 #cards-button", "WARN")
            return False
        
        await cards_btn.click()
        await asyncio.sleep(3)
        
        # Step 2: 点击「播放列表」的 + 按钮
        # 先调试: 打印所有可见的卡片选项
        all_labels = await page.evaluate(r"""
            () => {
                const result = [];
                // 方式1: 用 class 查找
                const containers = document.querySelectorAll('.info-card-type-option-container');
                for (const c of containers) {
                    const label = c.querySelector('.info-card-type-option-label');
                    if (label) result.push({method: 'class', text: (label.innerText || '').trim()});
                }
                // 方式2: 用通用遍历 (备选)
                if (result.length === 0) {
                    const allVisible = document.querySelectorAll('ytcp-text, span, div, p');
                    for (const el of allVisible) {
                        if (el.offsetWidth === 0 || el.children.length > 2) continue;
                        const t = (el.innerText || '').trim();
                        if (t.length > 2 && t.length < 30) {
                            // 在卡片编辑器区域内
                            const parent = el.closest('ytve-info-cards-editor, [role="dialog"]');
                            if (parent) result.push({method: 'scan', text: t});
                        }
                    }
                }
                return result;
            }
        """)
        log(f"卡片编辑器可见选项: {all_labels}", "DEBUG")
        
        clicked = await page.evaluate(r"""
            () => {
                const containers = document.querySelectorAll('.info-card-type-option-container');
                for (const c of containers) {
                    const label = c.querySelector('.info-card-type-option-label');
                    if (label) {
                        const text = (label.innerText || '').trim().toLowerCase();
                        if (text === '播放列表' || text === 'playlist' || text === 'oynatma listesi' ||
                            text.includes('playlist') || text.includes('播放') || text.includes('oynatma')) {
                            const addIcon = c.querySelector('ytcp-icon-button, iron-icon');
                            if (addIcon) { addIcon.click(); return 'icon'; }
                            c.click();
                            return 'container';
                        }
                    }
                }
                // 备选: 如果没有 class 匹配, 尝试在卡片编辑器中找 playlist 相关按钮
                const editor = document.querySelector('ytve-info-cards-editor');
                if (editor) {
                    const btns = editor.querySelectorAll('ytcp-button, button, [role="button"]');
                    for (const btn of btns) {
                        if (btn.offsetWidth === 0) continue;
                        const t = (btn.innerText || '').trim().toLowerCase();
                        if (t.includes('playlist') || t.includes('播放') || t.includes('oynatma')) {
                            btn.click();
                            return 'editor_btn:' + t;
                        }
                    }
                }
                return 'not_found';
            }
        """)
        
        log(f"播放列表点击结果: {clicked}", "DEBUG")
        
        if clicked == 'not_found':
            log("未找到播放列表选项", "WARN")
            # 关闭卡片编辑器
            await page.evaluate("document.querySelector('#discard-button')?.click()")
            await asyncio.sleep(1)
            return False
        
        await asyncio.sleep(3)
        
        # Step 3: 方案A - 用播放列表名字直接点击 (已验证成功)
        log(f"选择播放列表: '{playlist_name}'...")
        try:
            picked = await page.evaluate(
                r"""
                (playlistName) => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || '').trim();
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement) || !visible(el)) return false;
                        try { el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' }); } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };
                    const dialogs = Array.from(
                        document.querySelectorAll('ytve-modal-host, tp-yt-paper-dialog, ytcp-dialog, [role="dialog"]')
                    ).filter(visible);
                    const roots = dialogs.length ? dialogs : [document];
                    const clickableSelector = [
                        'tp-yt-paper-item',
                        '[role="option"]',
                        '[role="button"]',
                        'button',
                        'ytcp-menu-service-item-renderer',
                        'ytve-playlist-picker-item',
                    ].join(', ');
                    for (const root of roots) {
                        const nodes = Array.from(root.querySelectorAll(clickableSelector));
                        for (const node of nodes) {
                            if (!visible(node)) continue;
                            const text = textOf(node);
                            if (!text || !text.includes(playlistName)) continue;
                            if (
                                node.matches('input, textarea, [contenteditable="true"]') ||
                                node.querySelector?.('#textbox, input, textarea, [contenteditable="true"]')
                            ) {
                                continue;
                            }
                            if (clickEl(node)) return { clicked: true, method: 'dialog_click', text };
                        }
                    }
                    return { clicked: false, method: 'not_found' };
                }
                """,
                playlist_name,
            )
            if not picked.get("clicked"):
                pl_element = page.locator(
                    "tp-yt-paper-item, [role='option'], [role='button'], button, ytcp-menu-service-item-renderer"
                ).filter(has_text=playlist_name).first
                if await pl_element.count() > 0:
                    await pl_element.click(force=True)
                    picked = {"clicked": True, "method": "locator_filter"}
            if picked.get("clicked"):
                log(f"已点击播放列表 '{playlist_name}' ({picked.get('method')})", "OK")
            else:
                log(f"未找到包含 '{playlist_name}' 的播放列表选项", "WARN")
                await page.evaluate("document.querySelector('#discard-button')?.click()")
                await asyncio.sleep(1)
                return False
        except Exception as e:
            log(f"点击播放列表失败: {e}", "WARN")
            await page.evaluate("document.querySelector('#discard-button')?.click()")
            await asyncio.sleep(1)
            return False
        
        await asyncio.sleep(3)
        
        # Step 3.5: 确认创建卡片 (从 test_add_card_playlist.py Step 6)
        # 选择播放列表后, 可能出现中间对话框需要点击 "创建卡片" 确认
        log("确认创建卡片...")
        create_card = await page.evaluate(r"""
            () => {
                const dialogs = document.querySelectorAll('tp-yt-paper-dialog, ytcp-dialog');
                for (const d of dialogs) {
                    if (d.offsetWidth === 0) continue;
                    const text = (d.innerText || '');
                    // 跳过上传主对话框和商品对话框
                    if (text.includes('详细信息') && text.includes('创收') && text.includes('视频元素')) continue;
                    if (text.includes('添加商品链接') || text.includes('搜索商品')) continue;
                    
                    const btns = d.querySelectorAll('ytcp-button');
                    for (const btn of btns) {
                        if (btn.offsetWidth === 0) continue;
                        const t = (btn.innerText || '').trim();
                        if ((t === '创建卡片' || t === 'Create card' || t === 'Create') 
                            && btn.getAttribute('aria-disabled') !== 'true') {
                            btn.click();
                            return {clicked: true, text: t};
                        }
                    }
                }
                return {clicked: false};
            }
        """)
        
        if create_card.get("clicked"):
            log(f"已点击 '{create_card.get('text')}'", "OK")
            await asyncio.sleep(2)
        else:
            log("未找到创建卡片按钮 (可能已自动创建)", "WARN")
        
        # Step 4: 保存卡片 (遍历找可见的 Save/保存 按钮, 避免 #save-button ID 冲突)
        log("保存卡片...")
        card_saved = await page.evaluate(r"""
            () => {
                // 优先: ytve-modal-host 内精确定位
                const hostBtn = document.querySelector('ytve-modal-host #save-button');
                if (hostBtn && hostBtn.offsetWidth > 0 && hostBtn.getAttribute('aria-disabled') !== 'true') {
                    hostBtn.click();
                    return {clicked: true, method: 'host_id'};
                }
                // 备选: 遍历所有 ytcp-button 找可见的 Save/保存
                const allBtns = document.querySelectorAll('ytcp-button');
                for (const btn of allBtns) {
                    if (btn.offsetWidth === 0) continue;
                    const text = (btn.innerText || '').trim();
                    if ((text === 'Save' || text === '保存') && btn.getAttribute('aria-disabled') !== 'true') {
                        (btn.querySelector('button') || btn).click();
                        return {clicked: true, method: 'text', text};
                    }
                }
                return {clicked: false};
            }
        """)
        
        if card_saved.get("clicked"):
            await asyncio.sleep(3)
            log("播放列表卡片已保存 ✅", "OK")
            return True
        
        # 保存不可用, 舍弃
        await page.evaluate("document.querySelector('#discard-button')?.click()")
        await asyncio.sleep(1)
        log("卡片保存失败, 已舍弃", "WARN")
        return False
        
    except Exception as e:
        log(f"添加播放列表卡片失败: {e}", "WARN")
        # 尝试关闭任何打开的编辑器
        try:
            await page.evaluate("document.querySelector('#discard-button')?.click()")
            await asyncio.sleep(1)
        except:
            pass
        return False


# ============ Video Elements: 商品链接 ============
# 基于 test_add_product_v1_fix.py (2026-02-13 14:58) 验证通过的逻辑
# 关键修复: JS evaluate + 坐标点击, 精确 aria-label, 时间格式 '5:00:00',
#          keyboard.type(delay=50), 继续/完成用 querySelector('button')

async def add_product_links(page) -> bool:
    """
    在 Video Elements 页面添加商品链接 (已保存的商品)
    流程: 点击 #products-button → 已保存Tab → 添加商品 → 继续 → 添加时间戳 → 设时间 → 完成
    """
    log("添加商品链接...")
    
    try:
        # Step 1: 确保商品对话框打开
        has_dialog = await page.evaluate(
            "document.querySelector('ytshopping-product-tagging-dialog') && document.querySelector('ytshopping-product-tagging-dialog').offsetWidth > 0"
        )
        if not has_dialog:
            products_btn = page.locator("#products-button").first
            if await products_btn.count() == 0:
                log("未找到 #products-button, 可能非 YPP", "WARN")
                return False
            await products_btn.click()
            await asyncio.sleep(3)

        # Step 2: 检查是否已在时间戳页面
        on_ts_page = await page.evaluate(r"""
            () => {
                const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                if (!dialog) return false;
                for (const inp of dialog.querySelectorAll('input')) {
                    if (inp.offsetWidth === 0) continue;
                    const label = inp.getAttribute('aria-label') || '';
                    if (label.includes('小时') || label.includes('分钟')) return true;
                }
                return false;
            }
        """)

        if not on_ts_page:
            # ---- 在商品选择页面 ----
            # Step 2a: 点击 "已保存的商品" Tab (JS, dialog 内部查找)
            log("点击 '已保存的商品' Tab...")
            await page.evaluate(r"""
                () => {
                    const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                    if (!dialog) return;
                    const candidates = dialog.querySelectorAll('ytcp-chip, tp-yt-paper-tab, [role="tab"]');
                    for (const c of candidates) {
                        if (c.offsetWidth === 0) continue;
                        const t = (c.innerText || '').toLowerCase();
                        if (t.includes('saved') || t.includes('已保存')) {
                            c.click();
                            c.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                    }
                }
            """)
            await asyncio.sleep(5)  # 等待商品列表加载

            # Step 2b: 找 "添加链接" 按钮 (精确 aria-label + 坐标点击)
            add_btns = await page.evaluate(r"""
                () => {
                    const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                    if (!dialog) return [];
                    const btns = [];
                    const allBtns = dialog.querySelectorAll('ytcp-icon-button, ytcp-button, button, div[role="button"]');
                    for (const btn of allBtns) {
                        if (btn.offsetWidth === 0) continue;
                        const label = btn.getAttribute('aria-label') || '';
                        if (label.includes('添加链接') || label.includes('Add link') || label === 'Tag' || label.includes('Tag') || label === 'Add' || label === '添加') {
                            const rect = btn.getBoundingClientRect();
                            btns.push({
                                label,
                                x: Math.round(rect.x + rect.width/2),
                                y: Math.round(rect.y + rect.height/2)
                            });
                        }
                    }
                    return btns;
                }
            """)

            if len(add_btns) > 0:
                log(f"找到 {len(add_btns)} 个可添加商品, 添加前2个...")
                for i, btn in enumerate(add_btns[:2]):
                    await page.mouse.click(btn["x"], btn["y"])
                    await asyncio.sleep(2)
                    log(f"  商品{i+1} 已添加", "OK")
            else:
                log("未找到可添加的商品 (可能已添加或列表为空)", "WARN")

            # Step 2c: 点击 "继续" (dialog 内, 检查 disabled, querySelector('button'))
            log("点击继续...")
            cont = await page.evaluate(r"""
                () => {
                    const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                    if (!dialog) return {error: 'no_dialog'};
                    const btns = dialog.querySelectorAll('ytcp-button');
                    for (const btn of btns) {
                        if (btn.offsetWidth === 0) continue;
                        const text = (btn.innerText || '').trim();
                        if (text === '继续' || text === 'Continue') {
                            if (btn.getAttribute('aria-disabled') !== 'true') {
                                (btn.querySelector('button') || btn).click();
                                return {clicked: true, text};
                            }
                            return {clicked: false, disabled: true};
                        }
                    }
                    return {error: 'no_continue_btn'};
                }
            """)

            if not cont.get("clicked"):
                log(f"继续按钮问题: {cont}", "WARN")

            await asyncio.sleep(4)

        # Step 3: 时间戳页面 - 探测按钮和输入框 (JS evaluate + 坐标)
        log("探测时间戳页面...")
        ts_probe = await page.evaluate(r"""
            () => {
                const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                if (!dialog) return {tsBtns: [], timeInput: null};

                const result = {tsBtns: [], timeInput: null};

                // 找 "添加时间戳" 按钮
                const allBtns = dialog.querySelectorAll('ytcp-icon-button');
                for (const btn of allBtns) {
                    if (btn.offsetWidth === 0) continue;
                    const label = btn.getAttribute('aria-label') || '';
                    if (label.includes('时间戳') || label.includes('timestamp')) {
                        const rect = btn.getBoundingClientRect();
                        result.tsBtns.push({
                            label,
                            x: Math.round(rect.x + rect.width/2),
                            y: Math.round(rect.y + rect.height/2)
                        });
                    }
                }

                // 找时间输入框
                for (const inp of dialog.querySelectorAll('input')) {
                    if (inp.offsetWidth === 0) continue;
                    const label = inp.getAttribute('aria-label') || '';
                    if (label.includes('小时') || label.includes('分钟') ||
                        label.includes('秒') || label.includes('帧')) {
                        const rect = inp.getBoundingClientRect();
                        result.timeInput = {
                            label,
                            x: Math.round(rect.x + rect.width/2),
                            y: Math.round(rect.y + rect.height/2)
                        };
                        break;
                    }
                }

                return result;
            }
        """)

        ts_btns = ts_probe.get("tsBtns", [])
        time_input_info = ts_probe.get("timeInput")

        if len(ts_btns) == 0:
            log("未找到添加时间戳按钮, 尝试直接完成", "WARN")
        else:
            log(f"找到 {len(ts_btns)} 个添加时间戳按钮")

            # Step 4: 逐个设置时间 + 添加到时间轴
            time_values = ["5:00:00", "10:00:00"]

            for idx, btn in enumerate(ts_btns[:2]):
                time_val = time_values[idx] if idx < len(time_values) else time_values[-1]
                log(f"设置第{idx+1}个商品时间: {time_val}")

                # 4a. 设置时间 (坐标点击 + Control+a + keyboard.type)
                if time_input_info:
                    await page.mouse.click(time_input_info["x"], time_input_info["y"])
                    await asyncio.sleep(0.3)
                    await page.keyboard.press("Control+a")
                    await asyncio.sleep(0.2)
                    await page.keyboard.type(time_val, delay=50)
                    await asyncio.sleep(0.3)
                    await page.keyboard.press("Enter")
                    await asyncio.sleep(1)

                # 4b. 点击 "添加时间戳" 按钮 (坐标点击)
                await page.mouse.click(btn["x"], btn["y"])
                await asyncio.sleep(2)
                log(f"  商品{idx+1} 已添加到时间轴 @ {time_val}", "OK")

        # Step 5: 点击 "完成" (dialog 内, querySelector('button') || btn)
        log("点击完成...")
        done = await page.evaluate(r"""
            () => {
                const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                if (!dialog) return {error: 'no_dialog'};

                // 优先用 ID
                const doneById = dialog.querySelector('#timestamp-editor-done-button');
                if (doneById && doneById.offsetWidth > 0 &&
                    doneById.getAttribute('aria-disabled') !== 'true') {
                    (doneById.querySelector('button') || doneById).click();
                    return {clicked: true, method: 'id'};
                }

                // 备选: 文字匹配
                const btns = dialog.querySelectorAll('ytcp-button');
                for (const btn of btns) {
                    if (btn.offsetWidth === 0) continue;
                    const text = (btn.innerText || '').trim();
                    if (text === '完成' || text === 'Done') {
                        if (btn.getAttribute('aria-disabled') !== 'true') {
                            (btn.querySelector('button') || btn).click();
                            return {clicked: true, method: 'text', text};
                        }
                        return {clicked: false, disabled: true};
                    }
                }
                return {error: 'not_found'};
            }
        """)

        if done.get("clicked"):
            await asyncio.sleep(3)
            log("商品链接添加完成 ✅", "OK")
        else:
            log(f"完成按钮问题: {done}", "WARN")

        # Step 6: 强制确保对话框已关闭 + 清理 backdrop
        await asyncio.sleep(1)
        await page.evaluate(r"""
            () => {
                // 关闭商品对话框
                const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                if (dialog && dialog.offsetWidth > 0) {
                    const closeBtn = dialog.querySelector('#close-button');
                    if (closeBtn) closeBtn.click();
                    else {
                        const pd = dialog.querySelector('tp-yt-paper-dialog');
                        if (pd && pd.close) pd.close();
                    }
                }
                // 清除 backdrop
                document.querySelectorAll('tp-yt-iron-overlay-backdrop').forEach(b => {
                    if (b.offsetWidth > 0 || b.style.zIndex) b.style.display = 'none';
                });
            }
        """)
        await asyncio.sleep(1)
        return True

    except Exception as e:
        log(f"添加商品链接失败: {e}", "WARN")
        # 强制清理: 关闭对话框 + backdrop
        try:
            await page.evaluate(r"""
                () => {
                    const dialog = document.querySelector('ytshopping-product-tagging-dialog');
                    if (dialog) {
                        const closeBtn = dialog.querySelector('#close-button');
                        if (closeBtn) closeBtn.click();
                        const pd = dialog.querySelector('tp-yt-paper-dialog');
                        if (pd && pd.close) pd.close();
                        dialog.style.display = 'none';
                    }
                    document.querySelectorAll('tp-yt-iron-overlay-backdrop').forEach(b => b.style.display = 'none');
                }
            """)
            await asyncio.sleep(2)
        except:
            pass
        return False


# ============ Video Elements: 片尾画面 ============

async def add_endscreen(page) -> bool:
    """
    在 Video Elements 页面添加片尾画面 (使用模板)
    验证通过: 2026-02-13
    
    流程: 点击 #endscreens-button → 选择模板 → 保存
    """
    log("添加片尾画面...")
    
    try:
        # Step 1: 点击片尾画面按钮
        es_btn = page.locator("#endscreens-button").first
        if await es_btn.count() == 0:
            log("未找到 #endscreens-button", "WARN")
            return False
        
        await es_btn.click()
        await asyncio.sleep(3)
        
        # Step 2: 选择模板 - 点击第一个模板卡片
        # 模板标签在 ytve-endscreen-template-picker 中
        # 优先选「1个视频, 1个订阅元素」(最常用, 位置最靠前)
        template_clicked = await page.evaluate(r"""
            () => {
                const picker = document.querySelector('ytve-endscreen-template-picker');
                if (!picker) return false;
                
                // 找标签文字对应的父元素 (预览卡片)
                const items = picker.querySelectorAll('[class*="item"], [class*="template"]');
                for (const item of items) {
                    if (item.offsetWidth === 0 || item.offsetWidth > 300) continue;
                    const text = (item.innerText || '').trim();
                    // 匹配「1 个视频, 1 个订阅元素」的预览区域
                    if (text === '1 个视频, 1 个订阅元素' || text === '1 个播放列表, 1 个订阅元素') {
                        // 找父容器 (预览卡片)
                        const parent = item.parentElement;
                        if (parent && parent.offsetWidth > 100 && parent.offsetWidth <= 300) {
                            parent.click();
                            return true;
                        }
                    }
                }
                return false;
            }
        """)
        
        if not template_clicked:
            # 备选: 用坐标 (从探测知道第一个模板在 (164, 237))
            log("用坐标方式选择模板...", "WARN")
            await page.mouse.click(164, 237)
        
        await asyncio.sleep(3)
        
        # Step 3: 检查保存按钮
        save_ok = await page.evaluate(r"""
            () => {
                const btn = document.querySelector('ytve-modal-host #save-button');
                return btn && btn.getAttribute('aria-disabled') !== 'true' && btn.offsetWidth > 0;
            }
        """)
        
        if not save_ok:
            # 可能第一次点击不够精确, 再点一下
            await page.mouse.click(164, 237)
            await asyncio.sleep(3)
            save_ok = await page.evaluate(r"""
                () => {
                    const btn = document.querySelector('ytve-modal-host #save-button');
                    return btn && btn.getAttribute('aria-disabled') !== 'true' && btn.offsetWidth > 0;
                }
            """)
        
        if save_ok:
            await page.evaluate("document.querySelector('ytve-modal-host #save-button').click()")
            await asyncio.sleep(3)
            log("片尾画面已保存 ✅", "OK")
            return True
        else:
            log("片尾画面保存不可用, 舍弃", "WARN")
            await page.evaluate("document.querySelector('#discard-button')?.click()")
            await asyncio.sleep(1)
            return False
        
    except Exception as e:
        log(f"添加片尾画面失败: {e}", "WARN")
        try:
            await page.evaluate("document.querySelector('#discard-button')?.click()")
            await asyncio.sleep(1)
        except:
            pass
        return False


# ============ 播放列表操作 ============
async def set_playlist(page, playlist_name: str) -> bool:
    """
    设置播放列表: 如果已存在则勾选, 不存在则新建
    从 test_playlist_final.py 提取的经验证逻辑
    """
    if not playlist_name:
        return True
    
    log(f"设置播放列表: '{playlist_name}'")
    
    # --- 打开播放列表面板 ---
    # 先滚动到播放列表区域
    await page.evaluate("""
        () => {
            const el = document.querySelector('ytcp-video-metadata-playlists');
            if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    """)
    await asyncio.sleep(0.5)
    
    trigger = page.locator("ytcp-video-metadata-playlists ytcp-dropdown-trigger[role='button']").first
    if await trigger.count() > 0:
        await trigger.click(timeout=10000)
        log("播放列表面板已打开", "OK")
        await asyncio.sleep(2)
    else:
        log("无法打开播放列表面板", "WARN")
        return False
    
    # --- 获取已有播放列表 ---
    items = await page.evaluate("""
        () => {
            const group = document.querySelector('ytcp-checkbox-group#playlists-list');
            if (!group) return [];
            const checkboxes = group.querySelectorAll('ytcp-checkbox-lit');
            const results = [];
            for (let i = 0; i < checkboxes.length; i++) {
                const cb = checkboxes[i];
                let text = '';
                const labelId = cb.getAttribute('aria-labelledby');
                if (labelId) {
                    const labelEl = document.getElementById(labelId);
                    if (labelEl) text = labelEl.innerText.trim();
                }
                if (!text) {
                    const parent = cb.closest('li') || cb.closest('ytcp-ve') || cb.parentElement;
                    if (parent) {
                        const label = parent.querySelector('label');
                        if (label) text = label.innerText.trim();
                    }
                }
                results.push({
                    index: i,
                    text,
                    checked: cb.hasAttribute('checked') || cb.getAttribute('aria-checked') === 'true'
                });
            }
            return results;
        }
    """)
    
    log(f"已有 {len(items)} 个播放列表", "INFO")
    
    # --- 检查是否已存在 ---
    target = None
    for item in items:
        if item['text'] == playlist_name:
            target = item
            break
    
    if target:
        # 已存在, 直接勾选
        if not target['checked']:
            idx = target['index']
            await page.evaluate(f"""
                () => {{
                    const group = document.querySelector('ytcp-checkbox-group#playlists-list');
                    const cbs = group.querySelectorAll('ytcp-checkbox-lit');
                    (cbs[{idx}].querySelector('#checkbox-container') || cbs[{idx}]).click();
                }}
            """)
            log(f"已勾选已有播放列表: '{playlist_name}'", "OK")
            await asyncio.sleep(1)
        else:
            log(f"播放列表 '{playlist_name}' 已勾选", "OK")
    else:
        # 不存在, 新建
        log(f"播放列表不存在, 创建中...", "INFO")
        
        # Step 1: 点击 chevron 下拉
        clicked = await page.evaluate("""
            () => {
                const dialog = document.querySelector('ytcp-playlist-dialog');
                if (!dialog) return false;
                const btns = dialog.querySelectorAll('ytcp-button');
                for (const btn of btns) {
                    const icon = btn.getAttribute('icon') || '';
                    if (icon.includes('chevron') && (btn.offsetParent !== null || btn.offsetWidth > 0)) {
                        (btn.querySelector('button') || btn).click();
                        return true;
                    }
                }
                return false;
            }
        """)
        if not clicked:
            log("未找到新建按钮", "WARN")
            return False
        await asyncio.sleep(1.5)
        
        # Step 2: 选择 "新播放列表"
        selected = await page.evaluate("""
            () => {
                const dialog = document.querySelector('ytcp-playlist-dialog');
                if (!dialog) return { success: false };
                const menu = dialog.querySelector('ytcp-text-menu#action-menu');
                if (!menu) return { success: false };
                const paperDialog = menu.querySelector('tp-yt-paper-dialog');
                if (paperDialog) {
                    const listbox = paperDialog.querySelector('tp-yt-paper-listbox');
                    if (listbox) {
                        const items = listbox.querySelectorAll('tp-yt-paper-item');
                        if (items.length > 0) {
                            items[0].click();
                            return { success: true, clicked: (items[0].innerText || '').trim() };
                        }
                    }
                }
                return { success: false };
            }
        """)
        if not selected.get('success'):
            log("选择新播放列表失败", "WARN")
            return False
        log(f"已选择: '{selected.get('clicked', '')}'", "OK")
        await asyncio.sleep(2.5)
        
        # Step 3: 填入标题
        filled = await page.evaluate("""
            (playlistName) => {
                const dialogs = document.querySelectorAll('tp-yt-paper-dialog');
                let createDialog = null;
                for (const d of dialogs) {
                    if (d.offsetParent === null && d.offsetWidth === 0) continue;
                    if (d.closest('ytcp-text-menu')) continue;
                    if (d.closest('ytcp-playlist-dialog')) continue;
                    if (d.closest('ytcp-uploads-dialog')) continue;
                    if (d.closest('ytcp-multi-progress-monitor')) continue;
                    if (d.querySelector('#title-textarea, #textbox, [contenteditable], textarea, input')) {
                        createDialog = d;
                        break;
                    }
                }
                if (!createDialog) return { success: false, msg: 'dialog not found' };
                
                let titleEl = createDialog.querySelector('#title-textarea #textbox');
                let method = 'title-textbox';
                if (!titleEl) {
                    titleEl = createDialog.querySelector('textarea');
                    method = 'textarea';
                }
                if (!titleEl) {
                    titleEl = createDialog.querySelector('[contenteditable="true"]');
                    method = 'contenteditable';
                }
                if (!titleEl) return { success: false, msg: 'no input found' };
                
                titleEl.focus();
                if (titleEl.tagName === 'TEXTAREA' || titleEl.tagName === 'INPUT') {
                    titleEl.value = playlistName;
                    titleEl.dispatchEvent(new Event('input', { bubbles: true }));
                } else {
                    titleEl.textContent = playlistName;
                    titleEl.dispatchEvent(new Event('input', { bubbles: true }));
                }
                return { success: true, method };
            }
        """, playlist_name)
        
        if not filled.get('success'):
            log(f"填入标题失败: {filled.get('msg')}", "WARN")
            return False
        log(f"已填入标题 (method={filled['method']})", "OK")
        await asyncio.sleep(1.5)
        
        # Step 4: 点击创建按钮
        create_ok = await page.evaluate("""
            () => {
                const dialogs = document.querySelectorAll('tp-yt-paper-dialog');
                for (const dialog of dialogs) {
                    if (dialog.offsetParent === null && dialog.offsetWidth === 0) continue;
                    if (dialog.closest('ytcp-text-menu')) continue;
                    if (dialog.closest('ytcp-playlist-dialog')) continue;
                    if (dialog.closest('ytcp-uploads-dialog')) continue;
                    if (dialog.closest('ytcp-multi-progress-monitor')) continue;
                    if (!dialog.querySelector('#title-textarea, #textbox, [contenteditable], textarea, input')) continue;
                    
                    const buttons = dialog.querySelectorAll('ytcp-button');
                    for (const btn of buttons) {
                        if (btn.offsetParent !== null || btn.offsetWidth > 0) {
                            if (btn.getAttribute('aria-disabled') !== 'true') {
                                const text = (btn.innerText || '').trim();
                                if (text) {
                                    (btn.querySelector('button') || btn).click();
                                    return { success: true, clicked: text };
                                }
                            }
                        }
                    }
                }
                return { success: false };
            }
        """)
        
        if create_ok.get('success'):
            log(f"播放列表已创建: '{playlist_name}'", "OK")
            await asyncio.sleep(3)
        else:
            log("创建按钮未找到", "WARN")
            return False
    
    # --- 点击 Done 关闭面板 ---
    await page.evaluate("""
        () => {
            const dialog = document.querySelector('ytcp-playlist-dialog');
            if (!dialog) return;
            const btns = dialog.querySelectorAll('ytcp-button');
            for (const btn of btns) {
                if (btn.offsetParent === null && btn.offsetWidth === 0) continue;
                const icon = btn.getAttribute('icon') || '';
                if (icon.includes('chevron')) continue;
                if (btn.getAttribute('aria-disabled') === 'true') continue;
                (btn.querySelector('button') || btn).click();
                break;
            }
        }
    """)
    log("播放列表面板已关闭", "OK")
    await asyncio.sleep(2)
    
    return True


async def is_channel_creation_required(page) -> bool:
    """检测当前账号是否仍停留在 Create Channel 页。"""
    try:
        body_text = await page.locator("body").inner_text(timeout=8000)
    except Exception:
        return False

    lowered = body_text.lower()
    markers = [
        "how you'll appear",
        "create channel",
        "建立頻道",
        "创建频道",
        "by clicking create channel",
    ]
    return any(marker in lowered for marker in markers)


async def is_google_login_required(page) -> bool:
    """Detect account chooser / sign-in challenge pages and fail fast."""
    current_url = (getattr(page, "url", "") or "").lower()
    if "accounts.google.com" in current_url:
        return True

    try:
        body_text = await page.locator("body").inner_text(timeout=6000)
    except Exception:
        return False

    lowered = body_text.lower()
    markers = [
        "choose an account",
        "use another account",
        "sign in",
        "password",
        "已退出账号",
        "使用其他账号",
        "请选择账号",
        "登录",
        "登入",
    ]
    return any(marker in lowered for marker in markers)


async def select_best_upload_page(context, *, log_selection: bool = True):
    """Pick the most relevant YouTube/Studio page from a reused CDP context."""
    pages = list(getattr(context, "pages", []) or [])
    if not pages:
        return await context.new_page()

    ranked_pages = []
    for index, page in enumerate(pages):
        url = (getattr(page, "url", "") or "").lower()
        score = 0
        reason = "fallback"

        if "studio.youtube.com" in url:
            score = 100
            reason = "studio"
        elif "youtube.com" in url and "console.bitbrowser.net" not in url:
            score = 80
            reason = "youtube"
        elif "accounts.google.com" in url:
            score = 20
            reason = "google_login"
        elif "gds.google.com" in url:
            score = 10
            reason = "google_redirect"
        elif "console.bitbrowser.net" in url:
            score = -100
            reason = "bitbrowser_console"
        elif url.startswith("chrome-extension://"):
            score = -200
            reason = "extension"

        title = ""
        try:
            title = (await page.title()).strip()
        except Exception:
            title = ""

        if title:
            lowered_title = title.lower()
            if "youtube studio" in lowered_title and score < 90:
                score = max(score, 90 if "gds.google.com" not in url else 15)
                reason = "studio_title" if "gds.google.com" not in url else "google_redirect_title"
            elif "google account" in lowered_title and score < 25:
                score = max(score, 25)
                reason = "google_account_title"
            elif "bitbrowser" in lowered_title and score > -100:
                score = -100
                reason = "bitbrowser_title"

        ranked_pages.append((score, index, page, reason, title, url))

    ranked_pages.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    selected = ranked_pages[0]
    page = selected[2]
    if log_selection:
        log(
            "候选标签页: "
            + " | ".join(
                f"#{index}:{reason}:score={score}:title={(title or 'N/A')[:40]}:url={(url or 'about:blank')[:100]}"
                for score, index, _page, reason, title, url in ranked_pages
            )
        )
        log(
            f"选中上传标签页: #{selected[1]} ({selected[3]}) -> {(selected[4] or 'N/A')[:60]} | {(selected[5] or 'about:blank')[:160]}",
            "OK",
        )
    return page


# ============ 主上传函数 ============
async def upload_single(
    container_code: str,
    serial: int,
    video_path: Path,
    thumbnails: List[Path],
    title: str,
    description: str,
    is_ypp: bool,
    ab_test_titles: List[str] = None,
    playlist_name: str = None,
    tags: Optional[List[str]] = None,
    visibility: str = "public",
    scheduled_publish_at: Optional[str] = None,
    schedule_timezone: Optional[str] = None,
    made_for_kids: bool = False,
    altered_content: bool = True,
    notify_subscribers: bool = False,
    category: str = "Music",
) -> Dict[str, Any]:
    """上传单个视频到指定环境"""
    from playwright.async_api import async_playwright
    
    log(f"=" * 60)
    log(f"开始上传: 序号 {serial}")
    log(f"视频: {video_path.name}")
    log(f"YPP: {'是' if is_ypp else '否'}")
    log(f"=" * 60)
    
    # 启动浏览器
    debug_port = start_browser(container_code)
    if not debug_port:
        log("启动浏览器失败", "ERR")
        return make_upload_result(False, True, "启动浏览器失败", "start_browser_failed")
    
    log(f"浏览器端口: {debug_port}", "OK")
    
    # ========== 智能等待: 检测浏览器是否已经在运行 ==========
    # 如果端口已经可达，说明浏览器之前就开着，跳过 8 秒等待
    import requests as _requests
    browser_already_running = False
    try:
        check = _requests.get(f"http://127.0.0.1:{debug_port}/json/version", timeout=3)
        if check.status_code == 200:
            browser_already_running = True
            log(f"浏览器已在运行，跳过等待 ⚡", "OK")
    except:
        pass
    
    if not browser_already_running:
        log("等待浏览器完全启动...", "WAIT")
        time.sleep(8)
    
    async with async_playwright() as p:
        try:
            # 添加连接重试机制
            browser = None
            connect_retries = 3
            for attempt in range(connect_retries):
                try:
                    browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}")
                    log(f"浏览器连接成功", "OK")
                    break
                except Exception as e:
                    if attempt < connect_retries - 1:
                        log(f"连接失败，重试 ({attempt + 1}/{connect_retries})...", "WARN")
                        time.sleep(3)
                    else:
                        raise e
            
            context = browser.contexts[0]
            page = await select_best_upload_page(context)
            
            # ========== 窗口切换: 将浏览器窗口带到前台 ==========
            await page.bring_to_front()
            log(f"窗口已切换到前台 ✨", "OK")
            
            # 导航到 YouTube Studio (智能判断)
            current_url = page.url
            studio_url = with_studio_locale(current_url if "studio.youtube.com" in current_url else None)
            
            if "studio.youtube.com" not in current_url:
                # 不在 YouTube Studio，需要导航
                log(f"导航到 YouTube Studio (hl={STUDIO_UI_LANGUAGE}, gl={STUDIO_UI_LOCATION})...")
                nav_retries = 3
                for attempt in range(nav_retries):
                    try:
                        await page.goto(studio_url, wait_until="domcontentloaded", timeout=120000)
                        log("页面导航成功", "OK")
                        break
                    except Exception as e:
                        error_msg = str(e)
                        if "ERR_CONNECTION" in error_msg or "net::" in error_msg:
                            if attempt < nav_retries - 1:
                                log(f"网络连接错误，等待后重试 ({attempt + 1}/{nav_retries})...", "WARN")
                                await asyncio.sleep(5)
                                try:
                                    await page.goto("about:blank", timeout=10000)
                                    await asyncio.sleep(2)
                                except:
                                    pass
                            else:
                                raise e
                        else:
                            raise e
                await random_delay(10, 20, "等待页面加载")
            else:
                # 已在 YouTube Studio，补齐英文参数后再刷新，确保后续文案稳定
                if page.url != studio_url:
                    log(f"已在 YouTube Studio，切到英文界面 (hl={STUDIO_UI_LANGUAGE}, gl={STUDIO_UI_LOCATION})...")
                    await page.goto(studio_url, wait_until="domcontentloaded")
                else:
                    log(f"已在 YouTube Studio，刷新页面...")
                    await page.reload(wait_until="domcontentloaded")
                await random_delay(5, 8, "等待页面刷新")

            if await is_channel_creation_required(page):
                log("当前账号尚未创建 YouTube 频道，无法执行上传", "ERR")
                return make_upload_result(False, True, "当前账号尚未创建 YouTube 频道", "channel_not_created")
            if await is_google_login_required(page):
                log("当前窗口未登录 Google / YouTube Studio，无法自动上传", "ERR")
                return make_upload_result(
                    False,
                    False,
                    "当前窗口未登录 Google / YouTube Studio",
                    "login_required",
                    extra={"debug_port": debug_port},
                )
            
            # ========== 智能上传入口 (多语言通用) ==========
            # 策略: 优先使用 ID 选择器，自动检测是否需要下拉菜单
            # 2026-02-24 修复: 增加重试机制，避免页面未完全加载时找不到按钮
            
            # Step 1: 点击 Create/Upload 按钮
            # 新增 header Create 按钮选择器 (YouTube Studio 2026 新版 UI)
            create_selectors = [
                "#upload-icon",
                "#create-icon",
                "ytcp-button.ytcpAppHeaderCreateIcon",  # header 中的 Create 按钮
                "ytcp-quick-actions ytcp-icon-button:first-child",
            ]
            
            create_btn = None
            used_selector = None
            max_create_retries = 2  # 最多重试 2 轮 (含刷新)
            
            for create_round in range(max_create_retries):
                # 先快速扫一遍
                for sel in create_selectors:
                    btn = page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible():
                        create_btn = btn
                        used_selector = sel
                        break
                
                if create_btn:
                    break
                
                # 快速扫描没找到，用 wait_for_selector 等待
                log(f"Create 按钮未立即出现，等待页面加载... (round {create_round + 1}/{max_create_retries})", "WAIT")
                for sel in create_selectors:
                    try:
                        await page.wait_for_selector(sel, timeout=15000)
                        btn = page.locator(sel).first
                        if await btn.count() > 0 and await btn.is_visible():
                            create_btn = btn
                            used_selector = sel
                            break
                    except:
                        continue
                
                if create_btn:
                    break
                
                # 仍然找不到，刷新页面再试一次
                if create_round < max_create_retries - 1:
                    log("Create 按钮仍未找到，刷新页面重试...", "WARN")
                    await page.reload(wait_until="domcontentloaded")
                    await asyncio.sleep(10)
            
            opened_direct_upload = False
            if create_btn:
                await human_click(page, create_btn, f"Create ({used_selector})")
                await random_delay(1, 2)
            else:
                opened_direct_upload = await open_direct_upload_page(page)
                if not opened_direct_upload:
                    log("找不到 Create/Upload 按钮 (已重试)", "ERR")
                    return make_upload_result(False, True, "找不到 Create/Upload 按钮", "create_button_missing")
            
            # Step 2: 检查是否有下拉菜单 (有些版本直接打开对话框)
            menu = page.locator("ytcp-text-menu")
            menu_visible = False
            try:
                menu_visible = await menu.is_visible()
            except:
                pass
            
            if menu_visible and not opened_direct_upload:
                log("检测到下拉菜单，点击第一项 (Upload videos)")
                upload_option = page.locator("ytcp-text-menu tp-yt-paper-item").first
                await human_click(page, upload_option, "Upload videos")
                await random_delay(2, 3)
            elif not opened_direct_upload:
                log("直接打开上传对话框 (无需下拉菜单)")
                await random_delay(1, 2)
            else:
                log("已进入直达上传页，跳过 Create 下拉菜单检测", "INFO")
            
            # Step 3: 使用 CDP 选择视频文件（全程后台，不需要浏览器前台）
            title_selector = "ytcp-social-suggestions-textbox#title-textarea div#textbox"
            file_selected = False
            max_file_retries = 3
            
            for file_attempt in range(max_file_retries):
                # CDP 选择文件
                cdp_success = await select_file_with_cdp(page, str(video_path))
                
                if cdp_success:
                    # 等待上传详情页出现
                    try:
                        ready = await wait_for_upload_details_ready(page, timeout_ms=25000)
                        if not ready:
                            if await is_phone_verification_required_for_upload(page):
                                log("当前账号未完成手机验证，无法上传超过 15 分钟的视频", "ERR")
                                return make_upload_result(
                                    False,
                                    True,
                                    "当前账号未完成手机验证，无法上传超过 15 分钟的视频",
                                    "phone_verification_required",
                                )
                            raise TimeoutError("upload details not ready")
                        log("上传详情页面已出现 (CDP 后台模式)", "OK")
                        await asyncio.sleep(1.5)
                        unreadable_error = await detect_upload_file_read_error(page)
                        if unreadable_error:
                            return make_upload_result(
                                False,
                                True,
                                f"浏览器提示文件不可读: {unreadable_error}",
                                "upload_file_unreadable",
                            )
                        file_selected = True
                        break
                    except:
                        log(f"详情页未出现 (可能 YouTube 抽风)，刷新重试 ({file_attempt + 1}/{max_file_retries})", "WARN")
                else:
                    log(f"CDP 设置文件失败，刷新重试 ({file_attempt + 1}/{max_file_retries})", "WARN")
                
                # 重试：关闭对话框 → 刷新 → 重新打开 → 再 CDP
                if file_attempt < max_file_retries - 1:
                    # 关闭可能存在的对话框
                    close_btn = page.locator('ytcp-icon-button#close-button').first
                    if await close_btn.count() > 0:
                        try:
                            await close_btn.click()
                            await asyncio.sleep(1)
                            # 处理 Discard 确认
                            discard_btn = page.locator('ytcp-button[id="discard-button"]').first
                            if await discard_btn.count() > 0 and await discard_btn.is_visible():
                                await discard_btn.click()
                                await asyncio.sleep(1)
                        except:
                            pass
                    
                    # 优先走更稳的恢复链，避免 reload 在 Studio 卡死
                    reloaded = False
                    try:
                        await page.goto(with_studio_locale("https://studio.youtube.com"), wait_until="domcontentloaded", timeout=45000)
                        reloaded = True
                    except Exception as nav_exc:
                        log(f"返回 Studio 首页失败，改用 reload: {nav_exc}", "WARN")
                        try:
                            await page.reload(wait_until="domcontentloaded", timeout=45000)
                            reloaded = True
                        except Exception as reload_exc:
                            log(f"reload 失败，直接改走上传页兜底: {reload_exc}", "WARN")
                    if reloaded:
                        await random_delay(5, 8)
                    
                    # 重新打开上传对话框
                    reopened = False
                    for sel in ["#upload-icon", "#create-icon", "ytcp-button.ytcpAppHeaderCreateIcon"]:
                        btn = page.locator(sel).first
                        if await btn.count() > 0 and await btn.is_visible():
                            await human_click(page, btn, f"Create ({sel})")
                            reopened = True
                            break
                    if not reopened:
                        reopened = await open_direct_upload_page(page)
                    if not reopened:
                        log("恢复 Create/Upload 对话框失败，保留页面供下一轮 file chooser 重试", "WARN")
                    await random_delay(1, 2)
                    
                    # 检查下拉菜单
                    menu = page.locator("ytcp-text-menu")
                    if reopened and await menu.count() > 0:
                        try:
                            if await menu.is_visible():
                                upload_option = page.locator("ytcp-text-menu tp-yt-paper-item").first
                                await human_click(page, upload_option, "Upload videos")
                                await random_delay(2, 3)
                        except:
                            pass
            
            if not file_selected:
                log("视频文件选择失败（已重试 3 次）", "ERR")
                return make_upload_result(False, True, "视频文件选择失败", "file_select_failed")
            
            await random_delay(2, 3)
            
            # 根据 YPP 状态决定流程
            if is_ypp and thumbnails and len(thumbnails) >= 3 and ab_test_titles and len(ab_test_titles) >= 3:
                # ========== YPP 频道：先开 A/B Testing ==========
                log("YPP 频道：尝试寻找 A/B Testing 入口...", "OK")
                
                # === 增加重试检测机制 (2026-02-06 修复) ===
                # 解决因页面异步加载导致按钮未及时出现而被跳过的问题
                max_retries = 5
                ab_btn_found = False
                ab_btn = None
                
                for retry in range(max_retries):
                    # 优先使用 ID 选择器
                    ab_btn = page.locator('ytcp-button#ab-test-button').first
                    if await ab_btn.count() > 0 and await ab_btn.is_visible():
                        ab_btn_found = True
                        break
                    
                    # 备用: 文本选择器
                    ab_btn = page.locator('text=A/B Testing').first
                    if await ab_btn.count() > 0 and await ab_btn.is_visible():
                        ab_btn_found = True
                        break
                    
                    log(f"  等待 A/B Testing 按钮出现... ({retry+1}/{max_retries})", "WAIT")
                    await asyncio.sleep(2)  # 每次等 2 秒
                
                if ab_btn_found:
                    await human_click(page, ab_btn, "A/B Testing (#ab-test-button)")
                    await random_delay(2.0, 3.0)
                    
                    # 选择 Title and thumbnail (可能默认已选)
                    title_thumb_option = page.locator('text=Title and thumbnail').first
                    if await title_thumb_option.count() > 0 and await title_thumb_option.is_visible():
                        await human_click(page, title_thumb_option, "Title and thumbnail")
                        await random_delay(1.5, 2.5)
                    
                    # 填写 3 个标题
                    # 优化: 使用更精确的选择器，增加延迟和清空操作
                    title_boxes = page.locator('ytcp-video-experiment-create-dialog #textbox')
                    box_count = await title_boxes.count()
                    
                    if box_count >= 3:
                        for i in range(3):
                            box = title_boxes.nth(i)
                            await box.click()
                            await asyncio.sleep(0.5)
                            
                            # 清空并填入
                            await box.fill("")
                            await asyncio.sleep(0.3)
                            
                            title_content = ab_test_titles[i][:100]
                            await box.fill(title_content)
                            
                            # 按 ESC 关闭标签建议下拉菜单
                            try:
                                await page.keyboard.press('Escape')
                                await asyncio.sleep(0.3)
                            except:
                                pass
                            
                            log(f"填写标题 {i+1} 并等待...", "WAIT")
                            # 关键: 增加等待时间，防止数据丢失
                            await asyncio.sleep(1.5)
                            
                    log("3 个标题填写完成", "OK")
                    
                    # 上传 3 张封面 (使用 set_input_files)
                    log("上传 A/B 封面...")
                    await asyncio.sleep(2)
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=10000)
                    except Exception:
                        pass

                    uploaded_thumbs = 0
                    for idx in range(min(3, len(thumbnails))):
                        thumb_uploaded = False
                        for attempt in range(2):
                            try:
                                ab_inputs = page.locator('ytcp-video-experiment-create-dialog input[type="file"]')
                                input_count = await ab_inputs.count()
                                if input_count <= idx:
                                    log(f"  封面输入框数量不足，等待重试 ({attempt + 1}/2)", "WARN")
                                    await asyncio.sleep(2)
                                    continue
                                await ab_inputs.nth(idx).set_input_files(str(thumbnails[idx]))
                                log(f"  封面 {idx+1} 上传中...")
                                await random_delay(1.5, 2.5)
                                thumb_uploaded = True
                                uploaded_thumbs += 1
                                break
                            except Exception as e:
                                log(f"  封面 {idx+1} 上传异常，重试 {attempt + 1}/2: {e}", "WARN")
                                await asyncio.sleep(3)
                                try:
                                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                                except Exception:
                                    pass
                        if not thumb_uploaded:
                            return make_upload_result(
                                False,
                                True,
                                f"A/B 封面 {idx+1} 上传失败",
                                "ab_thumbnail_upload_failed",
                            )

                    log(f"{uploaded_thumbs} 张 A/B 封面上传完成", "OK")
                    
                    # 等待封面处理完成 (验证成功 2026-01-30: 需要等 10-15 秒)
                    log("等待封面处理完成...", "WAIT")
                    await asyncio.sleep(12)
                    
                    # 点击 Set test
                    set_test_btn = page.locator('ytcp-button:has-text("Set test")').first
                    if await set_test_btn.count() > 0:
                        await human_click(page, set_test_btn, "Set test")
                        # 关键修复: 等待 A/B Testing 对话框完全关闭
                        # Set test 后 YouTube Studio 有 transition 动画，需要足够时间
                        log("等待 A/B Testing 对话框关闭...", "WAIT")
                        await asyncio.sleep(5)
                        
                        # 验证对话框已关闭
                        ab_dialog = page.locator('ytcp-video-experiment-create-dialog').first
                        for check in range(5):
                            try:
                                if await ab_dialog.count() == 0 or not await ab_dialog.is_visible():
                                    log("A/B Testing 对话框已关闭", "OK")
                                    break
                            except:
                                break
                            log(f"  对话框仍在关闭中... ({check+1}/5)", "WAIT")
                            await asyncio.sleep(2)
                    
                    log("A/B Testing 设置完成", "OK")
                else:
                    # 重试后仍未找到 A/B Testing 按钮
                    log(f"⚠️ 重试 {max_retries} 次后仍未找到 A/B Testing 按钮，跳过", "WARN")
                
                # 填写简介（在 A/B Testing 设置完成后）
                # 关键修复: 增加重试机制，确保简介真的填上了
                desc_selector = "ytcp-social-suggestions-textbox#description-textarea div#textbox"
                desc_filled = False
                for desc_attempt in range(3):
                    desc_input = page.locator(desc_selector).first
                    try:
                        # 等待简介输入框可交互
                        await desc_input.wait_for(state="visible", timeout=10000)
                        # 滚动到简介输入框确保可见
                        await desc_input.scroll_into_view_if_needed()
                        await asyncio.sleep(1)
                        
                        # 填写简介
                        result = await human_fill(page, desc_input, description[:5000], "简介")
                        if not result:
                            result = await fill_visible_upload_field(page, "description", description[:5000])
                        
                        # 验证简介是否真的填上了
                        await asyncio.sleep(1)
                        filled_text = await desc_input.inner_text()
                        if filled_text and len(filled_text.strip()) > 10:
                            log(f"简介填写成功 ({len(filled_text)} 字符)", "OK")
                            desc_filled = True
                            break
                        else:
                            log(f"简介可能未填上 (内容长度: {len(filled_text.strip())}), 重试 ({desc_attempt+1}/3)...", "WARN")
                            await asyncio.sleep(2)
                    except Exception as e:
                        log(f"简介填写异常: {e}, 重试 ({desc_attempt+1}/3)...", "WARN")
                        await asyncio.sleep(3)
                
                if not desc_filled:
                    log("⚠️ 简介可能未成功填写，请手动检查", "ERR")
                
                # 按 ESC 关闭简介中标签建议下拉菜单
                try:
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(0.3)
                except:
                    pass
                
                await random_delay(1, 2)
                
                # 设置播放列表 (YPP)
                if playlist_name:
                    try:
                        await set_playlist(page, playlist_name)
                    except Exception as e:
                        log(f"播放列表设置失败 (非致命): {e}", "WARN")
                
            else:
                # ========== 非 YPP 频道：正常流程 ==========
                # 填写标题
                title_input = page.locator(title_selector).first
                title_filled = await human_fill(page, title_input, title[:100], "标题")
                if not title_filled:
                    await fill_visible_upload_field(page, "title", title[:100])
                # 按 ESC 关闭标签建议
                try:
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(0.3)
                except:
                    pass
                await random_delay(1, 2)
                
                # 填写简介
                desc_selector = "ytcp-social-suggestions-textbox#description-textarea div#textbox"
                desc_input = page.locator(desc_selector).first
                desc_filled = await human_fill(page, desc_input, description[:5000], "简介")
                if not desc_filled:
                    await fill_visible_upload_field(page, "description", description[:5000])
                # 按 ESC 关闭标签建议
                try:
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(0.3)
                except:
                    pass
                await random_delay(1, 2)
                
                # 上传单张封面
                if thumbnails:
                    log("上传封面图 (单张)...")
                    thumbnail_input = page.locator('.style-scope.ytcp-video-custom-still-editor input[type="file"]').first
                    if await thumbnail_input.count() > 0:
                        await thumbnail_input.set_input_files(str(thumbnails[0]))
                        await random_delay(2, 3)
                        unreadable_error = await detect_upload_file_read_error(page)
                        if unreadable_error:
                            return make_upload_result(
                                False,
                                True,
                                f"浏览器提示文件不可读: {unreadable_error}",
                                "upload_file_unreadable",
                            )
                        log("封面上传完成", "OK")
                
                # 设置播放列表 (非 YPP)
                if playlist_name:
                    try:
                        await set_playlist(page, playlist_name)
                    except Exception as e:
                        log(f"播放列表设置失败 (非致命): {e}", "WARN")
            
            # ========== 设置儿童内容 ==========
            log(f"设置儿童内容 = {'Yes' if made_for_kids else 'No'}...")
            kids_selected = await set_made_for_kids_setting(page, made_for_kids)
            if not kids_selected:
                log("未能确认儿童内容选项，继续后续流程", "WARN")
            await random_delay(0.5, 1)
            
            # ========== 展开高级设置 ==========
            advanced_ready = await ensure_advanced_settings_open(page)
            if not advanced_ready:
                log("未能确认高级设置已展开，继续尝试后续控件定位", "WARN")

            # ========== 标签 ==========
            if tags:
                log(f"填写标签 ({len(tags)} 个)...")
                tags_ok = await fill_video_tags(page, tags)
                if not tags_ok:
                    log("标签未能确认填写成功，继续后续流程", "WARN")
            
            # ========== Category ==========
            log(f"设置 Category = {category}...")
            category_ok = await set_video_category(page, category)
            if not category_ok:
                return make_upload_result(
                    False,
                    True,
                    f"未能确认 Category = {category}",
                    "category_selection_failed",
                )

            # ========== AI / 合成内容 ==========
            log(f"设置 Altered content = {'Yes' if altered_content else 'No'}...")
            altered_selected = await set_altered_content_setting(page, altered_content)
            if not altered_selected:
                return make_upload_result(
                    False,
                    True,
                    f"未能确认 Altered content = {'Yes' if altered_content else 'No'}",
                    "altered_content_selection_failed",
                )

            log(f"设置订阅通知 = {'On' if notify_subscribers else 'Off'}...")
            notify_ok = await set_notify_subscribers(page, notify_subscribers)
            if not notify_ok:
                log("未能确认订阅通知开关状态，继续后续流程", "WARN")
            
            await random_delay(0.5, 1)

            edit_page_result = await try_publish_from_video_edit_page(
                page,
                serial=serial,
                visibility=visibility,
                scheduled_publish_at=scheduled_publish_at,
                schedule_timezone=schedule_timezone,
                debug_port=debug_port,
                context=context,
            )
            if edit_page_result is not None:
                return edit_page_result
            
            # 点击 Next (Details → Monetization 或 Video elements)
            # 大文件时，Studio 可能在上传进度较低时不渲染 Next 按钮；这里需要长等待。
            next_clicked = await click_next_button(page, "Next", timeout_ms=30 * 60 * 1000)
            if not next_clicked:
                log("Next 未就绪，重试设置儿童内容后再试一次...", "WARN")
                await set_made_for_kids_setting(page, made_for_kids)
                next_clicked = await click_next_button(page, "Next (retry)", timeout_ms=10 * 60 * 1000)
            if not next_clicked:
                edit_page_result = await try_publish_from_video_edit_page(
                    page,
                    serial=serial,
                    visibility=visibility,
                    scheduled_publish_at=scheduled_publish_at,
                    schedule_timezone=schedule_timezone,
                    debug_port=debug_port,
                    context=context,
                )
                if edit_page_result is not None:
                    return edit_page_result
                return make_upload_result(
                    False,
                    True,
                    "点击 Next 失败（Details）",
                    "next_click_failed_details",
                )
            await random_delay(2, 3)
            
            # 检查是否是 YPP 频道（有 Monetization 步骤）
            # 这里不能只靠页面文字，也不能先额外点一次 Next，否则会直接跳过 Monetization。
            monetization_step = page.locator("ytcp-video-monetization").first
            actual_is_ypp = await monetization_step.count() > 0
            if not actual_is_ypp:
                try:
                    actual_is_ypp = await page.locator("text=/Monetization|營利|收益|获利/i").count() > 0
                except Exception:
                    actual_is_ypp = False

            needs_monetization = bool(is_ypp or actual_is_ypp)
            
            if needs_monetization:
                log("检测到/期望 YPP 频道，必须完成 Monetization", "OK")

                monetization_ready = await wait_for_monetization_section(page, timeout_ms=15000)
                if not monetization_ready:
                    return make_upload_result(
                        False,
                        True,
                        "YPP 频道未出现 Monetization 页面",
                        "monetization_page_missing",
                    )

                log("设置 Monetization = On...")
                monetization_ok = await ensure_monetization_enabled(page)
                if not monetization_ok:
                    return make_upload_result(
                        False,
                        True,
                        "未能确认 Monetization = On",
                        "monetization_not_confirmed",
                    )
                log("Monetization = On ✅", "OK")

                # Step: 进入 Ad suitability
                ad_clicked = await click_next_button(page, "Next (Ad suitability)", timeout_ms=30000)
                if not ad_clicked:
                    return make_upload_result(
                        False,
                        True,
                        "点击 Next 失败（Ad suitability）",
                        "next_click_failed_ad_suitability",
                    )
                await random_delay(2, 3)
                
                # 点击 Got it（如果有）- 使用通用 ID
                got_it_btn = page.locator('ytcp-button#intro-panel-ack-button').first
                if await got_it_btn.count() > 0 and await got_it_btn.is_visible():
                    await human_click(page, got_it_btn, "Got it (#intro-panel-ack-button)")
                    await random_delay(1, 2)
                
                # 滚动到底部找 None of the above
                log("滚动到底部寻找 Ad suitability 选项...")
                # 使用 dialog scroll 方式更稳健
                for _ in range(3):
                    await scroll_dialog(page, 500)
                    await asyncio.sleep(0.5)
                
                # 勾选 None of the above + Submit rating（含重试机制）
                max_ad_retries = 3
                ad_success = False
                
                for ad_attempt in range(max_ad_retries):
                    # 勾选 None of the above
                    none_checkbox = page.locator('ytcp-checkbox-lit:has-text("None of the above")').first
                    if await none_checkbox.count() > 0:
                        await human_click(page, none_checkbox, "None of the above")
                        await random_delay(1, 1.5)
                        
                        # 点击 Submit rating
                        submit_btn = page.locator('ytcp-button#submit-questionnaire-button').first
                        if await submit_btn.count() > 0:
                            await human_click(page, submit_btn, "Submit rating (#submit-questionnaire-button)")
                            await random_delay(2, 3)
                            
                            # 验证：检查 Next 按钮是否可点
                            next_btn_check = page.locator("ytcp-button#next-button").first
                            is_disabled = await next_btn_check.get_attribute("aria-disabled")
                            
                            if is_disabled != "true":
                                log("Ad suitability 完成 ✅", "OK")
                                ad_success = True
                                break
                            else:
                                log(f"Submit rating 可能未生效 (Next 仍为灰色)，重试 ({ad_attempt + 1}/{max_ad_retries})", "WARN")
                                # 重新滚动到底部
                                for _ in range(3):
                                    await scroll_dialog(page, 500)
                                    await asyncio.sleep(0.5)
                        else:
                            log("未找到 Submit rating 按钮", "ERR")
                            break
                    else:
                        log("未找到 None of the above 选项", "WARN")
                        # 可能需要滚动更多
                        for _ in range(3):
                            await scroll_dialog(page, 500)
                            await asyncio.sleep(0.5)
                
                if not ad_success:
                    log("Ad suitability 重试用尽，继续流程", "WARN")
                
                # Step: 进入 Video elements
                ve_clicked = await click_next_button(page, "Next (Video elements)", timeout_ms=30000)
                if not ve_clicked:
                    return make_upload_result(
                        False,
                        True,
                        "点击 Next 失败（Video elements）",
                        "next_click_failed_video_elements",
                    )
                await random_delay(1, 2)
            else:
                log("非 YPP 频道，跳过 Monetization")
                
            await random_delay(1, 2)
            
            # ========== Video elements = A/B Testing ==========
            # 只有 YPP 频道才有 A/B Testing
            if is_ypp:
                # 使用通用 ID: ab-test-button
                ab_test_btn = page.locator("ytcp-button#ab-test-button").first
                if await ab_test_btn.count() > 0:
                    await human_click(page, ab_test_btn, "A/B Testing (#ab-test-button)")
                    await random_delay(1.5, 2.5)
            
            # ========== Video Elements: 商品/片尾/卡片 ==========
            log("===== Video Elements: 添加商品/片尾/卡片 =====")
            
            # 1. 添加商品链接 (YPP 专有功能)
            if is_ypp:
                await ensure_ve_page_clean(page)  # 确保页面干净
                try:
                    await add_product_links(page)
                except Exception as e:
                    log(f"商品链接失败 (非致命): {e}", "WARN")
                await random_delay(1, 2)
            
            # 2. 添加片尾画面 (所有频道通用)
            await ensure_ve_page_clean(page)  # 确保页面干净
            try:
                await add_endscreen(page)
            except Exception as e:
                log(f"片尾画面失败 (非致命): {e}", "WARN")
            await random_delay(1, 2)
            
            # 3. 添加播放列表卡片 (所有频道通用)
            await ensure_ve_page_clean(page)  # 确保页面干净
            try:
                await add_playlist_card(page, playlist_name)
            except Exception as e:
                log(f"播放列表卡片失败 (非致命): {e}", "WARN")
            await random_delay(1, 2)
            
            # 最终清理: 确保所有 VE 操作结束后页面干净
            await ensure_ve_page_clean(page)
            log("===== Video Elements 完成 =====")
            
            # 点击 Next 进入 Checks
            checks_clicked = await click_next_button(page, "Next (Checks)", timeout_ms=30000)
            if not checks_clicked:
                return make_upload_result(
                    False,
                    True,
                    "点击 Next 失败（Checks）",
                    "next_click_failed_checks",
                )
            await random_delay(2, 3)
            
            # Video elements → Checks
            # 点击 Next 进入 Visibility
            visibility_clicked = await click_next_button(page, "Next (Visibility)", timeout_ms=30000)
            if not visibility_clicked:
                return make_upload_result(
                    False,
                    True,
                    "点击 Next 失败（Visibility）",
                    "next_click_failed_visibility",
                )
            await random_delay(2, 3)
            
            log(f"设置可见性 = {visibility}...")
            visibility_ok = await apply_visibility_settings(
                page,
                visibility,
                scheduled_publish_at=scheduled_publish_at,
                schedule_timezone=schedule_timezone,
            )
            await random_delay(1, 2)
            if not visibility_ok:
                return make_upload_result(
                    False,
                    True,
                    f"未能确认可见性 = {visibility}",
                    "visibility_selection_failed",
                )
            
            # ========== 检测并点击 Publish / Save / Schedule 按钮 ==========
            publish_state = await get_visible_upload_dialog_button_state(
                page,
                button_id="done-button",
                text_pattern="done|save|publish|schedule|完成|保存|yayınla|發佈|发布|公開|定時|定时|排程",
            )
            
            if not publish_state.get("found"):
                log("未找到最终提交按钮，尝试 DOM 兜底直接点击", "WARN")
                publish_clicked = await try_click_publish_button(page)
                if not publish_clicked:
                    log("未找到最终提交按钮", "ERR")
                    return make_upload_result(False, True, "未找到最终提交按钮", "publish_button_missing")
                await asyncio.sleep(2)
                dialog_result = await handle_publish_anyway_dialog(
                    page,
                    serial=serial,
                    max_wait_seconds=15,
                    poll_seconds=1,
                )
                if dialog_result.get("detected") and not dialog_result.get("clicked"):
                    log("检测到内容检查提示，但未能自动点击 'Publish anyway'，后续监控会继续重试", "WARN")
                log("✅ 已点击最终提交按钮!", "OK")
                publish_state = {"found": True, "disabled": False, "clicked_via_dom": True}
            
            # 检查按钮状态：aria-disabled="true" 表示灰色不可点
            is_disabled = "true" if publish_state.get("disabled") else "false"
            
            if is_disabled == "true":
                # ========== 灰色 = 上传失败, 立刻取消 ==========
                log("❌ 最终提交按钮灰色 (不可点击) = 上传失败!", "ERR")
                log("正在取消上传并清理...", "ACT")
                
                try:
                    # 方案: 点击对话框右上角的 X 关闭按钮
                    close_btn = page.locator("ytcp-uploads-dialog ytcp-button#close-button, ytcp-uploads-dialog [aria-label*='los' i], ytcp-uploads-dialog [aria-label*='kapat' i]").first
                    if await close_btn.count() > 0:
                        await close_btn.click(timeout=5000)
                        log("已点击关闭按钮", "OK")
                        await asyncio.sleep(2)
                    else:
                        # 备选: 按 ESC
                        await page.keyboard.press("Escape")
                        log("已按 ESC", "OK")
                        await asyncio.sleep(2)
                    
                    # 处理确认对话框 ("是否取消上传?")
                    # 找确认按钮: 通常是 "Discard" / "İptal et" / 确认取消
                    discard_clicked = await page.evaluate("""
                        () => {
                            // 找所有可见的确认/取消对话框
                            const dialogs = document.querySelectorAll('ytcp-dialog, tp-yt-paper-dialog');
                            for (const d of dialogs) {
                                if (d.offsetParent === null && d.offsetWidth === 0) continue;
                                if (d.closest('ytcp-uploads-dialog')) continue;
                                
                                // 找确认按钮 (通常第二个或类似 "discard" 的)
                                const buttons = d.querySelectorAll('ytcp-button');
                                for (const btn of buttons) {
                                    if (btn.offsetParent === null && btn.offsetWidth === 0) continue;
                                    const text = (btn.innerText || '').trim().toLowerCase();
                                    // 匹配: discard, iptal et, 取消, 确认 等
                                    if (text.includes('discard') || text.includes('iptal') || 
                                        text.includes('sil') || text.includes('vazgeç')) {
                                        (btn.querySelector('button') || btn).click();
                                        return { success: true, clicked: text };
                                    }
                                }
                                
                                // 找不到特定文字, 点最后一个可见按钮 (通常是确认)
                                const visibleBtns = [];
                                for (const btn of buttons) {
                                    if (btn.offsetParent !== null || btn.offsetWidth > 0) {
                                        visibleBtns.push(btn);
                                    }
                                }
                                if (visibleBtns.length > 0) {
                                    const last = visibleBtns[visibleBtns.length - 1];
                                    const text = (last.innerText || '').trim();
                                    (last.querySelector('button') || last).click();
                                    return { success: true, clicked: text, method: 'last-visible' };
                                }
                            }
                            return { success: false };
                        }
                    """)
                    
                    if discard_clicked.get('success'):
                        log(f"已确认取消: '{discard_clicked.get('clicked', '')}' ", "OK")
                        await asyncio.sleep(3)
                    else:
                        log("未找到确认对话框, 尝试刷新页面...", "WARN")
                        # 刷新页面并处理可能的 beforeunload 弹窗
                        try:
                            await reload_with_optional_dialog(page, timeout=15000)
                            await asyncio.sleep(3)
                        except Exception:
                            pass
                    
                except Exception as e:
                    log(f"取消上传操作异常: {e}", "WARN")
                    # 最后兜底: 强制刷新
                    try:
                        await reload_with_optional_dialog(page, timeout=15000)
                        await asyncio.sleep(3)
                    except Exception:
                        pass
                
                log("📋 此频道将记录为失败, 需要重新上传", "ERR")
                return make_upload_result(False, True, "最终提交按钮灰色，已取消上传", "publish_disabled")
            
            # ========== 提交按钮可点击, 正常提交 ==========
            if not publish_state.get("clicked_via_dom"):
                log("点击最终提交按钮...", "ACT")
                try:
                    done_btn = page.locator("ytcp-button#done-button").first
                    publish_clicked = await human_click(page, done_btn, "Done / Publish / Schedule")
                    if not publish_clicked:
                        publish_clicked = await click_visible_upload_dialog_button(
                            page,
                            "Done / Publish / Schedule",
                            button_id="done-button",
                            text_pattern="done|save|publish|schedule|完成|保存|yayınla|發佈|发布|公開|定時|定时|排程",
                        )
                    if not publish_clicked:
                        publish_clicked = await try_click_publish_button(page)
                    if not publish_clicked:
                        raise RuntimeError("human_click 返回 False")
                    await asyncio.sleep(2)
                    dialog_result = await handle_publish_anyway_dialog(
                        page,
                        serial=serial,
                        max_wait_seconds=15,
                        poll_seconds=1,
                    )
                    if dialog_result.get("detected") and not dialog_result.get("clicked"):
                        log("检测到内容检查提示，但未能自动点击 'Publish anyway'，后续监控会继续重试", "WARN")
                    log("✅ 已点击最终提交按钮!", "OK")
                except Exception as e:
                    log(f"点击最终提交按钮失败: {e}", "ERR")
                    return make_upload_result(False, True, f"点击最终提交按钮失败: {e}", "publish_click_failed")

            monitor_result = await wait_for_safe_close_after_publish(page, serial, context=context)
            if not monitor_result.get("confirmed"):
                log("=" * 60)
                log("⚠️ Publish 已点击，但尚未确认上传完成，浏览器将保持打开", "WARN")
                log(f"原因: {monitor_result.get('reason', '未知')}", "WARN")
                log("=" * 60)
                return make_upload_result(
                    False,
                    False,
                    monitor_result.get("reason", "未确认安全关闭状态"),
                    "publish_pending_monitor",
                    monitor_result.get("snapshot"),
                    extra={"debug_port": debug_port},
                )

            final_state = monitor_result.get("snapshot", {}).get("status", "safe_to_close")
            log("=" * 60)
            log(f"✅ 已确认上传进入安全关闭状态: {final_state}", "OK")
            log(f"监控摘要: {monitor_result.get('reason', '')}", "OK")
            log("=" * 60)

            return make_upload_result(
                True,
                True,
                monitor_result.get("reason", "已确认安全关闭"),
                str(final_state),
                monitor_result.get("snapshot"),
            )
            
        except Exception as e:
            log(f"上传失败: {e}", "ERR")
            return make_upload_result(False, True, f"上传失败: {e}", "upload_exception")

# ============ 批量上传 ============
async def batch_upload(
    tag: str,
    date: str,
    dry_run: bool = False,
    single_channel: int = None,
    auto_confirm: bool = False,
    auto_close_browser: bool = False,
    skip_channels: Optional[List[int]] = None,
    max_open_windows: int = 10,
    window_ttl_hours: float = 2.0,
    active_success_windows: Optional[List[Dict]] = None,
    window_plan: Optional[Dict[str, Any]] = None,
    retain_video_days: int = 0,
):
    """批量上传指定标签组的视频"""
    date_key = normalize_date_mmdd(date)
    
    # 加载配置
    config = load_config()
    
    # 检查标签配置
    tag_config = _get_tag_config(config, tag, {})
    if not tag_config:
        log(f"未找到标签配置: {tag}，将使用实时分组和计划文件继续上传", "WARN")
        log(f"可用标签: {list(config['tag_to_project'].keys())}")
        tag_config = {"project_name": "", "video_keyword": tag}

    project_name = tag_config.get("project_name", "")
    video_keyword = tag_config.get("video_keyword", tag)  # 使用配置的关键词或标签名
    project_folder = Path(config["projects_folder"]) / project_name if project_name else None
    
    # ========== 动态环境 / 计划任务 ==========
    # 优先保留计划任务里的显式窗口信息，实时环境列表只作为补充。
    plan_tasks = _iter_window_plan_tasks(window_plan, tag)
    plan_task_by_serial = {int(task["serial"]): task for task in plan_tasks}

    containers, container_source, serial_to_channel_name = resolve_containers_for_tag(tag, project_folder)
    live_container_by_serial = {
        int(container.get("serialNumber", 0) or 0): dict(container)
        for container in containers
        if int(container.get("serialNumber", 0) or 0) > 0
    }
    mapping_registry = load_channel_mapping_registry()
    serial_to_container_code = {
        int(serial): str(info.get("containerCode") or "").strip()
        for serial, info in mapping_registry.items()
        if str(info.get("containerCode") or "").strip()
    }

    ordered_serials = [int(task["serial"]) for task in plan_tasks] if plan_tasks else sorted(live_container_by_serial.keys())
    if not ordered_serials:
        log(f"未找到标签为 '{tag}' 的环境或计划任务", "ERR")
        return make_batch_result(0, 0, 1)

    skip_set = set(skip_channels or [])
    if skip_set:
        ordered_serials = [serial for serial in ordered_serials if serial not in skip_set]
        log(f"跳过指定序号: {sorted(skip_set)}", "WARN")

    if not ordered_serials:
        log(f"标签 {tag} 过滤后无可上传频道", "WARN")
        return make_batch_result(0, 0, 0)

    all_serials = list(ordered_serials)
    ypp_serials = [
        serial
        for serial in ordered_serials
        if _task_is_ypp(
            serial=serial,
            plan_task=plan_task_by_serial.get(serial),
            manifest_channel=None,
            live_container=live_container_by_serial.get(serial),
        )
    ]

    log(f"标签: {tag}")
    log(f"项目: {project_name}")
    log(f"环境序号: {ordered_serials} (共 {len(ordered_serials)} 个)")
    log(f"YPP 序号: {ypp_serials}")
    log(f"环境来源: {container_source if containers else ('window_plan' if plan_tasks else container_source)}")
    log(f"找到 {len(containers)} 个实时环境")
    if serial_to_container_code:
        log(f"加载 channel_mapping: {len(serial_to_container_code)} 个映射", "OK")
    if serial_to_channel_name:
        log(f"加载频道名称: {len(serial_to_channel_name)} 个 (channels.md)", "OK")
    
    # 查找视频（使用配置的 video_keyword）
    tag_output_dir = _resolve_plan_output_dir(window_plan, tag, date_key)
    if tag_output_dir and tag_output_dir.exists():
        videos = sorted(
            [
                item
                for item in tag_output_dir.iterdir()
                if item.is_file() and item.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS
            ],
            key=lambda item: item.name.lower(),
        )
        log(f"使用计划指定成品目录: {tag_output_dir}", "OK")
    else:
        videos = find_videos(config["video_folder"], video_keyword, date_key, tag=tag)
    
    if not videos:
        log("当前未在标准目录扫描到视频文件，将优先依赖 manifest 中的显式视频路径", "WARN")
    else:
        log(f"找到 {len(videos)} 个视频文件")
        for v in videos:
            log(f"  - {v.name}")
    
    # ========== 新增: 尝试读取 upload_manifest.json ==========
    # Manifest 优先模式：从冻结的清单读取标题、简介和封面路径
    # 如果不存在，fallback 到旧模式（实时读 metadata_channels.md）
    manifest_data = None
    video_folder_path = Path(config["video_folder"])
    resolved_video_dir = tag_output_dir if tag_output_dir and tag_output_dir.exists() else (video_folder_path / f"{date_key}_{tag}")
    manifest_path = resolved_video_dir / "upload_manifest.json"
    
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_data = json.load(f)
            log(f"📋 已加载 upload_manifest.json (冻结标题模式)", "OK")
            log(f"   来源: {manifest_data.get('source', 'unknown')}")
            log(f"   频道数: {len(manifest_data.get('channels', {}))}")
        except Exception as e:
            log(f"⚠️ 读取 manifest 失败，将使用实时 metadata: {e}", "WARN")
            manifest_data = None
    else:
        log(f"ℹ️ 无 manifest 文件，使用实时 metadata 模式")

    plan_default_options = {}
    if isinstance(window_plan, dict):
        plan_default_options = window_plan.get("default_upload_options", {}) or {}
    manifest_channels = manifest_data.get("channels", {}) if isinstance(manifest_data, dict) and isinstance(manifest_data.get("channels"), dict) else {}
    if not manifest_channels and not dry_run:
        log("上传缺少有效的 upload_manifest.json，已停止执行", "ERR")
        return make_batch_result(0, 0, 1)
    
    # 获取项目文件夹
    # 解析元数据（仅在无 manifest 时使用）
    metadata = []
    if not manifest_data:
        if project_folder and project_folder.exists():
            metadata = parse_metadata(str(project_folder))
            log(f"解析到 {len(metadata)} 个频道的元数据")
        else:
            log("未找到项目元数据，将使用默认标题", "WARN")
    
    # 获取封面图（仅在无 manifest 时使用旧路径查找）
    all_thumbnails = []
    if not manifest_data and project_folder:
        all_thumbnails = find_thumbnails(str(project_folder), count=100)
        log(f"找到 {len(all_thumbnails)} 张封面图")
    
    if dry_run:
        log("\n=== 预览模式 (新匹配逻辑) ===\n")
        for serial in all_serials:
            plan_task = plan_task_by_serial.get(serial)
            raw_manifest = manifest_channels.get(str(serial)) if isinstance(manifest_channels.get(str(serial)), dict) else None
            container = _resolve_container_for_serial(
                serial=serial,
                tag=tag,
                plan_task=plan_task,
                manifest_channel=raw_manifest,
                live_container=live_container_by_serial.get(serial),
                mapping_registry=mapping_registry,
                serial_to_channel_name=serial_to_channel_name,
            )

            is_ypp = _task_is_ypp(
                serial=serial,
                plan_task=plan_task,
                manifest_channel=raw_manifest,
                live_container=container,
            )
            video = _resolve_video_for_serial(
                serial=serial,
                manifest_channel=raw_manifest,
                videos=videos,
                manifest_dir=resolved_video_dir,
            )

            if raw_manifest:
                ch_manifest = raw_manifest
                ch_manifest = merge_manifest_with_window_task(
                    ch_manifest,
                    plan_task,
                    default_upload_options=plan_default_options,
                )
                is_ypp = bool(ch_manifest.get("is_ypp", is_ypp))
                thumbnails = [Path(p) for p in ch_manifest.get("thumbnails", []) if Path(p).exists()]
                title = ch_manifest.get("title", "默认")
                channel_name = ch_manifest.get("channel_name") or container.get("name") or serial_to_channel_name.get(serial, "未知")
                current_set_num = ch_manifest.get("set")
            else:
                # 按 Container ID 匹配元数据
                channel_meta = None
                for meta in metadata:
                    if meta.get("container_id") == serial:
                        channel_meta = meta
                        break
                if not channel_meta and i < len(metadata):
                    channel_meta = metadata[i]

                # 按 Container ID 查找封面
                thumbnails = find_thumbnails(str(project_folder), count=1, container=serial) if project_folder else []
                current_set_num = None
                if thumbnails:
                    match = re.match(r'^(\d+)_(\d+)', thumbnails[0].stem)
                    if match:
                        current_set_num = int(match.group(2))

                # 获取对应标题
                title = "默认"
                if channel_meta:
                    titles_dict = channel_meta.get("titles", {})
                    if isinstance(titles_dict, dict) and current_set_num and current_set_num in titles_dict:
                        title = titles_dict[current_set_num]
                    elif isinstance(titles_dict, dict) and titles_dict:
                        title = list(titles_dict.values())[0]
                    elif isinstance(titles_dict, list) and titles_dict:
                        title = titles_dict[0]
                channel_name = (
                    (channel_meta.get("name") if channel_meta else "")
                    or container.get("name")
                    or "未知"
                )

            print(f"序号 {serial}: {channel_name}")
            print(f"  视频: {video.name if video else '无'}")
            print(f"  封面: {thumbnails[0].name if thumbnails else '无'} (套{current_set_num or '?'})")
            print(f"  标题: {title[:50]}...")
            print(f"  YPP: {'是' if is_ypp else '否'}")
            print()
        return make_batch_result(0, 0, 0)

    
    # ========== 交互式确认上传 ==========
    # 检查上传记录，让用户确认要上传哪些频道
    if not single_channel:
        upload_serials = interactive_upload_confirm(tag, date_key, all_serials, ypp_serials, auto_confirm=auto_confirm)
        if not upload_serials:
            log("没有需要上传的频道，退出", "INFO")
            return make_batch_result(0, 0, 0)
    else:
        upload_serials = [single_channel]
    

    # print()
    # confirm = input("按 Enter 开始上传 (Ctrl+C 退出)...")

    
    # 初始化成功计数器
    success_count = 0
    failed_serials = []
    pending_serials = []
    if active_success_windows is None:
        active_success_windows = []

    async def close_due_success_windows(force: bool = False) -> int:
        """关闭达到保留时长的成功窗口。"""
        if not active_success_windows:
            return 0

        now = datetime.now()
        kept = []
        closed = 0
        for item in active_success_windows:
            if force or now >= item["close_at"]:
                log(
                    f"窗口到期关闭: 序号 {item['serial']} (Container {item['container_code']})",
                    "ACT"
                )
                stop_browser(item["container_code"])
                closed += 1
            else:
                kept.append(item)
        active_success_windows[:] = kept
        return closed

    async def wait_for_window_slot():
        """
        成功窗口达到上限时等待，直到有窗口到期关闭。
        失败窗口不计入该上限。
        """
        if max_open_windows <= 0:
            return

        await close_due_success_windows()

        while len(active_success_windows) >= max_open_windows:
            now = datetime.now()
            next_close = min(w["close_at"] for w in active_success_windows)
            wait_seconds = max(5, int((next_close - now).total_seconds()))
            wait_step = min(wait_seconds, 60)
            log(
                f"成功窗口已达上限 {max_open_windows}，当前 {len(active_success_windows)} 个，等待 {wait_step}s...",
                "WAIT",
            )
            await asyncio.sleep(wait_step)
            await close_due_success_windows()

    # 开始上传
    for serial in all_serials:
        if serial not in upload_serials:
            continue

        plan_task = plan_task_by_serial.get(serial)
        raw_manifest = manifest_channels.get(str(serial)) if isinstance(manifest_channels.get(str(serial)), dict) else None
        container = _resolve_container_for_serial(
            serial=serial,
            tag=tag,
            plan_task=plan_task,
            manifest_channel=raw_manifest,
            live_container=live_container_by_serial.get(serial),
            mapping_registry=mapping_registry,
            serial_to_channel_name=serial_to_channel_name,
        )
        container_code = str(container.get("containerCode") or serial_to_container_code.get(serial) or "").strip()
        is_ypp = _task_is_ypp(
            serial=serial,
            plan_task=plan_task,
            manifest_channel=raw_manifest,
            live_container=container,
        )

        video = _resolve_video_for_serial(
            serial=serial,
            manifest_channel=raw_manifest,
            videos=videos,
            manifest_dir=resolved_video_dir,
        )
        if not video:
            log(f"序号 {serial}: 未找到匹配的视频文件", "WARN")
            continue
        if not container_code:
            log(f"序号 {serial}: 缺少 container_code，无法启动对应浏览器窗口", "ERR")
            failed_serials.append(serial)
            continue
        
        # ========== Manifest 模式: 直接读取冻结数据 ==========
        thumbnail_prompts = []
        if raw_manifest:
            ch_manifest = raw_manifest
            ch_manifest = merge_manifest_with_window_task(
                ch_manifest,
                plan_task,
                default_upload_options=plan_default_options,
            )
            is_ypp = bool(ch_manifest.get("is_ypp", is_ypp))
            title = ch_manifest.get("title", "Video Title")
            description = ch_manifest.get("description", "")
            tag_list = [str(item).strip() for item in ch_manifest.get("tag_list", []) if str(item).strip()]
            thumbnail_prompts = [str(item).strip() for item in ch_manifest.get("thumbnail_prompts", []) if str(item).strip()]
            upload_options = ch_manifest.get("upload_options", {}) if isinstance(ch_manifest.get("upload_options", {}), dict) else {}
            visibility = str(upload_options.get("visibility") or "public").strip().lower()
            scheduled_publish_at = str(upload_options.get("scheduled_publish_at") or "").strip() or None
            schedule_timezone = str(upload_options.get("schedule_timezone") or "").strip() or None
            made_for_kids = bool(upload_options.get("made_for_kids", False))
            altered_content = bool(upload_options.get("altered_content", True))
            category = str(upload_options.get("category") or "Music").strip() or "Music"
            # 【空标题阻断】标题为空时跳过，避免浪费浏览器资源
            if not title or title == "Video Title":
                log(f"序号 {serial}: ❌ 标题为空，跳过上传（请检查 generation_map.json 和 metadata_channels.md）", "ERR")
                failed_serials.append(serial)
                continue
            
            # 封面路径（绝对路径列表）
            thumbnails = [Path(p) for p in ch_manifest.get("thumbnails", []) if Path(p).exists()]
            
            # A/B 标题
            # 【防御性修复】manifest 的 ab_titles 可能只存了 2 个备选标题（不含主标题）
            # upload_single 要求 ab_test_titles 必须 >= 3 个才会启用 A/B Testing
            # 因此这里自动将主标题插入第一位补足
            ab_titles = ch_manifest.get("ab_titles", None) or None
            if ab_titles and not isinstance(ab_titles, list):
                ab_titles = None
            if ab_titles and len(ab_titles) < 3 and title:
                ab_titles = [title] + ab_titles
                log(f"序号 {serial}: ab_titles 不足3个，已将主标题插入第一位补足为 {len(ab_titles)} 个", "OK")
            
            # 如果 manifest 有标题但封面不全，尝试从旧路径补充
            if not thumbnails and project_folder:
                thumbnails = find_thumbnails(str(project_folder), count=1, container=serial)
                log(f"序号 {serial}: manifest 封面不存在，fallback 旧路径", "WARN")
            
            # 【A/B 测试前置条件检查日志】方便调试
            if is_ypp:
                ab_ready = bool(ab_titles and len(ab_titles) >= 3 and thumbnails and len(thumbnails) >= 3)
                log(f"序号 {serial} (manifest): 标题={title[:40]}... 封面={len(thumbnails)}张 AB标题={len(ab_titles) if ab_titles else 0}个 → A/B测试={'✅ 就绪' if ab_ready else '❌ 跳过'}", "OK")
                if not ab_ready:
                    reasons = []
                    if not thumbnails or len(thumbnails) < 3:
                        reasons.append(f"封面不足3张({len(thumbnails)}张)")
                    if not ab_titles or len(ab_titles) < 3:
                        reasons.append(f"AB标题不足3个({len(ab_titles) if ab_titles else 0}个)")
                    log(f"序号 {serial}: A/B测试跳过原因: {', '.join(reasons)}", "WARN")
            else:
                log(f"序号 {serial} (manifest): 标题={title[:40]}... 封面={len(thumbnails)}张", "OK")
        else:
            # 旧模式已废弃 - 必须有 manifest
            log(f"序号 {serial}: 未在 manifest 中找到数据，跳过", "ERR")
            failed_serials.append(serial)
            continue
        
        
        # 上传
        await wait_for_window_slot()
        playlist_name = get_playlist_name(tag)
        if thumbnails:
            thumb_preview = ", ".join(str(path) for path in thumbnails[:3])
            log(f"序号 {serial}: 本次上传缩略图 -> {thumb_preview}", "INFO")
        else:
            log(f"序号 {serial}: 本次未提供缩略图", "WARN")
        upload_video = video
        upload_thumbnails = thumbnails
        staged_dir: Optional[Path] = None
        if IS_MAC:
            try:
                upload_video, upload_thumbnails, staged_dir = stage_upload_assets(
                    video_path=video,
                    thumbnails=thumbnails,
                    serial=serial,
                )
                log(f"序号 {serial}: 上传素材已复制到临时目录 {staged_dir}", "INFO")
            except Exception as stage_exc:
                log(f"序号 {serial}: 临时上传素材准备失败，回退原始路径: {stage_exc}", "WARN")
                upload_video = video
                upload_thumbnails = thumbnails
                staged_dir = None
        upload_result = make_upload_result(False, True, "未开始上传", "not_started")
        tail_watcher_started = False
        try:
            upload_result = await upload_single_with_browser_recovery(
                container_code=container_code,
                serial=serial,
                video_path=upload_video,
                thumbnails=upload_thumbnails,
                title=title,
                description=description,
                is_ypp=is_ypp,
                ab_test_titles=ab_titles,
                playlist_name=playlist_name,
                tags=tag_list,
                visibility=visibility,
                scheduled_publish_at=scheduled_publish_at,
                schedule_timezone=schedule_timezone,
                made_for_kids=made_for_kids,
                altered_content=altered_content,
                notify_subscribers=bool(upload_options.get("notify_subscribers", False)),
                category=category,
            )
        except Exception as e:
            log(f"序号 {serial} 上传异常: {e}", "ERR")
            upload_result = make_upload_result(False, True, f"外层捕获异常: {e}", "batch_upload_exception")
        finally:
            success = bool(upload_result.get("success"))
            close_browser_now = bool(upload_result.get("close_browser", True))
            stage = str(upload_result.get("stage") or "").strip()

            # 可选：每个频道处理后关闭容器浏览器
            if auto_close_browser:
                if success and close_browser_now:
                    stop_browser(container_code)
                elif not success and stage == "publish_pending_monitor" and not close_browser_now:
                    if upload_result.get("stage") == "publish_pending_monitor":
                        tail_watcher_started = launch_tail_close_watcher(
                            serial=serial,
                            container_code=container_code,
                            debug_port=upload_result.get("debug_port"),
                            tag=tag,
                            date_mmdd=date_key,
                        )
                    if tail_watcher_started:
                        log(
                            f"序号 {serial}: 主监控超时，已转交尾程 watcher 继续盯到可安全关闭",
                            "WARN",
                        )
                    else:
                        log(
                            f"序号 {serial}: 监控尚未确认可关闭浏览器，已保留容器现场继续观察",
                            "WARN",
                        )
                elif not success:
                    log(
                        f"序号 {serial}: 上传失败(stage={stage or 'unknown'})，保留浏览器窗口供人工检查",
                        "WARN",
                    )
                if staged_dir and success:
                    try:
                        shutil.rmtree(staged_dir, ignore_errors=True)
                        log(f"序号 {serial}: 已清理临时上传素材 {staged_dir}", "INFO")
                    except Exception as cleanup_exc:
                        log(f"序号 {serial}: 清理临时上传素材失败: {cleanup_exc}", "WARN")

        success = bool(upload_result.get("success"))
        close_browser_now = bool(upload_result.get("close_browser", True))
        stage = str(upload_result.get("stage") or "").strip()
        pending_publish = (not success) and stage == "publish_pending_monitor" and not close_browser_now

        if success:
            success_count += 1
            if not auto_close_browser:
                close_at = datetime.now() + timedelta(hours=window_ttl_hours)
                active_success_windows.append({
                    "serial": serial,
                    "container_code": container_code,
                    "close_at": close_at,
                })
                log(
                    f"序号 {serial}: 发布成功，窗口将于 {close_at.strftime('%H:%M:%S')} 自动关闭 "
                    f"(保留窗口 {len(active_success_windows)}/{max_open_windows})",
                    "OK",
                )
            
            # 封面归档已取消 - base image 按日期命名不会重复使用
            
            # === 删除视频文件：上传成功后释放磁盘空间 ===
            try:
                archive_uploaded_metadata(
                    tag=tag,
                    serial=serial,
                    date_mmdd=date_key,
                    title=title,
                    description=description,
                    tag_list=tag_list,
                    thumbnail_prompts=thumbnail_prompts,
                    thumbnails=thumbnails,
                    config=config,
                    move_files=True,
                    log=lambda message: log(message, "INFO"),
                )
            except Exception as e:
                log(f"Metadata archive failed (non-fatal): {e}", "WARN")

            try:
                if video.exists():
                    if retain_video_days > 0:
                        log(
                            f"🗂️ 已保留视频文件: {video.name}，将在后续运行时按 {retain_video_days} 天规则清理",
                            "OK",
                        )
                    else:
                        video_size_mb = video.stat().st_size / 1024 / 1024
                        video.unlink()
                        log(f"🗑️ 已删除视频文件: {video.name} ({video_size_mb:.0f} MB)", "OK")
            except Exception as e:
                log(f"删除视频文件失败 (非致命): {e}", "WARN")
        
        # 保存详细上传记录
        # 优先从 channels.md 获取频道名，其次从 API
        channel_name = str(
            (ch_manifest.get("channel_name") if isinstance(ch_manifest, dict) else "")
            or container.get("name")
            or serial_to_channel_name.get(serial)
            or f"频道{serial}"
        ).strip()
        save_upload_record(
            tag=tag,
            date=date_key,
            serial=serial,
            channel_name=channel_name,
            video_path=video,
            thumbnails=thumbnails,
            title=title,
            description=description,
            is_ypp=is_ypp,
            ab_test_titles=ab_titles,
            success=success
        )
        
        if not success:
            if tail_watcher_started:
                log(
                    f"序号 {serial} 已点击 Publish，主监控超时；尾程 watcher 已接手后续自动关窗",
                    "WARN",
                )
                pending_serials.append(serial)
            elif pending_publish:
                log(
                    f"序号 {serial} 尚未确认最终安全关闭，浏览器现场已保留继续观察",
                    "WARN",
                )
                pending_serials.append(serial)
            else:
                log(
                    f"序号 {serial} 上传失败(stage={stage or 'unknown'})，浏览器已保留待人工接管",
                    "WARN",
                )
                failed_serials.append(serial)

    # 批次结束时顺带清理一次已到期窗口，并提示剩余窗口
    await close_due_success_windows()
    if active_success_windows:
        nearest = min(active_success_windows, key=lambda x: x["close_at"])["close_at"]
        log(
            f"当前仍有 {len(active_success_windows)} 个成功窗口保留中，最近关闭时间: {nearest.strftime('%H:%M:%S')}",
            "INFO",
        )
    
    # === 生成 upload_report.json (供 daily_scheduler 清理用) ===
    try:
        video_folder_path = Path(config["video_folder"])
        report_dir = tag_output_dir if tag_output_dir and tag_output_dir.exists() else (video_folder_path / f"{date_key}_{tag}")
        if report_dir.exists():
            report_path = report_dir / "upload_report.json"
            report = {
                "date": date_key,
                "tag": tag,
                "completed_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "total": len(upload_serials),
                "success": success_count,
                "failed": len(failed_serials),
                "pending": len(pending_serials),
            }
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            log(f"📋 upload_report.json 已写入: {report_path}", "OK")
    except Exception as e:
        log(f"⚠️ 写入 upload_report 失败: {e}", "WARN")
    
    # === 新增：记录上传日志 ===
    try:
        history_status = "success"
        if failed_serials and pending_serials:
            history_status = "partial"
        elif failed_serials:
            history_status = "failed"
        elif pending_serials:
            history_status = "pending"

        new_record = {
            "date": date_key,
            "tag": tag,
            "count": success_count,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": history_status,
            "failed_count": len(failed_serials),
            "pending_count": len(pending_serials),
        }

        if append_upload_history(UPLOAD_HISTORY_PATH, new_record):
            log(f"✅ 上传记录已保存到: {UPLOAD_HISTORY_PATH.name}", "OK")

    except Exception as e:
        log(f"⚠️ 保存日志失败: {e}", "WARN")

    return make_batch_result(
        len(upload_serials),
        success_count,
        len(failed_serials),
        len(pending_serials),
    )

# ============ 主函数 ============
def parse_arguments():
    import argparse
    parser = argparse.ArgumentParser(description="YouTube 批量上传脚本")
    parser.add_argument("--tag", required=False, help="标签组名称 (如: 大提琴)")
    parser.add_argument("--date", required=False, help="视频日期 (如: 1.28)")
    parser.add_argument("--channel", type=int, help="单独上传指定频道序号 (如: 114)")
    parser.add_argument("--dry-run", action="store_true", help="预览模式，不实际执行")
    parser.add_argument("--auto", action="store_true", help="自动扫描 AutoTask 文件夹")
    parser.add_argument("--auto-confirm", action="store_true", help="免交互自动确认")
    parser.add_argument("--auto-close-browser", action="store_true", help="每个频道上传后自动关闭容器浏览器")
    parser.add_argument("--skip-channels", type=str, default="", help="跳过指定频道序号，逗号分隔 (如: 17,26,30)")
    parser.add_argument("--window-plan-file", type=str, default="", help="窗口任务计划 JSON 文件路径")
    parser.add_argument("--max-open-windows", type=int, default=10, help="成功发布后保留的最大窗口数 (默认: 10)")
    parser.add_argument("--window-ttl-hours", type=float, default=2.0, help="成功发布后窗口保留时长(小时) (默认: 2)")
    parser.add_argument("--retain-video-days", type=int, default=0, help="上传成功后保留视频文件的天数；0 表示立即删除")
    parser.add_argument("--pair-size", type=int, default=2, help="每组配令人数 (默认: 2)")
    parser.add_argument("--wait-hours", type=float, default=2.0, help="每组之间的等待时间(小时) (默认: 2.0)")
    return parser.parse_args()

def _run_auto_mode(args, config, window_plan=None):
    print("🔍 自动扫描 AutoTask 文件夹...")
    
    grouped = auto_detect_videos(config)
    
    if not grouped:
        print(f"\n❌ AutoTask 文件夹中没有找到待上传的视频")
        print(f"📂 扫描路径: {config['video_folder']}")
        return 0
    
    if args.auto_confirm:
        selected = []
        for date, tags in grouped.items():
            for tag in tags.keys():
                selected.append((date, tag))
        print(f"🤖 [自动模式] 默认选择所有 {len(selected)} 个批次")
    else:
        selected = show_available_uploads(grouped, config)
    
    if not selected:
        print("❌ 未选择任何批次，退出")
        return 0
    
    # 随机打乱标签顺序
    import random
    random.shuffle(selected)
    
    # ========== 方案A: 标签两两配对 + 2小时间隔 ==========
    pair_size = args.pair_size
    wait_seconds = int(args.wait_hours * 3600)
    
    groups = [selected[i:i+pair_size] for i in range(0, len(selected), pair_size)]
    total_groups = len(groups)
    
    print("\n" + "=" * 60)
    print(f"🎬 方案A: 标签配对上传 ({total_groups} 组, 每组{pair_size}个标签, 间隔{args.wait_hours}小时)")
    for gi, group in enumerate(groups, 1):
        tags_str = " + ".join([f"[{d}]{t}" for d, t in group])
        print(f"  第{gi}组: {tags_str}")
    print("=" * 60 + "\n")
    
    if not args.auto_confirm:
        input("按 Enter 开始执行 (Ctrl+C 退出)...")
    
    active_success_windows = []
    had_failures = False

    for gi, group in enumerate(groups):
        is_last_group = (gi == total_groups - 1)
        
        print(f"\n{'='*60}")
        print(f"🚀 第 {gi+1}/{total_groups} 组开始")
        print(f"{'='*60}")
        
        group_start = time.time()
        
        # 每个标签独立 asyncio.run()
        for date, tag in group:
            print(f"\n🚀 开始执行: [{date}] {tag}")
            try:
                result = asyncio.run(
                    batch_upload(
                        tag,
                        date,
                        args.dry_run,
                        auto_confirm=args.auto_confirm,
                        auto_close_browser=args.auto_close_browser,
                        skip_channels=parse_serial_list(args.skip_channels),
                        max_open_windows=args.max_open_windows,
                        window_ttl_hours=args.window_ttl_hours,
                        active_success_windows=active_success_windows,
                        window_plan=window_plan,
                        retain_video_days=args.retain_video_days,
                    )
                )
                if result and result.get("failed_count", 0) > 0:
                    had_failures = True
                print(f"✅ [{date}] {tag} 完成")
            except Exception as e:
                had_failures = True
                print(f"❌ [{date}] {tag} 执行异常: {e}")
        
        # 组间等待
        if not is_last_group:
            elapsed = time.time() - group_start
            remaining = max(0, wait_seconds - elapsed)
            if remaining > 0:
                resume_time = datetime.fromtimestamp(time.time() + remaining)
                print(f"\n⏳ 第{gi+1}组完成，等待 {remaining/60:.0f} 分钟后开始第{gi+2}组...")
                print(f"   预计恢复时间: {resume_time.strftime('%H:%M:%S')}")
                print(f"   (按 Ctrl+C 可跳过等待)")
                try:
                    time.sleep(remaining)
                except KeyboardInterrupt:
                    print("\n⚡ 跳过等待，立即开始下一组")
    
    print(f"\n{'='*60}")
    print(f"🎉 所有 {total_groups} 组任务完成!")
    print(f"{'='*60}")
    return 1 if had_failures else 0

def _run_traditional_mode(args, config, window_plan=None):
    tags = []
    interactive_session = bool(sys.stdin.isatty() and sys.stdout.isatty())
    
    if args.channel and not args.tag:
        found_tag = None
        for tag_name, tag_cfg in config.get("tag_to_project", {}).items():
            ypp = tag_cfg.get("ypp_serials", [])
            non_ypp = tag_cfg.get("non_ypp_serials", [])
            if args.channel in ypp or args.channel in non_ypp:
                found_tag = tag_name
                break
        if found_tag:
            print(f"🎯 检测到频道 {args.channel} 属于标签: [{found_tag}]")
            tags = [found_tag]

    if not tags and not args.tag:
        from utils import interactive_select_tags
        tags = interactive_select_tags()
        if not tags:
            print("❌ 未选择任何标签，退出")
            return 0
    elif args.tag:
        tags = [t.strip() for t in re.split(r"[，,]", args.tag) if t.strip()]
        
    date_str = args.date
    if not date_str:
        today = datetime.now().strftime("%-m.%d")
        default_date = today
        if not args.auto_confirm and interactive_session:
            date_input = input(f"请输入视频日期 (默认为 {default_date}): ").strip()
            date_str = date_input if date_input else default_date
        else:
            date_str = default_date
    
    print("\n" + "=" * 60)
    print(f"🎬 准备开始执行任务")
    print(f"📅 日期: {date_str}")
    print(f"🏷️  标签: {', '.join(tags)}")
    if args.channel:
        print(f"📺 单个频道: {args.channel}")
    print("=" * 60 + "\n")
    
    if not args.auto_confirm and not args.dry_run and interactive_session:
        input("按 Enter 开始执行 (Ctrl+C 退出)...")
    elif not interactive_session:
        print("ℹ️ 非交互终端，跳过开始前确认")
    
    active_success_windows = []
    had_failures = False

    for tag in tags:
        print(f"\n🚀 开始执行标签组: {tag}")
        result = asyncio.run(
            batch_upload(
                tag,
                date_str,
                args.dry_run,
                single_channel=args.channel,
                auto_confirm=args.auto_confirm,
                auto_close_browser=args.auto_close_browser,
                skip_channels=parse_serial_list(args.skip_channels),
                max_open_windows=args.max_open_windows,
                window_ttl_hours=args.window_ttl_hours,
                active_success_windows=active_success_windows,
                window_plan=window_plan,
                retain_video_days=args.retain_video_days,
            )
        )
        if result and result.get("failed_count", 0) > 0:
            had_failures = True
        print(f"✅ 标签组 {tag} 完成\n")
    
    print("\n✅ 所有任务完成")
    return 1 if had_failures else 0

async def set_video_category_music(page, max_attempts: int = 6) -> bool:
    async def _read_state() -> Dict[str, Any]:
        return await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                const categoryRe = /category|分類|分类/i;
                const excludeRe = /recording\\s*date|錄製日期|录制日期/i;
                const musicRe = /(^|\\s)music(\\s|$)|音樂|音乐/i;
                const roots = [document];
                const seen = new Set([document]);

                while (roots.length) {
                    const root = roots.shift();
                    const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                    for (const node of allNodes) {
                        if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                            seen.add(node.shadowRoot);
                            roots.push(node.shadowRoot);
                        }
                    }
                    const fields = Array.from(
                        root.querySelectorAll ? root.querySelectorAll("ytcp-form-select, tp-yt-paper-dropdown-menu") : []
                    ).filter(visible);
                    for (const field of fields) {
                        const labelText = [
                            textOf(field.querySelector?.("label")),
                            textOf(field.querySelector?.("tp-yt-paper-input-label")),
                            textOf(field.querySelector?.("[slot='label']")),
                            textOf(field.querySelector?.("yt-formatted-string#label")),
                        ].join(" ").trim();
                        if (!categoryRe.test(labelText) || excludeRe.test(labelText)) continue;
                        const selectedText =
                            textOf(field.querySelector?.("#label")) ||
                            textOf(field.querySelector?.("ytcp-dropdown-trigger")) ||
                            textOf(field.querySelector?.("[aria-haspopup='listbox']")) ||
                            "";
                        return {
                            found: true,
                            selected: musicRe.test(selectedText.toLowerCase()) || musicRe.test(selectedText),
                            value: selectedText,
                        };
                    }
                }
                return { found: false, selected: false, value: "" };
            }
            """
        )

    async def _open_dropdown() -> bool:
        return bool(
            await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                    const categoryRe = /category|分類|分类/i;
                    const excludeRe = /recording\\s*date|錄製日期|录制日期/i;
                    const roots = [document];
                    const seen = new Set([document]);
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement)) return false;
                        try { el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" }); } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };

                    while (roots.length) {
                        const root = roots.shift();
                        const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                        for (const node of allNodes) {
                            if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                                seen.add(node.shadowRoot);
                                roots.push(node.shadowRoot);
                            }
                        }
                        const fields = Array.from(
                            root.querySelectorAll ? root.querySelectorAll("ytcp-form-select, tp-yt-paper-dropdown-menu") : []
                        ).filter(visible);
                        for (const field of fields) {
                            const labelText = [
                                textOf(field.querySelector?.("label")),
                                textOf(field.querySelector?.("tp-yt-paper-input-label")),
                                textOf(field.querySelector?.("[slot='label']")),
                                textOf(field.querySelector?.("yt-formatted-string#label")),
                            ].join(" ").trim();
                            if (!categoryRe.test(labelText) || excludeRe.test(labelText)) continue;
                            const trigger =
                                field.querySelector?.("ytcp-dropdown-trigger") ||
                                field.querySelector?.("[aria-haspopup='listbox']") ||
                                field.querySelector?.("[role='button']") ||
                                field;
                            if (clickEl(trigger)) return true;
                        }
                    }
                    return false;
                }
                """
            )
        )

    async def _pick_music_option() -> bool:
        selectors = [
            "tp-yt-paper-item:has-text('Music')",
            "[role='option']:has-text('Music')",
            "ytcp-menu-service-item-renderer:has-text('Music')",
            "tp-yt-paper-item:has-text('音樂')",
            "[role='option']:has-text('音樂')",
            "ytcp-menu-service-item-renderer:has-text('音樂')",
            "tp-yt-paper-item:has-text('音乐')",
            "[role='option']:has-text('音乐')",
            "ytcp-menu-service-item-renderer:has-text('音乐')",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).last
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                if await human_click(page, locator, f"Category option ({selector})"):
                    return True
                await locator.click(force=True, timeout=3000)
                return True
            except Exception:
                continue
        return False

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"category-final-{attempt}")
        try:
            await page.mouse.wheel(0, 550)
        except Exception:
            pass
        state = await _read_state()
        log(
            f"Category final check#{attempt}: found={state.get('found')} selected={state.get('selected')} value={state.get('value', '')}",
            "INFO",
        )
        if state.get("selected"):
            log(f"Category confirmed as Music ({state.get('value', '')})", "OK")
            return True
        opened = await _open_dropdown()
        log(f"Category final open#{attempt}: opened={opened}", "INFO")
        if not opened:
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(1.0)
        picked = await _pick_music_option()
        log(f"Category final pick#{attempt}: picked={picked}", "INFO")
        if not picked:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(1.0)
        verify = await _read_state()
        log(
            f"Category final verify#{attempt}: found={verify.get('found')} selected={verify.get('selected')} value={verify.get('value', '')}",
            "INFO",
        )
        if verify.get("selected"):
            log(f"Category set to Music ({verify.get('value', '')})", "OK")
            return True
    log("Category still failed to become Music", "WARN")
    return False


async def set_video_category(page, category: str) -> bool:
    target = str(category or "").strip()
    if not target:
        return True
    if target.lower() == "music":
        return await set_video_category_music(page)
    return False


async def set_notify_subscribers(page, enabled: bool) -> bool:
    target_text = "Publish to subscriptions feed and notify subscribers"
    try:
        result = await page.evaluate(
            """
            ([targetText, enabled]) => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0 ||
                    el.getClientRects().length > 0
                );
                const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                const roots = [document];
                const seen = new Set([document]);

                const isChecked = (node) => {
                    const value = (
                        node?.getAttribute?.("aria-checked") ||
                        node?.getAttribute?.("checked") ||
                        node?.querySelector?.("[aria-checked]")?.getAttribute?.("aria-checked") ||
                        ""
                    ).toString().toLowerCase();
                    return value === "true";
                };

                const clickEl = (el) => {
                    if (!(el instanceof HTMLElement) || !visible(el)) return false;
                    try { el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" }); } catch (_) {}
                    try { el.click(); } catch (_) {}
                    for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                        try {
                            el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                        } catch (_) {}
                    }
                    return true;
                };

                while (roots.length) {
                    const root = roots.shift();
                    const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                    for (const node of allNodes) {
                        if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                            seen.add(node.shadowRoot);
                            roots.push(node.shadowRoot);
                        }
                    }

                    const candidates = Array.from(
                        root.querySelectorAll
                            ? root.querySelectorAll("ytcp-checkbox-lit, tp-yt-paper-checkbox, [role='checkbox']")
                            : []
                    );
                    for (const candidate of candidates) {
                        const container = candidate.closest?.("label, ytcp-checkbox-lit, tp-yt-paper-checkbox, div, span") || candidate;
                        const text = textOf(container);
                        if (!text.includes(targetText)) continue;
                        const checked = isChecked(candidate) || isChecked(container);
                        if (checked === !!enabled) {
                            return { found: true, changed: false, checked };
                        }
                        const target =
                            candidate.querySelector?.("#checkbox-container") ||
                            candidate.querySelector?.("[role='checkbox']") ||
                            candidate;
                        const clicked = clickEl(target);
                        return { found: true, changed: clicked, checked: !!enabled };
                    }
                }
                return { found: false, changed: false, checked: false };
            }
            """,
            [target_text, bool(enabled)],
        )
        if result.get("found"):
            log(f"订阅通知已设置为 {'开启' if enabled else '关闭'}", "OK")
            return True
    except Exception as exc:
        log(f"设置订阅通知失败: {exc}", "WARN")
    return False


async def read_advanced_settings_state(page) -> Dict[str, Any]:
    collapsed_markers = [
        "展开",
        "显示高级设置",
        "show more",
        "show advanced settings",
    ]
    expanded_markers = [
        "收起",
        "隐藏高级设置",
        "show less",
        "hide advanced settings",
    ]
    state: Dict[str, Any] = {
        "category_found": False,
        "category_visible": False,
        "toggle_found": False,
        "toggle_text": "",
        "toggle_label": "",
        "expanded": False,
        "collapsed": False,
        "ready": False,
    }
    for selector in ("#category-container", "ytcp-form-select#category", "#category"):
        locator = page.locator(selector).first
        try:
            if await locator.count() == 0:
                continue
            state["category_found"] = True
            if await locator.is_visible():
                state["category_visible"] = True
                break
        except Exception:
            continue

    toggle = page.locator("ytcp-button#toggle-button").first
    try:
        if await toggle.count() > 0:
            state["toggle_found"] = True
            try:
                state["toggle_text"] = (await toggle.inner_text(timeout=1500)).strip()
            except Exception:
                state["toggle_text"] = ""
            for attr_name in ("aria-label", "label", "title"):
                try:
                    attr_value = (await toggle.get_attribute(attr_name)) or ""
                except Exception:
                    attr_value = ""
                if attr_value.strip():
                    state["toggle_label"] = attr_value.strip()
                    break
            combined = f"{state['toggle_text']} {state['toggle_label']}".strip().lower()
            state["expanded"] = any(marker in combined for marker in expanded_markers)
            state["collapsed"] = any(marker in combined for marker in collapsed_markers)
    except Exception:
        pass

    state["ready"] = bool(state["category_visible"] or state["expanded"])
    return state


async def ensure_advanced_settings_open(page, max_attempts: int = 5) -> bool:
    async def _click_toggle_with_dom_fallback() -> bool:
        toggle = page.locator("ytcp-button#toggle-button").first
        try:
            if await toggle.count() == 0:
                return False
        except Exception:
            return False

        click_targets = [
            toggle.locator("button").first,
            toggle.locator("[role='button']").first,
            toggle,
        ]
        for target in click_targets:
            try:
                if await target.count() == 0 or not await target.is_visible():
                    continue
                try:
                    await target.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                clicked = await human_click(page, target, "高级设置切换")
                if clicked:
                    return True
                await target.click(force=True, timeout=3000)
                return True
            except Exception:
                continue

        try:
            clicked = await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement) || !visible(el)) return false;
                        try { el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" }); } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };
                    const host = document.querySelector("ytcp-button#toggle-button");
                    if (!host) return false;
                    const targets = [
                        host.querySelector?.("button"),
                        host.querySelector?.("[role='button']"),
                        host,
                    ].filter(Boolean);
                    for (const target of targets) {
                        if (clickEl(target)) return true;
                    }
                    return false;
                }
                """
            )
            return bool(clicked)
        except Exception:
            return False

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"advanced-settings-{attempt}")
        state = await read_advanced_settings_state(page)
        log(
            "Advanced settings "
            f"check#{attempt}: ready={state.get('ready')} category_visible={state.get('category_visible')} "
            f"toggle_found={state.get('toggle_found')} expanded={state.get('expanded')} "
            f"text={state.get('toggle_text', '')} label={state.get('toggle_label', '')}",
            "INFO",
        )
        if state.get("ready"):
            return True
        if not state.get("toggle_found"):
            await asyncio.sleep(0.8)
            continue

        clicked = await _click_toggle_with_dom_fallback()
        log(f"Advanced settings click#{attempt}: clicked={clicked}", "INFO")
        if not clicked:
            try:
                toggle = page.locator("ytcp-button#toggle-button").first
                await toggle.focus()
                await page.keyboard.press("Enter")
            except Exception:
                pass
        await asyncio.sleep(1.0)
        verify = await read_advanced_settings_state(page)
        log(
            "Advanced settings "
            f"verify#{attempt}: ready={verify.get('ready')} category_visible={verify.get('category_visible')} "
            f"expanded={verify.get('expanded')} text={verify.get('toggle_text', '')} "
            f"label={verify.get('toggle_label', '')}",
            "INFO",
        )
        if verify.get("ready"):
            return True

    log("未能确认高级设置已展开", "WARN")
    return False


async def set_video_category_music(page, max_attempts: int = 6) -> bool:
    music_markers = ("music", "音樂", "音乐")
    advanced_ready = await ensure_advanced_settings_open(page)
    if not advanced_ready:
        log("Category 设置前未能确认高级设置已展开，继续尝试直接定位分类控件", "WARN")

    async def _read_state() -> Dict[str, Any]:
        texts: list[str] = []
        found = False
        for selector in (
            "#category-container",
            "ytcp-form-select#category",
            "#category",
        ):
            locator = page.locator(selector).first
            try:
                if await locator.count() == 0:
                    continue
                found = True
                raw = (await locator.inner_text(timeout=2000)).strip()
                if raw:
                    texts.append(raw)
            except Exception:
                continue
        combined = " | ".join(texts)
        lowered = combined.lower()
        return {
            "found": found,
            "selected": any(marker in lowered for marker in music_markers),
            "value": combined,
        }

    async def _options_visible() -> bool:
        try:
            return bool(
                await page.evaluate(
                    """
                    () => {
                        const visible = (el) => !!el && (
                            el.offsetParent !== null ||
                            el.offsetWidth > 0 ||
                            el.offsetHeight > 0 ||
                            el.getClientRects().length > 0
                        );
                        const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                        const optionRe = /(^|\\s)music(\\s|$)|音樂|音乐|people\\s*&\\s*blogs|news\\s*&\\s*politics|教育|娛樂|娱乐|新聞|新闻/i;
                        const roots = [document];
                        const seen = new Set([document]);
                        while (roots.length) {
                            const root = roots.shift();
                            const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                            for (const node of allNodes) {
                                if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                                    seen.add(node.shadowRoot);
                                    roots.push(node.shadowRoot);
                                }
                            }
                            const items = Array.from(
                                root.querySelectorAll
                                    ? root.querySelectorAll("tp-yt-paper-item, [role='option'], [role='menuitem'], ytcp-menu-service-item-renderer")
                                    : []
                            );
                            for (const item of items) {
                                if (!visible(item)) continue;
                                if (optionRe.test(textOf(item))) return true;
                            }
                        }
                        return false;
                    }
                    """
                )
            )
        except Exception:
            return False

    async def _open_dropdown() -> bool:
        try:
            opened = await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const roots = [document];
                    const seen = new Set([document]);
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement) || !visible(el)) return false;
                        try { el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" }); } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };

                    while (roots.length) {
                        const root = roots.shift();
                        const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                        for (const node of allNodes) {
                            if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                                seen.add(node.shadowRoot);
                                roots.push(node.shadowRoot);
                            }
                        }

                        const field = root.querySelector?.("ytcp-form-select#category");
                        if (field && visible(field)) {
                            const triggers = [
                                field.querySelector?.("[aria-haspopup='listbox']"),
                                field.querySelector?.("[role='button']"),
                                field.querySelector?.("#trigger"),
                                field,
                            ].filter(Boolean);
                            for (const trigger of triggers) {
                                if (clickEl(trigger)) return true;
                            }
                        }

                        const containers = Array.from(
                            root.querySelectorAll ? root.querySelectorAll("#category-container, #category") : []
                        );
                        for (const container of containers) {
                            if (!visible(container)) continue;
                            const trigger =
                                container.querySelector?.("[aria-haspopup='listbox']") ||
                                container.querySelector?.("[role='button']") ||
                                container;
                            if (clickEl(trigger)) return true;
                        }
                    }
                    return false;
                }
                """
            )
            if opened:
                await asyncio.sleep(0.8)
                if await _options_visible():
                    return True
        except Exception:
            pass

        for selector in (
            "ytcp-form-select#category",
            "#category",
            "#category-container",
            "#category-container [aria-haspopup='listbox']",
            "#category-container [role='button']",
        ):
            locator = page.locator(selector).first
            try:
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                try:
                    await locator.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                if await human_click(page, locator, f"Category trigger ({selector})"):
                    await asyncio.sleep(0.8)
                    if await _options_visible():
                        return True
                await locator.click(force=True, timeout=3000)
                await asyncio.sleep(0.8)
                if await _options_visible():
                    return True
            except Exception:
                continue
        return False

    async def _pick_music_option() -> bool:
        try:
            picked = await page.evaluate(
                """
                () => {
                    const visible = (el) => !!el && (
                        el.offsetParent !== null ||
                        el.offsetWidth > 0 ||
                        el.offsetHeight > 0 ||
                        el.getClientRects().length > 0
                    );
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || "").trim();
                    const musicRe = /(^|\\s)music(\\s|$)|音樂|音乐/i;
                    const roots = [document];
                    const seen = new Set([document]);
                    const clickEl = (el) => {
                        if (!(el instanceof HTMLElement) || !visible(el)) return false;
                        try { el.scrollIntoView({ block: "center", inline: "center", behavior: "instant" }); } catch (_) {}
                        try { el.click(); } catch (_) {}
                        for (const type of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
                            try {
                                el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true }));
                            } catch (_) {}
                        }
                        return true;
                    };

                    while (roots.length) {
                        const root = roots.shift();
                        const allNodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
                        for (const node of allNodes) {
                            if (node && node.shadowRoot && !seen.has(node.shadowRoot)) {
                                seen.add(node.shadowRoot);
                                roots.push(node.shadowRoot);
                            }
                        }

                        const items = Array.from(
                            root.querySelectorAll
                                ? root.querySelectorAll("tp-yt-paper-item, [role='option'], [role='menuitem'], ytcp-menu-service-item-renderer")
                                : []
                        );
                        for (const item of items) {
                            if (!musicRe.test(textOf(item))) continue;
                            const target =
                                item.querySelector?.("[role='option']") ||
                                item.querySelector?.("[role='menuitem']") ||
                                item;
                            if (clickEl(target)) return true;
                        }
                    }
                    return false;
                }
                """
            )
            if picked:
                return True
        except Exception:
            pass

        for selector in (
            "tp-yt-paper-item:has-text('Music')",
            "[role='option']:has-text('Music')",
            "[role='menuitem']:has-text('Music')",
            "ytcp-menu-service-item-renderer:has-text('Music')",
            "tp-yt-paper-item:has-text('音樂')",
            "[role='option']:has-text('音樂')",
            "tp-yt-paper-item:has-text('音乐')",
            "[role='option']:has-text('音乐')",
        ):
            locator = page.locator(selector).first
            try:
                if await locator.count() == 0 or not await locator.is_visible():
                    continue
                if await human_click(page, locator, f"Category option ({selector})"):
                    return True
                await locator.click(force=True, timeout=3000)
                return True
            except Exception:
                continue
        return False

    for attempt in range(1, max_attempts + 1):
        await clear_blocking_overlays(page, f"category-final-{attempt}")
        try:
            await page.mouse.wheel(0, 550)
        except Exception:
            pass
        state = await _read_state()
        log(
            f"Category final check#{attempt}: found={state.get('found')} selected={state.get('selected')} value={state.get('value', '')}",
            "INFO",
        )
        if state.get("selected"):
            log(f"Category confirmed as Music ({state.get('value', '')})", "OK")
            return True
        opened = await _open_dropdown()
        log(f"Category final open#{attempt}: opened={opened}", "INFO")
        if not opened:
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(0.8)
        picked = await _pick_music_option()
        log(f"Category final pick#{attempt}: picked={picked}", "INFO")
        if not picked:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            await asyncio.sleep(0.8)
            continue
        await asyncio.sleep(1.0)
        verify = await _read_state()
        log(
            f"Category final verify#{attempt}: found={verify.get('found')} selected={verify.get('selected')} value={verify.get('value', '')}",
            "INFO",
        )
        if verify.get("selected"):
            log(f"Category set to Music ({verify.get('value', '')})", "OK")
            return True
    log("Category still failed to become Music", "WARN")
    return False


async def set_video_category(page, category: str) -> bool:
    target = str(category or "").strip()
    if not target:
        return True
    if target.lower() == "music":
        return await set_video_category_music(page)
    return False


def load_channel_mapping_registry() -> Dict[int, Dict[str, Any]]:
    registry: Dict[int, Dict[str, Any]] = {}
    if not CHANNEL_MAPPING_PATH.exists():
        return registry
    try:
        with open(CHANNEL_MAPPING_PATH, "r", encoding="utf-8") as f:
            mapping_data = json.load(f)
    except Exception as e:
        log(f"璇诲彇 channel_mapping.json 澶辫触: {e}", "WARN")
        return registry

    for container_code, info in (mapping_data.get("channels", {}) or {}).items():
        try:
            serial = int(info.get("serial_number") or 0)
        except (TypeError, ValueError):
            continue
        if not serial:
            continue
        tag_name = str(info.get("tag") or "").strip()
        registry[serial] = {
            "serialNumber": serial,
            "containerCode": str(container_code),
            "name": str(info.get("channel_name") or ""),
            "tag": tag_name,
            "tagName": tag_name,
            "remark": "",
        }
    return registry


def extract_window_plan_serials(tag: str) -> List[int]:
    plan = globals().get("ACTIVE_WINDOW_PLAN")
    if not isinstance(plan, dict):
        return []
    wanted = _normalize_tag_for_match(tag)
    serials: List[int] = []
    for task in plan.get("tasks", []) or []:
        if _normalize_tag_for_match(task.get("tag", "")) != wanted:
            continue
        try:
            serial = int(task.get("serial") or 0)
        except (TypeError, ValueError):
            continue
        if serial:
            serials.append(serial)
    return sorted(set(serials))


def get_all_containers() -> List[Dict]:
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            containers = list_browser_envs(CONFIG_PATH)
            normalized: List[Dict[str, Any]] = []
            for container in containers:
                item = dict(container)
                if "tagName" not in item:
                    item["tagName"] = item.get("tag", "")
                normalized.append(item)
            return normalized
        except Exception as e:
            last_error = e
            if attempt < 3:
                log(f"鑾峰彇鐜鍒楄〃澶辫触锛岀 {attempt} 娆￠噸璇? {e}", "WARN")
                time.sleep(min(1.5 * attempt, 4.0))
                continue
    if last_error is not None:
        log(f"鑾峰彇鐜鍒楄〃澶辫触: {last_error}", "ERR")
    return []


def resolve_containers_for_tag(tag: str, project_folder: Optional[Path]) -> tuple[List[Dict], str, Dict[int, str]]:
    serial_to_channel_name = load_channels_registry(project_folder)

    tagged_containers = get_containers_by_tag(tag)
    if tagged_containers:
        return tagged_containers, "hubstudio_tag", serial_to_channel_name

    wanted_serials = set(serial_to_channel_name.keys())
    wanted_serials.update(extract_window_plan_serials(tag))

    if wanted_serials:
        matched = [
            c for c in get_all_containers()
            if int(c.get("serialNumber", 0) or 0) in wanted_serials
        ]
        matched = sorted(matched, key=lambda x: int(x.get("serialNumber", 0) or 0))
        if matched:
            log(
                f"鏈壘鍒版爣绛句负 '{tag}' 鐨勫垎缁勭幆澧冿紝鏀圭敤璁″垝/registry 搴忓彿鍥為€€鍖归厤: {sorted(wanted_serials)}",
                "WARN",
            )
            return matched, "serial_fallback", serial_to_channel_name

        mapping_registry = load_channel_mapping_registry()
        mapped = [
            mapping_registry[serial]
            for serial in sorted(wanted_serials)
            if serial in mapping_registry
        ]
        if mapped:
            log(
                f"BitBrowser 鍒楄〃鏆傛椂涓嶅彲鐢紝鏀圭敤 channel_mapping 鍥為€€鍖归厤: {sorted(wanted_serials)}",
                "WARN",
            )
            return mapped, "channel_mapping_fallback", serial_to_channel_name

    return [], "missing", serial_to_channel_name


def main():
    args = parse_arguments()
    
    print("\n" + "=" * 60)
    print("   YouTube 批量上传脚本")
    print("=" * 60 + "\n")
    
    config = load_config()
    window_plan = load_window_upload_plan(args.window_plan_file)
    globals()["ACTIVE_WINDOW_PLAN"] = window_plan
    if args.window_plan_file:
        if window_plan:
            print(f"📋 已加载窗口任务计划: {args.window_plan_file}")
        else:
            print(f"⚠️ 窗口任务计划加载失败或不存在: {args.window_plan_file}")
    
    if args.auto or (not args.tag and not args.date and not args.channel):
        exit_code = _run_auto_mode(args, config, window_plan=window_plan)
    else:
        exit_code = _run_traditional_mode(args, config, window_plan=window_plan)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
