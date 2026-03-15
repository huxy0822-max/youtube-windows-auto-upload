from __future__ import annotations

import json
import random
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from content_generation import (
    append_metadata_history,
    call_image_model,
    generate_content_bundle,
    get_recent_metadata_history,
    save_data_url_image,
)
from daily_scheduler import (
    RenderOptions,
    VIDEO_BITRATE,
    VIDEO_CODEC,
    _CODEC_EXTRA_ARGS,
    _safe_path_for_ffmpeg,
    build_effect_kwargs,
    clean_incomplete,
    get_audio_duration,
    mark_complete,
)
from effects_library import get_effect
from group_upload_workflow import IMAGE_EXTENSIONS, load_channel_name_map, normalize_mmdd
from path_helpers import normalize_scheduler_config
from prompt_studio import (
    default_api_preset,
    default_content_template,
    load_generation_map,
    load_prompt_studio_config,
    save_generation_map,
    save_prompt_studio_config,
)
from utils import get_all_tags, get_tag_info

SCRIPT_DIR = Path(__file__).parent
SCHEDULER_CONFIG_FILE = SCRIPT_DIR / "scheduler_config.json"
PROMPT_STUDIO_FILE = SCRIPT_DIR / "config" / "prompt_studio.json"
CHANNEL_MAPPING_FILE = SCRIPT_DIR / "config" / "channel_mapping.json"

AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg"}
PROGRESS_INTERVAL_SECONDS = 3.0

LogFunc = Callable[[str], None]


def _noop_log(_message: str) -> None:
    return


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


@dataclass(slots=True)
class WindowInfo:
    tag: str
    serial: int
    channel_name: str = ""
    is_ypp: bool = False


@dataclass(slots=True)
class WindowTask:
    tag: str
    serial: int
    is_ypp: bool = False
    title: str = ""
    description: str = ""
    visibility: str = "public"
    category: str = "Music"
    made_for_kids: bool = False
    altered_content: bool = True
    scheduled_publish_at: str = ""
    schedule_timezone: str = ""
    source_dir: str = ""
    channel_name: str = ""
    tag_list: list[str] = field(default_factory=list)
    thumbnails: list[str] = field(default_factory=list)
    ab_titles: list[str] = field(default_factory=list)

    def to_plan_dict(self, index: int) -> dict[str, Any]:
        row = {
            "index": index,
            "tag": self.tag,
            "serial": int(self.serial),
            "is_ypp": bool(self.is_ypp),
            "visibility": self.visibility,
            "category": self.category,
            "made_for_kids": bool(self.made_for_kids),
            "altered_content": bool(self.altered_content),
        }
        if self.title.strip():
            row["title"] = self.title.strip()
        if self.description.strip():
            row["description"] = self.description.strip()
        if self.scheduled_publish_at.strip():
            row["scheduled_publish_at"] = self.scheduled_publish_at.strip()
        if self.schedule_timezone.strip():
            row["schedule_timezone"] = self.schedule_timezone.strip()
        if self.source_dir.strip():
            row["source_dir"] = self.source_dir.strip()
        if self.channel_name.strip():
            row["channel_name"] = self.channel_name.strip()
        if self.tag_list:
            row["tag_list"] = [item for item in self.tag_list if str(item).strip()]
        if self.thumbnails:
            row["thumbnails"] = [item for item in self.thumbnails if str(item).strip()]
        if self.ab_titles:
            row["ab_titles"] = [item for item in self.ab_titles if str(item).strip()]
        return row


@dataclass(slots=True)
class WorkflowDefaults:
    date_mmdd: str
    visibility: str = "public"
    category: str = "Music"
    made_for_kids: bool = False
    altered_content: bool = True
    schedule_enabled: bool = False
    schedule_start: str = ""
    schedule_interval_minutes: int = 60
    schedule_timezone: str = "Asia/Taipei (+08:00)"
    metadata_mode: str = "prompt_api"
    generate_text: bool = True
    generate_thumbnails: bool = True
    sync_daily_content: bool = True
    randomize_effects: bool = True

    def upload_defaults(self) -> dict[str, Any]:
        values = {
            "visibility": self.visibility,
            "category": self.category,
            "made_for_kids": bool(self.made_for_kids),
            "altered_content": bool(self.altered_content),
        }
        if self.schedule_enabled and self.visibility == "schedule" and self.schedule_start.strip():
            values["scheduled_publish_at"] = self.schedule_start.strip()
            values["schedule_timezone"] = self.schedule_timezone.strip()
        return values


@dataclass(slots=True)
class SimulationOptions:
    simulate_seconds: int = 90
    consume_sources: bool = False
    save_manifest: bool = True


@dataclass(slots=True)
class RenderedItem:
    tag: str
    serial: int
    output_video: str
    source_image: str
    source_audio: str
    title: str
    description: str
    thumbnails: list[str]
    tag_list: list[str]
    ab_titles: list[str]
    effect_desc: str


@dataclass(slots=True)
class WorkflowResult:
    date_mmdd: str
    plan_path: str
    manifest_paths: list[str] = field(default_factory=list)
    output_dirs: list[str] = field(default_factory=list)
    items: list[RenderedItem] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "date_mmdd": self.date_mmdd,
            "plan_path": self.plan_path,
            "manifest_paths": list(self.manifest_paths),
            "output_dirs": list(self.output_dirs),
            "warnings": list(self.warnings),
            "items": [asdict(item) for item in self.items],
        }


def load_scheduler_settings(path: Path = SCHEDULER_CONFIG_FILE) -> dict[str, Any]:
    raw = _read_json(path, {})
    return normalize_scheduler_config(raw, SCRIPT_DIR)


def save_scheduler_settings(config: dict[str, Any], path: Path = SCHEDULER_CONFIG_FILE) -> dict[str, Any]:
    normalized = normalize_scheduler_config(config, SCRIPT_DIR)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return normalized


def load_prompt_settings(path: Path = PROMPT_STUDIO_FILE) -> dict[str, Any]:
    return load_prompt_studio_config(path)


def save_prompt_settings(config: dict[str, Any], path: Path = PROMPT_STUDIO_FILE) -> None:
    save_prompt_studio_config(path, config)


def ensure_prompt_presets(
    *,
    api_name: str,
    api_payload: dict[str, Any],
    content_name: str,
    content_payload: dict[str, Any],
    tag: str | None = None,
    path: Path = PROMPT_STUDIO_FILE,
) -> dict[str, Any]:
    config = load_prompt_settings(path)
    clean_api_name = api_name.strip() or "默认API模板"
    clean_content_name = content_name.strip() or "默认内容模板"
    api_value = default_api_preset(clean_api_name)
    api_value.update(api_payload or {})
    api_value["name"] = clean_api_name
    content_value = default_content_template(clean_content_name)
    content_value.update(content_payload or {})
    content_value["name"] = clean_content_name
    config.setdefault("apiPresets", {})[clean_api_name] = api_value
    config.setdefault("contentTemplates", {})[clean_content_name] = content_value
    if tag:
        config.setdefault("tagApiBindings", {})[tag] = clean_api_name
        config.setdefault("tagBindings", {})[tag] = clean_content_name
    save_prompt_settings(config, path)
    return config


def get_group_catalog() -> dict[str, list[WindowInfo]]:
    channel_name_map = load_channel_name_map(CHANNEL_MAPPING_FILE)
    catalog: dict[str, list[WindowInfo]] = {}
    for tag in get_all_tags():
        info = get_tag_info(tag) or {}
        ypp_serials = {int(item) for item in info.get("ypp_serials", [])}
        windows: list[WindowInfo] = []
        for serial in info.get("all_serials", []):
            windows.append(
                WindowInfo(
                    tag=tag,
                    serial=int(serial),
                    channel_name=channel_name_map.get(int(serial), ""),
                    is_ypp=int(serial) in ypp_serials,
                )
            )
        catalog[tag] = windows
    return catalog


def get_group_bindings(config: dict[str, Any] | None = None) -> dict[str, str]:
    cfg = config or load_scheduler_settings()
    bindings = cfg.get("group_source_bindings") or {}
    return {str(tag): str(path) for tag, path in bindings.items() if str(path).strip()}


def set_group_binding(tag: str, folder: str, *, config_path: Path = SCHEDULER_CONFIG_FILE) -> dict[str, Any]:
    config = load_scheduler_settings(config_path)
    bindings = dict(config.get("group_source_bindings") or {})
    clean_tag = str(tag).strip()
    clean_folder = str(folder).strip()
    if clean_tag:
        if clean_folder:
            bindings[clean_tag] = clean_folder
        else:
            bindings.pop(clean_tag, None)
    config["group_source_bindings"] = bindings
    return save_scheduler_settings(config, config_path)


def build_window_plan(tasks: list[WindowTask], defaults: WorkflowDefaults) -> dict[str, Any]:
    groups: dict[str, list[int]] = {}
    ordered: list[dict[str, Any]] = []
    preview_lines: list[str] = []

    for index, task in enumerate(tasks, 1):
        row = task.to_plan_dict(index)
        ordered.append(row)
        groups.setdefault(task.tag, []).append(int(task.serial))

    for tag in groups:
        groups[tag] = sorted(groups[tag])
        preview_lines.append(f"[{tag}] {', '.join(str(item) for item in groups[tag])}")

    if defaults.schedule_enabled and defaults.visibility == "schedule" and defaults.schedule_start.strip():
        start = _parse_schedule_time(defaults.schedule_start)
        for index, row in enumerate(ordered):
            if row.get("scheduled_publish_at"):
                continue
            current = start + timedelta(minutes=max(1, defaults.schedule_interval_minutes) * index)
            row["scheduled_publish_at"] = current.strftime("%Y-%m-%d %H:%M")

    default_upload_options = defaults.upload_defaults()
    if default_upload_options:
        preview_lines.append(
            "默认上传规则: "
            + ", ".join(f"{key}={value}" for key, value in default_upload_options.items())
        )

    return {
        "scope_mode": "manual_windows",
        "default_tag": "",
        "default_upload_options": default_upload_options,
        "schedule_start": defaults.schedule_start.strip(),
        "schedule_interval_minutes": max(1, int(defaults.schedule_interval_minutes)),
        "tasks": ordered,
        "groups": groups,
        "tags": sorted(groups.keys()),
        "warnings": [],
        "preview_lines": preview_lines,
    }


def save_window_plan(plan: dict[str, Any], date_mmdd: str, path: Path | None = None) -> Path:
    target = path or (SCRIPT_DIR / "data" / f"window_upload_plan_{date_mmdd}.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def list_media_files(folder: Path, suffixes: set[str]) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(
        [
            item
            for item in folder.iterdir()
            if item.is_file() and item.suffix.lower() in suffixes and not item.name.startswith(".")
        ],
        key=lambda item: item.name.lower(),
    )


def resolve_task_source_dir(task: WindowTask, config: dict[str, Any]) -> Path:
    if task.source_dir.strip():
        return Path(task.source_dir.strip())
    bindings = get_group_bindings(config)
    path_text = str(bindings.get(task.tag) or "").strip()
    if not path_text:
        raise ValueError(f"{task.tag} 还没有绑定素材目录")
    return Path(path_text)


def validate_group_sources(
    tasks: list[WindowTask],
    *,
    config: dict[str, Any] | None = None,
    log: LogFunc = _noop_log,
) -> tuple[list[str], list[str]]:
    cfg = config or load_scheduler_settings()
    errors: list[str] = []
    warnings: list[str] = []
    for tag in sorted({task.tag for task in tasks}):
        current_tasks = [task for task in tasks if task.tag == tag]
        override_folders = {task.source_dir.strip() for task in current_tasks if task.source_dir.strip()}
        if len(override_folders) > 1:
            warnings.append(f"{tag} 有多个窗口使用不同素材目录，将按窗口覆盖逐个处理。")
        try:
            folder = resolve_task_source_dir(current_tasks[0], cfg)
        except Exception as exc:
            errors.append(str(exc))
            continue
        if not folder.exists():
            errors.append(f"{tag} 的素材目录不存在: {folder}")
            continue
        image_count = len(list_media_files(folder, IMAGE_EXTENSIONS))
        audio_count = len(list_media_files(folder, AUDIO_EXTENSIONS))
        if image_count <= 0:
            errors.append(f"{tag} 的素材目录没有图片: {folder}")
        if audio_count <= 0:
            errors.append(f"{tag} 的素材目录没有音频: {folder}")
        log(f"[检查] {tag}: {folder} | 图片 {image_count} | 音频 {audio_count}")
    return errors, warnings


def _parse_schedule_time(value: str) -> datetime:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    raise ValueError("定时发布时间格式应为 YYYY-MM-DD HH:MM")


def _copy_if_needed(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve(strict=False) == target.resolve(strict=False):
        return target
    if target.exists():
        target.unlink()
    shutil.copy2(source, target)
    return target


def _move_to_used(source: Path, used_root: Path, *, tag: str, kind: str) -> Path:
    target_dir = used_root / tag / kind
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / source.name
    if target.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = target_dir / f"{source.stem}_{stamp}{source.suffix.lower()}"
    shutil.move(str(source), str(target))
    return target


def cleanup_old_uploaded_videos(output_root: Path, retention_days: int, log: LogFunc = _noop_log) -> int:
    if retention_days <= 0 or not output_root.exists():
        return 0
    cutoff = datetime.now() - timedelta(days=retention_days)
    cleaned = 0
    for folder in output_root.iterdir():
        if not folder.is_dir():
            continue
        report_path = folder / "upload_report.json"
        if not report_path.exists():
            continue
        if datetime.fromtimestamp(report_path.stat().st_mtime) >= cutoff:
            continue
        for video_path in folder.glob("*.mp4"):
            try:
                video_path.unlink()
                Path(str(video_path) + ".done").unlink(missing_ok=True)
                cleaned += 1
            except Exception:
                continue
    if cleaned:
        log(f"[清理] 已删除超过 {retention_days} 天的本地成品 {cleaned} 个")
    return cleaned


def _load_daily_entry(tag_dir: Path, date_mmdd: str, serial: int) -> dict[str, Any]:
    generation_map_path = tag_dir / "generation_map.json"
    generation_map = load_generation_map(generation_map_path)
    channel = (generation_map.get("channels") or {}).get(str(serial)) or {}
    return (channel.get("days") or {}).get(date_mmdd) or {}


def _save_daily_entry(
    generation_map_path: Path,
    *,
    date_mmdd: str,
    serial: int,
    is_ypp: bool,
    title: str,
    description: str,
    covers: list[str],
    ab_titles: list[str],
) -> None:
    generation_map = load_generation_map(generation_map_path)
    channels = generation_map.setdefault("channels", {})
    channel_info = channels.setdefault(str(serial), {"is_ypp": bool(is_ypp), "days": {}})
    channel_info["is_ypp"] = bool(is_ypp)
    channel_info.setdefault("days", {})
    channel_info["days"][date_mmdd] = {
        "title": title,
        "description": description,
        "covers": covers,
        "ab_titles": ab_titles,
        "set": 1,
    }
    save_generation_map(generation_map_path, generation_map)


def _make_cover_fallbacks(source_image: Path, tag_dir: Path, date_mmdd: str, serial: int, count: int) -> list[Path]:
    covers: list[Path] = []
    for index in range(1, count + 1):
        target = tag_dir / f"{date_mmdd}_{serial}_cover_{index:02d}{source_image.suffix.lower()}"
        covers.append(_copy_if_needed(source_image, target))
    return covers


def _write_manifest(
    *,
    output_dir: Path,
    date_mmdd: str,
    tag: str,
    channels: dict[str, Any],
    source_label: str,
) -> Path:
    manifest = {
        "date": date_mmdd,
        "tag": tag,
        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source": source_label,
        "channels": channels,
    }
    target = output_dir / "upload_manifest.json"
    target.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _pair_media(tasks: list[WindowTask], folder: Path, *, shuffle: bool = False) -> list[tuple[WindowTask, Path, Path]]:
    images = list_media_files(folder, IMAGE_EXTENSIONS)
    audio = list_media_files(folder, AUDIO_EXTENSIONS)
    if shuffle:
        random.shuffle(images)
        random.shuffle(audio)
    usable = min(len(tasks), len(images), len(audio))
    return [(tasks[index], images[index], audio[index]) for index in range(usable)]


def _build_upload_options(task: WindowTask) -> dict[str, Any]:
    values = {
        "visibility": task.visibility.strip() or "public",
        "category": task.category.strip() or "Music",
        "made_for_kids": bool(task.made_for_kids),
        "altered_content": bool(task.altered_content),
    }
    if task.scheduled_publish_at.strip():
        values["scheduled_publish_at"] = task.scheduled_publish_at.strip()
    if task.schedule_timezone.strip():
        values["schedule_timezone"] = task.schedule_timezone.strip()
    return values


def _render_with_progress(
    *,
    image_path: Path,
    audio_path: Path,
    output_path: Path,
    filter_complex: str,
    extra_inputs: list[str] | None,
    clip_seconds: int | None,
    log: LogFunc,
) -> dict[str, Any]:
    from daily_scheduler import FFMPEG_BIN

    safe_image, tmp_img = _safe_path_for_ffmpeg(image_path, "img")
    safe_audio, tmp_aud = _safe_path_for_ffmpeg(audio_path, "aud")
    target_duration = get_audio_duration(audio_path)
    if clip_seconds:
        target_duration = min(target_duration, float(max(clip_seconds, 1)))

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-progress",
        "pipe:1",
        "-nostats",
        "-loop",
        "1",
        "-r",
        "25",
        "-i",
        safe_image,
        "-i",
        safe_audio,
    ]
    if extra_inputs:
        cmd.extend(extra_inputs)
    cmd.extend(
        [
            "-filter_complex",
            filter_complex,
            "-map",
            "[outv]",
            "-map",
            "1:a",
            "-c:v",
            VIDEO_CODEC,
            "-b:v",
            VIDEO_BITRATE,
        ]
        + list(_CODEC_EXTRA_ARGS)
        + [
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-color_range",
            "pc",
            "-aspect",
            "16:9",
            "-c:a",
            "copy",
        ]
    )
    if clip_seconds:
        cmd.extend(["-t", str(int(clip_seconds))])
    cmd.extend(["-shortest", str(output_path)])

    start = time.time()
    last_report = start
    last_ratio = -1.0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    try:
        while True:
            line = process.stdout.readline() if process.stdout else ""
            if not line:
                if process.poll() is not None:
                    break
                now = time.time()
                if now - last_report >= PROGRESS_INTERVAL_SECONDS:
                    log(f"[渲染] 仍在处理 {output_path.name} | 已耗时 {now - start:.0f}s")
                    last_report = now
                time.sleep(0.2)
                continue

            text = line.strip()
            if text.startswith("out_time_ms="):
                try:
                    out_seconds = int(text.split("=", 1)[1]) / 1_000_000
                except Exception:
                    continue
                now = time.time()
                ratio = 0.0 if target_duration <= 0 else min(out_seconds / target_duration, 1.0)
                if (ratio - last_ratio) >= 0.05 or (now - last_report) >= PROGRESS_INTERVAL_SECONDS:
                    ratio = 0.0 if target_duration <= 0 else min(out_seconds / target_duration, 1.0)
                    log(
                        f"[渲染] {output_path.name} 进度 {ratio * 100:.0f}% "
                        f"({out_seconds:.0f}/{target_duration:.0f}s)"
                    )
                    last_report = now
                    last_ratio = ratio
            elif text.startswith("progress=end"):
                break

        return_code = process.wait(timeout=30)
        if return_code != 0:
            raise RuntimeError(f"ffmpeg 退出码 {return_code}")
        elapsed = time.time() - start
        mark_complete(output_path, duration=target_duration)
        log(f"[渲染] 完成 {output_path.name} | 耗时 {elapsed:.1f}s")
        return {"success": True, "time": elapsed}
    finally:
        for temp_path in (tmp_img, tmp_aud):
            if temp_path:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass


def execute_direct_media_workflow(
    *,
    tasks: list[WindowTask],
    defaults: WorkflowDefaults,
    simulation: SimulationOptions | None = None,
    log: LogFunc = _noop_log,
) -> WorkflowResult:
    if not tasks:
        raise ValueError("至少需要一个窗口任务")

    config = load_scheduler_settings()
    output_root = Path(config["output_root"])
    base_image_root = Path(config["base_image_dir"])
    used_media_root = Path(config.get("used_media_root") or (SCRIPT_DIR / "workspace" / "AutoTask" / "_used_media"))
    cleanup_old_uploaded_videos(output_root, int(config.get("render_cleanup_days", 5)), log=log)

    plan = build_window_plan(tasks, defaults)
    plan_path = save_window_plan(plan, defaults.date_mmdd)
    result = WorkflowResult(date_mmdd=defaults.date_mmdd, plan_path=str(plan_path))
    log(f"[计划] 已写入窗口计划: {plan_path}")

    grouped: dict[str, list[WindowTask]] = {}
    for task in tasks:
        grouped.setdefault(task.tag, []).append(task)

    for tag, tag_tasks in grouped.items():
        source_dir = resolve_task_source_dir(tag_tasks[0], config)
        if not source_dir.exists():
            raise ValueError(f"{tag} 的素材目录不存在: {source_dir}")
        paired = _pair_media(tag_tasks, source_dir, shuffle=False)
        if not paired:
            raise ValueError(f"{tag} 的素材目录没有可用的图音组合")
        if len(paired) < len(tag_tasks):
            warning = f"{tag} 只够处理 {len(paired)} 个窗口，剩余窗口会跳过"
            result.warnings.append(warning)
            log(f"[警告] {warning}")

        output_dir = output_root / f"{defaults.date_mmdd}_{tag}"
        output_dir.mkdir(parents=True, exist_ok=True)
        tag_base_dir = base_image_root / tag
        tag_base_dir.mkdir(parents=True, exist_ok=True)
        generation_map_path = tag_base_dir / "generation_map.json"
        manifest_channels: dict[str, Any] = {}
        result.output_dirs.append(str(output_dir))

        for task, source_image, source_audio in paired:
            output_video = output_dir / f"{defaults.date_mmdd}_{task.serial}.mp4"
            clean_incomplete(output_video)
            output_video.unlink(missing_ok=True)
            Path(str(output_video) + ".done").unlink(missing_ok=True)

            render_options = RenderOptions()
            render_options.fx_randomize = bool(defaults.randomize_effects)
            effect_kwargs = build_effect_kwargs(render_options)
            duration = get_audio_duration(source_audio)
            filter_complex, effect_desc, extra_inputs = get_effect(duration, **effect_kwargs)
            log(
                f"[任务] {tag}/{task.serial}: {source_image.name} + {source_audio.name} "
                f"-> {output_video.name}"
            )
            log(f"[渲染] 编码器 {VIDEO_CODEC} | 码率 {VIDEO_BITRATE} | 特效 {effect_desc}")
            _render_with_progress(
                image_path=source_image,
                audio_path=source_audio,
                output_path=output_video,
                filter_complex=filter_complex,
                extra_inputs=extra_inputs,
                clip_seconds=simulation.simulate_seconds if simulation else None,
                log=log,
            )

            bundle = None
            title = task.title.strip()
            description = task.description.strip()
            tag_list = [item for item in task.tag_list if str(item).strip()]
            ab_titles = [item for item in task.ab_titles if str(item).strip()]
            cover_paths: list[Path] = [Path(item) for item in task.thumbnails if Path(item).exists()]
            thumbnail_prompts: list[str] = []
            history_scope = get_recent_metadata_history(tag, limit=24)
            unique_seed = f"{defaults.date_mmdd}|{tag}|{task.serial}|{source_audio.stem}|{source_image.stem}"

            if defaults.metadata_mode == "prompt_api" and (defaults.generate_text or defaults.generate_thumbnails):
                bundle = generate_content_bundle(
                    PROMPT_STUDIO_FILE,
                    tag,
                    is_ypp=task.is_ypp,
                    unique_seed=unique_seed,
                    avoid_titles=history_scope.get("titles"),
                    avoid_descriptions=history_scope.get("descriptions"),
                    avoid_thumbnail_prompts=history_scope.get("thumbnail_prompts"),
                    avoid_tag_signatures=history_scope.get("tag_signatures"),
                )
                thumbnail_prompts = [
                    str(item.get("prompt") or "").strip()
                    for item in bundle.get("thumbnail_prompts", [])
                    if str(item.get("prompt") or "").strip()
                ]

            if defaults.generate_text:
                if bundle:
                    if not title:
                        title = str((bundle.get("titles") or [output_video.stem])[0]).strip() or output_video.stem
                    description = str((bundle.get("descriptions") or [""])[0]).strip()
                    if not tag_list:
                        tag_list = [str(item).strip() for item in bundle.get("tag_list", []) if str(item).strip()]
                    if not ab_titles and task.is_ypp:
                        ab_titles = [
                            str(item).strip()
                            for item in (bundle.get("titles") or [])[:3]
                            if str(item).strip()
                        ]
                else:
                    legacy = _load_daily_entry(tag_base_dir, defaults.date_mmdd, task.serial)
                    if not title:
                        title = str(legacy.get("title") or output_video.stem).strip() or output_video.stem
                    if not description:
                        description = str(legacy.get("description") or "").strip()
                    if not tag_list:
                        tag_list = [str(item).strip() for item in legacy.get("tag_list", []) if str(item).strip()]
                    if not ab_titles and task.is_ypp:
                        ab_titles = [str(item).strip() for item in legacy.get("ab_titles", []) if str(item).strip()]
            elif not title:
                title = output_video.stem

            cover_count = 3 if task.is_ypp else 1
            if defaults.generate_thumbnails:
                if bundle and str(bundle["api_preset"].get("autoImageEnabled") or "0") == "1" and not cover_paths:
                    for cover_index, prompt in enumerate(thumbnail_prompts[:cover_count], 1):
                        target = tag_base_dir / f"{defaults.date_mmdd}_{task.serial}_cover_{cover_index:02d}.png"
                        try:
                            image_result = call_image_model(bundle["api_preset"], prompt)
                            if image_result.get("data_url"):
                                cover_paths.append(save_data_url_image(image_result["data_url"], target))
                        except Exception as exc:
                            result.warnings.append(f"{tag}/{task.serial} 缩略图生成失败: {exc}")
                            log(f"[警告] {tag}/{task.serial} 缩略图生成失败: {exc}")
                if not cover_paths:
                    cover_paths.extend(_make_cover_fallbacks(source_image, tag_base_dir, defaults.date_mmdd, task.serial, cover_count))

            if defaults.sync_daily_content:
                _save_daily_entry(
                    generation_map_path,
                    date_mmdd=defaults.date_mmdd,
                    serial=task.serial,
                    is_ypp=task.is_ypp,
                    title=title,
                    description=description,
                    covers=[path.name for path in cover_paths],
                    ab_titles=ab_titles,
                )

            append_metadata_history(
                tag=tag,
                title=title,
                description=description,
                tag_list=tag_list,
                thumbnail_prompts=thumbnail_prompts,
            )

            manifest_channels[str(task.serial)] = {
                "video": output_video.name,
                "source_image": str(source_image),
                "source_audio": str(source_audio),
                "effect_desc": effect_desc,
                "channel_name": task.channel_name.strip(),
                "title": title,
                "description": description,
                "thumbnails": [str(path) for path in cover_paths],
                "thumbnail_prompts": thumbnail_prompts,
                "tag_list": tag_list,
                "is_ypp": bool(task.is_ypp),
                "ab_titles": ab_titles,
                "set": 1,
                "upload_options": _build_upload_options(task),
            }
            result.items.append(
                RenderedItem(
                    tag=tag,
                    serial=task.serial,
                    output_video=str(output_video),
                    source_image=str(source_image),
                    source_audio=str(source_audio),
                    title=title,
                    description=description,
                    thumbnails=[str(path) for path in cover_paths],
                    tag_list=tag_list,
                    ab_titles=ab_titles,
                    effect_desc=effect_desc,
                )
            )

            if simulation and simulation.consume_sources:
                _move_to_used(source_image, used_media_root, tag=tag, kind="images")
                _move_to_used(source_audio, used_media_root, tag=tag, kind="audio")

        if simulation is None or simulation.save_manifest:
            manifest_path = _write_manifest(
                output_dir=output_dir,
                date_mmdd=defaults.date_mmdd,
                tag=tag,
                channels=manifest_channels,
                source_label="group_bound_media",
            )
            result.manifest_paths.append(str(manifest_path))
            log(f"[清单] {tag} manifest 已写入: {manifest_path}")

    return result


def describe_group_bindings(config: dict[str, Any] | None = None) -> str:
    bindings = get_group_bindings(config)
    if not bindings:
        return "还没有任何分组绑定。"
    lines = []
    for tag in get_all_tags():
        if bindings.get(tag):
            lines.append(f"{tag}: {bindings[tag]}")
    return "\n".join(lines) if lines else "还没有任何分组绑定。"


def create_task(
    *,
    tag: str,
    serial: int,
    is_ypp: bool = False,
    title: str = "",
    description: str = "",
    visibility: str = "public",
    category: str = "Music",
    made_for_kids: bool = False,
    altered_content: bool = True,
    scheduled_publish_at: str = "",
    schedule_timezone: str = "",
    source_dir: str = "",
    channel_name: str = "",
) -> WindowTask:
    return WindowTask(
        tag=str(tag).strip(),
        serial=int(serial),
        is_ypp=bool(is_ypp),
        title=str(title or "").strip(),
        description=str(description or "").strip(),
        visibility=str(visibility or "public").strip() or "public",
        category=str(category or "Music").strip() or "Music",
        made_for_kids=bool(made_for_kids),
        altered_content=bool(altered_content),
        scheduled_publish_at=str(scheduled_publish_at or "").strip(),
        schedule_timezone=str(schedule_timezone or "").strip(),
        source_dir=str(source_dir or "").strip(),
        channel_name=str(channel_name or "").strip(),
    )
