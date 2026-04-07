#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from prompt_studio import normalize_tag_key


SCOPE_SAME_GROUP = "same_group"
SCOPE_MULTI_GROUP = "multi_group"
SCOPE_MANUAL = "manual_windows"
VALID_SCOPE_MODES = {SCOPE_SAME_GROUP, SCOPE_MULTI_GROUP, SCOPE_MANUAL}

TASK_META_KEYS = {"index", "serial", "tag"}
UPLOAD_OPTION_KEYS = {
    "visibility",
    "category",
    "made_for_kids",
    "altered_content",
    "scheduled_publish_at",
    "schedule_timezone",
}


def parse_serials_text(raw: str) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    normalized = str(raw or "").replace("，", ",").replace("；", ",").replace(";", ",")
    for token in normalized.split(","):
        token = token.strip()
        if token.isdigit():
            serial = int(token)
            if serial not in seen:
                values.append(serial)
                seen.add(serial)
    return values


def _parse_bool(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "y", "on", "是"}


def _parse_tags(value: str) -> list[str]:
    tags: list[str] = []
    for token in str(value or "").replace("，", ",").split(","):
        token = token.strip()
        if token:
            tags.append(token)
    return tags


def _normalize_override(task: dict[str, Any], key: str, value: str) -> None:
    key = key.strip().lower()
    value = value.strip()
    if not key:
        return
    if key in {"visibility", "category", "title", "description", "scheduled_publish_at", "schedule_timezone", "channel_name"}:
        task[key] = value
        return
    if key in {"made_for_kids", "altered_content"}:
        task[key] = _parse_bool(value)
        return
    if key in {"is_ypp", "ypp"}:
        task["is_ypp"] = _parse_bool(value)
        return
    if key in {"tag_list", "tags"}:
        task["tag_list"] = _parse_tags(value)
        return
    if key in {"thumbnail", "thumbnails"}:
        task["thumbnails"] = _parse_tags(value)
        return
    if key == "ab_titles":
        task["ab_titles"] = _parse_tags(value)


def _finalize_task(task: dict[str, Any], index: int) -> dict[str, Any]:
    tag = str(task.get("tag") or "").strip()
    serial = int(task["serial"])
    if not tag:
        raise ValueError(f"第 {index} 条窗口任务缺少分组 tag")
    finalized = {
        "index": index,
        "serial": serial,
        "tag": tag,
    }
    for key in (
        "title",
        "description",
        "channel_name",
        "tag_list",
        "thumbnails",
        "ab_titles",
        "is_ypp",
        "visibility",
        "category",
        "made_for_kids",
        "altered_content",
        "scheduled_publish_at",
        "schedule_timezone",
    ):
        if key in task:
            finalized[key] = task[key]
    return finalized


def _build_groups(tasks: list[dict[str, Any]]) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = {}
    for task in tasks:
        groups.setdefault(task["tag"], []).append(int(task["serial"]))
    return {tag: sorted(serials) for tag, serials in groups.items()}


def _clean_default_options(default_upload_options: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(default_upload_options or {})
    cleaned: dict[str, Any] = {}
    for key in UPLOAD_OPTION_KEYS:
        if key in data and data[key] not in (None, ""):
            cleaned[key] = data[key]
    return cleaned


def _apply_schedule_defaults(
    tasks: list[dict[str, Any]],
    default_upload_options: dict[str, Any],
    schedule_start: str,
    schedule_interval_minutes: int,
) -> None:
    if str(default_upload_options.get("visibility") or "").strip().lower() != "schedule":
        return
    if not schedule_start:
        return

    start_value = str(schedule_start).strip()
    parsed = None
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            parsed = datetime.strptime(start_value, fmt)
            break
        except Exception:
            continue
    if not parsed:
        raise ValueError("定时发布时间格式应为 YYYY-MM-DD HH:MM")

    for index, task in enumerate(tasks):
        if task.get("scheduled_publish_at"):
            continue
        schedule_time = parsed + timedelta(minutes=max(schedule_interval_minutes, 1) * index)
        task["scheduled_publish_at"] = schedule_time.strftime("%Y-%m-%d %H:%M")


def parse_same_group_tasks(default_tag: str, serials_text: str) -> list[dict[str, Any]]:
    tag = str(default_tag or "").strip()
    if not tag:
        raise ValueError("同分组模式必须先选择一个默认分组 tag")
    tasks = []
    for index, serial in enumerate(parse_serials_text(serials_text), 1):
        tasks.append({"index": index, "serial": serial, "tag": tag})
    return tasks


def parse_multi_group_tasks(raw: str) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    index = 1
    for line_no, line in enumerate(str(raw or "").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        if ":" not in line:
            raise ValueError(f"多分组第 {line_no} 行格式错误，应写成 tag: 90,94,95")
        tag, serials_text = line.split(":", 1)
        tag = tag.strip()
        if not tag:
            raise ValueError(f"多分组第 {line_no} 行缺少 tag")
        serials = parse_serials_text(serials_text)
        if not serials:
            raise ValueError(f"多分组第 {line_no} 行没有解析到窗口序号")
        for serial in serials:
            tasks.append({"index": index, "serial": serial, "tag": tag})
            index += 1
    return tasks


def parse_manual_tasks(raw: str, default_tag: str = "") -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for line_no, line in enumerate(str(raw or "").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split("|") if part.strip()]
        if not parts:
            continue
        if not parts[0].isdigit():
            raise ValueError(f"逐窗口模式第 {line_no} 行第一列必须是数字序号: '{parts[0]}'")
        task: dict[str, Any] = {"serial": int(parts[0]), "tag": default_tag}
        cursor = 1
        if cursor < len(parts) and "=" not in parts[cursor]:
            task["tag"] = parts[cursor]
            cursor += 1
        for token in parts[cursor:]:
            if "=" not in token:
                raise ValueError(f"逐窗口模式第 {line_no} 行的覆盖项必须写成 key=value")
            key, value = token.split("=", 1)
            _normalize_override(task, key, value)
        tasks.append(_finalize_task(task, line_no))
    return tasks


def build_window_upload_plan(
    *,
    scope_mode: str,
    default_tag: str = "",
    same_group_serials_text: str = "",
    multi_group_text: str = "",
    manual_text: str = "",
    default_upload_options: dict[str, Any] | None = None,
    schedule_start: str = "",
    schedule_interval_minutes: int = 60,
) -> dict[str, Any]:
    scope_mode = scope_mode if scope_mode in VALID_SCOPE_MODES else SCOPE_SAME_GROUP
    if scope_mode == SCOPE_SAME_GROUP:
        tasks = parse_same_group_tasks(default_tag, same_group_serials_text)
    elif scope_mode == SCOPE_MULTI_GROUP:
        tasks = parse_multi_group_tasks(multi_group_text)
    else:
        tasks = parse_manual_tasks(manual_text, default_tag=default_tag)

    default_upload_options = _clean_default_options(default_upload_options)
    _apply_schedule_defaults(tasks, default_upload_options, schedule_start, schedule_interval_minutes)

    unique: dict[tuple[str, int], dict[str, Any]] = {}
    warnings: list[str] = []
    ordered_tasks: list[dict[str, Any]] = []
    for idx, task in enumerate(tasks, 1):
        key = (task["tag"], int(task["serial"]))
        if key in unique:
            warnings.append(f"重复窗口已自动去重: {task['tag']} / {task['serial']}")
            continue
        task = dict(task)
        task["index"] = idx
        unique[key] = task
        ordered_tasks.append(task)

    groups = _build_groups(ordered_tasks)
    preview_lines: list[str] = []
    for tag, serials in groups.items():
        preview_lines.append(f"[{tag}] {', '.join(str(item) for item in serials)}")
    if default_upload_options:
        preview_lines.append("默认上传规则: " + ", ".join(f"{k}={v}" for k, v in default_upload_options.items()))
    for task in ordered_tasks:
        override_keys = [key for key in task.keys() if key not in TASK_META_KEYS]
        if override_keys:
            preview_lines.append(f"  - 窗口 {task['serial']} 覆盖: {', '.join(sorted(override_keys))}")

    return {
        "scope_mode": scope_mode,
        "default_tag": default_tag,
        "default_upload_options": default_upload_options,
        "schedule_start": str(schedule_start or "").strip(),
        "schedule_interval_minutes": int(max(schedule_interval_minutes, 1)),
        "tasks": ordered_tasks,
        "groups": groups,
        "tags": list(groups.keys()),
        "warnings": warnings,
        "preview_lines": preview_lines,
    }


def save_window_upload_plan(path: Path, plan: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_window_upload_plan(path: Path | str | None) -> dict[str, Any] | None:
    if not path:
        return None
    plan_path = Path(path)
    if not plan_path.exists():
        return None
    try:
        data = json.loads(plan_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _same_tag(left: str, right: str) -> bool:
    left_text = str(left or "").strip()
    right_text = str(right or "").strip()
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True
    return normalize_tag_key(left_text) == normalize_tag_key(right_text)


def find_window_task(plan: dict[str, Any] | None, tag: str, serial: int) -> dict[str, Any] | None:
    if not plan:
        return None
    for task in plan.get("tasks", []):
        if _same_tag(task.get("tag") or "", tag) and int(task.get("serial") or 0) == int(serial):
            return dict(task)
    return None


def merge_manifest_with_window_task(
    channel_manifest: dict[str, Any],
    task: dict[str, Any] | None,
    default_upload_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = dict(channel_manifest or {})
    upload_options = dict(
        merged.get("upload_options", {})
        if isinstance(merged.get("upload_options", {}), dict)
        else {}
    )
    for key, value in _clean_default_options(default_upload_options).items():
        upload_options.setdefault(key, value)

    if task:
        for key in ("title", "description", "channel_name", "tag_list", "thumbnails", "ab_titles", "is_ypp"):
            if key in task:
                merged[key] = task[key]
        for key in UPLOAD_OPTION_KEYS:
            if key in task:
                upload_options[key] = task[key]

    if upload_options:
        merged["upload_options"] = upload_options
    return merged


def derive_tags_and_skip_channels(plan: dict[str, Any], tag_info_getter) -> tuple[list[str], list[int]]:
    tags = [str(tag).strip() for tag in plan.get("tags", []) if str(tag).strip()]
    skip: list[int] = []
    for tag in tags:
        info = tag_info_getter(tag) or {}
        if not info:
            for raw_tag in plan.get("groups", {}).keys():
                if _same_tag(raw_tag, tag):
                    info = tag_info_getter(raw_tag) or {}
                    if info:
                        break
        all_serials = {int(item) for item in info.get("all_serials", [])}
        wanted: set[int] = set()
        for raw_tag, serials in (plan.get("groups", {}) or {}).items():
            if _same_tag(raw_tag, tag):
                wanted.update(int(item) for item in serials)
        skip.extend(sorted(all_serials - wanted))
    return tags, skip
