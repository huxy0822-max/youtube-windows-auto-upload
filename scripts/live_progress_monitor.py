# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

LOG_PREFIX_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2}(?: \[[A-Z]+\])?(?: \[[^\]]+\])?(?: \|)? (?P<body>.*)$"
)
TASK_LINE_RE = re.compile(
    r"^(?P<group>.+?)/(?P<serial>\d+)\[(?P<slot>\d+)/(?P<total>\d+)\]: .* -> (?P<output>\S+)$"
)
RENDER_RE = re.compile(
    r"^(?:\[[^\]]+\]\s+)?(?P<output>\S+) .* (?P<pct>\d+)% \((?P<elapsed>\d+)/(?P<duration>\d+)s\)$"
)
METADATA_PROMPT_RE = re.compile(
    r"^(?P<group>.+?)/(?P<serial>\d+): API=(?P<api>.+?) \| .*?=(?P<template>.+?) \| seed="
)
METADATA_READY_PATTERNS = [
    re.compile(r"^(?P<group>.+?) .* (?P<serial>\d+) .*: (?P<title>.*)$"),
    re.compile(r"^\[文案\] (?P<group>.+?) 窗口 (?P<serial>\d+) 标题: (?P<title>.*)$"),
]
UPLOAD_START_PATTERNS = [
    re.compile(r"^\[上传\] (?P<group>.+?) 窗口 (?P<serial>\d+) 开始上传$"),
    re.compile(r"^\[Upload (?P<group>.+?)/(?P<serial>\d+)\] 开始上传: 序号 \d+$"),
]
UPLOAD_FINISH_PATTERNS = [
    re.compile(r"^\[上传\] (?P<group>.+?) 窗口 (?P<serial>\d+) (?P<status>[✅❌])(?: \((?P<count>\d+)/(?P<total>\d+)\))? ?(?P<detail>.*)$"),
    re.compile(r"^\[上传\] (?P<group>.+?) .* (?P<serial>\d+) .* \((?P<count>\d+)/(?P<total>\d+)\) (?P<detail>.*)$"),
]
UPLOAD_MONITOR_RE = re.compile(
    r"^序号 (?P<serial>\d+): 监控#\d+ 状态=(?P<status>[^|]+)\s+\|\s+进度=(?P<pct>\d+)%"
)


def _message_body(line: str) -> str:
    text = str(line or "").rstrip()
    matched = LOG_PREFIX_RE.match(text)
    return str(matched.group("body") if matched else text).strip()


def _normalize_group(value: str) -> str:
    return re.sub(r"^\[[^\]]+\]\s*", "", str(value or "").strip())


def _slot_key(group: str, serial: int, slot_index: int, output: str = "") -> str:
    clean_group = _normalize_group(group)
    clean_output = str(output or "").strip()
    if clean_output:
        return f"{clean_group}::{int(serial or 0)}::{int(slot_index or 1)}::{clean_output}"
    return f"{clean_group}::{int(serial or 0)}::{int(slot_index or 1)}"


def _blank_slot(
    *,
    output: str = "",
    group: str = "",
    serial: int = 0,
    slot_index: int = 1,
    slot_total: int = 1,
    api: str = "",
    template: str = "",
) -> dict[str, Any]:
    return {
        "output": output,
        "group": _normalize_group(group),
        "serial": int(serial or 0),
        "slot_index": int(slot_index or 1),
        "slot_total": int(slot_total or 1),
        "stage": "等待开始",
        "render_progress": 0,
        "upload_progress": 0,
        "elapsed_seconds": 0,
        "duration_seconds": 0,
        "metadata_prompt_started": False,
        "metadata_ready": False,
        "upload_started": False,
        "upload_finished": False,
        "upload_success": None,
        "detail": "",
        "api": api,
        "template": template,
        "title": "",
    }


def _load_lines(log_path: Path) -> list[str]:
    if not log_path.exists():
        return []
    return log_path.read_text(encoding="utf-8", errors="replace").splitlines()


def _slot_sort_key(item: dict[str, Any]) -> tuple[int, int]:
    return int(item.get("serial") or 0), int(item.get("slot_index") or 0)


def _recent_lines(lines: list[str], limit: int = 30) -> list[str]:
    return [str(line or "").rstrip() for line in lines if str(line or "").strip()][-limit:]


def _serial_slots(slots: dict[str, dict[str, Any]], serial: int, group: str = "") -> list[dict[str, Any]]:
    return sorted(
        (
            item
            for item in slots.values()
            if int(item.get("serial") or 0) == serial
            and (not group or _normalize_group(item.get("group") or "") == _normalize_group(group))
        ),
        key=_slot_sort_key,
    )


def _pick_serial_slot(
    slots: dict[str, dict[str, Any]],
    serial: int,
    predicate,
    group: str = "",
    fallback_to_last: bool = True,
) -> dict[str, Any] | None:
    candidates = _serial_slots(slots, serial, group=group)
    for item in candidates:
        if predicate(item):
            return item
    if fallback_to_last:
        return candidates[-1] if candidates else None
    return None


def _load_job_context(job_path: Path | None) -> dict[str, Any]:
    if not job_path or not job_path.exists():
        return {}
    try:
        payload = json.loads(job_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    summary_payload = payload.get("summary") or {}
    slot_payload = payload.get("slots") or []
    jobs = payload.get("jobs") or []
    slot_seeds: list[dict[str, Any]] = []
    groups: list[str] = []
    windows: list[str] = []
    videos_per_window = 0

    for raw_slot in slot_payload:
        if not isinstance(raw_slot, dict):
            continue
        group = _normalize_group(raw_slot.get("group") or "")
        serial = int(raw_slot.get("serial") or 0)
        slot_index = max(1, int(raw_slot.get("slot_index") or 1))
        slot_total = max(1, int(raw_slot.get("slot_total") or raw_slot.get("total_slots") or 1))
        if group and group not in groups:
            groups.append(group)
        if serial > 0 and str(serial) not in windows:
            windows.append(str(serial))
        videos_per_window = max(videos_per_window, slot_total)
        slot_seeds.append(
            {
                "group": group,
                "serial": serial,
                "slot_index": slot_index,
                "slot_total": slot_total,
                "output": str(raw_slot.get("output") or "").strip(),
                "api": str(raw_slot.get("api") or "").strip(),
                "template": str(raw_slot.get("template") or "").strip(),
            }
        )

    for job in jobs:
        if not isinstance(job, dict):
            continue
        group = _normalize_group(job.get("group_tag") or "")
        if group and group not in groups:
            groups.append(group)
        job_windows = [str(item) for item in (job.get("window_serials") or []) if str(item).strip()]
        for item in job_windows:
            if item not in windows:
                windows.append(item)
        slot_total = max(1, int(job.get("videos_per_window") or 1))
        videos_per_window = max(videos_per_window, slot_total)
        api = str(job.get("api_template") or "").strip()
        template = str(job.get("prompt_template") or "").strip()
        for serial_text in job_windows:
            serial = int(serial_text)
            for slot_index in range(1, slot_total + 1):
                slot_seeds.append(
                    {
                        "group": group,
                        "serial": serial,
                        "slot_index": slot_index,
                        "slot_total": slot_total,
                        "output": "",
                        "api": api,
                        "template": template,
                    }
                )

    summary_group = _normalize_group(summary_payload.get("group") or "") or " / ".join(groups)
    return {
        "group": summary_group,
        "windows": [str(item) for item in (summary_payload.get("windows") or windows) if str(item).strip()],
        "videos_per_window": int(summary_payload.get("videos_per_window") or videos_per_window or 0),
        "slots": slot_seeds,
    }


def _find_slot_by_output(slots: dict[str, dict[str, Any]], output_name: str) -> dict[str, Any] | None:
    clean_output = str(output_name or "").strip()
    if not clean_output:
        return None
    for item in slots.values():
        if str(item.get("output") or "").strip() == clean_output:
            return item
    return None


def build_snapshot(log_path: Path, job_path: Path | None = None) -> dict[str, Any]:
    lines = _load_lines(log_path)
    slots: dict[str, dict[str, Any]] = {}
    recent = _recent_lines(lines, limit=30)
    job_context = _load_job_context(job_path)

    for seed in job_context.get("slots") or []:
        key = _slot_key(
            seed.get("group", ""),
            int(seed.get("serial") or 0),
            int(seed.get("slot_index") or 1),
            str(seed.get("output") or "").strip(),
        )
        slots[key] = _blank_slot(
            output=str(seed.get("output") or "").strip(),
            group=str(seed.get("group") or "").strip(),
            serial=int(seed.get("serial") or 0),
            slot_index=int(seed.get("slot_index") or 1),
            slot_total=int(seed.get("slot_total") or 1),
            api=str(seed.get("api") or "").strip(),
            template=str(seed.get("template") or "").strip(),
        )

    for line in lines:
        line = str(line or "").rstrip()
        if not line:
            continue
        body = _message_body(line)

        task_match = TASK_LINE_RE.match(body)
        if task_match:
            group = _normalize_group(task_match.group("group"))
            serial = int(task_match.group("serial"))
            slot_index = int(task_match.group("slot"))
            slot_total = int(task_match.group("total"))
            output_name = task_match.group("output")
            seed_key = _slot_key(group, serial, slot_index)
            key = seed_key if seed_key in slots else _slot_key(group, serial, slot_index, output_name)
            slot = slots.setdefault(key, _blank_slot(group=group, serial=serial, slot_index=slot_index, slot_total=slot_total))
            slot["output"] = output_name
            slot["group"] = group
            slot["serial"] = serial
            slot["slot_index"] = slot_index
            slot["slot_total"] = slot_total
            slot["stage"] = "渲染中"
            continue

        render_match = RENDER_RE.match(body)
        if render_match:
            output_name = render_match.group("output")
            slot = _find_slot_by_output(slots, output_name)
            if slot is None:
                key = _slot_key("", 0, len(slots) + 1)
                slot = slots.setdefault(key, _blank_slot(output=output_name))
            slot["stage"] = "渲染中"
            slot["output"] = output_name
            slot["render_progress"] = int(render_match.group("pct"))
            slot["elapsed_seconds"] = int(render_match.group("elapsed"))
            slot["duration_seconds"] = int(render_match.group("duration"))
            continue

        meta_match = METADATA_PROMPT_RE.match(body)
        if meta_match:
            group = _normalize_group(meta_match.group("group"))
            serial = int(meta_match.group("serial"))
            slot = _pick_serial_slot(
                slots,
                serial,
                lambda item: not bool(item.get("metadata_prompt_started")),
                group=group,
            )
            if slot:
                slot["metadata_prompt_started"] = True
                slot["api"] = meta_match.group("api")
                slot["template"] = meta_match.group("template")
            continue

        if "标题:" in line:
            meta_ready_match = None
            for pattern in METADATA_READY_PATTERNS:
                matched = pattern.match(body)
                if matched:
                    meta_ready_match = matched
                    break
            if meta_ready_match is not None:
                group = _normalize_group(meta_ready_match.group("group"))
                serial = int(meta_ready_match.group("serial"))
                title = meta_ready_match.group("title")
                slot = _pick_serial_slot(
                    slots,
                    serial,
                    lambda item: not bool(item.get("metadata_ready")),
                    group=group,
                )
                if slot:
                    slot["metadata_prompt_started"] = True
                    slot["metadata_ready"] = True
                    slot["title"] = title
                    if not slot["upload_started"]:
                        slot["stage"] = "文案完成，等待上传"
                continue

        upload_start_match = None
        for pattern in UPLOAD_START_PATTERNS:
            matched = pattern.match(body)
            if matched:
                upload_start_match = matched
                break
        if upload_start_match is not None:
            group = _normalize_group(upload_start_match.groupdict().get("group") or "")
            serial = int(upload_start_match.group("serial"))
            slot = _pick_serial_slot(
                slots,
                serial,
                lambda item: (
                    not bool(item.get("upload_started"))
                    and not bool(item.get("upload_finished"))
                    and bool(item.get("metadata_ready"))
                ),
                group=group,
                fallback_to_last=False,
            ) or _pick_serial_slot(
                slots,
                serial,
                lambda item: not bool(item.get("upload_started")) and not bool(item.get("upload_finished")),
                group=group,
                fallback_to_last=False,
            )
            if slot:
                slot["upload_started"] = True
                slot["stage"] = "上传中"
            continue

        upload_monitor_match = UPLOAD_MONITOR_RE.match(body)
        if upload_monitor_match:
            serial = int(upload_monitor_match.group("serial"))
            pct = int(upload_monitor_match.group("pct"))
            status = str(upload_monitor_match.group("status") or "").strip()
            slot = _pick_serial_slot(
                slots,
                serial,
                lambda item: bool(item.get("upload_started")) and not bool(item.get("upload_finished")),
                fallback_to_last=False,
            ) or _pick_serial_slot(
                slots,
                serial,
                lambda item: not bool(item.get("upload_finished")),
                fallback_to_last=False,
            )
            if slot:
                slot["upload_started"] = True
                slot["stage"] = "上传中"
                slot["upload_progress"] = pct
                slot["detail"] = f"{status} | {pct}%"
            continue

        if "[上传]" in line and ("✓" in line or "✗" in line or "✅" in line or "❌" in line):
            upload_finish_match = None
            for pattern in UPLOAD_FINISH_PATTERNS:
                matched = pattern.match(body)
                if matched:
                    upload_finish_match = matched
                    break
            if upload_finish_match:
                group = _normalize_group(upload_finish_match.groupdict().get("group") or "")
                serial = int(upload_finish_match.group("serial"))
                status_token = str(upload_finish_match.groupdict().get("status") or "")
                success = status_token == "✅" or "✓" in line
                detail = str(upload_finish_match.groupdict().get("detail") or "").strip()
                slot = _pick_serial_slot(
                    slots,
                    serial,
                    lambda item: bool(item.get("upload_started")) and not bool(item.get("upload_finished")),
                    group=group,
                    fallback_to_last=False,
                ) or _pick_serial_slot(
                    slots,
                    serial,
                    lambda item: not bool(item.get("upload_finished")),
                    group=group,
                    fallback_to_last=False,
                )
                if slot:
                    slot["upload_started"] = True
                    slot["upload_finished"] = True
                    slot["upload_success"] = success
                    slot["detail"] = detail
                    slot["stage"] = "上传成功" if success else "上传失败"
                continue

    slot_list = sorted(slots.values(), key=_slot_sort_key)
    summary_group = job_context.get("group") or " / ".join(
        dict.fromkeys(_normalize_group(item.get("group") or "") for item in slot_list if _normalize_group(item.get("group") or ""))
    )
    windows = job_context.get("windows") or sorted({str(item.get("serial")) for item in slot_list if item.get("serial")})
    videos_per_window = int(job_context.get("videos_per_window") or 0) or max((int(item.get("slot_total") or 0) for item in slot_list), default=0)
    for item in slot_list:
        if item.get("upload_finished"):
            item["stage"] = "上传成功" if item.get("upload_success") else "上传失败"
        elif item.get("upload_started"):
            item["stage"] = "上传中"
        elif int(item.get("render_progress") or 0) < 100 and int(item.get("duration_seconds") or 0) > 0:
            item["stage"] = "渲染中"
        elif item.get("metadata_ready"):
            item["stage"] = "文案完成，等待上传"
        else:
            item["stage"] = "等待开始"
    counts = {
        "total_slots": len(slot_list),
        "metadata_ready": sum(1 for item in slot_list if item.get("metadata_ready")),
        "upload_started": sum(1 for item in slot_list if item.get("upload_started")),
        "upload_success": sum(1 for item in slot_list if item.get("upload_success") is True),
        "upload_failed": sum(1 for item in slot_list if item.get("upload_success") is False),
    }
    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "log_path": str(log_path),
        "summary": {
            "group": summary_group,
            "windows": windows,
            "videos_per_window": videos_per_window,
        },
        "counts": counts,
        "slots": slot_list,
        "recent_lines": recent,
    }


def _render_html(snapshot: dict[str, Any]) -> str:
    summary = snapshot.get("summary", {})
    counts = snapshot.get("counts", {})
    title = str(summary.get("group") or Path(str(snapshot.get("log_path") or "任务")).stem or "任务").strip() or "任务"
    rows: list[str] = []
    for item in snapshot.get("slots", []):
        rows.append(
            "<tr>"
            f"<td>{item.get('serial')}</td>"
            f"<td>{item.get('slot_index')}/{item.get('slot_total')}</td>"
            f"<td>{html.escape(str(item.get('output') or '-'))}</td>"
            f"<td>{html.escape(str(item.get('stage') or ''))}</td>"
            f"<td>{item.get('render_progress', 0)}%</td>"
            f"<td>{item.get('upload_progress', 0)}%</td>"
            f"<td>{item.get('elapsed_seconds', 0)} / {item.get('duration_seconds', 0)}s</td>"
            f"<td>{'是' if item.get('metadata_prompt_started') else '否'}</td>"
            f"<td>{'是' if item.get('metadata_ready') else '否'}</td>"
            f"<td>{'是' if item.get('upload_started') else '否'}</td>"
            f"<td>{html.escape(str(item.get('api') or ''))}</td>"
            f"<td>{html.escape(str(item.get('template') or ''))}</td>"
            f"<td>{html.escape(str(item.get('detail') or ''))}</td>"
            "</tr>"
        )

    recent_lines = "".join(f"<li>{html.escape(line)}</li>" for line in snapshot.get("recent_lines", []))
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>{html.escape(title)} 实时进度</title>
  <style>
    body {{ font-family: 'Microsoft YaHei UI', 'PingFang SC', sans-serif; background:#111827; color:#f3f4f6; margin:0; padding:24px; }}
    h1,h2 {{ margin:0 0 12px 0; }}
    .grid {{ display:grid; grid-template-columns: repeat(5, minmax(180px, 1fr)); gap:12px; margin:16px 0 24px; }}
    .card {{ background:#1f2937; border:1px solid #374151; border-radius:12px; padding:14px; }}
    .label {{ color:#93c5fd; font-size:13px; margin-bottom:6px; }}
    .value {{ font-size:20px; font-weight:700; }}
    table {{ width:100%; border-collapse:collapse; background:#1f2937; border-radius:12px; overflow:hidden; }}
    th, td {{ border-bottom:1px solid #374151; padding:10px 12px; text-align:left; font-size:14px; }}
    th {{ background:#0f172a; color:#93c5fd; }}
    ul {{ margin:0; padding-left:20px; }}
    .path {{ color:#93c5fd; word-break:break-all; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)} 实时进度</h1>
  <div>分组：<strong>{html.escape(str(summary.get('group') or ''))}</strong> | 窗口：<strong>{html.escape(', '.join(summary.get('windows') or []))}</strong> | 每窗口：<strong>{summary.get('videos_per_window', 0)}</strong> | 更新时间：<strong>{html.escape(str(snapshot.get('updated_at') or ''))}</strong></div>
  <div class="path">日志：{html.escape(str(snapshot.get('log_path') or ''))}</div>
  <div class="grid">
    <div class="card"><div class="label">总视频槽位</div><div class="value">{counts.get('total_slots', 0)}</div></div>
    <div class="card"><div class="label">文案已完成</div><div class="value">{counts.get('metadata_ready', 0)}</div></div>
    <div class="card"><div class="label">已开始上传</div><div class="value">{counts.get('upload_started', 0)}</div></div>
    <div class="card"><div class="label">上传成功</div><div class="value">{counts.get('upload_success', 0)}</div></div>
    <div class="card"><div class="label">上传失败</div><div class="value">{counts.get('upload_failed', 0)}</div></div>
  </div>
  <h2>逐条状态</h2>
  <table>
    <thead>
      <tr>
        <th>窗口</th>
        <th>槽位</th>
        <th>输出文件</th>
        <th>当前阶段</th>
        <th>渲染进度</th>
        <th>上传进度</th>
        <th>耗时</th>
        <th>文案已启动</th>
        <th>文案已完成</th>
        <th>上传已启动</th>
        <th>API</th>
        <th>模板</th>
        <th>详情</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
  <h2 style="margin-top:24px;">最近日志</h2>
  <div class="card"><ul>{recent_lines}</ul></div>
</body>
</html>
"""


def write_outputs(snapshot: dict[str, Any], output_json: Path, output_html: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    output_html.write_text(_render_html(snapshot), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="生成运行日志实时进度看板")
    parser.add_argument("--log", required=True, help="运行日志文件")
    parser.add_argument("--json", required=True, help="输出 JSON 文件")
    parser.add_argument("--html", required=True, help="输出 HTML 文件")
    parser.add_argument("--job", help="任务 JSON 文件，用于补充分组/API/模板信息")
    parser.add_argument("--interval", type=int, default=5, help="刷新间隔秒数")
    parser.add_argument("--once", action="store_true", help="只生成一次后退出")
    args = parser.parse_args()

    log_path = Path(args.log)
    output_json = Path(args.json)
    output_html = Path(args.html)
    job_path = Path(args.job) if args.job else None
    interval = max(1, int(args.interval or 5))

    while True:
        snapshot = build_snapshot(log_path, job_path=job_path)
        write_outputs(snapshot, output_json, output_html)
        if args.once:
            return 0
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
