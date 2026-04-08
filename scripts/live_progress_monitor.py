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

# 兼容当前项目日志里历史遗留的乱码前缀，只提取稳定结构字段。
TASK_LINE_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2} \[INFO\] \[[^\]]+\] (?P<group>.+?)/(?P<serial>\d+)\[(?P<slot>\d+)/(?P<total>\d+)\]: .* -> (?P<output>\S+)$"
)
RENDER_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2} \[INFO\] \[[^\]]+\] (?P<output>\S+) .* (?P<pct>\d+)% \((?P<elapsed>\d+)/(?P<duration>\d+)s\)$"
)
METADATA_PROMPT_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2} \[INFO\] \[[^\]]+\] (?P<group>.+?)/(?P<serial>\d+): API=(?P<api>.+?) \| .*?=(?P<template>.+?) \| seed="
)
METADATA_READY_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2} \[INFO\] \[[^\]]+\] (?P<group>.+?) .* (?P<serial>\d+) .*: (?P<title>.*)$"
)
UPLOAD_START_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2} \[INFO\] \[[^\]]+\] (?P<group>.+?) .* (?P<serial>\d+) .*$"
)
UPLOAD_FINISH_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2} \[INFO\] \[[^\]]+\] (?P<group>.+?) .* (?P<serial>\d+) .* \((?P<count>\d+)/(?P<total>\d+)\) (?P<detail>.*)$"
)


def _load_lines(log_path: Path) -> list[str]:
    if not log_path.exists():
        return []
    return log_path.read_text(encoding="utf-8", errors="replace").splitlines()


def _slot_sort_key(item: dict[str, Any]) -> tuple[int, int]:
    return int(item.get("serial") or 0), int(item.get("slot_index") or 0)


def _recent_lines(lines: list[str], limit: int = 30) -> list[str]:
    return [str(line or "").rstrip() for line in lines if str(line or "").strip()][-limit:]


def _load_job_context(job_path: Path | None) -> dict[str, Any]:
    if not job_path or not job_path.exists():
        return {}
    try:
        payload = json.loads(job_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    jobs = payload.get("jobs") or []
    if not jobs:
        return {}
    job = jobs[0]
    return {
        "group": str(job.get("group_tag") or "").strip(),
        "windows": [str(item) for item in (job.get("window_serials") or []) if str(item).strip()],
        "videos_per_window": int(job.get("videos_per_window") or 0),
        "api": str(job.get("api_template") or "").strip(),
        "template": str(job.get("prompt_template") or "").strip(),
    }


def build_snapshot(log_path: Path, job_path: Path | None = None) -> dict[str, Any]:
    lines = _load_lines(log_path)
    slots: dict[str, dict[str, Any]] = {}
    recent = _recent_lines(lines, limit=30)
    job_context = _load_job_context(job_path)

    for line in lines:
        line = str(line or "").rstrip()
        if not line:
            continue

        task_match = TASK_LINE_RE.match(line)
        if task_match:
            output_name = task_match.group("output")
            slots[output_name] = {
                "output": output_name,
                "group": task_match.group("group"),
                "serial": int(task_match.group("serial")),
                "slot_index": int(task_match.group("slot")),
                "slot_total": int(task_match.group("total")),
                "stage": "渲染中",
                "render_progress": 0,
                "elapsed_seconds": 0,
                "duration_seconds": 0,
                "metadata_prompt_started": False,
                "metadata_ready": False,
                "upload_started": False,
                "upload_finished": False,
                "upload_success": None,
                "detail": "",
                "api": "",
                "template": "",
                "title": "",
            }
            continue

        render_match = RENDER_RE.match(line)
        if render_match:
            output_name = render_match.group("output")
            slot = slots.setdefault(
                output_name,
                {
                    "output": output_name,
                    "group": "",
                    "serial": 0,
                    "slot_index": 0,
                    "slot_total": 0,
                    "stage": "渲染中",
                    "render_progress": 0,
                    "elapsed_seconds": 0,
                    "duration_seconds": 0,
                    "metadata_prompt_started": False,
                    "metadata_ready": False,
                    "upload_started": False,
                    "upload_finished": False,
                    "upload_success": None,
                    "detail": "",
                    "api": "",
                    "template": "",
                    "title": "",
                },
            )
            slot["stage"] = "渲染中"
            slot["render_progress"] = int(render_match.group("pct"))
            slot["elapsed_seconds"] = int(render_match.group("elapsed"))
            slot["duration_seconds"] = int(render_match.group("duration"))
            continue

        meta_match = METADATA_PROMPT_RE.match(line)
        if meta_match:
            serial = int(meta_match.group("serial"))
            for slot in slots.values():
                if int(slot.get("serial") or 0) == serial:
                    slot["metadata_prompt_started"] = True
                    slot["api"] = meta_match.group("api")
                    slot["template"] = meta_match.group("template")
            continue

        if "标题:" in line:
            meta_ready_match = METADATA_READY_RE.match(line)
            if meta_ready_match:
                serial = int(meta_ready_match.group("serial"))
                title = meta_ready_match.group("title")
                for slot in slots.values():
                    if int(slot.get("serial") or 0) == serial:
                        slot["metadata_ready"] = True
                        slot["title"] = title
                        if not slot["upload_started"]:
                            slot["stage"] = "文案完成，等待上传"
                continue

        if "开始上传" in line:
            upload_start_match = UPLOAD_START_RE.match(line)
            if upload_start_match:
                serial = int(upload_start_match.group("serial"))
                for slot in slots.values():
                    if int(slot.get("serial") or 0) == serial and not slot["upload_finished"]:
                        slot["upload_started"] = True
                        slot["stage"] = "上传中"
                continue

        if "[上传]" in line and ("✓" in line or "✗" in line):
            upload_finish_match = UPLOAD_FINISH_RE.match(line)
            if upload_finish_match:
                serial = int(upload_finish_match.group("serial"))
                success = "✓" in line
                detail = upload_finish_match.group("detail")
                for slot in slots.values():
                    if int(slot.get("serial") or 0) == serial and not slot["upload_finished"]:
                        slot["upload_finished"] = True
                        slot["upload_success"] = success
                        slot["detail"] = detail
                        slot["stage"] = "上传成功" if success else "上传失败"
                continue

    slot_list = sorted(slots.values(), key=_slot_sort_key)
    summary_group = job_context.get("group") or next((str(item.get("group") or "") for item in slot_list if item.get("group")), "")
    windows = job_context.get("windows") or sorted({str(item.get("serial")) for item in slot_list if item.get("serial")})
    videos_per_window = int(job_context.get("videos_per_window") or 0) or max((int(item.get("slot_total") or 0) for item in slot_list), default=0)
    default_api = str(job_context.get("api") or "")
    default_template = str(job_context.get("template") or "")
    for item in slot_list:
        if default_api:
            item["api"] = default_api
        if default_template:
            item["template"] = default_template
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
    rows: list[str] = []
    for item in snapshot.get("slots", []):
        rows.append(
            "<tr>"
            f"<td>{item.get('serial')}</td>"
            f"<td>{item.get('slot_index')}/{item.get('slot_total')}</td>"
            f"<td>{html.escape(str(item.get('output') or ''))}</td>"
            f"<td>{html.escape(str(item.get('stage') or ''))}</td>"
            f"<td>{item.get('render_progress', 0)}%</td>"
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
  <title>0408 MegaBass 实时进度</title>
  <style>
    body {{ font-family: 'Microsoft YaHei UI', 'PingFang SC', sans-serif; background:#111827; color:#f3f4f6; margin:0; padding:24px; }}
    h1,h2 {{ margin:0 0 12px 0; }}
    .grid {{ display:grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap:12px; margin:16px 0 24px; }}
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
  <h1>0408 MegaBass 实时进度</h1>
  <div>分组：<strong>{html.escape(str(summary.get('group') or ''))}</strong> | 窗口：<strong>{html.escape(', '.join(summary.get('windows') or []))}</strong> | 每窗口：<strong>{summary.get('videos_per_window', 0)}</strong> | 更新时间：<strong>{html.escape(str(snapshot.get('updated_at') or ''))}</strong></div>
  <div class="path">日志：{html.escape(str(snapshot.get('log_path') or ''))}</div>
  <div class="grid">
    <div class="card"><div class="label">总视频槽位</div><div class="value">{counts.get('total_slots', 0)}</div></div>
    <div class="card"><div class="label">文案已完成</div><div class="value">{counts.get('metadata_ready', 0)}</div></div>
    <div class="card"><div class="label">已开始上传</div><div class="value">{counts.get('upload_started', 0)}</div></div>
    <div class="card"><div class="label">上传成功</div><div class="value">{counts.get('upload_success', 0)}</div></div>
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
