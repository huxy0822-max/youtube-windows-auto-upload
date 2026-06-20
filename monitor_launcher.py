# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).parent
MONITOR_ROOT = SCRIPT_DIR / "workspace" / "live_monitors"
MONITOR_SCRIPT = SCRIPT_DIR / "scripts" / "live_progress_monitor.py"


def _safe_name(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return text.strip("._-") or "monitor"


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_pid(pid_path: Path) -> int:
    try:
        return int((pid_path.read_text(encoding="utf-8").strip() or "0"))
    except Exception:
        return 0


def _open_in_browser(path: Path) -> None:
    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif os.name == "nt":
            os.startfile(str(path))  # type: ignore[attr-defined]
        else:
            subprocess.Popen(["xdg-open", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        return


def build_monitor_payload_from_queue(queue: Any) -> dict[str, Any]:
    jobs = getattr(queue, "jobs", []) or []
    groups: list[str] = []
    windows: list[str] = []
    slots: list[dict[str, Any]] = []
    max_slots = 0

    for job in jobs:
        group_tag = str(getattr(job, "group_tag", "") or "").strip()
        if group_tag and group_tag not in groups:
            groups.append(group_tag)
        window_serials = [int(item) for item in (getattr(job, "window_serials", []) or [])]
        videos_per_window = max(1, int(getattr(job, "videos_per_window", 1) or 1))
        max_slots = max(max_slots, videos_per_window)
        api_name = str(getattr(job, "api_template", "") or "").strip()
        template_name = str(getattr(job, "prompt_template", "") or "").strip()
        for serial in window_serials:
            serial_text = str(serial)
            if serial_text not in windows:
                windows.append(serial_text)
            for slot_index in range(1, videos_per_window + 1):
                slots.append(
                    {
                        "group": group_tag,
                        "serial": int(serial),
                        "slot_index": slot_index,
                        "slot_total": videos_per_window,
                        "api": api_name,
                        "template": template_name,
                    }
                )

    return {
        "summary": {
            "group": " / ".join(groups),
            "windows": windows,
            "videos_per_window": max_slots,
        },
        "slots": slots,
    }


def build_monitor_payload_from_run_plan(run_plan: Any) -> dict[str, Any]:
    tasks = getattr(run_plan, "tasks", []) or []
    groups: list[str] = []
    windows: list[str] = []
    slots: list[dict[str, Any]] = []
    max_slots = 0

    for task in tasks:
        group_tag = str(getattr(task, "tag", "") or "").strip()
        serial = int(getattr(task, "serial", 0) or 0)
        slot_index = max(1, int(getattr(task, "slot_index", 1) or 1))
        slot_total = max(1, int(getattr(task, "total_slots", 1) or 1))
        if group_tag and group_tag not in groups:
            groups.append(group_tag)
        if serial > 0 and str(serial) not in windows:
            windows.append(str(serial))
        max_slots = max(max_slots, slot_total)
        slots.append(
            {
                "group": group_tag,
                "serial": serial,
                "slot_index": slot_index,
                "slot_total": slot_total,
            }
        )

    return {
        "summary": {
            "group": " / ".join(groups),
            "windows": windows,
            "videos_per_window": max_slots,
        },
        "slots": slots,
    }


def launch_live_monitor(
    log_path: str | Path,
    payload: dict[str, Any] | None = None,
    *,
    run_name: str = "",
    refresh_interval: int = 5,
    open_browser: bool = True,
) -> dict[str, str]:
    log_file = Path(log_path)
    monitor_name = _safe_name(run_name or log_file.stem)
    monitor_dir = MONITOR_ROOT / monitor_name
    monitor_dir.mkdir(parents=True, exist_ok=True)

    json_path = monitor_dir / "live_progress.json"
    html_path = monitor_dir / "live_progress.html"
    job_path = monitor_dir / "job.json"
    pid_path = monitor_dir / "monitor.pid"
    out_path = monitor_dir / "monitor.out"

    if payload:
        job_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    elif job_path.exists():
        job_path.unlink()

    base_cmd = [
        sys.executable,
        str(MONITOR_SCRIPT),
        "--log",
        str(log_file),
        "--json",
        str(json_path),
        "--html",
        str(html_path),
    ]
    if payload:
        base_cmd.extend(["--job", str(job_path)])

    subprocess.run(
        [*base_cmd, "--once"],
        cwd=str(SCRIPT_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    existing_pid = _read_pid(pid_path)
    if not _pid_is_running(existing_pid):
        cmd = [*base_cmd, "--interval", str(max(1, int(refresh_interval or 5)))]
        with out_path.open("ab") as stream:
            proc = subprocess.Popen(
                cmd,
                cwd=str(SCRIPT_DIR),
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        pid_path.write_text(str(proc.pid), encoding="utf-8")

    if open_browser:
        _open_in_browser(html_path)

    return {
        "log_path": str(log_file),
        "json_path": str(json_path),
        "html_path": str(html_path),
        "job_path": str(job_path) if payload else "",
        "monitor_dir": str(monitor_dir),
    }
