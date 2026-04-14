# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from batch_upload import upload_single_window

GROUP_TAG = "挪威二测"
SOURCE_ROOT = Path(r"F:\挪威\0411")
FAILED_SLOTS: list[tuple[int, int]] = [
    (22, 1),
    (100, 1),
    (100, 2),
    (115, 1),
    (115, 2),
    (131, 1),
]
LOG_DIR = BASE_DIR / "logs"
REPORT_DIR = BASE_DIR / "data" / "repair_reports"


class FileLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def __call__(self, message: str) -> None:
        line = f"{datetime.now():%H:%M:%S} {message}"
        print(line)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def _slot_dir(serial: int, slot_index: int) -> Path:
    return SOURCE_ROOT / f"0411_{int(serial)}_{int(slot_index):02d}"


def _manifest_path(serial: int, slot_index: int) -> Path:
    return _slot_dir(serial, slot_index) / "upload_manifest.json"


def _load_manifest_channel(serial: int, slot_index: int) -> dict[str, Any]:
    manifest_path = _manifest_path(serial, slot_index)
    if not manifest_path.exists():
        raise FileNotFoundError(f"缺少 upload_manifest.json: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    channels = payload.get("channels") if isinstance(payload, dict) else {}
    if not isinstance(channels, dict):
        raise RuntimeError(f"manifest 格式错误: {manifest_path}")
    channel_data = channels.get(str(int(serial)))
    if not isinstance(channel_data, dict):
        raise RuntimeError(f"manifest 中缺少 serial={serial} 的频道数据: {manifest_path}")
    data = dict(channel_data)
    if "title" not in data or "description" not in data:
        raise RuntimeError(f"manifest 缺少标题或简介: {manifest_path}")
    return data


async def _retry_slot(serial: int, slot_index: int, logger: FileLogger) -> dict[str, Any]:
    slot_dir = _slot_dir(serial, slot_index)
    metadata_dict = _load_manifest_channel(serial, slot_index)
    upload_options = dict(metadata_dict.get("upload_options") or {})
    total_slots = int(metadata_dict.get("total_slots") or 2)
    logger(
        f"[Retry] serial={serial} slot={slot_index}/{total_slots} "
        f"title={str(metadata_dict.get('title') or '')[:48]}"
    )
    result = await upload_single_window(
        serial_number=int(serial),
        metadata_dict=metadata_dict,
        source_dir=str(slot_dir),
        upload_options=upload_options,
        group_tag=GROUP_TAG,
        has_prepare_step=False,
        slot_index=int(slot_index),
        total_slots=int(total_slots),
    )
    logger(
        f"[Retry] serial={serial} slot={slot_index} "
        f"success={bool(result.get('success'))} stage={result.get('stage')} "
        f"detail={result.get('detail') or result.get('reason') or ''}"
    )
    return result


async def main() -> int:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"retry_0411_failed_uploads_{timestamp}.log"
    report_path = REPORT_DIR / f"retry_0411_failed_uploads_{timestamp}.json"
    logger = FileLogger(log_path)
    results: list[dict[str, Any]] = []

    logger("[Start] retry_0411_failed_uploads")
    for serial, slot_index in FAILED_SLOTS:
        try:
            result = await _retry_slot(serial, slot_index, logger)
        except Exception as exc:
            logger(f"[ERR] serial={serial} slot={slot_index} -> {exc}")
            logger(traceback.format_exc().rstrip())
            result = {
                "group_tag": GROUP_TAG,
                "serial": int(serial),
                "slot_index": int(slot_index),
                "success": False,
                "stage": "retry_exception",
                "detail": str(exc),
                "video_path": str(_slot_dir(serial, slot_index) / f"0411_{int(serial)}_{int(slot_index):02d}.mp4"),
            }
        results.append(result)

    payload = {
        "timestamp": datetime.now().isoformat(),
        "group_tag": GROUP_TAG,
        "source_dir": str(SOURCE_ROOT),
        "results": results,
        "success_count": sum(1 for item in results if item.get("success")),
        "failed_count": sum(1 for item in results if not item.get("success")),
        "log_path": str(log_path),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger(f"[Done] report -> {report_path}")
    return 0 if payload["failed_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
