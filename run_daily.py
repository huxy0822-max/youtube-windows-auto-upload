# -*- coding: utf-8 -*-
"""
CLI 入口 —— 绕过 GUI，直接调用核心管道。

用法:
    python run_daily.py                         # 使用 daily_job.json
    python run_daily.py -f my_job.json          # 指定配置文件
    python run_daily.py --gen                   # 交互式生成配置文件

配置文件由 Claude 帮你生成，你只需要告诉他：
  "全球电影，窗口 1,28,48,68,82，每个3个视频"
然后把生成的 JSON 保存为 daily_job.json，运行本脚本即可。

功能完全等价于 GUI 的"开始运行"按钮：
  ✅ 渲染 + 生成标题/简介/标签/缩略图 并行
  ✅ 渲染完一个立即上传（流式上传）
  ✅ 多 GPU/CPU 混合渲染
  ✅ 素材用完自动移到已用文件夹
  ✅ 已渲染视频自动跳过
  ✅ 完整日志写入文件
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# ── 保证编码 ──
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

SCRIPT_DIR = Path(__file__).parent
DEFAULT_JOB_FILE = SCRIPT_DIR / "daily_job.json"
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── 日志配置 ──
def _setup_logging() -> logging.Logger:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"run_{timestamp}.log"
    logger = logging.getLogger("run_daily")
    logger.setLevel(logging.DEBUG)
    # 文件 handler —— 记录一切
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(fh)
    # 控制台 handler —— 只显示 INFO+
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(ch)
    logger.info(f"日志文件: {log_file}")
    return logger


def _log_func(logger: logging.Logger):
    """返回一个 log(message) 函数给管道使用"""
    def _log(message: str) -> None:
        clean = str(message or "").strip()
        if not clean:
            return
        if "[错误]" in clean or "EXCEPTION" in clean or "FAILED" in clean:
            logger.error(clean)
        elif "[警告]" in clean or "WARN" in clean:
            logger.warning(clean)
        else:
            logger.info(clean)
    return _log


# ══════════════════════════════════════════════════════════════════
# 配置文件格式
# ══════════════════════════════════════════════════════════════════
EXAMPLE_JOB = {
    "_注释": "这是一个示例配置，由 Claude 帮你生成",
    "jobs": [
        {
            "group_tag": "全球电影",
            "window_serials": [1, 28, 48, 68, 82],
            "videos_per_window": 3,
            "path_template": "0406全球",
            "visual_mode": "random",
            "modules": ["metadata", "render", "upload"],
            "visibility": "private",
            "category": "Music",
        }
    ],
}


def _load_job_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"配置文件不存在: {path}\n"
            f"请先让 Claude 帮你生成，或运行: python run_daily.py --gen"
        )
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "jobs" not in raw:
        raise ValueError(f"配置文件格式错误，需要包含 'jobs' 字段: {path}")
    return raw


def _generate_example_config(path: Path) -> None:
    """交互式生成配置文件"""
    if path.exists():
        print(f"⚠ 配置文件已存在: {path}")
        resp = input("是否覆盖? (y/N): ").strip().lower()
        if resp != "y":
            print("已取消")
            return

    path.write_text(
        json.dumps(EXAMPLE_JOB, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"✅ 示例配置已写入: {path}")
    print(f"请编辑该文件，或让 Claude 帮你生成配置内容。")


# ══════════════════════════════════════════════════════════════════
# 核心：构建 RunQueue 并执行
# ══════════════════════════════════════════════════════════════════
def _build_run_queue(job_config: dict[str, Any], log: Any) -> tuple:
    """从配置构建 RunQueue + UploadDefaults"""
    from run_queue import GroupJob, RunQueue, UploadDefaults
    from path_templates import get_path_template, load_path_templates, resolve_source_dir

    path_templates = load_path_templates()
    queue = RunQueue()

    for idx, job_spec in enumerate(job_config.get("jobs", []), 1):
        group_tag = str(job_spec.get("group_tag") or "").strip()
        if not group_tag:
            log(f"[警告] 第 {idx} 个任务缺少 group_tag，已跳过")
            continue

        window_serials = [int(s) for s in job_spec.get("window_serials", [])]
        if not window_serials:
            log(f"[警告] {group_tag}: 没有指定窗口号，已跳过")
            continue

        videos_per_window = max(1, int(job_spec.get("videos_per_window", 1)))
        path_template_name = str(job_spec.get("path_template") or "默认路径").strip()
        _, path_template = get_path_template(path_template_name, templates=path_templates)
        source_dir = str(job_spec.get("source_dir") or "").strip()
        if not source_dir:
            source_dir = resolve_source_dir(path_template, group_tag=group_tag)

        # 模块选择
        modules = job_spec.get("modules") or ["metadata", "render", "upload"]

        # 上传设置
        visibility = str(job_spec.get("visibility") or "private").strip()
        category = str(job_spec.get("category") or "Music").strip()
        schedule_date = str(job_spec.get("schedule_date") or "").strip() or None
        schedule_time = str(job_spec.get("schedule_time") or "").strip() or None

        upload_defaults = UploadDefaults(
            visibility=visibility,
            category=category,
            is_for_kids=bool(job_spec.get("is_for_kids", False)),
            ai_content=str(job_spec.get("ai_content") or "yes"),
            altered_content=str(job_spec.get("altered_content") or "yes"),
            notify_subscribers=str(job_spec.get("notify_subscribers") or "no"),
            schedule_date=schedule_date,
            schedule_time=schedule_time,
            timezone=str(job_spec.get("timezone") or "Asia/Taipei"),
            auto_close_after=bool(job_spec.get("auto_close_after", False)),
        )

        # 视觉设置
        visual_mode = str(job_spec.get("visual_mode") or "random").strip()
        visual_settings = None
        if visual_mode not in ("random", "manual"):
            # 视为 preset 名称，从配置加载
            from workflow_core import _load_visual_presets
            presets = _load_visual_presets()
            if visual_mode in presets:
                visual_settings = dict(presets[visual_mode])
            else:
                log(f"[警告] {group_tag}: 视觉预设 '{visual_mode}' 不存在，使用 random")
                visual_mode = "random"
        elif visual_mode == "manual":
            visual_settings = dict(job_spec.get("visual_settings") or {})

        job = GroupJob(
            group_tag=group_tag,
            window_serials=window_serials,
            source_dir=source_dir,
            visual_mode=visual_mode,
            visual_settings=visual_settings,
            prompt_template=str(job_spec.get("prompt_template") or "default"),
            api_template=str(job_spec.get("api_template") or "default"),
            path_template=path_template_name,
            videos_per_window=videos_per_window,
            upload_defaults=upload_defaults,
            modules=modules,
            browser_provider=str(job_spec.get("browser_provider") or "auto"),
        )
        queue.add_job(job)
        log(
            f"[任务 {idx}] {group_tag} | "
            f"窗口={window_serials} | "
            f"每窗口={videos_per_window}个 | "
            f"模块={modules} | "
            f"素材={source_dir}"
        )

    if queue.is_empty():
        raise ValueError("配置文件中没有有效任务")

    # 全局默认值
    global_defaults = UploadDefaults(
        visibility=str(job_config.get("default_visibility") or "private"),
        category=str(job_config.get("default_category") or "Music"),
        timezone=str(job_config.get("default_timezone") or "Asia/Taipei"),
    )

    return queue, global_defaults


def _build_run_plan_for_job(job, log):
    """
    独立版本的 build_run_plan_for_job —— 不依赖 GUI。
    等价于 dashboard_app._patched_build_run_plan_for_job_v2。
    """
    from workflow_core import (
        WorkflowDefaults,
        WindowInfo,
        create_task,
        load_prompt_settings,
        ensure_prompt_presets,
        _load_visual_presets,
        load_channel_name_map,
        CHANNEL_MAPPING_FILE,
        PROMPT_STUDIO_FILE,
    )
    from run_plan_service import build_run_plan, build_module_selection
    from path_templates import get_path_template, load_path_templates, build_runtime_config
    from workflow_core import load_scheduler_settings, SCHEDULER_CONFIG_FILE

    # ── 1. 构建 runtime config ──
    path_templates = load_path_templates()
    template_name, template = get_path_template(job.path_template, templates=path_templates)
    base_config = load_scheduler_settings(SCHEDULER_CONFIG_FILE)
    runtime_config = build_runtime_config(
        base_config, template,
        template_name=template_name,
        source_dir=str(job.source_dir or "").strip(),
    )

    # ── 2. 应用提示词绑定 ──
    prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
    api_presets = prompt_config.get("apiPresets") or {}
    content_templates = prompt_config.get("contentTemplates") or {}
    tag_api_bindings = prompt_config.get("tagApiBindings") or {}
    tag_bindings = prompt_config.get("tagBindings") or {}

    api_name = tag_api_bindings.get(job.group_tag, "默认API模板")
    content_name = tag_bindings.get(job.group_tag, "默认内容模板")

    requested_api = str(job.api_template or getattr(job, "api_preset", "") or "").strip()
    requested_content = str(job.prompt_template or "").strip()
    if requested_api and requested_api != "default" and requested_api in api_presets:
        api_name = requested_api
    if requested_content and requested_content != "default" and requested_content in content_templates:
        content_name = requested_content

    api_payload = dict(api_presets.get(api_name) or {})
    content_payload = dict(content_templates.get(content_name) or {})
    if api_payload or content_payload:
        ensure_prompt_presets(
            api_name=api_name,
            api_payload=api_payload,
            content_name=content_name,
            content_payload=content_payload,
            tag=job.group_tag,
            path=PROMPT_STUDIO_FILE,
        )
    log(f"[提示词] {job.group_tag}: API={api_name} | 内容模板={content_name}")

    # ── 3. 查询频道信息 ──
    channel_name_map = load_channel_name_map(CHANNEL_MAPPING_FILE)
    upload_config_path = SCRIPT_DIR / "config" / "upload_config.json"
    ypp_serials: set[int] = set()
    if upload_config_path.exists():
        try:
            uc = json.loads(upload_config_path.read_text(encoding="utf-8"))
            tag_info = (uc.get("tag_to_project") or {}).get(job.group_tag) or {}
            ypp_serials = set(int(s) for s in tag_info.get("ypp_serials", []))
        except Exception:
            pass

    # ── 4. 构建 WindowTask 列表 ──
    tasks = []
    upload_defaults = job.upload_defaults
    for serial in job.window_serials:
        is_ypp = int(serial) in ypp_serials
        tasks.append(
            create_task(
                tag=job.group_tag,
                serial=int(serial),
                quantity=max(1, int(job.videos_per_window or 1)),
                is_ypp=is_ypp,
                title="",
                visibility=str(upload_defaults.visibility or "private"),
                category=str(upload_defaults.category or "Music"),
                made_for_kids=bool(upload_defaults.is_for_kids),
                altered_content=True,
                notify_subscribers=False,
                source_dir=str(job.source_dir or "").strip(),
                channel_name=channel_name_map.get(int(serial), ""),
            )
        )

    # ── 5. 构建 WorkflowDefaults ──
    has_metadata = "metadata" in job.modules
    has_render = "render" in job.modules
    has_upload = "upload" in job.modules
    today_mmdd = datetime.now().strftime("%m%d")

    # 解析视觉设置
    visual_settings: dict[str, Any] = {}
    visual_mode = str(job.visual_mode or "random").strip()
    if visual_mode == "random":
        visual_settings["visual_mode"] = "random"
        visual_settings["preset"] = "none"
        for key in ("zoom", "style", "color_spectrum", "color_timeline",
                     "color_tint", "particle", "text_font", "text_pos", "text_style"):
            visual_settings[key] = "random"
    elif visual_mode == "manual":
        visual_settings = dict(job.visual_settings or {})
        visual_settings["visual_mode"] = "manual"
        visual_settings["preset"] = "none"
    else:
        # 预设模式
        presets = _load_visual_presets()
        if visual_mode in presets:
            visual_settings = dict(presets[visual_mode])
        visual_settings["visual_mode"] = visual_mode
        visual_settings["preset"] = visual_mode
        if job.visual_settings:
            visual_settings.update(dict(job.visual_settings))

    defaults = WorkflowDefaults(
        date_mmdd=today_mmdd,
        visibility=str(upload_defaults.visibility or "private"),
        category=str(upload_defaults.category or "Music"),
        made_for_kids=bool(upload_defaults.is_for_kids),
        altered_content=True,
        notify_subscribers=False,
        schedule_enabled=False,
        schedule_start="",
        schedule_interval_minutes=60,
        schedule_timezone=str(upload_defaults.timezone or "Asia/Taipei"),
        metadata_mode="prompt_api",
        generate_text=has_metadata,
        generate_thumbnails=has_metadata,
        sync_daily_content=has_metadata,
        randomize_effects=False,
        visual_settings=visual_settings,
    )

    # ── 6. 构建 ModuleSelection ──
    modules = build_module_selection(
        metadata=has_metadata,
        render=has_render,
        upload=has_upload,
    )

    # ── 7. 调用 build_run_plan ──
    return build_run_plan(
        tasks=tasks,
        defaults=defaults,
        modules=modules,
        config=runtime_config,
    )


# ══════════════════════════════════════════════════════════════════
# 主执行流程
# ══════════════════════════════════════════════════════════════════
def _run(job_file: Path) -> int:
    logger = _setup_logging()
    log = _log_func(logger)

    log("=" * 60)
    log("🎬 CLI 管道启动")
    log(f"配置文件: {job_file}")
    log(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    try:
        job_config = _load_job_config(job_file)
    except Exception as exc:
        log(f"[错误] 加载配置失败: {exc}")
        return 1

    try:
        queue, global_defaults = _build_run_queue(job_config, log)
    except Exception as exc:
        log(f"[错误] 构建任务队列失败: {exc}")
        import traceback
        log(traceback.format_exc())
        return 1

    # 统计
    total_windows = sum(len(j.window_serials) for j in queue.jobs)
    total_videos = sum(len(j.window_serials) * max(1, j.videos_per_window) for j in queue.jobs)
    log(f"共 {len(queue.jobs)} 个分组 | {total_windows} 个窗口 | 预计 {total_videos} 个视频")

    # 进度回调
    start_time = time.time()
    _completed = {"renders": 0, "uploads": 0}

    def progress_callback(event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip()
        if event_type == "log":
            message = str(event.get("message") or "").strip()
            if message:
                log(message)
            return
        if event_type == "prepare_started":
            serial = event.get("window_serial", "?")
            tag = event.get("group_tag", "?")
            log(f"[进度] {tag} 窗口 {serial} 开始渲染/生成文案")
        elif event_type == "prepare_finished":
            _completed["renders"] += 1
            serial = event.get("window_serial", "?")
            tag = event.get("group_tag", "?")
            elapsed = time.time() - start_time
            log(f"[进度] {tag} 窗口 {serial} 渲染完成 ({_completed['renders']}/{total_windows}) | 已用 {elapsed/60:.0f}分")
        elif event_type == "window_started":
            serial = event.get("serial", "?")
            tag = event.get("group_tag", "?")
            log(f"[上传] {tag} 窗口 {serial} 开始上传")
        elif event_type == "window_finished":
            _completed["uploads"] += 1
            serial = event.get("serial", "?")
            tag = event.get("group_tag", "?")
            success = bool(event.get("success"))
            detail = str(event.get("detail") or "").strip()
            status = "✅" if success else "❌"
            log(f"[上传] {tag} 窗口 {serial} {status} ({_completed['uploads']}/{total_videos}) {detail}")
        elif event_type == "metadata_ready":
            serial = event.get("serial", "?")
            tag = event.get("group_tag", "?")
            title = str(event.get("title") or "").strip()[:40]
            log(f"[文案] {tag} 窗口 {serial} 标题: {title}")

    # 导入管道
    from run_plan_service import execute_run_queue
    from workflow_core import ExecutionControl

    control = ExecutionControl()

    def _build_plan_callback(queue_job):
        return _build_run_plan_for_job(queue_job, log)

    def _apply_prompt_bindings(queue_job):
        """在每个 job 开始前应用提示词绑定"""
        from workflow_core import load_prompt_settings, ensure_prompt_presets, PROMPT_STUDIO_FILE
        prompt_config = load_prompt_settings(PROMPT_STUDIO_FILE)
        api_presets = prompt_config.get("apiPresets") or {}
        content_templates = prompt_config.get("contentTemplates") or {}
        tag_api_bindings = prompt_config.get("tagApiBindings") or {}
        tag_bindings = prompt_config.get("tagBindings") or {}
        api_name = tag_api_bindings.get(queue_job.group_tag, "默认API模板")
        content_name = tag_bindings.get(queue_job.group_tag, "默认内容模板")
        requested_api = str(queue_job.api_template or "").strip()
        requested_content = str(queue_job.prompt_template or "").strip()
        if requested_api and requested_api != "default" and requested_api in api_presets:
            api_name = requested_api
        if requested_content and requested_content != "default" and requested_content in content_templates:
            content_name = requested_content
        api_payload = dict(api_presets.get(api_name) or {})
        content_payload = dict(content_templates.get(content_name) or {})
        if api_payload or content_payload:
            ensure_prompt_presets(
                api_name=api_name,
                api_payload=api_payload,
                content_name=content_name,
                content_payload=content_payload,
                tag=queue_job.group_tag,
                path=PROMPT_STUDIO_FILE,
            )

    # 执行
    log("[启动] 开始执行管道...")
    try:
        results = asyncio.run(
            execute_run_queue(
                queue,
                global_defaults,
                control=control,
                before_job_callback=_apply_prompt_bindings,
                build_run_plan_for_job=_build_plan_callback,
                progress_callback=progress_callback,
                log=log,
            )
        )
    except KeyboardInterrupt:
        log("[中断] 用户按 Ctrl+C 取消")
        return 130
    except Exception as exc:
        log(f"[错误] 管道执行失败: {exc}")
        import traceback
        log(traceback.format_exc())
        return 1

    # ── 汇总结果 ──
    elapsed = time.time() - start_time
    total_success = sum(r.get("success_count", 0) for r in results)
    total_failed = sum(r.get("failed_count", 0) for r in results)

    log("=" * 60)
    log(f"🏁 管道执行完成")
    log(f"耗时: {elapsed/60:.1f} 分钟")
    log(f"成功: {total_success} | 失败: {total_failed}")

    # 打印每个分组的详细结果
    for r in results:
        tag = r.get("group_tag", "?")
        s = r.get("success_count", 0)
        f = r.get("failed_count", 0)
        log(f"  {tag}: 成功={s} 失败={f}")
        for item in r.get("results", []):
            if not item.get("success"):
                detail = item.get("detail") or item.get("stage") or "unknown"
                serial = item.get("serial", "?")
                log(f"    ❌ 窗口 {serial}: {detail}")

    log("=" * 60)

    # 写入结果摘要
    summary_file = LOG_DIR / f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        summary_file.write_text(
            json.dumps(
                {
                    "timestamp": datetime.now().isoformat(),
                    "elapsed_minutes": round(elapsed / 60, 1),
                    "total_success": total_success,
                    "total_failed": total_failed,
                    "results": results,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        log(f"结果摘要: {summary_file}")
    except Exception:
        pass

    return 1 if total_failed > 0 else 0


# ══════════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════════
def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description="YouTube 自动化 CLI — 渲染 + 文案 + 上传一键管道",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_daily.py                    使用 daily_job.json
  python run_daily.py -f my_job.json     指定配置文件
  python run_daily.py --gen              生成示例配置文件

配置文件格式:
{
  "jobs": [
    {
      "group_tag": "全球电影",
      "window_serials": [1, 28, 48, 68, 82],
      "videos_per_window": 3,
      "path_template": "0406全球",
      "visual_mode": "random",
      "modules": ["metadata", "render", "upload"]
    }
  ]
}
""",
    )
    parser.add_argument("-f", "--file", type=str, default=str(DEFAULT_JOB_FILE),
                        help="配置文件路径 (默认: daily_job.json)")
    parser.add_argument("--gen", action="store_true",
                        help="生成示例配置文件")

    args = parser.parse_args()

    if args.gen:
        _generate_example_config(Path(args.file))
        return 0

    return _run(Path(args.file))


if __name__ == "__main__":
    sys.exit(main())
