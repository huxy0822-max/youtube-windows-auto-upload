# -*- coding: utf-8 -*-
"""
冒烟测试套件 - 验证重构后所有核心功能正常。
不需要真实浏览器，只测试逻辑层。
"""

from __future__ import annotations

import asyncio
import json
import io
import sys
import tempfile
import time
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_imports() -> None:
    """测试所有模块能正常导入"""
    module_names = [
        "dashboard_app",
        "batch_upload",
        "workflow_core",
        "run_plan_service",
        "run_queue",
        "metadata_service",
        "content_generation",
        "prompt_studio",
        "browser_api",
        "human_interaction",
        "effects_library",
        "utils",
        "path_helpers",
        "group_upload_workflow",
        "upload_window_planner",
        "daily_scheduler",
        "archive_manager",
        "path_templates",
    ]
    loaded = [__import__(module_name) for module_name in module_names]
    assert len(loaded) == len(module_names)
    print("✓ 所有模块导入成功")


def test_run_queue() -> None:
    """测试运行队列完整流程"""
    from run_queue import GroupJob, RunQueue

    q = RunQueue()
    assert q.is_empty()

    q.add_job(
        GroupJob(
            "华尔兹",
            [84, 85],
            r"F:\test1",
            steps=["generate", "upload"],
            videos_per_window=2,
            api_template="codex",
            api_preset="codex",
        )
    )
    q.add_job(GroupJob("MegaBass", [88], r"F:\test2", visual_mode="MegaBass"))
    assert not q.is_empty()
    assert len(q.jobs) == 2

    data = q.to_dict()
    q2 = RunQueue.from_dict(data)
    assert len(q2.jobs) == 2
    assert q2.jobs[0].group_tag == "华尔兹"
    assert q2.jobs[0].steps == ["generate", "upload"]
    assert q2.jobs[0].modules == ["metadata", "upload"]
    assert q2.jobs[0].videos_per_window == 2
    assert q2.jobs[0].api_template == "codex"
    assert q2.jobs[0].api_preset == "codex"
    assert q2.jobs[1].visual_mode == "MegaBass"

    q.remove_job(0)
    assert len(q.jobs) == 1
    assert q.jobs[0].group_tag == "MegaBass"

    q.clear()
    assert q.is_empty()
    print("✓ RunQueue 测试通过")


def test_window_overrides() -> None:
    """测试窗口级覆盖设置序列化"""
    from run_queue import GroupJob, RunQueue, WindowOverride

    job = GroupJob(
        "MegaBass",
        [84, 85, 88],
        r"F:\Mega Bass\2222",
        path_template="默认路径",
        steps=["generate", "upload"],
        videos_per_window=3,
        api_template="codex新",
        api_preset="codex新",
    )
    job.set_window_override(WindowOverride(serial=84, visibility="private", ypp="no"))
    job.set_window_override(
        WindowOverride(
            serial=88,
            visibility="schedule",
            schedule_mode="custom",
            schedule_date="2026-03-25",
            schedule_time="08:30",
        )
    )

    queue = RunQueue()
    queue.add_job(job)

    payload = queue.to_dict()
    restored = RunQueue.from_dict(payload)
    restored_job = restored.jobs[0]
    assert restored_job.path_template == "默认路径"
    assert restored_job.steps == ["generate", "upload"]
    assert restored_job.modules == ["metadata", "upload"]
    assert restored_job.videos_per_window == 3
    assert restored_job.api_template == "codex新"
    assert restored_job.api_preset == "codex新"
    assert len(restored_job.window_overrides) == 2
    assert restored_job.get_window_override(84) is not None
    assert restored_job.get_window_override(88) is not None
    assert restored_job.get_window_override(88).schedule_mode == "custom"
    assert restored_job.get_window_override(88).schedule_date == "2026-03-25"
    print("✓ WindowOverride 测试通过")


def test_batch_dedup() -> None:
    """测试同批去重"""
    from metadata_service import BatchDedup

    dedup = BatchDedup()
    dedup.record("标题A", "简介A")
    dedup.record("标题B", "简介B")

    assert dedup.is_duplicate("标题A", "新简介") is True
    assert dedup.is_duplicate("新标题", "简介B") is True
    assert dedup.is_duplicate("标题C", "简介C") is False

    dedup.reset()
    assert dedup.is_duplicate("标题A", "简介A") is False
    print("✓ BatchDedup 测试通过")


def test_visual_presets() -> None:
    """测试视觉预设加载"""
    preset_path = Path(__file__).resolve().parent.parent / "config" / "visual_presets.json"
    assert preset_path.exists(), f"预设文件不存在: {preset_path}"

    with open(preset_path, encoding="utf-8") as handle:
        presets = json.load(handle)

    assert "MegaBass" in presets
    assert "description" in presets["MegaBass"]
    print(f"✓ 视觉预设加载成功，共 {len(presets)} 个预设")


def test_reactive_spectrum_presets() -> None:
    """测试真实音频驱动频谱预设加载"""
    from reactive_spectrum import list_reactive_spectrum_presets, load_reactive_spectrum_presets

    presets = load_reactive_spectrum_presets()
    names = list_reactive_spectrum_presets()
    assert len(presets) >= 8, f"真实频谱预设过少: {len(presets)}"
    assert "Aurora Horizon" in presets
    assert "Laser Orbit" in presets
    assert names
    print(f"✓ 真实频谱预设加载成功，共 {len(names)} 个预设")


def test_spectrum_assets() -> None:
    """测试文件式频谱素材库"""
    from effects_library import discover_spectrum_files, list_spectrum_assets

    spectrum_files = discover_spectrum_files()
    asset_names = list_spectrum_assets()
    assert len(spectrum_files) >= 10, f"频谱素材过少: {len(spectrum_files)}"
    assert "mirin_horizon_emerald_bloom" in spectrum_files
    assert "mirin_radial_crimson_fire" in spectrum_files
    assert "random_asset" in asset_names
    for filename in spectrum_files.values():
        path = Path(__file__).resolve().parent.parent / "spectrums" / filename
        assert path.exists(), f"频谱文件不存在: {path}"
    print(f"✓ 频谱素材库加载成功，共 {len(spectrum_files)} 个文件式频谱")


def test_reactive_effect_kwargs() -> None:
    """测试真实频谱参数能传入效果构建"""
    from daily_scheduler import RenderOptions, build_effect_kwargs

    opts = RenderOptions()
    opts.fx_reactive_spectrum_enabled = True
    opts.fx_reactive_spectrum_preset = "random"
    kwargs = build_effect_kwargs(opts)
    assert kwargs["reactive_spectrum_enabled"] is True
    assert str(kwargs["reactive_spectrum_preset"]).strip()
    print(f"✓ 真实频谱参数构建成功: {kwargs['reactive_spectrum_preset']}")


def test_sticker_assets() -> None:
    """测试贴纸素材库"""
    from effects_library import discover_sticker_files, list_sticker_effects

    stickers = discover_sticker_files()
    assert len(stickers) >= 40, f"贴纸素材过少: {len(stickers)}"
    assert "kenney_music_on_01.png" in stickers
    assert "kenney_equalizer_v_01.png" in stickers
    sticker_choices = list_sticker_effects()
    assert "none" in sticker_choices
    assert "random" in sticker_choices
    print(f"✓ 贴纸素材库加载成功，共 {len(stickers)} 个贴纸")


def test_visual_asset_sources_manifest() -> None:
    """测试视觉素材来源清单可读取"""
    manifest_path = (
        Path(__file__).resolve().parent.parent
        / "downloaded_projects"
        / "visual_assets_20260406"
        / "sources.json"
    )
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)

    sources = manifest.get("sources", [])
    assert sources, "sources.json 中没有来源记录"
    assert any("miirriin.com" in str(item.get("source_page", "")) for item in sources)
    assert any(
        any(str(output).startswith("spectrums/") for output in item.get("selected_outputs", []))
        for item in sources
    ), "sources.json 中没有频谱产物记录"
    print(f"✓ 视觉素材来源清单读取成功，共 {len(sources)} 个来源")


def test_random_effects() -> None:
    """测试随机视觉确实有变化"""
    from effects_library import get_random_effects

    results = set()
    for _ in range(10):
        fx = get_random_effects()
        results.add((fx.get("spectrum"), fx.get("tint"), fx.get("zoom")))
    assert len(results) >= 3, f"随机性不足: 仅 {len(results)} 种组合"
    print(f"✓ 随机视觉测试通过，10 次生成 {len(results)} 种组合")


def test_render_profile_selection() -> None:
    """测试渲染设备偏好选择"""
    from workflow_core import _resolve_render_profile

    cpu_profile = _resolve_render_profile({"render_device_preference": "cpu"})
    assert cpu_profile.video_codec == "libx264"
    gpu_profile = _resolve_render_profile({"render_device_preference": "gpu"})
    assert str(gpu_profile.video_codec).strip()
    print(f"✓ 渲染设备偏好测试通过: cpu={cpu_profile.video_codec}, gpu={gpu_profile.video_codec}")


def test_path_templates() -> None:
    """测试路径模板加载与保存"""
    from path_templates import DEFAULT_PATH_TEMPLATE_NAME, load_path_templates, save_path_templates

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "path_templates.json"
        templates = load_path_templates(path)
        assert DEFAULT_PATH_TEMPLATE_NAME in templates

        templates["测试路径"] = {
            "description": "测试",
            "source_root": r"F:\Demo",
            "copywriting_output": "",
            "thumbnail_output": "",
            "render_output": "",
            "used_materials_dir": r"F:\Archive\materials",
            "used_videos_dir": r"F:\Archive\videos",
            "auto_delete_days": 3,
        }
        save_path_templates(templates, path)
        loaded = load_path_templates(path)
        assert "测试路径" in loaded
        assert loaded["测试路径"]["auto_delete_days"] == 3
    print("✓ 路径模板测试通过")


def test_archive_manager() -> None:
    """测试归档逻辑"""
    from archive_manager import ArchiveManager

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        source_dir = root / "source"
        source_dir.mkdir()
        materials_dir = root / "used_materials"
        videos_dir = root / "used_videos"

        audio = source_dir / "audio.mp3"
        image = source_dir / "cover.png"
        video = source_dir / "rendered.mp4"
        audio.write_text("audio", encoding="utf-8")
        image.write_text("image", encoding="utf-8")
        video.write_text("video", encoding="utf-8")

        manager = ArchiveManager(
            {
                "used_materials_dir": str(materials_dir),
                "used_videos_dir": str(videos_dir),
                "auto_delete_days": 1,
            }
        )
        moved_materials = manager.archive_materials(str(source_dir), [str(audio), str(image)])
        moved_video = manager.archive_video(str(video))
        assert len(moved_materials) == 2
        assert moved_video
        assert not audio.exists()
        assert not image.exists()
        assert not video.exists()

        old_dir = videos_dir / "2000-01-01"
        old_dir.mkdir(parents=True, exist_ok=True)
        (old_dir / "old.mp4").write_text("old", encoding="utf-8")
        deleted = manager.cleanup_old_videos()
        assert deleted >= 1
    print("✓ ArchiveManager 测试通过")


def test_render_history_empty_file() -> None:
    """测试空 render_history.json 不会导致崩溃"""
    import daily_scheduler

    original_history_file = daily_scheduler.HISTORY_FILE
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_history = Path(temp_dir) / "render_history.json"
            temp_history.write_text("", encoding="utf-8")
            daily_scheduler.HISTORY_FILE = temp_history
            options = SimpleNamespace(
                target_date="0325",
                fx_randomize=True,
                fx_spectrum="yes",
                fx_timeline="yes",
                fx_letterbox="no",
                fx_zoom="normal",
                fx_color_spectrum="WhiteGold",
                fx_color_timeline="WhiteGold",
                fx_spectrum_y="0.65",
                fx_style="pointed",
            )
            with redirect_stdout(io.StringIO()):
                daily_scheduler.save_render_history(options, [{"tag": "测试分组"}], 1, 1, 60.0)
            payload = json.loads(temp_history.read_text(encoding="utf-8"))
            assert isinstance(payload, list)
            assert len(payload) == 1
            assert payload[0]["tag_count"] == 1
    finally:
        daily_scheduler.HISTORY_FILE = original_history_file
    print("✓ 空 render_history.json 容错通过")


def test_config_files() -> None:
    """测试所有配置文件能正确读取"""
    base = Path(__file__).resolve().parent.parent / "config"
    for name in ["upload_config.json", "channel_mapping.json", "visual_presets.json", "path_templates.json"]:
        path = base / name
        if path.exists():
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            text = json.dumps(data, ensure_ascii=False)
            assert "???" not in text, f"{name} 中仍有乱码"
            print(f"  ✓ {name} 加载正常，无乱码")

    print("✓ 配置文件测试通过")


def test_encoding() -> None:
    """测试所有 .py 文件的编码"""
    base = Path(__file__).resolve().parent.parent
    issues: list[str] = []
    for py_file in base.glob("*.py"):
        try:
            content = py_file.read_text(encoding="utf-8")
            if "???" in content and "test" not in py_file.name:
                issues.append(f"{py_file.name}: 仍有 ??? 乱码")
        except UnicodeDecodeError:
            issues.append(f"{py_file.name}: 不是有效 UTF-8")

    if issues:
        for issue in issues:
            print(f"  ✗ {issue}")
        raise AssertionError(f"编码问题: {len(issues)} 个文件")
    print("✓ 所有 .py 文件编码正常")


def test_upload_config() -> None:
    """测试 upload_config 能正确解析分组"""
    config_path = Path(__file__).resolve().parent.parent / "config" / "upload_config.json"
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)

    tag_to_project = config.get("tag_to_project", {})
    assert len(tag_to_project) > 0, "tag_to_project 为空"
    print(f"✓ upload_config 解析成功，共 {len(tag_to_project)} 个分组")


def test_path_helpers() -> None:
    """测试环境检测"""
    from path_helpers import ensure_environment

    environment = ensure_environment()
    assert "platform" in environment
    assert "gpu" in environment
    print(
        f"✓ 环境检测: platform={environment['platform']}, "
        f"gpu={environment['gpu']}, ffmpeg={'有' if environment.get('ffmpeg') else '无'}"
    )


def test_dashboard_instantiation() -> None:
    """测试 Dashboard 模块能正常导入"""
    loaded = __import__("dashboard_app")
    assert loaded is not None
    print("✓ Dashboard 模块加载正常")


def test_dashboard_upload_step_selection() -> None:
    """测试上传页执行步骤勾选框"""
    import dashboard_app

    app = dashboard_app.DashboardApp()
    try:
        groups = list(app.group_catalog.keys())
        assert groups, "没有可用分组"
        target_group = next((name for name in groups if str(name).startswith("0127")), groups[0])
        app.current_group_var.set(target_group)
        app.queue_path_template_var.set("默认路径")
        app._apply_current_group_context(preserve_selection=False)
        expected_api_name, _ = app._queue_template_defaults_for_group(target_group)
        app.queue_videos_per_window_var.set("3")
        app._step_generate_var.set(True)
        app._step_render_var.set(False)
        app._step_upload_var.set(True)
        app.queue_windows_var.set(str((app._live_groups.get(target_group) or [app.group_catalog[target_group][0].serial])[0]))
        app._add_current_group_to_queue()
        assert app.run_queue.jobs, "队列为空"
        job = app.run_queue.jobs[-1]
        assert "render" not in job.steps
        assert "render" not in job.modules
        assert job.videos_per_window == 3
        assert hasattr(app, "queue_api_template_menu")
        assert str(app.queue_api_template_var.get() or "").strip() == expected_api_name
        assert job.api_template == expected_api_name
        assert hasattr(app, "current_group_menu")
        assert getattr(app, "_cjk_font_family", "")
    finally:
        app.destroy()
    print("✓ 上传页执行步骤勾选框测试通过")


def test_dashboard_upload_defaults_notify_flag() -> None:
    """默认规则里的通知订阅者应进入 UploadDefaults"""
    import dashboard_app

    app = dashboard_app.DashboardApp()
    try:
        app.default_notify_var.set(True)
        app.default_kids_var.set("no")
        app.default_ai_var.set("yes")
        defaults = app._current_upload_defaults_model()
        payload = defaults.to_dict()
        assert payload["notify_subscribers"] == "yes"
        assert payload["is_for_kids"] is False
        assert payload["ai_content"] == "yes"
        assert payload["altered_content"] == "yes"
    finally:
        app.destroy()
    print("✓ 上传默认规则通知订阅者传递测试通过")


def test_dashboard_path_template_does_not_reset_prompt_bindings() -> None:
    """切换路径模板时不应重置 API/提示词模板选择"""
    import dashboard_app

    app = dashboard_app.DashboardApp()
    try:
        groups = list(app.group_catalog.keys())
        assert groups, "没有可用分组"
        target_group = next((name for name in groups if str(name).startswith("0127")), groups[0])
        app.current_group_var.set(target_group)
        app._apply_current_group_context(preserve_selection=False)

        api_values = list(app.queue_api_template_menu.cget("values"))
        prompt_values = list(app.queue_prompt_template_menu.cget("values"))
        assert api_values, "没有可用 API 模板"
        assert prompt_values, "没有可用提示词模板"

        custom_api = "codex新" if "codex新" in api_values else api_values[-1]
        custom_prompt = "MEGABASS" if "MEGABASS" in prompt_values else prompt_values[-1]
        app.queue_api_template_var.set(custom_api)
        app.queue_prompt_template_var.set(custom_prompt)

        current_template = str(app.queue_path_template_var.get() or "").strip() or "默认路径"
        alternative = next((name for name in app.path_templates.keys() if name != current_template), "")
        if not alternative:
            alternative = "测试路径模板"
            app.path_templates[alternative] = {
                "description": "测试",
                "source_root": r"F:\SmokeTest",
                "copywriting_output": "",
                "thumbnail_output": "",
                "render_output": "",
                "used_materials_dir": "",
                "used_videos_dir": "",
                "auto_delete_days": 0,
            }
            app.queue_path_template_menu.configure(values=list(app.path_templates.keys()))
        app.queue_path_template_var.set(alternative)
        app.update_idletasks()

        assert str(app.queue_api_template_var.get() or "").strip() == custom_api
        assert str(app.queue_prompt_template_var.get() or "").strip() == custom_prompt
    finally:
        app.destroy()
    print("✓ 路径模板切换不会重置 API/提示词模板")


def test_workflow_core_bootstrap_video_lookup() -> None:
    """workflow_core 的视频探测不应再因 VIDEO_EXTENSIONS 崩溃"""
    from workflow_core import WindowTask, _claim_bootstrap_source_video

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        source_dir = root / "source"
        output_dir = root / "out"
        source_dir.mkdir()
        output_dir.mkdir()
        video_path = source_dir / "0325_84.mp4"
        video_path.write_text("video", encoding="utf-8")

        task = WindowTask(tag="测试", serial=84, source_dir=str(source_dir))
        claimed = _claim_bootstrap_source_video(
            task=task,
            output_dir=output_dir,
            date_mmdd="0325",
            serial=84,
            claimed_videos={},
        )
        assert claimed is not None
        assert claimed.exists()
        assert claimed.suffix.lower() == ".mp4"
    print("✓ workflow_core 视频探测测试通过")


def test_workflow_core_nested_video_lookup() -> None:
    """workflow_core 能从窗口子目录中找到现成视频"""
    from workflow_core import WindowTask, _find_existing_video

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        serial_dir = root / "0322_85_01"
        serial_dir.mkdir(parents=True, exist_ok=True)
        nested_video = serial_dir / "rendered_final.mp4"
        nested_video.write_text("video", encoding="utf-8")

        task = WindowTask(tag="测试", serial=85, source_dir=str(root))
        found = _find_existing_video(root, "0325", 85, {}, task=task)
        assert found is not None
    assert found.resolve() == nested_video.resolve()
    print("✓ workflow_core 子目录视频探测通过")


def test_workflow_core_global_media_assignment() -> None:
    """多窗口并发前应先做全局素材分配，避免重复抢同一批素材"""
    from workflow_core import assign_media_to_tasks, create_task, WorkflowDefaults
    from run_plan_service import build_module_selection, build_run_plan

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        for index in range(1, 5):
            (root / f"asset_{index:02d}.png").write_text("img", encoding="utf-8")
            (root / f"output_{index:02d}.mp3").write_text("aud", encoding="utf-8")

        tasks = [
            create_task(tag="测试分配", serial=84, quantity=2, source_dir=str(root)),
            create_task(tag="测试分配", serial=85, quantity=2, source_dir=str(root)),
        ]
        run_plan = build_run_plan(
            tasks=tasks,
            defaults=WorkflowDefaults(date_mmdd="0407"),
            modules=build_module_selection(metadata=False, render=True, upload=False),
            config={
                "music_dir": str(root),
                "base_image_dir": str(root),
                "metadata_root": str(root),
                "output_root": str(root),
            },
        )
        assignment, warnings = assign_media_to_tasks(run_plan.tasks, config=run_plan.config)
        assert not warnings, warnings
        assert len(assignment) == 4
        assigned_images = [pair[0] for pair in assignment.values()]
        assigned_audio = [pair[1] for pair in assignment.values()]
        assert len(set(assigned_images)) == 4
        assert len(set(assigned_audio)) == 4
    print("✓ workflow_core 全局素材分配测试通过")


def test_batch_upload_nested_video_lookup() -> None:
    """batch_upload 上传阶段能从窗口子目录中找到视频"""
    from batch_upload import _resolve_job_video_path

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        serial_dir = root / "0322_85_01"
        serial_dir.mkdir(parents=True, exist_ok=True)
        nested_video = serial_dir / "final_video.mp4"
        nested_video.write_text("video", encoding="utf-8")

        found = _resolve_job_video_path(root, {}, 85)
        assert found.resolve() == nested_video.resolve()
    print("✓ batch_upload 子目录视频探测通过")


def test_batch_upload_bool_coercion() -> None:
    """batch_upload 不应把 'no' 误判成 True"""
    from batch_upload import (
        _apply_window_override_to_metadata,
        _build_group_upload_options,
        _build_upload_options_for_serial,
    )
    from run_queue import GroupJob, UploadDefaults, WindowOverride

    defaults = UploadDefaults(
        is_for_kids=False,
        altered_content="yes",
        notify_subscribers="yes",
        auto_close_after=False,
    )
    metadata = {
        "is_ypp": "no",
        "notify_subscribers": "false",
        "upload_options": {
            "made_for_kids": "no",
            "altered_content": "yes",
            "notify_subscribers": "false",
            "auto_close_after": "false",
        },
    }
    options = _build_group_upload_options(metadata, defaults)
    assert options["made_for_kids"] is False
    assert options["altered_content"] is True
    assert options["notify_subscribers"] is False
    assert options["auto_close_after"] is False

    inherited_options = _build_group_upload_options({"upload_options": {}}, defaults)
    assert inherited_options["notify_subscribers"] is True

    job = GroupJob("测试", [84], r"F:\test")
    job.set_window_override(
        WindowOverride(
            serial=84,
            ypp="no",
            kids_content="no",
            ai_content="yes",
            notify_subscribers="yes",
        )
    )
    serial_options = _build_upload_options_for_serial(job, {"upload_options": {}}, defaults, 84)
    assert serial_options["notify_subscribers"] is True
    payload = _apply_window_override_to_metadata(job, metadata, defaults, 84)
    assert payload["is_ypp"] is False
    assert payload["upload_options"]["made_for_kids"] is False
    assert payload["upload_options"]["altered_content"] is True
    assert payload["upload_options"]["notify_subscribers"] is True
    print("✓ batch_upload 布尔转换测试通过")


def test_no_quick_start() -> None:
    """确认快捷开始 Tab 已删除"""
    import dashboard_app

    app = dashboard_app.DashboardApp()
    try:
        tab_names = list(getattr(app.tabview, "_tab_dict", {}).keys())
        assert "快捷开始" not in tab_names
        assert "上传" in tab_names
        assert "路径模板" in tab_names
    finally:
        app.destroy()
    print("✓ 快捷开始 Tab 已删除")


def test_run_plan_service_worker_counts() -> None:
    """run_plan_service 并发工人数策略"""
    from run_plan_service import _metadata_prepare_worker_count, _upload_worker_count

    assert _metadata_prepare_worker_count(1) == 1
    assert _metadata_prepare_worker_count(3) >= 3
    assert _upload_worker_count(1) == 1
    assert _upload_worker_count(3) >= 2
    assert _upload_worker_count(8) == 3
    print("鉁?run_plan_service 工人数策略测试通过")


def test_run_plan_service_parallel_uploads() -> None:
    """execute_run_queue 应该允许多个窗口上传并行"""
    import run_plan_service
    from run_queue import GroupJob, RunQueue, UploadDefaults

    original_execute_group_job = run_plan_service.execute_group_job
    active_uploads = 0
    peak_uploads = 0
    lock = asyncio.Lock()

    async def fake_execute_group_job(job, defaults, progress_callback=None):
        nonlocal active_uploads, peak_uploads
        async with lock:
            active_uploads += 1
            peak_uploads = max(peak_uploads, active_uploads)
        await asyncio.sleep(0.05)
        async with lock:
            active_uploads -= 1
        serial = int((job.window_serials or [0])[0] or 0)
        return {
            "group_tag": str(job.group_tag or "").strip(),
            "results": [
                {
                    "serial": serial,
                    "success": True,
                    "slot_index": 1,
                    "total_slots": 1,
                    "stage": "success",
                }
            ],
            "success_count": 1,
            "failed_count": 0,
        }

    queue = RunQueue()
    queue.add_job(GroupJob("并发上传", [84, 85, 88], r"F:\test", steps=["upload"], modules=["upload"]))

    try:
        run_plan_service.execute_group_job = fake_execute_group_job
        results = asyncio.run(
            run_plan_service.execute_run_queue(
                queue,
                UploadDefaults(),
                log=lambda _message: None,
            )
        )
    finally:
        run_plan_service.execute_group_job = original_execute_group_job

    assert results
    assert peak_uploads >= 2, f"上传仍然串行，峰值并发={peak_uploads}"
    print(f"鉁?run_plan_service 上传并发测试通过，峰值并发={peak_uploads}")


def test_run_plan_service_streaming_uploads() -> None:
    """单个素材就绪后，上传应在该窗口全部准备完成前启动"""
    import run_plan_service
    from run_queue import GroupJob, RunQueue, UploadDefaults

    original_execute_run_plan = run_plan_service.execute_run_plan
    original_execute_group_job = run_plan_service.execute_group_job
    upload_started_at = 0.0
    prepare_finished_at = 0.0

    async def fake_execute_group_job(job, defaults, progress_callback=None):
        nonlocal upload_started_at
        if not upload_started_at:
            upload_started_at = time.monotonic()
        await asyncio.sleep(0.01)
        serial = int((job.window_serials or [0])[0] or 0)
        return {
            "group_tag": str(job.group_tag or "").strip(),
            "results": [
                {
                    "serial": serial,
                    "success": True,
                    "slot_index": 1,
                    "total_slots": 1,
                    "stage": "success",
                }
            ],
            "success_count": 1,
            "failed_count": 0,
        }

    def fake_execute_run_plan(plan, *, control=None, on_metadata_ready=None, on_item_ready=None, log=None):
        nonlocal prepare_finished_at
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_dir = Path(temp_dir) / "0322_84_01"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = manifest_dir / "upload_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "channels": {
                            "84": {
                                "title": "test",
                                "video": "0322_84_01.mp4",
                            }
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            if callable(on_item_ready):
                on_item_ready(
                    SimpleNamespace(serial=84, slot_index=1, total_slots=2),
                    manifest_dir,
                    manifest_path,
                )
            time.sleep(0.12)
            prepare_finished_at = time.monotonic()
            return SimpleNamespace(
                prepared_output_dirs={"串流上传": str(manifest_dir)},
                run_plan=SimpleNamespace(window_plan={"tag_output_dirs": {"串流上传": str(manifest_dir)}}),
                workflow_result=SimpleNamespace(manifest_paths=[str(manifest_path)]),
            )

    queue = RunQueue()
    queue.add_job(GroupJob("串流上传", [84], r"F:\test", steps=["render", "upload"], modules=["render", "upload"]))

    try:
        run_plan_service.execute_run_plan = fake_execute_run_plan
        run_plan_service.execute_group_job = fake_execute_group_job
        asyncio.run(
            run_plan_service.execute_run_queue(
                queue,
                UploadDefaults(),
                build_run_plan_for_job=lambda _job: SimpleNamespace(config={}, tasks=[]),
                log=lambda _message: None,
            )
        )
    finally:
        run_plan_service.execute_run_plan = original_execute_run_plan
        run_plan_service.execute_group_job = original_execute_group_job

    assert upload_started_at > 0, "上传从未启动"
    assert prepare_finished_at > 0, "准备阶段未完成"
    assert upload_started_at < prepare_finished_at, (
        f"上传仍在等待整窗准备完成: upload_started_at={upload_started_at}, "
        f"prepare_finished_at={prepare_finished_at}"
    )
    print("✓ run_plan_service 串流上传测试通过")


def test_run_plan_service_metadata_ready_event() -> None:
    """metadata_ready 事件应在准备完成前发出"""
    import run_plan_service
    from run_queue import GroupJob, RunQueue, UploadDefaults

    original_execute_run_plan = run_plan_service.execute_run_plan
    original_execute_group_job = run_plan_service.execute_group_job
    progress_events: list[tuple[str, float]] = []

    async def fake_execute_group_job(job, defaults, progress_callback=None):
        await asyncio.sleep(0.01)
        serial = int((job.window_serials or [0])[0] or 0)
        return {
            "group_tag": str(job.group_tag or "").strip(),
            "results": [
                {
                    "serial": serial,
                    "success": True,
                    "slot_index": 1,
                    "total_slots": 1,
                    "stage": "success",
                }
            ],
            "success_count": 1,
            "failed_count": 0,
        }

    def fake_execute_run_plan(plan, *, control=None, on_metadata_ready=None, on_item_ready=None, log=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_dir = Path(temp_dir) / "0322_84_01"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = manifest_dir / "upload_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "channels": {
                            "84": {
                                "title": "test",
                                "video": "0322_84_01.mp4",
                            }
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            if callable(on_metadata_ready):
                on_metadata_ready(
                    SimpleNamespace(serial=84, slot_index=1, total_slots=1),
                    manifest_dir,
                    {
                        "title": "Test Title",
                        "cover_paths": ["cover.png"],
                        "bundle": {
                            "api_preset": {"name": "codex"},
                            "content_template": {"name": "木吉他"},
                        },
                    },
                )
            time.sleep(0.05)
            if callable(on_item_ready):
                on_item_ready(
                    SimpleNamespace(serial=84, slot_index=1, total_slots=1),
                    manifest_dir,
                    manifest_path,
                )
            return SimpleNamespace(
                prepared_output_dirs={"元数据事件": str(manifest_dir)},
                run_plan=SimpleNamespace(window_plan={"tag_output_dirs": {"元数据事件": str(manifest_dir)}}),
                workflow_result=SimpleNamespace(manifest_paths=[str(manifest_path)]),
            )

    def progress_callback(event):
        progress_events.append((str(event.get("type") or ""), time.monotonic()))

    queue = RunQueue()
    queue.add_job(GroupJob("元数据事件", [84], r"F:\test", steps=["render", "upload"], modules=["render", "upload"]))

    try:
        run_plan_service.execute_run_plan = fake_execute_run_plan
        run_plan_service.execute_group_job = fake_execute_group_job
        asyncio.run(
            run_plan_service.execute_run_queue(
                queue,
                UploadDefaults(),
                build_run_plan_for_job=lambda _job: SimpleNamespace(config={}, tasks=[]),
                progress_callback=progress_callback,
                log=lambda _message: None,
            )
        )
    finally:
        run_plan_service.execute_run_plan = original_execute_run_plan
        run_plan_service.execute_group_job = original_execute_group_job

    metadata_time = next((ts for event_type, ts in progress_events if event_type == "metadata_ready"), 0.0)
    prepare_finished_time = next((ts for event_type, ts in progress_events if event_type == "prepare_finished"), 0.0)
    assert metadata_time > 0, f"未收到 metadata_ready 事件: {progress_events}"
    assert prepare_finished_time > 0, f"未收到 prepare_finished 事件: {progress_events}"
    assert metadata_time <= prepare_finished_time, (
        f"metadata_ready 没有在 prepare_finished 前发出: "
        f"metadata_time={metadata_time}, prepare_finished_time={prepare_finished_time}"
    )
    print("✓ run_plan_service metadata_ready 事件测试通过")


if __name__ == "__main__":
    tests = [
        test_imports,
        test_run_queue,
        test_window_overrides,
        test_batch_dedup,
        test_visual_presets,
        test_reactive_spectrum_presets,
        test_spectrum_assets,
        test_reactive_effect_kwargs,
        test_sticker_assets,
        test_visual_asset_sources_manifest,
        test_random_effects,
        test_render_profile_selection,
        test_path_templates,
        test_archive_manager,
        test_render_history_empty_file,
        test_config_files,
        test_encoding,
        test_upload_config,
        test_path_helpers,
        test_dashboard_instantiation,
        test_dashboard_upload_step_selection,
        test_dashboard_upload_defaults_notify_flag,
        test_dashboard_path_template_does_not_reset_prompt_bindings,
        test_workflow_core_bootstrap_video_lookup,
        test_workflow_core_nested_video_lookup,
        test_workflow_core_global_media_assignment,
        test_batch_upload_nested_video_lookup,
        test_batch_upload_bool_coercion,
        test_no_quick_start,
        test_run_plan_service_worker_counts,
        test_run_plan_service_parallel_uploads,
        test_run_plan_service_streaming_uploads,
        test_run_plan_service_metadata_ready_event,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as exc:
            print(f"  ✗ {test.__name__} 失败: {exc}")
            failed += 1

    print(f"\n{'=' * 40}")
    print(f"结果: {passed} 通过, {failed} 失败, 共 {len(tests)} 项")
    if failed:
        print("请修复上面的失败项！")
