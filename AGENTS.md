# AGENTS.md

This repository is the public-safe Windows version of a YouTube automation project.

Current daily workflow is centered on:

- `dashboard.py`
- `dashboard_app.py`
- `workflow_core.py`
- `batch_upload.py`
- `daily_scheduler.py`

Old GUI entry points and old batch wrappers were removed. Do not guide users toward deleted scripts.

## 1. Read order

Before changing code, read in this order:

1. `README.md`
2. `docs/新控制台使用说明.md`
3. the target Python file you plan to edit

If the task is mainly about uploading, inspect:

- `dashboard_app.py`
- `workflow_core.py`
- `batch_upload.py`
- `browser_api.py`
- `upload_window_planner.py`
- `group_upload_workflow.py`
- `utils.py`
- `config/upload_config.json`
- `config/channel_mapping.json`

If the task is mainly about rendering, inspect:

- `dashboard_app.py`
- `workflow_core.py`
- `daily_scheduler.py`
- `effects_library.py`
- `scheduler_config.json`

If the task is mainly about prompt generation, inspect:

- `dashboard_app.py`
- `content_generation.py`
- `prompt_studio.py`
- `config/prompt_studio.json`

## 2. Important project facts

- This is a public repo copy.
- Files under `config/` are templates, not production secrets.
- Real browser IDs, real mappings, local assets, and local API keys must not be committed.
- Runtime state should stay untracked.

Expected local folders for real usage:

- `workspace/music/`
- `workspace/base_image/`
- `workspace/AutoTask/`
- `workspace/projects/`

These may be empty in the public repo.

## 3. What not to commit

Never commit:

- real API keys
- real browser env IDs
- real channel mappings
- local path bindings
- upload history
- rendered videos
- private thumbnails or source assets
- large binary archives
- local runtime state

Respect `.gitignore`. If unsure whether a value is safe, treat it as private.

## 4. Default Codex workflow

When helping operate this repo:

1. Prefer `dashboard.py` as the only user-facing entry point.
2. Use the `上传` tab to define which BitBrowser windows work today.
3. Use the `提示词` tab for API presets and content templates.
4. Use the `当日内容` tab only for hand-editing one channel/day payload.
5. Use `路径配置` for global folders and long-term group bindings.
6. Run the smallest realistic flow first.
7. If behavior changes, update `README.md` and `docs/新控制台使用说明.md`.

## 5. Upload workflow

Use this flow:

1. Confirm `config/upload_config.json` has the user’s local BitBrowser API settings.
2. Confirm `config/channel_mapping.json` reflects the user’s real local mapping.
3. Confirm the `上传` tab has the correct windows in the task area.
4. Confirm metadata mode is either:
   - `提示词那套`
   - `原先那套`
5. Confirm the source folder is either:
   - the long-term group binding
   - or the one-off source override entered on the `上传` tab
6. Prefer a dry or short render simulation before a real long upload.

Important behavior:

- `上传 = 单个或批量的统一入口`
- if the task area has 1 window, that is a single upload
- if the task area has multiple windows, that is a batch upload
- `Altered content` should be `Yes`
- `Category` should usually be `Music`

## 6. Render workflow

Use this flow:

1. Confirm `scheduler_config.json`
2. Confirm source assets exist in the bound source folder
3. Run `模拟 1-2 分钟` first when testing
4. Then run real render or render+upload

Current encoder behavior:

- macOS prefers `h264_videotoolbox`
- Windows with usable NVIDIA runtime prefers `h264_nvenc`
- Windows with usable AMD AMF runtime prefers `h264_amf`
- otherwise it falls back to `libx264`

If Windows render is still slow after GPU encode is active, the likely bottleneck is the filter/effects chain, not the encoder.

## 7. Prompt generation behavior

- Metadata generation now tracks recent history and tries to avoid reusing recent title/description/tag/thumbnail prompt combinations.
- Runtime metadata history is local-only and should not be committed.
- Public `config/prompt_studio.json` must remain sanitized.

## 8. Editing rules

- Prefer minimal targeted fixes.
- Preserve Windows compatibility.
- Preserve BitBrowser support.
- Keep user-facing docs in Chinese unless asked otherwise.
- Add comments only when logic is genuinely non-obvious.
- Do not reintroduce deleted legacy entry points without a clear reason.

## 9. Collaboration

If multiple people maintain this repo:

- keep commits small
- explain workflow changes
- keep docs in sync with behavior
- keep public config sanitized
- unless the user explicitly says not to, sync each stable local version to GitHub after the fixes are finished
