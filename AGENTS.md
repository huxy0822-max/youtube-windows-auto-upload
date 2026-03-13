# AGENTS.md

This repository is a Windows-oriented YouTube automation project.
The main focus is:

- rendering long-form music videos
- scheduling render jobs
- uploading through BitBrowser / HubStudio-compatible browser APIs

When working in this repo, follow these rules first.

## 1. Read order

Before making changes, read files in this order:

1. `README.md`
2. `docs/统一控制台说明.md`
3. `docs/实操配置与提示词说明.md`
4. `docs/GitHub协作入门.md`
5. the target Python file you plan to edit

If the task is about uploading, inspect these files first:

- `batch_upload.py`
- `bulk_upload.py`
- `browser_api.py`
- `utils.py`
- `config/upload_config.json`
- `config/channel_mapping.json`

If the task is about rendering, inspect these files first:

- `daily_scheduler.py`
- `render_engine.py`
- `effects_library.py`
- `scheduler_config.json`

## 2. Important project facts

- This is a public-safe repo copy.
- Config files under `config/` are templates, not production secrets.
- Do not assume the template browser IDs, channel mapping, paths, or tags are real.
- Real local assets are intentionally not committed.

Expected local folders for actual use:

- `workspace/music/`
- `workspace/base_image/`
- `workspace/AutoTask/`
- `workspace/projects/`

These may be empty in the public repo.

## 3. What not to commit

Never commit these kinds of files unless the user explicitly asks:

- real browser environment IDs
- real channel mappings
- upload history
- rendered videos
- audio assets
- thumbnails or private project素材
- large binary files
- tokens, cookies, local secrets

Respect `.gitignore`.

## 4. Default workflow for Codex

When the user asks for help operating this repo, prefer this workflow:

1. Prefer `dashboard.py` as the daily entry point.
2. If the user has multiple tags for one day, use the multi-tag task list in `dashboard.py` or `bulk_upload.py`.
3. Verify config files exist and explain what fields the user must fill.
4. Verify required local files exist for the requested task.
5. Run the smallest realistic command for the requested workflow.
6. If automation fails, diagnose from `batch_upload.py`, `bulk_upload.py`, or the relevant runtime logs.
7. Update docs when the operating method changes.

Do not jump straight to broad refactors if the user asked for an operational fix.

## 5. Upload workflow

Use this flow for upload-related help:

1. Confirm `config/upload_config.json` is filled with the user's local BitBrowser API and tag mapping.
2. Confirm `config/channel_mapping.json` contains the user's real browser env IDs and serial numbers.
3. Confirm the target video exists under `workspace/AutoTask/<date_tag>/`.
4. Confirm title/description/cover data exists in `workspace/base_image/<tag>/generation_map.json`.
5. Confirm BitBrowser is running before starting Playwright automation.
6. Prefer single-channel verification before batch upload.

Typical command:

```bash
py -3 batch_upload.py --tag 面壁者 --date 3.12 --channel 90 --auto-confirm --auto-close-browser
```

For multiple tags in one day:

```bash
py -3 bulk_upload.py --date 3.12 --tags 面壁者,芝加哥蓝调 --auto-confirm --auto-close-browser
```

Known behavior:

- `Altered content` should be selected as `Yes`.
- `Category` should be selected as `Music`.
- For large files, the first `Next` button may not appear until upload progresses much further.

## 6. Render workflow

Use this flow for rendering help:

1. Prefer launching `dashboard.py` or `启动统一控制台.bat`.
2. If the user has more than one tag today, fill the multi-tag task list in `dashboard.py`.
3. Check `scheduler_config.json`.
4. Confirm music files exist under `workspace/music/<tag>/`.
5. Confirm base images exist under `workspace/base_image/<tag>/`.
6. Run render-only first.
7. Verify output under `workspace/AutoTask/`.

Typical command:

```bash
py -3 daily_scheduler.py --standard 0312 --tags=面壁者 --render-only --song-count=1
```

Recommended daily GUI entry:

```bash
py -3 dashboard.py
```

## 7. Setup notes

Typical local setup on Windows:

```bash
py -3 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

If Playwright is missing, install browser dependencies before debugging upload issues.

## 8. Editing rules for Codex

- Prefer minimal, targeted fixes.
- Preserve Windows compatibility.
- Preserve BitBrowser support.
- Keep user-facing docs in Chinese unless the user asks otherwise.
- Add concise comments only where the logic is genuinely non-obvious.
- If behavior changes, update the matching docs in `docs/`.

## 9. Collaboration expectations

If multiple people maintain this repo:

- prefer small commits
- explain why an automation change is needed
- update docs with any workflow change
- avoid mixing private production config with public template files

When unsure whether a config value is safe to publish, treat it as private.
