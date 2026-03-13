#!/usr/bin/env python3
"""
统一控制台用到的提示词 / 内容模板 / generation_map 辅助逻辑。

这里尽量沿用网页版 `youtube-music-ai-site` 的字段命名，方便后续继续对齐。
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_TITLE_LIBRARY = """最怕Rapper唱情歌🎧 一聽就放鬆的中文嘻哈歌單｜深夜靜靜聽・讀書用音樂・Chill 氛圍推薦放空一下的最佳選擇
【中文R&B歌單】寫給過去的自己：謝謝你，當時撐過了那些沒人知道的夜晚｜療癒系中文原創音樂
【路過人間】2025最好聽華語歌單 | 讀書・工作・放鬆最佳背景音樂 | 民歌風格原創抒情歌
【美國鄉村音樂×公路旅行】踏上美妙的公路音樂之旅，行駛在66號公路上，溫暖治愈的鄉村音樂帶你回家"""

DEFAULT_MASTER_PROMPT = """# Role: YouTube 爆款音乐内容策划师 & 视觉总监

## Background & Input
我已为你提供以下核心参数，请直接基于这些信息输出完整的 YouTube 视频发布方案，核心目标是“高点击率（High CTR）”和“高完播率”：
- 内容/音乐类型：【音乐类型】
- 切入角度：【切入角度】
- 目标群体：【目标群体】
- 参考爆款标题库：【爆款标题库】

## Output Structure

### Part 1: 爆款标题 (共 【标题数量】 组)
结合【音乐类型】与【切入角度】，为【目标群体】生成【标题数量】个不同的爆款标题。
参数要求：
- 生成数量：【标题数量】 个
- 字数限制：每个标题字数必须严格控制在 【标题字数区间】 字之间
- 输出语言：【输出语言】

### Part 2: 标准化简介与标签 (共 【简介数量】 组)
- 简介数量：【简介数量】
- 简介默认字数：【简介默认字数】
- 标签数量区间：【标签数量区间】

### Part 3: 封面设计指令 (共 【缩略图指令数量】 组)
缩略图指令可以用英文描述场景，但必须明确要求封面文字使用【thumbnail_text_language】。
请直接开始执行，输出以上三部分内容，不要寒暄。"""


def clone_json(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


def default_api_preset(name: str = "默认API模板") -> dict:
    return {
        "templateType": "api",
        "name": name,
        "provider": "openai_compatible",
        "apiKey": "",
        "baseUrl": "https://api.deepseek.com/chat/completions",
        "model": "deepseek-chat",
        "temperature": "0.9",
        "maxTokens": "16000",
        "autoImageEnabled": "0",
        "imageBaseUrl": "https://yunwu.ai/v1/chat/completions",
        "imageApiKey": "",
        "imageModel": "gemini-3-pro-image-preview",
        "imageConcurrency": "3",
        "outputLanguage": "zh-TW",
    }


def default_content_template(name: str = "默认内容模板") -> dict:
    return {
        "templateType": "content",
        "name": name,
        "musicGenre": "",
        "angle": "",
        "audience": "",
        "outputLanguage": "zh-TW",
        "titleCount": "3",
        "descCount": "1",
        "thumbCount": "3",
        "titleMin": "80",
        "titleMax": "95",
        "descLen": "300",
        "tagRange": "10-20",
        "masterPrompt": DEFAULT_MASTER_PROMPT,
        "titleLibrary": DEFAULT_TITLE_LIBRARY,
    }


def default_prompt_studio_config() -> dict:
    return {
        "version": 1,
        "defaultApiPreset": "默认API模板",
        "defaultContentTemplate": "默认内容模板",
        "tagBindings": {},
        "apiPresets": {
            "默认API模板": default_api_preset(),
        },
        "contentTemplates": {
            "默认内容模板": default_content_template(),
        },
    }


def normalize_prompt_studio_config(raw: Any) -> dict:
    config = default_prompt_studio_config()
    if not isinstance(raw, dict):
        return config

    api_presets = {}
    for name, value in (raw.get("apiPresets") or {}).items():
        if isinstance(value, dict):
            preset = default_api_preset(str(name))
            preset.update(value)
            preset["name"] = str(name)
            preset["templateType"] = "api"
            api_presets[str(name)] = preset
    if api_presets:
        config["apiPresets"] = api_presets

    content_templates = {}
    for name, value in (raw.get("contentTemplates") or {}).items():
        if isinstance(value, dict):
            template = default_content_template(str(name))
            template.update(value)
            template["name"] = str(name)
            template["templateType"] = "content"
            content_templates[str(name)] = template
    if content_templates:
        config["contentTemplates"] = content_templates

    default_api = str(raw.get("defaultApiPreset") or "")
    default_content = str(raw.get("defaultContentTemplate") or "")
    if default_api in config["apiPresets"]:
        config["defaultApiPreset"] = default_api
    if default_content in config["contentTemplates"]:
        config["defaultContentTemplate"] = default_content

    for tag, template_name in (raw.get("tagBindings") or {}).items():
        if template_name in config["contentTemplates"]:
            config["tagBindings"][str(tag)] = template_name

    return config


def load_prompt_studio_config(path: Path) -> dict:
    if path.exists():
        try:
            return normalize_prompt_studio_config(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            pass
    return default_prompt_studio_config()


def save_prompt_studio_config(path: Path, config: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def pick_content_template_name(config: dict, tag: str) -> str:
    templates = config.get("contentTemplates", {})
    if not templates:
        return "默认内容模板"
    bound = config.get("tagBindings", {}).get(tag)
    if bound in templates:
        return bound
    if tag in templates:
        return tag
    default_name = config.get("defaultContentTemplate")
    if default_name in templates:
        return default_name
    return next(iter(templates))


def parse_tag_range(raw: str) -> tuple[int, int]:
    match = re.match(r"\s*(\d+)\s*[-~]\s*(\d+)\s*", str(raw or ""))
    if not match:
        return 10, 20
    return int(match.group(1)), int(match.group(2))


def language_meta(code: str) -> tuple[str, str]:
    mapping = {
        "zh-TW": ("繁体中文", "Traditional Chinese"),
        "zh-CN": ("简体中文", "Simplified Chinese"),
        "en": ("英文", "English"),
        "ja": ("日语", "Japanese"),
        "ko": ("韩语", "Korean"),
        "es": ("西班牙语", "Spanish"),
    }
    return mapping.get(code or "zh-TW", mapping["zh-TW"])


def render_master_prompt(content_template: dict) -> str:
    prompt = str(content_template.get("masterPrompt") or "")
    title_range = f"{content_template.get('titleMin', '80')}-{content_template.get('titleMax', '95')}"
    ui_language, english_language = language_meta(str(content_template.get("outputLanguage") or "zh-TW"))

    replacements = {
        "【音乐类型】": content_template.get("musicGenre", ""),
        "【音樂類型】": content_template.get("musicGenre", ""),
        "【切入角度】": content_template.get("angle", "") or "（未填写，请自动生成）",
        "【目标群体】": content_template.get("audience", ""),
        "【目標群體】": content_template.get("audience", ""),
        "【爆款标题库】": content_template.get("titleLibrary", ""),
        "【爆款標題庫】": content_template.get("titleLibrary", ""),
        "【标题数量】": str(content_template.get("titleCount", "3")),
        "【標題數量】": str(content_template.get("titleCount", "3")),
        "【标题字数区间】": title_range,
        "【標題字數區間】": title_range,
        "【简介数量】": str(content_template.get("descCount", "1")),
        "【簡介數量】": str(content_template.get("descCount", "1")),
        "【简介默认字数】": str(content_template.get("descLen", "300")),
        "【簡介默認字數】": str(content_template.get("descLen", "300")),
        "【标签数量区间】": str(content_template.get("tagRange", "10-20")),
        "【標籤數量區間】": str(content_template.get("tagRange", "10-20")),
        "【缩略图指令数量】": str(content_template.get("thumbCount", "3")),
        "【縮略圖指令數量】": str(content_template.get("thumbCount", "3")),
        "【输出语言】": ui_language,
        "【輸出語言】": ui_language,
        "【thumbnail_text_language】": english_language,
    }
    for key, value in replacements.items():
        prompt = prompt.replace(key, str(value))
    return prompt


def build_site_preview(content_template: dict, api_preset: dict) -> str:
    ui_language, english_language = language_meta(str(content_template.get("outputLanguage") or "zh-TW"))
    tag_min, tag_max = parse_tag_range(str(content_template.get("tagRange") or "10-20"))
    payload = {
        "musicGenre": content_template.get("musicGenre", ""),
        "angle": content_template.get("angle", ""),
        "audience": content_template.get("audience", ""),
        "config": {
            "titleCount": int(content_template.get("titleCount", "3") or 3),
            "descriptionCount": int(content_template.get("descCount", "1") or 1),
            "thumbnailCount": int(content_template.get("thumbCount", "3") or 3),
            "titleCharMin": int(content_template.get("titleMin", "80") or 80),
            "titleCharMax": int(content_template.get("titleMax", "95") or 95),
            "descriptionCharTarget": int(content_template.get("descLen", "300") or 300),
            "tagCountMin": tag_min,
            "tagCountMax": tag_max,
        },
        "customPromptReplaced": render_master_prompt(content_template),
        "outputLanguage": ui_language,
        "thumbnailTextLanguage": english_language,
        "imageProviderEnabled": str(api_preset.get("autoImageEnabled", "0")) == "1",
        "titleLibrary": content_template.get("titleLibrary", ""),
    }
    return (
        f"Provider: {api_preset.get('provider', '')}\n"
        f"Text Model: {api_preset.get('model', '')}\n"
        f"Image Model: {api_preset.get('imageModel', '')}\n\n"
        f"=== 替换后的主提示词 ===\n{payload['customPromptReplaced']}\n\n"
        f"=== 送给模型的结构化输入 ===\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def load_generation_map(path: Path) -> dict:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("channels", {})
                return data
        except Exception:
            pass
    return {"channels": {}}


def save_generation_map(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def guess_cover_names(tag_dir: Path, date_mmdd: str, channel_serial: int) -> list[str]:
    patterns = [
        f"{date_mmdd}_{channel_serial}_cover_*.*",
        f"{date_mmdd}_{channel_serial}_thumb_*.*",
        f"{date_mmdd}_{channel_serial}_thumbnail_*.*",
    ]
    found = []
    for pattern in patterns:
        found.extend(tag_dir.glob(pattern))
    return sorted({path.name for path in found})


def sync_manifest_from_generation_map(
    generation_map: dict,
    tag_dir: Path,
    output_root: Path,
    tag: str,
    date_mmdd: str,
) -> tuple[Path, int]:
    channels = {}
    for channel_serial, channel_info in generation_map.get("channels", {}).items():
        day_info = (channel_info.get("days") or {}).get(date_mmdd)
        if not day_info:
            continue
        covers = [str(tag_dir / Path(name).name) for name in (day_info.get("covers") or [])]
        channels[str(channel_serial)] = {
            "video": f"{date_mmdd}_{channel_serial}.mp4",
            "title": day_info.get("title", ""),
            "description": day_info.get("description", ""),
            "thumbnails": covers,
            "is_ypp": bool(channel_info.get("is_ypp", False)),
            "ab_titles": day_info.get("ab_titles", []),
            "set": day_info.get("set", 1),
        }

    out_dir = output_root / f"{date_mmdd}_{tag}"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "upload_manifest.json"
    manifest = {
        "date": date_mmdd,
        "tag": tag,
        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source": "generation_map.json",
        "channels": channels,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path, len(channels)
