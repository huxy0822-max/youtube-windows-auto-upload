#!/usr/bin/env python3
"""
网页版提示词/API 生成逻辑的 Python 版封装。

用途：
1. 复用 `prompt_studio.json` 里的 API 模板和内容模板。
2. 生成结构化的标题 / 简介 / 标签 / 缩略图指令。
3. 可选调用图片模型，把缩略图指令直接生成为本地图片。
"""

from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from typing import Any

import requests

from prompt_studio import (
    clone_json,
    language_meta,
    load_prompt_studio_config,
    parse_tag_range,
    pick_content_template_name,
    render_master_prompt,
)


DEFAULT_TIMEOUT_SECONDS = 70
AUDIENCE_ANALYSIS_MAX_TOKENS = 1400


def _float_value(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _int_value(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _count_chars(value: str) -> int:
    return len([ch for ch in str(value or "").replace(" ", "").replace("\n", "")])


def _build_openai_chat_urls(base_url: str) -> list[str]:
    clean = str(base_url or "").strip().rstrip("/")
    urls = {clean}
    urls.add(f"{clean}/chat/completions")
    urls.add(f"{clean}/v1/chat/completions")
    if clean.endswith("/models"):
        urls.add(clean.removesuffix("/models") + "/chat/completions")
        urls.add(clean.removesuffix("/models") + "/v1/chat/completions")
    if clean.endswith("/v1"):
        urls.add(f"{clean}/chat/completions")
    return [url for url in urls if url]


def _extract_data_url(value: Any) -> str | None:
    if isinstance(value, str):
        match = re.search(r"data:image/[^;]+;base64,[A-Za-z0-9+/=\n\r]+", value)
        return match.group(0).replace("\n", "").replace("\r", "") if match else None
    if isinstance(value, list):
        for item in value:
            found = _extract_data_url(item)
            if found:
                return found
        return None
    if isinstance(value, dict):
        if isinstance(value.get("image_url"), str) and value["image_url"].startswith("data:image/"):
            return value["image_url"]
        if isinstance(value.get("url"), str) and value["url"].startswith("data:image/"):
            return value["url"]
        for inner in value.values():
            found = _extract_data_url(inner)
            if found:
                return found
    return None


def _media_type_from_data_url(data_url: str) -> str:
    match = re.search(r"data:(.*?);base64", str(data_url or ""))
    return match.group(1) if match else "image/png"


def _parse_json_like(raw: str) -> dict[str, Any]:
    cleaned = str(raw or "").strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^```\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"```$", "", cleaned, flags=re.I)

    first = cleaned.find("{")
    last = cleaned.rfind("}")
    if first < 0 or last < 0 or last <= first:
        raise ValueError("模型返回不是合法 JSON")

    try:
        parsed = json.loads(cleaned[first : last + 1])
    except Exception as exc:
        raise ValueError("模型返回的 JSON 无法解析") from exc

    parsed["titles"] = parsed["titles"] if isinstance(parsed.get("titles"), list) else []
    parsed["descriptions"] = parsed["descriptions"] if isinstance(parsed.get("descriptions"), list) else []
    parsed["seoHashtags"] = parsed["seoHashtags"] if isinstance(parsed.get("seoHashtags"), list) else []
    parsed["tagList"] = parsed["tagList"] if isinstance(parsed.get("tagList"), list) else []
    parsed["thumbnails"] = parsed["thumbnails"] if isinstance(parsed.get("thumbnails"), list) else []
    return parsed


def _extract_json_object(raw: str) -> dict[str, Any]:
    cleaned = str(raw or "").strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^```\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"```$", "", cleaned, flags=re.I)
    first = cleaned.find("{")
    last = cleaned.rfind("}")
    if first < 0 or last < 0 or last <= first:
        raise ValueError("模型返回内容里没有可解析的 JSON 对象")
    parsed = json.loads(cleaned[first : last + 1])
    if not isinstance(parsed, dict):
        raise ValueError("模型返回的 JSON 不是对象")
    return parsed


def _normalize_percent(value: Any) -> float | None:
    text = str(value or "").strip().replace("%", "")
    if not text:
        return None
    try:
        return round(float(text), 2)
    except Exception:
        return None


def _normalize_ranked_items(items: Any, *label_keys: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        label = ""
        for key in label_keys:
            candidate = str(item.get(key) or "").strip()
            if candidate:
                label = candidate
                break
        percent = _normalize_percent(item.get("percent"))
        if not label or percent is None:
            continue
        normalized.append({"label": label, "percent": percent})
    normalized.sort(key=lambda row: row["percent"], reverse=True)
    return normalized


def _normalize_audience_payload(parsed: dict[str, Any]) -> dict[str, Any]:
    return {
        "devices": _normalize_ranked_items(parsed.get("devices"), "name", "device"),
        "age": _normalize_ranked_items(parsed.get("age"), "range", "name"),
        "gender": _normalize_ranked_items(parsed.get("gender"), "name", "range"),
        "regions": _normalize_ranked_items(parsed.get("regions"), "name", "region"),
        "summary": str(parsed.get("summary") or "").strip(),
    }


def format_audience_analysis(payload: dict[str, Any]) -> str:
    def format_rows(rows: list[dict[str, Any]]) -> str:
        return " | ".join(f"{row['label']}: {row['percent']:.1f}%" for row in rows)

    lines: list[str] = []
    devices = format_rows(payload.get("devices") or [])
    age = format_rows(payload.get("age") or [])
    gender = format_rows(payload.get("gender") or [])
    regions = format_rows(payload.get("regions") or [])
    if devices:
        lines.append(f"设备占比: {devices}")
    if age:
        lines.append(f"年龄段占比: {age}")
    if gender:
        lines.append(f"性别占比: {gender}")
    if regions:
        lines.append(f"地区占比: {regions}")
    if payload.get("summary"):
        lines.append(f"总结: {payload['summary']}")
    return "\n".join(lines) if lines else "未识别到可用受众数据"


def build_audience_summary(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    devices = payload.get("devices") or []
    age = payload.get("age") or []
    gender = payload.get("gender") or []
    regions = payload.get("regions") or []
    if regions:
        parts.append(f"{regions[0]['label']}占比约{regions[0]['percent']:.1f}%")
    if age:
        parts.append(f"{age[0]['label']}人群占比约{age[0]['percent']:.1f}%")
    if gender:
        parts.append(f"{gender[0]['label']}占比约{gender[0]['percent']:.1f}%")
    if devices:
        parts.append(f"{devices[0]['label']}设备占比约{devices[0]['percent']:.1f}%")
    summary = str(payload.get("summary") or "").strip()
    if summary and parts:
        parts.append(summary)
    return "，".join(parts)


def _parse_audience_json(raw: str) -> dict[str, Any]:
    return _normalize_audience_payload(_extract_json_object(raw))


def _repair_audience_json(raw: str, api_preset: dict) -> dict[str, Any]:
    repair_prompt = (
        "把下面内容改写成严格 JSON，只允许 devices/age/gender/regions/summary 这五个字段。"
        "如果某个维度没有识别到数据，就返回空数组。不要输出 markdown。\n\n"
        f"原始内容:\n{str(raw or '').strip()[:4000]}"
    )
    repair_preset = clone_json(api_preset)
    repair_preset["temperature"] = "0"
    repair_preset["maxTokens"] = "900"
    repaired_raw = call_text_model(repair_preset, repair_prompt)
    return _parse_audience_json(repaired_raw)


def _build_generation_prompt(content_template: dict, image_data_url: str | None = None) -> str:
    tag_range = parse_tag_range(str(content_template.get("tagRange") or "10-20"))
    output_language = str(content_template.get("outputLanguage") or "zh-TW")
    language_ui, language_english = language_meta(output_language)

    payload = {
        "musicGenre": content_template.get("musicGenre", ""),
        "angle": content_template.get("angle", ""),
        "audience": content_template.get("audience", ""),
        "config": {
            "titleCount": _int_value(content_template.get("titleCount"), 3),
            "descriptionCount": _int_value(content_template.get("descCount"), 1),
            "thumbnailCount": _int_value(content_template.get("thumbCount"), 3),
            "titleCharMin": _int_value(content_template.get("titleMin"), 80),
            "titleCharMax": _int_value(content_template.get("titleMax"), 95),
            "descriptionCharTarget": _int_value(content_template.get("descLen"), 300),
            "tagCountMin": tag_range[0],
            "tagCountMax": tag_range[1],
        },
        "customPromptReplaced": render_master_prompt(content_template),
        "outputLanguage": language_ui,
        "thumbnailTextLanguage": language_english,
        "imageProvided": bool(image_data_url),
        "titleLibrary": content_template.get("titleLibrary", ""),
    }

    return (
        "你是专业YouTube内容频道策划师。\n"
        "请严格输出JSON，禁止输出markdown。\n"
        "最高优先级是遵守用户主提示词（已完成变量替换）与用户自定义限制，不可偏离。\n"
        "必须严格满足：标题数量/字数范围、简介数量/目标字数、标签数量、缩略图数量。\n"
        "若未提供切入角度，你必须自动生成一个中竞争、可落地、有真实使用场景的切入角度。\n"
        f"所有文案（标题/简介/标签）必须使用：{language_ui}。\n"
        f"缩略图指令可用英文描述场景，但必须要求封面文字使用：{language_english}。\n"
        f"每条缩略图指令结尾必须包含: Use {language_english} text in the image.\n\n"
        f"输入参数如下:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        "JSON Schema:\n"
        "{\n"
        '  "usedAngle": "string",\n'
        '  "titles": ["string"],\n'
        '  "descriptions": ["string"],\n'
        '  "seoHashtags": ["#tag"],\n'
        '  "tagList": ["tag1", "tag2"],\n'
        '  "thumbnails": [{"forTitle": "string", "prompt": "string"}]\n'
        "}\n"
        "务必返回合法JSON。"
    )


def _validate_output(parsed: dict[str, Any], content_template: dict) -> list[str]:
    cfg = {
        "titleCount": _int_value(content_template.get("titleCount"), 3),
        "descCount": _int_value(content_template.get("descCount"), 1),
        "thumbCount": _int_value(content_template.get("thumbCount"), 3),
        "titleMin": _int_value(content_template.get("titleMin"), 80),
        "titleMax": _int_value(content_template.get("titleMax"), 95),
        "descLen": _int_value(content_template.get("descLen"), 300),
    }
    issues: list[str] = []
    if len(parsed.get("titles", [])) != cfg["titleCount"]:
        issues.append(f"标题数量应为 {cfg['titleCount']}，当前 {len(parsed.get('titles', []))}")
    for idx, title in enumerate(parsed.get("titles", []), 1):
        length = _count_chars(title)
        if length < cfg["titleMin"] or length > cfg["titleMax"]:
            issues.append(f"标题{idx}字数 {length} 不在 {cfg['titleMin']}-{cfg['titleMax']}")
    if len(parsed.get("descriptions", [])) != cfg["descCount"]:
        issues.append(f"简介数量应为 {cfg['descCount']}，当前 {len(parsed.get('descriptions', []))}")
    for idx, desc in enumerate(parsed.get("descriptions", []), 1):
        length = _count_chars(desc)
        if abs(length - cfg["descLen"]) > 60:
            issues.append(f"简介{idx}字数 {length} 偏离目标 {cfg['descLen']}")
    if len(parsed.get("thumbnails", [])) != cfg["thumbCount"]:
        issues.append(f"缩略图指令数量应为 {cfg['thumbCount']}，当前 {len(parsed.get('thumbnails', []))}")
    for idx, item in enumerate(parsed.get("thumbnails", []), 1):
        prompt = str((item or {}).get("prompt") or "").lower()
        if "traditional chinese" not in prompt and "use traditional chinese text in the image" not in prompt:
            issues.append(f"缩略图指令{idx}缺少 Traditional Chinese 约束")
    return issues


def _call_openai_compatible(api_preset: dict, user_prompt: str, image_data_url: str | None = None) -> str:
    content: list[dict[str, Any]] = [{"type": "text", "text": user_prompt}]
    if image_data_url:
        content.append({"type": "image_url", "image_url": {"url": image_data_url}})

    payload = {
        "model": api_preset.get("model", ""),
        "temperature": _float_value(api_preset.get("temperature"), 0.9),
        "max_tokens": _int_value(api_preset.get("maxTokens"), 16000),
        "messages": [
            {"role": "system", "content": "你必须输出合法JSON，不要输出其他内容。"},
            {"role": "user", "content": content},
        ],
    }

    last_error = "unknown error"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_preset.get('apiKey', '')}",
    }
    for url in _build_openai_chat_urls(str(api_preset.get("baseUrl") or "")):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=DEFAULT_TIMEOUT_SECONDS)
            data = response.json()
            if response.ok:
                text = data.get("choices", [{}])[0].get("message", {}).get("content") or data.get("output_text")
                if text:
                    return text
            last_error = data.get("error", {}).get("message") or f"HTTP {response.status_code}"
        except Exception as exc:
            last_error = str(exc)
    raise RuntimeError(f"OpenAI-compatible 调用失败: {last_error}")


def _call_anthropic(api_preset: dict, user_prompt: str, image_data_url: str | None = None) -> str:
    content: list[dict[str, Any]] = [{"type": "text", "text": user_prompt}]
    if image_data_url:
        meta, base64_data = image_data_url.split(",", 1)
        media_type = _media_type_from_data_url(meta)
        content.append(
            {
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": base64_data},
            }
        )

    response = requests.post(
        str(api_preset.get("baseUrl") or ""),
        headers={
            "Content-Type": "application/json",
            "x-api-key": str(api_preset.get("apiKey") or ""),
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": api_preset.get("model", ""),
            "max_tokens": _int_value(api_preset.get("maxTokens"), 16000),
            "temperature": _float_value(api_preset.get("temperature"), 0.9),
            "messages": [{"role": "user", "content": content}],
        },
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    data = response.json()
    if not response.ok:
        raise RuntimeError(data.get("error", {}).get("message") or f"Anthropic HTTP {response.status_code}")
    for item in data.get("content", []):
        if item.get("type") == "text" and item.get("text"):
            return item["text"]
    raise RuntimeError("Anthropic 未返回文本内容")


def _call_gemini(api_preset: dict, user_prompt: str, image_data_url: str | None = None) -> str:
    root = str(api_preset.get("baseUrl") or "").strip().rstrip("/")
    root = re.sub(r"/models(?:\?.*)?$", "", root, flags=re.I)
    endpoint = f"{root}/models/{api_preset.get('model', '')}:generateContent"
    parts: list[dict[str, Any]] = [{"text": user_prompt}]
    if image_data_url:
        meta, base64_data = image_data_url.split(",", 1)
        media_type = _media_type_from_data_url(meta)
        parts.append({"inlineData": {"mimeType": media_type, "data": base64_data}})

    response = requests.post(
        endpoint,
        params={"key": str(api_preset.get("apiKey") or "")},
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "temperature": _float_value(api_preset.get("temperature"), 0.9),
                "maxOutputTokens": _int_value(api_preset.get("maxTokens"), 16000),
            },
        },
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    data = response.json()
    if not response.ok:
        raise RuntimeError(data.get("error", {}).get("message") or f"Gemini HTTP {response.status_code}")
    texts = []
    for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", []):
        if part.get("text"):
            texts.append(part["text"])
    if not texts:
        raise RuntimeError("Gemini 未返回文本内容")
    return "\n".join(texts)


def call_text_model(api_preset: dict, user_prompt: str, image_data_url: str | None = None) -> str:
    provider = str(api_preset.get("provider") or "openai_compatible").strip().lower()
    if provider == "anthropic":
        return _call_anthropic(api_preset, user_prompt, image_data_url=image_data_url)
    if provider == "gemini":
        return _call_gemini(api_preset, user_prompt, image_data_url=image_data_url)
    return _call_openai_compatible(api_preset, user_prompt, image_data_url=image_data_url)


def analyze_audience_screenshot(api_preset: dict, image_data_url: str) -> dict[str, Any]:
    if not str(image_data_url or "").startswith("data:image/"):
        raise ValueError("截图数据无效，请重新选择受众截图")

    output_language = str(api_preset.get("outputLanguage") or "zh-TW")
    ui_language, _ = language_meta(output_language)
    analysis_preset = clone_json(api_preset)
    image_base_url = str(api_preset.get("imageBaseUrl") or "").strip()
    image_model = str(api_preset.get("imageModel") or "").strip()
    image_api_key = str(api_preset.get("imageApiKey") or api_preset.get("apiKey") or "").strip()
    if image_base_url and image_model:
        analysis_preset["baseUrl"] = image_base_url
        analysis_preset["model"] = image_model
        analysis_preset["apiKey"] = image_api_key
        lowered_image_base = image_base_url.lower()
        if "generativelanguage.googleapis.com" in lowered_image_base:
            analysis_preset["provider"] = "gemini"
        elif "anthropic.com" in lowered_image_base:
            analysis_preset["provider"] = "anthropic"
        elif "chat/completions" in lowered_image_base or "/v1/" in lowered_image_base:
            analysis_preset["provider"] = "openai_compatible"

    if not analysis_preset.get("baseUrl") or not analysis_preset.get("apiKey") or not analysis_preset.get("model"):
        raise ValueError("当前 API 模板缺少可用的截图识别模型配置（baseUrl / apiKey / model）")

    analysis_preset["temperature"] = "0.1"
    analysis_preset["maxTokens"] = str(
        min(
            AUDIENCE_ANALYSIS_MAX_TOKENS,
            _int_value(api_preset.get("maxTokens"), AUDIENCE_ANALYSIS_MAX_TOKENS),
        )
    )
    prompt = (
        "你是 YouTube Analytics 受众截图解析器。"
        "只根据截图里明确可见的信息返回严格 JSON，禁止臆测，禁止输出 markdown。\n"
        f"summary 字段请使用 {ui_language}，其余字段按原始截图中的标签语义归类即可。\n"
        "JSON schema:\n"
        "{\n"
        '  "devices": [{"name": "Mobile", "percent": 72.1}],\n'
        '  "age": [{"range": "18-24", "percent": 12.3}],\n'
        '  "gender": [{"name": "男性", "percent": 61.2}],\n'
        '  "regions": [{"name": "台湾", "percent": 34.5}],\n'
        '  "summary": "一句话总结主要受众信号"\n'
        "}\n"
        "要求:\n"
        "1. 只提取截图里能看见的数据。\n"
        "2. percent 必须返回数字，不要带 % 符号。\n"
        "3. 没识别到的维度返回空数组。\n"
        "4. 如果截图内容不完整，也不要补猜。"
    )

    raw = call_text_model(analysis_preset, prompt, image_data_url=image_data_url)
    try:
        payload = _parse_audience_json(raw)
    except Exception:
        payload = _repair_audience_json(raw, analysis_preset)

    return {
        "raw_text": raw,
        "parsed": payload,
        "formatted_text": format_audience_analysis(payload),
        "audience_summary": build_audience_summary(payload),
    }


def _repair_output_if_needed(
    parsed: dict[str, Any],
    api_preset: dict,
    content_template: dict,
    image_data_url: str | None = None,
) -> dict[str, Any]:
    issues = _validate_output(parsed, content_template)
    if not issues:
        return parsed

    for _ in range(3):
        repair_prompt = (
            "请修正下方JSON，使其严格符合限制并只输出修正后的JSON。\n"
            f"限制: {json.dumps(content_template, ensure_ascii=False)}\n"
            f"问题: {' | '.join(issues)}\n"
            f"主提示词: {render_master_prompt(content_template)}\n"
            f"原JSON: {json.dumps(parsed, ensure_ascii=False)}"
        )
        repair_preset = clone_json(api_preset)
        repair_preset["temperature"] = "0.3"
        repaired_raw = call_text_model(repair_preset, repair_prompt, image_data_url=image_data_url)
        parsed = _parse_json_like(repaired_raw)
        issues = _validate_output(parsed, content_template)
        if not issues:
            return parsed

    raise RuntimeError("模型输出多轮修正后仍不满足限制")


def load_generation_context(
    prompt_studio_path: Path,
    tag: str,
    *,
    api_preset_name: str | None = None,
    content_template_name: str | None = None,
) -> tuple[dict, dict, dict]:
    prompt_cfg = load_prompt_studio_config(prompt_studio_path)
    api_presets = prompt_cfg.get("apiPresets", {})
    content_templates = prompt_cfg.get("contentTemplates", {})

    chosen_api_name = api_preset_name or prompt_cfg.get("defaultApiPreset") or next(iter(api_presets), "")
    if chosen_api_name not in api_presets:
        raise ValueError(f"未找到 API 模板: {chosen_api_name}")

    chosen_content_name = content_template_name or pick_content_template_name(prompt_cfg, tag)
    if chosen_content_name not in content_templates:
        raise ValueError(f"未找到内容模板: {chosen_content_name}")

    return prompt_cfg, clone_json(api_presets[chosen_api_name]), clone_json(content_templates[chosen_content_name])


def generate_content_bundle(
    prompt_studio_path: Path,
    tag: str,
    *,
    is_ypp: bool = False,
    image_data_url: str | None = None,
    api_preset_name: str | None = None,
    content_template_name: str | None = None,
) -> dict[str, Any]:
    _, api_preset, content_template = load_generation_context(
        prompt_studio_path,
        tag,
        api_preset_name=api_preset_name,
        content_template_name=content_template_name,
    )

    # 上传落地时按频道只需要 1 套；YPP 需要 3 套 A/B 标题和更多缩略图。
    content_template["titleCount"] = "3" if is_ypp else "1"
    content_template["descCount"] = "1"
    content_template["thumbCount"] = "3" if is_ypp else "1"

    if not api_preset.get("baseUrl") or not api_preset.get("apiKey") or not api_preset.get("model"):
        raise ValueError("当前 API 模板缺少 baseUrl / apiKey / model，无法生成内容")

    user_prompt = _build_generation_prompt(content_template, image_data_url=image_data_url)
    raw = call_text_model(api_preset, user_prompt, image_data_url=image_data_url)
    parsed = _parse_json_like(raw)
    parsed = _repair_output_if_needed(parsed, api_preset, content_template, image_data_url=image_data_url)

    return {
        "api_preset": api_preset,
        "content_template": content_template,
        "raw_text": raw,
        "used_angle": parsed.get("usedAngle", content_template.get("angle", "")),
        "titles": [str(item).strip() for item in parsed.get("titles", []) if str(item).strip()],
        "descriptions": [str(item).strip() for item in parsed.get("descriptions", []) if str(item).strip()],
        "seo_hashtags": [str(item).strip() for item in parsed.get("seoHashtags", []) if str(item).strip()],
        "tag_list": [str(item).strip() for item in parsed.get("tagList", []) if str(item).strip()],
        "thumbnail_prompts": [
            {
                "for_title": str((item or {}).get("forTitle") or "").strip(),
                "prompt": str((item or {}).get("prompt") or "").strip(),
            }
            for item in parsed.get("thumbnails", [])
            if str((item or {}).get("prompt") or "").strip()
        ],
    }


def call_image_model(api_preset: dict, prompt: str) -> dict[str, Any]:
    base_url = str(api_preset.get("imageBaseUrl") or "").strip()
    api_key = str(api_preset.get("imageApiKey") or api_preset.get("apiKey") or "").strip()
    model = str(api_preset.get("imageModel") or "").strip()
    if not base_url or not api_key or not model:
        raise ValueError("当前图片 API 模板缺少 imageBaseUrl / imageApiKey / imageModel")

    response = requests.post(
        base_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json={
            "model": model,
            "modalities": ["text", "image"],
            "messages": [{"role": "user", "content": str(prompt or "").strip()}],
        },
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    raw = response.text
    data = response.json()
    if not response.ok:
        raise RuntimeError(data.get("error", {}).get("message") or f"图片接口 HTTP {response.status_code}")
    content = data.get("choices", [{}])[0].get("message", {}).get("content")
    data_url = _extract_data_url(content) or _extract_data_url(data)
    return {
        "data_url": data_url,
        "text": content if isinstance(content, str) else json.dumps(content or "", ensure_ascii=False),
        "raw": raw,
    }


def save_data_url_image(data_url: str, target_path: Path) -> Path:
    match = re.match(r"data:image/[^;]+;base64,(.+)", str(data_url or ""), flags=re.S)
    if not match:
        raise ValueError("图片接口未返回 base64 data URL")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(base64.b64decode(match.group(1)))
    return target_path
