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
import difflib
import hashlib
import json
import re
import time
import urllib.parse
import unicodedata
from pathlib import Path
from typing import Any, Callable

import requests

from prompt_studio import (
    clone_json,
    language_meta,
    load_prompt_studio_config,
    normalize_tag_key,
    parse_tag_range,
    pick_api_preset_name,
    pick_content_template_name,
    render_master_prompt,
)


DEFAULT_TIMEOUT_SECONDS = 70
AUDIENCE_ANALYSIS_MAX_TOKENS = 1400
METADATA_HISTORY_FILE = Path(__file__).parent / "data" / "metadata_history.json"


def _history_tag_key(tag: str) -> str:
    normalized = normalize_tag_key(tag)
    return normalized or str(tag).strip()


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


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def _post_json(
    url: str,
    *,
    headers: dict[str, Any] | None = None,
    json_payload: dict[str, Any] | None = None,
    timeout: int | float = DEFAULT_TIMEOUT_SECONDS,
    params: dict[str, Any] | None = None,
):
    with requests.Session() as session:
        # 某些第三方网关在系统代理环境下会随机断开，这里统一直连。
        session.trust_env = False
        return session.post(
            url,
            headers=headers,
            json=json_payload,
            timeout=timeout,
            params=params,
        )


def load_metadata_history(path: Path = METADATA_HISTORY_FILE) -> dict[str, Any]:
    raw = _read_json(path, {"tags": {}})
    if not isinstance(raw, dict):
        return {"tags": {}}
    tags = raw.get("tags")
    if not isinstance(tags, dict):
        raw["tags"] = {}
    return raw


def get_recent_metadata_history(tag: str, *, limit: int = 20, path: Path = METADATA_HISTORY_FILE) -> dict[str, list[str]]:
    data = load_metadata_history(path)
    rows = data.get("tags", {}).get(_history_tag_key(tag), [])
    if not isinstance(rows, list):
        rows = []
    rows = rows[-max(1, limit):]
    titles = [str(item.get("title") or "").strip() for item in rows if isinstance(item, dict) and str(item.get("title") or "").strip()]
    descriptions = [str(item.get("description") or "").strip() for item in rows if isinstance(item, dict) and str(item.get("description") or "").strip()]
    thumbnail_prompts = [
        str(prompt).strip()
        for item in rows
        if isinstance(item, dict)
        for prompt in (item.get("thumbnail_prompts") or [])
        if str(prompt).strip()
    ]
    tag_signatures = [
        str(item.get("tag_signature") or "").strip()
        for item in rows
        if isinstance(item, dict) and str(item.get("tag_signature") or "").strip()
    ]
    return {
        "titles": titles,
        "descriptions": descriptions,
        "thumbnail_prompts": thumbnail_prompts,
        "tag_signatures": tag_signatures,
    }


def append_metadata_history(
    *,
    tag: str,
    title: str,
    description: str,
    tag_list: list[str],
    thumbnail_prompts: list[str],
    path: Path = METADATA_HISTORY_FILE,
    keep_per_tag: int = 240,
) -> None:
    data = load_metadata_history(path)
    tags = data.setdefault("tags", {})
    tag_key = _history_tag_key(tag)
    rows = tags.setdefault(tag_key, [])
    if not isinstance(rows, list):
        rows = []
        tags[tag_key] = rows
    rows.append(
        {
            "title": str(title or "").strip(),
            "description": str(description or "").strip(),
            "tag_signature": " | ".join(str(item).strip() for item in tag_list if str(item).strip()),
            "thumbnail_prompts": [str(item).strip() for item in thumbnail_prompts if str(item).strip()],
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    )
    tags[tag_key] = rows[-max(20, keep_per_tag):]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_openai_chat_urls(base_url: str) -> list[str]:
    clean = str(base_url or "").strip().rstrip("/")
    if not clean:
        return []

    ordered: list[str] = []

    def add(url: str) -> None:
        if url and url not in ordered:
            ordered.append(url)

    parsed = urllib.parse.urlparse(clean)
    host = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip("/")

    if host.endswith("right.codes") and clean.endswith("/chat/completions") and not clean.endswith("/v1/chat/completions"):
        root = clean.removesuffix("/chat/completions")
        add(f"{root}/v1/chat/completions")
        add(clean)
    elif clean.endswith("/v1/chat/completions") or clean.endswith("/chat/completions"):
        add(clean)
    elif clean.endswith("/v1"):
        add(f"{clean}/chat/completions")
        add(clean)
    elif clean.endswith("/models"):
        root = clean.removesuffix("/models")
        add(f"{root}/v1/chat/completions")
        add(f"{root}/chat/completions")
        add(clean)
    elif path:
        add(f"{clean}/v1/chat/completions")
    else:
        add(f"{clean}/v1/chat/completions")
        add(f"{clean}/chat/completions")
        add(clean)
    return ordered


def _extract_openai_response_text(data: Any) -> str:
    if not isinstance(data, dict):
        return ""

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else {}
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, str) and item.strip():
                    text_parts.append(item.strip())
                    continue
                if not isinstance(item, dict):
                    continue
                candidate = str(item.get("text") or item.get("content") or "").strip()
                if candidate:
                    text_parts.append(candidate)
            if text_parts:
                return "\n".join(text_parts)
        direct_text = str(first.get("text") or "").strip()
        if direct_text:
            return direct_text

    output_text = str(data.get("output_text") or "").strip()
    if output_text:
        return output_text

    response_payload = data.get("response")
    if isinstance(response_payload, dict):
        response_text = str(response_payload.get("output_text") or "").strip()
        if response_text:
            return response_text

    return ""


def _describe_openai_error(*, status_code: int, data: Any, url: str) -> str:
    if isinstance(data, dict):
        error_payload = data.get("error")
        if isinstance(error_payload, dict):
            message = str(error_payload.get("message") or "").strip()
            if message:
                return f"{message} | url={url} | status={status_code}"
        if error_payload:
            return f"{error_payload} | url={url} | status={status_code}"
        message = str(data.get("message") or "").strip()
        if message:
            return f"{message} | url={url} | status={status_code}"
        raw_text = str(data.get("raw_text") or "").strip()
        if raw_text:
            return f"{raw_text[:240]} | url={url} | status={status_code}"
    return f"HTTP {status_code} | url={url}"


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

    if isinstance(parsed, dict) and isinstance(parsed.get("data"), dict):
        wrapped = parsed.get("data") or {}
        if any(key in wrapped for key in ("titles", "descriptions", "seoHashtags", "tagList", "thumbnails", "usedAngle")):
            parsed = wrapped

    if not isinstance(parsed, dict):
        raise ValueError("模型返回的 JSON 不是对象")

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


def _build_generation_prompt(
    content_template: dict,
    image_data_url: str | None = None,
    *,
    unique_seed: str = "",
    avoid_titles: list[str] | None = None,
    avoid_descriptions: list[str] | None = None,
    avoid_thumbnail_prompts: list[str] | None = None,
    avoid_tag_signatures: list[str] | None = None,
) -> str:
    tag_range = parse_tag_range(str(content_template.get("tagRange") or "10-20"))
    output_language = str(content_template.get("outputLanguage") or "zh-TW")
    language_ui, language_english = language_meta(output_language)

    payload = {
        "musicGenre": content_template.get("musicGenre", ""),
        "angle": content_template.get("angle", ""),
        "audience": content_template.get("audience", ""),
        "uniqueSeed": str(unique_seed or "").strip(),
        "recentlyUsedExamples": {
            "titles": [str(item).strip() for item in (avoid_titles or []) if str(item).strip()][:8],
            "descriptions": [str(item).strip() for item in (avoid_descriptions or []) if str(item).strip()][:4],
            "thumbnailPrompts": [str(item).strip() for item in (avoid_thumbnail_prompts or []) if str(item).strip()][:4],
            "tagSignatures": [str(item).strip() for item in (avoid_tag_signatures or []) if str(item).strip()][:4],
        },
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
        "本次输出必须和 recentlyUsedExamples 明显不同，禁止复用相同标题、相同简介、相同标签组合、相同缩略图指令。\n"
        "uniqueSeed 是这支视频的唯一指纹，你必须把它作为创意分流依据，确保每支视频都产出新的标题、简介、标签和缩略图方案。\n"
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


def _build_compact_generation_prompt(
    content_template: dict,
    image_data_url: str | None = None,
    *,
    unique_seed: str = "",
    avoid_titles: list[str] | None = None,
    avoid_descriptions: list[str] | None = None,
    avoid_thumbnail_prompts: list[str] | None = None,
    avoid_tag_signatures: list[str] | None = None,
) -> str:
    tag_range = parse_tag_range(str(content_template.get("tagRange") or "10-20"))
    output_language = str(content_template.get("outputLanguage") or "zh-TW")
    language_ui, language_english = language_meta(output_language)
    title_count = _int_value(content_template.get("titleCount"), 1)
    desc_count = _int_value(content_template.get("descCount"), 1)
    thumb_count = _int_value(content_template.get("thumbCount"), 1)
    angle_text = str(content_template.get("angle") or "").strip()
    audience_text = str(content_template.get("audience") or "").strip()
    genre_text = str(content_template.get("musicGenre") or "").strip()
    title_library = str(content_template.get("titleLibrary") or "").strip()
    master_prompt = str(render_master_prompt(content_template) or "").strip()
    payload = {
        "musicGenre": genre_text,
        "angle": angle_text,
        "audience": audience_text,
        "masterPrompt": master_prompt,
        "titleLibrary": title_library,
        "seed": str(unique_seed or "").strip(),
        "titleCount": title_count,
        "descriptionCount": desc_count,
        "thumbnailCount": thumb_count,
        "tagCountMin": tag_range[0],
        "tagCountMax": tag_range[1],
        "language": language_ui,
        "thumbnailTextLanguage": language_english,
        "imageProvided": bool(image_data_url),
    }
    return (
        "Return strict JSON only.\n"
        "Task: create the full YouTube metadata bundle for one music video in a single response.\n"
        "All titles, descriptions and tags must be Traditional Chinese.\n"
        f"Every thumbnail prompt must end with: Use {language_english} text in the image.\n"
        "Use the provided musicGenre, angle, and audience exactly as given. Do not rewrite, replace, soften, or auto-create them.\n"
        "Treat masterPrompt as the highest-priority instruction.\n"
        "Use titleLibrary as a style reference and flavor guide, but do not copy it verbatim.\n"
        "Keep titles clearly different from each other.\n"
        "Return the whole result in one valid JSON object.\n\n"
        f"Input:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        "Need exact JSON schema:\n"
        '{"usedAngle":"string","titles":["string"],"descriptions":["string"],"seoHashtags":["#tag"],"tagList":["keyword"],"thumbnails":[{"forTitle":"string","prompt":"string"}]}'
    )


def _trim_avoid_values(values: list[str] | None, *, limit: int = 12) -> list[str]:
    cleaned = [str(item or "").strip() for item in (values or []) if str(item or "").strip()]
    return cleaned[-max(1, limit):]


def _compact_master_rules(content_template: dict, *, limit: int = 480) -> str:
    raw = str(render_master_prompt(content_template) or "").strip()
    if not raw:
        return ""
    cut_markers = [
        "CASE PATCH LOCK",
        "案例补丁",
        "案例補丁",
        "案例补丁来了",
        "案例補丁來啦",
        "【案例补丁",
        "【案例補丁",
    ]
    lowered = raw.lower()
    cut_positions = [lowered.find(marker.lower()) for marker in cut_markers if lowered.find(marker.lower()) >= 0]
    if cut_positions:
        raw = raw[: min(cut_positions)]
    compact = re.sub(r"\s+", " ", raw).strip()
    return compact[:limit]


def _metadata_style_summary(content_template: dict) -> str:
    summary_parts = [
        f"musicGenre={str(content_template.get('musicGenre') or '').strip()}",
        f"angle={str(content_template.get('angle') or '').strip()}",
        f"audience={str(content_template.get('audience') or '').strip()}",
        f"language={str(content_template.get('outputLanguage') or 'zh-TW').strip()}",
    ]
    compact_rules = _compact_master_rules(content_template)
    if compact_rules:
        summary_parts.append(f"rules={compact_rules}")
    return " | ".join(part for part in summary_parts if part and not part.endswith("="))


def _compact_title_library(content_template: dict, *, limit: int = 480) -> str:
    raw = str(content_template.get("titleLibrary") or "").strip()
    if not raw:
        return ""
    compact = re.sub(r"\s+", " ", raw).strip()
    return compact[:limit]


def _extract_opening_fragments(values: list[str] | None, *, limit: int = 8) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        text = str(item or "").strip()
        if not text:
            continue
        opening = re.split(r"[｜|:：，,。.!！？；;、]", text, maxsplit=1)[0].strip()
        if len(opening) > 24:
            opening = opening[:24].rstrip()
        normalized = _normalize_generation_text(opening)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        fragments.append(opening)
        if len(fragments) >= max(1, limit):
            break
    return fragments


def _call_json_stage(
    *,
    api_preset: dict,
    prompt: str,
    stage: str,
    image_data_url: str | None = None,
    max_tokens_cap: int = 900,
) -> tuple[str, dict[str, Any]]:
    provider = str(api_preset.get("provider") or "").strip().lower()
    request_preset = clone_json(api_preset)
    request_preset["maxTokens"] = str(min(_int_value(api_preset.get("maxTokens"), 16000), max_tokens_cap))
    stage_presets: list[dict[str, Any]] = [request_preset]

    last_exc: Exception | None = None
    raw = ""
    for preset_index, active_preset in enumerate(stage_presets, 1):
        active_base_url = str(active_preset.get("baseUrl") or "").strip().lower()
        active_model = str(active_preset.get("model") or "").strip().lower()
        if (
            stage == "metadata_bundle"
            and provider == "openai_compatible"
            and "right.codes" in active_base_url
            and active_model == "gpt-5.4-xhigh"
        ):
            max_attempts = 1
        else:
            max_attempts = 3
        for api_attempt in range(1, max_attempts + 1):
            try:
                raw = call_text_model(active_preset, prompt, image_data_url=image_data_url)
                parsed = _parse_json_like(raw)
                if not isinstance(parsed, dict):
                    raise RuntimeError(f"{stage} did not return a JSON object")
                return raw, parsed
            except Exception as exc:
                last_exc = exc
                if api_attempt < max_attempts and _is_transient_text_api_error(exc):
                    time.sleep(2 * api_attempt)
                    continue
                break
        if preset_index < len(stage_presets):
            continue
    raise RuntimeError(
        f"文案生成失败: stage={stage} provider={provider or 'unknown'} error={last_exc}"
    ) from last_exc


def _build_title_stage_prompt(
    content_template: dict,
    *,
    unique_seed: str,
    avoid_titles: list[str] | None,
    count: int,
) -> str:
    title_min = _int_value(content_template.get("titleMin"), 80)
    title_max = _int_value(content_template.get("titleMax"), 95)
    language_ui, _ = language_meta(str(content_template.get("outputLanguage") or "zh-TW"))
    master_prompt = str(render_master_prompt(content_template) or "").strip()
    title_library = str(content_template.get("titleLibrary") or "").strip()
    payload = {
        "seed": str(unique_seed or "").strip(),
        "count": int(max(1, count)),
        "musicGenre": str(content_template.get("musicGenre") or "").strip(),
        "angle": str(content_template.get("angle") or "").strip(),
        "audience": str(content_template.get("audience") or "").strip(),
        "language": language_ui,
        "titleLength": {"min": title_min, "max": title_max},
        "masterPrompt": master_prompt,
        "titleLibrary": title_library,
        "styleSummary": _metadata_style_summary(content_template),
    }
    return (
        "Return strict JSON only.\n"
        "You are generating YouTube music video titles.\n"
        "All titles must be Traditional Chinese.\n"
        "Every title must feel genuinely different in angle, not just paraphrased.\n"
        "Treat masterPrompt as the hard creative constraint.\n"
        "Use titleLibrary as a style reference and inspiration source, but do not copy it verbatim.\n"
        "Do not output the same opening clause across multiple titles.\n"
        "Treat the seed as a hard uniqueness constraint: produce a fresh hook and a fresh opening clause.\n"
        "Make the titles emotionally vivid, specific, and mature.\n\n"
        f"Input:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        'Return JSON schema: {"usedAngle":"string","titles":["string"]}'
    )


def _build_description_stage_prompt(
    content_template: dict,
    *,
    unique_seed: str,
    title: str,
    avoid_descriptions: list[str] | None,
    avoid_tag_signatures: list[str] | None,
) -> str:
    tag_range = parse_tag_range(str(content_template.get("tagRange") or "10-20"))
    language_ui, _ = language_meta(str(content_template.get("outputLanguage") or "zh-TW"))
    master_prompt = str(render_master_prompt(content_template) or "").strip()
    title_library = str(content_template.get("titleLibrary") or "").strip()
    payload = {
        "seed": str(unique_seed or "").strip(),
        "title": str(title or "").strip(),
        "musicGenre": str(content_template.get("musicGenre") or "").strip(),
        "angle": str(content_template.get("angle") or "").strip(),
        "audience": str(content_template.get("audience") or "").strip(),
        "language": language_ui,
        "descriptionLength": _int_value(content_template.get("descLen"), 300),
        "tagCount": {"min": tag_range[0], "max": tag_range[1]},
        "masterPrompt": master_prompt,
        "titleLibrary": title_library,
        "styleSummary": _metadata_style_summary(content_template),
    }
    return (
        "Return strict JSON only.\n"
        "You are generating one YouTube description and one SEO tag list for a music video.\n"
        "Description and tags must be Traditional Chinese.\n"
        "Treat masterPrompt as the hard content constraint.\n"
        "Use titleLibrary to stay aligned with the intended channel tone and headline flavor.\n"
        "Make the wording natural, emotionally coherent, and clearly matched to the title.\n\n"
        f"Input:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        'Return JSON schema: {"descriptions":["string"],"seoHashtags":["#tag"],"tagList":["keyword"]}'
    )


def _build_thumbnail_prompt_stage(
    content_template: dict,
    *,
    unique_seed: str,
    titles: list[str],
    description: str,
    avoid_thumbnail_prompts: list[str] | None,
    count: int,
) -> str:
    _, language_english = language_meta(str(content_template.get("outputLanguage") or "zh-TW"))
    master_prompt = str(render_master_prompt(content_template) or "").strip()
    title_library = str(content_template.get("titleLibrary") or "").strip()
    payload = {
        "seed": str(unique_seed or "").strip(),
        "titles": [str(item or "").strip() for item in titles if str(item or "").strip()][: max(1, count)],
        "description": str(description or "").strip(),
        "musicGenre": str(content_template.get("musicGenre") or "").strip(),
        "angle": str(content_template.get("angle") or "").strip(),
        "audience": str(content_template.get("audience") or "").strip(),
        "thumbnailTextLanguage": language_english,
        "masterPrompt": master_prompt,
        "titleLibrary": title_library,
        "styleSummary": _metadata_style_summary(content_template),
    }
    return (
        "Return strict JSON only.\n"
        "Generate thumbnail image prompts for a music video.\n"
        "Each prompt must describe a clear image concept and must end with "
        f"'Use {language_english} text in the image.'\n"
        "Treat masterPrompt as the hard creative constraint.\n"
        "Use titleLibrary to inherit the channel's proven title/thumbnail flavor, but do not copy it literally.\n"
        "Make each prompt visually clear, elegant, and directly usable by an image model.\n\n"
        f"Input:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        'Return JSON schema: {"thumbnails":[{"forTitle":"string","prompt":"string"}]}'
    )


def _fit_text_length(text: str, *, min_len: int, max_len: int, filler: str) -> str:
    result = str(text or "").strip()
    extra = str(filler or "").strip()
    while _count_chars(result) < min_len and extra:
        result = f"{result}{extra}"
    if _count_chars(result) > max_len:
        trimmed = []
        for ch in result:
            candidate = "".join(trimmed) + ch
            if _count_chars(candidate) > max_len:
                break
            trimmed.append(ch)
        result = "".join(trimmed).rstrip("｜，、。 ")
    return result


def _normalize_generation_text(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "").strip().lower())
    collapsed = "".join(raw.split())
    return re.sub(r"[\W_]+", "", collapsed, flags=re.UNICODE)


def _looks_like_blocked_duplicate(candidate: str, blocked_values: list[str] | None = None) -> bool:
    normalized_candidate = _normalize_generation_text(candidate)
    if not normalized_candidate:
        return False
    prefix = normalized_candidate[:32]
    for blocked in blocked_values or []:
        normalized_blocked = _normalize_generation_text(blocked)
        if not normalized_blocked:
            continue
        if normalized_candidate == normalized_blocked:
            return True
        if prefix and prefix == normalized_blocked[:32]:
            return True
        similarity = difflib.SequenceMatcher(None, normalized_candidate, normalized_blocked).ratio()
        if similarity >= 0.93:
            return True
    return False


def _ensure_unique_generated_values(
    *,
    label: str,
    values: list[str],
    blocked_values: list[str] | None = None,
) -> None:
    normalized_values = [_normalize_generation_text(item) for item in values if _normalize_generation_text(item)]
    if len(normalized_values) != len(set(normalized_values)):
        raise RuntimeError(f"API returned duplicate {label} values in the same response.")
    duplicated_blocked = [
        item
        for item in values
        if str(item or "").strip() and _looks_like_blocked_duplicate(str(item), blocked_values)
    ]
    if duplicated_blocked:
        raise RuntimeError(f"API returned a blocked duplicate {label} value.")


def _pick_unique_generation_text(
    candidates: list[str],
    avoid_values: list[str] | None,
    fallback: str,
) -> str:
    cleaned = [str(item or "").strip() for item in candidates if str(item or "").strip()]
    for item in cleaned:
        if not _looks_like_blocked_duplicate(item, avoid_values):
            return item
    return cleaned[0] if cleaned else str(fallback or "").strip()


def _seed_number(unique_seed: str) -> int:
    raw = str(unique_seed or "").strip()
    if not raw:
        raw = f"default-{time.time_ns()}"
    return int(hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12], 16)


def _rotate_pick(items: list[str], seed: int, index: int) -> str:
    if not items:
        return ""
    return items[(seed + index) % len(items)]


def _build_local_fallback_output(
    content_template: dict,
    *,
    is_ypp: bool = False,
    unique_seed: str = "",
    avoid_titles: list[str] | None = None,
    avoid_descriptions: list[str] | None = None,
    avoid_thumbnail_prompts: list[str] | None = None,
    avoid_tag_signatures: list[str] | None = None,
) -> dict[str, Any]:
    raise RuntimeError("Local metadata fallback has been retired. API generation is required.")
    genre = str(content_template.get("musicGenre") or "背景音樂").strip() or "背景音樂"
    audience_text = str(content_template.get("audience") or "").strip()
    audience_hint = "台灣熟齡聽眾" if ("台灣" in audience_text or "台湾" in audience_text or "65" in audience_text) else "喜歡耐聽旋律的人"
    seed = _seed_number(unique_seed)
    title_count = 3 if is_ypp else 1
    desc_count = 1
    thumb_count = 3 if is_ypp else 1
    title_min = _int_value(content_template.get("titleMin"), 80)
    title_max = _int_value(content_template.get("titleMax"), 95)
    desc_len = _int_value(content_template.get("descLen"), 300)
    tag_min, tag_max = parse_tag_range(str(content_template.get("tagRange") or "10-20"))

    title_seed_pool = [
        f"找了很久，終於又聽見這首{genre}｜寫給{audience_hint}的夜晚、客廳與回憶慢慢安靜下來的優雅旋律",
        f"前奏一響，就像把多年以前那段時光找回來｜這首{genre}寫給{audience_hint}，越聽越耐聽",
        f"不是熱鬧，是終於找到對味的陪伴｜這首{genre}把熟悉、體面與溫柔慢慢放回你的夜晚",
        f"這首{genre}沒有故作熱鬧｜只把節奏放得剛剛好，讓{audience_hint}一聽就願意把夜晚留給它",
        f"真正耐聽的{genre}不是越快越好｜而是讓{audience_hint}在安靜裡慢慢把情緒放穩",
        f"如果你喜歡體面、溫柔又不吵的{genre}｜這首歌會像老朋友一樣慢慢陪你回到舒服的位置",
    ]
    title_filler = "｜前奏一響，就像把多年以前那段溫柔又體面的時光重新找回來"
    title_seeds = [_rotate_pick(title_seed_pool, seed, idx) for idx in range(max(title_count * 3, 8))]
    title_candidates = [
        _fit_text_length(item, min_len=title_min, max_len=title_max, filler=title_filler)
        for item in title_seeds
    ]
    titles: list[str] = []
    blocked_titles = [str(item).strip() for item in (avoid_titles or []) if str(item).strip()]
    for index in range(title_count):
        fallback_title = title_candidates[index % len(title_candidates)] if title_candidates else f"{genre} {index + 1}"
        chosen_title = _pick_unique_generation_text(
            title_candidates[index:],
            [*blocked_titles, *titles],
            fallback_title,
        )
        titles.append(chosen_title)

    desc_variants = [
        f"這支 {genre} 不是吵鬧取勝，而是把節奏、空氣感與情緒慢慢放回耳朵裡。",
        f"這首 {genre} 想做的不是把注意力搶走，而是把房間裡的氣氛悄悄安頓好。",
        f"如果你近來特別需要一種體面、乾淨、可以長時間陪伴的聲音，這支 {genre} 會很合適。",
        f"它不急著討好任何人，只是用穩定的旋律和空氣感，把夜晚留給真正想慢下來的人。",
    ]
    base_desc = (
        f"{_rotate_pick(desc_variants, seed, 0)}"
        f"如果你喜歡能長時間陪伴、不搶注意力、又越聽越耐聽的旋律，這首歌很適合在夜晚、閱讀、整理房間、泡茶或安靜放空時播放。"
        f"它特別適合 {audience_hint} 的聆聽節奏，不需要追趕，只要讓聲音穩穩地陪著你，把回憶、客廳和一天的情緒慢慢安放好。"
        f"{_rotate_pick(['如果你也喜歡這種乾淨、優雅、耐聽的音樂氛圍，歡迎把它留在你的播放清單裡。', '如果這種耐聽又不吵的聲音剛好對你的胃口，記得把它收藏起來，留給真正需要安靜的時候。', '若你也偏愛這種不炫技、卻越聽越有味道的旋律，這支影片會很值得你留著慢慢播。'], seed, 1)}"
    )
    descriptions = [
        _fit_text_length(base_desc, min_len=max(220, desc_len - 40), max_len=desc_len + 40, filler=" 讓旋律慢慢陪你把心情放回舒服的位置。")
        for _ in range(desc_count)
    ]

    tag_candidates = [
        genre,
        f"{genre}背景音樂",
        f"{genre}純音樂",
        "熟齡音樂",
        "台灣背景音樂",
        "懷舊音樂",
        "放鬆音樂",
        "閱讀音樂",
        "夜晚音樂",
        "客廳背景音樂",
        "優雅旋律",
        "耐聽音樂",
        "舒壓音樂",
        "安靜音樂",
        "泡茶音樂",
        "回憶感音樂",
        "溫柔背景音樂",
        "長時間播放音樂",
        "高質感背景音樂",
        "熟齡聽眾音樂",
        f"{genre}夜晚歌單",
        f"{genre}放空歌單",
        f"{genre}客廳音樂",
    ]
    rotated_tags = tag_candidates[seed % len(tag_candidates) :] + tag_candidates[: seed % len(tag_candidates)]
    tag_list = []
    for item in rotated_tags:
        if item not in tag_list:
            tag_list.append(item)
        if len(tag_list) >= tag_max:
            break
    if len(tag_list) < tag_min:
        tag_list.extend([f"{genre}推薦", f"{genre}收藏", f"{genre}歌單"][: max(0, tag_min - len(tag_list))])

    seo_hashtags = [f"#{item.replace(' ', '')}" for item in tag_list[: min(6, len(tag_list))]]
    thumbnails = []
    visual_styles = [
        "Warm cinematic lighting, refined atmosphere, premium composition.",
        "Elegant nostalgic mood, soft golden light, premium editorial composition.",
        "Moody living-room ambience, tasteful contrast, polished premium layout.",
        "Refined evening atmosphere, gentle glow, sophisticated music-channel composition.",
    ]
    for idx in range(thumb_count):
        title = titles[min(idx, len(titles) - 1)]
        headline = title.split("｜", 1)[0][:16]
        thumbnails.append(
            {
                "forTitle": title,
                "prompt": (
                    f"Create an elegant nostalgic YouTube thumbnail for {genre}. "
                    f"Audience: {audience_hint}. {_rotate_pick(visual_styles, seed, idx)} "
                    f"Main Traditional Chinese text: 「{headline}」. Use Traditional Chinese text in the image."
                ),
            }
        )

    return {
        "usedAngle": str(content_template.get("angle") or "").strip() or f"{genre} x {audience_hint}",
        "titles": titles,
        "descriptions": descriptions,
        "seoHashtags": seo_hashtags,
        "tagList": tag_list,
        "thumbnails": thumbnails,
    }


def _build_local_fallback_output_v2(
    content_template: dict,
    *,
    is_ypp: bool = False,
    unique_seed: str = "",
    avoid_titles: list[str] | None = None,
    avoid_descriptions: list[str] | None = None,
    avoid_thumbnail_prompts: list[str] | None = None,
    avoid_tag_signatures: list[str] | None = None,
) -> dict[str, Any]:
    raise RuntimeError("Local metadata fallback has been retired. API generation is required.")
    genre = str(content_template.get("musicGenre") or "背景音樂").strip() or "背景音樂"
    audience_text = str(content_template.get("audience") or "").strip()
    audience_hint = "台灣熟齡聽眾" if ("台灣" in audience_text or "台湾" in audience_text or "65" in audience_text) else "喜歡耐聽旋律的人"
    seed = _seed_number(unique_seed)
    title_count = 3 if is_ypp else 1
    desc_count = 1
    thumb_count = 3 if is_ypp else 1
    title_min = _int_value(content_template.get("titleMin"), 80)
    title_max = _int_value(content_template.get("titleMax"), 95)
    desc_len = _int_value(content_template.get("descLen"), 300)
    tag_min, tag_max = parse_tag_range(str(content_template.get("tagRange") or "10-20"))

    title_seed_pool = [
        f"找了很久，終於又聽見這首{genre}｜寫給{audience_hint}的夜晚、客廳與回憶慢慢安靜下來的優雅旋律",
        f"前奏一響，就像把多年以前那段時光找回來｜這首{genre}寫給{audience_hint}，越聽越耐聽",
        f"不是熱鬧，是終於找到對味的陪伴｜這首{genre}把熟悉、體面與溫柔慢慢放回你的夜晚",
        f"這首{genre}沒有故作熱鬧｜只把節奏放得剛剛好，讓{audience_hint}一聽就願意把夜晚留給它",
        f"真正耐聽的{genre}不是越快越好｜而是讓{audience_hint}在安靜裡慢慢把情緒放穩",
        f"如果你喜歡體面、溫柔又不吵的{genre}｜這首歌就像老朋友一樣慢慢陪你回到舒服的位置",
        f"今晚想把房間留給安靜的{genre}｜這支版本更適合{audience_hint}慢慢聽、慢慢想",
        f"有些{genre}不是拿來炫技｜而是讓{audience_hint}把一天的情緒安穩放下來",
    ]
    title_filler = "｜前奏一響，就像把多年以前那段溫柔又體面的時光重新找回來"
    title_candidates = [
        _fit_text_length(_rotate_pick(title_seed_pool, seed, idx), min_len=title_min, max_len=title_max, filler=title_filler)
        for idx in range(max(title_count * 4, 10))
    ]
    titles: list[str] = []
    blocked_titles = [str(item).strip() for item in (avoid_titles or []) if str(item).strip()]
    for index in range(title_count):
        fallback_title = title_candidates[index % len(title_candidates)] if title_candidates else f"{genre} {index + 1}"
        titles.append(
            _pick_unique_generation_text(
                title_candidates[index:],
                [*blocked_titles, *titles],
                fallback_title,
            )
        )

    desc_variants = [
        f"這支 {genre} 不是喧鬧取勝，而是把節奏、空氣感與情緒慢慢放回耳朵裡。",
        f"這首 {genre} 想做的不是把注意力搶走，而是把房間裡的氛圍悄悄安頓好。",
        f"如果你最近特別需要一段體面、輕盈、可以長時間陪伴的聲音，這支 {genre} 很適合。",
        f"它不急著討好任何人，只是用穩定的旋律和空氣感，把夜晚留給真正想慢下來的人。",
    ]
    closing_variants = [
        "如果你也喜歡這種不喧嘩、優雅、耐聽的音樂氛圍，歡迎把它留在你的播放清單裡。",
        "如果這種耐聽又不吵的聲音剛好對你的胃口，記得把它收藏起來，留給真正需要安靜的時候。",
        "若你也偏愛這種不炫技、卻越聽越有味道的旋律，這支影片很值得你留著慢慢播。",
        "如果你正想找一段夜晚能長時間放著的聊天感音樂，這支版本就很值得留下來作陪伴。",
    ]
    desc_candidates = [
        _fit_text_length(
            (
                f"{_rotate_pick(desc_variants, seed, idx)}"
                f"如果你喜歡能長時間陪伴、不搶注意力、又越聽越耐聽的旋律，這首歌很適合在夜晚、閱讀、整理房間、泡茶或安靜放空時播放。"
                f"它特別適合 {audience_hint} 的聆聽節奏，不需要追趕，只要讓聲音穩穩地陪著你，把回憶、客廳和一天的情緒慢慢安放好。"
                f"{_rotate_pick(closing_variants, seed, idx)}"
            ),
            min_len=max(220, desc_len - 40),
            max_len=desc_len + 40,
            filler=" 讓旋律慢慢陪你把心情放回舒服的位置。",
        )
        for idx in range(max(desc_count * 4, 8))
    ]
    descriptions: list[str] = []
    blocked_descriptions = [str(item).strip() for item in (avoid_descriptions or []) if str(item).strip()]
    for index in range(desc_count):
        fallback_description = desc_candidates[index % len(desc_candidates)] if desc_candidates else genre
        descriptions.append(
            _pick_unique_generation_text(
                desc_candidates[index:],
                [*blocked_descriptions, *descriptions],
                fallback_description,
            )
        )

    tag_candidates = [
        genre,
        f"{genre}背景音樂",
        f"{genre}純音樂",
        "熟齡音樂",
        "台灣背景音樂",
        "懷舊音樂",
        "放鬆音樂",
        "閱讀音樂",
        "夜晚音樂",
        "客廳背景音樂",
        "優雅旋律",
        "耐聽音樂",
        "舒壓音樂",
        "安靜音樂",
        "泡茶音樂",
        "回憶感音樂",
        "溫柔背景音樂",
        "長時間播放音樂",
        "高質感背景音樂",
        "熟齡聽眾音樂",
        f"{genre}夜晚歌單",
        f"{genre}放空歌單",
        f"{genre}客廳音樂",
    ]
    tag_pool: list[str] = []
    for item in tag_candidates[seed % len(tag_candidates):] + tag_candidates[: seed % len(tag_candidates)]:
        if item not in tag_pool:
            tag_pool.append(item)
    tag_list = tag_pool[:tag_max]
    blocked_signatures = [str(item).strip() for item in (avoid_tag_signatures or []) if str(item).strip()]
    blocked_signature_keys = {_normalize_generation_text(item) for item in blocked_signatures}
    for shift in range(len(tag_pool)):
        candidate_list = (tag_pool[shift:] + tag_pool[:shift])[:tag_max]
        if _normalize_generation_text(" | ".join(candidate_list)) not in blocked_signature_keys:
            tag_list = candidate_list
            break
    if len(tag_list) < tag_min:
        tag_list.extend([f"{genre}推薦", f"{genre}收藏", f"{genre}歌單"][: max(0, tag_min - len(tag_list))])

    seo_hashtags = [f"#{item.replace(' ', '')}" for item in tag_list[: min(6, len(tag_list))]]
    visual_styles = [
        "Warm cinematic lighting, refined atmosphere, premium composition.",
        "Elegant nostalgic mood, soft golden light, premium editorial composition.",
        "Moody living-room ambience, tasteful contrast, polished premium layout.",
        "Refined evening atmosphere, gentle glow, sophisticated music-channel composition.",
        "Painterly ghibli-inspired ambience, soft nostalgic interior, premium composition.",
        "Quiet late-night room, warm lamp glow, tasteful editorial layout.",
    ]
    prompt_candidates = [
        (
            f"Create an elegant nostalgic YouTube thumbnail for {genre}. "
            f"Audience: {audience_hint}. {_rotate_pick(visual_styles, seed, idx)} "
            f"Use Traditional Chinese text in the image."
        )
        for idx in range(max(thumb_count * 4, 8))
    ]
    thumbnails: list[dict[str, str]] = []
    blocked_prompts = [str(item).strip() for item in (avoid_thumbnail_prompts or []) if str(item).strip()]
    for idx in range(thumb_count):
        title = titles[min(idx, len(titles) - 1)]
        fallback_prompt = prompt_candidates[idx % len(prompt_candidates)] if prompt_candidates else f"Thumbnail for {genre}. Use Traditional Chinese text in the image."
        prompt = _pick_unique_generation_text(
            prompt_candidates[idx:],
            [*blocked_prompts, *[item.get('prompt', '') for item in thumbnails]],
            fallback_prompt,
        )
        thumbnails.append({"forTitle": title, "prompt": prompt})

    return {
        "usedAngle": str(content_template.get("angle") or "").strip() or f"{genre} x {audience_hint}",
        "titles": titles,
        "descriptions": descriptions,
        "seoHashtags": seo_hashtags,
        "tagList": tag_list,
        "thumbnails": thumbnails,
    }


def _validate_output(parsed: dict[str, Any], content_template: dict) -> list[str]:
    cfg = {
        "titleCount": _int_value(content_template.get("titleCount"), 3),
        "descCount": _int_value(content_template.get("descCount"), 1),
        "thumbCount": _int_value(content_template.get("thumbCount"), 3),
    }
    issues: list[str] = []
    if len(parsed.get("titles", [])) != cfg["titleCount"]:
        issues.append(f"标题数量应为 {cfg['titleCount']}，当前 {len(parsed.get('titles', []))}")
    if len(parsed.get("descriptions", [])) != cfg["descCount"]:
        issues.append(f"简介数量应为 {cfg['descCount']}，当前 {len(parsed.get('descriptions', []))}")
    if len(parsed.get("thumbnails", [])) != cfg["thumbCount"]:
        issues.append(f"缩略图指令数量应为 {cfg['thumbCount']}，当前 {len(parsed.get('thumbnails', []))}")
    for idx, item in enumerate(parsed.get("thumbnails", []), 1):
        prompt = str((item or {}).get("prompt") or "").lower()
        if "traditional chinese" not in prompt and "use traditional chinese text in the image" not in prompt:
            issues.append(f"缩略图指令{idx}缺少 Traditional Chinese 约束")
    return issues


def _call_openai_compatible(api_preset: dict, user_prompt: str, image_data_url: str | None = None) -> str:
    if image_data_url:
        content: Any = [{"type": "text", "text": user_prompt}]
        content.append({"type": "image_url", "image_url": {"url": image_data_url}})
    else:
        content = user_prompt

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
    prompt_chars = len(str(user_prompt or ""))
    token_budget = _int_value(api_preset.get("maxTokens"), 16000)
    timeout_seconds = 105
    if prompt_chars > 4000:
        timeout_seconds += 30
    if prompt_chars > 9000:
        timeout_seconds += 30
    if token_budget > 8000:
        timeout_seconds += 30
    base_url = str(api_preset.get("baseUrl") or "").strip().lower()
    model_name = str(api_preset.get("model") or "").strip().lower()
    if "right.codes" in base_url and model_name == "gpt-5.4-xhigh":
        timeout_seconds = max(timeout_seconds, 420)
    timeout_seconds = max(timeout_seconds, DEFAULT_TIMEOUT_SECONDS)
    timeout_seconds = min(timeout_seconds, 420)
    retryable_codes = {408, 429, 500, 502, 503, 504}
    for url in _build_openai_chat_urls(str(api_preset.get("baseUrl") or "")):
        for attempt in range(2):
            try:
                response = _post_json(
                    url,
                    headers=headers,
                    json_payload=payload,
                    timeout=timeout_seconds,
                )
                try:
                    data = response.json()
                except Exception:
                    data = {"raw_text": response.text}
                if response.ok:
                    text = _extract_openai_response_text(data)
                    if text:
                        return text
                    last_error = _describe_openai_error(
                        status_code=response.status_code,
                        data=data,
                        url=url,
                    )
                else:
                    last_error = _describe_openai_error(
                        status_code=response.status_code,
                        data=data,
                        url=url,
                    )
                if response.status_code not in retryable_codes:
                    break
            except Exception as exc:
                last_error = str(exc)
            if attempt < 1:
                time.sleep(2 + attempt)
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

    response = _post_json(
        str(api_preset.get("baseUrl") or ""),
        headers={
            "Content-Type": "application/json",
            "x-api-key": str(api_preset.get("apiKey") or ""),
            "anthropic-version": "2023-06-01",
        },
        json_payload={
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

    response = _post_json(
        endpoint,
        params={"key": str(api_preset.get("apiKey") or "")},
        headers={"Content-Type": "application/json"},
        json_payload={
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

    chosen_api_name = api_preset_name or pick_api_preset_name(prompt_cfg, tag)
    if chosen_api_name not in api_presets:
        raise ValueError(f"未找到 API 模板: {chosen_api_name}")

    chosen_content_name = content_template_name or pick_content_template_name(prompt_cfg, tag)
    if chosen_content_name not in content_templates:
        raise ValueError(f"未找到内容模板: {chosen_content_name}")

    return prompt_cfg, clone_json(api_presets[chosen_api_name]), clone_json(content_templates[chosen_content_name])


def _is_transient_text_api_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    hints = (
        "remote end closed connection",
        "remotedisconnected",
        "connection aborted",
        "connection reset",
        "connectionreseterror",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "502",
        "503",
        "504",
        "408",
        "429",
    )
    return any(hint in text for hint in hints)


def generate_content_bundle(
    prompt_studio_path: Path,
    tag: str,
    *,
    is_ypp: bool = False,
    image_data_url: str | None = None,
    api_preset_name: str | None = None,
    content_template_name: str | None = None,
    unique_seed: str = "",
    avoid_titles: list[str] | None = None,
    avoid_descriptions: list[str] | None = None,
    avoid_thumbnail_prompts: list[str] | None = None,
    avoid_tag_signatures: list[str] | None = None,
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    def emit(message: str) -> None:
        if log:
            try:
                log(message)
            except Exception:
                pass

    _, api_preset, content_template = load_generation_context(
        prompt_studio_path,
        tag,
        api_preset_name=api_preset_name,
        content_template_name=content_template_name,
    )

    if not api_preset.get("baseUrl") or not api_preset.get("apiKey") or not api_preset.get("model"):
        raise ValueError("当前 API 模板缺少 baseUrl / apiKey / model，无法生成内容")

    title_count = 3 if is_ypp else 1
    thumb_count = 3 if is_ypp else 1
    content_template["titleCount"] = str(title_count)
    content_template["descCount"] = "1"
    content_template["thumbCount"] = str(thumb_count)

    bundle_prompt = _build_compact_generation_prompt(
        content_template,
        image_data_url=image_data_url,
        unique_seed=unique_seed,
        avoid_titles=None,
        avoid_descriptions=None,
        avoid_thumbnail_prompts=None,
        avoid_tag_signatures=None,
    )
    emit(
        f"[API] {tag}: stage=metadata_bundle -> provider={api_preset.get('provider', '')} "
        f"model={api_preset.get('model', '')} chars={len(bundle_prompt)}"
    )
    raw_bundle, bundle_payload = _call_json_stage(
        api_preset=api_preset,
        prompt=bundle_prompt,
        stage="metadata_bundle",
        image_data_url=image_data_url,
        max_tokens_cap=2200,
    )
    emit(f"[API] {tag}: stage=metadata_bundle <- ok chars={len(raw_bundle)}")
    bundle_payload = _repair_output_if_needed(
        bundle_payload,
        api_preset,
        content_template,
        image_data_url=image_data_url,
    )
    used_angle = str(bundle_payload.get("usedAngle") or content_template.get("angle") or "").strip()
    titles = [str(item).strip() for item in bundle_payload.get("titles", []) if str(item).strip()]
    descriptions = [
        str(item).strip()
        for item in bundle_payload.get("descriptions", [])
        if str(item).strip()
    ]
    seo_hashtags = [
        str(item).strip()
        for item in bundle_payload.get("seoHashtags", [])
        if str(item).strip()
    ]
    tag_list = [
        str(item).strip()
        for item in bundle_payload.get("tagList", [])
        if str(item).strip()
    ]
    thumbnail_prompt_rows = [
        {
            "for_title": str((item or {}).get("forTitle") or "").strip(),
            "prompt": str((item or {}).get("prompt") or "").strip(),
        }
        for item in bundle_payload.get("thumbnails", [])
        if str((item or {}).get("prompt") or "").strip()
    ]
    if not titles:
        raise RuntimeError("文案生成失败: stage=metadata_bundle provider=api error=API 未返回标题")
    if not descriptions:
        raise RuntimeError("文案生成失败: stage=metadata_bundle provider=api error=API 未返回简介")
    if not tag_list:
        raise RuntimeError("文案生成失败: stage=metadata_bundle provider=api error=API 未返回标签")
    if not thumbnail_prompt_rows:
        raise RuntimeError("文案生成失败: stage=metadata_bundle provider=api error=API 未返回缩略图提示词")
    return {
        "api_preset": api_preset,
        "content_template": content_template,
        "raw_text": raw_bundle,
        "used_angle": used_angle,
        "titles": titles,
        "descriptions": descriptions,
        "seo_hashtags": seo_hashtags,
        "tag_list": tag_list,
        "generation_source": "api",
        "thumbnail_prompts": thumbnail_prompt_rows,
    }

    title_prompt = _build_title_stage_prompt(
        content_template,
        unique_seed=unique_seed,
        avoid_titles=avoid_titles,
        count=max(title_count + 2, 4),
    )
    emit(
        f"[API] {tag}: stage=titles -> provider={api_preset.get('provider', '')} "
        f"model={api_preset.get('model', '')} chars={len(title_prompt)}"
    )
    raw_titles, title_payload = _call_json_stage(
        api_preset=api_preset,
        prompt=title_prompt,
        stage="titles",
        image_data_url=image_data_url,
        max_tokens_cap=700,
    )
    emit(f"[API] {tag}: stage=titles <- ok chars={len(raw_titles)}")
    used_angle = str(title_payload.get("usedAngle") or content_template.get("angle") or "").strip()
    titles = [str(item).strip() for item in title_payload.get("titles", []) if str(item).strip()]
    if not titles:
        raise RuntimeError("文案生成失败: stage=titles provider=api error=API 未返回标题")
    primary_title = titles[0]
    description_prompt = _build_description_stage_prompt(
        content_template,
        unique_seed=f"{unique_seed}|desc",
        title=primary_title,
        avoid_descriptions=avoid_descriptions,
        avoid_tag_signatures=avoid_tag_signatures,
    )
    emit(
        f"[API] {tag}: stage=description_tags -> provider={api_preset.get('provider', '')} "
        f"model={api_preset.get('model', '')} chars={len(description_prompt)}"
    )
    raw_desc, description_payload = _call_json_stage(
        api_preset=api_preset,
        prompt=description_prompt,
        stage="description_tags",
        image_data_url=image_data_url,
        max_tokens_cap=900,
    )
    emit(f"[API] {tag}: stage=description_tags <- ok chars={len(raw_desc)}")
    descriptions = [
        str(item).strip()
        for item in description_payload.get("descriptions", [])
        if str(item).strip()
    ]
    seo_hashtags = [
        str(item).strip()
        for item in description_payload.get("seoHashtags", [])
        if str(item).strip()
    ]
    tag_list = [
        str(item).strip()
        for item in description_payload.get("tagList", [])
        if str(item).strip()
    ]
    if not descriptions:
        raise RuntimeError("文案生成失败: stage=description_tags provider=api error=API 未返回简介")
    if not tag_list:
        raise RuntimeError("文案生成失败: stage=description_tags provider=api error=API 未返回标签")
    thumb_prompt = _build_thumbnail_prompt_stage(
        content_template,
        unique_seed=f"{unique_seed}|thumb",
        titles=titles[: max(1, thumb_count)],
        description=descriptions[0],
        avoid_thumbnail_prompts=avoid_thumbnail_prompts,
        count=thumb_count,
    )
    emit(
        f"[API] {tag}: stage=thumbnail_prompts -> provider={api_preset.get('provider', '')} "
        f"model={api_preset.get('model', '')} chars={len(thumb_prompt)}"
    )
    raw_thumb, thumb_payload = _call_json_stage(
        api_preset=api_preset,
        prompt=thumb_prompt,
        stage="thumbnail_prompts",
        image_data_url=image_data_url,
        max_tokens_cap=900,
    )
    emit(f"[API] {tag}: stage=thumbnail_prompts <- ok chars={len(raw_thumb)}")
    thumbnail_prompt_rows = [
        {
            "for_title": str((item or {}).get("forTitle") or "").strip(),
            "prompt": str((item or {}).get("prompt") or "").strip(),
        }
        for item in thumb_payload.get("thumbnails", [])
        if str((item or {}).get("prompt") or "").strip()
    ]
    if not thumbnail_prompt_rows:
        raise RuntimeError("文案生成失败: stage=thumbnail_prompts provider=api error=API 未返回缩略图提示词")
    thumbnail_prompts = [item["prompt"] for item in thumbnail_prompt_rows if item.get("prompt")]
    return {
        "api_preset": api_preset,
        "content_template": content_template,
        "raw_text": json.dumps(
            {
                "titles_raw": raw_titles,
                "description_raw": raw_desc,
                "thumbnail_raw": raw_thumb,
            },
            ensure_ascii=False,
        ),
        "used_angle": used_angle,
        "titles": titles,
        "descriptions": descriptions,
        "seo_hashtags": seo_hashtags,
        "tag_list": tag_list,
        "generation_source": "api",
        "thumbnail_prompts": thumbnail_prompt_rows,
    }


def call_image_model(api_preset: dict, prompt: str) -> dict[str, Any]:
    base_url = str(api_preset.get("imageBaseUrl") or "").strip()
    api_key = str(api_preset.get("imageApiKey") or api_preset.get("apiKey") or "").strip()
    model = str(api_preset.get("imageModel") or "").strip()
    if not base_url or not api_key or not model:
        raise ValueError("当前图片 API 模板缺少 imageBaseUrl / imageApiKey / imageModel")

    response = _post_json(
        base_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json_payload={
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
