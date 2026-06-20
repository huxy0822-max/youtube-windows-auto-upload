# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
REACTIVE_PRESETS_FILE = BASE_DIR / "config" / "reactive_spectrum_presets.json"
DEFAULT_REACTIVE_PRESET = "Aurora Horizon"


def _default_presets() -> dict[str, dict[str, Any]]:
    try:
        return json.loads(REACTIVE_PRESETS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {
            DEFAULT_REACTIVE_PRESET: {
                "description": "极光横向频谱，偏梦幻氛围，适合 LoFi / Jazz / Chill。",
                "engine": "showcqt",
                "layout": "band",
                "height": 300,
                "bar_v": "10",
                "sono_v": "22",
                "bar_g": 1.5,
                "sono_g": 4.2,
                "timeclamp": 0.18,
                "attack": 0.12,
                "count": 5,
                "basefreq": 24,
                "endfreq": 18000,
                "cscheme": "0.06|0.80|1.00|0.00|0.62|0.98",
                "glow_sigma": 10,
                "glow_alpha": 0.74,
                "core_alpha": 0.92,
            }
        }


def _normalize_presets(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return _default_presets()
    normalized: dict[str, dict[str, Any]] = {}
    for raw_name, raw_payload in raw.items():
        name = str(raw_name or "").strip()
        if not name or not isinstance(raw_payload, dict):
            continue
        normalized[name] = dict(raw_payload)
    return normalized or _default_presets()


def load_reactive_spectrum_presets(path: Path = REACTIVE_PRESETS_FILE) -> dict[str, dict[str, Any]]:
    if not path.exists():
        defaults = _default_presets()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(defaults, ensure_ascii=False, indent=2), encoding="utf-8")
        return defaults
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _default_presets()
    return _normalize_presets(raw)


def list_reactive_spectrum_presets() -> list[str]:
    return list(load_reactive_spectrum_presets().keys())


def normalize_reactive_spectrum_preset(name: str, *, rng: random.Random | None = None) -> str:
    presets = load_reactive_spectrum_presets()
    if not presets:
        return DEFAULT_REACTIVE_PRESET
    clean_name = str(name or "").strip()
    if clean_name in presets:
        return clean_name
    if clean_name.lower() == "random":
        chooser = rng or random
        return chooser.choice(list(presets.keys()))
    if DEFAULT_REACTIVE_PRESET in presets:
        return DEFAULT_REACTIVE_PRESET
    return next(iter(presets.keys()))


def _coerce_int(value: Any, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        number = int(float(str(value).strip()))
    except Exception:
        number = default
    if minimum is not None:
        number = max(minimum, number)
    if maximum is not None:
        number = min(maximum, number)
    return number


def _coerce_float(value: Any, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    try:
        number = float(str(value).strip())
    except Exception:
        number = default
    if minimum is not None:
        number = max(minimum, number)
    if maximum is not None:
        number = min(maximum, number)
    return number


def _layout_position_expr(layout: str, spectrum_x: int | None, spectrum_y: int, size_expr: str) -> tuple[str, str]:
    if layout == "fullframe":
        return "0", "0"
    if layout == "center":
        x_expr = str(spectrum_x) if spectrum_x is not None and spectrum_x >= 0 else "(W-w)/2"
        y_expr = str(max(0, spectrum_y - 220))
        return x_expr, y_expr
    x_expr = str(spectrum_x) if spectrum_x is not None and spectrum_x >= 0 else "(W-w)/2"
    return x_expr, str(max(0, spectrum_y))


def build_reactive_spectrum_overlay(
    *,
    current_label: str,
    preset_name: str,
    spectrum_x: int | None,
    spectrum_y: int,
    spectrum_w: int,
    duration: float,
    rng: random.Random | None = None,
) -> tuple[list[str], str, str]:
    chooser = rng or random.Random()
    resolved_name = normalize_reactive_spectrum_preset(preset_name, rng=chooser)
    payload = dict(load_reactive_spectrum_presets().get(resolved_name) or {})
    engine = str(payload.get("engine") or "showcqt").strip().lower()
    layout = str(payload.get("layout") or "band").strip().lower()
    glow_sigma = _coerce_int(payload.get("glow_sigma"), 10, minimum=1, maximum=30)
    glow_alpha = _coerce_float(payload.get("glow_alpha"), 0.74, minimum=0.1, maximum=1.0)
    core_alpha = _coerce_float(payload.get("core_alpha"), 0.92, minimum=0.1, maximum=1.0)
    width = _coerce_int(spectrum_w, 1440, minimum=480, maximum=1920)
    if layout == "fullframe":
        size_text = str(payload.get("size") or "1920x1080").strip() or "1920x1080"
    elif layout == "center":
        radial_size = _coerce_int(payload.get("size"), min(max(width, 720), 1320), minimum=420, maximum=1440)
        size_text = f"{radial_size}x{radial_size}"
    else:
        height = _coerce_int(payload.get("height"), 280, minimum=120, maximum=720)
        size_text = f"{width}x{height}"

    x_expr, y_expr = _layout_position_expr(layout, spectrum_x, spectrum_y, size_text)
    chains: list[str] = []
    source_label = "reactive_src"
    glow_label = "reactive_glow"
    core_label = "reactive_core"
    mix_label = "reactive_mix"
    next_label = "reactive_out"

    if engine == "avectorscope":
        mode = str(payload.get("mode") or "polar").strip()
        draw = str(payload.get("draw") or "aaline").strip()
        scale = str(payload.get("scale") or "sqrt").strip()
        mirror = str(payload.get("mirror") or "xy").strip()
        zoom = _coerce_float(payload.get("zoom"), 1.12, minimum=0.2, maximum=3.0)
        rc = _coerce_int(payload.get("rc"), 240, minimum=0, maximum=255)
        gc = _coerce_int(payload.get("gc"), 150, minimum=0, maximum=255)
        bc = _coerce_int(payload.get("bc"), 255, minimum=0, maximum=255)
        rf = _coerce_int(payload.get("rf"), 12, minimum=0, maximum=255)
        gf = _coerce_int(payload.get("gf"), 8, minimum=0, maximum=255)
        bf = _coerce_int(payload.get("bf"), 4, minimum=0, maximum=255)
        chains.append(
            f"[1:a]aformat=channel_layouts=stereo,"
            f"avectorscope=mode={mode}:size={size_text}:rate=30:zoom={zoom:.3f}:"
            f"draw={draw}:scale={scale}:mirror={mirror}:"
            f"rc={rc}:gc={gc}:bc={bc}:ac=255:rf={rf}:gf={gf}:bf={bf}:af=4,"
            f"format=rgba,colorkey=0x000000:0.08:0.02[{source_label}]"
        )
    elif engine == "showspectrum":
        color = str(payload.get("color") or "plasma").strip()
        scale = str(payload.get("scale") or "log").strip()
        fscale = str(payload.get("fscale") or "log").strip()
        orientation = str(payload.get("orientation") or "horizontal").strip()
        slide = str(payload.get("slide") or "fullframe").strip()
        win_func = str(payload.get("win_func") or "bharris").strip()
        saturation = _coerce_float(payload.get("saturation"), 2.0, minimum=0.1, maximum=10.0)
        overlap = _coerce_float(payload.get("overlap"), 0.72, minimum=0.0, maximum=1.0)
        gain = _coerce_float(payload.get("gain"), 2.0, minimum=0.0, maximum=128.0)
        opacity = _coerce_float(payload.get("opacity"), 1.8, minimum=0.0, maximum=10.0)
        drange = _coerce_float(payload.get("drange"), 105.0, minimum=10.0, maximum=200.0)
        limit = _coerce_float(payload.get("limit"), -2.0, minimum=-100.0, maximum=100.0)
        chains.append(
            f"[1:a]showspectrum=s={size_text}:slide={slide}:mode=combined:color={color}:"
            f"scale={scale}:fscale={fscale}:orientation={orientation}:win_func={win_func}:"
            f"saturation={saturation:.3f}:overlap={overlap:.3f}:gain={gain:.3f}:"
            f"opacity={opacity:.3f}:legend=0:drange={drange:.3f}:limit={limit:.3f},"
            f"format=rgba,colorkey=0x000000:0.10:0.02[{source_label}]"
        )
    else:
        bar_v = str(payload.get("bar_v") or "10").strip() or "10"
        sono_v = str(payload.get("sono_v") or "22").strip() or "22"
        bar_g = _coerce_float(payload.get("bar_g"), 1.5, minimum=1.0, maximum=7.0)
        sono_g = _coerce_float(payload.get("sono_g"), 4.2, minimum=1.0, maximum=7.0)
        timeclamp = _coerce_float(payload.get("timeclamp"), 0.18, minimum=0.002, maximum=1.0)
        attack = _coerce_float(payload.get("attack"), 0.12, minimum=0.0, maximum=1.0)
        count = _coerce_int(payload.get("count"), 5, minimum=1, maximum=30)
        basefreq = _coerce_float(payload.get("basefreq"), 24.0, minimum=10.0, maximum=100000.0)
        endfreq = _coerce_float(payload.get("endfreq"), 18000.0, minimum=10.0, maximum=100000.0)
        cscheme = str(payload.get("cscheme") or "0.06|0.80|1.00|0.00|0.62|0.98").strip()
        chains.append(
            f"[1:a]showcqt=s={size_text}:r=30:bar_h=-1:axis_h=0:sono_h=0:"
            f"bar_v={bar_v}:sono_v={sono_v}:bar_g={bar_g:.3f}:sono_g={sono_g:.3f}:"
            f"tc={timeclamp:.3f}:attack={attack:.3f}:count={count}:basefreq={basefreq:.3f}:"
            f"endfreq={endfreq:.3f}:axis=0:text=0:csp=bt709:cscheme={cscheme},"
            f"format=rgba,colorkey=0x000000:0.10:0.02[{source_label}]"
        )

    chains.append(
        f"[{source_label}]gblur=sigma={glow_sigma},"
        f"eq=brightness=0.02:contrast=1.04:saturation=1.08,"
        f"colorchannelmixer=aa={glow_alpha:.3f}[{glow_label}]"
    )
    chains.append(f"[{source_label}]colorchannelmixer=aa={core_alpha:.3f}[{core_label}]")
    chains.append(
        f"[{current_label}][{glow_label}]overlay=x={x_expr}:y={y_expr}:format=auto[{mix_label}]"
    )
    chains.append(
        f"[{mix_label}][{core_label}]overlay=x={x_expr}:y={y_expr}:format=auto[{next_label}]"
    )
    effect_desc = f"真实频谱:{resolved_name}"
    return chains, next_label, effect_desc
