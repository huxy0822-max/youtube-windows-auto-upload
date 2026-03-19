#!/usr/bin/env python3
"""
FFmpeg 特效库。

当前仓库原本缺少这个模块，导致 GUI 和调度脚本一启动就会报 `ModuleNotFoundError`。
这里补上一套“够用、稳定、跨平台”的实现，目标不是极致花哨，而是：
- 让预览和正式渲染都能跑起来。
- 参数接口兼容现有 GUI / CLI 调用方式。
- 频谱、时间轴、文字、粒子、色调等常用效果都有基础实现。
"""

from __future__ import annotations

import random
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
OVERLAY_DIR = BASE_DIR / "overlays"
FONT_DIR = BASE_DIR / "fonts"
OVERLAY_EXTENSIONS = {".mp4", ".mov", ".webm", ".mkv"}

PALETTES = {
    "WhiteGold": {"spectrum": "white|#FFD700", "timeline": "#FFD700", "text": "#FFF5D6"},
    "CoolBlue": {"spectrum": "#D8F3FF|#56CCF2", "timeline": "#56CCF2", "text": "#E8F7FF"},
    "RoseGold": {"spectrum": "#FFE2D6|#E6A57E", "timeline": "#E6A57E", "text": "#FFF0EA"},
    "Champagne": {"spectrum": "#FFF0D1|#D6B36F", "timeline": "#D6B36F", "text": "#FFF8E8"},
    "Platinum": {"spectrum": "#F5F7FA|#AAB2BD", "timeline": "#AAB2BD", "text": "#FFFFFF"},
    "Pearl": {"spectrum": "#FFFDF7|#E8DCC8", "timeline": "#E8DCC8", "text": "#FFFDF7"},
    "Ivory": {"spectrum": "#FFF8E7|#E8D8B0", "timeline": "#E8D8B0", "text": "#FFF8E7"},
    "Silver": {"spectrum": "#E9EEF2|#A0AEC0", "timeline": "#A0AEC0", "text": "#F8FAFC"},
    "SageGreen": {"spectrum": "#E7F5EC|#8BBF9F", "timeline": "#8BBF9F", "text": "#F1FAF4"},
    "DustyBlue": {"spectrum": "#E6EEF7|#7C95B5", "timeline": "#7C95B5", "text": "#F2F7FC"},
    "MidnightBlue": {"spectrum": "#DDE8FF|#294172", "timeline": "#294172", "text": "#EDF4FF"},
    "Burgundy": {"spectrum": "#FFDCE6|#7D243F", "timeline": "#7D243F", "text": "#FFF0F4"},
    "Sunset": {"spectrum": "#FFE3C2|#FF7A59", "timeline": "#FF7A59", "text": "#FFF2E8"},
}

ZOOM_SPEEDS = {
    "off": 0.0,
    "slow": 0.0006,
    "normal": 0.0011,
    "fast": 0.0018,
}

PARTICLE_FILES = {
    "snow": "snow.mp4",
    "dust_bokeh": "dust_bokeh.mp4",
    "fireflies": "fireflies.mp4",
    "rain": "rain.mp4",
}

FONT_FILES = {
    "default": None,
    "songti": "noto_serif_tc.otf",
    "heiti": "noto_sans_tc.otf",
    "handwrite": "honglei_banshu_ft.ttf",
    "edu_kaishu": "edu_kaishu.ttf",
    "edu_songti": "edu_songti.ttf",
}

TEXT_POSITIONS = {
    "bottom_left": ("80", "H-th-110"),
    "bottom_center": ("(W-tw)/2", "H-th-110"),
    "top_center": ("(W-tw)/2", "90"),
    "center": ("(W-tw)/2", "(H-th)/2"),
}

TINT_FILTERS = {
    "none": "",
    "warm": "eq=brightness=0.02:contrast=1.04:saturation=1.08,colorbalance=rs=.04:bs=-.02",
    "cool": "eq=brightness=0.01:contrast=1.03:saturation=1.04,colorbalance=rs=-.02:bs=.04",
    "vintage": "eq=brightness=0.01:contrast=0.96:saturation=0.88,colorbalance=rs=.05:gs=.01:bs=-.05",
    "blue_night": "eq=brightness=-0.02:contrast=1.08:saturation=0.92,colorbalance=rs=-.08:bs=.10",
    "golden": "eq=brightness=0.03:contrast=1.05:saturation=1.06,colorbalance=rs=.06:gs=.02:bs=-.03",
    "forest": "eq=brightness=0.00:contrast=1.03:saturation=0.98,colorbalance=rs=-.02:gs=.05:bs=-.01",
}


def list_effects() -> list[str]:
    """返回当前支持的基础风格名。"""
    return ["bar", "bar_mirror", "wave", "circular"]


def discover_particle_files() -> dict[str, str]:
    mapping = dict(PARTICLE_FILES)
    if OVERLAY_DIR.exists():
        for path in sorted(OVERLAY_DIR.iterdir()):
            if not path.is_file() or path.suffix.lower() not in OVERLAY_EXTENSIONS:
                continue
            mapping.setdefault(path.stem, path.name)
    return mapping


def list_palette_names() -> list[str]:
    return list(PALETTES.keys())


def list_zoom_modes() -> list[str]:
    return list(ZOOM_SPEEDS.keys())


def list_tint_names() -> list[str]:
    return list(TINT_FILTERS.keys())


def list_particle_effects() -> list[str]:
    return ["none", *discover_particle_files().keys()]


def list_text_positions() -> list[str]:
    return list(TEXT_POSITIONS.keys())


def list_text_styles() -> list[str]:
    return ["Classic", "Glow", "Neon", "Bold", "Box"]


def list_font_names() -> list[str]:
    return list(FONT_FILES.keys())


def _overlay_needs_colorkey(path: Path) -> bool:
    return path.suffix.lower() in {".mp4", ".mkv"}


def _particle_overlay_plan(name: str) -> dict[str, float]:
    lower = name.lower()
    if any(token in lower for token in ("snow", "glitter", "dust", "spark", "magic", "fairy", "bokeh")):
        scale = random.uniform(1.55, 2.30)
    elif any(token in lower for token in ("smoke", "rain", "fireflies", "light", "flare")):
        scale = random.uniform(1.30, 1.95)
    else:
        scale = random.uniform(1.40, 2.10)
    return {
        "scale": scale,
        "base_x": random.uniform(0.0, 1.0),
        "base_y": random.uniform(0.0, 1.0),
        "sway_px": random.uniform(18.0, 80.0),
        "sway_speed": random.uniform(0.12, 0.56),
        "drift_px": random.uniform(12.0, 64.0),
        "drift_speed": random.uniform(0.10, 0.40),
        "flip_h": 1.0 if random.random() < 0.5 else 0.0,
        "flip_v": 1.0 if random.random() < 0.18 else 0.0,
    }


def _pick_palette(name: str) -> dict:
    if name == "random":
        name = random.choice(list(PALETTES.keys()))
    return PALETTES.get(name, PALETTES["WhiteGold"])


def _pick_flag(value, *, probability_true: float = 0.5) -> bool:
    if value == "random":
        return random.random() < probability_true
    return bool(value)


def _pick_style(name: str) -> str:
    if name == "random":
        return random.choice(["bar", "wave", "circular", "bar_mirror"])
    return name


def _pick_particle(name: str) -> str:
    particle_files = discover_particle_files()
    if name == "random":
        choices = list(particle_files.keys())
        return random.choice(choices) if choices else "none"
    return name if name in particle_files or name == "none" else "none"


def _pick_tint(name: str) -> str:
    if name == "random":
        return random.choice(list(TINT_FILTERS.keys()))
    return name


def _pick_text_style(name: str) -> str:
    if name == "random":
        return random.choice(["Classic", "Glow", "Neon", "Bold", "Box"])
    return name


def _escape_drawtext(text: str) -> str:
    return (
        text.replace("\\", r"\\")
        .replace(":", r"\:")
        .replace("'", r"\'")
        .replace("%", r"\%")
        .replace("\n", r"\n")
    )


def _escape_font_path(path: Path) -> str:
    return str(path.resolve(strict=False)).replace("\\", "/").replace(":", r"\:")


def _coerce_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _text_filter(label_in: str, label_out: str, text: str, text_pos: str, text_size: int, text_style: str, text_font: str, text_color: str) -> str:
    x_expr, y_expr = TEXT_POSITIONS.get(text_pos, TEXT_POSITIONS["bottom_center"])
    escaped_text = _escape_drawtext(text.strip())
    font_clause = ""
    font_filename = FONT_FILES.get(text_font)
    if font_filename:
        font_path = FONT_DIR / font_filename
        if font_path.exists():
            font_clause = f":fontfile='{_escape_font_path(font_path)}'"

    borderw = 2
    shadowcolor = "black@0.65"
    shadowx = 2
    shadowy = 2
    extra = ""

    if text_style == "Glow":
        borderw = 3
        shadowcolor = f"{text_color}@0.65"
        shadowx = 0
        shadowy = 0
    elif text_style == "Neon":
        borderw = 4
        shadowcolor = f"{text_color}@0.8"
        shadowx = 0
        shadowy = 0
    elif text_style == "Bold":
        borderw = 5
    elif text_style == "Box":
        extra = ":box=1:boxcolor=black@0.32:boxborderw=20"

    return (
        f"[{label_in}]drawtext=text='{escaped_text}'{font_clause}:fontsize={text_size}"
        f":fontcolor={text_color}:x={x_expr}:y={y_expr}:borderw={borderw}"
        f":bordercolor=black@0.35:shadowcolor={shadowcolor}:shadowx={shadowx}:shadowy={shadowy}{extra}"
        f"[{label_out}]"
    )


def get_effect(
    duration: float,
    *,
    spectrum: bool = True,
    timeline: bool = True,
    letterbox: bool = False,
    zoom: str = "normal",
    color_spectrum: str = "WhiteGold",
    color_timeline: str = "WhiteGold",
    spectrum_y: int = 530,
    spectrum_x: int | None = None,
    spectrum_w: int = 1600,
    style: str = "bar",
    text: str = "",
    text_pos: str = "bottom_center",
    text_size: int = 42,
    text_style: str = "Classic",
    film_grain: bool = False,
    grain_strength: int = 15,
    vignette: bool = False,
    color_tint: str = "none",
    soft_focus: bool = False,
    soft_focus_sigma: float = 1.5,
    particle: str = "none",
    particle_opacity: float = 0.6,
    particle_speed: float = 1.0,
    text_font: str = "default",
    **_,
):
    """
    生成 `filter_complex`、效果描述和额外输入参数。

    返回:
    - filter_complex: 供 FFmpeg 直接使用
    - effect_desc: 给日志/UI 展示的简短说明
    - extra_inputs: 例如粒子覆盖层需要追加的 `-i`
    """
    duration = max(_coerce_float(duration, 5.0), 1.0)
    text_size = max(_coerce_int(text_size, 42), 18)
    spectrum_y = _coerce_int(spectrum_y, 530)
    spectrum_w = max(_coerce_int(spectrum_w, 1600), 360)
    grain_strength = max(_coerce_int(grain_strength, 15), 0)
    soft_focus_sigma = max(_coerce_float(soft_focus_sigma, 1.5), 0.3)
    particle_opacity = max(0.0, min(_coerce_float(particle_opacity, 0.6), 1.0))
    particle_speed = max(_coerce_float(particle_speed, 1.0), 0.2)

    spectrum = _pick_flag(spectrum, probability_true=0.85)
    timeline = _pick_flag(timeline, probability_true=0.85)
    letterbox = _pick_flag(letterbox, probability_true=0.5)
    film_grain = _pick_flag(film_grain, probability_true=0.6)
    vignette = _pick_flag(vignette, probability_true=0.45)
    soft_focus = _pick_flag(soft_focus, probability_true=0.35)
    style = _pick_style(style)
    if zoom == "random":
        zoom = random.choice(list(ZOOM_SPEEDS.keys()))
    text_style = _pick_text_style(text_style)
    if text_style not in {"Classic", "Glow", "Neon", "Bold", "Box"}:
        text_style = "Classic"

    spectrum_palette = _pick_palette(color_spectrum)
    timeline_palette = _pick_palette(color_timeline)
    tint_name = _pick_tint(color_tint)
    particle_name = _pick_particle(particle)
    zoom_speed = ZOOM_SPEEDS.get(zoom, ZOOM_SPEEDS["normal"])

    chains = []
    extra_inputs: list[str] = []
    current = "base0"

    base_parts = [
        "scale=1920:1080:force_original_aspect_ratio=increase",
        "crop=1920:1080",
        "setsar=1",
    ]
    if zoom_speed > 0:
        zoom_expr = f"if(lte(on,1),1.0,min(1.18,zoom+{zoom_speed:.6f}))"
        base_parts.append(
            f"zoompan=z='{zoom_expr}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:s=1920x1080:fps=30"
        )
    base_parts.append("format=rgba")
    chains.append(f"[0:v]{','.join(base_parts)}[{current}]")

    if tint_name != "none":
        next_label = "base1"
        chains.append(f"[{current}]{TINT_FILTERS[tint_name]}[{next_label}]")
        current = next_label

    if soft_focus:
        next_label = "base2"
        chains.append(f"[{current}]gblur=sigma={soft_focus_sigma:.2f}[{next_label}]")
        current = next_label

    if film_grain and grain_strength > 0:
        next_label = "base3"
        chains.append(f"[{current}]noise=alls={grain_strength}:allf=t+u[{next_label}]")
        current = next_label

    if vignette:
        next_label = "base4"
        chains.append(f"[{current}]vignette=PI/4[{next_label}]")
        current = next_label

    if letterbox:
        next_label = "base5"
        chains.append(
            f"[{current}]drawbox=x=0:y=0:w=iw:h=94:color=black@1:t=fill,"
            f"drawbox=x=0:y=ih-94:w=iw:h=94:color=black@1:t=fill[{next_label}]"
        )
        current = next_label

    particle_files = discover_particle_files()
    if particle_name != "none":
        overlay_file = OVERLAY_DIR / particle_files.get(particle_name, "")
        if overlay_file.exists():
            overlay_plan = _particle_overlay_plan(particle_name)
            overlay_scale = max(float(overlay_plan.get("scale", 1.0)), 1.0)
            overlay_w = max(1920, int(round((1920 * overlay_scale) / 2.0) * 2))
            overlay_h = max(1080, int(round((1080 * overlay_scale) / 2.0) * 2))
            input_index = 2
            extra_inputs.extend(["-stream_loop", "-1", "-i", str(overlay_file)])
            overlay_label = "overlay0"
            next_label = "base6"
            crop_x_base = max(0.0, min(1.0, float(overlay_plan.get("base_x", 0.5))))
            crop_y_base = max(0.0, min(1.0, float(overlay_plan.get("base_y", 0.5))))
            overlay_parts = [
                f"scale={overlay_w}:{overlay_h}:force_original_aspect_ratio=increase",
                f"setpts=PTS/{particle_speed:.3f}",
                "format=rgba",
            ]
            if overlay_plan["flip_h"] > 0.5:
                overlay_parts.append("hflip")
            if overlay_plan["flip_v"] > 0.5:
                overlay_parts.append("vflip")
            crop_x_expr = (
                "max(0,min(iw-ow,"
                f"(iw-ow)*{crop_x_base:.3f}+{overlay_plan['sway_px']:.1f}*sin(t*{overlay_plan['sway_speed']:.3f})))"
            )
            crop_y_expr = (
                "max(0,min(ih-oh,"
                f"(ih-oh)*{crop_y_base:.3f}+{overlay_plan['drift_px']:.1f}*sin(t*{overlay_plan['drift_speed']:.3f})))"
            )
            overlay_parts.append(f"crop=1920:1080:x='{crop_x_expr}':y='{crop_y_expr}'")
            if _overlay_needs_colorkey(overlay_file):
                overlay_parts.append("colorkey=0x000000:0.20:0.10")
            overlay_parts.append(f"colorchannelmixer=aa={particle_opacity:.3f}")
            chains.append(f"[{input_index}:v]{','.join(overlay_parts)}[{overlay_label}]")
            chains.append(
                f"[{current}][{overlay_label}]overlay=x=0:y=0:shortest=1:format=auto[{next_label}]"
            )
            current = next_label

    if spectrum:
        spec_label = "spectrum0"
        spec_width = min(spectrum_w, 1800)
        spec_height = 170
        x_expr = str(spectrum_x) if spectrum_x is not None else "(W-w)/2"
        mode = "line" if style in {"wave", "circular"} else "cline"
        chains.append(
            f"[1:a]showwaves=s={spec_width}x{spec_height}:mode={mode}:colors={spectrum_palette['spectrum']},"
            f"format=rgba,colorchannelmixer=aa=0.92[{spec_label}]"
        )
        next_label = "base7"
        chains.append(f"[{current}][{spec_label}]overlay=x={x_expr}:y={spectrum_y}[{next_label}]")
        current = next_label

    if timeline:
        next_label = "base8"
        bar_x = 140
        bar_y = 1000
        bar_w = 1640
        bar_h = 6
        progress_expr = f"max(8,{bar_w}*min(t/{duration:.3f},1))"
        chains.append(
            f"[{current}]drawbox=x={bar_x}:y={bar_y}:w={bar_w}:h={bar_h}:color=white@0.18:t=fill,"
            f"drawbox=x={bar_x}:y={bar_y}:w='{progress_expr}':h={bar_h}:color={timeline_palette['timeline']}@0.95:t=fill[{next_label}]"
        )
        current = next_label

    if text and text.strip():
        next_label = "base9"
        chains.append(
            _text_filter(
                current,
                next_label,
                text,
                text_pos,
                text_size,
                text_style,
                text_font,
                timeline_palette["text"],
            )
        )
        current = next_label

    chains.append(f"[{current}]format=yuv420p[outv]")

    enabled = []
    if spectrum:
        enabled.append("频谱")
    if timeline:
        enabled.append("时间轴")
    if letterbox:
        enabled.append("黑边")
    if particle_name != "none":
        enabled.append(f"粒子:{particle_name}")
    if tint_name != "none":
        enabled.append(f"色调:{tint_name}")
    if text and text.strip():
        enabled.append("文字")

    effect_desc = " / ".join(enabled) if enabled else "基础渲染"
    return ";".join(chains), effect_desc, extra_inputs
