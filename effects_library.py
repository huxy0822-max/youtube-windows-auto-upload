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

from reactive_spectrum import build_reactive_spectrum_overlay


BASE_DIR = Path(__file__).resolve().parent
OVERLAY_DIR = BASE_DIR / "overlays"
SPECTRUM_DIR = BASE_DIR / "spectrums"
FONT_DIR = BASE_DIR / "fonts"
STICKER_DIR = BASE_DIR / "stickers" / "mediterranean"
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
    "MegaBassPurple": {"spectrum": "#F5D7FF|#A855F7", "timeline": "#A855F7", "text": "#FFF0FF"},
    "MegaBassGreen": {"spectrum": "#D6FFE6|#22C55E", "timeline": "#22C55E", "text": "#F1FFF6"},
    "MegaBassAmber": {"spectrum": "#FFE5BF|#F59E0B", "timeline": "#F59E0B", "text": "#FFF7E6"},
    "MegaBassCyan": {"spectrum": "#D8F8FF|#22D3EE", "timeline": "#22D3EE", "text": "#ECFEFF"},
    "MegaBassMagenta": {"spectrum": "#FFD6F6|#EC4899", "timeline": "#EC4899", "text": "#FFF1FA"},
    # ── 地中海系列色板 (Mediterranean) ──
    "MedGoldenSun":    {"spectrum": "#FFF5D6|#D4AF37", "timeline": "#D4AF37", "text": "#FFF8E7"},
    "MedAzureSea":     {"spectrum": "#D6EFFF|#4682B4", "timeline": "#4682B4", "text": "#E8F4FF"},
    "MedAmberDusk":    {"spectrum": "#FFE0B2|#FF9800", "timeline": "#FF9800", "text": "#FFF3E0"},
    "MedCoralReef":    {"spectrum": "#FFD6CC|#F08060", "timeline": "#F08060", "text": "#FFF0EA"},
    "MedTerracotta":   {"spectrum": "#FFD8C0|#CC774D", "timeline": "#CC774D", "text": "#FFF0E0"},
    "MedOliveGrove":   {"spectrum": "#E8F0D8|#808000", "timeline": "#808000", "text": "#F4F8EC"},
    "MedWineNoir":     {"spectrum": "#F5D0DC|#722F37", "timeline": "#722F37", "text": "#FFF0F4"},
    "MedIvoryMist":    {"spectrum": "#FFFDF0|#E8D8B0", "timeline": "#E8D8B0", "text": "#FFFDF7"},
    "MedMoonlight":    {"spectrum": "#E0E8F5|#6B7DA0", "timeline": "#6B7DA0", "text": "#EDF2FA"},
    "MedSmokeyBlue":   {"spectrum": "#D8E4F0|#5A7D9A", "timeline": "#5A7D9A", "text": "#E8F0FA"},
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

MEGA_BASS_PRIMARY_PARTICLES = [
    "mega_neon_sparks_18151",
    "mega_luminous_particles_18142",
    "mega_white_particles_4407",
    "mega_light_waves_particles_18063",
    "spark_burst_loop",
    "mega_gold_glitters_2866",
    "mega_black_sparkles_14865",
    "gold_glitter_fall_02_dense_b",
]

MEGA_BASS_ACCENT_PARTICLES = [
    "gold_glitter_fall_01_dense_a",
    "gold_glitter_fall_02_dense_a",
    "silver_glitter_fall_01_dense_a",
    "amber_spark_fall_01_dense_a",
    "snow_magic_fall_01_dense_a",
]

MEGA_BASS_STYLE_VARIANTS = [
    "mega_neon_line",
    "mega_dense_wave",
    "mega_pulse_scope",
    "mega_laser",
    "mega_glow_band",
]

MEGA_BASS_PALETTE_NAMES = [
    "MegaBassPurple",
    "MegaBassGreen",
    "MegaBassAmber",
    "MegaBassCyan",
    "MegaBassMagenta",
]

# ── 地中海 (Mediterranean) 系列常量 ──
MED_PALETTE_NAMES = [
    "MedGoldenSun", "MedAzureSea", "MedAmberDusk", "MedCoralReef",
    "MedTerracotta", "MedOliveGrove", "MedWineNoir", "MedIvoryMist",
    "MedMoonlight", "MedSmokeyBlue",
]

MED_BOSSA_PALETTES = ["MedGoldenSun", "MedAzureSea", "MedCoralReef", "MedIvoryMist", "MedAmberDusk"]
MED_SPY_PALETTES   = ["MedWineNoir", "MedMoonlight", "MedSmokeyBlue", "MedOliveGrove"]
MED_SUNSET_PALETTES = ["MedGoldenSun", "MedAmberDusk", "MedTerracotta", "MedCoralReef", "Sunset"]

MED_TINT_WARM    = ["warm", "golden", "vintage"]
MED_TINT_COOL    = ["cool", "blue_night"]
MED_TINT_SUNSET  = ["golden", "warm", "vintage"]

# 地中海频谱风格 (12 种新频谱)
MED_SPECTRUM_STYLES = [
    "med_gentle_wave",       # 1.  柔和波浪 — Bossa Nova 吉他弦感
    "med_breath_line",       # 2.  呼吸线条 — 慢节奏海浪起伏
    "med_shimmer_bar",       # 3.  微光条 — 阳光折射水面
    "med_dual_mirror",       # 4.  双镜面 — 上下对称海天倒影
    "med_dot_scatter",       # 5.  星点散布 — 夜空/萤火虫感
    "med_smoke_trail",       # 6.  烟雾尾迹 — 间谍雪茄烟雾
    "med_nylon_string",      # 7.  尼龙弦 — 极细线条震动
    "med_golden_pulse",      # 8.  金色脉冲 — 日落余晖节拍
    "med_moonlit_ripple",    # 9.  月光涟漪 — 港口夜晚水面
    "med_vintage_scope",     # 10. 复古示波器 — 60年代电子仪器
    "med_haze_band",         # 11. 薄雾带 — 清晨港湾薄雾
    "med_jazz_flicker",      # 12. 爵士闪烁 — 烛光/打火机忽明忽暗
]

# 地中海粒子优选
MED_WARM_PARTICLES = [
    "gold_dust_fall_01", "gold_dust_fall_02",
    "gold_glitter_fall_01_soft_a", "gold_glitter_fall_02_soft_a",
    "fairy_dust_fall_01_soft_a", "amber_spark_fall_01_soft_a",
    "dreamy_bokeh_fall_01_soft_a",
]
MED_COOL_PARTICLES = [
    "silver_glitter_fall_01_soft_a", "snow_fall_soft_01",
    "snow_fall_soft_02", "dreamy_bokeh_fall_01_drift_a",
]

# 地中海字体优选 (复古/优雅风格)
MED_FONT_NAMES = [
    "songti", "heiti", "handwrite", "edu_kaishu",
    "bebas_neue", "teko_bold", "din_bold",
]

FONT_FILES = {
    "default": None,
    "songti": "noto_serif_tc.otf",
    "heiti": "noto_sans_tc.otf",
    "handwrite": "honglei_banshu_ft.ttf",
    "edu_kaishu": "edu_kaishu.ttf",
    "edu_songti": "edu_songti.ttf",
    "anton": "Anton-Regular.ttf",
    "bebas_neue": "BebasNeue-Regular.ttf",
    "bungee": "Bungee-Regular.ttf",
    "black_ops_one": "BlackOpsOne-Regular.ttf",
    "audiowide": "Audiowide-Regular.ttf",
    "monoton": "Monoton-Regular.ttf",
    "teko_bold": "Teko-wght.ttf",
    "russo_one": "RussoOne-Regular.ttf",
    "orbitron": "Orbitron-wght.ttf",
    "agency_bold": "AGENCYB.TTF",
    "impact": "impact.ttf",
    "din_bold": "DINNextLTPro-Bold.ttf",
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


def discover_spectrum_files() -> dict[str, str]:
    mapping: dict[str, str] = {}
    if SPECTRUM_DIR.exists():
        for path in sorted(SPECTRUM_DIR.iterdir()):
            if not path.is_file() or path.suffix.lower() not in OVERLAY_EXTENSIONS:
                continue
            mapping.setdefault(path.stem, path.name)
    return mapping


def list_spectrum_assets() -> list[str]:
    return ["code", "random_asset", *discover_spectrum_files().keys()]


def discover_sticker_files() -> list[str]:
    """发现所有地中海贴纸 PNG 文件。"""
    if not STICKER_DIR.exists():
        return []
    return sorted(
        p.name for p in STICKER_DIR.iterdir()
        if p.is_file() and p.suffix.lower() == ".png"
    )


def list_sticker_effects() -> list[str]:
    return ["none", "random", *discover_sticker_files()]


def list_med_spectrum_styles() -> list[str]:
    return list(MED_SPECTRUM_STYLES)


def list_med_palette_names() -> list[str]:
    return list(MED_PALETTE_NAMES)


def discover_particle_files() -> dict[str, str]:
    mapping = dict(PARTICLE_FILES)
    if OVERLAY_DIR.exists():
        for path in sorted(OVERLAY_DIR.iterdir()):
            if not path.is_file() or path.suffix.lower() not in OVERLAY_EXTENSIONS:
                continue
            mapping.setdefault(path.stem, path.name)
    return mapping


def list_palette_names() -> list[str]:
    return ["MegaBassNeon", *PALETTES.keys()]


def list_zoom_modes() -> list[str]:
    return list(ZOOM_SPEEDS.keys())


def list_tint_names() -> list[str]:
    return list(TINT_FILTERS.keys())


def get_random_effects() -> dict[str, str]:
    rng = random.Random()
    return {
        "spectrum": rng.choice(list_effects()),
        "tint": rng.choice(list_tint_names()),
        "zoom": rng.choice(list_zoom_modes()),
        "timeline": rng.choice(["on", "off"]),
        "letterbox": rng.choice(["on", "off"]),
    }


def list_particle_effects() -> list[str]:
    return ["none", *discover_particle_files().keys()]


def list_text_positions() -> list[str]:
    return list(TEXT_POSITIONS.keys())


def list_text_styles() -> list[str]:
    return ["Classic", "Glow", "Neon", "Bold", "Box"]


def list_font_names() -> list[str]:
    return list(FONT_FILES.keys())


def list_mega_bass_font_names() -> list[str]:
    return [
        "anton",
        "bebas_neue",
        "bungee",
        "black_ops_one",
        "audiowide",
        "monoton",
        "teko_bold",
        "russo_one",
        "orbitron",
        "agency_bold",
        "impact",
        "din_bold",
    ]


def list_mega_bass_particle_effects() -> list[str]:
    particle_files = discover_particle_files()
    return [item for item in MEGA_BASS_PRIMARY_PARTICLES if item in particle_files]


def list_mega_bass_palette_names() -> list[str]:
    return list(MEGA_BASS_PALETTE_NAMES)


def list_mega_bass_style_variants() -> list[str]:
    return list(MEGA_BASS_STYLE_VARIANTS)


def _overlay_needs_colorkey(path: Path) -> bool:
    return path.suffix.lower() in {".mp4", ".mkv"}


def _particle_overlay_plan(name: str, *, visual_preset: str = "none", rng=None) -> dict[str, float]:
    """粒子覆盖层参数规划，使用本地随机实例避免污染全局随机状态。"""
    rng = rng or random.Random()
    lower = name.lower()
    if visual_preset == "mega_bass":
        if "light_waves" in lower:
            scale = rng.uniform(1.10, 1.45)
        elif any(token in lower for token in ("neon", "spark_burst", "luminous", "white_particles")):
            scale = rng.uniform(1.45, 1.95)
        else:
            scale = rng.uniform(1.35, 1.80)
        return {
            "scale": scale,
            "flip_h": 1.0 if rng.random() < 0.18 else 0.0,
            "flip_v": 0.0,
        }
    if any(token in lower for token in ("snow", "glitter", "dust", "spark", "magic", "fairy", "bokeh")):
        scale = rng.uniform(1.70, 2.45)
    elif any(token in lower for token in ("smoke", "rain", "fireflies", "light", "flare")):
        scale = rng.uniform(1.35, 2.00)
    else:
        scale = rng.uniform(1.50, 2.20)
    return {
        "scale": scale,
        "flip_h": 1.0 if rng.random() < 0.5 else 0.0,
        "flip_v": 1.0 if rng.random() < 0.18 else 0.0,
    }


def _pick_palette(name: str, *, rng=None) -> dict:
    rng = rng or random
    if name == "MegaBassNeon":
        name = rng.choice(MEGA_BASS_PALETTE_NAMES)
    elif name == "random_med":
        name = rng.choice(MED_PALETTE_NAMES)
    elif name == "random_spy":
        name = rng.choice(MED_SPY_PALETTES)
    elif name == "random_sunset":
        name = rng.choice(MED_SUNSET_PALETTES)
    if name == "random":
        name = rng.choice(list(PALETTES.keys()))
    return PALETTES.get(name, PALETTES["WhiteGold"])


def _pick_flag(value, *, probability_true: float = 0.5, rng=None) -> bool:
    rng = rng or random
    if value == "random":
        return rng.random() < probability_true
    return bool(value)


def _pick_style(name: str, *, rng=None) -> str:
    rng = rng or random
    if name == "random":
        return rng.choice(["bar", "wave", "circular", "bar_mirror"])
    if name == "random_med":
        return rng.choice(MED_SPECTRUM_STYLES)
    if name == "random_spy":
        return rng.choice(["med_smoke_trail", "med_moonlit_ripple", "med_vintage_scope", "med_dot_scatter"])
    if name == "random_sunset":
        return rng.choice(["med_golden_pulse", "med_shimmer_bar", "med_breath_line", "med_haze_band"])
    return name


def _pick_particle(name: str, *, visual_preset: str = "none", rng=None) -> str:
    rng = rng or random
    particle_files = discover_particle_files()
    if name == "random":
        if visual_preset == "mega_bass":
            choices = [item for item in MEGA_BASS_PRIMARY_PARTICLES if item in particle_files]
            if choices:
                return rng.choice(choices)
        choices = list(particle_files.keys())
        return rng.choice(choices) if choices else "none"
    if name == "random_med":
        choices = [p for p in MED_WARM_PARTICLES if p in particle_files]
        return rng.choice(choices) if choices else rng.choice(list(particle_files.keys()) or ["none"])
    if name == "random_spy":
        choices = [p for p in MED_COOL_PARTICLES if p in particle_files]
        return rng.choice(choices) if choices else rng.choice(list(particle_files.keys()) or ["none"])
    if name == "random_sunset":
        choices = [p for p in MED_WARM_PARTICLES if p in particle_files]
        return rng.choice(choices) if choices else rng.choice(list(particle_files.keys()) or ["none"])
    return name if name in particle_files or name == "none" else "none"


def _pick_spectrum_asset(name: str, *, rng=None) -> str:
    rng = rng or random
    spectrum_files = discover_spectrum_files()
    value = str(name or "").strip()
    if not value or value in {"code", "none"}:
        return "code"
    if value == "random_asset":
        choices = list(spectrum_files.keys())
        return rng.choice(choices) if choices else "code"
    return value if value in spectrum_files else "code"


def _pick_tint(name: str, *, rng=None) -> str:
    rng = rng or random
    if name == "random":
        return rng.choice(list(TINT_FILTERS.keys()))
    if name == "random_med":
        return rng.choice(MED_TINT_WARM)
    if name == "random_spy":
        return rng.choice(MED_TINT_COOL)
    if name == "random_sunset":
        return rng.choice(MED_TINT_SUNSET)
    return name


def _pick_text_style(name: str, *, rng=None) -> str:
    rng = rng or random
    if name == "random":
        return rng.choice(["Classic", "Glow", "Neon", "Bold", "Box"])
    if name == "random_med":
        return rng.choice(["Classic", "Glow", "Box"])  # 地中海用柔和风格
    if name == "random_spy":
        return rng.choice(["Neon", "Bold", "Classic"])  # 间谍用冷冽风格
    if name == "random_sunset":
        return rng.choice(["Glow", "Classic", "Box"])
    return name


def _pick_font(name: str, *, rng=None) -> str:
    """选择字体，支持地中海专用随机。"""
    rng = rng or random
    if name == "random":
        return rng.choice(list(FONT_FILES.keys()))
    if name == "random_med":
        return rng.choice(MED_FONT_NAMES)
    return name


def _pick_stickers(sticker_config: str, count_range: str = "2,4",
                   opacity_range: str = "0.35,0.55", *, rng=None) -> list[dict]:
    """
    选择贴纸叠加方案。返回 [{file, x, y, size, opacity, rotation}...] 列表。
    """
    rng = rng or random
    sticker_value = str(sticker_config or "").strip()
    if not sticker_value or sticker_value == "none":
        return []

    stickers = discover_sticker_files()
    if not stickers:
        return []

    # 解析数量范围
    try:
        parts = count_range.split(",")
        min_count = int(parts[0])
        max_count = int(parts[1]) if len(parts) > 1 else min_count
    except (ValueError, IndexError):
        min_count, max_count = 2, 4
    count = rng.randint(min_count, max_count)

    # 解析透明度范围
    try:
        parts = opacity_range.split(",")
        min_opa = float(parts[0])
        max_opa = float(parts[1]) if len(parts) > 1 else min_opa
    except (ValueError, IndexError):
        min_opa, max_opa = 0.35, 0.55

    # 选择不重复的贴纸
    if sticker_value == "random":
        chosen = rng.sample(stickers, min(count, len(stickers)))
    else:
        requested = [item.strip() for item in sticker_value.split("|") if item.strip()]
        if not requested:
            requested = [sticker_value]
        available = [item for item in requested if item in stickers]
        if not available:
            return []
        if len(available) >= count:
            chosen = available[:count]
        else:
            chosen = [rng.choice(available) for _ in range(count)]

    result = []
    # 预定义安全位置区域 (避免挡住频谱和中心区域)
    safe_zones = [
        (40, 40, 200, 200),     # 左上
        (1680, 40, 200, 200),   # 右上
        (40, 830, 200, 200),    # 左下
        (1680, 830, 200, 200),  # 右下
        (40, 400, 160, 160),    # 左中
        (1720, 400, 160, 160),  # 右中
        (700, 40, 160, 160),    # 上中偏左
        (1060, 40, 160, 160),   # 上中偏右
    ]

    for i, sticker_file in enumerate(chosen):
        zone = safe_zones[i % len(safe_zones)]
        size = rng.randint(64, 140)
        x = zone[0] + rng.randint(0, max(0, zone[2] - size))
        y = zone[1] + rng.randint(0, max(0, zone[3] - size))
        opacity = rng.uniform(min_opa, max_opa)
        result.append({
            "file": sticker_file,
            "x": x,
            "y": y,
            "size": size,
            "opacity": opacity,
        })

    return result


def _med_spectrum_config(style: str, spectrum_y: int, *, rng=None) -> dict:
    """
    返回地中海频谱样式的 FFmpeg 参数配置。
    12 种风格，各有不同的 showwaves mode、光晕、高度等参数。
    """
    rng = rng or random
    cfg = {
        "mode": "line",
        "height": 140,
        "glow_sigma": 8,
        "glow_alpha": 0.50,
        "core_alpha": 0.85,
        "brightness": "0.02",
        "extra_eq": "",
        "core_y": spectrum_y,
    }

    if style == "med_gentle_wave":
        # 柔和波浪 — Bossa Nova 吉他弦质感
        cfg.update(mode="line", height=120, glow_sigma=10, glow_alpha=0.45,
                   core_alpha=0.80, brightness="0.015")
    elif style == "med_breath_line":
        # 呼吸线条 — 海浪起伏，极缓
        cfg.update(mode="cline", height=100, glow_sigma=14, glow_alpha=0.55,
                   core_alpha=0.75, brightness="0.018")
    elif style == "med_shimmer_bar":
        # 微光条 — 阳光折射水面
        cfg.update(mode="cline", height=150, glow_sigma=6, glow_alpha=0.60,
                   core_alpha=0.90, brightness="0.025")
    elif style == "med_dual_mirror":
        # 双镜面 — 上下对称海天倒影
        cfg.update(mode="cline", height=160, glow_sigma=8, glow_alpha=0.48,
                   core_alpha=0.82, brightness="0.020",
                   core_y=spectrum_y + 4)
    elif style == "med_dot_scatter":
        # 星点散布 — 夜空萤火虫
        cfg.update(mode="point", height=180, glow_sigma=16, glow_alpha=0.42,
                   core_alpha=0.70, brightness="0.030")
    elif style == "med_smoke_trail":
        # 烟雾尾迹 — 间谍雪茄
        cfg.update(mode="line", height=130, glow_sigma=20, glow_alpha=0.55,
                   core_alpha=0.65, brightness="0.012",
                   extra_eq=",eq=saturation=0.85")
    elif style == "med_nylon_string":
        # 尼龙弦 — 极细线条震动
        cfg.update(mode="cline", height=80, glow_sigma=4, glow_alpha=0.38,
                   core_alpha=0.92, brightness="0.015")
    elif style == "med_golden_pulse":
        # 金色脉冲 — 日落余晖节拍
        cfg.update(mode="cline", height=140, glow_sigma=12, glow_alpha=0.62,
                   core_alpha=0.88, brightness="0.028",
                   extra_eq=",eq=saturation=1.15")
    elif style == "med_moonlit_ripple":
        # 月光涟漪 — 港口夜晚水面
        cfg.update(mode="line", height=110, glow_sigma=18, glow_alpha=0.50,
                   core_alpha=0.72, brightness="0.010",
                   extra_eq=",eq=saturation=0.80")
    elif style == "med_vintage_scope":
        # 复古示波器 — 60年代仪器
        cfg.update(mode="p2p", height=160, glow_sigma=6, glow_alpha=0.55,
                   core_alpha=0.95, brightness="0.022")
    elif style == "med_haze_band":
        # 薄雾带 — 清晨港湾薄雾
        cfg.update(mode="line", height=100, glow_sigma=24, glow_alpha=0.60,
                   core_alpha=0.60, brightness="0.014",
                   extra_eq=",eq=saturation=0.90")
    elif style == "med_jazz_flicker":
        # 爵士闪烁 — 烛光忽明忽暗
        cfg.update(mode="point", height=140, glow_sigma=10, glow_alpha=0.58,
                   core_alpha=0.78, brightness="0.024")
    return cfg


def _escape_drawtext(text: str) -> str:
    return (
        text.replace("\\", r"\\")
        .replace(":", r"\:")
        .replace("'", r"\'")
        .replace("%", r"\%")
        .replace("\n", r"\n")
    )


def _escape_font_path(path: Path) -> str:
    """转义字体路径供 FFmpeg drawtext 使用，正确处理 Windows 盘符和反斜杠。"""
    try:
        resolved = str(path.resolve(strict=False))
    except OSError:
        # Windows 上 resolve 偶尔会因权限或路径格式失败
        resolved = str(path.absolute())
    raw = resolved.replace("\\", "/")
    # FFmpeg drawtext 要求转义冒号（包括 Windows 盘符中的冒号）
    return raw.replace(":", r"\:")


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
    spectrum_asset: str = "code",
    reactive_spectrum_enabled: bool = False,
    reactive_spectrum_preset: str = "random",
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
    visual_preset: str = "none",
    bass_pulse: bool = False,
    bass_pulse_scale: float = 0.03,
    bass_pulse_brightness: float = 0.04,
    bass_pulse_bpm: float = 128.0,
    bass_pulse_phase: float = 0.0,
    sticker: str = "",
    sticker_count: str = "2,4",
    sticker_opacity: str = "0.35,0.55",
    rng=None,
    **_,
):
    """
    生成 `filter_complex`、效果描述和额外输入参数。

    返回:
    - filter_complex: 供 FFmpeg 直接使用
    - effect_desc: 给日志/UI 展示的简短说明
    - extra_inputs: 例如粒子覆盖层需要追加的 `-i`
    """
    rng = rng or random
    duration = max(_coerce_float(duration, 5.0), 1.0)
    text_size = max(_coerce_int(text_size, 42), 18)
    spectrum_y = _coerce_int(spectrum_y, 530)
    spectrum_w = max(_coerce_int(spectrum_w, 1600), 360)
    grain_strength = max(_coerce_int(grain_strength, 15), 0)
    soft_focus_sigma = max(_coerce_float(soft_focus_sigma, 1.5), 0.3)
    particle_opacity = max(0.0, min(_coerce_float(particle_opacity, 0.6), 1.0))
    particle_speed = max(_coerce_float(particle_speed, 1.0), 0.2)
    bass_pulse_scale = max(0.0, min(_coerce_float(bass_pulse_scale, 0.03), 0.12))
    bass_pulse_brightness = max(0.0, min(_coerce_float(bass_pulse_brightness, 0.04), 0.12))
    bass_pulse_bpm = max(_coerce_float(bass_pulse_bpm, 128.0), 60.0)
    bass_pulse_phase = _coerce_float(bass_pulse_phase, 0.0)

    spectrum = _pick_flag(spectrum, probability_true=0.85, rng=rng)
    timeline = _pick_flag(timeline, probability_true=0.85, rng=rng)
    letterbox = _pick_flag(letterbox, probability_true=0.5, rng=rng)
    film_grain = _pick_flag(film_grain, probability_true=0.6, rng=rng)
    vignette = _pick_flag(vignette, probability_true=0.45, rng=rng)
    soft_focus = _pick_flag(soft_focus, probability_true=0.35, rng=rng)
    style = _pick_style(style, rng=rng)
    text_font = _pick_font(text_font, rng=rng)
    if zoom == "random":
        zoom = rng.choice(list(ZOOM_SPEEDS.keys()))
    text_style = _pick_text_style(text_style, rng=rng)
    if text_style not in {"Classic", "Glow", "Neon", "Bold", "Box"}:
        text_style = "Classic"

    spectrum_palette = _pick_palette(color_spectrum, rng=rng)
    timeline_palette = _pick_palette(color_timeline, rng=rng)
    tint_name = _pick_tint(color_tint, rng=rng)
    spectrum_asset_name = _pick_spectrum_asset(spectrum_asset, rng=rng)
    particle_name = _pick_particle(particle, visual_preset=visual_preset, rng=rng)
    zoom_speed = ZOOM_SPEEDS.get(zoom, ZOOM_SPEEDS["normal"])
    pulse_freq = bass_pulse_bpm / 60.0
    pulse_expr = f"pow(max(0,sin(2*PI*{pulse_freq:.6f}*t+{bass_pulse_phase:.6f})),2.2)"

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
            overlay_plan = _particle_overlay_plan(particle_name, visual_preset=visual_preset, rng=rng)
            overlay_scale = max(float(overlay_plan.get("scale", 1.0)), 1.0)
            overlay_w = max(1920, int(round((1920 * overlay_scale) / 2.0) * 2))
            overlay_h = max(1080, int(round((1080 * overlay_scale) / 2.0) * 2))
            input_index = 2
            extra_inputs.extend(["-stream_loop", "-1", "-i", str(overlay_file)])
            overlay_label = "overlay0"
            next_label = "base6"
            if visual_preset == "mega_bass":
                mega_scale_expr = (
                    f"max(1920,trunc(iw*({overlay_scale:.4f}+0.18*{pulse_expr})/2)*2)"
                )
                mega_h_expr = (
                    f"max(1080,trunc(ih*({overlay_scale:.4f}+0.18*{pulse_expr})/2)*2)"
                )
                overlay_parts = [
                    f"scale=w='{mega_scale_expr}':h='{mega_h_expr}':eval=frame",
                    "crop=1920:1080",
                    f"setpts=PTS/{particle_speed:.3f}",
                    f"eq=brightness='0.02+0.06*{pulse_expr}':contrast=1.05:saturation=1.18",
                    "format=rgba",
                ]
            else:
                overlay_parts = [
                    f"scale={overlay_w}:{overlay_h}:force_original_aspect_ratio=increase",
                    "crop=1920:1080",
                    f"setpts=PTS/{particle_speed:.3f}",
                    "format=rgba",
                ]
            if overlay_plan["flip_h"] > 0.5:
                overlay_parts.append("hflip")
            if overlay_plan["flip_v"] > 0.5:
                overlay_parts.append("vflip")
            if _overlay_needs_colorkey(overlay_file):
                overlay_parts.append("colorkey=0x000000:0.20:0.10")
            overlay_parts.append(f"colorchannelmixer=aa={particle_opacity:.3f}")
            chains.append(f"[{input_index}:v]{','.join(overlay_parts)}[{overlay_label}]")
            chains.append(
                f"[{current}][{overlay_label}]overlay=x=0:y=0:shortest=1:format=auto[{next_label}]"
            )
            current = next_label
            if visual_preset == "mega_bass":
                accent_choices = [item for item in MEGA_BASS_ACCENT_PARTICLES if item in particle_files]
                if accent_choices:
                    accent_name = rng.choice(accent_choices)
                    accent_file = OVERLAY_DIR / particle_files.get(accent_name, "")
                    if accent_file.exists():
                        input_index = 3
                        extra_inputs.extend(["-stream_loop", "-1", "-i", str(accent_file)])
                        overlay_label = "overlay1"
                        next_label = "base6b"
                        accent_opacity = min(0.28, max(0.10, particle_opacity * 0.55))
                        accent_speed = max(0.80, particle_speed * 0.92)
                        accent_parts = [
                            f"scale=w='max(1920,trunc(iw*(1.22+0.10*{pulse_expr})/2)*2)':"
                            f"h='max(1080,trunc(ih*(1.22+0.10*{pulse_expr})/2)*2)':eval=frame",
                            "crop=1920:1080",
                            f"setpts=PTS/{accent_speed:.3f}",
                            f"eq=brightness='0.03+0.05*{pulse_expr}':contrast=1.04:saturation=1.12",
                            "format=rgba",
                        ]
                        if _overlay_needs_colorkey(accent_file):
                            accent_parts.append("colorkey=0x000000:0.20:0.10")
                        accent_parts.append(f"colorchannelmixer=aa={accent_opacity:.3f}")
                        chains.append(f"[{input_index}:v]{','.join(accent_parts)}[{overlay_label}]")
                        chains.append(
                            f"[{current}][{overlay_label}]overlay=x=0:y=0:shortest=1:format=auto[{next_label}]"
                        )
                        current = next_label

    if bass_pulse and (bass_pulse_scale > 0 or bass_pulse_brightness > 0):
        next_label = "base_pulse"
        chains.append(
            f"[{current}]scale=w='max(1920,trunc(iw*(1+{bass_pulse_scale:.4f}*{pulse_expr})/2)*2)':"
            f"h='max(1080,trunc(ih*(1+{bass_pulse_scale:.4f}*{pulse_expr})/2)*2)':eval=frame,"
            f"crop=1920:1080,eq=brightness='{bass_pulse_brightness:.4f}*{pulse_expr}':"
            f"contrast=1.03:saturation=1.05,setsar=1[{next_label}]"
        )
        current = next_label

    if visual_preset == "mega_bass" and bass_pulse:
        next_label = "base_pulse_shake"
        shake_x = (
            f"(iw-1920)/2 + 10*sin(2*PI*{pulse_freq:.6f}*t+{bass_pulse_phase:.6f})"
            f" + 4*sin(4*PI*{pulse_freq:.6f}*t+{bass_pulse_phase + 0.5:.6f})"
        )
        shake_y = (
            f"(ih-1080)/2 + 6*sin(2*PI*{pulse_freq:.6f}*t+{bass_pulse_phase + 1.2:.6f})"
        )
        chains.append(
            f"[{current}]scale=1948:1096:force_original_aspect_ratio=increase,"
            f"crop=1920:1080:x='{shake_x}':y='{shake_y}',setsar=1[{next_label}]"
        )
        current = next_label

    use_code_spectrum = spectrum
    reactive_effect_desc = ""
    if spectrum and reactive_spectrum_enabled:
        reactive_chains, reactive_label, reactive_effect_desc = build_reactive_spectrum_overlay(
            current_label=current,
            preset_name=reactive_spectrum_preset,
            spectrum_x=spectrum_x,
            spectrum_y=spectrum_y,
            spectrum_w=spectrum_w,
            duration=duration,
            rng=rng,
        )
        chains.extend(reactive_chains)
        current = reactive_label
        use_code_spectrum = False
        spectrum_asset_name = "code"
    elif spectrum and spectrum_asset_name != "code":
        spectrum_files = discover_spectrum_files()
        spectrum_file = SPECTRUM_DIR / spectrum_files.get(spectrum_asset_name, "")
        if spectrum_file.exists():
            next_label = "base7_asset"
            input_index = 2 + len(extra_inputs) // 4
            extra_inputs.extend(["-stream_loop", "-1", "-i", str(spectrum_file)])
            spec_parts = [
                "scale=1920:1080:force_original_aspect_ratio=increase",
                "crop=1920:1080",
                "format=rgba",
            ]
            if _overlay_needs_colorkey(spectrum_file):
                spec_parts.append("colorkey=0x000000:0.20:0.08")
            spec_parts.append("colorchannelmixer=aa=0.94")
            chains.append(f"[{input_index}:v]{','.join(spec_parts)}[spectrum_asset0]")
            chains.append(
                f"[{current}][spectrum_asset0]overlay=x=0:y=0:shortest=1:format=auto[{next_label}]"
            )
            current = next_label
            use_code_spectrum = False
        else:
            spectrum_asset_name = "code"

    if use_code_spectrum:
        spec_label = "spectrum0"
        spec_width = min(spectrum_w, 1800)
        spec_height = 220 if visual_preset == "mega_bass" else 170
        x_expr = str(spectrum_x) if spectrum_x is not None else "(W-w)/2"
        next_label = "base7"
        is_med_style = style.startswith("med_")
        if visual_preset == "mega_bass":
            mega_variant = style if style in MEGA_BASS_STYLE_VARIANTS else rng.choice(MEGA_BASS_STYLE_VARIANTS)
            if mega_variant == "mega_neon_line":
                mode = "cline"
                glow_sigma = 12
                glow_alpha = 0.60
                core_alpha = 0.98
                spec_height = 220
                core_y = spectrum_y
            elif mega_variant == "mega_dense_wave":
                mode = "line"
                glow_sigma = 14
                glow_alpha = 0.64
                core_alpha = 0.94
                spec_height = 250
                core_y = spectrum_y - 8
            elif mega_variant == "mega_pulse_scope":
                mode = "p2p"
                glow_sigma = 9
                glow_alpha = 0.56
                core_alpha = 0.90
                spec_height = 180
                core_y = spectrum_y + 6
            elif mega_variant == "mega_laser":
                mode = "point"
                glow_sigma = 16
                glow_alpha = 0.52
                core_alpha = 0.88
                spec_height = 150
                core_y = spectrum_y + 18
            else:
                mode = "cline"
                glow_sigma = 18
                glow_alpha = 0.68
                core_alpha = 0.92
                spec_height = 190
                core_y = spectrum_y + 12
            spec_core = "spectrum_core"
            spec_glow = "spectrum_glow"
            chains.append(
                f"[1:a]showwaves=s={spec_width}x{spec_height}:mode={mode}:colors={spectrum_palette['spectrum']},"
                f"format=rgba,colorkey=0x000000:0.08:0.02,gblur=sigma={glow_sigma},"
                f"eq=brightness='0.04+0.08*{pulse_expr}':contrast=1.05:saturation=1.18,"
                f"colorchannelmixer=aa={glow_alpha:.2f}[{spec_glow}]"
            )
            chains.append(
                f"[1:a]showwaves=s={spec_width}x{spec_height}:mode={mode}:colors={spectrum_palette['spectrum']},"
                f"format=rgba,colorkey=0x000000:0.08:0.02,"
                f"eq=brightness='0.02+0.06*{pulse_expr}':contrast=1.03:saturation=1.10,"
                f"colorchannelmixer=aa={core_alpha:.2f}[{spec_core}]"
            )
            chains.append(f"[{current}][{spec_glow}]overlay=x={x_expr}:y={spectrum_y}[base7_glow]")
            chains.append(f"[base7_glow][{spec_core}]overlay=x={x_expr}:y={core_y}[{next_label}]")
        elif is_med_style:
            # ── 地中海频谱样式 (12种) ──
            med_cfg = _med_spectrum_config(style, spectrum_y, rng=rng)
            mode = med_cfg["mode"]
            spec_height = med_cfg["height"]
            glow_sigma = med_cfg["glow_sigma"]
            glow_alpha = med_cfg["glow_alpha"]
            core_alpha = med_cfg["core_alpha"]
            brightness_expr = med_cfg.get("brightness", "0.02")
            extra_eq = med_cfg.get("extra_eq", "")
            core_y = med_cfg.get("core_y", spectrum_y)

            spec_core = "spectrum_core"
            spec_glow = "spectrum_glow"
            # 光晕层
            glow_eq = f"eq=brightness={brightness_expr}:contrast=1.03:saturation=1.05"
            chains.append(
                f"[1:a]showwaves=s={spec_width}x{spec_height}:mode={mode}:colors={spectrum_palette['spectrum']},"
                f"format=rgba,colorkey=0x000000:0.08:0.02,gblur=sigma={glow_sigma},"
                f"{glow_eq}{extra_eq},colorchannelmixer=aa={glow_alpha:.2f}[{spec_glow}]"
            )
            # 核心层
            core_eq = f"eq=brightness={float(brightness_expr)*0.7:.3f}:contrast=1.02:saturation=1.03"
            chains.append(
                f"[1:a]showwaves=s={spec_width}x{spec_height}:mode={mode}:colors={spectrum_palette['spectrum']},"
                f"format=rgba,colorkey=0x000000:0.08:0.02,"
                f"{core_eq},colorchannelmixer=aa={core_alpha:.2f}[{spec_core}]"
            )
            chains.append(f"[{current}][{spec_glow}]overlay=x={x_expr}:y={spectrum_y}[base7_glow]")
            chains.append(f"[base7_glow][{spec_core}]overlay=x={x_expr}:y={core_y}[{next_label}]")
        else:
            mode = "line" if style in {"wave", "circular"} else "cline"
            chains.append(
                f"[1:a]showwaves=s={spec_width}x{spec_height}:mode={mode}:colors={spectrum_palette['spectrum']},"
                f"format=rgba,colorkey=0x000000:0.08:0.02,colorchannelmixer=aa=0.92[{spec_label}]"
            )
            chains.append(f"[{current}][{spec_label}]overlay=x={x_expr}:y={spectrum_y}[{next_label}]")
        current = next_label

    # ── 贴纸叠加层 (Sticker Overlay) ──
    sticker_plans = _pick_stickers(sticker, sticker_count, sticker_opacity, rng=rng)
    sticker_input_base = 2 + len(extra_inputs) // 4  # 计算当前输入索引
    for si, sp in enumerate(sticker_plans):
        sticker_path = STICKER_DIR / sp["file"]
        if not sticker_path.exists():
            continue
        input_idx = sticker_input_base + si
        extra_inputs.extend(["-i", str(sticker_path)])
        slabel = f"stk{si}"
        next_label = f"base_stk{si}"
        chains.append(
            f"[{input_idx}:v]scale={sp['size']}:{sp['size']}:force_original_aspect_ratio=decrease,"
            f"format=rgba,colorchannelmixer=aa={sp['opacity']:.2f}[{slabel}]"
        )
        chains.append(
            f"[{current}][{slabel}]overlay=x={sp['x']}:y={sp['y']}:format=auto[{next_label}]"
        )
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
        if reactive_spectrum_enabled:
            enabled.append(reactive_effect_desc or f"真实频谱:{reactive_spectrum_preset}")
        elif spectrum_asset_name != "code":
            enabled.append(f"频谱素材:{spectrum_asset_name}")
        else:
            spec_style_desc = style if style.startswith("med_") else style
            enabled.append(f"频谱:{spec_style_desc}")
    if timeline:
        enabled.append("时间轴")
    if letterbox:
        enabled.append("黑边")
    if particle_name != "none":
        enabled.append(f"粒子:{particle_name}")
    if sticker_plans:
        enabled.append(f"贴纸x{len(sticker_plans)}")
    if bass_pulse:
        enabled.append(f"低频脉冲:{round(bass_pulse_bpm)}bpm")
    if tint_name != "none":
        enabled.append(f"色调:{tint_name}")
    if text and text.strip():
        enabled.append("文字")
    if visual_preset != "none":
        enabled.append(f"预设:{visual_preset}")

    effect_desc = " / ".join(enabled) if enabled else "基础渲染"
    return ";".join(chains), effect_desc, extra_inputs
