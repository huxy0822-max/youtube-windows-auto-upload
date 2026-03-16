from __future__ import annotations

import math
import random
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageEnhance, ImageFilter


ROOT = Path(__file__).resolve().parents[1]
ASSET_ROOT = ROOT / "downloaded_projects" / "effect_assets_20260316"
OVERLAYS_DIR = ROOT / "overlays"
GENERATED_DIR = ASSET_ROOT / "generated_overlays"
TMP_DIR = ASSET_ROOT / "_tmp_generated_frames"


@dataclass(frozen=True)
class OverlaySpec:
    name: str
    palette: tuple[int, int, int]
    sprite_names: tuple[str, ...]
    count: int
    min_scale: float
    max_scale: float
    min_speed: float
    max_speed: float
    opacity: tuple[float, float]
    blur_chance: float
    sway: tuple[float, float]
    width: int = 1920
    height: int = 1080
    frame_count: int = 144
    fps: int = 24
    canvas_rgba: tuple[int, int, int, int] = (0, 0, 0, 0)
    output_ext: str = ".mov"


@dataclass(frozen=True)
class VariantSpec:
    suffix: str
    speed: float
    scale: float
    blur: float
    saturation: float
    brightness: float
    contrast: float
    x_bias: float
    y_bias: float
    opacity: float


PREMIUM_SPECS = [
    OverlaySpec("snow_fall_soft_01", (245, 248, 255), ("circle_01.png", "circle_02.png", "circle_04.png"), 42, 0.08, 0.32, 1.5, 3.6, (0.25, 0.75), 0.35, (4.0, 16.0)),
    OverlaySpec("snow_fall_soft_02", (230, 240, 255), ("circle_02.png", "circle_03.png", "light_01.png"), 54, 0.06, 0.26, 1.2, 2.8, (0.22, 0.62), 0.45, (6.0, 20.0)),
    OverlaySpec("snow_fall_dense_01", (250, 250, 255), ("circle_01.png", "circle_03.png", "particlefx_02.png"), 78, 0.05, 0.24, 1.8, 4.1, (0.18, 0.68), 0.28, (3.0, 14.0)),
    OverlaySpec("snow_magic_fall_01", (235, 245, 255), ("circle_05.png", "particlefx_04.png", "light_02.png"), 58, 0.06, 0.28, 1.0, 2.6, (0.20, 0.65), 0.40, (8.0, 28.0)),
    OverlaySpec("gold_dust_fall_01", (242, 205, 96), ("spark_01.png", "spark_02.png", "flare_01.png"), 48, 0.08, 0.34, 0.9, 2.1, (0.20, 0.70), 0.22, (8.0, 24.0)),
    OverlaySpec("gold_dust_fall_02", (233, 187, 71), ("magic_01.png", "magic_02.png", "light_03.png"), 60, 0.08, 0.30, 0.7, 1.8, (0.20, 0.66), 0.30, (10.0, 34.0)),
    OverlaySpec("gold_glitter_fall_01", (255, 220, 120), ("spark_01.png", "spark_03.png", "particlefx_08.png"), 72, 0.05, 0.22, 0.8, 1.7, (0.22, 0.78), 0.18, (10.0, 30.0)),
    OverlaySpec("gold_glitter_fall_02", (255, 214, 92), ("flare_01.png", "light_01.png", "particlefx_10.png"), 64, 0.06, 0.24, 0.7, 1.5, (0.16, 0.55), 0.26, (14.0, 42.0)),
    OverlaySpec("silver_glitter_fall_01", (220, 235, 255), ("spark_02.png", "light_02.png", "particlefx_06.png"), 70, 0.05, 0.20, 0.9, 1.9, (0.18, 0.60), 0.20, (12.0, 38.0)),
    OverlaySpec("fairy_dust_fall_01", (255, 216, 174), ("magic_03.png", "magic_04.png", "particlefx_12.png"), 56, 0.08, 0.26, 0.6, 1.4, (0.18, 0.58), 0.34, (16.0, 46.0)),
    OverlaySpec("dreamy_bokeh_fall_01", (248, 229, 194), ("light_01.png", "light_02.png", "flare_01.png"), 38, 0.14, 0.42, 0.5, 1.3, (0.10, 0.38), 0.44, (22.0, 64.0)),
    OverlaySpec("amber_spark_fall_01", (252, 182, 74), ("spark_01.png", "spark_03.png", "particlefx_14.png"), 66, 0.05, 0.18, 1.2, 2.4, (0.16, 0.62), 0.16, (8.0, 20.0)),
]

VARIANT_PRESETS = [
    VariantSpec("soft_a", 0.88, 0.84, 0.8, 0.94, 0.02, 0.98, -0.10, -0.08, 0.86),
    VariantSpec("soft_b", 0.76, 0.72, 1.2, 0.90, 0.01, 0.96, 0.12, -0.04, 0.82),
    VariantSpec("dense_a", 1.18, 1.10, 0.2, 1.04, 0.01, 1.05, -0.06, -0.10, 0.92),
    VariantSpec("dense_b", 1.30, 1.26, 0.4, 1.06, 0.02, 1.08, 0.08, -0.12, 0.94),
    VariantSpec("wide_a", 1.00, 0.96, 0.3, 1.00, 0.00, 1.00, -0.16, 0.00, 0.84),
    VariantSpec("wide_b", 1.06, 0.92, 0.5, 1.02, 0.00, 1.02, 0.16, 0.02, 0.86),
    VariantSpec("drift_a", 0.82, 0.88, 0.6, 1.03, 0.01, 1.00, 0.00, 0.08, 0.82),
    VariantSpec("drift_b", 0.92, 0.78, 0.9, 0.98, 0.02, 0.97, -0.04, 0.12, 0.80),
]


def _sprite_dirs() -> list[Path]:
    return [
        ASSET_ROOT / "particles" / "kenney_particle-pack",
        ASSET_ROOT / "particles" / "kenney_smoke-particles",
        ASSET_ROOT / "extracted" / "opengameart_animated_particle_effects_1",
    ]


def _load_sprite(name: str) -> Image.Image:
    for directory in _sprite_dirs():
        path = directory / name
        if path.exists():
            return Image.open(path).convert("RGBA")
    raise FileNotFoundError(name)


def _tint_sprite(image: Image.Image, rgb: tuple[int, int, int]) -> Image.Image:
    base = image.convert("RGBA")
    alpha = base.getchannel("A")
    tinted = Image.new("RGBA", base.size, rgb + (0,))
    tinted.putalpha(alpha)
    return tinted


def _build_particle_field(spec: OverlaySpec, seed: int) -> list[dict]:
    rng = random.Random(seed)
    particles: list[dict] = []
    for _ in range(spec.count):
        particles.append(
            {
                "sprite": rng.choice(spec.sprite_names),
                "scale": rng.uniform(spec.min_scale, spec.max_scale),
                "x": rng.uniform(-spec.width * 0.1, spec.width * 1.1),
                "y": rng.uniform(-spec.height * 0.2, spec.height * 1.1),
                "speed": rng.uniform(spec.min_speed, spec.max_speed),
                "sway_amp": rng.uniform(*spec.sway),
                "sway_speed": rng.uniform(0.35, 1.4),
                "opacity": rng.uniform(*spec.opacity),
                "rotation": rng.uniform(0, 360),
                "blur": rng.random() < spec.blur_chance,
            }
        )
    return particles


def _run_ffmpeg(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _render_spec(spec: OverlaySpec) -> Path:
    output = GENERATED_DIR / f"{spec.name}{spec.output_ext}"
    overlay_copy = OVERLAYS_DIR / output.name
    if output.exists() and overlay_copy.exists():
        return output

    frames_dir = TMP_DIR / spec.name
    if frames_dir.exists():
        shutil.rmtree(frames_dir)
    frames_dir.mkdir(parents=True, exist_ok=True)

    sprite_cache: dict[tuple[str, tuple[int, int, int]], Image.Image] = {}
    particles = _build_particle_field(spec, seed=abs(hash(spec.name)) % 2**32)

    for frame_index in range(spec.frame_count):
        canvas = Image.new("RGBA", (spec.width, spec.height), spec.canvas_rgba)
        progress = frame_index / spec.frame_count
        for particle in particles:
            key = (particle["sprite"], spec.palette)
            if key not in sprite_cache:
                sprite_cache[key] = _tint_sprite(_load_sprite(particle["sprite"]), spec.palette)
            sprite = sprite_cache[key]
            y = (particle["y"] + progress * spec.height * particle["speed"]) % (spec.height + 240) - 120
            x = particle["x"] + particle["sway_amp"] * math.sin(progress * 6.28318 * particle["sway_speed"])
            size = max(12, int(sprite.width * particle["scale"]))
            rotated = sprite.resize((size, size), Image.Resampling.LANCZOS).rotate(
                particle["rotation"] + frame_index * 0.6,
                resample=Image.Resampling.BICUBIC,
                expand=True,
            )
            if particle["blur"]:
                rotated = rotated.filter(ImageFilter.GaussianBlur(radius=1.0))
            alpha = ImageEnhance.Brightness(rotated.getchannel("A")).enhance(particle["opacity"])
            rotated.putalpha(alpha)
            canvas.alpha_composite(rotated, (int(x), int(y)))
        canvas.save(frames_dir / f"frame_{frame_index:03d}.png")

    cmd = [
        "ffmpeg",
        "-y",
        "-framerate",
        str(spec.fps),
        "-i",
        str(frames_dir / "frame_%03d.png"),
    ]
    if spec.output_ext.lower() == ".mov":
        cmd.extend(["-c:v", "qtrle", "-pix_fmt", "argb", str(output)])
    else:
        cmd.extend(["-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p", str(output)])

    _run_ffmpeg(cmd)
    shutil.copy2(output, overlay_copy)
    shutil.rmtree(frames_dir, ignore_errors=True)
    return output


def _premium_sources() -> list[Path]:
    return [_render_spec(spec) for spec in PREMIUM_SPECS]


def _render_variant(source: Path, preset: VariantSpec) -> Path:
    target = GENERATED_DIR / f"{source.stem}_{preset.suffix}.mp4"
    overlay_copy = OVERLAYS_DIR / target.name
    if target.exists() and overlay_copy.exists():
        return target

    width = 1280
    height = 720
    fps = 20
    duration = 4.8
    base_width = max(240, int(width * preset.scale))
    base_height = max(160, int(height * preset.scale))
    x_offset = int((width - base_width) * (0.5 + preset.x_bias))
    y_offset = int((height - base_height) * (0.5 + preset.y_bias))
    x_offset = min(max(x_offset, -width // 3), width)
    y_offset = min(max(y_offset, -height // 3), height)

    source_chain = [
        f"fps={fps}",
        f"setpts=PTS/{preset.speed:.3f}",
        f"scale={base_width}:{base_height}",
        "format=rgba",
        f"gblur=sigma={preset.blur:.2f}",
        f"eq=brightness={preset.brightness:.3f}:contrast={preset.contrast:.3f}:saturation={preset.saturation:.3f}",
        f"colorchannelmixer=aa={preset.opacity:.3f}",
    ]
    filter_complex = (
        f"[0:v]{','.join(source_chain)}[ov];"
        f"[1:v][ov]overlay=x={x_offset}:y={y_offset}:shortest=1:format=auto,"
        "format=yuv420p[v]"
    )
    cmd = [
        "ffmpeg",
        "-y",
        "-stream_loop",
        "-1",
        "-i",
        str(source),
        "-f",
        "lavfi",
        "-i",
        f"color=c=black:s={width}x{height}:r={fps}:d={duration}",
        "-filter_complex",
        filter_complex,
        "-map",
        "[v]",
        "-t",
        str(duration),
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-pix_fmt",
        "yuv420p",
        str(target),
    ]
    _run_ffmpeg(cmd)
    shutil.copy2(target, overlay_copy)
    return target


def _render_bulk_variants(sources: list[Path]) -> list[Path]:
    created: list[Path] = []
    for source in sources:
        for preset in VARIANT_PRESETS:
            created.append(_render_variant(source, preset))
    return created


def main() -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    premium = _premium_sources()
    bulk = _render_bulk_variants(premium)
    created = premium + bulk

    print(f"generated_or_reused {len(created)} overlays")
    print(f"premium={len(premium)} bulk={len(bulk)}")
    for item in created:
        print(item.name)


if __name__ == "__main__":
    main()
