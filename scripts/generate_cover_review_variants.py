from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont


CANVAS = (1280, 720)
TITLE = "超好聽的"


@dataclass(slots=True)
class FontPack:
    heavy: Path
    regular: Path


def load_fonts() -> FontPack:
    candidates = [
        FontPack(Path("C:/Windows/Fonts/msjhbd.ttc"), Path("C:/Windows/Fonts/msjh.ttc")),
        FontPack(Path("C:/Windows/Fonts/msyhbd.ttc"), Path("C:/Windows/Fonts/msyh.ttc")),
    ]
    for item in candidates:
        if item.heavy.exists() and item.regular.exists():
            return item
    raise FileNotFoundError("No suitable CJK font pair found.")


def fit_background(source: Image.Image, size: tuple[int, int]) -> Image.Image:
    sw, sh = source.size
    tw, th = size
    src_ratio = sw / sh
    tgt_ratio = tw / th
    if src_ratio > tgt_ratio:
        new_h = th
        new_w = int(th * src_ratio)
    else:
        new_w = tw
        new_h = int(tw / src_ratio)
    resized = source.resize((new_w, new_h), Image.Resampling.LANCZOS)
    left = (new_w - tw) // 2
    top = (new_h - th) // 2
    return resized.crop((left, top, left + tw, top + th))


def rounded_panel(
    overlay: Image.Image,
    box: tuple[int, int, int, int],
    fill: tuple[int, int, int, int],
    outline: tuple[int, int, int, int] | None = None,
    radius: int = 32,
) -> None:
    draw = ImageDraw.Draw(overlay)
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=2 if outline else 0)


def draw_text_shadow(
    draw: ImageDraw.ImageDraw,
    pos: tuple[float, float],
    text: str,
    font: ImageFont.FreeTypeFont,
    fill: tuple[int, int, int, int],
    shadow: tuple[int, int, int, int] = (0, 0, 0, 150),
    offset: tuple[int, int] = (6, 8),
    anchor: str = "mm",
) -> None:
    x, y = pos
    ox, oy = offset
    draw.text((x + ox, y + oy), text, font=font, fill=shadow, anchor=anchor)
    draw.text((x, y), text, font=font, fill=fill, anchor=anchor)


def title_font(fonts: FontPack, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(fonts.heavy), size=size)


def regular_font(fonts: FontPack, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(fonts.regular), size=size)


def pill(
    overlay: Image.Image,
    center: tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
    fill: tuple[int, int, int, int],
    text_fill: tuple[int, int, int, int],
    pad_x: int = 42,
    pad_y: int = 20,
    radius: int = 26,
    outline: tuple[int, int, int, int] | None = None,
) -> None:
    draw = ImageDraw.Draw(overlay)
    bbox = draw.textbbox((0, 0), text, font=font, anchor="lt")
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    cx, cy = center
    box = (
        cx - w // 2 - pad_x,
        cy - h // 2 - pad_y,
        cx + w // 2 + pad_x,
        cy + h // 2 + pad_y,
    )
    rounded_panel(overlay, box, fill=fill, outline=outline, radius=radius)
    draw.text((cx, cy), text, font=font, fill=text_fill, anchor="mm")


def build_base(source_path: Path) -> Image.Image:
    base = Image.open(source_path).convert("RGB")
    fitted = fit_background(base, CANVAS)
    blurred = fitted.filter(ImageFilter.GaussianBlur(radius=1.8))
    overlay = Image.new("RGBA", CANVAS, (18, 10, 8, 90))
    composed = Image.alpha_composite(blurred.convert("RGBA"), overlay)
    return composed.convert("RGBA")


def render_variant(template_id: int, source_path: Path, genre: str, fonts: FontPack) -> Image.Image:
    canvas = build_base(source_path)
    overlay = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    warm_white = (252, 245, 236, 255)
    gold = (225, 198, 146, 255)
    amber = (210, 164, 93, 255)
    deep = (35, 25, 22, 210)
    soft = (255, 245, 235, 62)

    if template_id == 1:
        rounded_panel(overlay, (70, 95, 1210, 635), (28, 19, 17, 110), (255, 255, 255, 42), 44)
        draw_text_shadow(draw, (640, 340), TITLE, title_font(fonts, 212), warm_white)
        pill(overlay, (640, 520), genre, title_font(fonts, 84), (35, 22, 19, 180), warm_white, outline=(255, 255, 255, 24))
    elif template_id == 2:
        rounded_panel(overlay, (55, 390, 920, 660), (18, 10, 8, 152), (255, 255, 255, 28), 32)
        draw_text_shadow(draw, (160, 475), TITLE, title_font(fonts, 164), warm_white, anchor="lm")
        pill(overlay, (320, 590), genre, title_font(fonts, 64), (55, 34, 26, 205), warm_white, outline=(255, 230, 190, 28))
    elif template_id == 3:
        rounded_panel(overlay, (760, 70, 1225, 648), (19, 12, 10, 168), (255, 255, 255, 32), 38)
        draw_text_shadow(draw, (994, 315), TITLE, title_font(fonts, 144), warm_white)
        pill(overlay, (994, 470), genre, title_font(fonts, 58), (58, 36, 24, 215), warm_white)
    elif template_id == 4:
        rounded_panel(overlay, (130, 165, 1150, 565), (20, 12, 11, 125), (255, 255, 255, 26), 52)
        pill(overlay, (640, 285), TITLE, title_font(fonts, 176), (255, 248, 241, 44), warm_white, pad_x=52, pad_y=32, radius=34)
        pill(overlay, (640, 465), genre, title_font(fonts, 72), (33, 20, 16, 190), warm_white)
    elif template_id == 5:
        draw.ellipse((245, 80, 1035, 670), fill=(28, 18, 15, 120), outline=(255, 255, 255, 28), width=3)
        draw_text_shadow(draw, (640, 330), TITLE, title_font(fonts, 188), warm_white)
        pill(overlay, (640, 500), genre, title_font(fonts, 70), (22, 13, 11, 182), gold)
    elif template_id == 6:
        rounded_panel(overlay, (55, 455, 1225, 665), (15, 10, 8, 165), (255, 255, 255, 24), 28)
        draw_text_shadow(draw, (640, 525), TITLE, title_font(fonts, 168), warm_white)
        draw.text((640, 615), genre, font=title_font(fonts, 56), fill=gold, anchor="mm")
    elif template_id == 7:
        pill(overlay, (245, 120), "MUSIC COVER", regular_font(fonts, 28), (35, 23, 19, 160), (242, 223, 198, 255), pad_x=24, pad_y=12, radius=20)
        rounded_panel(overlay, (60, 170, 920, 610), (19, 11, 10, 132), (255, 255, 255, 28), 36)
        draw_text_shadow(draw, (152, 345), TITLE, title_font(fonts, 176), warm_white, anchor="lm")
        pill(overlay, (285, 505), genre, title_font(fonts, 62), (58, 37, 28, 204), warm_white)
    elif template_id == 8:
        rounded_panel(overlay, (140, 115, 1140, 605), (34, 21, 18, 118), (215, 186, 145, 88), 18)
        rounded_panel(overlay, (175, 150, 1105, 570), (255, 248, 241, 20), None, 10)
        draw_text_shadow(draw, (640, 332), TITLE, title_font(fonts, 192), warm_white)
        pill(overlay, (640, 505), genre, title_font(fonts, 68), (34, 24, 18, 202), gold, outline=(210, 174, 120, 82))
    elif template_id == 9:
        draw.polygon([(0, 520), (1280, 360), (1280, 720), (0, 720)], fill=(17, 11, 9, 165))
        draw.polygon([(0, 570), (1280, 410), (1280, 720), (0, 720)], fill=(255, 255, 255, 16))
        draw_text_shadow(draw, (640, 430), TITLE, title_font(fonts, 176), warm_white)
        pill(overlay, (640, 585), genre, title_font(fonts, 66), (30, 18, 14, 195), warm_white)
    elif template_id == 10:
        rounded_panel(overlay, (180, 95, 1100, 625), (16, 11, 10, 142), (255, 255, 255, 24), 58)
        rounded_panel(overlay, (280, 420, 1000, 568), (255, 248, 241, 18), None, 42)
        draw_text_shadow(draw, (640, 298), TITLE, title_font(fonts, 204), warm_white)
        draw.text((640, 495), genre, font=title_font(fonts, 86), fill=gold, anchor="mm")
    else:
        raise ValueError(f"Unknown template: {template_id}")

    vignette = Image.new("L", CANVAS, 0)
    vdraw = ImageDraw.Draw(vignette)
    vdraw.ellipse((-140, -90, 1420, 810), fill=175)
    vignette = vignette.filter(ImageFilter.GaussianBlur(50))
    dark = Image.new("RGBA", CANVAS, (0, 0, 0, 68))
    dark.putalpha(vignette.point(lambda value: max(0, 190 - value)))
    canvas = Image.alpha_composite(canvas, dark)
    canvas = Image.alpha_composite(canvas, overlay)
    return canvas.convert("RGB")


def build_contact_sheet(images: list[Image.Image]) -> Image.Image:
    thumb_w, thumb_h = 480, 270
    margin = 28
    cols = 2
    rows = 5
    canvas = Image.new("RGB", (cols * thumb_w + margin * 3, rows * thumb_h + margin * 6), (20, 16, 15))
    fonts = load_fonts()
    label_font = regular_font(fonts, 30)
    draw = ImageDraw.Draw(canvas)
    for idx, img in enumerate(images, start=1):
        row = (idx - 1) // cols
        col = (idx - 1) % cols
        x = margin + col * (thumb_w + margin)
        y = margin + row * (thumb_h + margin + 34)
        thumb = img.copy().resize((thumb_w, thumb_h), Image.Resampling.LANCZOS)
        canvas.paste(thumb, (x, y))
        draw.text((x + 4, y + thumb_h + 10), f"{idx:02d}", font=label_font, fill=(244, 236, 226))
    return canvas


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Generate multiple cover layout variants for review.")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--genre", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--prefix", default="cover_review")
    args = parser.parse_args()

    fonts = load_fonts()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    generated: list[Image.Image] = []

    for template_id in range(1, 11):
        image = render_variant(template_id, args.source, args.genre, fonts)
        out = args.out_dir / f"{args.prefix}_{template_id:02d}.jpg"
        image.save(out, quality=95)
        generated.append(image)

    contact = build_contact_sheet(generated)
    contact.save(args.out_dir / f"{args.prefix}_grid.jpg", quality=92)


if __name__ == "__main__":
    main()
