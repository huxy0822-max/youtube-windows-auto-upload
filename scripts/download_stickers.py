#!/usr/bin/env python3
"""
Generate 25+ transparent PNG stickers for a 1960s Mediterranean music video channel.
Uses Pillow to create clean, vintage-style graphics with warm gold/amber/cream colors
on transparent backgrounds. Each sticker is 512x512.
"""

import os
import math
from PIL import Image, ImageDraw, ImageFont

OUTPUT_DIR = r"C:\youtube自动化-claude优化版本\stickers\mediterranean"
SIZE = 512
CENTER = SIZE // 2

# Vintage Mediterranean color palette
GOLD = (212, 175, 55, 255)
DARK_GOLD = (170, 130, 30, 255)
AMBER = (191, 144, 0, 255)
CREAM = (255, 240, 210, 255)
WARM_BROWN = (139, 90, 43, 255)
TERRACOTTA = (204, 119, 34, 255)
DEEP_RED = (178, 60, 50, 255)
OLIVE = (128, 128, 0, 255)
TEAL = (0, 128, 128, 255)
SOFT_WHITE = (255, 248, 235, 255)
TRANSPARENT = (0, 0, 0, 0)


def new_canvas():
    return Image.new("RGBA", (SIZE, SIZE), TRANSPARENT)


def get_font(size):
    """Try to get a nice font, fall back to default."""
    font_paths = [
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/calibri.ttf",
        "C:/Windows/Fonts/segoeui.ttf",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                pass
    return ImageFont.load_default()


def draw_vinyl_record(variant=0):
    """Draw a vinyl record with label."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER

    colors = [
        (GOLD, CREAM, WARM_BROWN),
        (AMBER, SOFT_WHITE, TERRACOTTA),
        (DARK_GOLD, CREAM, DEEP_RED),
        (TERRACOTTA, SOFT_WHITE, OLIVE),
    ]
    outer_col, label_col, accent_col = colors[variant % len(colors)]

    # Outer disc
    r = 220
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(40, 40, 40, 240))

    # Grooves
    for gr in range(200, 100, -8):
        d.ellipse([cx - gr, cy - gr, cx + gr, cy + gr], outline=(60, 60, 60, 200), width=1)

    # Sheen highlight
    for i in range(180, 120, -2):
        alpha = max(0, 30 - abs(i - 150))
        d.ellipse([cx - i, cy - i, cx + i, cy + i], outline=(200, 200, 200, alpha), width=1)

    # Label
    lr = 80
    d.ellipse([cx - lr, cy - lr, cx + lr, cy + lr], fill=accent_col)
    d.ellipse([cx - lr + 5, cy - lr + 5, cx + lr - 5, cy + lr - 5], fill=label_col)

    # Label text
    font = get_font(16)
    d.text((cx, cy - 20), "VINYL", fill=accent_col, font=font, anchor="mm")
    d.text((cx, cy), "RECORDS", fill=accent_col, font=font, anchor="mm")
    if variant > 0:
        d.text((cx, cy + 20), f"Vol.{variant}", fill=accent_col, font=font, anchor="mm")

    # Center hole
    d.ellipse([cx - 6, cy - 6, cx + 6, cy + 6], fill=TRANSPARENT)

    # Outer rim highlight
    d.ellipse([cx - r, cy - r, cx + r, cy + r], outline=outer_col, width=3)

    return img


def draw_headphones(variant=0):
    """Draw headphones silhouette."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = GOLD if variant == 0 else AMBER if variant == 1 else TERRACOTTA

    # Headband arc
    d.arc([cx - 140, cy - 180, cx + 140, cy + 40], 180, 0, fill=col, width=18)

    # Left ear cup
    d.rounded_rectangle([cx - 170, cy - 30, cx - 100, cy + 100], radius=20, fill=col)
    d.rounded_rectangle([cx - 160, cy - 15, cx - 110, cy + 85], radius=15, fill=WARM_BROWN)

    # Right ear cup
    d.rounded_rectangle([cx + 100, cy - 30, cx + 170, cy + 100], radius=20, fill=col)
    d.rounded_rectangle([cx + 110, cy - 15, cx + 160, cy + 85], radius=15, fill=WARM_BROWN)

    # Padding detail
    d.ellipse([cx - 155, cy + 10, cx - 115, cy + 60], fill=DARK_GOLD)
    d.ellipse([cx + 115, cy + 10, cx + 155, cy + 60], fill=DARK_GOLD)

    if variant == 2:
        # Add cord
        points = []
        for t in range(0, 101):
            tt = t / 100.0
            x = cx - 135 + tt * 10
            y = cy + 100 + tt * 120 + math.sin(tt * 6) * 15
            points.append((x, y))
        if len(points) > 1:
            d.line(points, fill=col, width=4)

    return img


def draw_musical_note(variant=0):
    """Draw musical note symbols."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = [GOLD, AMBER, TERRACOTTA, CREAM][variant % 4]

    if variant == 0:
        # Single eighth note
        d.ellipse([cx - 60, cy + 40, cx + 20, cy + 120], fill=col)
        d.rectangle([cx + 10, cy - 120, cx + 20, cy + 80], fill=col)
        # Flag
        points = [(cx + 20, cy - 120), (cx + 90, cy - 60), (cx + 20, cy - 40)]
        d.polygon(points, fill=col)
    elif variant == 1:
        # Double eighth note (beamed)
        d.ellipse([cx - 100, cy + 50, cx - 30, cy + 120], fill=col)
        d.ellipse([cx + 30, cy + 30, cx + 100, cy + 100], fill=col)
        d.rectangle([cx - 40, cy - 110, cx - 30, cy + 85], fill=col)
        d.rectangle([cx + 90, cy - 130, cx + 100, cy + 65], fill=col)
        d.rectangle([cx - 40, cy - 130, cx + 100, cy - 110], fill=col)
    elif variant == 2:
        # Treble clef approximation
        font = get_font(350)
        d.text((cx, cy), "\u266B", fill=col, font=font, anchor="mm")
    else:
        # Quarter note
        d.ellipse([cx - 50, cy + 30, cx + 30, cy + 110], fill=col)
        d.rectangle([cx + 20, cy - 130, cx + 30, cy + 70], fill=col)

    return img


def draw_microphone(variant=0):
    """Draw a vintage microphone."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = GOLD if variant == 0 else AMBER
    accent = WARM_BROWN if variant == 0 else DARK_GOLD

    if variant == 0:
        # Classic stage mic
        # Mic head
        d.ellipse([cx - 60, cy - 200, cx + 60, cy - 60], fill=col)
        d.ellipse([cx - 50, cy - 190, cx + 50, cy - 70], fill=accent)
        # Grill lines
        for y in range(int(cy - 180), int(cy - 80), 12):
            d.line([(cx - 40, y), (cx + 40, y)], fill=col, width=2)
        # Stand
        d.rectangle([cx - 8, cy - 60, cx + 8, cy + 140], fill=col)
        # Base
        d.polygon([(cx - 80, cy + 140), (cx + 80, cy + 140),
                    (cx + 50, cy + 180), (cx - 50, cy + 180)], fill=col)
    else:
        # Vintage ribbon mic
        d.rounded_rectangle([cx - 40, cy - 180, cx + 40, cy - 20], radius=30, fill=col)
        d.rounded_rectangle([cx - 30, cy - 170, cx + 30, cy - 30], radius=25, fill=accent)
        # Ribbon detail
        d.rectangle([cx - 5, cy - 160, cx + 5, cy - 40], fill=col)
        # Mount
        d.rectangle([cx - 50, cy - 30, cx + 50, cy - 10], fill=col)
        # Stand
        d.rectangle([cx - 6, cy - 10, cx + 6, cy + 140], fill=col)
        d.ellipse([cx - 60, cy + 130, cx + 60, cy + 180], fill=col)

    return img


def draw_guitar(variant=0):
    """Draw an acoustic guitar outline."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = GOLD if variant == 0 else TERRACOTTA

    # Neck
    d.rectangle([cx - 12, cy - 220, cx + 12, cy - 20], fill=col)
    # Fret markers
    for y in range(int(cy - 200), int(cy - 30), 30):
        d.line([(cx - 12, y), (cx + 12, y)], fill=WARM_BROWN, width=2)

    # Headstock
    d.rounded_rectangle([cx - 25, cy - 240, cx + 25, cy - 210], radius=8, fill=col)
    # Tuning pegs
    for dy in [-235, -225]:
        d.ellipse([cx - 35, dy - 5, cx - 25, dy + 5], fill=DARK_GOLD)
        d.ellipse([cx + 25, dy - 5, cx + 35, dy + 5], fill=DARK_GOLD)

    # Body - upper bout
    d.ellipse([cx - 70, cy - 40, cx + 70, cy + 60], fill=col)
    # Body - lower bout
    d.ellipse([cx - 100, cy + 30, cx + 100, cy + 200], fill=col)

    # Sound hole
    d.ellipse([cx - 30, cy + 20, cx + 30, cy + 80], fill=WARM_BROWN)
    d.ellipse([cx - 25, cy + 25, cx + 25, cy + 75], outline=col, width=2)

    # Bridge
    d.rectangle([cx - 40, cy + 110, cx + 40, cy + 120], fill=WARM_BROWN)

    # Strings
    for sx in range(-8, 12, 4):
        d.line([(cx + sx, cy - 210), (cx + sx, cy + 115)], fill=CREAM, width=1)

    if variant == 1:
        # Add pick guard
        d.pieslice([cx - 50, cy + 40, cx + 10, cy + 140], 200, 350, fill=DARK_GOLD)

    return img


def draw_piano_keys(variant=0):
    """Draw piano keys."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    col = CREAM if variant == 0 else SOFT_WHITE

    start_x = 56
    key_w = 50
    key_h = 280
    y_start = 116

    # White keys
    for i in range(8):
        x = start_x + i * key_w
        d.rectangle([x, y_start, x + key_w - 2, y_start + key_h],
                     fill=col, outline=GOLD, width=2)

    # Black keys
    black_positions = [0, 1, 3, 4, 5]  # standard pattern
    for i in black_positions:
        x = start_x + i * key_w + key_w * 0.65
        bw = key_w * 0.6
        bh = key_h * 0.6
        d.rectangle([x, y_start, x + bw, y_start + bh], fill=WARM_BROWN)
        d.rectangle([x + 2, y_start + 2, x + bw - 2, y_start + bh - 4],
                     fill=(60, 40, 20, 255))

    # Frame
    d.rectangle([start_x - 5, y_start - 5, start_x + 8 * key_w + 3, y_start + key_h + 5],
                 outline=GOLD, width=4)

    if variant == 1:
        # Add musical notes floating above
        font = get_font(40)
        d.text((150, 80), "\u266A", fill=GOLD, font=font)
        d.text((300, 60), "\u266B", fill=AMBER, font=font)
        d.text((400, 90), "\u266A", fill=TERRACOTTA, font=font)

    return img


def draw_saxophone(variant=0):
    """Draw a saxophone silhouette."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    col = GOLD if variant == 0 else AMBER

    # Bell
    d.pieslice([cx - 120, cy + 60, cx + 40, cy + 220], 250, 80, fill=col)
    d.pieslice([cx - 100, cy + 80, cx + 20, cy + 200], 250, 80, fill=DARK_GOLD)
    cx, cy = CENTER, CENTER

    # Body tube (curved)
    points = []
    for t in range(0, 100):
        tt = t / 100.0
        x = cx - 40 + math.sin(tt * 1.5) * 60
        y = cy - 200 + tt * 360
        points.append((x, y))

    for i in range(len(points) - 1):
        x1, y1 = points[i]
        x2, y2 = points[i + 1]
        w = 16 + i * 0.15
        d.line([(x1, y1), (x2, y2)], fill=col, width=int(w))

    # Mouthpiece
    d.polygon([(cx - 50, cy - 200), (cx - 30, cy - 200),
               (cx - 20, cy - 160), (cx - 55, cy - 170)], fill=WARM_BROWN)

    # Keys
    key_positions = [(cx - 15, cy - 100), (cx - 10, cy - 50),
                     (cx + 5, cy), (cx + 15, cy + 50)]
    for kx, ky in key_positions:
        d.ellipse([kx - 8, ky - 8, kx + 8, ky + 8], fill=DARK_GOLD, outline=col, width=2)

    # Bell opening
    d.ellipse([cx - 90, cy + 100, cx + 30, cy + 200], fill=col)
    d.ellipse([cx - 75, cy + 115, cx + 15, cy + 185], fill=DARK_GOLD)

    return img


def draw_trumpet(variant=0):
    """Draw a trumpet."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = GOLD if variant == 0 else AMBER

    # Bell
    points = []
    for angle in range(0, 360, 5):
        r = 80
        x = cx + 140 + int(r * math.cos(math.radians(angle)))
        y = cy + int(r * math.sin(math.radians(angle)) * 0.9)
        points.append((x, y))
    if points:
        d.polygon(points, fill=col)
    d.ellipse([cx + 80, cy - 65, cx + 220, cy + 65], fill=col)
    d.ellipse([cx + 95, cy - 50, cx + 205, cy + 50], fill=DARK_GOLD)

    # Main tube
    d.rectangle([cx - 180, cy - 15, cx + 140, cy + 15], fill=col)

    # Valves
    for vx in [-60, 0, 60]:
        d.rectangle([cx + vx - 12, cy - 70, cx + vx + 12, cy - 15], fill=col)
        d.ellipse([cx + vx - 10, cy - 80, cx + vx + 10, cy - 60], fill=DARK_GOLD)

    # Mouthpiece
    d.ellipse([cx - 200, cy - 15, cx - 170, cy + 15], fill=col)
    d.rectangle([cx - 195, cy - 8, cx - 175, cy + 8], fill=DARK_GOLD)

    # Slide tubes
    d.rectangle([cx - 180, cy + 15, cx - 170, cy + 60], fill=col)
    d.rectangle([cx - 180, cy + 50, cx - 80, cy + 60], fill=col)
    d.rectangle([cx - 90, cy + 15, cx - 80, cy + 60], fill=col)

    return img


def draw_speaker(variant=0):
    """Draw a speaker / amplifier."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = GOLD if variant == 0 else TERRACOTTA

    # Cabinet
    d.rounded_rectangle([cx - 150, cy - 180, cx + 150, cy + 180],
                         radius=15, fill=WARM_BROWN)
    d.rounded_rectangle([cx - 140, cy - 170, cx + 140, cy + 170],
                         radius=12, fill=(80, 55, 30, 255))

    # Speaker cone
    r = 110
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(50, 35, 20, 255), outline=col, width=3)
    d.ellipse([cx - 80, cy - 80, cx + 80, cy + 80], fill=(60, 42, 25, 255), outline=col, width=2)
    d.ellipse([cx - 40, cy - 40, cx + 40, cy + 40], fill=(70, 50, 30, 255), outline=col, width=2)
    d.ellipse([cx - 15, cy - 15, cx + 15, cy + 15], fill=col)

    if variant == 1:
        # Add knobs on top
        for kx in [-80, -30, 30, 80]:
            d.ellipse([cx + kx - 10, cy - 165, cx + kx + 10, cy - 145],
                       fill=col, outline=DARK_GOLD, width=2)

    return img


def draw_cassette(variant=0):
    """Draw a cassette tape."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = CREAM if variant == 0 else SOFT_WHITE
    accent = GOLD if variant == 0 else TERRACOTTA

    # Outer shell
    d.rounded_rectangle([cx - 190, cy - 120, cx + 190, cy + 120],
                         radius=10, fill=accent)
    d.rounded_rectangle([cx - 180, cy - 110, cx + 180, cy + 110],
                         radius=8, fill=col)

    # Label area
    d.rounded_rectangle([cx - 140, cy - 90, cx + 140, cy - 10],
                         radius=5, fill=accent)
    font = get_font(18)
    d.text((cx, cy - 65), "MEDITERRANEAN", fill=col, font=font, anchor="mm")
    d.text((cx, cy - 40), "BOSSA NOVA MIX", fill=col, font=font, anchor="mm")

    # Tape reels
    for rx in [-70, 70]:
        d.ellipse([cx + rx - 35, cy + 10, cx + rx + 35, cy + 80],
                   fill=(60, 40, 20, 255), outline=accent, width=3)
        d.ellipse([cx + rx - 12, cy + 32, cx + rx + 12, cy + 58],
                   fill=accent)
        # Spokes
        for angle in [0, 120, 240]:
            x1 = cx + rx + int(8 * math.cos(math.radians(angle)))
            y1 = cy + 45 + int(8 * math.sin(math.radians(angle)))
            d.ellipse([x1 - 3, y1 - 3, x1 + 3, y1 + 3], fill=col)

    # Tape window
    d.rectangle([cx - 30, cy + 25, cx + 30, cy + 65], fill=(40, 25, 15, 255))

    # Screw holes
    for sx, sy in [(-160, -95), (160, -95), (-160, 95), (160, 95)]:
        d.ellipse([cx + sx - 5, cy + sy - 5, cx + sx + 5, cy + sy + 5], fill=accent)

    return img


def draw_vintage_radio(variant=0):
    """Draw a vintage radio."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = WARM_BROWN if variant == 0 else TERRACOTTA
    accent = GOLD if variant == 0 else AMBER

    # Body
    d.rounded_rectangle([cx - 180, cy - 140, cx + 180, cy + 140],
                         radius=20, fill=col)
    d.rounded_rectangle([cx - 170, cy - 130, cx + 170, cy + 130],
                         radius=15, fill=(100, 65, 35, 255))

    # Speaker grille area
    d.rounded_rectangle([cx - 150, cy - 110, cx + 60, cy + 50],
                         radius=10, fill=(80, 50, 25, 255))
    # Grille lines
    for y in range(int(cy - 100), int(cy + 45), 10):
        d.line([(cx - 140, y), (cx + 50, y)], fill=accent, width=1)

    # Dial
    d.ellipse([cx + 80, cy - 90, cx + 150, cy - 20], fill=(60, 38, 18, 255),
              outline=accent, width=3)
    # Dial needle
    d.line([(cx + 115, cy - 55), (cx + 140, cy - 75)], fill=DEEP_RED, width=2)

    # Knobs
    for kx in [90, 130]:
        d.ellipse([cx + kx - 15, cy + 10, cx + kx + 15, cy + 40],
                   fill=accent, outline=DARK_GOLD, width=2)

    # Frequency display
    d.rectangle([cx + 75, cy + 55, cx + 155, cy + 80], fill=(50, 30, 15, 255))
    font = get_font(12)
    d.text((cx + 115, cy + 67), "FM 98.5", fill=accent, font=font, anchor="mm")

    # Feet
    for fx in [-140, 140]:
        d.rounded_rectangle([cx + fx - 15, cy + 130, cx + fx + 15, cy + 150],
                             radius=5, fill=accent)

    # Antenna
    if variant == 1:
        d.line([(cx - 100, cy - 130), (cx - 60, cy - 220)], fill=accent, width=3)
        d.ellipse([cx - 65, cy - 225, cx - 55, cy - 215], fill=accent)

    return img


def draw_sun(variant=0):
    """Draw a Mediterranean sun."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER
    col = GOLD if variant == 0 else AMBER

    # Rays
    num_rays = 16 if variant == 0 else 12
    for i in range(num_rays):
        angle = (360 / num_rays) * i
        inner_r = 90
        outer_r = 200
        a_rad = math.radians(angle)
        half_w = math.radians(360 / num_rays / 3)

        x1 = cx + int(inner_r * math.cos(a_rad - half_w))
        y1 = cy + int(inner_r * math.sin(a_rad - half_w))
        x2 = cx + int(outer_r * math.cos(a_rad))
        y2 = cy + int(outer_r * math.sin(a_rad))
        x3 = cx + int(inner_r * math.cos(a_rad + half_w))
        y3 = cy + int(inner_r * math.sin(a_rad + half_w))
        d.polygon([(x1, y1), (x2, y2), (x3, y3)], fill=col)

    # Center circle
    d.ellipse([cx - 90, cy - 90, cx + 90, cy + 90], fill=col)
    d.ellipse([cx - 75, cy - 75, cx + 75, cy + 75], fill=CREAM)

    # Face or decoration
    if variant == 0:
        # Simple smiley
        d.arc([cx - 40, cy - 30, cx + 40, cy + 40], 10, 170, fill=GOLD, width=4)
        d.ellipse([cx - 30, cy - 30, cx - 15, cy - 15], fill=GOLD)
        d.ellipse([cx + 15, cy - 30, cx + 30, cy - 15], fill=GOLD)
    else:
        # Spiral pattern
        for r in range(70, 10, -15):
            d.ellipse([cx - r, cy - r, cx + r, cy + r], outline=col, width=2)

    return img


def draw_wave(variant=0):
    """Draw ocean/Mediterranean wave decorative element."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    col = TEAL if variant == 0 else (70, 130, 180, 255)
    accent = GOLD

    # Multiple wave layers
    for layer in range(4):
        y_base = 180 + layer * 60
        alpha = 255 - layer * 40
        wave_col = (col[0], col[1], col[2], alpha)

        points = []
        for x in range(0, SIZE + 10, 2):
            y = y_base + math.sin((x + layer * 40) / 40.0) * 30
            y += math.sin((x + layer * 20) / 80.0) * 15
            points.append((x, y))

        # Fill below wave
        bottom_points = points + [(SIZE, SIZE), (0, SIZE)]
        d.polygon(bottom_points, fill=wave_col)

    # Sun reflection sparkles
    for sx, sy in [(120, 160), (250, 140), (380, 170), (180, 200), (320, 190)]:
        d.ellipse([sx - 4, sy - 4, sx + 4, sy + 4], fill=accent)

    # Decorative border
    d.rounded_rectangle([20, 20, SIZE - 20, SIZE - 20], radius=15, outline=accent, width=3)

    return img


def draw_bossa_nova_element(variant=0):
    """Draw bossa nova / jazz themed decorative elements."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER

    if variant == 0:
        # "Bossa Nova" text badge
        d.rounded_rectangle([cx - 170, cy - 80, cx + 170, cy + 80],
                             radius=40, fill=GOLD)
        d.rounded_rectangle([cx - 160, cy - 70, cx + 160, cy + 70],
                             radius=35, fill=WARM_BROWN)
        font_large = get_font(42)
        font_small = get_font(24)
        d.text((cx, cy - 15), "BOSSA", fill=CREAM, font=font_large, anchor="mm")
        d.text((cx, cy + 25), "NOVA", fill=GOLD, font=font_small, anchor="mm")

        # Decorative dots
        for angle in range(0, 360, 30):
            r = 160
            x = cx + int(r * math.cos(math.radians(angle)))
            y = cy + int(r * math.sin(math.radians(angle)))
            d.ellipse([x - 5, y - 5, x + 5, y + 5], fill=GOLD)

    elif variant == 1:
        # Jazz club sign
        d.rounded_rectangle([cx - 150, cy - 120, cx + 150, cy + 120],
                             radius=10, fill=(40, 25, 15, 255))
        d.rounded_rectangle([cx - 140, cy - 110, cx + 140, cy + 110],
                             radius=8, outline=GOLD, width=3)

        font_large = get_font(48)
        font_small = get_font(22)
        d.text((cx, cy - 30), "JAZZ", fill=GOLD, font=font_large, anchor="mm")
        d.text((cx, cy + 20), "CLUB", fill=AMBER, font=font_large, anchor="mm")
        d.text((cx, cy + 65), "est. 1962", fill=CREAM, font=font_small, anchor="mm")

        # Corner decorations
        for sx, sy in [(-130, -100), (130, -100), (-130, 100), (130, 100)]:
            d.ellipse([cx + sx - 6, cy + sy - 6, cx + sx + 6, cy + sy + 6], fill=GOLD)

    else:
        # Star/sparkle decorative
        d.rounded_rectangle([cx - 160, cy - 60, cx + 160, cy + 60],
                             radius=30, fill=TERRACOTTA)
        d.rounded_rectangle([cx - 150, cy - 50, cx + 150, cy + 50],
                             radius=25, outline=CREAM, width=2)
        font = get_font(28)
        d.text((cx, cy - 8), "MEDITERRANEAN", fill=CREAM, font=font, anchor="mm")
        d.text((cx, cy + 20), "\u2605 VIBES \u2605", fill=GOLD, font=get_font(20), anchor="mm")

    return img


def draw_turntable():
    """Draw a turntable / record player from above."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER

    # Base
    d.rounded_rectangle([cx - 210, cy - 160, cx + 210, cy + 160],
                         radius=12, fill=WARM_BROWN)
    d.rounded_rectangle([cx - 200, cy - 150, cx + 200, cy + 150],
                         radius=10, fill=(80, 55, 30, 255))

    # Platter
    r = 120
    px, py = cx - 30, cy
    d.ellipse([px - r, py - r, px + r, py + r], fill=(50, 35, 20, 255), outline=GOLD, width=3)

    # Record on platter
    d.ellipse([px - 100, py - 100, px + 100, py + 100], fill=(30, 30, 30, 240))
    for gr in range(90, 30, -6):
        d.ellipse([px - gr, py - gr, px + gr, py + gr], outline=(50, 50, 50, 180), width=1)
    d.ellipse([px - 25, py - 25, px + 25, py + 25], fill=DEEP_RED)
    d.ellipse([px - 4, py - 4, px + 4, py + 4], fill=(40, 40, 40, 255))

    # Tonearm
    arm_base_x, arm_base_y = cx + 150, cy - 110
    d.ellipse([arm_base_x - 15, arm_base_y - 15, arm_base_x + 15, arm_base_y + 15],
              fill=GOLD)
    d.line([(arm_base_x, arm_base_y), (px + 60, py - 50)], fill=GOLD, width=5)
    d.line([(px + 60, py - 50), (px + 80, py - 30)], fill=GOLD, width=3)

    # Speed selector
    d.ellipse([cx + 160, cy + 100, cx + 180, cy + 120], fill=GOLD)

    return img


def draw_star_decoration():
    """Draw a vintage star/starburst."""
    img = new_canvas()
    d = ImageDraw.Draw(img)
    cx, cy = CENTER, CENTER

    # Starburst
    num_points = 8
    for i in range(num_points):
        angle = (360 / num_points) * i
        a_rad = math.radians(angle)

        # Long ray
        x_out = cx + int(220 * math.cos(a_rad))
        y_out = cy + int(220 * math.sin(a_rad))
        half = math.radians(360 / num_points / 4)
        x1 = cx + int(30 * math.cos(a_rad - half))
        y1 = cy + int(30 * math.sin(a_rad - half))
        x2 = cx + int(30 * math.cos(a_rad + half))
        y2 = cy + int(30 * math.sin(a_rad + half))
        d.polygon([(x1, y1), (x_out, y_out), (x2, y2)], fill=GOLD)

        # Short ray between
        angle2 = angle + 360 / num_points / 2
        a2 = math.radians(angle2)
        x_out2 = cx + int(130 * math.cos(a2))
        y_out2 = cy + int(130 * math.sin(a2))
        x3 = cx + int(25 * math.cos(a2 - half))
        y3 = cy + int(25 * math.sin(a2 - half))
        x4 = cx + int(25 * math.cos(a2 + half))
        y4 = cy + int(25 * math.sin(a2 + half))
        d.polygon([(x3, y3), (x_out2, y_out2), (x4, y4)], fill=AMBER)

    # Center circle
    d.ellipse([cx - 35, cy - 35, cx + 35, cy + 35], fill=GOLD)
    d.ellipse([cx - 25, cy - 25, cx + 25, cy + 25], fill=CREAM)

    return img


# =========================================================
# Main: generate all stickers
# =========================================================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stickers = []

    def add(name, img):
        path = os.path.join(OUTPUT_DIR, name)
        img.save(path, "PNG")
        stickers.append(name)
        print(f"  [OK] {name}")

    print("=" * 60)
    print("Generating Mediterranean Music Stickers")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

    # Vinyl records (4 variants)
    print("\n-- Vinyl Records --")
    for i in range(4):
        add(f"vinyl_record_{i+1:02d}.png", draw_vinyl_record(i))

    # Turntable
    print("\n-- Turntable --")
    add("turntable_01.png", draw_turntable())

    # Headphones (3 variants)
    print("\n-- Headphones --")
    for i in range(3):
        add(f"headphones_{i+1:02d}.png", draw_headphones(i))

    # Musical notes (4 variants)
    print("\n-- Musical Notes --")
    for i in range(4):
        add(f"musical_note_{i+1:02d}.png", draw_musical_note(i))

    # Microphones (2 variants)
    print("\n-- Microphones --")
    for i in range(2):
        add(f"microphone_{i+1:02d}.png", draw_microphone(i))

    # Guitars (2 variants)
    print("\n-- Guitars --")
    for i in range(2):
        add(f"guitar_{i+1:02d}.png", draw_guitar(i))

    # Piano keys (2 variants)
    print("\n-- Piano Keys --")
    for i in range(2):
        add(f"piano_keys_{i+1:02d}.png", draw_piano_keys(i))

    # Saxophone
    print("\n-- Saxophone --")
    add("saxophone_01.png", draw_saxophone(0))

    # Trumpet
    print("\n-- Trumpet --")
    add("trumpet_01.png", draw_trumpet(0))

    # Speakers (2 variants)
    print("\n-- Speakers --")
    for i in range(2):
        add(f"speaker_{i+1:02d}.png", draw_speaker(i))

    # Cassette tapes (2 variants)
    print("\n-- Cassette Tapes --")
    for i in range(2):
        add(f"cassette_tape_{i+1:02d}.png", draw_cassette(i))

    # Vintage radios (2 variants)
    print("\n-- Vintage Radios --")
    for i in range(2):
        add(f"vintage_radio_{i+1:02d}.png", draw_vintage_radio(i))

    # Bossa Nova elements (3 variants)
    print("\n-- Bossa Nova / Jazz Elements --")
    for i in range(3):
        add(f"bossa_nova_{i+1:02d}.png", draw_bossa_nova_element(i))

    # Sun (2 variants)
    print("\n-- Sun / Mediterranean --")
    for i in range(2):
        add(f"sun_{i+1:02d}.png", draw_sun(i))

    # Wave / ocean
    print("\n-- Wave / Ocean --")
    add("wave_01.png", draw_wave(0))

    # Star decoration
    print("\n-- Star Decoration --")
    add("star_decoration_01.png", draw_star_decoration())

    # Summary
    print("\n" + "=" * 60)
    print(f"DONE: Generated {len(stickers)} stickers")
    print(f"Location: {OUTPUT_DIR}")
    print("=" * 60)
    print("\nAll stickers:")
    for i, name in enumerate(stickers, 1):
        print(f"  {i:2d}. {name}")


if __name__ == "__main__":
    main()
