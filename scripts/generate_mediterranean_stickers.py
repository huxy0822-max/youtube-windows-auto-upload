#!/usr/bin/env python3
"""
生成 25+ 地中海风格音乐主题贴纸 (512x512 透明 PNG)
风格: 1960s 复古、暖金色、琥珀色调、Bossa Nova 元素
"""
import math, sys
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("pip install Pillow")
    sys.exit(1)

OUT = Path(__file__).resolve().parent.parent / "stickers" / "mediterranean"
OUT.mkdir(parents=True, exist_ok=True)

# ── 色板：地中海暖色系 ──
GOLD      = (212, 175, 55, 255)
AMBER     = (255, 191, 0, 255)
CREAM     = (255, 253, 208, 255)
WARM_WHITE= (255, 248, 231, 255)
CORAL     = (240, 128, 100, 255)
SKY_BLUE  = (135, 206, 235, 255)
SEA_BLUE  = (70, 130, 180, 255)
TERRACOTTA= (204, 119, 77, 255)
OLIVE     = (128, 128, 0, 255)
WINE_RED  = (114, 47, 55, 255)
DARK_BROWN= (101, 67, 33, 255)
LIGHT_GOLD= (255, 223, 128, 255)
SUNSET_ORG= (255, 165, 79, 255)


def new_canvas(size=512):
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    return img, ImageDraw.Draw(img)


def save(img, name):
    p = OUT / f"{name}.png"
    img.save(str(p), "PNG")
    print(f"  ✓ {name}.png")


# ══════════════════════════════════════════
# 贴纸 1-4: 黑胶唱片 (Vinyl Record)
# ══════════════════════════════════════════
def vinyl_record(variant=1):
    img, d = new_canvas()
    cx, cy = 256, 256
    colors = [GOLD, AMBER, TERRACOTTA, WINE_RED]
    col = colors[(variant - 1) % len(colors)]
    # 外圈
    d.ellipse([56, 56, 456, 456], fill=(30, 30, 30, 240))
    # 纹路环
    for r in range(60, 196, 8):
        d.ellipse([cx-r, cy-r, cx+r, cy+r], outline=(50, 50, 50, 120), width=1)
    # 标签
    d.ellipse([cx-70, cy-70, cx+70, cy+70], fill=col)
    d.ellipse([cx-12, cy-12, cx+12, cy+12], fill=(40, 40, 40, 255))
    # 高光
    d.arc([80, 80, 280, 280], 200, 320, fill=(255, 255, 255, 50), width=3)
    save(img, f"vinyl_record_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 5-7: 耳机 (Headphones)
# ══════════════════════════════════════════
def headphones(variant=1):
    img, d = new_canvas()
    cols = [GOLD, CREAM, SEA_BLUE]
    col = cols[(variant-1) % len(cols)]
    # 头带
    d.arc([120, 80, 392, 340], 180, 360, fill=col, width=18)
    # 左耳罩
    d.rounded_rectangle([100, 260, 190, 400], radius=20, fill=col)
    d.rounded_rectangle([110, 270, 180, 390], radius=15, fill=(col[0]//2, col[1]//2, col[2]//2, 200))
    # 右耳罩
    d.rounded_rectangle([322, 260, 412, 400], radius=20, fill=col)
    d.rounded_rectangle([332, 270, 402, 390], radius=15, fill=(col[0]//2, col[1]//2, col[2]//2, 200))
    # 垫圈
    d.ellipse([108, 300, 182, 370], outline=(255,255,255,80), width=2)
    d.ellipse([330, 300, 404, 370], outline=(255,255,255,80), width=2)
    save(img, f"headphones_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 8-11: 音符 (Musical Notes)
# ══════════════════════════════════════════
def musical_note(variant=1):
    img, d = new_canvas()
    cols = [GOLD, AMBER, CREAM, CORAL]
    col = cols[(variant-1) % len(cols)]
    if variant <= 2:
        # 单音符 ♪
        d.ellipse([140, 310, 240, 390], fill=col)
        d.rectangle([230, 120, 248, 320], fill=col)
        d.polygon([(248, 120), (320, 170), (320, 200), (248, 160)], fill=col)
        # 光晕
        d.ellipse([130, 300, 250, 400], outline=(255,255,255,60), width=2)
    else:
        # 双音符 ♫
        d.ellipse([120, 310, 210, 380], fill=col)
        d.ellipse([290, 330, 380, 400], fill=col)
        d.rectangle([200, 130, 216, 320], fill=col)
        d.rectangle([370, 150, 386, 340], fill=col)
        d.rectangle([216, 130, 370, 150], fill=col)
        d.rectangle([216, 170, 370, 188], fill=col)
    save(img, f"music_note_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 12-14: 麦克风 (Microphone)
# ══════════════════════════════════════════
def microphone(variant=1):
    img, d = new_canvas()
    cols = [GOLD, CREAM, TERRACOTTA]
    col = cols[(variant-1) % len(cols)]
    cx = 256
    # 话筒头 (椭圆)
    d.rounded_rectangle([196, 100, 316, 280], radius=60, fill=col)
    # 网格线
    for y in range(120, 270, 16):
        d.line([(206, y), (306, y)], fill=(col[0]//2, col[1]//2, col[2]//2, 150), width=1)
    # 手柄
    d.rectangle([240, 280, 272, 420], fill=(col[0]*3//4, col[1]*3//4, col[2]*3//4, 255))
    # 底座
    d.ellipse([210, 400, 302, 440], fill=col)
    save(img, f"microphone_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 15-16: 吉他 (Guitar)
# ══════════════════════════════════════════
def guitar(variant=1):
    img, d = new_canvas()
    col = AMBER if variant == 1 else TERRACOTTA
    # 琴颈
    d.rectangle([240, 40, 272, 240], fill=DARK_BROWN)
    # 品格
    for y in range(60, 230, 24):
        d.line([(242, y), (270, y)], fill=GOLD, width=1)
    # 琴身上部
    d.ellipse([160, 200, 352, 360], fill=col)
    # 琴身下部
    d.ellipse([140, 290, 372, 470], fill=col)
    # 音孔
    d.ellipse([220, 300, 292, 372], outline=DARK_BROWN, width=4)
    d.ellipse([230, 310, 282, 362], outline=DARK_BROWN, width=2)
    # 琴弦
    for x in range(248, 268, 4):
        d.line([(x, 60), (x, 440)], fill=(200, 200, 200, 100), width=1)
    # 琴桥
    d.rectangle([235, 400, 277, 412], fill=DARK_BROWN)
    save(img, f"guitar_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 17-18: 钢琴键 (Piano Keys)
# ══════════════════════════════════════════
def piano_keys(variant=1):
    img, d = new_canvas()
    col = GOLD if variant == 1 else CREAM
    # 外框
    d.rounded_rectangle([60, 140, 452, 380], radius=10, fill=col, outline=DARK_BROWN, width=3)
    # 白键
    kw = 48
    for i in range(8):
        x = 68 + i * kw
        d.rectangle([x, 150, x + kw - 4, 370], fill=WARM_WHITE, outline=(180, 170, 150, 200), width=1)
    # 黑键
    black_pos = [0, 1, 3, 4, 5]
    for i in black_pos:
        x = 68 + i * kw + kw * 2 // 3
        d.rectangle([x, 150, x + 28, 280], fill=(30, 30, 30, 240))
    save(img, f"piano_keys_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 19-20: 萨克斯风 / 小号 (Saxophone / Trumpet)
# ══════════════════════════════════════════
def saxophone(variant=1):
    img, d = new_canvas()
    col = GOLD
    # 简化萨克斯形状
    # 吹嘴
    d.rectangle([200, 60, 220, 140], fill=col)
    # 上管
    d.rectangle([195, 130, 225, 300], fill=col)
    # 弯管
    d.arc([180, 270, 340, 420], 90, 270, fill=col, width=28)
    # 喇叭口
    d.ellipse([260, 340, 380, 460], fill=col)
    d.ellipse([275, 355, 365, 445], fill=(col[0]*3//4, col[1]*3//4, col[2]*3//4, 200))
    # 按键
    for y in range(160, 290, 25):
        d.ellipse([228, y, 248, y+14], fill=LIGHT_GOLD)
    save(img, f"saxophone_{variant:02d}")


def trumpet(variant=1):
    img, d = new_canvas()
    col = GOLD if variant == 1 else AMBER
    # 管身
    d.rectangle([80, 240, 350, 270], fill=col)
    # 活塞
    for x in [160, 220, 280]:
        d.rectangle([x, 190, x+24, 245], fill=LIGHT_GOLD)
        d.ellipse([x-2, 180, x+26, 200], fill=col)
    # 吹嘴
    d.rectangle([60, 245, 85, 265], fill=col)
    d.ellipse([45, 242, 65, 268], fill=col)
    # 喇叭口
    d.pieslice([310, 190, 460, 320], -50, 50, fill=col)
    d.ellipse([390, 210, 450, 300], fill=(col[0]*3//4, col[1]*3//4, col[2]*3//4, 200))
    save(img, f"trumpet_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 21-22: 音箱 / 扬声器 (Speaker)
# ══════════════════════════════════════════
def speaker(variant=1):
    img, d = new_canvas()
    col = DARK_BROWN if variant == 1 else TERRACOTTA
    # 箱体
    d.rounded_rectangle([130, 80, 382, 432], radius=18, fill=col)
    d.rounded_rectangle([140, 90, 372, 422], radius=14, outline=GOLD, width=2)
    # 大喇叭
    d.ellipse([180, 200, 332, 352], fill=(50, 50, 50, 220))
    d.ellipse([210, 230, 302, 322], fill=(80, 80, 80, 200))
    d.ellipse([235, 255, 277, 297], fill=(60, 60, 60, 230))
    # 小高音
    d.ellipse([225, 110, 287, 172], fill=(50, 50, 50, 220))
    d.ellipse([240, 125, 272, 157], fill=(80, 80, 80, 200))
    # 品牌文字区
    d.rounded_rectangle([215, 385, 297, 410], radius=5, fill=GOLD)
    save(img, f"speaker_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 23-24: 卡带 (Cassette Tape)
# ══════════════════════════════════════════
def cassette_tape(variant=1):
    img, d = new_canvas()
    col = CREAM if variant == 1 else AMBER
    # 外壳
    d.rounded_rectangle([70, 140, 442, 370], radius=14, fill=col, outline=DARK_BROWN, width=3)
    # 标签区
    d.rounded_rectangle([100, 160, 412, 260], radius=6, fill=WARM_WHITE)
    # 标签线条装饰
    for y in range(175, 250, 12):
        d.line([(120, y), (392, y)], fill=(200, 180, 140, 100), width=1)
    # 磁带窗口
    d.rounded_rectangle([160, 275, 352, 340], radius=8, fill=(40, 40, 40, 220))
    # 卷轴
    d.ellipse([185, 285, 245, 335], fill=(60, 60, 60, 255))
    d.ellipse([205, 300, 225, 320], fill=(100, 100, 100, 255))
    d.ellipse([267, 285, 327, 335], fill=(60, 60, 60, 255))
    d.ellipse([287, 300, 307, 320], fill=(100, 100, 100, 255))
    # 螺丝
    for pos in [(90, 150), (422, 150), (90, 350), (422, 350)]:
        d.ellipse([pos[0]-6, pos[1]-6, pos[0]+6, pos[1]+6], fill=(180, 170, 150, 200))
    save(img, f"cassette_tape_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 25-26: 复古收音机 (Vintage Radio)
# ══════════════════════════════════════════
def vintage_radio(variant=1):
    img, d = new_canvas()
    col = TERRACOTTA if variant == 1 else DARK_BROWN
    # 机身
    d.rounded_rectangle([80, 130, 432, 400], radius=24, fill=col)
    # 扬声器格栅
    for y in range(160, 310, 8):
        d.line([(110, y), (300, y)], fill=(col[0]*3//4, col[1]*3//4, col[2]*3//4, 180), width=2)
    # 调频窗
    d.rounded_rectangle([320, 160, 410, 230], radius=6, fill=CREAM)
    d.line([(330, 195), (400, 195)], fill=WINE_RED, width=2)
    # 旋钮
    d.ellipse([330, 260, 370, 300], fill=GOLD)
    d.ellipse([340, 270, 360, 290], fill=(col[0]//2, col[1]//2, col[2]//2, 200))
    d.ellipse([370, 260, 410, 300], fill=GOLD)
    d.ellipse([380, 270, 400, 290], fill=(col[0]//2, col[1]//2, col[2]//2, 200))
    # 底部脚
    d.rounded_rectangle([120, 390, 160, 420], radius=4, fill=DARK_BROWN)
    d.rounded_rectangle([352, 390, 392, 420], radius=4, fill=DARK_BROWN)
    save(img, f"vintage_radio_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 27-28: 地中海太阳 (Mediterranean Sun)
# ══════════════════════════════════════════
def mediterranean_sun(variant=1):
    img, d = new_canvas()
    col = GOLD if variant == 1 else SUNSET_ORG
    cx, cy = 256, 256
    # 光芒
    for angle in range(0, 360, 15):
        rad = math.radians(angle)
        x1 = cx + int(90 * math.cos(rad))
        y1 = cy + int(90 * math.sin(rad))
        x2 = cx + int(200 * math.cos(rad))
        y2 = cy + int(200 * math.sin(rad))
        d.line([(x1, y1), (x2, y2)], fill=(col[0], col[1], col[2], 120), width=6)
    # 太阳圆
    d.ellipse([cx-90, cy-90, cx+90, cy+90], fill=col)
    d.ellipse([cx-70, cy-70, cx+70, cy+70], fill=LIGHT_GOLD)
    # 微笑
    d.arc([cx-35, cy-10, cx+35, cy+40], 10, 170, fill=DARK_BROWN, width=3)
    save(img, f"mediterranean_sun_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 29-30: 海浪 (Ocean Waves)
# ══════════════════════════════════════════
def ocean_wave(variant=1):
    img, d = new_canvas()
    col = SEA_BLUE if variant == 1 else SKY_BLUE
    # 多层波浪
    for layer in range(4):
        y_base = 220 + layer * 50
        alpha = 255 - layer * 40
        c = (col[0], col[1], col[2], alpha)
        points = []
        for x in range(40, 480, 4):
            y = y_base + int(30 * math.sin((x + layer * 40) * math.pi / 80))
            points.append((x, y))
        # 填充到底部
        points.append((476, 500))
        points.append((40, 500))
        d.polygon(points, fill=c)
    # 浪花
    for x in [120, 280, 400]:
        d.ellipse([x-12, 200, x+12, 220], fill=(255, 255, 255, 100))
    save(img, f"ocean_wave_{variant:02d}")


# ══════════════════════════════════════════
# 贴纸 31: 高脚杯 (Cocktail Glass - Lounge 元素)
# ══════════════════════════════════════════
def cocktail_glass():
    img, d = new_canvas()
    col = GOLD
    # 杯身 (三角形)
    d.polygon([(180, 160), (332, 160), (256, 340)], fill=(255, 220, 180, 160))
    d.polygon([(180, 160), (332, 160), (256, 340)], outline=col, width=3)
    # 杯沿
    d.line([(175, 160), (337, 160)], fill=col, width=4)
    # 杯柄
    d.rectangle([250, 340, 262, 410], fill=col)
    # 底座
    d.ellipse([210, 405, 302, 435], fill=col)
    # 橄榄
    d.ellipse([240, 195, 272, 225], fill=OLIVE)
    d.line([(256, 165), (256, 210)], fill=DARK_BROWN, width=2)
    save(img, "cocktail_glass_01")


# ══════════════════════════════════════════
# 贴纸 32: 棕榈树 (Palm Tree)
# ══════════════════════════════════════════
def palm_tree():
    img, d = new_canvas()
    # 树干
    d.rectangle([240, 250, 272, 460], fill=DARK_BROWN)
    d.rectangle([244, 250, 268, 460], fill=TERRACOTTA)
    # 横纹
    for y in range(260, 450, 20):
        d.line([(242, y), (270, y)], fill=DARK_BROWN, width=2)
    # 叶子 (扇形弧线)
    leaf_col = (60, 140, 60, 220)
    for angle_offset in [-60, -30, 0, 30, 60]:
        rad = math.radians(-90 + angle_offset)
        ex = 256 + int(180 * math.cos(rad))
        ey = 240 + int(120 * math.sin(rad))
        d.line([(256, 240), (ex, ey)], fill=leaf_col, width=8)
        # 叶片
        for t in range(3, 10):
            px = 256 + int(t * 18 * math.cos(rad))
            py = 240 + int(t * 12 * math.sin(rad))
            side_rad = rad + math.pi / 2
            sx, sy = int(12 * math.cos(side_rad)), int(12 * math.sin(side_rad))
            d.line([(px, py), (px + sx, py + sy)], fill=leaf_col, width=3)
            d.line([(px, py), (px - sx, py - sy)], fill=leaf_col, width=3)
    save(img, "palm_tree_01")


# ══════════════════════════════════════════
# 贴纸 33: 电影场记板 (Clapperboard)
# ══════════════════════════════════════════
def clapperboard():
    img, d = new_canvas()
    # 板身
    d.rounded_rectangle([90, 180, 422, 420], radius=8, fill=CREAM, outline=DARK_BROWN, width=3)
    # 拍板 (上部斜条纹)
    d.polygon([(90, 130), (422, 130), (422, 185), (90, 185)], fill=DARK_BROWN)
    stripe_w = 40
    for x in range(90, 430, stripe_w * 2):
        d.polygon([(x, 130), (x + stripe_w, 130), (x + stripe_w, 185), (x, 185)], fill=CREAM)
    # 文字行
    for y in [210, 260, 310, 360]:
        d.line([(120, y), (392, y)], fill=(180, 170, 150, 150), width=1)
    save(img, "clapperboard_01")


# ══════════════════════════════════════════
# 贴纸 34: 音叉 / 调音叉 (Tuning Fork)
# ══════════════════════════════════════════
def tuning_fork():
    img, d = new_canvas()
    col = GOLD
    # 双叉
    d.rounded_rectangle([215, 80, 235, 280], radius=6, fill=col)
    d.rounded_rectangle([277, 80, 297, 280], radius=6, fill=col)
    # 弧形连接
    d.arc([215, 260, 297, 320], 0, 180, fill=col, width=18)
    # 手柄
    d.rectangle([248, 310, 264, 450], fill=col)
    # 圆头
    d.ellipse([242, 440, 270, 468], fill=col)
    save(img, "tuning_fork_01")


# ══════════════════════════════════════════
# 贴纸 35: 节拍器 (Metronome)
# ══════════════════════════════════════════
def metronome():
    img, d = new_canvas()
    col = TERRACOTTA
    # 三角体
    d.polygon([(256, 80), (150, 430), (362, 430)], fill=col, outline=DARK_BROWN, width=3)
    # 面板
    d.polygon([(256, 140), (185, 400), (327, 400)], fill=CREAM)
    # 摆臂
    d.line([(256, 400), (310, 140)], fill=GOLD, width=4)
    d.ellipse([300, 128, 320, 148], fill=GOLD)
    # 刻度
    for y in range(180, 390, 25):
        w = int((y - 140) * 0.25)
        d.line([(256 - w, y), (256 - w + 10, y)], fill=DARK_BROWN, width=1)
    save(img, "metronome_01")


# ══════════════════════════════════════════
# 运行全部生成
# ══════════════════════════════════════════
def main():
    print("🎨 生成地中海风格音乐贴纸...")
    print(f"   输出目录: {OUT}\n")

    # 黑胶唱片 x4
    for i in range(1, 5):
        vinyl_record(i)
    # 耳机 x3
    for i in range(1, 4):
        headphones(i)
    # 音符 x4
    for i in range(1, 5):
        musical_note(i)
    # 麦克风 x3
    for i in range(1, 4):
        microphone(i)
    # 吉他 x2
    for i in range(1, 3):
        guitar(i)
    # 钢琴 x2
    for i in range(1, 3):
        piano_keys(i)
    # 萨克斯 x2
    for i in range(1, 3):
        saxophone(i)
    # 小号 x2
    for i in range(1, 3):
        trumpet(i)
    # 音箱 x2
    for i in range(1, 3):
        speaker(i)
    # 卡带 x2
    for i in range(1, 3):
        cassette_tape(i)
    # 收音机 x2
    for i in range(1, 3):
        vintage_radio(i)
    # 太阳 x2
    for i in range(1, 3):
        mediterranean_sun(i)
    # 海浪 x2
    for i in range(1, 3):
        ocean_wave(i)
    # 鸡尾酒杯
    cocktail_glass()
    # 棕榈树
    palm_tree()
    # 场记板
    clapperboard()
    # 音叉
    tuning_fork()
    # 节拍器
    metronome()

    total = len(list(OUT.glob("*.png")))
    print(f"\n✅ 共生成 {total} 个贴纸")


if __name__ == "__main__":
    main()
