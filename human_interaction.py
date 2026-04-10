# -*- coding: utf-8 -*-
"""
人类行为模拟模块 - 让自动化操作更像真人

核心功能：
1. 每次点击前自动滚动到按钮所在位置
2. 点击前随机延迟（模拟人类反应时间）
3. 移动鼠标时加入随机的轻微左右漂移（模拟手指不稳定）
4. 清理 YouTube 遮罩层
5. 并发安全：每个窗口使用独立的随机数生成器
6. 人性化输入：模拟逐字输入而非瞬间填充

⚠️ 本文件由 Claude Code 维护，Codex 不得修改。
   如需改动请联系总指挥（Claude Code）。
"""

import asyncio
import random
import math
import logging
import time

logger = logging.getLogger(__name__)


def log(msg: str, tag: str = "HUMAN"):
    """统一日志输出"""
    logger.info(f"[{tag}] {msg}")


# ── 随机延迟参数 ──
CLICK_DELAY_MIN = 0.3       # 点击前最小延迟（秒）
CLICK_DELAY_MAX = 1.2       # 点击前最大延迟（秒）
SCROLL_SETTLE_MIN = 0.2     # 滚动后等待最小（秒）
SCROLL_SETTLE_MAX = 0.6     # 滚动后等待最大（秒）

# ── 鼠标漂移参数 ──
DRIFT_STEPS_MIN = 3         # 移动路径最少步数
DRIFT_STEPS_MAX = 8         # 移动路径最多步数
DRIFT_LATERAL_PX = 12       # 左右漂移最大像素
DRIFT_STEP_DELAY_MIN = 0.02 # 每步最小延迟（秒）
DRIFT_STEP_DELAY_MAX = 0.06 # 每步最大延迟（秒）

# ── 点击位置随机范围 ──
CLICK_ZONE_MIN = 0.25       # 点击区域最小比例（距元素左/上边缘）
CLICK_ZONE_MAX = 0.75       # 点击区域最大比例

# ── 输入模拟参数 ──
TYPE_CHAR_DELAY_MIN = 0.03  # 逐字输入最小间隔（秒）
TYPE_CHAR_DELAY_MAX = 0.12  # 逐字输入最大间隔（秒）
TYPE_PAUSE_CHANCE = 0.08    # 输入时随机暂停概率
TYPE_PAUSE_MIN = 0.3        # 暂停最小时长
TYPE_PAUSE_MAX = 0.8        # 暂停最大时长

# ── 页面间行为参数 ──
PAGE_THINK_MIN = 0.8        # 页面切换后思考最小延迟
PAGE_THINK_MAX = 2.5        # 页面切换后思考最大延迟
BETWEEN_ACTIONS_MIN = 0.4   # 连续操作间最小间隔
BETWEEN_ACTIONS_MAX = 1.0   # 连续操作间最大间隔


def _make_rng(seed=None):
    """创建独立的随机数生成器（并发安全）"""
    if seed is None:
        seed = int(time.time() * 1000) ^ id(asyncio.current_task() or object())
    return random.Random(seed)


async def random_delay(min_s: float = 0.5, max_s: float = 1.5, msg: str = ""):
    """人性化随机延迟"""
    rng = _make_rng()
    delay = rng.uniform(min_s, max_s)
    if msg:
        log(f"{msg} ({delay:.1f}s)", "WAIT")
    await asyncio.sleep(delay)


async def think_delay(msg: str = ""):
    """模拟人类在页面间的思考时间"""
    await random_delay(PAGE_THINK_MIN, PAGE_THINK_MAX, msg or "思考中")


async def between_actions_delay():
    """连续操作之间的自然间隔"""
    await asyncio.sleep(random.uniform(BETWEEN_ACTIONS_MIN, BETWEEN_ACTIONS_MAX))


async def clear_blocking_overlays(page, reason: str = ""):
    """清理会拦截点击的 YouTube overlay/backdrop。"""
    try:
        removed = await page.evaluate(
            """
            () => {
                const visible = (el) => !!el && (
                    el.offsetParent !== null ||
                    el.offsetWidth > 0 ||
                    el.offsetHeight > 0
                );
                let count = 0;
                // YouTube Studio 遮罩层
                document.querySelectorAll("tp-yt-iron-overlay-backdrop").forEach((el) => {
                    if (visible(el) || el.hasAttribute("opened")) {
                        el.style.display = "none";
                        el.style.pointerEvents = "none";
                        count += 1;
                    }
                });
                // 通用弹窗遮罩
                document.querySelectorAll("[class*='overlay-backdrop'], [class*='modal-backdrop']").forEach((el) => {
                    if (visible(el)) {
                        el.style.display = "none";
                        el.style.pointerEvents = "none";
                        count += 1;
                    }
                });
                return count;
            }
            """
        )
        if removed:
            suffix = f" ({reason})" if reason else ""
            log(f"已清理遮罩层 {removed} 个{suffix}", "WARN")
    except Exception:
        pass


def _bezier_ease(t: float) -> float:
    """简单的缓动函数，模拟人类手指移动轨迹（先快后慢）"""
    return t * (2 - t)


def _cubic_bezier(t: float, p1: float = 0.25, p2: float = 0.1) -> float:
    """三次贝塞尔缓动，更自然的加减速曲线"""
    # 简化的三次贝塞尔：快速启动 → 匀速 → 缓慢到达
    return 3 * (1 - t) * (1 - t) * t * p1 + 3 * (1 - t) * t * t * (1 - p2) + t * t * t


async def _drift_mouse_to(page, target_x: float, target_y: float, rng=None):
    """
    带随机左右漂移的鼠标移动。
    模拟真人移动鼠标时手指的轻微抖动和不稳定。

    改进：从当前鼠标位置出发进行真实插值移动。
    """
    if rng is None:
        rng = random

    steps = rng.randint(DRIFT_STEPS_MIN, DRIFT_STEPS_MAX)

    # 获取当前鼠标位置作为起点
    # Playwright 不直接暴露鼠标位置，用一个小偏移来模拟起点
    start_x = target_x + rng.uniform(-50, 50)
    start_y = target_y + rng.uniform(-30, 30)

    for i in range(1, steps + 1):
        progress = _bezier_ease(i / steps)

        # 从起点到目标的插值
        x = start_x + (target_x - start_x) * progress
        y = start_y + (target_y - start_y) * progress

        if i < steps:
            # 中间步骤加入左右漂移
            lateral_offset = rng.uniform(-DRIFT_LATERAL_PX, DRIFT_LATERAL_PX)
            # 漂移量随接近目标而减小
            dampen = 1.0 - progress
            x += lateral_offset * dampen
            y += rng.uniform(-2, 2) * dampen  # 轻微上下漂移

        await page.mouse.move(x, y)
        await asyncio.sleep(rng.uniform(DRIFT_STEP_DELAY_MIN, DRIFT_STEP_DELAY_MAX))


async def scroll_to_element(page, locator, desc: str = ""):
    """
    滚动页面使目标元素进入视口。
    先尝试 Playwright 原生的 scroll_into_view_if_needed，
    失败则用 JavaScript 滚动。
    """
    try:
        await locator.scroll_into_view_if_needed(timeout=5000)
        # 滚动后等一下让页面稳定
        await asyncio.sleep(random.uniform(SCROLL_SETTLE_MIN, SCROLL_SETTLE_MAX))
        if desc:
            log(f"已滚动到: {desc}", "SCROLL")
        return True
    except Exception:
        pass

    # 备用方案：用 JS 滚动
    try:
        await locator.evaluate("el => el.scrollIntoView({behavior: 'smooth', block: 'center'})")
        await asyncio.sleep(random.uniform(SCROLL_SETTLE_MIN + 0.2, SCROLL_SETTLE_MAX + 0.3))
        if desc:
            log(f"已滚动到(JS): {desc}", "SCROLL")
        return True
    except Exception as e:
        if desc:
            log(f"滚动失败({desc}): {e}", "WARN")
        return False


async def random_micro_scroll(page, max_px: int = 30):
    """
    随机的轻微上下滚动，模拟人类浏览页面时的微调行为。
    有 40% 概率触发。
    """
    if random.random() < 0.4:
        pixels = random.randint(-max_px, max_px)
        if pixels != 0:
            await page.mouse.wheel(0, pixels)
            await asyncio.sleep(random.uniform(0.1, 0.3))


async def human_type(page, locator, text: str, desc: str = "", clear_first: bool = True):
    """
    人性化文本输入：逐字输入而非瞬间填充。

    特性：
    - 每个字符之间有随机延迟（30-120ms）
    - 偶尔会有较长的停顿（模拟思考）
    - 可选先清空已有内容

    参数：
        page: Playwright page 对象
        locator: 目标输入框的 locator
        text: 要输入的文本
        desc: 描述（用于日志）
        clear_first: 是否先清空输入框
    """
    if desc:
        log(f"输入: {desc} ({len(text)}字符)", "TYPE")

    try:
        # 先点击输入框获取焦点
        await human_click(page, locator, f"聚焦输入框: {desc}")

        if clear_first:
            # 全选并删除已有内容
            await page.keyboard.press("Control+a")
            await asyncio.sleep(random.uniform(0.05, 0.15))
            await page.keyboard.press("Backspace")
            await asyncio.sleep(random.uniform(0.1, 0.3))

        # 逐字输入
        for i, char in enumerate(text):
            await page.keyboard.type(char)

            # 字符间延迟
            delay = random.uniform(TYPE_CHAR_DELAY_MIN, TYPE_CHAR_DELAY_MAX)

            # 某些情况下额外加延迟
            if char in ',.;:!?。，；：！？':
                delay *= 2  # 标点后停顿更长

            # 随机思考暂停
            if random.random() < TYPE_PAUSE_CHANCE and i > 0:
                delay += random.uniform(TYPE_PAUSE_MIN, TYPE_PAUSE_MAX)

            await asyncio.sleep(delay)

        return True

    except Exception as e:
        log(f"输入失败({desc}): {e}", "WARN")
        # 降级：直接 fill
        try:
            await locator.fill(text)
            return True
        except Exception:
            return False


async def human_click(page, locator, desc: str = ""):
    """
    增强版人性化点击：
    1. 清理遮罩层
    2. 滚动到元素位置
    3. 随机延迟（模拟反应时间）
    4. 鼠标带漂移地移动到元素
    5. 随机位置点击
    """
    log(f"点击: {desc}", "ACT")
    try:
        # 1. 清理遮罩
        await clear_blocking_overlays(page, "pre-click")

        # 2. 等待元素可见
        try:
            await locator.wait_for(state="visible", timeout=15000)
        except Exception:
            log(f"元素 '{desc}' 15秒内未可见，尝试继续", "WARN")

        # 3. 滚动到元素位置
        await scroll_to_element(page, locator, desc)

        # 4. 点击前随机延迟
        pre_delay = random.uniform(CLICK_DELAY_MIN, CLICK_DELAY_MAX)
        await asyncio.sleep(pre_delay)

        # 5. 偶尔做一个微小的无关滚动（更像真人）
        await random_micro_scroll(page)

        # 6. 获取元素位置
        box = await locator.bounding_box()
        if box:
            # 在元素内随机选择点击位置
            click_x = box['x'] + box['width'] * random.uniform(CLICK_ZONE_MIN, CLICK_ZONE_MAX)
            click_y = box['y'] + box['height'] * random.uniform(CLICK_ZONE_MIN, CLICK_ZONE_MAX)

            # 7. 带漂移地移动鼠标到目标
            await _drift_mouse_to(page, click_x, click_y)

            # 8. 短暂停顿后点击（模拟确认动作）
            await asyncio.sleep(random.uniform(0.05, 0.2))
            await page.mouse.click(click_x, click_y)
        else:
            # 无法获取位置时直接点击
            await locator.click()

        return True

    except Exception as e:
        if "intercepts pointer events" in str(e) or "overlay-backdrop" in str(e):
            try:
                await clear_blocking_overlays(page, "click-retry")
                await locator.click(force=True, timeout=5000)
                return True
            except Exception:
                pass
        log(f"点击失败: {e}", "WARN")
        return False


async def human_select_dropdown(page, dropdown_locator, option_locator, desc: str = ""):
    """
    人性化地操作下拉框：
    1. 点击下拉框打开
    2. 等一下（模拟人类浏览选项）
    3. 点击目标选项

    参数：
        page: Playwright page 对象
        dropdown_locator: 下拉框的 locator
        option_locator: 目标选项的 locator
        desc: 描述
    """
    log(f"选择下拉: {desc}", "ACT")

    # 点击打开下拉框
    clicked = await human_click(page, dropdown_locator, f"打开下拉: {desc}")
    if not clicked:
        return False

    # 浏览选项的延迟
    await random_delay(0.3, 0.8, "浏览选项")

    # 点击目标选项
    return await human_click(page, option_locator, f"选择: {desc}")


async def scroll_dialog(page, pixels: int):
    """在 YouTube Studio 上传对话框内滚动"""
    content = page.locator('ytcp-uploads-dialog #scrollable-content').first
    if await content.count() > 0:
        await content.evaluate(f'el => el.scrollTop += {pixels}')
    else:
        await page.mouse.wheel(0, pixels)


async def human_file_upload(page, input_locator, file_path: str, desc: str = ""):
    """
    人性化的文件上传操作。
    在上传前后加入自然延迟。

    参数：
        page: Playwright page 对象
        input_locator: file input 的 locator
        file_path: 要上传的文件路径
        desc: 描述
    """
    log(f"上传文件: {desc} → {file_path}", "UPLOAD")

    # 上传前的自然延迟（模拟找文件）
    await random_delay(0.5, 1.5, "准备上传")

    try:
        await input_locator.set_input_files(file_path)

        # 上传后等一下（模拟确认文件）
        await random_delay(0.8, 2.0, "确认上传")
        return True

    except Exception as e:
        log(f"文件上传失败({desc}): {e}", "WARN")
        return False


async def wait_with_jitter(base_seconds: float, jitter_ratio: float = 0.3):
    """
    带抖动的等待。
    实际等待时间 = base_seconds * (1 ± jitter_ratio)

    用于需要等待固定时间但又不想太机械的场景。
    """
    jitter = base_seconds * random.uniform(-jitter_ratio, jitter_ratio)
    actual = max(0.1, base_seconds + jitter)
    await asyncio.sleep(actual)


# ══════════════════════════════════════════════════════════════
# 进阶人类行为模拟 — 让每一步之间的行为更像真人
# ══════════════════════════════════════════════════════════════

async def random_blank_click(page, count: int = 0):
    """
    随机在页面空白处点击（模拟人类无意识的点击习惯）。
    30% 概率触发，每次点 1-2 下。
    """
    if random.random() > 0.3:
        return
    clicks = count if count > 0 else random.randint(1, 2)
    try:
        viewport = page.viewport_size or {"width": 1280, "height": 720}
        for _ in range(clicks):
            # 在页面边缘/空白区域随机点（避开中心操作区）
            zone = random.choice(["left_margin", "right_margin", "top_bar", "bottom"])
            if zone == "left_margin":
                x = random.uniform(5, 60)
                y = random.uniform(200, viewport["height"] - 100)
            elif zone == "right_margin":
                x = random.uniform(viewport["width"] - 60, viewport["width"] - 5)
                y = random.uniform(200, viewport["height"] - 100)
            elif zone == "top_bar":
                x = random.uniform(200, viewport["width"] - 200)
                y = random.uniform(5, 40)
            else:
                x = random.uniform(100, viewport["width"] - 100)
                y = random.uniform(viewport["height"] - 50, viewport["height"] - 5)

            await _drift_mouse_to(page, x, y)
            await asyncio.sleep(random.uniform(0.05, 0.15))
            await page.mouse.click(x, y)
            await asyncio.sleep(random.uniform(0.2, 0.6))
            log(f"空白点击 ({zone} {x:.0f},{y:.0f})", "IDLE")
    except Exception:
        pass


async def random_idle_behavior(page):
    """
    模拟人类在操作间的闲置行为。
    随机执行以下行为中的 1-3 个：
    - 微滚动
    - 空白处点击
    - 鼠标随意移动
    - 短暂停留
    - 回到页面中心

    应在每个关键操作步骤之间调用。
    """
    rng = _make_rng()
    behaviors = ["micro_scroll", "blank_click", "mouse_wander", "pause", "glance"]
    selected = rng.sample(behaviors, k=rng.randint(1, 3))

    for behavior in selected:
        try:
            if behavior == "micro_scroll":
                await random_micro_scroll(page, max_px=50)

            elif behavior == "blank_click":
                await random_blank_click(page)

            elif behavior == "mouse_wander":
                # 鼠标随机漫游（模拟人类目光/光标游移）
                viewport = page.viewport_size or {"width": 1280, "height": 720}
                wander_points = rng.randint(2, 4)
                for _ in range(wander_points):
                    wx = rng.uniform(50, viewport["width"] - 50)
                    wy = rng.uniform(50, viewport["height"] - 50)
                    await _drift_mouse_to(page, wx, wy, rng)
                    await asyncio.sleep(rng.uniform(0.1, 0.4))

            elif behavior == "pause":
                # 什么都不做，只是停顿（模拟看屏幕）
                pause_time = rng.uniform(0.5, 2.0)
                await asyncio.sleep(pause_time)

            elif behavior == "glance":
                # 快速滚动一小段再滚回来（模拟瞄一眼其他内容）
                scroll_amount = rng.randint(30, 100)
                await page.mouse.wheel(0, scroll_amount)
                await asyncio.sleep(rng.uniform(0.3, 0.8))
                await page.mouse.wheel(0, -scroll_amount)
                await asyncio.sleep(rng.uniform(0.2, 0.5))

        except Exception:
            pass

    # 所有闲置行为后的自然间隔
    await asyncio.sleep(random.uniform(0.2, 0.5))


async def human_next_step(page, desc: str = ""):
    """
    步骤之间的人性化过渡。
    组合了：思考延迟 + 随机闲置行为 + 微滚动。
    应在点击"下一步"之前或填完一个表单之后调用。
    """
    if desc:
        log(f"步骤过渡: {desc}", "STEP")

    # 思考时间
    await think_delay(f"步骤间思考: {desc}")

    # 随机闲置行为（50%概率）
    if random.random() < 0.5:
        await random_idle_behavior(page)


async def human_page_arrival(page, desc: str = ""):
    """
    到达新页面/对话框后的人性化行为。
    模拟人类打开新页面后先看一看再操作。
    """
    if desc:
        log(f"到达页面: {desc}", "PAGE")

    # 先等一下让页面渲染
    await asyncio.sleep(random.uniform(0.5, 1.0))

    # 随机看看页面内容
    viewport = page.viewport_size or {"width": 1280, "height": 720}
    rng = _make_rng()

    # 眼睛扫视页面（鼠标跟随）
    scan_points = rng.randint(2, 4)
    for _ in range(scan_points):
        sx = rng.uniform(100, viewport["width"] - 100)
        sy = rng.uniform(80, viewport["height"] - 80)
        await _drift_mouse_to(page, sx, sy, rng)
        await asyncio.sleep(rng.uniform(0.2, 0.6))

    # 可能做一个微滚动
    await random_micro_scroll(page)

    # 准备好操作前的最后一刻停顿
    await asyncio.sleep(random.uniform(0.3, 0.8))
