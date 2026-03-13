#!/usr/bin/env python3
"""
批量频道诊断脚本 (重构版 v2.0)
scripts/batch_diagnose_channels.py

功能:
1. 交互式选择要抓取的分组 (Tag/Group)
2. 批量启动浏览器，模拟真人操作
3. **一开始就抓取频道名称，直接保存到频道文件夹**
4. 长截图（滚动拼接）
5. 每个视频独立文件夹
6. 增强数据提取（流量来源、设备、观众类型等）
7. 生成汇总表（CSV + Markdown）
8. **自动更新 channel_mapping.json**

使用方法:
  python3 batch_diagnose_channels.py           # 交互式选择分组
  python3 batch_diagnose_channels.py <容器码>   # 单频道模式
"""

import asyncio
import json
import os
import random
import re
import shutil
import sys
import requests
from datetime import datetime
from pathlib import Path
from browser_api import list_browser_envs, start_browser_debug_port, stop_browser_container, load_browser_settings
from path_helpers import resolve_config_file

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

# === 配置 ===
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get("UPLOAD_CONFIG_PATH", str(resolve_config_file(SCRIPT_DIR, "upload_config.json"))))
CHANNEL_MAPPING_FILE = str(Path(os.environ.get("CHANNEL_MAPPING_PATH", str(resolve_config_file(SCRIPT_DIR, "channel_mapping.json")))))
_browser_settings = load_browser_settings(CONFIG_PATH)
API_BASE_URL = _browser_settings.get("base_url", "http://127.0.0.1:6873")
DATA_FILE = str(SCRIPT_DIR / "data" / "hubstudio_all_containers.json")
OUTPUT_DIR = str(SCRIPT_DIR / "diagnose_output")
DEBUG_DIR = str(Path(OUTPUT_DIR) / "debug_screenshots")
SCRAPED_RECORD_FILE = str(Path(OUTPUT_DIR) / "scraped_videos.json")
HUBSTUDIO_LIVE_TO_CANONICAL = {
    "LoFi嘻哈": "Lo-Fi嘻哈",
}

# Windows 控制台默认可能是 GBK，含 emoji 输出会抛 UnicodeEncodeError。
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# 跳过这些分组（Tag）
SKIP_TAGS = ['遺棄', '未使用新频道', '无标签']

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

processed_videos = set()
current_channel_dir = None  # 当前频道的输出目录
current_channel_name = None  # 当前频道名称
current_container_info = None  # 当前容器信息


def normalize_hubstudio_tag(tag):
    return HUBSTUDIO_LIVE_TO_CANONICAL.get(tag, tag)

def load_scraped_record():
    """加载已抓取视频记录"""
    if os.path.exists(SCRAPED_RECORD_FILE):
        try:
            with open(SCRAPED_RECORD_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get('video_ids', []))
        except:
            pass
    return set()

def save_scraped_record(video_ids: set):
    """保存已抓取视频记录"""
    data = {
        'updated_at': datetime.now().isoformat(),
        'count': len(video_ids),
        'video_ids': list(video_ids)
    }
    with open(SCRAPED_RECORD_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def update_channel_mapping(container_code: str, serial_number: int, tag: str, channel_name: str, channel_id: str = None, update_time: bool = True):
    """更新频道映射配置"""
    try:
        if os.path.exists(CHANNEL_MAPPING_FILE):
            with open(CHANNEL_MAPPING_FILE, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
        else:
            mapping = {"version": "1.0", "channels": {}}
        
        mapping['last_updated'] = datetime.now().isoformat()
        
        # 获取旧数据以保留 scraped_at (如果 update_time 为 False)
        old_data = mapping['channels'].get(str(container_code), {})
        old_scraped_at = old_data.get('scraped_at')
        
        new_scraped_at = old_scraped_at
        if update_time:
            new_scraped_at = datetime.now().isoformat()
            
        mapping['channels'][str(container_code)] = {
            "serial_number": serial_number,
            "tag": tag,
            "channel_name": channel_name,
            "channel_id": channel_id,
            "scraped_at": new_scraped_at
        }
        
        with open(CHANNEL_MAPPING_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        
        if update_time:
            print(f"   📝 频道映射已更新 & 标记完成: {container_code} → {channel_name}")
        else:
            print(f"   📝 频道映射已更新 (未完成): {container_code} → {channel_name}")
            
    except Exception as e:
        print(f"   ⚠️ 更新映射失败: {e}")

# ============================================================
# 工具函数
# ============================================================

async def random_delay(min_sec=2.0, max_sec=4.0):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def human_like_mouse_move(page, start_x, start_y, end_x, end_y):
    """拟人鼠标移动"""
    steps = random.randint(8, 15)
    for i in range(steps):
        t = i / steps
        x = start_x + (end_x - start_x) * t + random.uniform(-3, 3)
        y = start_y + (end_y - start_y) * t + random.uniform(-3, 3)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.01, 0.03))
    await page.mouse.move(end_x, end_y)

async def human_click(page, locator, description=""):
    """人类化点击"""
    try:
        box = await locator.bounding_box()
        if box:
            x = box['x'] + box['width'] / 2 + random.uniform(-5, 5)
            y = box['y'] + box['height'] / 2 + random.uniform(-3, 3)
            await human_like_mouse_move(page, x - 50, y - 30, x, y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
            await page.mouse.click(x, y)
            if description:
                print(f"   🖱️ 点击: {description}")
            return True
    except Exception as e:
        print(f"   ⚠️ 点击失败 ({description}): {e}")
    return False

async def take_scroll_screenshot(page, save_path: str):
    """
    截取完整长图 - 方案 A: 暴力视口法 (Giant Viewport)
    直接将视口拉长到内容高度，一次性截图，避免拼接缝隙。
    """
    # 获取当前视口大小以备恢复
    original_viewport = page.viewport_size
    if not original_viewport:
        original_viewport = {"width": 1280, "height": 720}
    
    try:
        # 1. 获取内容真实高度
        # YouTube Studio 的内容通常在 main#main 中，但也可能是在 body
        # 我们获取 main#main 的 scrollHeight
        content_height = await page.evaluate("""
            () => {
                const container = document.querySelector('main#main');
                // 如果找不到 main，就退回到 body
                return container ? container.scrollHeight : document.body.scrollHeight;
            }
        """)
        
        # 增加一点安全余量 (头部导航栏等可能暂用空间)
        # 如果高度非常大，限制一下最大高度防止内存崩溃 (比如限制在 15000px)
        target_height = min(int(content_height) + 200, 15000)
        
        if target_height > original_viewport['height']:
            # 2. 调整视口大小
            await page.set_viewport_size({
                "width": original_viewport["width"],
                "height": target_height
            })
            
            # 3. 强制渲染
            # 有些图表是懒加载的，必须"出现在视口内"才会渲染。
            # 虽然现在视口变大了，但为了保险，稍微触发一下滚动事件或等待
            await asyncio.sleep(1.0) # 等待布局调整
            
            # 模拟一个微小的滚动来触发 IntersectionObserver
            await page.mouse.wheel(0, 100)
            await asyncio.sleep(0.5)
            await page.mouse.wheel(0, -100)
            
            # 再多等一会儿让图表动画完成
            await asyncio.sleep(2.0)
            
        # 4. 截图
        # 因为视口已经包含了所有内容，直接截取视口即可
        await page.screenshot(path=save_path)
        
        return save_path

    except Exception as e:
        print(f"      ⚠️ 长截图失败: {e}, 尝试普通截图")
        try:
            await page.screenshot(path=save_path, full_page=True)
        except:
            pass
        return save_path
        
    finally:
        # 5. 恢复视口
        try:
            await page.set_viewport_size(original_viewport)
        except:
            pass

# ============================================================
# 数据提取
# ============================================================

async def extract_title_description(page):
    """提取标题和简介 (增强版 - 带 placeholder 过滤和重试)"""
    
    # YouTube Studio 各语言的 placeholder 文字列表
    TITLE_PLACEHOLDERS = {
        'edit title', 'add a title', 'add title',
        '编辑标题', '添加标题', '新增標題', '編輯標題',
        'タイトルを編集', 'タイトルを追加',
        'titre', 'título', 'titolo',
    }
    
    DESC_PLACEHOLDERS = {
        'edit description', 'add a description', 'add description',
        'tell viewers about your video',
        '编辑说明', '添加说明', '新增說明', '編輯說明',
        '向觀眾介紹你的影片',
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        result = await page.evaluate(r"""
            () => {
                const result = {};
                
                // 标题选择器列表 (按优先级)
                const titleSelectors = [
                    '#textbox[aria-label*="title" i]',
                    '#title-textarea textarea',
                    'ytcp-social-suggestions-textbox[label*="title" i] #textbox',
                    '#basics #textbox:first-of-type',
                    'ytcp-video-metadata-editor-basics #title-textarea #textbox'
                ];
                
                for (const sel of titleSelectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const val = el.value || el.innerText || el.textContent || '';
                        if (val.trim()) {
                            result.title = val.trim();
                            break;
                        }
                    }
                }
                
                // 简介选择器列表 (按优先级)
                const descSelectors = [
                    '#textbox[aria-label*="description" i]',
                    '#description-textarea textarea',
                    'ytcp-social-suggestions-textbox[label*="description" i] #textbox',
                    '#basics #textbox:nth-of-type(2)',
                    'ytcp-video-metadata-editor-basics #description-textarea #textbox',
                    '#description-container #textbox',
                    'ytcp-mentions-textbox#description-textarea #textbox'
                ];
                
                for (const sel of descSelectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const val = el.value || el.innerText || el.textContent || '';
                        if (val.trim()) {
                            result.description = val.trim();
                            break;
                        }
                    }
                }
                
                return result;
            }
        """)
        
        # 检查标题是否为 placeholder
        title = result.get('title', '')
        title_is_placeholder = title.lower().strip() in TITLE_PLACEHOLDERS
        
        # 检查简介是否为 placeholder
        desc = result.get('description', '')
        desc_is_placeholder = desc.lower().strip() in DESC_PLACEHOLDERS
        
        if title_is_placeholder:
            result.pop('title', None)
            if attempt < max_retries - 1:
                print(f"      ⚠️ 标题为占位符 '{title}'，等待重试 ({attempt+1}/{max_retries})...")
                await asyncio.sleep(2)
                # 尝试点击标题区域激活编辑框
                try:
                    title_box = page.locator('#title-textarea, [aria-label*="title" i]').first
                    if await title_box.count() > 0:
                        await title_box.click()
                        await asyncio.sleep(1)
                except:
                    pass
                continue
        
        if desc_is_placeholder:
            result.pop('description', None)
        
        # 标题不是 placeholder，或者已经重试完毕
        break
    
    return result

async def extract_channel_name(page):
    """从 YouTube Studio 页面提取频道名称"""
    return await page.evaluate(r"""
        () => {
            // 方法1: 从 entity-name 获取 (验证成功 2026-01-31)
            const entityName = document.querySelector('#entity-name');
            if (entityName) {
                const val = entityName.innerText || entityName.textContent || '';
                if (val.trim()) return val.trim();
            }
            
            // 方法2: 从侧边栏账号信息获取
            const accountSelectors = [
                '#account-name',
                'ytcp-account-info #account-name',
                '.ytcp-account-info #account-name',
                'yt-formatted-string#account-name',
                '#channel-title',
                'ytd-channel-name #text'
            ];
            
            for (const sel of accountSelectors) {
                const el = document.querySelector(sel);
                if (el) {
                    const val = el.innerText || el.textContent || '';
                    if (val.trim()) return val.trim();
                }
            }
            
            // 方法3: 从页面标题提取 (过滤掉无意义的标题)
            const pageTitle = document.title;
            if (pageTitle && pageTitle.includes(' - YouTube Studio')) {
                const extracted = pageTitle.replace(' - YouTube Studio', '').trim();
                // 排除通用标题
                const ignoreTitles = ['Channel content', 'Dashboard', 'Analytics', 'Comments'];
                if (!ignoreTitles.includes(extracted)) {
                    return extracted;
                }
            }
            
            // 方法4: 从 URL 提取 channel ID (最后备用)
            const url = window.location.href;
            const channelMatch = url.match(/channel\/([^\/]+)/);
            if (channelMatch) return channelMatch[1];
            
            return null;
        }
    """)

async def extract_metrics(page):
    """提取页面上的所有指标（完整版 - 从 diagnose_single_channel.py 移植）"""
    return await page.evaluate(r"""
        () => {
            const data = {};
            const text = document.body.innerText;
            
            // === Helper: 展开 K/M/B 缩写为完整数字 ===
            function expandAbbrev(val) {
                if (!val) return val;
                val = val.trim();
                const m = val.match(/^([+-]?[\d,\.]+)\s*([KMBkmb万亿]?)$/);
                if (!m) return val;
                const suffix = m[2].toUpperCase();
                if (!suffix) return val;
                let num = parseFloat(m[1].replace(/,/g, ''));
                if (suffix === 'K') num *= 1000;
                else if (suffix === 'M' || suffix === '万') num *= (suffix === '万' ? 10000 : 1000000);
                else if (suffix === 'B' || suffix === '亿') num *= (suffix === '亿' ? 100000000 : 1000000000);
                if (isNaN(num)) return val;
                return Math.round(num).toLocaleString('en-US');
            }
            
            // 基础指标 - 英文 + 中文 (按优先级排列，先匹配的生效)
            // expand:true 表示需要对捕获值做 K/M/B 展开
            const patterns = [
                // Views - 优先匹配精确句子格式
                { re: /has gotten\s*([\d,]+)\s*views/i, key: 'Views' },
                { re: /获得了\s*([\d,]+)\s*次观看/i, key: 'Views' },
                // Views - header card 格式 (label\nvalue)
                { re: /\bViews\n([\d,\.]+[KMB]?)\n/i, key: 'Views', expand: true },
                { re: /\bViews\n([\d,\.]+[KMB]?)/i, key: 'Views', expand: true },
                // Views - 中文 card 格式
                { re: /观看次数\n([\d,\.]+[KMB万]?)/i, key: 'Views', expand: true },
                { re: /([\d,]+)\s*次观看/i, key: 'Views' },
                
                // Watch Hours
                { re: /Watch time \(hours\)\n([\d,\.]+)/i, key: 'Watch Hours' },
                { re: /Watch time \(hours\)\s+([\d,\.]+)/i, key: 'Watch Hours' },
                { re: /([\d,\.]+)\s*Watch time \(hours\)/i, key: 'Watch Hours' },
                { re: /观看时长[（(]小时[）)]\s*([\d,\.]+)/i, key: 'Watch Hours' },
                
                // Avg Duration
                { re: /Average view duration\n(\d+:\d+)/i, key: 'Avg Duration' },
                { re: /Average view duration\s+(\d+:\d+)/i, key: 'Avg Duration' },
                { re: /(\d+:\d+)\s*Average view duration/i, key: 'Avg Duration' },
                { re: /平均观看时长[\s\S]*?(\d+:\d+)/i, key: 'Avg Duration' },
                
                // Avg % Viewed
                { re: /Average percentage viewed\n(\d+\.?\d*)%/i, key: 'Avg % Viewed' },
                { re: /Average percentage viewed\s+(\d+\.?\d*)%/i, key: 'Avg % Viewed' },
                { re: /(\d+\.?\d*)%\s*Average percentage viewed/i, key: 'Avg % Viewed' },
                { re: /平均观看百分比[\s\S]*?(\d+\.?\d*)%/i, key: 'Avg % Viewed' },
                
                // CTR
                { re: /Impressions click-through rate\n(\d+\.?\d*)%/i, key: 'CTR' },
                { re: /Impressions click-through rate\s+(\d+\.?\d*)%/i, key: 'CTR' },
                { re: /(\d+\.?\d*)%\s*Impressions click-through rate/i, key: 'CTR' },
                { re: /曝光点击率[\s\S]*?(\d+\.?\d*)%/i, key: 'CTR' },
                { re: /点击率[\s\S]*?(\d+\.?\d*)%/i, key: 'CTR' },
                
                // Impressions - header card 格式
                { re: /\bImpressions\n([\d,\.]+[KMB]?)\n/i, key: 'Impressions', expand: true },
                { re: /\bImpressions\n([\d,\.]+[KMB]?)/i, key: 'Impressions', expand: true },
                { re: /([\d,\.]+[KMB]?)\s*Impressions(?!\s*click)/i, key: 'Impressions', expand: true },
                { re: /曝光次数[\s\S]*?([\d,\.]+[KMB万亿]?)/i, key: 'Impressions', expand: true },
                
                // Revenue
                { re: /Estimated revenue\n(\$[\d,\.]+)/i, key: 'Revenue' },
                { re: /Estimated revenue\s+(\$[\d,\.]+)/i, key: 'Revenue' },
                { re: /预估收入[\s\S]*?([\$￥][\d,\.]+)/i, key: 'Revenue' },
                
                // Subscribers
                { re: /Subscribers\n([+-]?[\d,\.]+[KMB]?)/i, key: 'Subscribers', expand: true },
                { re: /([+-]?\d[\d,]*)\s*Subscribers/i, key: 'Subscribers' },
                { re: /订阅人数[\s\S]*?([+-]?\d[\d,]*)/i, key: 'Subscribers' },
                
                // Likes - 英文
                { re: /(\d[\d,]*)\s*Likes/i, key: 'Likes' },
                // 喜欢 - 中文
                { re: /(\d[\d,]*)\s*次顶/i, key: 'Likes' },
                
                // Comments - 英文
                { re: /(\d[\d,]*)\s*Comments/i, key: 'Comments' },
                // 评论 - 中文
                { re: /(\d[\d,]*)\s*条评论/i, key: 'Comments' },
                
                // Unique viewers - header card 格式
                { re: /Unique viewers\n([\d,\.]+[KMB]?)/i, key: 'Unique Viewers', expand: true },
                { re: /Unique viewers\s+([\d,\.]+[KMB]?)/i, key: 'Unique Viewers', expand: true },
                { re: /([\d,\.]+[KMB]?)\s+Unique viewers/i, key: 'Unique Viewers', expand: true },
                { re: /独立观看者[\s\S]*?([\d,\.]+[KMB万]?)/i, key: 'Unique Viewers', expand: true }
            ];
            
            patterns.forEach(p => {
                if (!data[p.key]) {
                    const m = text.match(p.re);
                    if (m && m[1]) {
                        let val = m[1].trim();
                        if (p.expand) val = expandAbbrev(val);
                        data[p.key] = val;
                    }
                }
            });
            
            // 30秒留存
            const retentionMatch = text.match(/(\d+)%\s*of viewers are still watching at around the 0:30 mark/i);
            if (retentionMatch) {
                data['Retention 30s'] = retentionMatch[1] + '%';
            }
            
            // 流量来源 (How viewers find this video) - 详细表格解析
            const trafficSection = document.querySelector('[aria-label*="Traffic source"]') || 
                                   Array.from(document.querySelectorAll('ytcp-chart-card')).find(c => c.innerText.includes('How viewers find this video'));
            if (trafficSection) {
                const trafficData = {};
                const rows = trafficSection.querySelectorAll('tr, [role="row"]');
                rows.forEach(row => {
                    const cells = row.querySelectorAll('td, [role="cell"]');
                    if (cells.length >= 2) {
                        const source = cells[0].innerText.trim();
                        const percent = cells[1].innerText.match(/[\d\.]+%/);
                        if (source && percent) {
                            trafficData[source] = percent[0];
                        }
                    }
                });
                if (Object.keys(trafficData).length > 0) {
                    data['Traffic Sources'] = trafficData;
                }
            }
            
            // 简化版流量来源（从文本提取）
            const sources = ['Browse features', 'Suggested videos', 'YouTube search', 'Channel pages', 'Notifications', 'Playlists'];
            sources.forEach(src => {
                const re = new RegExp(src + '\\s*(\\d+\\.?\\d*)%', 'i');
                const m = text.match(re);
                if (m) {
                    data['Traffic_' + src.replace(/\s+/g, '_')] = m[1] + '%';
                }
            });
            
            // 设备类型
            const devices = ['Computer', 'Mobile', 'TV', 'Tablet'];
            devices.forEach(dev => {
                const re = new RegExp(dev + '\\s*(\\d+\\.?\\d*)%', 'i');
                const m = text.match(re);
                if (m) {
                    data['Device_' + dev] = m[1] + '%';
                }
            });
            
            // 观众类型
            const viewerTypes = ['New viewers', 'Casual viewers', 'Regular viewers'];
            viewerTypes.forEach(vt => {
                const re = new RegExp(vt + '\\s*(\\d+\\.?\\d*)%', 'i');
                const m = text.match(re);
                if (m) {
                    data['Viewer_' + vt.replace(/\s+/g, '_')] = m[1] + '%';
                }
            });
            
            // Watch time from subscribers
            const subWatchMatch = text.match(/Not subscribed\s*(\d+\.?\d*)%/i);
            if (subWatchMatch) {
                data['Watch_Not_Subscribed'] = subWatchMatch[1] + '%';
            }
            const subscribedMatch = text.match(/Subscribed\s*(\d+\.?\d*)%/i);
            if (subscribedMatch) {
                data['Watch_Subscribed'] = subscribedMatch[1] + '%';
            }
            
            // Top geographies - 国家/地区分布
            const geoSection = Array.from(document.querySelectorAll('ytcp-chart-card')).find(c => c.innerText.includes('Top geographies'));
            if (geoSection) {
                const geoData = {};
                const lines = geoSection.innerText.split('\n');
                lines.forEach(line => {
                    const m = line.match(/([A-Za-z\s]+)\s+(\d+\.?\d*)%/);
                    if (m && !m[1].includes('Top') && !m[1].includes('See') && !m[1].includes('Views')) {
                        geoData[m[1].trim()] = m[2] + '%';
                    }
                });
                if (Object.keys(geoData).length > 0) {
                    data['Geographies'] = geoData;
                }
            }
            
            // Content suggesting this video - 推荐来源视频
            const suggestSection = Array.from(document.querySelectorAll('ytcp-chart-card')).find(c => c.innerText.includes('Content suggesting this video'));
            if (suggestSection) {
                const suggestions = [];
                const items = suggestSection.querySelectorAll('[role="listitem"], .suggested-item');
                items.forEach(item => {
                    const title = item.innerText.split('\n')[0];
                    if (title && title.length > 5 && !title.includes('Proportion')) {
                        suggestions.push(title.slice(0, 50));
                    }
                });
                if (suggestions.length > 0) {
                    data['Suggested_By'] = suggestions.slice(0, 5);
                }
            }
            
            // YouTube search terms - 搜索关键词
            const searchSection = Array.from(document.querySelectorAll('ytcp-chart-card')).find(c => c.innerText.includes('YouTube search terms'));
            if (searchSection) {
                const terms = [];
                const lines = searchSection.innerText.split('\n').filter(l => l.length > 2 && !l.includes('YouTube search') && !l.includes('See more') && !l.includes('Proportion'));
                terms.push(...lines.slice(0, 5));
                if (terms.length > 0) {
                    data['Search_Terms'] = terms;
                }
            }
            
            return data;
        }
    """)

async def get_visible_videos(page):
    """获取当前可见的所有视频"""
    return await page.evaluate(r"""
        () => {
            const videoLinks = document.querySelectorAll('a[href*="/video/"]');
            const videos = [];
            const seen = new Set();
            
            videoLinks.forEach(link => {
                const match = link.href.match(/\/video\/([^\/]+)/);
                if (match && !seen.has(match[1])) {
                    seen.add(match[1]);
                    const row = link.closest('tr, ytcp-video-row');
                    let title = 'Unknown';
                    if (row) {
                        const titleEl = row.querySelector('#video-title, .video-title-text, [id*="video-title"]');
                        if (titleEl) title = titleEl.innerText.trim();
                    }
                    videos.push({ id: match[1], title: title });
                }
            });
            return videos;
        }
    """)

def download_thumbnail(video_id: str, save_dir: str) -> str:
    """下载封面图到指定目录"""
    url = f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"
    save_path = os.path.join(save_dir, "thumbnail.jpg")
    
    if os.path.exists(save_path):
        return save_path
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(resp.content)
            return save_path
    except:
        pass
    return None

# ============================================================
# 单视频抓取
# ============================================================

async def scrape_single_video(page, video_id: str, video_count: int):
    """抓取单个视频的完整数据（直接保存到频道文件夹）"""
    global current_channel_dir
    
    # 检查是否已在记录中（历史抓取过）
    scraped_record = load_scraped_record()
    if video_id in scraped_record:
        print(f"   ⏩ 历史已抓取，跳过")
        return None
    
    # 使用当前频道目录
    if not current_channel_dir:
        print(f"   ❌ 频道目录未设置，无法保存")
        return None
    
    video_dir = os.path.join(current_channel_dir, f"video_{video_id}")
    os.makedirs(video_dir, exist_ok=True)
    
    json_path = os.path.join(video_dir, "info.json")
    if os.path.exists(json_path):
        print(f"   ⏩ 数据已存在，跳过抓取")
        return None
    
    video_data = {
        "id": video_id,
        "scraped_at": datetime.now().isoformat(),
        "metrics": {}
    }
    
    # 1. 下载封面
    thumb_path = download_thumbnail(video_id, video_dir)
    if thumb_path:
        video_data["thumbnail"] = thumb_path
    
    # 2. 抓取标题简介
    print("   📝 抓取标题简介...")
    title_desc = await extract_title_description(page)
    video_data.update(title_desc)
    
    if title_desc.get('title'):
        print(f"      标题: {title_desc['title'][:50]}...")
    
    # 3. 进入 Analytics
    print("   📊 进入 Analytics...")
    analytics_selectors = [
        "a[href*='/analytics']",
        "ytcp-ve:has-text('Analytics')",
        "[role='tab']:has-text('Analytics')"
    ]
    
    clicked = False
    for sel in analytics_selectors:
        try:
            tab = page.locator(sel).first
            if await tab.count() > 0:
                await human_click(page, tab, "Analytics")
                clicked = True
                break
        except:
            continue
    
    if not clicked:
        print("   ⚠️ 找不到 Analytics")
        return video_data
    
    await random_delay(4, 6)
    
    # 4. Overview
    print("   📊 Overview...")
    overview_metrics = await extract_metrics(page)
    video_data["metrics"].update(overview_metrics)
    # 保存 Overview 的精确 Views（来自 "has gotten X views" 句子）
    precise_views = overview_metrics.get('Views')
    await take_scroll_screenshot(page, os.path.join(video_dir, "overview.png"))
    print(f"      📸 overview.png")
    
    # 5. Reach (语言无关: 第2个tab，或匹配多语言文本)
    print("   📊 Reach...")
    reach_selectors = [
        "tp-yt-paper-tab:nth-of-type(2)",  # 语言无关，第2个tab
        "tp-yt-paper-tab:has-text('Reach')",
        "tp-yt-paper-tab:has-text('覆盖面')",  # 中文界面
        "tp-yt-paper-tab:has-text('触及')",
        "[role='tab']:nth-of-type(2)"
    ]
    for sel in reach_selectors:
        try:
            reach_tab = page.locator(sel).first
            if await reach_tab.count() > 0:
                await human_click(page, reach_tab, "Reach")
                await random_delay(3, 5)
                video_data["metrics"].update(await extract_metrics(page))
                await take_scroll_screenshot(page, os.path.join(video_dir, "reach.png"))
                print(f"      📸 reach.png")
                break
        except:
            continue
    
    # 6. Engagement (语言无关: 第3个tab)
    print("   📊 Engagement...")
    eng_selectors = [
        "tp-yt-paper-tab:nth-of-type(3)",  # 语言无关，第3个tab
        "tp-yt-paper-tab:has-text('Engagement')",
        "tp-yt-paper-tab:has-text('互动')",  # 中文界面
        "tp-yt-paper-tab:has-text('参与')",
        "[role='tab']:nth-of-type(3)"
    ]
    for sel in eng_selectors:
        try:
            eng_tab = page.locator(sel).first
            if await eng_tab.count() > 0:
                await human_click(page, eng_tab, "Engagement")
                await random_delay(3, 5)
                video_data["metrics"].update(await extract_metrics(page))
                await take_scroll_screenshot(page, os.path.join(video_dir, "engagement.png"))
                print(f"      📸 engagement.png")
                break
        except:
            continue
    
    # 7. Audience (语言无关: 第4个tab)
    print("   📊 Audience...")
    aud_selectors = [
        "tp-yt-paper-tab:nth-of-type(4)",  # 语言无关，第4个tab
        "tp-yt-paper-tab:has-text('Audience')",
        "tp-yt-paper-tab:has-text('观众')",  # 中文界面
        "tp-yt-paper-tab:has-text('觀眾')",  # 繁体中文
        "[role='tab']:nth-of-type(4)"
    ]
    for sel in aud_selectors:
        try:
            aud_tab = page.locator(sel).first
            if await aud_tab.count() > 0:
                await human_click(page, aud_tab, "Audience")
                await random_delay(3, 5)
                video_data["metrics"].update(await extract_metrics(page))
                await take_scroll_screenshot(page, os.path.join(video_dir, "audience.png"))
                print(f"      📸 audience.png")
                break
        except:
            continue
    
    # 8. 恢复 Overview 的精确 Views（防止被后续 tab 的近似值 3.9K→3900 覆盖）
    if precise_views:
        video_data['metrics']['Views'] = precise_views
    
    # 9. 保存到视频文件夹
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(video_data, f, indent=2, ensure_ascii=False)
    
    # 更新已抓取记录
    scraped_record.add(video_id)
    save_scraped_record(scraped_record)
    
    m = video_data['metrics']
    print(f"   📋 数据汇总:")
    print(f"      Views: {m.get('Views', 'N/A')}, CTR: {m.get('CTR', 'N/A')}%")
    print(f"      Avg Duration: {m.get('Avg Duration', 'N/A')}, Retention 30s: {m.get('Retention 30s', 'N/A')}")
    print(f"   💾 已保存: video_{video_id}/info.json")
    
    return video_data

# ============================================================
# 汇总与归档
# ============================================================

async def generate_summary():
    """生成汇总报告（数据已直接保存到频道文件夹）"""
    global current_channel_dir, current_channel_name
    
    print("\n📊 生成汇总报告...")
    import csv
    
    if not current_channel_dir or not os.path.exists(current_channel_dir):
        print("   ⚠️ 频道目录不存在，无法生成汇总")
        return
    
    video_dirs = [d for d in os.listdir(current_channel_dir) if d.startswith('video_') and os.path.isdir(os.path.join(current_channel_dir, d))]
    
    if not video_dirs:
        print("   ⚠️ 没有找到视频数据文件夹")
        return
    
    videos = []
    for vd in video_dirs:
        json_path = os.path.join(current_channel_dir, vd, "info.json")
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    data['folder'] = vd
                    videos.append(data)
            except Exception as e:
                print(f"   ⚠️ 读取失败: {vd} - {e}")
    
    def get_views(v):
        try:
            return int(v.get('metrics', {}).get('Views', '0').replace(',', ''))
        except:
            return 0
    videos.sort(key=get_views, reverse=True)
    
    # 生成 CSV
    csv_path = os.path.join(current_channel_dir, "summary.csv")
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['video_id', 'title', 'views', 'watch_hours', 'avg_duration', 'ctr', 'retention_30s', 'revenue'])
        for v in videos:
            m = v.get('metrics', {})
            writer.writerow([
                v.get('id', ''),
                v.get('title', '')[:60],
                m.get('Views', ''),
                m.get('Watch Hours', ''),
                m.get('Avg Duration', ''),
                m.get('CTR', ''),
                m.get('Retention 30s', ''),
                m.get('Revenue', '')
            ])
    print(f"   ✅ CSV: {csv_path}")
    
    # 生成 Markdown
    md_path = os.path.join(current_channel_dir, "summary.md")
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# {current_channel_name or '频道'}诊断汇总\n\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"共 {len(videos)} 个视频\n\n")
        
        f.write("| # | 视频ID | 标题 | Views | CTR | Avg Duration | Retention 30s | Revenue |\n")
        f.write("|---|--------|------|-------|-----|--------------|---------------|--------|\n")
        
        for i, v in enumerate(videos, 1):
            m = v.get('metrics', {})
            title_short = v.get('title', '')[:25] + '...' if len(v.get('title', '')) > 25 else v.get('title', '')
            f.write(f"| {i} | {v.get('id', '')} | {title_short} | {m.get('Views', '-')} | {m.get('CTR', '-')}% | {m.get('Avg Duration', '-')} | {m.get('Retention 30s', '-')} | {m.get('Revenue', '-')} |\n")
    
    print(f"   ✅ Markdown: {md_path}")
    print(f"\n   📂 所有数据已保存到: {current_channel_dir}")

# ============================================================
# 频道诊断主流程
# ============================================================

async def go_back_to_content(page):
    """返回 Content 页面（语言无关）"""
    selectors = [
        "a[href*='/videos/upload']",  # 语言无关
        "a[href*='/videos']",  # 语言无关
        "ytcp-ve:has-text('Content')",
        "ytcp-ve:has-text('内容')",  # 简体中文
        "ytcp-ve:has-text('內容')",  # 繁体中文
        "[id*='menu-item']:has-text('Content')",
        "[id*='menu-item']:has-text('内容')",
        "[id*='menu-item']:has-text('內容')"
    ]
    
    for sel in selectors:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0:
                await human_click(page, elem, "Content 菜单")
                await random_delay(3, 5)
                return True
        except:
            continue
    
    # 从当前 URL 提取 channel ID，然后导航到正确的 Content 页面
    url = page.url
    match = re.search(r'/channel/([^/]+)', url)
    if match:
        channel_id = match.group(1)
        await page.goto(f"https://studio.youtube.com/channel/{channel_id}/videos/upload")
        await random_delay(3, 5)
        return True
    
    # 如果无法获取 channel ID，尝试导航到 Studio 主页
    await page.goto("https://studio.youtube.com")
    await random_delay(3, 5)
    return True

async def scroll_down_a_bit(page):
    """滚动一点点"""
    await page.mouse.wheel(0, 500)
    await asyncio.sleep(1)

async def diagnose_channel(page):
    """诊断整个频道"""
    global processed_videos, current_channel_dir, current_channel_name, current_container_info
    
    print("\n" + "="*60)
    print("🎬 开始全量诊断")
    print("="*60)
    
    # 首先抓取频道名称
    print("\n📍 正在获取频道名称...")
    channel_name = await extract_channel_name(page)
    
    if not channel_name:
        print("   ❌ 无法获取频道名称，诊断中止")
        print("   提示: 请确保当前页面是 YouTube Studio")
        return
    
    print(f"   ✅ 频道名称: {channel_name}")
    current_channel_name = channel_name
    
    # 从 URL 提取 channel ID
    channel_id = None
    url = page.url
    match = re.search(r'/channel/([^/]+)', url)
    if match:
        channel_id = match.group(1)
        print(f"   ✅ Channel ID: {channel_id}")
    
    # 创建频道文件夹（按分组分类）
    safe_name = re.sub(r'[\\/:*?"<>|]', '_', channel_name)
    
    # 获取分组标签作为父文件夹
    tag = "未分组"
    if current_container_info:
        tag = current_container_info.get('tag', '未分组')
    safe_tag = re.sub(r'[\\/:*?"<>|]', '_', tag)
    
    # 路径结构: 03_原始数据/{分组}/{频道名}/
    group_dir = os.path.join(OUTPUT_DIR, safe_tag)
    os.makedirs(group_dir, exist_ok=True)
    current_channel_dir = os.path.join(group_dir, safe_name)
    os.makedirs(current_channel_dir, exist_ok=True)
    print(f"   📂 数据将保存到: {current_channel_dir}")
    
    # 更新频道映射 (先不更新时间，防止中断后被误判为已完成)
    if current_container_info:
        update_channel_mapping(
            container_code=current_container_info.get('container_code'),
            serial_number=current_container_info.get('serial_number'),
            tag=current_container_info.get('tag'),
            channel_name=channel_name,
            channel_id=channel_id,
            update_time=False  # <--- 这里改为 False
        )
    
    video_count = 0
    max_no_new_scrolls = 3
    no_new_count = 0
    
    while no_new_count < max_no_new_scrolls:
        videos = await get_visible_videos(page)
        unprocessed = [v for v in videos if v['id'] not in processed_videos]
        
        print(f"\n   📊 可见 {len(videos)} 个视频, 未处理 {len(unprocessed)} 个")
        
        if not unprocessed:
            no_new_count += 1
            print(f"   ⚠️ 没有新视频，滚动查看更多 ({no_new_count}/{max_no_new_scrolls})")
            await scroll_down_a_bit(page)
            continue
        
        no_new_count = 0
        
        video = unprocessed[0]
        video_id = video['id']
        video_title = video['title']
        video_count += 1
        
        print(f"\n{'='*60}")
        print(f"📹 [{video_count}] {video_title[:45]}...")
        print(f"    ID: {video_id}")
        print(f"{'='*60}")
        
        video_link = page.locator(f"a[href*='/video/{video_id}']").first
        
        if await video_link.count() > 0:
            try:
                await video_link.scroll_into_view_if_needed()
                await asyncio.sleep(0.5)
            except:
                pass
            
            success = await human_click(page, video_link, "视频链接")
            
            if success:
                await random_delay(4, 6)
                processed_videos.add(video_id)
                await scrape_single_video(page, video_id, video_count)
                
                print("   ↩️ 返回 Content 页面...")
                await go_back_to_content(page)
            else:
                print(f"   ❌ 点击失败，跳过")
                processed_videos.add(video_id)
        else:
            print(f"   ❌ 找不到视频链接，跳过")
            processed_videos.add(video_id)
        
        await random_delay(2, 4)
    
    print("\n" + "="*60)
    print("🎉 全量诊断完成!")
    print(f"   总共处理: {len(processed_videos)} 个视频")
    print("="*60)
    
    await generate_summary()
    
    # 任务全部完成，更新时间戳
    if current_container_info:
        update_channel_mapping(
            container_code=current_container_info.get('container_code'),
            serial_number=current_container_info.get('serial_number'),
            tag=current_container_info.get('tag'),
            channel_name=current_channel_name,
            channel_id=channel_id,
            update_time=True  # <--- 完成后打卡
        )

# ============================================================
# 批量模式
# ============================================================

def load_groups():
    """加载分组数据。优先读本地快照，缺失时从浏览器 API 实时构建。"""
    groups = {}

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            raw_groups = json.load(f).get("groups", {})
        for tag, containers in raw_groups.items():
            normalized_tag = normalize_hubstudio_tag(tag)
            groups.setdefault(normalized_tag, []).extend(containers)
        return groups

    try:
        envs = list_browser_envs(CONFIG_PATH)
    except Exception as e:
        print(f"❌ 读取浏览器环境失败: {e}")
        print(f"   本地快照也不存在: {DATA_FILE}")
        return None

    for env in envs:
        tag = normalize_hubstudio_tag(env.get("tag") or "未分组")
        groups.setdefault(tag, []).append({
            "container_code": str(env.get("containerCode")),
            "serial_number": env.get("serialNumber"),
            "name": env.get("name", ""),
            "remark": env.get("remark", ""),
        })
    return groups

async def start_browser(container_code):
    """API 启动浏览器"""
    try:
        return start_browser_debug_port(container_code, CONFIG_PATH)
    except Exception:
        pass
    return None

async def stop_browser(container_code):
    """API 关闭浏览器"""
    try:
        stop_browser_container(container_code, CONFIG_PATH)
    except Exception:
        pass

async def process_single_container(container_code: str, container_info: dict = None):
    """处理单个容器"""
    global processed_videos, current_container_info, current_channel_dir, current_channel_name
    processed_videos = set()
    current_channel_dir = None
    current_channel_name = None
    
    # 设置当前容器信息
    current_container_info = container_info or {"container_code": container_code}
    
    serial = current_container_info.get('serial_number', '?')
    tag = current_container_info.get('tag', '未知')
    
    print(f"\n{'='*60}")
    print(f"   📊 批量频道诊断")
    print(f"   Container: {container_code} (序号 {serial}, 标签: {tag})")
    print(f"{'='*60}")
    
    # === 智能跳过判断 (Cool-down Check) ===
    # 加载映射文件检查上次抓取时间
    try:
        if os.path.exists(CHANNEL_MAPPING_FILE):
            with open(CHANNEL_MAPPING_FILE, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
                
            channel_data = mapping.get('channels', {}).get(str(container_code))
            if channel_data and 'scraped_at' in channel_data:
                last_time_str = channel_data['scraped_at']
                try:
                    last_time = datetime.fromisoformat(last_time_str)
                    time_diff = datetime.now() - last_time
                    hours_diff = time_diff.total_seconds() / 3600
                    
                    # 设定冷却时间为 20 小时 (每天抓一次)
                    COOL_DOWN_HOURS = 20
                    
                    if hours_diff < COOL_DOWN_HOURS:
                        print(f"\n   ⏳ 数据尚新 (上次抓取: {hours_diff:.1f} 小时前)")
                        print(f"   ⏩ 跳过此容器 (冷却期 {COOL_DOWN_HOURS} 小时)")
                        return
                    else:
                        print(f"\n   ⏰ 数据已过期 ({hours_diff:.1f} 小时 > {COOL_DOWN_HOURS} 小时)，准备重新抓取")
                except ValueError:
                    pass # 时间格式解析失败，继续抓取
    except Exception as e:
        print(f"   ⚠️ 检查历史记录失败: {e}")
    # ==========================================
    
    print("\n🔌 连接浏览器...")
    port = await start_browser(container_code)
    if not port:
        print("   ❌ 启动失败: 未拿到调试端口")
        return

    print(f"   ✅ 端口: {port}")
    
    print("   ⏳ 等待浏览器...")
    await asyncio.sleep(5)
    
    try:
        async with async_playwright() as p:
            # 增加30秒连接超时设置
            try:
                browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{port}", timeout=30000)
            except Exception as e:
                print(f"   ❌ 连接浏览器CDP失败: {e}")
                return

            context = browser.contexts[0]
            page = context.pages[0] if context.pages else await context.new_page()
            
            print(f"\n   📍 当前页面: {page.url}")
            
            # 自动进入 Content 页面
            if "/videos" not in page.url:
                print("   🔄 自动进入 Content 页面...")
                
                # 先导航到 YouTube Studio 主页
                # 增加 goto 的超时容错，如果是网络中断，这里会抛出异常被外层捕获
                if "studio.youtube.com" not in page.url:
                    await page.goto("https://studio.youtube.com", timeout=60000)
                    await asyncio.sleep(5)
                
                # 尝试从当前 URL 提取 channel ID
                url = page.url
                match = re.search(r'/channel/([^/]+)', url)
                if match:
                    channel_id = match.group(1)
                    await page.goto(f"https://studio.youtube.com/channel/{channel_id}/videos/upload", timeout=60000)
                    await asyncio.sleep(5)
                else:
                    # 如果无法获取 channel ID，尝试点击 Content 菜单
                    content_menu = page.locator("a[href*='/videos'], ytcp-ve:has-text('Content')").first
                    if await content_menu.count() > 0:
                        await content_menu.click()
                        await asyncio.sleep(3)
            
            if "/videos" not in page.url:
                print("   ⚠️ 无法进入 Content 页面，跳过此容器")
                return
            
            await diagnose_channel(page)
            
            print("\n🎉 完成！")

    except Exception as e:
        print(f"\n   ❌ 发生严重错误 (可能是网络中断或浏览器崩溃): {e}")
        print("   ⏩ 跳过当前容器，继续执行下一个...")
    
    finally:
        # 无论成功失败，尝试关闭浏览器 (通过 API)
        await stop_browser(container_code)

async def main():
    import sys
    if async_playwright is None:
        print("❌ 缺少依赖: playwright")
        print("   请先执行: py -3 -m pip install playwright")
        print("   然后执行: py -3 -m playwright install chromium")
        return
    
    if len(sys.argv) >= 2:
        # 单容器模式
        await process_single_container(sys.argv[1])
    else:
        # 批量模式
        print("🌙 批量频道诊断脚本 🌙")
        
        groups = load_groups()
        if not groups:
            return
        
        group_names = list(groups.keys())
        print("\n📦 可用分组:")
        for i, name in enumerate(group_names):
            print(f"{i+1}. {name} ({len(groups[name])} 个容器)")
        
        choice = input("\n请选择要抓取的分组序号 (可用逗号分隔多个，如 1,3,5，或输入 'all'): ")
        
        target_groups = []
        if choice.lower() == 'all':
            target_groups = group_names
        elif ',' in choice:
            # 多个序号
            try:
                indices = [int(x.strip()) - 1 for x in choice.split(',')]
                for idx in indices:
                    if 0 <= idx < len(group_names):
                        target_groups.append(group_names[idx])
                    else:
                        print(f"⚠️ 序号 {idx+1} 无效，跳过")
                if not target_groups:
                    print("❌ 没有有效的分组")
                    return
            except:
                print("❌ 输入格式错误")
                return
        else:
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(group_names):
                    target_groups = [group_names[idx]]
                else:
                    print("❌ 无效序号")
                    return
            except:
                print("❌ 输入错误")
                return
        
        total_containers = sum(len(groups[g]) for g in target_groups)
        print(f"\n🗓️  即将处理 {len(target_groups)} 个分组，共 {total_containers} 个容器")
        print(f"⏳ 预计耗时: {total_containers * 10} 分钟")
        confirm = input("确认开始挂机吗? (y/n): ")
        if confirm.lower() != 'y':
            return
        
        for gname in target_groups:
            # 检查跳过标签
            if gname in SKIP_TAGS or not gname:
                print(f"\n⚠️ 跳过分组: {gname} (在跳过列表中)")
                continue

            containers = groups[gname]
            print(f"\n🚀 开始处理分组: {gname}")
            for c in containers:
                # 传递完整的容器信息
                container_info = {
                    'container_code': c['container_code'],
                    'serial_number': c.get('serial_number'),
                    'tag': gname,
                    'name': c.get('name', '')
                }
                
                # 双重保险：检查单个容器的 tag 属性（如果是单个选择可能会用到）
                tag_check = container_info.get('tag')
                if tag_check in SKIP_TAGS:
                     print(f"   ⏩ 跳过容器 {container_info['serial_number']} (标签: {tag_check})")
                     continue

                await process_single_container(str(c['container_code']), container_info)
                await random_delay(5, 10)
        
        print("\n🎉🎉🎉 所有任务完成！")

if __name__ == "__main__":
    asyncio.run(main())
