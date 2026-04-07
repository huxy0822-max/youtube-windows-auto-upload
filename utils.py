#!/usr/bin/env python3
"""
上传侧共享工具。

这个文件主要给“上传脚本”一侧使用，职责包括：
- 读取 `config/upload_config.json` 和 `config/channel_mapping.json`
- 解析 `metadata_channels.md`
- 统一拿标题、简介、封面、频道映射
- 提供浏览器本地 API 的统一入口

它不是渲染器本体，而是渲染完成后的“素材/频道信息中台”。
"""

import os
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from browser_api import list_browser_envs, start_browser_debug_port
from path_helpers import resolve_config_file
from prompt_studio import normalize_tag_key

# 兼容旧脚本里直接引用该常量的情况；新代码请走 browser_api.py。
HUBSTUDIO_API = "http://127.0.0.1:6873"
SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = Path(os.environ.get("UPLOAD_CONFIG_PATH", str(resolve_config_file(SCRIPT_DIR, "upload_config.json"))))
CHANNEL_MAPPING_PATH = Path(os.environ.get("CHANNEL_MAPPING_PATH", str(resolve_config_file(SCRIPT_DIR, "channel_mapping.json"))))

# get_channel_info 的内存缓存，key=(tag, serial)，value=(timestamp, result)
_CHANNEL_INFO_CACHE: Dict[tuple, tuple] = {}
_CHANNEL_INFO_CACHE_TTL = 30.0  # 缓存有效期（秒）

def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {"INFO": "ℹ️", "OK": "✅", "ERR": "❌", "WARN": "⚠️", "ACT": "🖱️", "WAIT": "⏳"}
    print(f"[{timestamp}] {icons.get(level, '•')} {msg}")

# ============ Hubstudio API ============

def get_env_list():
    """获取所有环境列表（HubStudio / BitBrowser 统一格式）"""
    try:
        return list_browser_envs(CONFIG_PATH)
    except Exception as e:
        log(f"获取环境列表失败: {e}", "ERR")
        return []

def get_port_by_env(serial_number: int) -> int:
    """根据环境序号获取调试端口（如果浏览器未启动会自动启动）"""
    envs = get_env_list()
    
    container_code = None
    for env in envs:
        if env.get("serialNumber") == serial_number:
            container_code = env.get("containerCode")
            break
    
    if not container_code:
        log(f"未找到序号 {serial_number} 的环境", "ERR")
        return None
    
    try:
        port = start_browser_debug_port(container_code, CONFIG_PATH)
        log(f"环境 {serial_number} 的调试端口: {port}", "OK")
        return port
    except Exception as e:
        log(f"启动浏览器失败: {e}", "ERR")
        return None

# ============ 配置读取 ============

def load_config() -> dict:
    """加载 upload_config.json"""
    if not CONFIG_PATH.exists():
        log(f"配置文件不存在: {CONFIG_PATH}", "ERR")
        return {}
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_all_tags() -> List[str]:
    """获取所有可用标签"""
    config = load_config()
    return list(config.get("tag_to_project", {}).keys())

def _resolve_tag_config(tag_to_project: Dict, tag: str):
    direct = tag_to_project.get(tag)
    if direct:
        return tag, direct
    wanted = normalize_tag_key(tag)
    if not wanted:
        return None, None
    for raw_tag, raw_value in tag_to_project.items():
        if normalize_tag_key(raw_tag) == wanted:
            return raw_tag, raw_value
    return None, None

def get_tag_info(tag: str) -> dict:
    """获取指定标签的配置信息"""
    config = load_config()
    matched_tag, tag_config = _resolve_tag_config(config.get("tag_to_project", {}), tag)

    if not tag_config:
        log(f"未找到标签 '{tag}' 的配置信息，请检查 upload_config.json 中的 tag_to_project", "WARN")
        return None
    
    ypp_serials = tag_config.get("ypp_serials", [])
    non_ypp_serials = tag_config.get("non_ypp_serials", [])
    all_serials = ypp_serials + non_ypp_serials
    
    return {
        "tag": matched_tag or tag,
        "project_name": tag_config.get("project_name"),
        "video_keyword": tag_config.get("video_keyword"),
        "ypp_serials": ypp_serials,
        "non_ypp_serials": non_ypp_serials,
        "all_serials": sorted(all_serials),
    }

def parse_metadata(project_path: Path) -> List[dict]:
    """
    解析项目的 metadata_channels.md 文件
    
    支持三种格式:
    
    格式1 (新版套数格式 - 2026-02-03):
    ---
    ## 📻 頻道 1：迷霧節拍 (Container: 12)
    ### 套1
    💨 Slow Thoughts, Clear Mind｜...
    ### 套2
    💨 2026 迷霧嘻哈精選｜...
    ### 📝 簡介
    ```markdown
    ...
    ```
    ---
    
    格式2 (A/B/C 格式):
    ---
    ## 📻 頻道 1：爵士·獨白 (container: 116) 【YPP ✅】
    ### 🅰️ 標題 A（功能派）
    ```
    🍷 2026深夜爵士大提琴 BGM｜...
    ```
    ---
    
    格式3 (旧版):
    ---
    ## 📻 頻道 1：靜謐拾光
    ### 標題庫 (Titles)
    **首選複刻版**
    🎻 ...
    ---
    """
    metadata_file = project_path / "metadata_channels.md"
    
    if not metadata_file.exists():
        return []
    
    with open(metadata_file, "r", encoding="utf-8") as f:
        content = f.read()
    
    channels = []
    
    # 检测格式类型
    is_set_format = '### 套1' in content or '### 套 1' in content  # 新版套数格式
    is_abc_format = '🅰️' in content or '### 🅰️' in content  # A/B/C 格式
    
    if is_set_format:
        # 新版套数格式: 按频道分割
        channel_splits = re.split(r'(?=^#+ 📻 頻道)', content, flags=re.MULTILINE)
        
        for block in channel_splits:
            if not block.strip() or '📻 頻道' not in block:
                continue
            
            # 提取频道信息
            header_match = re.search(r'頻道 (\d+)[：:]\s*([^\n(]+)', block)
            if not header_match:
                continue
            
            channel_num = int(header_match.group(1))
            channel_name = header_match.group(2).strip()
            
            # 提取 container ID
            container_match = re.search(r'Container[：:]\s*(\d+)', block, re.IGNORECASE)
            container_id = container_match.group(1) if container_match else ""
            
            # 提取所有套数的标题和简介
            # 格式: ### 套1\n💨 标题内容... \n**簡介**\n```markdown\n...\n```
            titles = []
            title_by_set = {}  # {套数: 标题}
            description_by_set = {}  # {套数: 简介}
            
            # 按 "### 套" 分块
            set_blocks = re.split(r'(?=### 套\s*\d+)', block)
            for set_block in set_blocks:
                set_match = re.search(r'### 套\s*(\d+)', set_block)
                if not set_match:
                    continue
                set_num = int(set_match.group(1))
                
                # 在该块中查找标题（emoji 开头的行）
                lines = set_block.split('\n')
                for line in lines[1:]:  # 跳过 ### 套N 那行
                    line = line.strip()
                    if line and len(line) > 10:
                        # 跳过简介块标记
                        if line.startswith('### ') or line.startswith('```') or line.startswith('**簡介'):
                            break
                        # 这是标题行
                        title_by_set[set_num] = line
                        break
                
                # 在该块中查找简介（```markdown ... ```）
                desc_match = re.search(r'```markdown\n(.+?)```', set_block, re.DOTALL)
                if desc_match:
                    description_by_set[set_num] = desc_match.group(1).strip()
            
            # 按套数顺序排列标题
            for set_num in sorted(title_by_set.keys()):
                titles.append(title_by_set[set_num])
            
            # 默认描述：取套1的简介（向后兼容）
            description = description_by_set.get(1, "")
            
            channels.append({
                "number": channel_num,
                "name": channel_name,
                "container_id": container_id,
                "titles": titles,
                "title_by_set": title_by_set,  # 按套数索引的标题
                "description": description,  # 默认简介（套1）
                "description_by_set": description_by_set  # 新增：按套数索引的简介
            })
    
    elif is_abc_format:
        # A/B/C 格式: 按频道分割
        channel_splits = re.split(r'(?=^#+ 📻 頻道)', content, flags=re.MULTILINE)
        
        for block in channel_splits:
            if not block.strip() or '📻 頻道' not in block:
                continue
            
            # 提取频道信息
            header_match = re.search(r'頻道 (\d+)[：:]\s*([^\n(]+)', block)
            if not header_match:
                continue
            
            channel_num = int(header_match.group(1))
            channel_name = header_match.group(2).strip()
            
            # 提取 container ID
            container_match = re.search(r'container:\s*(\d+)', block)
            container_id = container_match.group(1) if container_match else ""
            
            titles = []
            
            # 提取 A/B/C 标题 (在 ``` 代码块中)
            title_blocks = re.findall(r'### 🅰️.*?```\n(.+?)\n```|### 🅱️.*?```\n(.+?)\n```|### 🅲️.*?```\n(.+?)\n```', block, re.DOTALL)
            for match in title_blocks:
                for t in match:
                    if t and t.strip():
                        titles.append(t.strip())
            
            # 备用方法: 直接匹配代码块内的 emoji 标题
            if not titles:
                code_blocks = re.findall(r'```\n([^`]+?)\n```', block)
                for cb in code_blocks:
                    if cb.strip() and len(cb.strip()) > 10:
                        # 排除提示词 (通常很长且包含英文)
                        if not any(word in cb.lower() for word in ['cozy', 'warm', 'lighting', 'photography']):
                            titles.append(cb.strip())
            
            # 提取描述
            desc_match = re.search(r'### 📝 簡介\n+```markdown\n(.+?)```', block, re.DOTALL)
            description = desc_match.group(1).strip() if desc_match else ""
            
            channels.append({
                "number": channel_num,
                "name": channel_name,
                "container_id": container_id,
                "titles": titles,
                "description": description
            })
    else:
        # 旧版格式
        channel_pattern = r'## 📻 頻道 \d+[：:].*?(?=## 📻 頻道|\Z)'
        channel_blocks = re.findall(channel_pattern, content, re.DOTALL)
        
        for block in channel_blocks:
            title_section_match = re.search(r'### 標題庫.*?(?=### 簡介|$)', block, re.DOTALL)
            title_section = title_section_match.group(0) if title_section_match else ""
            
            titles = []
            
            # 提取反引号包裹的标题
            backtick_titles = re.findall(r'`([^`]+)`', title_section)
            for t in backtick_titles:
                t = t.strip()
                if t and len(t) > 10:
                    titles.append(t)
            
            # 提取 emoji 开头的行
            if not titles:
                emoji_pattern = r'^[\U0001F300-\U0001F9FF🎻🎞🌙💫☀📖🎵✨🌧📼📚🌆💔🎹🎸🎷🎺🎼🎧☕🌿🌸🌺🍃🍷🚗💭🌿🏙️].*$'
                emoji_titles = re.findall(emoji_pattern, title_section, re.MULTILINE)
                for t in emoji_titles:
                    t = t.strip()
                    if t and len(t) > 10:
                        titles.append(t)
            
            titles = list(dict.fromkeys(titles))
            
            desc_match = re.search(r'```markdown\s*([\s\S]*?)```', block)
            description = desc_match.group(1).strip() if desc_match else ""
            
            if titles or description:
                channels.append({
                    "titles": titles,
                    "description": description
                })
    
    return channels

def get_thumbnails(project_path: Path, channel_name: str = None) -> List[Path]:
    """
    获取项目的封面图
    
    如果指定 channel_name，从 images/organized/频道名/ 读取
    否则从 images/text/ 读取
    """
    if channel_name:
        # 新版: 从 organized 目录读取
        organized_dir = project_path / "images" / "organized"
        if organized_dir.exists():
            # 查找匹配的频道目录
            for d in organized_dir.iterdir():
                if d.is_dir() and channel_name in d.name:
                    # 按 A.png, B.png, C.png 顺序返回
                    thumbnails = []
                    for letter in ['A', 'B', 'C']:
                        for ext in ['.png', '.jpg', '.jpeg', '.webp']:
                            thumb_path = d / f"{letter}{ext}"
                            if thumb_path.exists():
                                thumbnails.append(thumb_path)
                                break
                    if thumbnails:
                        return thumbnails
    
    # 旧版: 从 text 目录读取
    thumbnails_dir = project_path / "images" / "text"
    if not thumbnails_dir.exists():
        thumbnails_dir = project_path / "images"
    
    if not thumbnails_dir.exists():
        return []
    
    thumbnails = sorted(thumbnails_dir.glob("*.png"))
    thumbnails.extend(sorted(thumbnails_dir.glob("*.jpg")))
    thumbnails.extend(sorted(thumbnails_dir.glob("*.jpeg")))
    return thumbnails[:3]  # 最多返回3张


def get_thumbnail_by_container(project_path: Path, container: int, set_num: int = None) -> Optional[Path]:
    """
    根据 container ID 和套数获取封面图
    
    支持新格式: {container}_{套数}.png (如 12_01.png)
    
    参数:
        project_path: 项目路径
        container: Container ID (如 12, 18, 24)
        set_num: 套数 (如 1, 2, 3)，如果不指定则返回该 container 的第一个可用封面
    
    返回: 封面图路径，如果未找到返回 None
    """
    images_dir = project_path / "images"
    if not images_dir.exists():
        return None
    
    # 查找匹配的封面
    # 格式: {container}_{套数}.png
    for ext in ['.png', '.jpg', '.jpeg', '.webp']:
        if set_num:
            # 指定了套数，精确匹配
            filename = f"{container}_{set_num:02d}{ext}"
            thumb_path = images_dir / filename
            if thumb_path.exists():
                return thumb_path
        else:
            # 未指定套数，找该 container 的第一个可用封面
            pattern = f"{container}_*{ext}"
            matches = sorted(images_dir.glob(pattern))
            # 排除 used 目录中的
            matches = [m for m in matches if 'used' not in str(m)]
            if matches:
                return matches[0]
    
    return None


def get_next_thumbnail_set(project_path: Path, container: int) -> Optional[int]:
    """
    获取指定 container 的下一个可用套数
    
    扫描 images 目录，找到最小的未使用套数
    """
    images_dir = project_path / "images"
    if not images_dir.exists():
        return None
    
    # 查找所有该 container 的封面
    available_sets = set()
    for ext in ['png', 'jpg', 'jpeg', 'webp']:
        for f in images_dir.glob(f"{container}_*.{ext}"):
            # 排除 used 目录
            if 'used' in str(f):
                continue
            # 提取套数
            match = re.match(rf'{container}_(\d+)', f.stem)
            if match:
                available_sets.add(int(match.group(1)))
    
    if not available_sets:
        return None
    
    return min(available_sets)


def mark_thumbnail_used(project_path: Path, container: int, set_num: int) -> bool:
    """
    标记封面为已使用（移动到 images/used/ 目录）
    
    返回: 是否成功
    """
    images_dir = project_path / "images"
    used_dir = images_dir / "used"
    
    # 查找封面文件
    thumb_path = get_thumbnail_by_container(project_path, container, set_num)
    if not thumb_path:
        log(f"未找到封面: {container}_{set_num:02d}", "WARN")
        return False
    
    # 创建 used 目录
    used_dir.mkdir(exist_ok=True)
    
    # 移动文件
    dest_path = used_dir / thumb_path.name
    try:
        import shutil
        shutil.move(str(thumb_path), str(dest_path))
        log(f"封面已标记为已使用: {thumb_path.name} -> used/", "OK")
        return True
    except Exception as e:
        log(f"移动封面失败: {e}", "ERR")
        return False


def get_inventory_status(project_path: Path, container_ids: List[int]) -> Dict:
    """
    统计项目的库存状态（封面和标题）
    
    参数:
        project_path: 项目路径
        container_ids: 需要检查的 Container ID 列表
    
    返回:
    {
        "thumbnails": {
            12: {"available": [1, 2, 3], "next": 1, "total": 3},
            18: {"available": [1, 2], "next": 1, "total": 2},
        },
        "titles": {
            12: {"available": [1, 2, 3, 4, 5], "total": 5},
            18: {"available": [1, 2, 3], "total": 3},
        },
        "warnings": [
            "Container 18: 封面只剩 2 套",
            "Container 24: 无可用封面！"
        ]
    }
    """
    result = {
        "thumbnails": {},
        "titles": {},
        "warnings": []
    }
    
    if not project_path or not project_path.exists():
        return result
    
    images_dir = project_path / "images"
    
    # ========== 加载 Container Code 映射 ==========
    # 用于匹配长格式封面文件名 (如 1444359002_01.png)
    serial_to_container_code = {}
    if CHANNEL_MAPPING_PATH.exists():
        try:
            with open(CHANNEL_MAPPING_PATH, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            for container_code, info in mapping.get("channels", {}).items():
                serial = info.get("serial_number")
                if serial:
                    serial_to_container_code[serial] = container_code
        except Exception as e:
            pass  # 静默处理，回退到默认逻辑
    
    # ========== 1. 统计封面库存 ==========
    for serial in container_ids:
        available_sets = set()
        container_code = serial_to_container_code.get(serial)
        
        if images_dir.exists():
            for ext in ['png', 'jpg', 'jpeg', 'webp']:
                # 方式1: 检查序号格式 (12_01.png)
                for f in images_dir.glob(f"{serial}_*.{ext}"):
                    if 'used' in str(f):
                        continue
                    match = re.match(rf'{serial}_(\d+)', f.stem)
                    if match:
                        available_sets.add(int(match.group(1)))
                
                # 方式2: 检查 Container Code 格式 (1444359002_01.png)
                if container_code:
                    for f in images_dir.glob(f"{container_code}_*.{ext}"):
                        if 'used' in str(f):
                            continue
                        match = re.match(rf'{container_code}_(\d+)', f.stem)
                        if match:
                            available_sets.add(int(match.group(1)))
        
        available_list = sorted(available_sets)
        next_set = min(available_sets) if available_sets else None
        
        result["thumbnails"][serial] = {
            "available": available_list,
            "next": next_set,
            "total": len(available_list)
        }
        
        # 生成预警
        if len(available_list) == 0:
            result["warnings"].append(f"Container {serial}: ❌ 封面已用完！")
        elif len(available_list) <= 2:
            result["warnings"].append(f"Container {serial}: ⚠️ 封面只剩 {len(available_list)} 套")
    
    # ========== 2. 统计标题库存 ==========
    # 构建 Container Code 到序号的反向映射
    container_code_to_serial = {v: k for k, v in serial_to_container_code.items()}
    
    metadata = parse_metadata(project_path)
    
    for meta in metadata:
        container_id_str = meta.get("container_id", "")
        if not container_id_str:
            continue
        
        # 尝试匹配: 可能是序号，也可能是 Container Code
        try:
            container_value = int(container_id_str)
        except:
            continue
        
        # 确定对应的序号
        if container_value in container_ids:
            # 直接匹配序号
            serial = container_value
        elif str(container_value) in container_code_to_serial or container_value in container_code_to_serial:
            # 是 Container Code，转换为序号
            serial = container_code_to_serial.get(str(container_value)) or container_code_to_serial.get(container_value)
        else:
            continue
        
        if serial not in container_ids:
            continue
        
        # 获取标题数量
        title_by_set = meta.get("title_by_set", {})
        titles_list = meta.get("titles", [])
        
        if title_by_set:
            available_sets = sorted(title_by_set.keys())
            total = len(available_sets)
        else:
            available_sets = list(range(1, len(titles_list) + 1))
            total = len(titles_list)
        
        result["titles"][serial] = {
            "available": available_sets,
            "total": total
        }
        
        # 标题预警
        if total == 0:
            result["warnings"].append(f"Container {serial}: ❌ 无标题！")
        elif total <= 2:
            result["warnings"].append(f"Container {serial}: ⚠️ 标题只剩 {total} 套")
    
    return result


def get_channel_info(tag: str, serial: int) -> Optional[dict]:
    """
    获取指定频道的完整配置信息（带内存缓存，减少重复IO）

    返回:
    {
        "serial": 117,
        "tag": "大提琴",
        "project_name": "古典大提琴_心靈典藏",
        "is_ypp": True,
        "ypp_index": 3,  # 在 YPP 列表中的索引
        "channel_index": 4,  # 在所有频道中的索引
        "titles": ["标题1", "标题2", "标题3"],
        "description": "...",
        "thumbnails": [Path(...), Path(...), Path(...)],
        "port": None  # 如果浏览器未启动则为 None
    }
    """
    # 检查缓存
    cache_key = (tag, serial)
    cached = _CHANNEL_INFO_CACHE.get(cache_key)
    if cached is not None:
        cached_time, cached_result = cached
        if time.time() - cached_time < _CHANNEL_INFO_CACHE_TTL:
            return cached_result

    config = load_config()
    tag_config = config.get("tag_to_project", {}).get(tag)
    
    if not tag_config:
        log(f"未找到标签 '{tag}' 的配置", "ERR")
        return None
    
    ypp_serials = tag_config.get("ypp_serials", [])
    non_ypp_serials = tag_config.get("non_ypp_serials", [])
    # 重要：使用 sorted 确保顺序与 metadata 文件一致
    all_serials = sorted(ypp_serials + non_ypp_serials)
    
    if serial not in all_serials:
        log(f"序号 {serial} 不在标签 '{tag}' 的频道列表中", "ERR")
        return None
    
    is_ypp = serial in ypp_serials
    channel_index = all_serials.index(serial)  # 在 sorted 列表中的位置 = metadata 索引
    ypp_index = ypp_serials.index(serial) if is_ypp else -1
    
    # 获取项目路径
    project_name = tag_config.get("project_name")
    projects_folder = Path(config.get("projects_folder", ""))
    project_path = projects_folder / project_name if project_name else None
    
    # 解析元数据
    metadata = []
    if project_path and project_path.exists():
        metadata = parse_metadata(project_path)
    
    # 获取标题 (轮询分配)
    titles = []
    description = ""
    
    if channel_index < len(metadata):
        channel_meta = metadata[channel_index]
        base_titles = channel_meta.get("titles", [])
        description = channel_meta.get("description", "")
        
        if is_ypp and len(base_titles) >= 3:
            # YPP 频道：轮询分配 3 个标题给 A/B Testing
            start_index = (ypp_index * 3) % len(base_titles)
            for k in range(3):
                idx = (start_index + k) % len(base_titles)
                titles.append(base_titles[idx])
        else:
            # 非 YPP 频道：按频道位置选择单个标题
            title_index = channel_index % len(base_titles) if base_titles else 0
            titles = [base_titles[title_index]] if base_titles else []
    
    # 获取封面 - 尝试从 organized 目录读取
    thumbnails = []
    if project_path:
        # 尝试从 channel_mapping.json 获取频道名
        channel_name = None
        if CHANNEL_MAPPING_PATH.exists():
            try:
                with open(CHANNEL_MAPPING_PATH, "r", encoding="utf-8") as f:
                    mapping = json.load(f)
                for container_code, info in mapping.get("channels", {}).items():
                    if info.get("serial_number") == serial:
                        channel_name = info.get("channel_name")
                        break
            except:
                pass
        
        if channel_name:
            # 新版: 从 organized 目录按频道名读取
            thumbnails = get_thumbnails(project_path, channel_name)
        
        if not thumbnails:
            # 旧版: 计算封面偏移
            all_thumbnails = get_thumbnails(project_path)
            if all_thumbnails:
                prev_thumb_count = 0
                for prev_serial in all_serials[:channel_index]:
                    prev_thumb_count += 3 if prev_serial in ypp_serials else 1
                thumb_count = 3 if is_ypp else 1
                thumbnails = all_thumbnails[prev_thumb_count:prev_thumb_count + thumb_count]
    
    result = {
        "serial": serial,
        "tag": tag,
        "project_name": project_name,
        "project_path": project_path,
        "is_ypp": is_ypp,
        "ypp_index": ypp_index,
        "channel_index": channel_index,
        "titles": titles,
        "description": description,
        "thumbnails": thumbnails,
    }
    # 写入缓存
    _CHANNEL_INFO_CACHE[cache_key] = (time.time(), result)
    return result

def interactive_select_tags() -> List[str]:
    """
    交互式选择多个标签
    返回: ["大提琴", "小提琴"]
    """
    tags = get_all_tags()
    
    if not tags:
        log("没有找到任何标签配置", "ERR")
        return []
    
    print("\n" + "="*50)
    print("   📋 可用标签 (支持多选，用逗号分隔)")
    print("="*50)
    
    for i, tag in enumerate(tags, 1):
        tag_info = get_tag_info(tag)
        channel_count = len(tag_info["all_serials"]) if tag_info else 0
        ypp_count = len(tag_info["ypp_serials"]) if tag_info else 0
        print(f"   {i}. {tag} ({channel_count}个频道, {ypp_count}个YPP)")
    
    print()
    try:
        choice = input("请选择标签 [例如: 1, 2]: ")
        indices = [int(x.strip()) - 1 for x in choice.replace("，", ",").split(",") if x.strip().isdigit()]
        
        selected_tags = []
        for idx in indices:
            if 0 <= idx < len(tags):
                selected_tags.append(tags[idx])
        
        if not selected_tags:
            log("未选择有效标签", "ERR")
            return []
            
        print(f"\n✅ 已选择: {', '.join(selected_tags)}")
        return selected_tags
        
    except (ValueError, KeyboardInterrupt):
        return []

def interactive_select() -> Optional[dict]:
    """
    交互式选择单个标签和频道 (用于测试脚本)
    返回 get_channel_info() 的结果
    """
    tags = get_all_tags()
    
    if not tags:
        log("没有找到任何标签配置", "ERR")
        return None
    
    print("\n" + "="*50)
    print("   📋 可用标签")
    print("="*50)
    
    for i, tag in enumerate(tags, 1):
        tag_info = get_tag_info(tag)
        channel_count = len(tag_info["all_serials"]) if tag_info else 0
        ypp_count = len(tag_info["ypp_serials"]) if tag_info else 0
        print(f"   {i}. {tag} ({channel_count}个频道, {ypp_count}个YPP)")
    
    print()
    try:
        choice = input("请选择标签 [1-{}]: ".format(len(tags)))
        tag_index = int(choice) - 1
        if tag_index < 0 or tag_index >= len(tags):
            log("无效选择", "ERR")
            return None
    except (ValueError, KeyboardInterrupt):
        return None
    
    selected_tag = tags[tag_index]
    tag_info = get_tag_info(selected_tag)
    
    print("\n" + "="*50)
    print(f"   📋 {selected_tag} 下的频道")
    print("="*50)
    
    for serial in tag_info["all_serials"]:
        is_ypp = serial in tag_info["ypp_serials"]
        ypp_mark = " (YPP)" if is_ypp else ""
        print(f"   {serial}{ypp_mark}")
    
    print()
    try:
        serial_input = input("请选择频道序号: ")
        serial = int(serial_input)
        if serial not in tag_info["all_serials"]:
            log(f"序号 {serial} 不在列表中", "ERR")
            return None
    except (ValueError, KeyboardInterrupt):
        return None
    
    # 获取完整信息
    channel_info = get_channel_info(selected_tag, serial)
    
    if channel_info:
        print("\n" + "="*50)
        print(f"   ✅ 已选择频道 {serial}")
        print("="*50)
        print(f"   标签: {channel_info['tag']}")
        print(f"   项目: {channel_info['project_name']}")
        print(f"   YPP: {'是' if channel_info['is_ypp'] else '否'}")
        if channel_info['titles']:
            print(f"   标题 1: {channel_info['titles'][0][:50]}...")
        if channel_info['thumbnails']:
            print(f"   封面: {', '.join([t.name for t in channel_info['thumbnails']])}")
        print()
    
    return channel_info
