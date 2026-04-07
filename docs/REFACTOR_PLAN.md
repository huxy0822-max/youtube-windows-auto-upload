# YouTube 自动化控制台 — 全面重构方案

> 本文档由 Claude Code（总指挥）编写，供 Codex（执行者）按步骤实施。
> 日期：2026-03-25

---

## 一、项目现状总结

### 1.1 这个项目是什么
一套 **YouTube 音乐频道自动化控制台**，统一管理三个核心能力：
1. **生成文案**（标题/简介/标签/缩略图）— 通过你在提示词页面配置的自定义 API（支持任意 OpenAI 兼容接口，地址和密钥随时可换）
2. **剪辑视频**（音频+底图→成品视频）— 通过 FFmpeg
3. **上传到 YouTube**（自动操作 YouTube Studio）— 通过 Playwright + BitBrowser

### 1.2 当前已有的文件结构
```
C:\youtube自动化-claude优化版本\
├── dashboard.py              # 启动入口（仅调用 dashboard_app.py）
├── dashboard_app.py          # GUI 主体（CustomTkinter，4380行）
├── batch_upload.py           # 上传引擎（Playwright 自动化，8500+行）
├── workflow_core.py          # 渲染+元数据工作流（2600行）
├── daily_scheduler.py        # 批量渲染调度（1000+行）
├── browser_api.py            # BitBrowser/HubStudio 适配器（500行）
├── human_interaction.py      # 人类行为模拟（218行）— Claude新增
├── content_generation.py     # LLM API 调用（500+行）
├── prompt_studio.py          # 提示词模板管理（425行）
├── metadata_service.py       # 已用元数据去重（503行）
├── group_upload_workflow.py  # 分组上传准备（430行）
├── upload_window_planner.py  # 窗口任务规划（355行）
├── run_plan_service.py       # 运行计划翻译层（522行）
├── effects_library.py        # FFmpeg 视觉特效库（500+行）
├── utils.py                  # 通用工具（840行）
├── path_helpers.py           # 跨平台路径（129行）
├── scheduler_config.json     # 全局路径+编码器配置
└── config/
    ├── upload_config.json      # 分组定义（tag→serial映射）
    ├── channel_mapping.json    # 容器→频道映射
    └── prompt_studio.json      # API/内容模板
```

### 1.3 当前存在的严重问题

| # | 问题 | 严重程度 | 原因 |
|---|------|----------|------|
| 1 | UI 中文标签显示为 `???` | **高** | dashboard_app.py 文件编码损坏，中文字符丢失 |
| 2 | "刷新分组"按钮不好使 | **高** | `_refresh_groups()` 方法依赖的数据源或逻辑有bug |
| 3 | 一次只能上传一个分组 | **高** | 架构限制：提示词/路径/视觉设置绑定到整次任务而非单个分组 |
| 4 | 无法多分组批量运行 | **高** | 缺少分组队列机制 |
| 5 | 不适配其他电脑 | **中** | 硬编码路径、缺少首次配置引导 |
| 6 | channel_mapping.json description 乱码 | **低** | 同样的编码问题 |

---

## 二、用户核心需求（从 GitHub 维护记录提炼）

### P0 — 必须解决
1. **多分组批量运行**：一次运行可以处理多个分组的多个内容，每个分组有自己的提示词/路径/视觉设置
2. **批量文案同批唯一**：同一批次内的窗口之间标题/简介/标签不重复即可（API调用本身很难历史重复，不做历史去重，节省资源）
3. **多窗口并发上传**：多个窗口同时打开同时上传，不是排队串行
4. **路径逻辑不乱猜**：优先级固定为 手动覆盖 > 分组绑定 > 全局默认
5. **修复所有 ??? 乱码**：恢复所有中文标签和描述

### P1 — 重要
6. **暂停/取消功能**：运行中可暂停队列、取消当前窗口或整个批次
7. **上传细节稳定**：每次都正确设置 Music 分类、AI 声明、Altered content
8. **只上传模式**：不要求图片和音频目录有素材，只检查成品视频和文案

### P2 — 锦上添花
9. **视觉预设下拉系统**：将 MegaBass 等预设做成下拉框，支持无限扩展自定义预设；选中预设的窗口跳过手动视觉设置，直接走预设方案
10. **渲染速度优化**：NVENC 优先，减少 CPU 瓶颈
11. **跨电脑适配**：首次运行自动检测环境，引导配置

---

## 三、重构架构设计

### 3.1 核心架构变更：引入「运行队列」概念

**现状**：用户选一个分组 → 选窗口 → 运行 → 结束 → 再选下一个分组

**目标**：用户选多个分组 → 每个分组各自配置 → 一键全部运行

```
┌─────────────────────────────────────────────┐
│              RunQueue（运行队列）              │
│                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │ GroupJob │  │ GroupJob │  │ GroupJob │     │
│  │ 华尔兹   │  │ 面壁者   │  │ MegaBass │     │
│  │          │  │          │  │          │     │
│  │ windows: │  │ windows: │  │ windows: │     │
│  │ [22,47]  │  │ [91,95]  │  │ [101]    │     │
│  │          │  │          │  │          │     │
│  │ prompt:  │  │ prompt:  │  │ prompt:  │     │
│  │ 模板A    │  │ 模板B    │  │ 模板A    │     │
│  │          │  │          │  │          │     │
│  │ source:  │  │ source:  │  │ source:  │     │
│  │ F:\华尔兹│  │ F:\面壁者│  │ F:\Mega  │     │
│  │          │  │          │  │          │     │
│  │ visual:  │  │ visual:  │  │ visual:  │     │
│  │ 随机     │  │ 手动     │  │ 随机     │     │
│  └─────────┘  └─────────┘  └─────────┘     │
│                                             │
│  执行策略：分组串行，组内窗口并发              │
└─────────────────────────────────────────────┘
```

### 3.2 新增核心数据结构

```python
# run_queue.py — 新文件

@dataclass
class GroupJob:
    """一个分组的完整任务定义"""
    group_tag: str                    # 分组标签（如 "0128-华尔兹"）
    windows: list[WindowTask]         # 该分组要处理的窗口列表
    source_dir: str                   # 素材目录（已按优先级解析）
    prompt_template: str              # 内容模板名
    api_template: str                 # API模板名
    visual_mode: str                  # "random" | "manual"
    visual_settings: dict | None      # 手动视觉参数（仅 manual 模式）
    upload_defaults: UploadDefaults   # 上传默认值（可见性/分类/儿童/AI等）
    modules: list[str]                # 要执行的模块 ["metadata", "render", "upload"]

@dataclass
class UploadDefaults:
    """上传默认参数"""
    visibility: str = "private"       # public | private | unlisted | scheduled
    category: str = "Music"
    is_for_kids: bool = False
    ai_content: str = "yes"
    altered_content: str = "yes"
    schedule_date: str | None = None
    schedule_time: str | None = None
    timezone: str = "Asia/Taipei"
    auto_close_after: bool = False

@dataclass
class RunQueue:
    """整个运行队列"""
    jobs: list[GroupJob]
    execution_mode: str = "group_serial_window_parallel"
    # 分组串行（保证资源不冲突），组内窗口并发（最大化效率）
```

### 3.3 GUI 重构方案

#### 上传页（Tab 1）重新设计

**现状布局**：
```
┌──────────────────────────────────┐
│ 分组: [下拉框] [刷新分组]         │
│ 窗口按钮区域                      │
│ 任务区域（已选窗口）              │
│ 默认上传参数                      │
└──────────────────────────────────┘
```

**目标布局**：
```
┌──────────────────────────────────────────────┐
│ 【运行队列区】                                │
│ ┌──────────────────────────────────────────┐ │
│ │ ✕ 华尔兹 (3窗口) | 模板A | F:\华尔兹     │ │
│ │ ✕ 面壁者 (2窗口) | 模板B | F:\面壁者     │ │
│ │ ✕ MegaBass (1窗口) | 模板A | F:\Mega    │ │
│ └──────────────────────────────────────────┘ │
│                                              │
│ 【添加分组到队列】                             │
│ 分组: [下拉框▼] [刷新分组]                    │
│ 素材目录: [路径] [选择文件夹]                  │
│ 提示词模板: [下拉框▼]  API模板: [下拉框▼]     │
│ 视觉模式: [随机/手动▼]                        │
│ 窗口: [全选] [22] [47] [59] [91]             │
│ [➕ 添加到队列]                                │
│                                              │
│ 【统一默认规则】                               │
│ 可见性 | 分类 | 儿童内容 | AI内容              │
│ 定时发布 | 时区 | 自动关闭                     │
│                                              │
│ [▶ 开始运行] [⏸ 暂停] [⏹ 取消]               │
└──────────────────────────────────────────────┘
```

关键变化：
1. **运行队列区**：顶部显示已加入队列的所有分组任务，可单独删除
2. **添加分组区**：选分组→配参数→加入队列，可重复操作添加多个分组
3. **统一默认规则**：所有分组共享的上传参数
4. **控制按钮**：开始/暂停/取消

#### 其他Tab调整
- 提示词页：维持现有多模板+分组绑定逻辑
- 路径配置页：维持现有分组→目录绑定逻辑
- **高级视觉页改造**：
  - 顶部新增：**视觉预设下拉框**（默认"无预设 - 使用下方手动设置"）
  - 预设选项举例：MegaBass、其他未来自定义预设...
  - 选中预设后：下方手动参数区域**灰显不可编辑**（明确告知用户这些参数不生效）
  - 选"无预设"时：下方手动参数正常可用
  - 预设数据存储在 `config/visual_presets.json`，格式：
    ```json
    {
      "MegaBass": {
        "spectrum": "bass_heavy",
        "timeline": "on",
        "letterbox": "off",
        "zoom": "slow_drift",
        "particles": ["bass_pulse", "neon_glow"],
        "tint": "purple_deep"
      },
      "LoFi夜曲": { ... },
      "古典金色": { ... }
    }
    ```
  - 用户可通过"保存当前为预设"按钮，把当前手动参数保存为新预设
  - 分组配置区的"视觉模式"下拉也要包含所有预设选项：`随机 | 手动 | MegaBass | LoFi夜曲 | ...`
- 日志页：增加分组级别的进度显示

### 3.4 上传引擎重构

**现状**：`batch_upload.py` 串行处理每个窗口

**目标**：
```python
# 伪代码
async def execute_run_queue(queue: RunQueue):
    for job in queue.jobs:                    # 分组串行
        # 1. 生成文案（如果勾选了）
        metadata_list = await generate_all_metadata(job)

        # 2. 剪辑视频（如果勾选了）
        video_list = await render_all_videos(job)

        # 3. 并发上传（如果勾选了）
        tasks = []
        for window in job.windows:
            task = asyncio.create_task(
                upload_single_window(window, metadata_list[window.id])
            )
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 4. 记录结果
        record_results(job, results)
```

### 3.5 文案同批去重（轻量级）

只需保证**同一批次内**各窗口的文案不重复，不做历史去重（API调用天然低重复率，历史去重浪费资源）。

```python
# metadata_service.py 增强

class BatchDedup:
    """同批次文案去重"""

    def __init__(self):
        self.batch_titles = set()   # 当前批次已用标题
        self.batch_descs = set()    # 当前批次已用简介

    def is_duplicate(self, title: str, description: str) -> bool:
        return title in self.batch_titles or description in self.batch_descs

    def record(self, title: str, description: str):
        self.batch_titles.add(title)
        self.batch_descs.add(description)

    def generate_unique(self, prompt, max_retries=3) -> dict:
        """生成文案，同批重复则重试（最多3次）"""
        for attempt in range(max_retries):
            result = call_text_model(prompt)
            if not self.is_duplicate(result['title'], result['description']):
                self.record(result['title'], result['description'])
                return result
        # 3次都重复（极罕见），强制使用最后一次结果
        self.record(result['title'], result['description'])
        return result
```

---

## 四、具体实施步骤（给 Codex 的指令）

### 阶段 1：修复基础问题（先让现有功能恢复正常）

#### Step 1.1：修复所有 ??? 乱码
- **文件**：`dashboard_app.py`
- **操作**：对照 GitHub 仓库 `huxy0822-max/youtube-windows-auto-upload` 的 `dashboard_app.py`，恢复所有被损坏的中文标签
- **具体位置**：
  - 第762行 `"????? YPP"` → `"加入窗口时 YPP"`
  - 第766行 `"??(???)"` → `"自定义标题"`
  - 第770行 `"???"` → `"可见性"`
  - 第772行 `"??"` → `"分类"`
  - 第778行 `"????"` → `"儿童内容"`
  - 第780行 `"AI ??"` → `"AI 内容"`
  - 搜索整个文件中所有 `???` 并逐一修复
- **文件**：`config/channel_mapping.json`
  - 第4行 description 修复为正确中文

#### Step 1.2：修复"刷新分组"功能
- **文件**：`dashboard_app.py` 的 `_refresh_groups()` 方法（第2071行）
- **操作**：
  1. 读取 `config/upload_config.json` 中的 `tag_to_project` 获取所有分组
  2. 用 `browser_api.list_browser_envs()` 获取当前在线的 BitBrowser 环境
  3. 与 `config/channel_mapping.json` 交叉匹配，得到每个分组下的可用窗口列表
  4. 更新所有下拉框的选项列表
  5. 如果当前选中的分组不在新列表里，切换到第一个分组
- **验证**：点击"刷新分组"后，下拉框应显示所有已配置的分组名

#### Step 1.3：修复编码一致性
- **所有 .py 文件**：确保文件头有 `# -*- coding: utf-8 -*-`
- **所有 JSON 文件**：确保用 UTF-8 无 BOM 保存
- **所有文件 I/O**：确保 `open()` 都带 `encoding="utf-8"`

### 阶段 2：引入运行队列架构

#### Step 2.1：创建 `run_queue.py`
- 新建文件，定义 `GroupJob`、`UploadDefaults`、`RunQueue` 数据类
- 提供 `add_job()`、`remove_job()`、`get_queue_summary()` 方法
- 提供序列化/反序列化（保存/恢复队列状态）

#### Step 2.2：重构上传页 GUI
- **文件**：`dashboard_app.py`
- **操作**：按 3.3 节的目标布局重写上传页
- **关键点**：
  - 顶部：运行队列展示区（CTkScrollableFrame）
  - 中部：分组配置区（选分组→配参数→加入队列）
  - 底部：统一默认规则 + 控制按钮
  - "添加到队列"按钮：创建 GroupJob，加入 RunQueue，刷新队列展示
  - 每个队列项有 ✕ 删除按钮
  - "全选"按钮：一键选中当前分组所有窗口

#### Step 2.3：重构 `run_plan_service.py`
- 修改 `build_run_plan()` 接受 `RunQueue` 而非单个分组
- 修改 `execute_run_plan()` 按分组串行、窗口并发执行
- 每个分组用自己绑定的提示词模板和路径

### 阶段 3：实现多窗口并发上传

#### Step 3.1：重构 `batch_upload.py` 的批量入口
- 将 `batch_upload()` 改为接受 `GroupJob` 参数
- 内部使用 `asyncio.gather()` 并发启动多个窗口的上传
- 每个窗口独立持有自己的 browser context、metadata、错误处理

#### Step 3.2：增强 `human_interaction.py`
- 确保每个并发窗口的随机延迟互不影响
- 每个窗口用独立的 Random 实例（避免共享随机种子）

#### Step 3.3：实现暂停/取消机制
- 引入 `asyncio.Event` 作为暂停信号
- 每个关键步骤前检查暂停/取消标志
- GUI 的暂停/取消按钮设置对应标志

### 阶段 4：同批文案去重 + 视觉预设系统

#### Step 4.1：轻量级同批去重
- 在 `metadata_service.py` 中实现 `BatchDedup` 类
- 仅在内存中跟踪当前批次已用的标题和简介
- 同批重复时自动重试（最多3次），不做历史持久化
- 修改 `content_generation.py` 的 `generate_content_bundle()` 接受 `BatchDedup` 参数

#### Step 4.2：视觉预设系统
- 新建 `config/visual_presets.json`，存储预设数据
- 在 `dashboard_app.py` 高级视觉页顶部添加预设下拉框
- 选中预设后灰显手动参数区域
- 添加"保存当前为预设"按钮
- 在上传页的分组配置区，视觉模式下拉包含所有预设选项
- `workflow_core.py` 渲染时根据预设名从 `visual_presets.json` 读取参数

### 阶段 5：跨电脑适配

#### Step 5.1：路径自动检测
- **文件**：`path_helpers.py`
- 首次运行检测：FFmpeg 是否在 PATH、GPU 类型、默认工作目录
- 如果 `scheduler_config.json` 不存在，自动生成默认配置

#### Step 5.2：BitBrowser 适配
- **文件**：`browser_api.py`
- 自动检测 BitBrowser 端口（如果默认端口不通，扫描常见端口）
- 支持在 GUI 配置页手动填写 API 地址

---

## 五、文件修改清单

| 文件 | 操作 | 改动量 |
|------|------|--------|
| `dashboard_app.py` | **重写上传页** + 修复乱码 | 大 |
| `run_queue.py` | **新建** | 中 |
| `batch_upload.py` | 重构批量入口为并发 | 中 |
| `run_plan_service.py` | 适配 RunQueue | 中 |
| `metadata_service.py` | 增加 MetadataGuard | 中 |
| `content_generation.py` | 接入 MetadataGuard | 小 |
| `human_interaction.py` | 并发安全增强 | 小 |
| `browser_api.py` | 端口自动检测 | 小 |
| `path_helpers.py` | 首次运行检测 | 小 |
| `workflow_core.py` | 适配多分组 | 中 |
| `group_upload_workflow.py` | 适配 GroupJob | 小 |
| `config/channel_mapping.json` | 修复乱码 | 极小 |

---

## 六、冒烟测试计划

### 测试 1：基础启动
- `py -3 dashboard.py` 能正常启动
- 所有 UI 标签显示正确中文（无 ???）
- 所有 Tab 可以正常切换

### 测试 2：刷新分组
- 点击"刷新分组"按钮
- 下拉框显示所有已配置的分组
- 选择不同分组，窗口按钮区域正确更新

### 测试 3：运行队列
- 选择分组A → 选窗口 → 添加到队列
- 选择分组B → 选窗口 → 添加到队列
- 队列区显示两个分组任务
- 删除其中一个，队列正确更新

### 测试 4：单分组上传（1个窗口）
- 添加1个窗口到队列
- 点击开始运行
- 观察 BitBrowser 窗口打开
- 观察 YouTube Studio 页面加载
- 观察文件上传流程
- 观察分类/AI声明正确设置
- 上传完成后状态正确更新

### 测试 5：单分组并发上传（2+窗口）
- 添加2个以上窗口到队列
- 点击开始运行
- 确认多个 BitBrowser 窗口同时打开
- 确认每个窗口独立上传不互相干扰
- 确认每个窗口的标题/简介不重复

### 测试 6：多分组批量运行
- 添加分组A（2窗口）和分组B（1窗口）到队列
- 点击开始运行
- 确认分组A先执行（2窗口并发）
- 分组A完成后分组B自动开始
- 所有窗口文案唯一

### 测试 7：暂停/取消
- 运行中点暂停 → 确认任务暂停
- 点继续 → 确认任务恢复
- 运行中点取消 → 确认任务终止、资源清理

---

## 七、注意事项

1. **不要动 effects_library.py**：视觉特效系统已经稳定，不需要重构
2. **不要动 daily_scheduler.py 的渲染逻辑**：只需要适配新的 RunQueue 入口
3. **保持 human_interaction.py 的接口不变**：batch_upload.py 已经在用它
4. **所有文件操作必须 encoding="utf-8"**：防止再出现乱码
5. **Windows 兼容**：所有路径用 `pathlib.Path`，不要硬编码 `/` 或 `\`
6. **不要删除任何现有配置文件格式**：保持向后兼容
7. **GPU 编码器**：当前机器是 RTX 3070，用 NVENC
