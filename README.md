# YouTube 自动化控制台

当前仓库只保留这一套主流程：

- `dashboard.py`
- `dashboard_app.py`
- `workflow_core.py`
- `batch_upload.py`
- `daily_scheduler.py`

旧版 GUI、旧批量入口、重复说明文档和历史归档已经清掉，不再推荐单独记一堆旧脚本名。

## 启动

```powershell
cd C:\youtube自动化
py -3 dashboard.py
```

或者双击：

- `C:\youtube自动化\启动统一控制台.bat`

## 当前逻辑

### 1. 快捷开始

这里只决定今天做什么：

- `本日只剪辑`
- `本日只上传`
- `本日剪辑并上传`

说明：

- 现在的 `本日只上传` 不会再直接吃旧 `upload_manifest.json`
- 它会先按上传页当前素材目录准备新视频、新标题、新简介、新标签、新缩略图，再进入上传

### 2. 上传页

这是唯一上传入口。

你只需要在这里做几件事：

- 选择 BitBrowser 分组
- 点窗口按钮，把今天要处理的窗口加入任务区
- 可选填本次临时素材目录覆盖
- 选择文案来源：
  - `提示词那套`
  - `原先那套`
- 设置默认规则：
  - 可见性
  - 分类
  - 儿童内容
  - AI 内容
  - 定时发布日期 / 时间 / 时区
  - 上传完成后自动关闭窗口

任务区里只有 1 个窗口就是单个上传，多个窗口就是批量上传，不再分两个 tab。

### 3. 提示词页

这里只管两类模板：

- API 模板
- 内容模板

支持：

- 多套模板保存
- 绑定到不同 BitBrowser 分组
- 文本 API / 图片 API 连通性测试
- 受众截图自动识别

### 4. 当日内容页

这里只改某个频道某一天的落地内容：

- 标题
- 简介
- 封面
- A/B 标题
- YPP

可以同步回 `upload_manifest.json`。

### 5. 路径配置页

这里只管：

- 音乐目录
- 底图目录
- 输出目录
- FFmpeg
- 已用素材目录
- 上传后保留天数
- 分组 -> 素材目录 的长期绑定

## 渲染与加速

当前程序会按机器实际能力自动选择编码器：

- macOS: `h264_videotoolbox`
- Windows + NVIDIA 可用: `h264_nvenc`
- Windows + AMD AMF 可用: `h264_amf`
- 否则回退 `libx264`

注意：

- 你这台机器实测是 `NVIDIA GeForce RTX 3070`
- 所以正确的 GPU 路线是 `NVENC`，不是 `AMF`
- 日志里现在会直接打印当前编码器，避免再靠猜
- 如果仍然慢，瓶颈通常在特效滤镜链，而不是编码器没启用

## 当前保留文档

- `C:\youtube自动化\docs\项目功能总纲-2026-03-17.md`
- `C:\youtube自动化\docs\重构阶段1审阅-2026-03-17.md`
- `C:\youtube自动化\docs\新控制台使用说明.md`

## 主要文件

```text
C:\youtube自动化
├── dashboard.py
├── dashboard_app.py
├── workflow_core.py
├── batch_upload.py
├── browser_api.py
├── daily_scheduler.py
├── content_generation.py
├── prompt_studio.py
├── upload_window_planner.py
├── group_upload_workflow.py
├── path_helpers.py
├── effects_library.py
├── utils.py
├── scheduler_config.json
├── config\
│   ├── upload_config.json
│   ├── channel_mapping.json
│   └── prompt_studio.json
└── docs\
    └── 新控制台使用说明.md
```
