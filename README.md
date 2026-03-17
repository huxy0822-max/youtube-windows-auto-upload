# YouTube 自动化控制台

这是当前可稳定运行的 Windows 主线仓库。

## 当前稳定线

- 电脑专属分支：`windows-huxy`
- 稳定标签：`windows-huxy-stable-v1.0`
- 历史稳定标签：`stable-refactor-v1.0`

## 多电脑多分支规则

这个仓库以后按“每台电脑一条分支”维护。

推荐命名规则：

- `windows-huxy`
- `mac-huxy`
- `windows-朋友名`
- `mac-朋友名`

原则：

- 通用逻辑尽量共用
- 电脑专属适配放各自分支
- 每个稳定版本都单独打标签，方便随时回退

## Windows 版当前主流程

当前仓库主要使用这几个文件：

- `dashboard.py`
- `dashboard_app.py`
- `workflow_core.py`
- `run_plan_service.py`
- `metadata_service.py`
- `batch_upload.py`
- `browser_api.py`
- `effects_library.py`

## 启动

Windows:

```powershell
cd C:\youtube自动化
py -3 dashboard.py
```

或者双击：

- `C:\youtube自动化\启动统一控制台.bat`

macOS:

- 建议直接双击 `启动统一控制台.command`
- 它会优先选择可用的 Tk Python，并在仓库内自动创建 `.venv/`
- 首次运行会自动安装 `requirements.txt`
- 如果提示没有可用 Tk Python，先执行 `brew install python@3.14 python-tk@3.14`

## 当前功能规则

### 1. 三块能力可独立，也可组合

- 生成标题 / 简介 / 标签 / 缩略图
- 剪辑生成视频
- 上传到 YouTube

勾选哪个就运行哪个。

### 2. 文案只走提示词那套

旧的 `daily / generation_map` 不再作为正式文案来源。

### 3. 批量时每个窗口都必须唯一

唯一范围包括：

- 同一批多个窗口之间不能重复
- 不同批之间也不能重复
- 已用标题 / 简介 / 标签 / 缩略图会进入已用库，后续不再复用

### 4. 上传支持并发

不是传完第一个再传第二个，而是：

- 有几个窗口任务
- 就同时打开几个浏览器窗口
- 并发上传几个任务

### 5. 上传固定规则

- `Altered content = Yes`
- `Category = Music`
- 默认不勾选 `Publish to subscriptions feed and notify subscribers`
- 页面上可切换是否勾选
- 支持 `Asia/Taipei (+08:00)` 定时发布

### 6. 路径优先级

始终按这个顺序：

1. 当前任务里手动选的目录
2. 分组长期绑定目录
3. 全局默认目录

目录名不再强绑分组名。

## 多电脑接手方式

### 你的 Mac 电脑

建议流程：

```bash
git clone https://github.com/huxy0822-max/youtube-windows-auto-upload.git
cd youtube-windows-auto-upload
git checkout -b mac-huxy origin/windows-huxy
```

然后让 Mac 上的 Codex 先读这些文件：

- `AGENTS.md`
- `README.md`
- `docs/项目功能总纲-2026-03-17.md`

### 朋友的 Windows 电脑

建议流程：

```powershell
git clone https://github.com/huxy0822-max/youtube-windows-auto-upload.git
cd youtube-windows-auto-upload
git checkout -b windows-朋友名 origin/windows-huxy
```

这样朋友可以在自己的电脑分支上改适配，不会影响你的稳定线。

## 建议直接发给 Codex 的话

### Mac 上发给 Codex

```text
这是一个 YouTube 自动化项目。先读 AGENTS.md、README.md、docs/项目功能总纲-2026-03-17.md。当前稳定 Windows 线是 windows-huxy，请基于它创建并维护 mac-huxy 分支。目标是在不破坏现有功能逻辑的前提下，把浏览器启动、路径、FFmpeg 编码器、系统命令改成适配我的 Mac。不要动 Windows 专属分支。
```

### 朋友 Windows 上发给 Codex

```text
这是一个 YouTube 自动化项目。先读 AGENTS.md、README.md、docs/项目功能总纲-2026-03-17.md。当前稳定线是 windows-huxy，请基于它创建并维护当前这台电脑专属的 Windows 分支，只处理这台电脑的路径、浏览器、FFmpeg、依赖适配。不要破坏现有上传、文案、剪辑主流程。
```

## 稳定版回退

回退到这版：

```powershell
git checkout windows-huxy-stable-v1.0
```

## 主要文档

- `docs/项目功能总纲-2026-03-17.md`
- `docs/重构阶段1审阅-2026-03-17.md`
- `docs/新控制台使用说明.md`
