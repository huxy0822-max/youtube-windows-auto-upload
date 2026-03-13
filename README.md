# YouTube 自动化项目整理说明

这个仓库目前不是“完整上传项目”，而是以**视频渲染 + 日常调度**为主的子项目。  
你朋友给的 `朋友使用教程.md` 对应的是另一套更完整的“上传视频自动化”仓库，里面应该还有 `scripts/batch_upload.py`、`config/upload_config.json` 等文件；这些文件在当前目录里原本并不完整。

这次我已经先把当前仓库整理成一个更适合 Windows 使用的版本，重点做了这些事：

1. 补上了原本缺失的 `effects_library.py`，避免 GUI 和调度器一启动就报错。
2. 修正了路径解析，优先使用当前仓库内的 `config/`，不再默认写死 macOS 绝对路径。
3. 增加了 `HubStudio / BitBrowser` 可切换的浏览器 API 适配层。
4. 新增了基础配置模板和中文说明文档，方便后续继续补上传脚本。

如果你重点关心“参数在哪里改 / 提示词怎么改 / 模型怎么换”，直接看：

- `docs/实操配置与提示词说明.md`

现在日常推荐入口：

- `dashboard.py`
- `启动统一控制台.bat`
- `config/prompt_studio.json`

## 当前目录结构

```text
youtube自动化/
├── app.py                    # 渲染工作站 GUI
├── daily_scheduler.py        # 批量调度入口，可选调用外部上传脚本
├── render_engine.py          # FFmpeg 渲染核心
├── scheduler_gui.py          # 调度器 GUI 壳层
├── utils.py                  # 上传侧配置/元数据/频道工具函数
├── browser_api.py            # HubStudio / BitBrowser API 适配层
├── path_helpers.py           # 路径与配置文件查找辅助函数
├── effects_library.py        # 频谱/时间轴/粒子/文字等特效生成
├── scheduler_config.json     # 调度器本地配置（已改成 Windows 友好默认值）
├── config/
│   ├── upload_config.json    # 上传/浏览器配置模板
│   └── channel_mapping.json  # 频道映射模板
├── docs/
│   └── 文件用途说明.md        # 每个文件/目录是干嘛的
├── fonts/                    # 字体素材
├── overlays/                 # 粒子叠层视频素材
├── 朋友使用教程.md            # 原始朋友教程，偏 Mac + HubStudio
└── 归档.zip                  # 老归档，内容与当前仓库部分重复
```

## BitBrowser 适配方式

默认配置已经改成支持在 `config/upload_config.json` 里切换浏览器提供方：

```json
{
  "browser_provider": "bitbrowser",
  "browser_api": {
    "provider": "bitbrowser",
    "base_url": "http://127.0.0.1:54345",
    "list_endpoint": "/browser/list",
    "open_endpoint": "/browser/open",
    "open_payload_id_key": "id"
  }
}
```

如果你后面接入的 BitBrowser 版本字段不同，直接改这个 JSON 就行，不需要再去改 Python 代码。

## 现在能做什么

- 可以把它当成一个 Windows 下可用的渲染/调度工程继续整理。
- 如果你补齐真实的上传脚本 `batch_upload.py`，现有 `utils.py + browser_api.py` 已经为 BitBrowser/HubStudio 预留好了适配口。

## 现在还缺什么

- 当前仓库仍然**不包含**朋友教程里说的完整上传脚本。
- 所以 `daily_scheduler.py` 里的“自动上传”仍然是可选功能；找不到外部上传脚本时，会自动回退成“只渲染不上传”。
