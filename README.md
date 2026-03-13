# YouTube 自动化项目整理说明

这是一个面向 Windows 的 YouTube 自动化仓库，当前重点是：

- 视频渲染
- 日常调度
- BitBrowser / HubStudio 上传适配

这个公开版已经做过一次“去私有化”整理：

- 去掉了本地运行记录、素材、成品视频和大文件
- 保留了昨天调试好的 Windows 自动上传代码
- 保留了 BitBrowser 适配逻辑
- 把配置改成了可公开分享的模板

如果你重点关心“参数在哪里改 / 提示词怎么改 / 模型怎么换”，先看：

- `docs/实操配置与提示词说明.md`

如果你准备和朋友一起维护，先看：

- `docs/GitHub协作入门.md`

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

## 首次使用前要改的文件

这个公开仓库里的配置是模板，不是你的真实生产配置。

你需要先改：

- `config/upload_config.json`
- `config/channel_mapping.json`

你需要自己填进去的内容主要是：

- 比特浏览器 API 地址
- 分组和频道序号对应关系
- 浏览器环境 ID
- 你自己的项目目录和视频目录

## 现在能做什么

- 可以把它当成一个 Windows 下可用的渲染/调度工程继续整理。
- 如果你补齐真实的上传脚本 `batch_upload.py`，现有 `utils.py + browser_api.py` 已经为 BitBrowser/HubStudio 预留好了适配口。

## 公开仓库里不包含什么

以下内容不会放进 GitHub 公开仓库：

- 真正的频道映射
- 上传记录
- 素材图、音频、成品视频
- 你本地调试时生成的大文件
