# YouTube 自动化项目说明

这是一个已经整理成 **Windows + BitBrowser** 日常可用形态的 YouTube 自动化项目，当前包含：

- 长视频渲染
- 随机视觉特效
- 单频道上传
- 多赛道批量上传
- 同分组多视频批量上传
- 提示词 / 标题 / 简介 / 缩略图配置
- 路径集中配置

日常推荐入口：

- `dashboard.py`
- `启动统一控制台.bat`

如果你重点关心“参数在哪里改 / 提示词怎么改 / 模型怎么换 / 多赛道怎么跑”，优先看这些文档：

- `docs/统一控制台说明.md`
- `docs/实操配置与提示词说明.md`
- `docs/文件用途说明.md`

## 当前这版能做什么

1. 用 `dashboard.py` 统一管理渲染、上传、提示词、当日内容和路径。
2. 在“快捷开始”里直接选择上传方式：`单频道上传 / 多赛道任务清单 / 同分组现成视频`。
3. 在“快捷开始”里维护多赛道任务清单，一天可以连跑多个赛道。
4. 在“路径配置”里统一改音乐目录、底图目录、输出目录，并预览多赛道目录结构。
5. 在“提示词”里维护文本模型 / 图片模型 / 主提示词 / 标题库 / 生成数量。
6. 在“当日内容”里直接改 `generation_map.json`，保存后同步 `upload_manifest.json`。
7. 上传侧固定执行：
   - `Altered content = Yes`
   - `Category = Music`

## 推荐使用方式

1. 启动 `dashboard.py`
2. 在“路径配置”确认目录
3. 在“提示词”确认模板
4. 在“当日内容”确认标题、简介、封面
5. 在“快捷开始”选择今天的上传方式
6. 如果是多赛道，再填写多赛道任务清单
7. 点“开始当前流程”，或者点“仅跑当前上传方式”

如果你是“一个文件夹里有多条现成视频，要按顺序上传到同一个比特浏览器分组下的多个频道”，以后推荐这样走：

1. `dashboard.py -> 快捷开始 -> 上传方式 = 同分组现成视频`
2. 点“打开当前上传设置页”
3. 在 `分组批量上传` 页里配置并执行

## 主要文件

```text
youtube自动化/
├── dashboard.py              # 统一控制台，日常主入口
├── group_upload_batch.py     # 同分组多视频批量上传入口
├── group_upload_workflow.py  # 同分组批量上传的编排层（扫文件夹/分配频道/写manifest）
├── bulk_upload.py            # 多赛道批量上传入口
├── batch_upload.py           # 单 tag / 单频道上传主脚本
├── daily_scheduler.py        # 批量渲染调度入口，可顺带触发上传
├── app.py                    # 旧版渲染工作站 GUI
├── scheduler_gui.py          # 旧版调度器 GUI
├── content_generation.py     # 网页版 API 文案/标签/缩略图提示词生成逻辑
├── prompt_studio.py          # 提示词模板 / generation_map / manifest 辅助
├── render_engine.py          # FFmpeg 渲染核心
├── browser_api.py            # HubStudio / BitBrowser API 适配层
├── utils.py                  # 上传侧配置 / 元数据 / 频道工具函数
├── path_helpers.py           # 路径与配置文件查找辅助
├── effects_library.py        # 频谱 / 粒子 / 文字等特效生成
├── scheduler_config.json     # 本地路径配置
├── config/
│   ├── upload_config.json    # 上传 / 浏览器配置模板
│   ├── channel_mapping.json  # 频道映射模板
│   ├── prompt_studio.json    # 提示词 / 模型 / 内容模板配置
│   └── upload_batch_settings.json # 分组批量上传默认设置
├── docs/                     # 中文说明文档
├── fonts/                    # 字体资源
├── overlays/                 # 粒子叠层视频
└── workspace/                # 本地素材 / 输出目录
```

## BitBrowser 适配方式

浏览器提供方已经支持在 `config/upload_config.json` 里切换，默认就是 `bitbrowser`。

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

如果你本地 BitBrowser 的接口字段不同，优先改这个 JSON，不要先改 Python。

## 2026-03-13 新增：按窗口任务上传

现在总控制台里新增了一个更清晰的主上传入口：`按窗口任务上传`。

它先解决 3 个问题：

1. 今天要上传的是同一个分组里的几号窗口？
2. 还是多个分组，各自有哪些窗口？
3. 还是每个窗口都要单独配置标题 / 可见性 / 分类 / 是否 AI 内容？

对应 3 种范围模式：

- `同一分组的一批窗口`
  直接填：`90,91,92`
- `多个分组，各自列窗口`
  每行写：`tag: 90,91`
- `逐窗口单独配置`
  每行写：`窗口号 | tag | key=value | key=value`

逐窗口模式示例：

```text
90 | 面壁者
91 | 面壁者 | visibility=private | category=Music
95 | 芝加哥蓝调 | title=自定义标题 | altered_content=true
```

支持的覆盖字段包括：

- `visibility`
- `category`
- `made_for_kids`
- `altered_content`
- `scheduled_publish_at`
- `title`
- `description`
- `tag_list`
- `thumbnails`
- `ab_titles`

默认规则在控制台 `上传` 页统一设置：

- 可见性
- 分类
- 是否儿童内容
- 是否 AI / 合成内容
- 是否定时发布
- 定时开始时间
- 定时间隔

单个窗口没有单独写覆盖项时，就使用这组默认规则。
