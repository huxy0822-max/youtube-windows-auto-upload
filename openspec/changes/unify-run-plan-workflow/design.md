# Design: unify-run-plan-workflow

## 1. 目标架构

重构后保留 5 层：

1. `UI Layer`
2. `Plan Layer`
3. `Service Layer`
4. `Artifact Layer`
5. `Infra Layer`

## 2. 模块拆分

### 2.1 UI Layer

建议保留：

- `dashboard.py`
- `dashboard_app.py`

但 UI 只负责：

- 表单收集
- 窗口任务编辑
- 进度展示
- 日志展示

UI 不再负责：

- 拼子进程命令
- 直接做路径 fallback
- 直接决定上传目录扫描逻辑

### 2.2 Plan Layer

新增目标：

- `core/run_plan.py`
- `core/task_allocator.py`
- `core/path_policy.py`

职责：

- 解析本次勾选模块
- 解析窗口任务
- 解析路径优先级
- 绑定窗口到容器
- 分配音频、图片、已有视频、已有 metadata

`RunPlan` 是系统唯一输入。

## 3. RunPlan 数据结构

### 3.1 顶层

```json
{
  "run_id": "20260316_230000_0315",
  "date_mmdd": "0315",
  "modules": {
    "metadata": true,
    "render": true,
    "upload": true
  },
  "defaults": {},
  "tasks": []
}
```

### 3.2 Task

每个任务必须显式包含：

- `tag`
- `serial`
- `container_code`
- `channel_name`
- `source_scope`
- `audio_path`
- `image_path`
- `video_path`
- `metadata_output_dir`
- `video_output_dir`
- `thumbnail_output_dir`
- `upload_options`

## 4. Artifact 目录结构

建议新增：

```text
data/runs/<run_id>/
  run_plan.json
  metadata/
    <serial>.json
  thumbnails/
    <serial>.png
  videos/
    <serial>.mp4
  upload_manifest.json
  upload_report.json
```

规则：

- Metadata 只写 `metadata/`
- Render 只写 `videos/`
- Upload 只读 `run_plan.json + metadata + videos`

## 5. 三个服务

### 5.1 MetadataService

输入：

- `RunPlan.task`
- Prompt preset
- Content template

输出：

- `title`
- `description`
- `tag_list`
- `thumbnail_path`
- `ab_titles`

约束：

- 一任务一份产物
- 不允许一组窗口默认共用同一条标题

### 5.2 RenderService

输入：

- `audio_path`
- `image_path`
- `visual_profile`
- `video_output_path`

输出：

- `video_path`
- `duration`
- `effect_desc`

约束：

- 只认显式素材路径
- 不负责文案生成
- 不负责上传

### 5.3 UploadService

输入：

- `container_code`
- `serial`
- `video_path`
- `thumbnail_path`
- `title`
- `description`
- `tag_list`
- `upload_options`

输出：

- `upload_result`
- `video_url`
- `studio_status`

约束：

- 不再重新按 tag 扫描目录
- 不再重新猜 manifest 来源

## 6. BrowserRegistry 设计

现有问题是分组查询、窗口计划、channel mapping 三套来源没有明确优先级。

重构后统一成：

1. `RunPlan.task.container_code`
2. `channel_mapping.json`
3. 实时 BitBrowser 列表

说明：

- UI 构建任务时尽量解析并写入 `container_code`
- 上传时如果实时分组列表失败，仍能按 `container_code` 继续
- tag 分组列表只用于 UI 选窗，不再作为上传硬依赖

## 7. 路径策略

### 7.1 长期设置

长期设置里保留：

- `metadata_root`
- `music_root`
- `image_root`
- `video_output_root`
- `used_media_root`
- `group_bindings`

### 7.2 运行时覆盖

每个任务允许：

- `source_override`
- `metadata_output_override`
- `video_output_override`

最终路径优先级：

1. 任务覆盖
2. 分组绑定
3. 全局默认

## 8. UI 重构思路

### 8.1 快捷开始

快捷开始只负责：

- 选择模块
- 输入日期
- 预览计划
- 路径检查
- 开始执行

### 8.2 上传页

上传页只负责：

- 选分组
- 加窗口任务
- 编辑默认上传规则
- 编辑单窗口覆盖

### 8.3 路径页

路径页只负责：

- 全局根目录
- 分组绑定目录
- 清理策略

### 8.4 提示词页

提示词页只负责模板管理，不直接决定运行状态。

## 9. 迁移策略

### Phase 1

引入 `RunPlan` 和 artifact 目录，但旧 UI 暂时不删。

### Phase 2

把文案生成、渲染、上传改成新服务，旧逻辑仍保留兼容。

### Phase 3

让 `dashboard_app.py` 改为只调服务，不再自己编排 fallback。

### Phase 4

清理旧兼容入口、重复函数和历史状态文件依赖。

