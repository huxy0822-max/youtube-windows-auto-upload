# Spec: run-plan-orchestration

## Capability

统一本次运行的计划生成、产物目录、模块执行和上传输入，确保文案生成、剪辑、上传三块在单任务和批量任务下都使用同一套显式数据流。

## Requirement 1: 明确生成 RunPlan

系统必须在点击开始前生成一个明确的 `RunPlan`。

### Acceptance Criteria

1. Given 用户已经选择模块、日期、窗口任务和路径覆盖  
   When 用户点击预览或开始  
   Then 系统必须生成包含所有任务、目录、窗口、上传选项的 `RunPlan`

2. Given 一个任务没有足够的输入素材  
   When 生成 `RunPlan`  
   Then 系统必须在计划阶段直接报错，而不是在渲染或上传阶段才发现

3. Given 本次运行是“只上传”  
   When 生成 `RunPlan`  
   Then 每个任务都必须显式绑定到已有视频路径，而不是在上传阶段二次扫描

## Requirement 2: 模块可独立运行

文案生成、剪辑、上传必须能独立运行，也能组合运行。

### Acceptance Criteria

1. Given 只勾选文案生成  
   When 开始执行  
   Then 系统只生成 metadata 产物，不生成视频，不触发上传

2. Given 只勾选剪辑  
   When 开始执行  
   Then 系统只生成视频，不调用 metadata API，不触发上传

3. Given 只勾选上传  
   When 开始执行  
   Then 系统只消费已有视频和已有 metadata 产物，不重新生成文案，不重新剪辑

4. Given 同时勾选三块  
   When 开始执行  
   Then 系统必须按 `Metadata -> Render -> Upload` 的顺序消费同一份 `RunPlan`

## Requirement 3: 任务以窗口任务为最小单位

批量执行必须以窗口任务为核心，而不是以 tag 为核心。

### Acceptance Criteria

1. Given 一个 tag 下有多个窗口  
   When 开始执行  
   Then 每个窗口都必须生成独立任务记录、独立 metadata、独立视频、独立上传输入

2. Given 多个任务共享同一套提示词模板  
   When 生成 metadata  
   Then 每个任务的标题、简介、标签和缩略图仍然必须独立产出

3. Given 多个任务使用同一分组  
   When 构建计划  
   Then 分组只用于辅助选窗，不得作为上传阶段重新查找输入的唯一依据

## Requirement 4: 路径优先级固定

系统必须使用固定的目录优先级解析规则。

### Acceptance Criteria

1. Given 某个任务设置了临时目录覆盖  
   When 生成 `RunPlan`  
   Then 系统必须优先使用任务覆盖目录

2. Given 任务没有临时覆盖，但当前分组设置了绑定目录  
   When 生成 `RunPlan`  
   Then 系统必须使用分组绑定目录

3. Given 任务和分组都没有覆盖  
   When 生成 `RunPlan`  
   Then 系统必须回退到全局默认目录

4. Given 上传阶段开始执行  
   When 读取输入  
   Then 系统不得再次基于 tag 名或目录命名规则重新推断路径

## Requirement 5: 上传只消费显式产物

上传模块必须只依赖 `RunPlan` 和产物目录里的显式文件。

### Acceptance Criteria

1. Given `RunPlan` 中已经明确了 `video_path`、`thumbnail_path`、`container_code`  
   When 开始上传  
   Then 上传模块必须直接使用这些路径和容器信息

2. Given 分组列表接口临时失败  
   When 执行上传  
   Then 系统必须优先使用 `RunPlan` 中已有的窗口信息和本地映射，而不是直接整批失败

3. Given 某个任务缺少缩略图  
   When 执行上传  
   Then 系统必须准确报告该任务缺少的 artifact，而不是上传别的旧缩略图

## Requirement 6: 运行产物可追溯

每次运行都必须生成单独的 artifact 目录，便于审计和复用。

### Acceptance Criteria

1. Given 一次新的运行  
   When `RunPlan` 生成  
   Then 系统必须为本次运行创建独立 `run_id`

2. Given 某个任务成功生成 metadata  
   When 保存产物  
   Then 系统必须把标题、简介、标签、缩略图信息保存到本次运行目录

3. Given 某个任务成功渲染或上传  
   When 保存产物  
   Then 系统必须把成品视频路径和上传报告写入本次运行目录

