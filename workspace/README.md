# workspace 目录说明

- `base_image/`：底图素材目录。按标签分子目录最稳，例如 `base_image/钢琴/`。
- `music/`：音乐素材目录。按标签分目录或统一放都可以。
- `AutoTask/`：渲染输出目录，调度器默认会把成品视频放这里。
- `projects/`：项目资料目录，后续如果你补上传脚本，可以把 `metadata_channels.md`、`images/` 等放在这里。

这几个目录只是当前仓库整理后的默认结构。

推荐做法：

- 公共模板保留在 `scheduler_config.json`
- 当前电脑的真实路径改到 `scheduler_config.local.json`
- 当前电脑的真实 API key 改到 `config/prompt_studio.local.json`
