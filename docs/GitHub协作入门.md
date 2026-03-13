# GitHub 协作入门

这份说明是给“两个或多个人一起维护这个项目”准备的。

## 1) 先做什么

1. 你创建仓库并把代码推上去。
2. 在 GitHub 仓库页面点 `Settings`。
3. 进入 `Collaborators and teams`。
4. 点 `Add people`。
5. 输入你朋友的 GitHub 用户名，邀请他。
6. 他接受邀请后，就能访问这个仓库。

## 2) 你朋友如何拿到代码

第一次使用时：

```bash
git clone https://github.com/你的用户名/你的仓库名.git
cd 你的仓库名
```

以后同步最新代码：

```bash
git pull
```

## 3) 最简单的协作方式

如果只有你和你朋友两个人，而且都信任彼此，可以直接在主分支上提交：

```bash
git pull
git add .
git commit -m "说明这次改了什么"
git push
```

这套方式最省事，但要注意一件事：

- 提交前先 `git pull`

否则很容易把别人的最新修改顶掉。

## 4) 更稳的方式

更推荐每次改动都新建一个分支：

```bash
git checkout -b fix/upload-next-button
git add .
git commit -m "fix upload next button waiting logic"
git push -u origin fix/upload-next-button
```

然后在 GitHub 页面发 `Pull Request`，让另一个人看完再合并。

## 5) 你们两个人怎么分工

建议这样分：

- 你维护真实上传环境、频道配置、比特浏览器适配
- 你朋友维护通用代码、文档、特效、渲染逻辑

不要把这些东西直接推到公开仓库：

- 真实频道映射
- 上传记录
- 成品视频
- 音频素材
- 私人项目素材

## 6) 哪些文件适合公开，哪些不适合

适合公开：

- Python 代码
- 通用说明文档
- 模板配置

不适合公开：

- `upload_records/`
- `workspace/AutoTask/`
- `workspace/music/`
- `workspace/base_image/`
- 你真实使用的 `channel_mapping.json` 内容

## 7) 如果你完全不懂 Git，先记住这 4 条

1. `git clone` = 把远程仓库下载到本地
2. `git pull` = 拉最新代码
3. `git add` + `git commit` = 记录这次修改
4. `git push` = 把本地修改传到 GitHub
