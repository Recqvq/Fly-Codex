# Fly-Codex

把本机 `codex` CLI 直接桥接到飞书群聊/私聊，让不同飞书群映射到不同项目路径，并在同一路径下持续复用 Codex 上下文。

## 功能

- 多项目路由：通过 `routes.json` 维护 `chat_id -> workdir`
- 连续上下文：一个群持续复用一个 Codex 会话
- 图片与文件输入：支持飞书图片、文件下载后交给 Codex 处理
- 输入拼包：图片/文件可在短时间内等待文字说明再合并执行
- 任务控制：支持 `/status`、`/usage`、`/lastcmd`、`/interrupt`、`/clearqueue`、`/send`、`/dropinput`
- 结果回传：支持图片、文档等结果文件自动回传到飞书

## 依赖

在项目目录下使用 `uv`：

```bash
uv sync
```

如果还没有环境，可以先：

```bash
uv init
uv sync
```

## 飞书配置

在 `config.py` 中填写飞书应用信息：

```python
APP_ID = "cli_xxx"
APP_SECRET = "xxx"
```

## 路由配置

复制模板后编辑：

```bash
cp routes.example.json routes.json
```

示例：

```json
{
  "defaults": {
    "codex_bin": "codex",
    "codex_args": [],
    "auto_send_recent_artifacts": true,
    "max_auto_artifacts": 3
  },
  "routes": [
    {
      "name": "demo-project",
      "chat_id": "oc_xxx",
      "workdir": "/absolute/path/to/project",
      "allowed_senders": []
    }
  ]
}
```

## 启动

```bash
uv run python codex-feishu-server.py
```

## 常用命令

- `/help`：查看帮助
- `/status`：查看当前群状态、排队情况、待补充输入
- `/usage`：查看当前上下文与当前项目累计 token 用量
- `/lastcmd`：查看上一条成功执行的 shell 命令
- `/interrupt`：中断当前正在运行的任务
- `/clearqueue`：清空当前群里排队但未开始的消息
- `/send`：立即提交当前待补充的图文输入
- `/dropinput`：丢弃当前待补充的图文输入
- `/new` / `/reset`：重开当前群的 Codex 上下文

## 使用方式

- 纯文本消息：直接作为 Codex 任务执行
- 图片或文件：先进入短暂拼包窗口，等待你补文字说明后再合并执行
- 同群多条消息：自动排队顺序执行
- 不同群：可并行处理不同项目

## 会话存储

默认会把会话状态存到：

- `./.fly-codex-sessions.json`

旧版本单项目 session 存储：

- `./.codex-feishu-sessions.json`
