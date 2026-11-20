# Dual-Plane v4.6 使用说明（满血 4 Carbon High × 单会话守护接管 × 网页会话无痕自净）

## 1. 核心架构一览
- **双通道高智商推理引擎（Dual-Plane v4.6 Clean-Carbon）**：
  - **通道 A：前台 `Gemini.app` 单会话持久守护（`./cc-demo.sh task-mode` · Plane 2）**：
    1. **接入 `Gemini.app` 的 Task 模式（`Spark / Remy`）**：在 `Gemini.app` 新建 **1 个** `Task`，粘贴自动复制的 `dsh_worker_cli.py daemon` 守护口令。底层走 `robinfrontend-pa`（与网页共享消费级 Gemini 订阅额度），多轮工具调用全部复用这 **1 个** Task 容器，不再每次提问新建一条对话。
    2. **接入 `Gemini.app` 的 `Antigravity`（`CodeMode`）或无头 `agy-acp-gateway`**：`/Applications/Gemini.app/Contents/Helpers/agy` 真正驱动的是 `Gemini.app` 右上角的 **`Antigravity`（`CodeMode`）** 面板（走 `daily-cloudcode-pa`、独立 Antigravity 额度池，且 **100% 不写入网页侧边栏**）；把同一条守护口令粘贴进 `Antigravity` 面板，或单独完成一次 `agy` CLI 登录后配合 `agy-acp-gateway`（`18890` 端口）运行，即可彻底隔离 Web 会话与额度。
  - **通道 B：常驻免值守 `DSHWebKitBridge.app`（Plane 1 满血 4 Carbon + 自动无痕删会话）**：
    - 默认主模型（Opus / Sonnet / Default）全线锁定 **`gemini-app-carbon`（`4 Carbon 深度思考 High`）**，支持最长 `6~10 分钟` 单轮深度思考，**彻底取消超时降级 `Medium` / 误封 `Carbon` 的降智逻辑**。
    - **旁路审查请求隔离（即用即焚）**：Claude Code 内部的 `<block>yes/no</block>` Bash 安全审查与退出摘要（Recap）请求自动剥离到一次性无痕旁路，不再撞碎主对话 `web_cursor`，跑完 `<0.5s` 内自动调用网页 `batchexecute` (`GzXR5e`) 从侧边栏删除。
    - **全生命周期网页会话自净**：会话发生 192K 轮转、闲置超过 15 分钟、网关退出/重启，或执行 `./cc-demo.sh clean-chats` 时，自动批量清理网关产生的网页侧边栏对话。

## 2. 常用命令
```bash
# 1. 默认启动（满血 4 Carbon High + 网页会话自动无痕自净）：
./cc-demo.sh

# 2. 切换为 Gemini.app 前台单会话持久守护（自动复制守护口令，可粘贴进 Task 或 Antigravity 面板）：
./cc-demo.sh task-mode

# 3. 立即一键清空网关在网页侧边栏产生的所有历史/残留对话：
./cc-demo.sh clean-chats

# 4. 查看网关状态 / 运行 6 阶段全自动构建与压测验收：
./cc-demo.sh status
./cc-demo.sh workflow
```

## 3. 添加自定义免公网本地 MCP 服务
直接编辑 `gemini_local_mcp_config.json`（或在任意目标工程根目录创建 `.mcp.json`），在 `mcpServers` 下添加标准 `stdio` 命令即可，网关会自动发现并注入纯 Web 推理工具目录。
