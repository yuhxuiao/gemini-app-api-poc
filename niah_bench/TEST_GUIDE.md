# Gemini.app Task (Carbon) 上下文上限与自动压缩水位实测手册

本测试集专门用于在不调用 `grep` 或脚本检索绕过上下文的前提下，精确测量 **Gemini.app Task (Carbon)** 的两项核心指标：
1. **单次上下文读取硬上限**（区分「内置工具 / Local MCP 单次返回通道」与「UI 拖入附件直读通道」两种路径，并定位截断位置）。
2. **多轮连续对话触发云端自动压缩（Auto-Compaction）的精确 Token 水位**（精确到 `50K` 步长）。

## 一、已完成的本机直测基线结论（Task / Carbon 模式）

1. **工具通道（`view_file` / `run_command` / Local MCP `tool_result`）单次硬截断上限：`256,000` 字符（约 `64.8K` Tokens）**
   * 对 `single_turn/niah_100k.txt`（`395,061` 字符，约 `100K` Tokens）不设行号整文件读取时，客户端直接返回硬截断横幅：
     `[TOOL OUTPUT TRUNCATED] Total output: 395061 chars. Showing first 256000 chars below. 139061 chars are hidden.`
   * 位于前 `256,000` 字符内的 3 枚校准针（`ALPHA-05=3104`、`ALPHA-25=5821`、`ALPHA-50=7490`）100% 完整保留且中段零丢失；位于 `297K` 与 `376K` 字符处的 `ALPHA-75`、`ALPHA-95` 被 `256,000` 字符单次上限截断。
   * **网关分片推论**：通过 `dsh_call_tool` 向 `Gemini.app` 喂入超长上下文时，单次 `tool_result` 必须控制在 `240,000` 字符以内（例如分片为多轮/多步读取），否则超过 `256,000` 字符的尾部会被客户端直接裁掉。
2. **多轮累计触发云端自动压缩（Auto-Compaction）实测水位：约 `180K–200K` Tokens**
   * 当单次 `256,000` 字符（约 `64.8K` Tokens）工具输出叠加到已有约 `120K` Tokens 的多轮历史时，累计上下文跨过约 `185K` Tokens 即刻触发服务端自动上下文压缩（将早期轮次折叠为结构化进展摘要）。
   * 本测试集的 `multi_turn/step_01_cum050k.txt` 至 `step_06_cum300k.txt` 每步体积专门校准为 `197.5 KB`（严格小于 `256,000` 字符单次工具截断线），既支持在空白窗口拖入附件测，也支持通过分步工具读取零截断复现压缩水位。

## 二、文件清单

* 单步极限测试集（每个文件在 `5% / 25% / 50% / 75% / 95%` 深度各埋入 1 条 `prism-calibrator` 校准记录，共 5 针）：
  * `single_turn/niah_100k.txt`（`395,061 B`，约 `100K` Tokens，`2,516` 行）
  * `single_turn/niah_200k.txt`（`790,032 B`，约 `200K` Tokens，`5,025` 行）
  * `single_turn/niah_400k.txt`（`1,580,018 B`，约 `400K` Tokens，`10,063` 行）
  * `single_turn/niah_800k.txt`（`3,160,052 B`，约 `800K` Tokens，`20,122` 行）
* 多轮压缩水位探测集（共 6 步，每步约 `50K` Tokens / `197.5 KB`，累计 `50K -> 300K`，每步在 `25%` 与 `75%` 深度各埋 1 针）：
  * `multi_turn/step_01_cum050k.txt` 至 `multi_turn/step_06_cum300k.txt`

## 三、附件直读通道极限复核（测 200K / 400K / 800K 纯模型窗口）

为了绕过工具通道 `256,000` 字符单次返回上限、直接测试云端附件解析管道（`1,048,576` Tokens 模型池），请在 `Gemini.app` 新建 **Task（选 Carbon，不挂载本地文件夹以防自动调用检索工具）**，依次将 `niah_200k.txt`、`niah_400k.txt`、`niah_800k.txt` **直接拖入输入框作为附件**，发送以下防检索提问：

```text
请直接完整阅读附件中的全部日志内容（严禁编写或运行任何 Python/Shell 脚本检索），找出所有 unit=prism-calibrator 的校准检查点记录，按顺序列出每个检查点的名称（如 ALPHA-05 / BETA-05 / GAMMA-05 / OMEGA-05）、drift_val 数值与 codename 代号，并计算这 5 个 drift_val 的总和。
```

拿到回复后，在终端运行自动判分器即可诊断完整保留率或首尾/中段截断位置：

```bash
python3 ./niah_bench/verify_answers.py --mode 400k --reply "粘贴模型回复内容"
```

## 四、空白会话从零复测多轮自动压缩水位（50K 步进）

在 `Gemini.app` 新建一个空白 **Task（选 Carbon）**，按顺序分 6 轮依次拖入（或分轮读取）`multi_turn/step_01_cum050k.txt` 到 `step_06_cum300k.txt`：

* **第 1 轮（累计 ~50K）**：放入 `step_01_cum050k.txt`，发送：
  `请完整阅读这份日志（严禁使用脚本检索），只告诉我日志里出现过哪几个 region 名称即可，先不要列出具体数值。`
  *（设计原理：第 1 轮故意不让模型把 `STEP1-A / STEP1-B` 写进回答正文；一旦后续轮次跨过自动压缩水位把第 1 轮原始日志折叠为摘要，`STEP1-A=1142` 就会从当前上下文消失。）*
* **第 2 轮至第 6 轮（累计 ~100K -> 150K -> 200K -> 250K -> 300K）**：每轮放入下一个 `step_0X` 文件，发送：
  `请完整阅读这份新日志（不要使用脚本），列出新日志的 region；同时仅凭你当前的上下文记忆（不要重新翻阅旧文件），报出第一份日志中 STEP1-A 和 STEP1-B 的 drift_val 与 codename。`
