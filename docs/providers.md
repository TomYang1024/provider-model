# Provider 兼容性与验证矩阵

以下内容于 **2026-11-05** 对照各 Provider 官方 API 文档核实：

| Provider | 注册名 | 官方文档 |
|---|---|---|
| DeepSeek | `deepseek` | <https://api-docs.deepseek.com/quick_start/pricing>、<https://api-docs.deepseek.com/guides/thinking_mode> |
| 智谱 GLM | `glm` | <https://docs.bigmodel.cn/api-reference/模型-api/对话补全> |
| 月之暗面 Kimi | `kimi` | <https://platform.kimi.com/docs/api/chat> |
| 阿里云 Qwen | `qwen` | <https://help.aliyun.com/zh/model-studio/compatibility-of-openai-with-dashscope>、<https://help.aliyun.com/zh/model-studio/deep-thinking> |

**状态图例：** ✅ 官方文档明确说明 · ⚠️ 文档未说明 / 属于假设（配置中标记为 `verification="unverified"`）·
🧪 已通过真实 API 测试。**目前没有任何一项是 🧪**：开发过程中没有可用的 API Key。
请在配置密钥后运行 `pytest -m integration` 进行确认，然后在 TOML 中把对应项设置为 `verification = "tested"`。

所有内置适配器都走同一条 OpenAI 兼容 Chat Completions 链路，下表中的差异通过各 Provider 的少量钩子处理。

## 接入地址与模型（内置默认值，可通过 TOML 覆盖）

| | Base URL | 默认模型目录 |
|---|---|---|
| DeepSeek | `https://api.deepseek.com` ✅ | `deepseek-flash`、`deepseek-v4-pro`（1M 上下文）✅ |
| GLM | `https://open.bigmodel.cn/api/paas/v4/` ✅ | `glm-5.3`（始终思考）、`glm-5.2`、`glm-5.1`、`glm-5`、`glm-5-turbo`、`glm-4.7`、`glm-4.7-flash`、`glm-4.6`、`glm-4.5-air`、`glm-4.6v`（视觉）✅ |
| Kimi | `https://api.moonshot.cn/v1` ✅ | `kimi-k3.1-pro`、`kimi-k3.1-pro-highspeed`、`kimi-k3`、`kimi-k2.7-code`、`kimi-k2.6` ✅ |
| Qwen | `https://dashscope.aliyuncs.com/compatible-mode/v1` ✅（旧域名，仍可用；阿里云推荐使用工作空间域名 `https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1`，通过 `QWEN_BASE_URL` 设置；API Key 与地域绑定） | `qwen3.8-max`、`qwen3.7-plus`、`qwen3.5-flash`、`qwen-plus`、`qwen-flash`、`qwen-max` ✅ |

`strict_models` 默认关闭：未列出的模型 ID 也会被接受，使用该 Provider 的 `default_capabilities`，
并在指标中统一标记为 `_other`，以控制指标基数。

## 能力矩阵

| 能力 | DeepSeek | GLM | Kimi | Qwen |
|---|---|---|---|---|
| 流式输出（SSE，以 `[DONE]` 结束） | ✅ | ✅ | ✅ | ✅ |
| Tool Calling | ✅ | ✅ | ✅ | ✅（文档明确 `qwen-plus`、`qwen-max` 支持） |
| `tool_choice` | `auto`/`none` ✅；`required` ⚠️（假设支持） | **仅 `auto`** ✅（SDK 会拒绝强制工具选择） | `auto`/`none`/`required`/指定函数 ✅ | 假设仅 `auto` ⚠️（其他取值文档未说明，SDK 拒绝强制选择） |
| 推理内容输出 | `reasoning_content` ✅ | `reasoning_content` ✅ | `reasoning_content` ✅ | `reasoning_content` ✅ |
| 推理开关 | `thinking:{type}`（默认开启）✅ | `thinking:{type}`（默认开启，GLM-4.5 及以上）✅；glm-5.3 始终开启 | `kimi-k2.6`：`thinking:{type}` ✅；K3 系列：始终开启 ✅；k2.7-code：必须开启 ✅ | `enable_thinking` ✅（默认值因模型而异） |
| 推理深度 / 预算 | `reasoning_effort` low/high/max ✅ | — | K3：`reasoning_effort` low/high/max ✅ | `thinking_budget` ✅ |
| 历史消息回传 `reasoning_content` | **请求带 `tools` 时必须回传**（否则 400）✅ → `send_reasoning_content=true` | 仅 `clear_thinking=false` 时需要 ✅ → 默认关闭 | 必须原样回传 ✅ → `true` | 文档未说明 ⚠️ → 关闭 |
| 非流式思考 | ✅ | ✅ | ✅ | 商业版 ✅；部分开源模型仅支持流式（返回 400 → `InvalidRequestError`） |
| 流式 Usage | `stream_options.include_usage` ⚠️（当前文档页未提及，SDK 仍会发送） | chunk 顶层 `usage` 字段 ✅，`stream_options` 未在文档中说明 → 不发送 | 必须设置 `stream_options.include_usage` ✅；在 `choices: []` 的最后一个 chunk 中返回 | `stream_options.include_usage` ✅ |
| 缓存 Token 统计 | `prompt_cache_hit_tokens` ✅ | ⚠️ | `prompt_tokens_details.cached_tokens` ✅ | ⚠️ |
| JSON 模式 | ✅ | ⚠️ | ⚠️ | ⚠️ |
| 图片输入 | ⚠️ | `glm-4.6v` 等 ✅ | ⚠️ | ⚠️（VL 模型不在默认目录中） |

`ReasoningConfig` 的映射规则：

| `reasoning=` | DeepSeek / GLM / Kimi | Qwen |
|---|---|---|
| `None` | 不发送任何参数（使用 Provider 默认值） | 不发送 |
| `{"enabled": true/false}` | `thinking: {"type": "enabled"/"disabled"}`（始终思考的模型不发送该参数；对这类模型关闭推理会抛出 `UnsupportedCapabilityError`） | `enable_thinking: true/false` |
| `effort` | `reasoning_effort`（DeepSeek、Kimi；GLM 抛出异常） | 抛出异常 |
| `budget_tokens` | 抛出异常 | `thinking_budget` |

## 异常映射

先匹配通用规则，再匹配 Provider 业务错误码（来自 `error.code` / `error.type`）。
重试只发生在产生任何输出之前，最多 `max_retries` 次（≤5），流式输出开始后绝不重试。

| 条件 | SDK 异常 | `error_code` | 可重试 |
|---|---|---|---|
| 401/403 | `AuthenticationError` | `authentication_failed` | 否 |
| 402，或余额 / 配额相关文本（DeepSeek 余额不足、GLM `1113`、Kimi `exceeded_current_quota_error`、Qwen `Arrearage`） | `RateLimitError` | `insufficient_quota` | 否 |
| 429（GLM `1302`/`1303`、Qwen `Throttling*`） | `RateLimitError` | `rate_limited` | 是 |
| 400 且包含上下文超长文本（GLM `1261`） | `ContextLengthExceededError` | `context_length_exceeded` | 否 |
| 内容安全拦截（GLM `1301`、Kimi `content_filter`、Qwen `data_inspection_failed`） | `InvalidRequestError` | `content_filtered` | 否 |
| 其他 4xx | `InvalidRequestError` | `invalid_request` / `not_found` | 否 |
| 5xx（Kimi `engine_overloaded_error`、GLM `1305`） | `ProviderServerError` | `provider_server_error` | 是 |
| 传输层超时 / SDK 截止时间 | `TimeoutError` | `timeout` / `deadline_exceeded` | 是（已有输出时为否） |
| 连接失败 | `ConnectionError` | `connection_failed` | 是 |
| 流内错误事件 | `ProviderServerError` | `stream_error` | 否 |

GLM / Kimi / Qwen 的业务错误码来自其公开错误码表，在真实调用验证之前均属于 ⚠️；
未知错误码会回退到按 HTTP 状态码映射的规则。

## 重新验证流程

1. 配置密钥后运行 `pytest -m integration`（每个 Provider 覆盖：普通对话、流式 Usage、流式工具调用、提前关闭）。
   可通过 `LIVE_<PROVIDER>_MODEL` 指定测试模型。
2. 在 TOML 覆盖配置或 `provider_sdk/config.py`（`BUILTIN_PROVIDERS`）中更新 `verification` 和能力声明。
