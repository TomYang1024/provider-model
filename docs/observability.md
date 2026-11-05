# 可观测性

## Trace

属性命名遵循 OpenTelemetry GenAI 语义规范，**锁定 `opentelemetry-semantic-conventions==0.66b2`
（schema `https://opentelemetry.io/schemas/1.44.0`）**。GenAI 规范仍处于孵化阶段，因此属性名被复制到
`provider_sdk/observability/semconv.py` 中，并由单元测试与已安装的规范包逐一比对。

```
[网关]  POST /v1/stream                    SERVER   （提取 W3C traceparent）
  └─ chat {model}                          CLIENT   SDK 请求 Span
       └─ provider_sdk.adapter {provider}  INTERNAL 适配器 Span
            └─ POST                        CLIENT   HTTP Span（响应体关闭时结束）
```

| 属性 | 示例 |
|---|---|
| `gen_ai.operation.name` | `chat` |
| `gen_ai.provider.name` | `deepseek`、`zhipu`、`moonshot`、`alibaba.dashscope` |
| `gen_ai.request.model` / `gen_ai.response.model` | `deepseek-flash` / `DeepSeek-V4.1-Flash-1017` |
| `gen_ai.request.{max_tokens,temperature,top_p,seed,stream}` | 与请求一致 |
| `gen_ai.response.{id,finish_reasons}` | |
| `gen_ai.usage.{input_tokens,output_tokens}`、`gen_ai.usage.cache_read.input_tokens`、`gen_ai.usage.reasoning.output_tokens` | Provider 返回值 |
| `gen_ai.response.time_to_first_chunk`（秒）、`app.llm.ttft_ms` | 流式请求 |
| `server.address`、`server.port` | |
| `app.llm.request_id`、`app.llm.provider`、`app.llm.operation`、`app.llm.stream` | |
| `app.llm.outcome` | `success` · `error` · `timeout` · `cancelled` · `closed_early` |
| `app.llm.usage_source`、`app.llm.stream.events`、`app.llm.partial_output` | |
| `error.type`、`app.llm.error_code`、`app.llm.retryable` | 失败时 |

Span 事件：`app.llm.stream.connected`、`app.llm.first_token`、`exception`。

状态规则：只有 `success` 才是 `OK`；`error` / `timeout` 为 `ERROR`；`cancelled` 和 `closed_early`
保持 `UNSET`，由 `app.llm.outcome` 和 `error.type` 说明原因：它们不是 Provider 故障，但明确**不是成功**。

**默认不写入 Span 的内容：** Prompt、模型输出、工具参数、请求头、API Key。
设置 `PROVIDER_SDK_CAPTURE_CONTENT=true` 后，会写入脱敏并截断的 `gen_ai.input.messages` /
`gen_ai.output.messages`。`url.full` 属性会去掉查询字符串。

**上下文传播：** SDK 继承调用方的当前上下文；除非为某个 Provider 设置
`propagate_trace_context = true`，否则不会向 Provider 发送 `traceparent`。

### 导出链路可靠性

`configure_telemetry()`（需安装 `otel` 扩展）使用 `BatchSpanProcessor`：有界队列（`max_queue_size=2048`，
溢出即丢弃）、后台线程导出、采样器来自 `OTEL_TRACES_SAMPLER[_ARG]`。`shutdown(timeout_s)` 先 Flush
再关闭，并在工作线程中执行，有严格的时间上限。测试证明：Collector 不可达时调用既不会失败也不会变慢，
停机耗时有界。每个进程配置自己的 Provider；通过增加容器 / 进程扩容，借助 `trace_id` 以及
`service.instance.id` / `OTEL_RESOURCE_ATTRIBUTES` 资源属性进行关联。

## Metrics

| 指标 | 类型 | 单位 |
|---|---|---|
| `gen_ai.client.operation.duration` | 直方图 | s |
| `gen_ai.client.token.usage`（`gen_ai.token.type`=input/output；仅统计 Provider 返回值） | 直方图 | {token} |
| `gen_ai.client.operation.time_to_first_chunk` | 直方图 | s |
| `app.llm.requests` | 计数器 | {request} |
| `app.llm.errors` | 计数器 | {error} |
| `app.llm.timeouts` | 计数器 | {timeout} |
| `app.llm.active_streams` | 加减计数器 | {stream} |

只允许低基数标签：provider → `app.llm.provider` + `gen_ai.provider.name`；model → `gen_ai.request.model`
（未登记的模型 ID → `_other`）；operation → `app.llm.operation` + `gen_ai.operation.name`；
status → `app.llm.status`；error_type → `error.type`。trace_id、request_id、user_id、Prompt
绝不会作为标签，测试会断言这一点。

## 日志（structlog，JSON）

| 事件 | 触发时机 | 级别 |
|---|---|---|
| `llm.request` | 调用开始（按采样） | info |
| `llm.response` | 非流式调用成功（按采样） | info |
| `llm.stream.completed` | 流式调用成功（按采样） | info |
| `llm.stream.partial` | 调用方提前关闭流 | warning |
| `llm.stream.cancelled` | 流被取消（客户端断开、停机） | warning |
| `llm.error` | 任何失败 / 超时 / 非流式调用被取消 | error/warning |

公共字段：`timestamp`、`level`、`service`、`trace_id`、`span_id`、`request_id`、`provider`、`model`、
`operation`、`status`、`duration_ms`、`ttft_ms`、`input_tokens`、`output_tokens`、`reasoning_tokens`、
`usage_source`、`finish_reason`、`error_type`、`error_code`、`retryable`、`stream_events`、
`partial_output`、`user_id`、`tenant_id`。

日志模式（`PROVIDER_SDK_LOG_MODE`）：

* `metadata_only`（默认）：不记录任何内容。
* `redacted`：记录 `input` / `output`，清除密钥与个人信息（邮箱、手机号、身份证号、银行卡号），
  并按 `PROVIDER_SDK_LOG_MAX_CHARS` 截断。
* `full`：必须同时设置 `PROVIDER_SDK_ALLOW_FULL_LOGGING=true`，否则自动降级为 `redacted` 并输出警告。
  即使在 full 模式下，凭证仍会被清除，长度限制依然生效。

安全与性能：不逐 Token 记录日志；成功日志按采样记录（错误总是记录）；日志器异常会被吞掉并计数。
`configure_logging()` 通过**有界**非阻塞队列写入 stdout，丢弃的记录数可通过 `handle.dropped` 查看；
标准库 / uvicorn 的日志也会渲染为同样的 JSON 格式。
