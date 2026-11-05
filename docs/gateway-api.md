# 网关 HTTP 接口

认证：`Authorization: Bearer <token>` 或 `X-API-Key: <token>`（令牌来自 `GATEWAY_API_KEYS` /
`GATEWAY_API_KEYS_FILE`，使用常量时间比较）。未配置令牌时网关拒绝启动，除非显式设置
`GATEWAY_AUTH_DISABLED=true`。

可选请求头：`traceparent` / `tracestate`（W3C 标准，所有 Span 都会延续该上下文）、
`X-Request-ID`（格式 `[A-Za-z0-9._:-]{1,128}`，会在响应中回传；缺省时自动生成）。

## `POST /v1/chat` → `ChatResponse`

```json
{"provider": "deepseek", "model": "deepseek-flash", "prompt": "你好", "system": "你是专业助手",
 "temperature": 0.3, "max_tokens": 512, "reasoning": {"enabled": false},
 "context": {"user_id": "u1", "tenant_id": "t1"}}
```

## `POST /v1/messages` → `ChatResponse`

```json
{"provider": "kimi", "model": "kimi-k2.6",
 "messages": [{"role": "user", "content": "北京天气?"}],
 "tools": [{"name": "get_weather", "parameters": {"type": "object",
            "properties": {"city": {"type": "string"}}}}],
 "tool_choice": "auto"}
```

公共可选字段：`temperature`、`top_p`、`max_tokens`、`stop`、`seed`、`response_format`
（`{"type": "json_object"}`）、`reasoning`（`{enabled, effort, budget_tokens}`）、`provider_options`
（可通过 `GATEWAY_ALLOW_PROVIDER_OPTIONS=false` 禁用；不能覆盖标准参数）、`timeout`（上限分别为
`GATEWAY_MAX_REQUEST_TIMEOUT` / `GATEWAY_MAX_STREAM_TIMEOUT`）、`context`。

响应示例（节选）：

```json
{"id": "...", "request_id": "req_...", "provider": "deepseek", "model": "deepseek-flash",
 "response_model": "...", "finish_reason": "stop",
 "message": {"role": "assistant", "content": "...", "tool_calls": null, "reasoning_content": null},
 "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15, "source": "provider"},
 "latency_ms": 812.4, "trace": {"trace_id": "...", "span_id": "...", "sampled": true}}
```

## `POST /v1/stream` → `text/event-stream`

请求体与 `/v1/messages` 相同。网关会在返回响应**之前**先连接 Provider，因此流开始前的失败会以
普通 JSON 错误和对应 HTTP 状态码返回。连接成功后输出如下帧：

```
id: 0
event: message_start
data: {"seq":0,"type":"message_start","request_id":"req_...","provider":"qwen","model":"qwen-plus","trace":{...}}

event: text_delta
data: {"seq":1,"type":"text_delta","text":"Py"}

: ping                                   （空闲时每 GATEWAY_SSE_PING_INTERVAL 秒发送一次）

event: message_end
data: {"seq":9,"type":"message_end","finish_reason":"stop","response":{...ChatResponse...}}
```

流中途失败时，会发送终止帧 `event: error`，其中的 `error` 对象与 JSON 错误相同（`retryable` 为
`false`）。客户端断开时，即使 Provider 当前处于静默（例如长时间推理），网关也会立即取消上游流，
并记录为 `cancelled`。

## 错误格式

```json
{"error": {"type": "rate_limit", "code": "rate_limited", "message": "deepseek returned HTTP 429: ...",
           "provider": "deepseek", "model": "deepseek-flash", "retryable": true,
           "status_code": 429, "request_id": "req_..."}}
```

| HTTP | 场景 |
|---|---|
| 400 | 请求非法、能力不支持、上下文超长、内容安全拦截 |
| 401 | 网关令牌缺失 / 无效 |
| 404 | Provider 不存在或已禁用 |
| 413 | 请求体超过 `GATEWAY_MAX_BODY_BYTES` |
| 422 | 请求结构校验失败（绝不回显输入内容） |
| 429 | Provider 限流 / 配额不足（可重试时返回 `Retry-After`） |
| 502 | Provider 认证失败、Provider 5xx、连接失败 |
| 503 | 正在停机（draining）或超过 `GATEWAY_MAX_CONCURRENCY` |
| 504 | 超时 / 超过截止时间 |

Provider 的 API Key 绝不会出现在响应或日志中。

## 健康检查

* `GET /health/live` → 进程存活即返回 200，绝不依赖 Provider 或 Collector。
* `GET /health/ready` → 初始化完成返回 200；启动中或停机中返回 503。
* `GET /v1/providers`（需认证）→ 已启用的 Provider 及其配置的模型 ID。
