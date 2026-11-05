# 架构设计

```
provider_gateway/            FastAPI 网关（可选；依赖 SDK，SDK 绝不反向依赖它）
  app.py  middleware.py  sse.py  settings.py  schemas.py  __main__.py
provider_sdk/                SDK 核心库
  client.py      ProviderSDK：chat / messages / stream / open_stream / complete / aclose
  stream.py      ChatStream：显式生命周期、截止时间、终态统计
  registry.py    ProviderRegistry：Provider 名称 -> ProviderConfig + 懒加载的适配器
  config.py      SDKConfig：内置模型目录、环境变量、*_FILE 密钥文件、TOML 覆盖
  exceptions.py  ProviderError 异常体系
  transport.py   带 HTTP CLIENT Span 的 httpx2 传输层；代理解析
  usage.py       可选的启发式 Usage 估算
  schemas/       Pydantic v2 数据模型（message、request、response、stream、context、provider）
  adapters/      BaseProviderAdapter -> OpenAICompatibleAdapter -> DeepSeek | GLM | Kimi | Qwen
  observability/ semconv、middleware（CallObserver）、metrics、logging、redaction、setup
```

## 请求生命周期

```
sdk.messages(...)
  └─ _build()：Pydantic 校验（错误信息绝不回显输入内容）      -> InvalidRequestError
  └─ registry.get(provider)                                     -> ProviderNotFoundError
  └─ Instrumentation.begin()：创建 SDK Span "chat {model}"（CLIENT，父级为当前上下文）
       + Adapter Span，输出 llm.request 日志（按采样）
  └─ asyncio.timeout(total)：
       adapter.validate()  在任何 I/O 之前做能力校验            -> UnsupportedCapabilityError
       adapter.build_params()  -> openai.AsyncOpenAI.chat.completions.create()
          └─ TracingTransport：HTTP Span（除非显式开启，否则不发送 traceparent）
       adapter.parse_response() / map_error()
  └─ CallObserver.finish(outcome)：Span 状态与属性、指标、llm.response / llm.error 日志
```

流式请求走同一条链路，但由 `ChatStream` 逐个事件驱动。每次 `__anext__` 都在内部挂载 Span 上下文，
并在返回前卸载，因此上下文 token 永远不会跨越调用方的 `await` 边界。ChatStream 同时负责：
事件协议、截止时间、`closed_early` / `cancelled` / `timeout` / `error` 终态，以及连接释放。

## 设计决策

* **适配器只做协议转换。** Trace、日志、指标都在 `observability.middleware` 中实现；
  适配器不负责路由、计费、限流，也不执行工具。
* **四个 Provider 是 `OpenAICompatibleAdapter` 的平级子类。** 差异都收敛在钩子中：
  `apply_reasoning`、`classify_error`（`ErrorRule` 错误码表），以及配置开关
  （`stream_usage_option`、`send_reasoning_content`）。
* **`ChatStream` 不是异步生成器。** 生成器在 `break` 后要等 GC 才会被终结，期间连接一直泄漏。
  显式的 `aclose()` / `async with` 让资源释放变得确定；SDK 还会跟踪所有打开的流，停机时统一关闭。
* **`message_start` 在 HTTP 响应被接受后才发出。** 调用方（或网关）因此可以把流开始前的失败
  转换为重试或正确的 HTTP 状态码。
* **从不伪造 Usage。** Provider 未返回时标记为 `unknown`；启发式估算需显式开启，并标记为 `estimated`。
* **import 无副作用。** SDK 只依赖 `opentelemetry-api`；初始化工具（`configure_telemetry`、
  `configure_logging`）均需显式调用，网关在 lifespan 中调用。两者都有测试保证。
* **代理处理显式化。** httpx 风格的环境变量代理 *mounts* 会绕过追踪传输层，并且忽略系统代理例外规则，
  因此代理在每个 Provider 初始化时解析一次，并始终经过 `TracingTransport`。
* **面向第三方加固 OpenAI 客户端。** 清除从环境变量读取的 `OPENAI_ORG_ID` / `OPENAI_PROJECT_ID`，
  避免它们被发送给其他 Provider；API Key 总是显式传入，绝不会使用 `OPENAI_API_KEY`。

## 并发模型

每个进程只创建一个 `ProviderSDK`，由所有任务共享。每个 Provider 拥有一个带连接池的
`httpx2.AsyncClient`（`max_connections`、`max_keepalive_connections`）。Client 上没有请求级可变状态，
上下文隔离依赖 contextvars；测试用 50 个并发调用（各自有独立父 Span）验证这一点。网关额外提供
不排队的并发上限（`GATEWAY_MAX_CONCURRENCY`，超出返回 503），过载时快速拒绝而不是让延迟不断增长。
