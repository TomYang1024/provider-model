# Provider SDK

面向 **DeepSeek、GLM（智谱）、Kimi（月之暗面）、Qwen（阿里云百炼）** 的生产级异步 Python SDK，
内置 OpenTelemetry Trace / Metrics、structlog 标准化输入输出日志，并提供可选的 FastAPI
HTTP/SSE 网关及 Docker 部署方案。

```
应用 / Agent ──► provider_sdk（Python 库）              ┐
其他语言服务 ──► provider_gateway（HTTP/SSE）──────────► │ ProviderSDK ─► 请求校验 ─► 可观测性中间件
                                                         │   ─► Provider 注册表 ─► 适配器 ─► LiteLLM
                                                         ┘      ├─ DeepSeek  ├─ GLM  ├─ Kimi  ├─ Qwen
                                                                └─ litellm（Anthropic / Gemini / Bedrock …）
```

* 统一接口：`chat()`、`messages()`、`stream()` / `open_stream()`；三个入口在适配器层汇聚为
  同一条调用路径，统一经由 [LiteLLM](https://docs.litellm.ai) 发起请求。
* 支持 Tool Calling（SDK 只负责协议，**不执行**工具）、Reasoning、带来源标记的 Token Usage
  （`provider` / `estimated` / `unknown`）、多模态 ContentPart。
* 统一异常体系：带 `retryable`、安全错误摘要，并保留异常链。
* OpenTelemetry 是唯一 Trace 标准：遵循 GenAI 语义规范（锁定 schema 1.44.0），W3C Trace
  Context，import 无副作用，禁用导出时正常工作。
* Docker：多阶段构建、非 root 用户、依赖哈希锁定、可复现构建；Compose 编排 OTel Collector +
  Jaeger；支持 SIGTERM 优雅停机。

## 安装

```bash
uv sync                                  # 开发环境（Python 3.11+）
pip install provider-sdk                 # 仅 SDK：litellm、openai、httpx2、pydantic、structlog、otel-api
pip install "provider-sdk[otel]"         # 追加 OpenTelemetry SDK / OTLP 导出器初始化工具
pip install "provider-sdk[gateway]"      # 追加 FastAPI 网关
```

精确版本锁定在 `uv.lock`，`pyproject.toml` 中只声明有上界的兼容范围。
注意：`openai>=3` 底层使用 **`httpx2`**（而非 `httpx`），因此 SDK 的传输层直接基于 `httpx2`。
`litellm` 内部接口变化频繁，版本锁定在单个 minor 内；升级后请运行 `make test`。

## 快速开始

```python
import asyncio
from provider_sdk import Message, ProviderSDK


async def main() -> None:
    async with ProviderSDK.from_env() as sdk:  # 从 DEEPSEEK_API_KEY 等环境变量读取密钥
        r = await sdk.chat(
            provider="deepseek", model="deepseek-flash", prompt="你好", system="你是专业助手"
        )
        print(r.text, r.usage.source, r.trace.trace_id)

        r = await sdk.messages(
            provider="kimi",
            model="kimi-k2.6",
            messages=[
                Message.system("你是编程助手"),
                Message.user("什么是 Python？"),
            ],
        )

        async with sdk.open_stream(
            provider="qwen", model="qwen-plus", messages=[Message.user("介绍一下 Python")]
        ) as stream:
            async for event in stream:
                if event.type == "text_delta":
                    print(event.text, end="")


asyncio.run(main())
```

更多示例见 [`examples/`](examples)：工具调用循环、流式输出、Telemetry 初始化、网关客户端、TOML 配置。

## Agent 工具调用示例

仓库内的 [`agent-tools`](packages/agent-tools/README.md) 是独立的工具调用基础设施，负责工具的
注册、路由、执行和治理。下面的 `agent_loop.py` 将 Provider SDK 与工具执行器连接起来，演示
“模型请求工具 → 执行工具 → 结果回填 → 模型回答”。

以下命令在仓库根目录运行。首次使用请先执行 `uv sync`；示例依赖 OpenTelemetry SDK，MCP 演示
还需要 MCP 可选依赖，这些依赖包含在本仓库默认开发环境中。

### 模拟模型：无需 API Key

```bash
uv run python packages/agent-tools/examples/agent_loop.py --verbose
```

默认使用预设的模拟模型响应，执行 Python 天气和记事工具，用于检查调用链和日志。天气数据是
模拟的；记事工具不会实际保存数据。

### 真实模型调用 Python 工具

先在当前终端设置 `DEEPSEEK_API_KEY`，然后运行：

```bash
uv run python packages/agent-tools/examples/agent_loop.py --real --verbose
```

默认使用 `deepseek` / `deepseek-flash`。可通过 `--provider`、`--model` 和 `--prompt` 指定
服务商、模型和请求内容；模型需存在于 SDK 的配置中，且账户有调用权限：

```bash
uv run python packages/agent-tools/examples/agent_loop.py \
  --real --verbose --provider deepseek --model deepseek-flash \
  --prompt "北京和上海今天天气怎么样？"
```

`--real` 只表示调用真实模型；示例中的天气和记事工具仍是模拟实现。真实模型请求可能产生费用。
API Key 应通过环境变量或项目支持的密钥文件提供，不要写进代码或提交到版本控制。

### 真实模型调用本地 MCP 工具

配置 `DEEPSEEK_API_KEY` 后运行：

```bash
uv run python packages/agent-tools/examples/agent_loop.py \
  --real --mcp-demo --verbose \
  --prompt "请调用 MCP 回显工具，处理文本：你好，Agent"
```

`--mcp-demo` 启动本地 stdio MCP 回显服务，将 `echo` 加入工具白名单，并在运行结束时关闭服务。
日志中的 `kind=mcp` 和 `status=success` 表示对应 MCP 工具调用成功；是否调用工具由模型决定。
这是本地演示服务，不连接业务 MCP 服务端。当前 `echo` 工具会在输入文本前后添加 `echo` 和
`123`。不加 `--real` 可用模拟模型验证同一条 MCP 调用链，
无需 API Key：

```bash
uv run python packages/agent-tools/examples/agent_loop.py --mcp-demo --verbose
```

### 会话、追踪与日志

示例每次运行自动生成 `session_id`，也可用 `--session-id my-session-001` 指定。一次运行内模型
调用和工具调用共享 `trace_id`，每次操作有各自的 `span_id`。示例中的 Trace 用于关联日志，
不会自动导出或持久化；接入追踪平台需要由宿主配置 OpenTelemetry 导出器。

`--verbose` 显示每轮调用、工具参数预览和结果摘要。参数预览经过规则脱敏，但不能保证识别所有
敏感内容，使用共享终端时请避免输入敏感信息。

更多工具配置、HTTP / MCP 接入和测试方法见 [`agent-tools` 文档](packages/agent-tools/README.md)。

### 流式事件协议

```
message_start ( reasoning_delta | text_delta | tool_call_start tool_call_delta* tool_call_end )* usage message_end
```

* `message_start` 只在 Provider 接受请求（收到 HTTP 响应头）之后发出；在此之前的失败直接抛出异常、
  不产生任何事件，调用方可以安全重试。
* 每个 `tool_call_start` 都恰好对应一个 `index` 相同的 `tool_call_end`。
* `message_end` 之前恰好有一个 `usage` 事件（Provider 未返回时 `source` 为 `unknown`）。
* `message_end` 是唯一的成功终态，携带聚合后的完整 `ChatResponse`。
* 失败时抛出 `ProviderError`；一旦已有输出，`retryable` 会被强制置为 `False`。
* 提前 `break` + `aclose()` / 退出 `async with` 记为 `closed_early`；被取消记为 `cancelled`；
  超过截止时间抛出 `TimeoutError`。以上情况都**不会**记为成功。

普通的 `async for … break` **不能**保证立即释放连接，请使用 `async with sdk.open_stream(...)`
或显式调用 `await stream.aclose()`。`sdk.aclose()` 会兜底关闭所有仍未关闭的流。

## 配置

| 变量 | 说明 |
|---|---|
| `DEEPSEEK_API_KEY`、`GLM_API_KEY`（`ZHIPUAI_`）、`KIMI_API_KEY`（`MOONSHOT_`）、`QWEN_API_KEY`（`DASHSCOPE_`） | API Key；`*_FILE` 变体用于读取 Docker Secrets。没有密钥的 Provider 自动禁用。 |
| `<P>_BASE_URL` | 覆盖 Base URL（如 Qwen 工作空间域名或国际站）。 |
| `PROVIDER_SDK_<P>_ENABLED` | 强制启用 / 禁用某个 Provider。 |
| `PROVIDER_SDK_<P>_PROXY`、`PROVIDER_SDK_TRUST_ENV` | 单个 Provider 的出口代理 / 是否遵循 `HTTPS_PROXY`+`NO_PROXY`（默认 true）。 |
| `PROVIDER_SDK_CONFIG_FILE` | TOML 覆盖配置：模型、能力声明、超时、重试、新增 Provider（[示例](examples/providers.example.toml)）。TOML 中禁止写密钥。 |
| `PROVIDER_SDK_LOG_MODE` | `metadata_only`（默认）· `redacted` · `full`（需同时设置 `PROVIDER_SDK_ALLOW_FULL_LOGGING=true`）。 |
| `PROVIDER_SDK_LOG_SAMPLE_RATE`、`PROVIDER_SDK_LOG_MAX_CHARS` | 成功日志采样率（错误日志总是记录）；内容长度上限。 |
| `PROVIDER_SDK_CAPTURE_CONTENT` | 在 Span 上记录脱敏后的输入输出（默认关闭）。 |
| `PROVIDER_SDK_ESTIMATE_USAGE` | Provider 未返回 Usage 时启用启发式估算（`source="estimated"`）。 |

新增 Provider 无需修改 Client：注册一个 `ProviderConfig`（TOML 或 `sdk.registry.register(...)`），
按协议选择适配器类型，或通过 `registry.register_adapter_type(...)` 注册自定义的
`BaseProviderAdapter` 子类。

### LiteLLM 接入方式

所有适配器都通过 `litellm.acompletion` 发起调用，分两条路由：

| 适配器类型 | 适用 Provider | HTTP 由谁发出 | HTTP Span / 连接池 / 代理配置 |
|---|---|---|---|
| `deepseek` / `glm` / `kimi` / `qwen` / `openai_compatible` | OpenAI 兼容接口 | SDK 自己的 `httpx2` 客户端（注入 LiteLLM） | 全部生效 |
| `litellm` | LiteLLM 支持的其他 Provider（Anthropic、Gemini、Bedrock、Azure …） | LiteLLM 原生实现 | 不生效（请求级 Span、指标、日志仍有） |

```toml
[providers.claude]
adapter = "litellm"
base_url = "https://api.anthropic.com"
api_key_env = "ANTHROPIC_API_KEY"
adapter_options = { litellm_provider = "anthropic" }
```

LiteLLM 的归一化层有几处与本 SDK 的约定不同，适配器已逐项处理，行为与接入前保持一致：

* Provider 未返回 Usage 时，LiteLLM 会补零或本地估算 —— SDK 仍报告 `usage.source="unknown"`；
* 未知的 `finish_reason` 会被 LiteLLM 改写为 `stop` —— SDK 按 Provider 原始值映射，
  原始值保留在 `raw_finish_reason`；
* 厂商异常会被重新包装 —— SDK 沿异常链取回原始异常，业务错误码分类规则不变；
* LiteLLM 会把环境变量 `OPENAI_ORGANIZATION` 写入请求头、用 600 秒默认值覆盖超时、给流式请求
  自动加上 `stream_options` —— SDK 均已阻止：不向第三方 Provider 泄露 OpenAI 标识，
  超时与 `stream_usage_option` 以 `ProviderConfig` 为准；
* `litellm` 在首个适配器创建时才导入（约 1~2 秒，一次性），导入时不联网。

已知差异：Moonshot 旧版模型放在 `choices[0].usage` 中的流式 Usage 不再解析（记为 `unknown`）。
`litellm` 适配器的请求级 `provider_options` 只有在配置了 `allowed_options` 白名单时才接受。

## Docker

```bash
cp .env.example .env            # 填写 Provider 密钥和 GATEWAY_API_KEYS
docker compose up -d --build --wait
curl -s localhost:8080/health/ready
curl -s localhost:8080/v1/chat -H "Authorization: Bearer $TOKEN" \
     -d '{"provider":"deepseek","model":"deepseek-flash","prompt":"你好"}'
open http://127.0.0.1:16686     # Jaeger UI（仅本机可访问）
```

详见 [docs/deployment.md](docs/deployment.md)（密钥管理、OTLP TLS、生命周期、扩容、镜像扫描）和
[docs/gateway-api.md](docs/gateway-api.md)。

## 测试

| 命令 | 范围 |
|---|---|
| `make test` | 单元测试 + 网关测试 + 进程级端到端测试（HTTP 全部 Mock，无需密钥和外网） |
| `make test-integration` | 调用真实 Provider API（`pytest -m integration`，需要密钥） |
| `make test-container` | 构建镜像并用假 Provider 启动 Compose：镜像加固、Trace 进入 Jaeger、Collector 故障、JSON 日志、流式过程中 SIGTERM |
| `make lint typecheck` | ruff + mypy `--strict` |

## 文档

* [docs/architecture.md](docs/architecture.md)：分层结构、请求生命周期、设计决策
* [docs/providers.md](docs/providers.md)：**Provider 兼容性与验证矩阵**
* [docs/observability.md](docs/observability.md)：Span、属性、指标、日志事件
* [docs/gateway-api.md](docs/gateway-api.md)：HTTP/SSE 接口
* [docs/deployment.md](docs/deployment.md)：Docker / Compose / 生产上线检查清单
