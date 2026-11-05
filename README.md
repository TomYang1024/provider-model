# Provider SDK

面向 **DeepSeek、GLM（智谱）、Kimi（月之暗面）、Qwen（阿里云百炼）** 的生产级异步 Python SDK，
内置 OpenTelemetry Trace / Metrics、structlog 标准化输入输出日志，并提供可选的 FastAPI
HTTP/SSE 网关及 Docker 部署方案。

```
应用 / Agent ──► provider_sdk（Python 库）              ┐
其他语言服务 ──► provider_gateway（HTTP/SSE）──────────► │ ProviderSDK ─► 请求校验 ─► 可观测性中间件
                                                         │   ─► Provider 注册表 ─► OpenAI 兼容适配器
                                                         ┘      ├─ DeepSeek  ├─ GLM  ├─ Kimi  └─ Qwen
```

* 统一接口：`chat()`、`messages()`、`stream()` / `open_stream()`。
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
pip install provider-sdk                 # 仅 SDK：openai、httpx2、pydantic、structlog、otel-api
pip install "provider-sdk[otel]"         # 追加 OpenTelemetry SDK / OTLP 导出器初始化工具
pip install "provider-sdk[gateway]"      # 追加 FastAPI 网关
```

精确版本锁定在 `uv.lock`，`pyproject.toml` 中只声明有上界的兼容范围。
注意：`openai>=3` 底层使用 **`httpx2`**（而非 `httpx`），因此 SDK 的传输层直接基于 `httpx2`。

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
使用 `adapter="openai_compatible"`，或通过 `registry.register_adapter_type(...)` 注册自定义的
`BaseProviderAdapter` 子类。

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
