# agent-tools

Agent 工具调用基础设施（P0 + P1 + P2 + P3）：工具的定义、注册、发现、执行与治理。
支持本地 Python 函数、HTTP API 和 MCP 服务端提供的工具，三者共用同一套注册、授权、执行、
治理、可观测性和安全机制。

* 与 Provider SDK、Agent Runtime 解耦：核心包只依赖 `pydantic`、`opentelemetry-api`、`structlog`；
* HTTP 工具与 JSON Schema 校验在可选 extra 中：`pip install "agent-tools[http]"`
  （`httpx2`、`jsonschema`）；
* MCP Client 工具适配在可选 extra 中：`pip install "agent-tools[mcp]"`
  （官方 SDK `mcp>=2.3,<2.4`，已在 2.3.0 上验证）；
* 和 `provider-sdk` 的转换在可选模块 `agent_tools.integrations.provider_sdk` 中
  （`pip install "agent-tools[provider-sdk]"`）；
* P3 扩展能力（熔断、人工审批、结果缓存与调用去重、进程隔离、超大输出钩子）**全部默认关闭**，
  见下文“P3 扩展能力”；
* MCP 租户级会话与凭证隔离、MCP Streamable HTTP 代理、分布式限流参考实现（Redis）为 2026-11-10
  新增的**可选**能力，默认关闭，见“MCP 工具”与“限流”；
* 按租户区分的注册表视图、共享状态的结果存储与熔断参考实现（Redis）为 2026-11-10 第二次新增的
  **可选**能力，默认关闭，见“按租户区分的工具目录”与“共享状态的参考实现”；
* 架构说明与实现记录见仓库根目录的 `agent-tools-architecture.md`（§10–§15），逐日进度见
  `docs/progress/`。

## 快速开始

```python
import asyncio

from agent_tools import (
    Principal,
    ToolAnnotations,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
)


async def get_weather(city: str) -> dict[str, str]:
    """查询城市天气。"""
    return {"city": city, "condition": "sunny"}


async def main() -> None:
    registry = ToolRegistry()
    registry.register_function(
        get_weather,
        name="weather.get",
        policy=ToolPolicy(access="public"),  # 访问方式必须显式声明
        annotations=ToolAnnotations(concurrency_safe=True),
    )
    async with ToolExecutor(registry) as executor:
        ctx = ToolContext.with_timeout(10, principal=Principal(user_id="u1"))
        listing = executor.list_tools(ctx.principal)  # 发给模型的工具（别名 weather_get）
        [result] = await executor.execute_many(
            [ToolInvocation(call_id="c1", tool="weather_get", arguments='{"city": "北京"}')],
            ctx,
            snapshot_id=listing.snapshot_id,
        )
        print(result.status, result.to_model_text())


asyncio.run(main())
```

### 使用真实模型测试工具调用

示例默认使用模拟模型。配置模型服务商的 API Key 环境变量后，可启用真实模型调用：

```bash
uv run python packages/agent-tools/examples/agent_loop.py --real --verbose
```

默认使用 `deepseek` 和 `deepseek-flash`，从 `DEEPSEEK_API_KEY` 读取密钥。可用
`--provider`、`--model` 和 `--prompt` 覆盖对应参数；模型必须在 SDK 的服务商配置中存在，
并且账户具备调用权限。真实模型调用会产生服务商费用。示例天气数据是模拟的，记事工具不会
实际保存数据。`--verbose` 展示轮次、模型返回、工具请求和结果摘要，并打印经过规则脱敏的
参数预览；脱敏不能识别所有敏感内容，请避免在共享终端使用敏感参数。不要将 API Key 写入代码或提交到版本控制。

示例每次运行自动生成一个 `session_id`，并为整次 Agent 运行创建一条 Trace；也可通过
`--session-id my-session` 传入已有会话 ID。模型请求和工具调用日志中的 `trace_id` 相同，
各操作使用各自的 `span_id`。示例只在进程内创建追踪上下文以关联日志，**不会导出或持久化**
Span；如需在追踪平台查看完整链路，应在宿主中配置 OpenTelemetry 导出器。该示例需要
`opentelemetry-sdk`（本仓库默认开发依赖包含它；仅安装核心包时需另行安装）。

### 在 Agent 示例中调用 MCP

运行以下命令会启动本地 stdio MCP 回显服务，将 `echo` 注册到工具白名单，随后由真实模型决定
是否调用它；运行结束时自动关闭服务端：

```bash
uv run python packages/agent-tools/examples/agent_loop.py --real --mcp-demo --verbose
```

需先配置 `DEEPSEEK_API_KEY`。不加 `--real` 时，使用模拟模型验证 MCP 调用链，不需要 API Key。
可通过 `--prompt` 指定请求；示例服务只回显文本，不连接第三方 MCP 服务。

## 关键语义

| 主题 | 行为 |
|---|---|
| 失败 | 工具层失败一律返回 `status="error"` 的结果；调用方被取消时 `CancelledError` 原样抛出 |
| 批量执行 | 结果顺序与输入一致；连续的 `concurrency_safe=True` 调用并发，其余逐个执行 |
| 访问控制 | `ToolPolicy.access` 必填；列出工具和执行工具都鉴权；可选的参数级鉴权在参数校验之后 |
| 不可逆工具 | 声明 `policy.approval`（人工审批）才能注册；或 `ToolRegistry(allow_destructive_without_approval=True)` 显式放开（不审批，风险自担） |
| 同步工具 | 在有上限的专用线程池中运行；超时后线程继续运行，额度要等所有线程结束才归还 |
| 重试 | 默认不重试；只有“错误可重试 **且** 工具声明幂等”才重试，退避不超过截止时间 |
| 限流 | 按（工具，租户）计数的令牌桶；默认超限直接失败，可配置在截止时间内等待 |
| 日志 | 默认 `metadata` 只记录字节数和哈希；`redacted` / `full` 记录脱敏、截断后的预览 |
| 输出 | 文本按 UTF-8 边界截断；JSON 和图片超限直接报错；输出 Schema 在截断之前校验 |
| 信任 | 工具输出一律标记 `trust="untrusted"`；Python 工具只能是宿主信任的代码 |

## HTTP 工具（需要 `[http]`）

```python
from agent_tools import (
    HttpTarget,
    MappingSecretProvider,
    NetworkPolicy,
    RetryPolicy,
    SecretRef,
    ToolAnnotations,
    ToolExecutor,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.adapters.http import HttpAdapter, HttpAdapterConfig

registry = ToolRegistry()
registry.register_http(
    HttpTarget(
        method="GET",
        base_url="https://crm.example.com/api",  # 协议、主机、端口在注册时固定
        path_template="/customers/{customer_id}",
        arg_mapping={"customer_id": "path"},  # 其余参数：GET/DELETE 进查询字符串，其他进 JSON 体
        auth=SecretRef(name="crm_token"),  # 注册信息里只有密钥名
    ),
    name="crm.customer.get",
    input_schema={
        "type": "object",
        "properties": {"customer_id": {"type": "string"}},
        "required": ["customer_id"],
        "additionalProperties": False,  # 必须为 false
    },
    output_schema={"type": "object", "required": ["id"]},  # 可选：截断之前校验
    policy=ToolPolicy(
        access="scoped",
        required_scopes=frozenset({"crm:read"}),
        retry=RetryPolicy(max_attempts=3),
    ),
    annotations=ToolAnnotations(read_only=True, idempotent=True, concurrency_safe=True),
)
http = HttpAdapter(
    HttpAdapterConfig(network=NetworkPolicy(allowed_hosts=frozenset({"crm.example.com"}))),
    secrets=MappingSecretProvider({"crm_token": "..."}),  # 生产环境请对接自己的密钥系统
)
executor = ToolExecutor(registry, adapters={"http": http})
```

### 出站网络安全（SSRF）

| 层次 | 行为 |
|---|---|
| URL | 主机、端口、协议注册时固定；路径参数完整编码，拒绝 `.` / `..`；默认只允许 `https` 和 443 端口 |
| 主机白名单 | `NetworkPolicy.allowed_hosts`（精确主机名或 `*.example.com`），每次请求都检查 |
| DNS 与连接地址 | 默认在**建立连接时**自行解析、检查**全部**解析结果，然后直接连接已校验的 IP；TLS 的 SNI 与证书校验仍用原始主机名。默认拒绝回环、私有、链路本地（含云元数据地址）、运营商级 NAT、组播、保留地址；`allowed_networks` 可显式放行内网网段 |
| 重定向 | 从不跟随，3xx 返回 `redirect_not_followed` |
| 代理 | 不读取环境变量中的代理。显式配置 `proxy` 时只能做**发送前预检**，无法在连接时检查，也不能防止 DNS 重绑定，必须依靠代理自身的访问控制。`http` / `https` 代理下连接池**只能连接代理地址**（`ProxyOnlyNetworkBackend`），不会绕过代理直连 |
| 响应 | 流式读取，超过 `max_response_bytes` 立即中断 |

**剩余风险与部署前提**：

* 连接时检查依赖替换 `httpx2` 传输层的内部连接池（私有属性 `_pool`），构造时会检查、版本已锁定在 `<3` 并有测试覆盖；升级 `httpx2` 时必须重新验证；
* TLS 主机名行为已在测试中对 httpcore 层断言，但没有在真实 TLS 连接上做端到端验证；
* 建议在部署层面同时配置出站网络策略（防火墙 / 网络策略），作为纵深防御；使用代理时这是必需的。

### Schema、重试与幂等

* JSON Schema 方言固定为 Draft 2020-12；只允许本地 `$ref`（注册时检查都能解析），**从不**联网加载远程 Schema；`format` 只作注释；Schema 有 64 KiB 和 32 层嵌套的上限；校验错误只报告路径和关键字，不含参数值。
* 重试条件：`attempt < max_attempts`、错误可重试、工具声明 `idempotent=True`；HTTP 的 POST / PATCH 还要求 `HttpTarget.idempotency_header=True`。本地限流错误不会触发重试。退避为指数加抖动，遵守 `Retry-After`（超过 `max_retry_after_s` 时放弃），且退避之后距离截止时间至少剩 0.05 秒。
* `Idempotency-Key` 只在 `idempotency_header=True` 时发送，作用域是“租户 + 请求 + 调用 + 工具 + 版本”，同一调用的所有重试相同。**发送这个头本身不能保证幂等**，需要下游真正实现去重。
* 一次调用（含所有重试）只占一份并发额度；取消会原样传播，额度在调用结束、且所有孤儿线程结束后归还。

### 限流

`ToolPolicy(rate_limit=RateLimit(requests=10, per_s=1, burst=20, max_wait_s=0.5))`：每次尝试消耗一个令牌；没有令牌时，若下一个令牌在 `min(max_wait_s, 剩余截止时间)` 内则等待，否则返回 `rate_limited`（带 `retry_after`）。等待中被取消会退还令牌。默认的 `TokenBucketRateLimiter` 只在当前进程内有效，多实例部署请通过 `rate_limiter=` 提供共享状态的实现。

**分布式限流参考实现（可选）**：`agent_tools.governance.redis_ratelimit.RedisTokenBucketRateLimiter`
用 Redis 上的 Lua 令牌桶让所有实例共享同一份限额。核心包不依赖 `redis`，客户端按鸭子类型传入：

```python
import redis.asyncio as redis  # 宿主自行安装：pip install redis

from agent_tools.governance.redis_ratelimit import RedisTokenBucketRateLimiter

limiter = RedisTokenBucketRateLimiter(
    redis.from_url("rediss://..."), prefix="myapp:ratelimit:", on_error="fail_closed"
)
executor = ToolExecutor(registry, rate_limiter=limiter)
```

* Redis 不可用（异常、超时、回复格式错误）时默认**拒绝调用**（`rate_limiter_unavailable`）；`on_error="fail_open"` 需要显式选择，放行并累加 `limiter.failures`；
* 时间取自 Redis 的 `TIME`；键是 `prefix + sha256(工具名|租户)`，租户不以明文出现；等待被取消时在后台尽力退还令牌；
* **它不是事务，也不提供严格一次的计数**：回复丢失或退还失败会多扣，主从切换丢写入、键被淘汰会少扣；
* **验证范围**：Python 一侧逻辑用内存假客户端测试；**Lua 脚本没有在真实 Redis 上运行过**（开发环境没有 Redis 且禁止网络连接）。投入使用前请先运行
  `AGENT_TOOLS_REDIS_URL=redis://... uv run --with redis pytest packages/agent-tools/tests -m redis`；
* 只协调限流。熔断、结果缓存、调用去重、单飞与并发额度的多实例作用范围见“多实例部署下各项治理能力的作用范围”。

### 密钥、日志模式与 ResultFilter

* 密钥通过宿主实现的 `SecretProvider.get_secret(name, ctx)` 在调用时获取（可按租户返回不同的值）。本次调用取得的密钥值会在**日志、Span、审计事件和返回给模型的结果**中按原值替换为 `[REDACTED_SECRET]`。经过编码、拆分或变形的密钥无法识别。
* `ExecutorConfig(log_mode=...)`：`off`、`metadata`（默认）、`redacted`、`full`。后两者在结束日志和 Span（`gen_ai.tool.call.arguments` / `gen_ai.tool.call.result`）中记录预览：先精确清除已知密钥，再按凭证格式清除（`redacted` 还清除邮箱、手机号等个人信息），最后截断到 1024 / 8192 字符。**规则脱敏只是尽力而为，不能保证识别所有敏感信息**，请只在必要时开启。
* `ResultFilter` 改变的是**返回给模型的结果本身**（例如去掉字段、按租户裁剪），而日志模式只影响日志和 Span 的副本。过滤器在大小限制和密钥清除之后执行，之后会再次检查大小；过滤器出错时返回内部错误，不会交出未过滤的内容。

### 审计和参数级授权

`ToolExecutor` 可通过 `audit` 接受实现 `AuditSink.emit()` 的异步审计接收端。事件包括授权决定（工具级、参数级各一条）和调用结果（含错误类型、错误码、尝试次数），**不包含**参数、结果内容或凭证。默认 `audit_failure_mode="fail_open"`：写入失败或超时会计入 `app.tool.audit.failures` 并记录错误类型。设置 `ExecutorConfig(audit_failure_mode="fail_closed")` 时必须同时提供接收端；工具执行**之前**的审计写入失败会拒绝执行，执行**之后**的审计失败只能计数和记录，无法撤回已发生的副作用。`audit_timeout_s` 控制单次写入的超时时间，默认一秒。审计写入期间调用方取消时，取消信号会原样传播。接收端负责审计记录的持久化和访问控制。

宿主可以传入同步、无 I/O 的 `authorize_args(principal, spec, args)` 回调，在工具级授权和参数校验之后执行参数级鉴权，返回 `AuthzDecision`；拒绝或回调抛出异常时工具都不会运行。回调不应修改参数或将敏感信息写入授权原因。

## MCP 工具（需要 `[mcp]`）

```python
import sys

from agent_tools import ToolAnnotations, ToolExecutor, ToolPolicy, ToolRegistry
from agent_tools.adapters.mcp import (
    McpAdapter,
    McpServerConfig,
    McpServerManager,
    StdioTransportConfig,
)
from agent_tools.integrations.mcp import McpToolAllow, McpToolSync


async def setup() -> ToolExecutor:
    manager = McpServerManager(
        [
            McpServerConfig(
                server_id="files",
                transport=StdioTransportConfig(
                    command=sys.executable,
                    args=("-m", "my_mcp_server"),
                    env={"ROOT": "/srv/data"},  # 只有显式配置的变量会额外传给子进程
                ),
            )
        ]
    )
    registry = ToolRegistry()
    report = await McpToolSync(registry, manager).sync(
        "files",
        [  # 白名单：只有这里列出的远端工具会被注册；策略和语义标注都由宿主决定
            McpToolAllow(
                remote_name="read.file",
                policy=ToolPolicy(access="scoped", required_scopes=frozenset({"files:read"})),
                annotations=ToolAnnotations(read_only=True, concurrency_safe=True),
                description="读取文件内容",  # 可选：覆盖远端描述（远端描述不可信）
            )
        ],
    )
    print(
        report.registered, report.ignored
    )  # ('mcp.files.read.file',)，模型侧别名 mcp_files_read_file
    return ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)})
```

Streamable HTTP 服务端使用 `HttpTransportConfig(url=..., network=NetworkPolicy(...), auth=SecretRef(...))`，
并通过 `McpServerManager(..., secrets=...)` 提供凭证。

| 主题 | 行为 |
|---|---|
| 发现与注册 | 分页读取（有页数、工具数上限和游标循环检测）；只注册白名单中的工具，新出现的工具默认不开放；同步通过 `ToolRegistry.apply` **原子生效**，冲突时整体失败、注册表不变；同步器只替换自己注册过的工具 |
| 名称 | 内部名 `mcp.<server_id>.<远端名>`（小写、非法字符替换、超长截断加哈希）；规范化后撞名的工具都被拒绝，可用 `name` 显式指定 |
| 不可信输入 | 远端描述去控制字符并截断到 1024 字符，可由宿主覆盖；参数 Schema 按 P1 规则校验（Draft 2020-12、只允许本地 `$ref`、大小与深度上限），顶层强制 `additionalProperties: false`；远端的 readOnly / idempotent 等提示**不被采信** |
| 刷新 | 定义不变保持版本；变化时以新版本替换；远端消失或移出白名单的工具被移除 |
| 会话 | 默认（`session_scope="server"`）每个服务端一条共享会话；`session_scope="tenant"` 时每个（服务端，租户）一条独立会话。会话由专属后台任务持有；同一会话的并发首次调用只连接一次 |
| 断开与重连 | 断开后本次调用返回可重试的 `mcp_connection_lost`，同一会话（同一租户）的下一次调用重连并重新获取凭证；连续连接失败达到上限后进入冷却期（`mcp_server_unavailable`），失败计数与冷却期按会话统计；没有后台无限重试 |
| 取消与超时 | 取消原样传播，SDK 向服务端发送取消通知（stdio 下已验证服务端停止执行）；通知**不保证**远端停止，也不能撤回副作用 |
| 结果 | 文本、PNG/JPEG/GIF/WebP 图片、内嵌文本资源直接映射；音频、资源链接、二进制资源、其他图片格式、未知类型变成说明性占位符，不会被静默丢弃；`isError` → `mcp_tool_error`（默认不把远端错误文本交给模型）；协议错误只暴露错误码 |
| 重试 | 与其他工具相同：只有宿主声明幂等并配置重试时才重试 |
| 回调 | 不提供 sampling、elicitation、roots、logging 回调；服务端要求额外输入时调用失败（`mcp_input_required`） |

**安全边界与限制**：

* stdio：命令、参数、工作目录、额外环境变量由宿主配置；SDK 总会继承一组固定变量（POSIX 上为 HOME、LOGNAME、PATH、SHELL、TERM、USER），其余父进程环境变量不会传给子进程；
* Streamable HTTP：地址必须通过网络策略；连接前做 DNS 预检，并复用 P1 的连接时检查传输层（SNI 仍用原始主机名）；SDK 只跟随同源重定向；默认不使用代理，也不读取环境变量中的代理（代理见下文）；
* 凭证默认是服务端级别的（同一会话被所有调用共享）；需要按租户区分时使用 `session_scope="tenant"`（见下文）。本次调用**实际使用的会话**的凭证值会登记到调用中做精确清除；
* 默认情况下 SDK 会把整个响应读入内存后才交给适配器，输出大小限制在映射之后执行。P3 提供可选的传输层上限：HTTP 用 `McpServerConfig.max_response_bytes`，stdio 用 `StdioTransportConfig(isolation=StdioIsolation(max_line_bytes=...))`（见下文）；未配置时与 P2 相同；
* SDK 在协议层要求每个工具的参数 Schema 都是对象类型；服务端只要有一个工具不满足，整个列表就会失败，同步随之失败（注册表不变）；
* 基础设施**不承诺**能防止提示注入：MCP 服务端和工具输出都视为不可信。

### 租户级会话与凭证隔离（可选，默认关闭）

```python
McpServerConfig(
    server_id="crm",
    transport=HttpTransportConfig(url=..., network=..., auth=SecretRef(name="crm_token")),
    session_scope="tenant",  # 默认 "server"：原有行为
    max_tenant_sessions=64,  # 同时保留的会话数上限（stdio 即子进程数上限）
    tenant_idle_ttl_s=900,  # 空闲回收；None 表示不按空闲回收
)
await McpToolSync(registry, manager).sync(
    "crm", allow, ctx=ToolContext(principal=Principal(tenant_id="ops"))
)
```

| 环节 | 租户上下文 |
|---|---|
| 会话选择 | 租户取自 `ToolContext.principal.tenant_id`（宿主认证后设置，不来自模型）。每个（服务端，租户）独立的 `mcp.Client`、HTTP 连接池或 stdio 子进程；**没有租户的调用被拒绝**（`mcp_tenant_required`），不会退回共享会话 |
| 凭证 | 建立会话时用**只带租户**的合成上下文调用 `SecretProvider`（不带触发连接的那次调用的用户、权限、请求 ID）。会话被同一租户的所有用户共享，所以凭证只能是租户级的，不能是用户级的。stdio 可用 `StdioTransportConfig(secret_env={"API_TOKEN": "crm_token"})` 把密钥作为环境变量传给该租户的子进程 |
| 发现 | 租户范围的服务端没有共享会话：`sync(..., ctx=...)` 必须指定用哪个租户读取工具列表，否则失败且注册表不变。默认（`scope="shared"`）结果写入共享目录，所有租户看到同一份工具定义；服务端按租户返回不同工具集时用 `scope="tenant"`，见下文“按租户区分的工具目录” |
| 复用与并发 | 同一租户的并发调用共用一条会话，首次调用单飞；不同租户的请求不会进入同一条会话 |
| 重连 | 断开后由同一租户的下一次调用重连，并重新获取该租户的凭证；冷却期按租户统计，一个租户的凭证错误不会让其他租户进入冷却 |
| 凭证变更 | `await manager.invalidate(server_id, tenant_id=...)`：会话立即从管理器移除，下一次调用建立新会话并重新取凭证。默认等在途请求结束后关闭旧会话（它们仍使用旧凭证）；`force=True` 立即关闭（在途请求得到可重试的 `mcp_connection_lost`），用于凭证吊销。不传 `tenant_id` 时该服务端的全部会话失效。**管理器不会自动感知凭证轮换**，需要宿主调用 |
| 回收与关闭 | 达到 `max_tenant_sessions` 时回收最久未使用的空闲会话；全部忙碌时新租户的调用返回可重试的 `mcp_session_limit`。空闲超时的会话在下一次取会话时回收（没有后台任务），也可定期调用 `manager.close_idle_sessions()`。`aclose()` 关闭所有租户的会话与子进程 |

**迁移**：现有配置无需修改（默认 `session_scope="server"`，凭证仍用不带租户的上下文获取）。改为
`"tenant"` 时需要：① `SecretProvider` 能按 `ctx.principal.tenant_id` 返回凭证；② 所有调用都带租户；
③ 发现时传入 `ctx`；④ 凭证轮换时调用 `invalidate`。`McpServerManager.known_secrets()` /
`connected()` 新增仅关键字参数 `tenant_id`（默认 `None`，即服务端级会话）。

### 按租户区分的工具目录（可选，默认不使用）

注册表有一个**共享层**（默认，所有租户可见，行为与以前完全相同）和若干**租户层**。带 `tenant_id`
注册的工具只存在于该租户的目录里：

```python
registry.register_function(search, name="crm.search", policy=policy, tenant_id="acme")
registry.register_http(
    target, name="billing.get", input_schema=schema, policy=policy, tenant_id="acme"
)
# MCP：用 acme 自己的会话与凭证发现，并只写入 acme 的目录
await McpToolSync(registry, manager).sync(
    "crm", allow, ctx=ToolContext(principal=Principal(tenant_id="acme")), scope="tenant"
)
executor.list_tools(Principal(tenant_id="acme"))  # 共享工具 + acme 的工具
registry.drop_tenant("acme")  # 租户下线：删除该租户层的全部工具
```

| 方面 | 行为 |
|---|---|
| 可见性 | `list_tools(principal)`、`execute`、`execute_many` 都按 `principal.tenant_id`（宿主认证后设置，不来自模型）取“共享层 + 该租户层”的视图。其他租户的工具表现为**不存在**（`not_found`，与名字本来就不存在无法区分），不会建立会话或访问下游。没有租户的调用方只看到共享工具 |
| 权限 | 租户层只决定目录里有什么，**不是授权**：解析之后照常执行工具级与参数级鉴权、审批和各项治理 |
| 名称与别名 | 同一个视图内不允许歧义：租户级工具与共享工具之间的同名、别名交叉在**注册阶段**拒绝（无论先注册哪一方），租户层不能遮盖共享工具。不同租户之间互不影响，可以用相同的内部名和别名注册不同的定义（例如同一个 MCP 工具在两个租户有不同的 Schema） |
| 修改 | `register` / `apply` / `register_function` / `register_http` / `set_active` / `enable` / `disable` / `unregister` 新增仅关键字参数 `tenant_id`（默认 `None` = 共享层），一次调用只修改一个作用域；`tenants()` 列出有租户层的租户；`drop_tenant()` 删除整层 |
| 快照 | 所有层共用一个快照序号，任何一层的变更都生成新快照；未变化的层在新旧快照之间共享同一个不可变对象。一批调用开始后目录变化不影响这一批。`snapshot_id` 只确定“哪个时刻”，目录始终由 `ctx` 的租户决定，拿到别的租户的编号也解析不到对方的工具。其他租户的变更同样占用保留名额（默认 8 个），租户多且刷新频繁、又依赖 `snapshot_id` 时调大 `ToolRegistry(history=...)` |
| MCP 刷新 | 每个租户各自 `sync(..., scope="tenant")`：只读取、修改、移除该租户层中由同步器注册的工具，其他租户与共享层不变；白名单传空列表即移除该租户在这个服务端上的全部工具。不同租户的同步可以并发；没有租户的 `scope="tenant"` 同步在连接之前被拒绝（`mcp_tenant_required`），不会退回共享层 |
| 并发 | 注册表的冲突检查与发布在同一把线程锁内；读取方使用不可变快照，不加锁 |

限制：同一个服务端的同名工具不能既在共享层又在租户层（后一次同步整体失败，注册表不变；可为租户级
工具显式指定 `name` / `alias`）；目录按租户而不是按用户区分；租户层与 MCP 会话各自管理——
`drop_tenant` 不会关闭会话，`manager.invalidate` 也不会修改目录；熔断的隔离键不含租户（见“熔断”）。

### Streamable HTTP 代理（可选，默认关闭）

```python
HttpTransportConfig(
    url="https://mcp.example.com/mcp",
    network=NetworkPolicy(allowed_hosts=frozenset({"mcp.example.com"})),
    proxy=McpProxyConfig(url="http://proxy.internal:3128", auth_secret="egress_proxy"),
)
```

* 只支持 `http` / `https` 代理；代理地址不能内嵌凭证，Basic 凭证（密钥值格式 `用户名:密码`）通过 `SecretProvider` 获取，只进入发给代理的 `Proxy-Authorization`，不会发给目标；目标凭证只在 TLS 隧道内；
* **默认网络安全策略不变**：不配置 `proxy` 时仍是连接时 IP 检查，环境变量里的代理不生效。`HttpTransportConfig.guard_mode` 如实报告当前模式（`connect` / `preflight`）；
* 代理模式的检查：建立会话前、以及**每个请求发送前**都检查 URL（协议、主机白名单、端口）并解析、检查全部 DNS 结果（包括 SDK 跟随的同源重定向）；连接池只能连接代理地址；
* **剩余风险**：目标由代理解析和连接，客户端**无法验证实际连接地址**。预检与代理各自解析一次 DNS，结果可能不同，**不能防止 DNS 重绑定**。使用前提是代理自身有访问控制：目标主机白名单、拒绝内网与云元数据地址、限制 `CONNECT` 端口。`https` 目标的 TLS 仍由客户端端到端校验证书和主机名；明文 `http` 目标（需要显式 `allow_http`）的全部内容对代理可见；
* 代理地址本身视为可信的部署配置，不经过 `NetworkPolicy` 的内网检查；不支持 SOCKS；
* **验证范围**：客户端一侧（只连接代理、`CONNECT` / 正向代理的请求形式、凭证去向、预检）用内存网络测试；经真实代理的完整 MCP 会话测试已编写（`-m network`），开发环境禁止本地 TCP，**未运行**。

## P3 扩展能力（全部默认关闭）

执行链的固定顺序（P3 新增步骤加粗）：

```
大小检查 → 解析 → 工具级鉴权 → 参数校验 → 参数级鉴权 → **调用去重** → **人工审批** → **结果缓存**
→ 并发额度 → 重试循环 { **熔断准入** → 限流令牌 → 超时内调用适配器 → **记入熔断** }
→ 输出 Schema 校验 → 大小限制（超限时 **on_oversize**）→ 密钥清除 → ResultFilter → 大小限制 → 审计
```

| 能力 | 怎么启用 | 默认 |
|---|---|---|
| 熔断 | `ToolExecutor(circuit_breaker=InProcessCircuitBreaker(CircuitBreakerConfig(...)))` | 不熔断 |
| 人工审批 | 工具 `ToolPolicy(approval=ApprovalPolicy(...))` + `ToolExecutor(approval_handler=...)` | 不审批；声明了审批却没有处理器时**安全失败** |
| 结果缓存 | 只读工具 `ToolPolicy(cache=CachePolicy(ttl_s=...))` + `ToolExecutor(result_cache=InMemoryResultStore())` | 不缓存；没有存储时照常执行 |
| 调用去重 | `ToolPolicy(dedupe=DedupePolicy(window_s=...))` + `ToolExecutor(dedupe_store=InMemoryResultStore())` | 不去重；声明了去重却没有存储时**安全失败** |
| Python 进程隔离 | `registry.register_function(fn, ..., isolation=ProcessIsolation(...))` | 进程内运行 |
| MCP stdio 隔离 | `StdioTransportConfig(..., isolation=StdioIsolation(...))` | 直接启动服务端 |
| MCP HTTP 响应上限 | `McpServerConfig(..., max_response_bytes=...)` | 不限制（P2 行为） |
| 超大输出钩子 | `ToolExecutor(on_oversize=handler)` | 超限即失败 |
| 私有 CA | `HttpAdapterConfig(ca_bundle=...)` / `HttpTransportConfig(ca_bundle=...)` | 默认信任库 |

```python
from agent_tools import (
    ApprovalDecision,
    ApprovalPolicy,
    ApprovalRequest,
    CachePolicy,
    CircuitBreakerConfig,
    InMemoryResultStore,
    InProcessCircuitBreaker,
    ProcessIsolation,
    ResourceLimits,
    ToolAnnotations,
    ToolExecutor,
    ToolPolicy,
    ToolRegistry,
)


class ConsoleApproval:
    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        # request.arguments 默认是脱敏后的预览；arguments_sha256 可把决定绑定到具体参数
        return ApprovalDecision(approved=await ask_human(request), approver="ops@example.com")


registry = ToolRegistry()
registry.register_function(
    delete_customer,
    name="crm.delete_customer",
    policy=ToolPolicy(
        access="scoped",
        required_scopes=frozenset({"crm:admin"}),
        approval=ApprovalPolicy(timeout_s=120),
    ),
    annotations=ToolAnnotations(destructive=True),
)
registry.register_function(
    search,
    name="kb.search",
    policy=ToolPolicy(access="public", cache=CachePolicy(ttl_s=60)),
    annotations=ToolAnnotations(read_only=True),
)
registry.register_function(
    render_pdf,
    name="doc.render",  # 模块顶层函数，参数 / 返回值可 JSON 序列化
    policy=ToolPolicy(access="public", timeout_s=20),
    isolation=ProcessIsolation(limits=ResourceLimits(cpu_s=10, open_files=64)),
)
executor = ToolExecutor(
    registry,
    circuit_breaker=InProcessCircuitBreaker(
        CircuitBreakerConfig(failure_threshold=5, open_s=30, scope="target")
    ),
    approval_handler=ConsoleApproval(),
    result_cache=InMemoryResultStore(max_entries=10_000),
)
```

### 熔断

* 状态：关闭 →（连续失败 `failure_threshold` 次）→ 开启 →（`open_s` 后）→ 半开（最多 `half_open_max_calls` 个探测）→ 成功 `success_threshold` 次关闭 / 任一失败重新开启。状态切换后，旧许可的结果被忽略（慢请求事后成功不会关闭刚开启的熔断）。
* 隔离粒度 `scope`：`tool`（按工具名，所有版本共享）或 `target`（HTTP 按 `scheme://host:port`，MCP 按服务端 ID，Python 按工具名）。不按租户隔离：熔断描述的是下游健康状况。
* 计入失败：可重试的 `UpstreamError`（5xx、429、连接错误、MCP 连接丢失）与单次尝试超时（`attempt_timeout`）。不计入：参数、权限、本地限流 / 并发受限、整体截止时间、工具业务错误、输出超限；调用方取消时释放探测名额、不计入。可传入自定义 `classifier`。
* 与其他治理的顺序：并发额度（一次调用一份）在外；**每次尝试**先过熔断准入，再取限流令牌，再在单次超时内调用适配器。熔断拒绝（`circuit_open` / `circuit_half_open`，`retryable=True`、带 `retry_after`）不消耗令牌、不调用下游，也**不触发重试**。缓存命中不经过熔断。
* 进程内实现只对当前进程有效，**不能替代**多实例之间共享状态的治理；需要时实现异步的 `CircuitBreaker` 协议，或使用下文“共享状态的参考实现”中的 `RedisCircuitBreaker`。
* 隔离键不含租户。使用按租户的 MCP 会话与凭证时，一个租户的凭证或连接问题产生的可重试失败会计入所有租户共用的熔断状态；需要按租户隔离时目前只能由宿主提供自己的 `CircuitBreaker`（按租户扩展隔离键需要改动协议，列为待决事项）。

### 人工审批

* 时点：参数级鉴权与调用去重之后、结果缓存与执行之前；未授权或参数非法的调用不会打扰审批人；等待审批不占用执行额度；每次调用只审批一次（重试不再审批；去重重放不执行，因此也不审批）。
* 结果：批准 → 执行；拒绝 → `approval_rejected`；没有处理器 → `approval_unavailable`；超过 `min(timeout_s, 剩余截止时间)` → `approval_timeout`；处理器异常或返回非法值 → `approval_failed`；调用方取消 → `CancelledError` 原样传播；执行器关闭 → `executor_closed`。除批准外工具都不执行。
* 审计：批准、拒绝、超时、失败、无处理器都写一条 `approval` 事件（含 `approver`、`reason`），在执行之前写入；`fail_closed` 下写入失败会阻止执行。拒绝理由只进审计，不返回给模型。
* 参数暴露（`ApprovalPolicy.arguments`）：`redacted`（默认，参数名含 password / token 等的值整体隐藏，其余做凭证与个人信息规则脱敏并截断，尽力而为）、`none`（只给摘要与大小）、`full`（原始参数，需显式选择）。
* 适用范围：同步等待，适合审批人在线、几分钟内响应的交互式场景。**不支持**跨进程或长时间暂停后恢复：进程重启、关闭、取消或截止时间到达都会结束等待，审批状态不保存。

### 结果缓存、调用去重与下游幂等

三者互不替代：**结果缓存**是只读工具的性能优化；**调用去重**防止同一逻辑调用（同一 `request_id` + `call_id` + 参数 + 身份）被重复执行；**下游幂等**（HTTP `Idempotency-Key`）需要下游自己支持。

* 缓存只允许 `read_only=True`、非不可逆、无需审批的工具（注册时校验），与去重互斥。键 = SHA-256（范围、租户、用户、排序后的权限集合、工具名、版本、规范化参数）；`scope="tenant"` 去掉用户但保留权限集合；缺少所需身份字段时不缓存。查找在鉴权之后，每次命中都重新鉴权；缓存的是 `ResultFilter` 之前的输出，命中时按本次上下文重新过滤。只缓存成功结果；同键并发未命中只执行一次；执行者失败或取消时等待者各自执行；存储读写失败按未命中处理。
* 去重键 = SHA-256（租户、用户、request_id、call_id、工具名、版本、规范化参数）。工具**开始执行后**的结果（成功或失败）在 `window_s` 内被重放；工具没有开始执行（审批拒绝、熔断、等不到执行资源）时不记录。并发重复提交等待第一次结束；第一次开始执行后被取消或记录写入失败时，等待者得到 `duplicate_call_aborted`（效果未知，不再执行）。没有存储或读取存储失败时 `dedupe_unavailable`（不执行）。
* 去重**不是事务**：执行完成到写入记录之间进程崩溃、记录过期或被容量淘汰后，重复提交会再次执行。`InMemoryResultStore` 只在本进程有效；`result_cache` 与 `dedupe_store` 必须是不同实例。缓存中的内容是工具输出（已清除本次调用的已知密钥），存储的访问控制、加密与持久化由宿主的 `ResultStore` 实现负责。

### 共享状态的参考实现（可选，Redis）

两个参考实现让多个实例共享结果存储与熔断状态。核心包**不依赖** `redis`，客户端按鸭子类型传入；
不配置时一切照旧（进程内实现）。

```python
import redis.asyncio as redis  # 宿主自行安装；不要开启 decode_responses

from agent_tools.governance.redis_circuit import RedisCircuitBreaker
from agent_tools.governance.redis_store import RedisResultStore

client = redis.from_url("rediss://...")
breaker = RedisCircuitBreaker(client, CircuitBreakerConfig(), prefix="myapp:circuit:")
executor = ToolExecutor(
    registry,
    circuit_breaker=breaker,
    result_cache=RedisResultStore(client, prefix="myapp:cache:"),
    dedupe_store=RedisResultStore(dedupe_client, prefix="myapp:dedupe:"),  # 建议用不淘汰键的实例
)
...
await executor.aclose()
await breaker.aclose()  # 等后台记录写完，再关闭 Redis 客户端
```

| | `RedisResultStore` | `RedisCircuitBreaker` |
|---|---|---|
| 作用 | 跨实例命中结果缓存；重放其他实例**已完成**的去重记录 | 所有实例共同统计失败、共同开合；半开探测名额合计 |
| 实现 | `GET` / `SET PX` 两条普通命令，没有 Lua | 两个 Lua 脚本（准入、记录），每个只访问一个键；时间取自 Redis `TIME` |
| 租户隔离 | 键是执行器给出的 SHA-256 摘要（含租户、用户、权限集合或请求与调用 ID），存储不解析也不放宽；键里没有明文身份 | 不按租户隔离（隔离键是工具或下游目标） |
| 过期 | 缓存 `ttl_s`、去重 `window_s`，以 Redis 时钟为准 | 状态哈希在 `max(2 × open_s, idle_ttl_s)` 内没有写入就过期，等同于恢复关闭 |
| Redis 不可用 | 由执行器处理：缓存按未命中（工具照常执行）；去重读取失败不执行（`dedupe_unavailable`） | `on_error="local"`（默认）改由本实例的进程内熔断器判断；`"fail_closed"` 拒绝调用（`circuit_unavailable`） |
| 安全边界 | **值是工具结果的明文**（已清除本次调用的已知密钥）。Redis 的访问控制、TLS 与静态加密由宿主负责；不同部署 / 用途使用不同前缀 | 键与值只含状态计数，不含调用内容 |

它们**不提供**的保证：

* **缓存 ≠ 去重 ≠ 并发协调 ≠ 下游幂等**。共享存储只能重放已经完成并写入的调用。同一个调用**同时**到达两个实例时两边都会执行：单飞仍只在进程内，跨实例单飞需要“先占位再执行”的原子语义，现有 `ResultStore` 协议（只有 `get` / `set`）表达不了，参考实现没有偷偷加锁；
* **不是事务，不是严格一次执行**。执行完成到写入之间崩溃、写入失败、记录过期、被内存淘汰或主从切换丢失后，重复提交会再次执行。超过 `max_value_bytes` 的记录不写入（记录日志）；
* 熔断的“连续失败”在多实例并发时只是近似：结果在尝试结束后由后台任务写入，写入失败时这次结果丢失；探测名额是带有效期的租约（`probe_ttl_s`），持有者崩溃后要等过期；
* **验证范围**：Python 一侧用内存假客户端测试（`tests/test_redis_state.py`，熔断脚本在假客户端里用 Python 按相同算法实现）。**两者都没有在真实 Redis 上运行过**（开发环境没有 Redis 且禁止网络连接），熔断的 Lua 脚本尤其需要先验证。投入使用前请运行
  `AGENT_TOOLS_REDIS_URL=redis://... uv run --with redis pytest packages/agent-tools/tests -m redis`。

### 执行隔离

* `ProcessIsolation`（Python 工具）：每次调用一个新的 `python -I` 子进程（自成进程组），参数经 stdin、结果经 stdout 以 JSON 传递（工具的 `print` 会被重定向到 stderr）。环境变量**只有**显式配置的 `env`；默认工作目录为每次调用新建、结束后删除的临时目录；结果超过 `max_output_bytes` 立即终止；同时运行的子进程数受 `ExecutorConfig.max_subprocesses` 限制（等待名额超时报告为 `start_timeout`）。
* 生命周期：正常结束、异常退出、超时、调用方取消、执行器关闭时都终止**整个进程组**（SIGTERM → `kill_grace_s` → SIGKILL）并回收；已测试孙进程也会被终止。调用 `setsid` 脱离进程组的后代无法回收。
* 资源限制 `ResourceLimits`：`cpu_s`、`open_files`、`file_size_bytes` 在 Linux / macOS 上生效（已在 macOS 上测试 CPU 与文件数）；`memory_bytes`（`RLIMIT_AS`）**只在 Linux 上可用**，macOS 拒绝设置——配置了无法生效的限制时**拒绝运行**（`isolation_unavailable`），不会静默放行。Linux 上的内存限制测试在本次开发环境（macOS）中被跳过，未经实测。
* `StdioIsolation`（MCP stdio 服务端）：经 `agent_tools.runtime.stdio_launcher` 启动，资源限制由服务端继承；单条 JSON-RPC 消息超过 `max_line_bytes`（默认 4 MiB）时丢弃该消息，若能从行首确认是响应则改为发送同 ID 的错误（`mcp_response_too_large`，连接继续可用）。
* **这不是完整的安全沙箱**：子进程仍以宿主用户身份运行，没有文件系统、网络、系统调用隔离，不支持 Windows。需要强隔离时用 `wrapper=("/usr/bin/bwrap", ..., "--")` 等外部沙箱 / 容器命令前缀，由部署方负责其配置与验证；基础设施只保证按前缀启动并在外层管理生命周期。

### 超大输出钩子 `on_oversize`

* 只在输出超过 `OutputLimits`（JSON、图片、合计）时调用；文本超限仍只截断。钩子收到工具规格、**已清除已知密钥**的完整输出、触发的 `OutputLimitError` 和上下文，返回新的 `ToolOutput`。
* 改写结果会再次经过输出 Schema 校验、大小限制、密钥清除与 `ResultFilter`；仍超限时返回 `output_too_large_after_hook`。钩子超时（`ExecutorConfig.oversize_timeout_s`，默认 5 秒）、抛出异常或返回非法值时返回原来的 `OutputLimitError`。
* 钩子拿到的是已在内存中的输出，**不能**减少传输阶段的内存占用；传输层限制见 HTTP 的 `max_response_bytes` 与 MCP 的传输层上限。

### 多实例部署下各项治理能力的作用范围

| 能力 | 内置实现的作用范围 | 多实例时 | 不提供的保证 |
|---|---|---|---|
| 限流 | 进程内令牌桶 | 每个实例各算各的限额；可换成 Redis 参考实现或宿主自己的 `RateLimiter` | 参考实现不是严格计数，见“限流” |
| 并发额度（全局 / 单工具 / 批内） | 进程内 | 各实例独立，总并发 = 实例数 × 上限 | 无跨实例并发上限 |
| 熔断 | 进程内（`InProcessCircuitBreaker`） | 各实例各自统计失败、各自开合；可换成 `RedisCircuitBreaker` 参考实现或宿主自己的 `CircuitBreaker` | 参考实现也不是严格一致的全局状态，见“共享状态的参考实现” |
| 结果缓存 | `ResultStore` 协议；内置 `InMemoryResultStore` 进程内 | 换成共享存储（如 `RedisResultStore` 参考实现）后可跨实例命中 | 普通缓存：可能读到 TTL 内的旧结果，不保证一致性 |
| 调用去重 | `ResultStore` 协议 + 进程内 `SingleFlight` | 共享存储（如 `RedisResultStore`）可以重放**已完成**调用的记录；两个实例同时收到同一调用时都会执行 | **不是**跨实例事务、严格一次执行或下游幂等保证；执行完成到写入记录之间崩溃、记录过期后会再次执行 |
| 单飞协调 | 进程内 | 只合并同一实例内的并发相同调用 | 无跨实例单飞 |
| MCP 会话、冷却期 | 进程内 | 每个实例有自己的会话（每个租户在每个实例各一条）与冷却状态 | 无跨实例会话共享 |
| 审批 | 进程内同步等待 | 审批请求由发起调用的实例等待 | 不支持跨进程或长时间暂停后恢复 |

需要对下游真正幂等时，仍然要靠下游自己的幂等机制（例如 `Idempotency-Key` 且下游实现了去重）。

### 已知限制的现状（2026-11-10）

| 限制 | 现状 |
|---|---|
| MCP HTTP 取消通知、真实 TCP / TLS | **仍未验证**。真实回环网络测试 `tests/test_network_e2e.py`（`-m network`）已编写；开发环境禁止回环 TCP（`connect()` 返回 `EPERM`），历次均未能运行。TLS 配置（私有 CA、主机名校验）已通过内存握手测试验证 |
| Linux 进程内存限制 | **仍未实测**。开发机是 macOS，Docker 不可访问；`test_memory_limit_enforced_on_linux` 被跳过。复现：在 Linux 上运行 `uv run pytest packages/agent-tools/tests/test_isolation.py` |
| 注册表不按租户区分 | **已解决（可选）**：租户层与 `sync(..., scope="tenant")`，见“按租户区分的工具目录” |
| 熔断、结果存储只在进程内 | **参考实现已提供，未在真实 Redis 上验证**：见“共享状态的参考实现”。跨实例单飞、按租户的熔断隔离仍不提供 |
| MCP 响应在内存中完整读取 | 可选传输层上限已实现并测试（HTTP JSON 响应、SSE 事件、stdio 单条消息）；默认关闭 |
| 凭证不能按租户区分 | **已解决（可选）**：`session_scope="tenant"`，见“租户级会话与凭证隔离” |
| MCP HTTP 不支持代理 | **已实现（可选），真实代理未验证**：见“Streamable HTTP 代理”。代理模式不具备连接时 IP 检查 |
| 分布式限流 | **参考实现已提供，Lua 脚本未在真实 Redis 上验证**：见“限流” |

## 从 P0 升级的兼容性说明

* `ToolBinding.target` 的类型从 `PythonTarget` 扩展为 `PythonTarget | HttpTarget | McpTarget`：运行时行为不变，但读取 `context_param` 等 Python 专有属性前需要先用 `isinstance` 收窄类型（仅影响类型检查）。
* `ToolRegistry` 新增 `apply()` 原子批量变更；`register()` 的行为不变（内部改为调用 `apply()`）。
* `ToolPolicy` 新增 `retry`（默认不重试）与 `rate_limit`（默认不限制），`ToolSpec` 新增 `output_schema`（默认无），`AuditEvent` 新增 `error_type` / `error_code` / `attempts`（默认 `None`）：都带默认值，旧代码无需修改。
* P3：`ToolPolicy` 新增 `approval` / `cache` / `dedupe`，`ToolExecutionResult` 新增 `reused`，`AuditEvent` 新增 `approver` / `reused` 且 `kind` 可以是 `approval`，`PythonTarget` 新增 `isolation`，`ExecutorConfig` 新增 `store_timeout_s` / `oversize_timeout_s` / `max_subprocesses`，MCP 与 HTTP 配置新增 `isolation` / `max_response_bytes` / `ca_bundle`：全部带默认值，默认行为不变。
* P3 行为变化（只放宽，不收紧）：声明了 `policy.approval` 的不可逆工具现在可以在默认注册表中注册（以前一律拒绝）；未声明审批的不可逆工具仍被拒绝，`allow_destructive_without_approval=True` 的含义不变。
* 审计接收端如果按 `kind` 做穷举处理，需要接受新的 `approval` 事件。
* 2026-11-10：`McpServerConfig` 新增 `session_scope` / `max_tenant_sessions` / `tenant_idle_ttl_s`，`HttpTransportConfig` 新增 `proxy`，`StdioTransportConfig` 新增 `secret_env`，`McpServerManager.list_tools` / `call_tool` 与 `McpToolSync.sync` 新增可选的 `ctx`，`create_http_client` 新增 `proxy_auth`：全部带默认值，默认行为不变。`McpServerManager.known_secrets()` / `connected()` 的签名新增仅关键字的 `tenant_id`。静态请求头中不再允许 `Proxy-Authorization`（以前会被当作普通请求头发给目标服务）。
* 2026-11-10 行为变化（只收紧）：HTTP 适配器与 MCP 在 `http` / `https` 代理模式下，连接池只能连接配置的代理地址；配置方式不变。
* 2026-11-10（第二次）：`ToolRegistry` 的 `register` / `apply` / `register_function` / `register_http` / `set_active` / `enable` / `disable` / `unregister` 新增仅关键字参数 `tenant_id`（默认 `None`），新增 `tenants()` / `drop_tenant()`；`RegistrySnapshot` 新增带默认值的字段 `tenants` / `tenant_id` 与方法 `for_tenant()`；`McpToolSync.sync` 新增 `scope`（默认 `"shared"`），`McpToolSync.registered` 新增仅关键字的 `tenant_id`，`McpSyncReport` 新增 `tenant_id`。全部带默认值：不使用租户层时，注册、列表、解析、执行与快照编号的行为不变（P0–P3 的测试断言未修改）。
* 2026-11-10（第二次）注意：直接从 `registry.snapshot()` 取得的快照**只解析共享层**。自行调用 `snapshot.resolve()` 的宿主代码如果要看到租户级工具，需要先 `snapshot.for_tenant(tenant_id)`；不使用租户层时没有区别。按位置构造 `RegistrySnapshot(id, entries, aliases)` 的代码不受影响。

## 开发

在仓库根目录执行：

```bash
uv run pytest packages/agent-tools/tests              # 单元 + 集成 + 端到端
uv run pytest packages/agent-tools/tests -m packaging # 构建 wheel 并在临时虚拟环境中安装验证（需要下载依赖）
uv run pytest packages/agent-tools/tests -m network   # 真实回环 TCP / TLS / 代理端到端测试（需要允许本地监听与连接）
AGENT_TOOLS_REDIS_URL=redis://127.0.0.1:6379/15 uv run --with redis pytest packages/agent-tools/tests -m redis  # 在真实 Redis 上验证限流与熔断的 Lua 脚本、结果存储（只使用并清理带随机前缀的测试键）
cd packages/agent-tools && uv run mypy && uv run ruff check . && uv run ruff format --check .
```
