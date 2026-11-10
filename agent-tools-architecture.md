# Agent Tool Calling Infrastructure 架构方案（修订版 v2）

> 状态：P0、P1、P2、P3 已实现（实现记录与对文档的修正见 §10、§11、§12、§13）；2026-11-10 补充了 MCP 租户会话隔离、MCP HTTP 代理与分布式限流参考实现（§14）。同日第二次补充了按租户区分的注册表视图、共享状态的结果存储与熔断参考实现（§15）。真实网络端到端、Linux 内存限制、真实代理与真实 Redis 上的验证**仍未完成**，当前状态以 §15.6 为准（取代 §14.6）。逐日进度见 `docs/progress/`
> 范围：`/Users/yanghao/work/provider-model`
> 核实日期：2026-11-09。文中“已核实”表示本次检查过源码、锁文件或包元数据；“待核实”表示目前只是设计假设，需要在对应阶段用 spike 或测试确认。

---

## 0. 本次修订摘要

| # | 问题 | v1 的写法 | v2 的修订 |
|---|---|---|---|
| 1 | 并发语义自相矛盾 | §3.6 写“全部并发”，§9 又推荐“只读并发、其余顺序执行” | 新增 `concurrency_safe` 标注，和 `read_only` 分开；给 `execute_many` 定义按顺序分段执行的规则（§3.7） |
| 2 | 取消和返回结果的边界不清楚 | 只写了“CancelledError 继续向上抛” | 明确哪些情况返回 error 结果、哪些情况向上抛（§3.8），以及截止时间、单次超时、退避的计算方式 |
| 3 | 对同步工具的建议有误导 | 建议“有副作用的工具写成异步函数” | 删掉这条建议；改为说明“取消不能撤回已经发生的副作用”，并给出线程池额度的管理方式（§3.9） |
| 4 | 安全默认值缺失 | P0 没有写清楚最低安全策略 | 新增 P0 的最低安全基线（§6.1），以及每项推迟能力的风险和使用前提 |
| 5 | 承诺了做不到的事 | 写过“不影响执行”“绝不泄露” | 改成可以验证的说法：遥测异常会被吞掉并计数；脱敏是尽力而为；对已知的密钥值做精确匹配清除（§6.4） |
| 6 | 关于 MCP SDK 的事实错误 | 说它“依赖 httpx v1” | 已核实 `mcp 2.3.0` 依赖的是 `httpx2>=2.10`；按实际接口重写 MCP 相关内容（§5.3） |
| 7 | 循环依赖 | `ToolBinding` 放在 `adapters/base`，导致 registry 依赖 adapters | 移到 `binding.py`；重新划分依赖层级（§4） |
| 8 | 用 Pydantic 装运行时对象 | `ToolBinding`、`PythonTarget` 是 Pydantic 模型，却装着函数对象 | 区分可序列化的契约（Pydantic）和运行时绑定（dataclass）（§3.1） |
| 9 | 打包隔离被混为一谈 | 把 import 隔离当成了安装依赖的隔离 | 比较三种打包方式，推荐 uv workspace 子项目，并补充构建产物的验证（§2.2、§7.3） |
| 10 | SSRF 方案过于简单 | 只写了“白名单 + 禁止内网 IP” | 分层处理 URL、DNS、连接地址、重定向、代理；标明首期的限制（§5.2） |
| 11 | 夸大幂等键的作用 | 暗示带上 `Idempotency-Key` 就能保证幂等 | 改为“和下游协商的机制”，明确键的作用域和重试条件（§5.2） |

---

## 1. 仓库现状、设计边界与关键假设

### 1.1 仓库现状（已核实）

| 项 | 实际情况 |
|---|---|
| 工作区 | 分支 `main`，一个提交 `012c2a2`；唯一的未跟踪文件就是本文档。没有其他未提交的变更 |
| 语言与版本 | `requires-python >=3.11`；本地 `.venv` 为 Python 3.12.13；mypy 的 `python_version = 3.11` |
| 打包 | hatchling；wheel 的 `packages = ["provider_sdk", "provider_gateway"]`；sdist 的 `include = ["provider_sdk", "provider_gateway", "README.md", "pyproject.toml"]`（不包含 tests 和 docs） |
| 核心依赖的锁定版本 | `openai 3.46.0`、`httpx2 2.16.0`、`pydantic 2.14.2`、`structlog 26.1.0`、`opentelemetry-api 1.45.2`；开发依赖 `opentelemetry-semantic-conventions 0.66b2`、`anyio 4.16.0` |
| 没有的依赖 | `jsonschema`、`mcp` 都不在 `uv.lock` 中 |
| 规范 | ruff（E/F/W/I/B/UP/ASYNC/S/SIM/RUF/A/PT，行宽 100）；mypy strict，`packages = ["provider_sdk", "provider_gateway"]` |
| 测试 | pytest + pytest-asyncio（auto）；默认排除 `integration` 和 `container` 标记；`tests/conftest.py` 提供 InMemorySpanExporter、InMemoryMetricReader 和基于 httpx2 的 Mock；`tests/e2e/fake_provider.py` 是一个假的 Provider |
| Makefile | `lint`、`typecheck`、`test`、`test-integration`、`test-container`、`test-all` |

### 1.2 Provider SDK 中和工具相关的部分（已核实，本方案不修改）

| 对象 | 位置 | 和本方案有关的事实 |
|---|---|---|
| `ToolDefinition` | `provider_sdk/schemas/message.py` | `name` 必须匹配 `^[A-Za-z0-9_\-]+$`，长度 1–64；`parameters` 是 JSON Schema 字典；可选 `strict` |
| `ToolCall` | 同上 | `id`、`name`、`arguments`（原始 JSON 字符串）；`parsed_arguments()` 遇到非法 JSON 或非对象时抛 `ValueError` |
| `ToolResult` | 同上 | `tool_call_id`、`content: str \| list[ContentPart]`（只有文本和图片）、`is_error`；`to_message()` 在 `is_error=True` 且内容是字符串时**自动加上 `[tool error] ` 前缀** |
| 流式工具事件 | `schemas/stream.py` | `tool_call_start / delta / end` |
| 设计原则 | `docs/architecture.md` | “适配器只做协议转换……不执行工具” |
| Agent 循环 | `examples/messages_tools.py` | 仓库里**没有** Agent Runtime，只有这个示例循环 |
| 可以借鉴的模式 | `exceptions.py`、`observability/*` | 带 `safe_message`、`retryable`、`error_type` 的异常体系；遥测钩子出错会被吞掉并计数；`redaction.py`；`semconv.py` 锁定属性名并用测试核对 |

### 1.3 外部依赖的核实结果

| 项 | 结论 | 状态 |
|---|---|---|
| OTel 语义规范 0.66b2 | 有 `execute_tool` 操作名，以及 `gen_ai.tool.name`、`gen_ai.tool.call.id`、`gen_ai.tool.type`、`gen_ai.tool.description`、`gen_ai.tool.call.arguments`、`gen_ai.tool.call.result` 等属性 | 已核实（读取了已安装的包） |
| OTel 中工具专用的**指标** | 没有确认规范里有工具指标，暂时使用 `app.tool.*` 命名空间 | 待核实 |
| `httpx2.MockTransport` | 存在 | 已核实 |
| `httpx2.AsyncHTTPTransport` 的参数 | 包括 `verify`、`trust_env`、`proxy`、`limits`、`retries`、`socket_options` 等，**不能传入 network backend** | 已核实 |
| `httpcore2.AsyncConnectionPool` | 可以传入 `network_backend`；存在 `AsyncNetworkBackend` 抽象 | 已核实。可以用来在连接时检查 IP，但具体实现需要 P1 spike 验证 |
| `mcp`（PyPI） | 最新版 **2.3.0**（2026-10-02 发布），`requires_python >=3.10` | 已核实（PyPI 元数据，以及下载到临时目录的 wheel 源码，没有安装） |
| `mcp 2.3.0` 的依赖 | `httpx2>=2.10.0`、`jsonschema>=4.20`、`mcp-types==2.3.0`、`anyio>=4.9`、`opentelemetry-api>=1.28`、`pydantic>=2.12`、`pyjwt[crypto]`、`python-multipart`、`sse-starlette`、`starlette`、`uvicorn` 等 | 已核实。注意：只用客户端也会把服务端依赖一起装上 |
| `mcp 2.3.0` 的客户端接口 | `mcp.Client(server)`：`server` 可以是 URL 字符串（Streamable HTTP）、`StdioServerParameters`（stdio 子进程）、`Transport`，或进程内的 `Server`（用于测试）；还有更底层的 `ClientSession`；`call_tool(name, arguments, read_timeout_seconds, progress_callback, ...)`；`list_tools(cursor=...)` 支持分页和缓存 | 已核实（读源码） |
| `mcp 2.3.0` 的取消 | 放弃一个请求时，dispatcher 默认发送 `notifications/cancelled`（`cancel_on_abandon` 默认为 True）；`initialize` 等握手请求不会发送取消 | 已核实（读源码）。在 asyncio 取消场景下的实际表现，**待 P2 测试验证** |
| `mcp 2.3.0` 的 stdio | 只继承 `DEFAULT_INHERITED_ENV_VARS` 里的环境变量，再合并显式传入的 `env`；关闭时依次关 stdin、等待退出、SIGTERM、SIGKILL | 已核实 |
| MCP 工具名 | SDK 内置的校验规则是 `^[A-Za-z0-9._-]{1,128}$`，**允许 `.`，最长 128**，和 Provider 的规则（不允许 `.`，最长 64）冲突 | 已核实。所以必须做别名映射 |
| MCP 的 sampling、elicitation、roots 回调 | `Client` 上默认都是 `None`，不提供就不会启用 | 已核实 |

### 1.4 设计边界

1. **不修改 `provider_sdk` 和 `provider_gateway` 的任何代码。** 工具基础设施作为独立的代码单元，SDK 不知道它存在。
2. **不依赖任何具体的 Agent Runtime。** 只提供“给定一批工具调用，返回一批结果”的 API；推理循环、调用之间的业务依赖、给模型的提示词都由宿主负责。
3. **核心不 import Provider SDK。** 和 SDK 之间的数据转换放在一个可选的桥接模块里。
4. **借鉴 SDK 的模式，但不复用它的代码**（脱敏、异常结构、遥测钩子）。复制的部分记为技术债，等以后抽公共包时一起处理（那需要单独评审）。

### 1.5 关键假设

- 宿主进程里只有一个 asyncio 事件循环；对外 API 都是异步的。
- P0 的 Python 工具都是**宿主自己编写、可信的代码**，在进程内运行，和宿主拥有相同的权限。
- 模型返回的 `ToolCall.id` 不保证全局唯一（不同轮次之间可能重复），所以凡是需要唯一性的地方，都用 `(ctx.request_id, call_id)` 组合。

---

## 2. 目录结构与打包

### 2.1 目录结构（以推荐的打包方式 B 为例）

```
provider-model/
├── provider_sdk/                    已存在，不改动
├── provider_gateway/                已存在，不改动
├── pyproject.toml                   已存在；方式 B 只需新增 [tool.uv.workspace]（需要你确认，见 §9 D1）
├── packages/
│   └── agent-tools/                 【新增】独立的发行包 `agent-tools`
│       ├── pyproject.toml           自己的依赖、extras、ruff、mypy 配置
│       ├── agent_tools/
│       │   ├── __init__.py          公开 API；import 无副作用
│       │   ├── py.typed
│       │   ├── protocol/            ① 可序列化的契约（Pydantic），不依赖包内其他模块
│       │   │   ├── spec.py          ToolSpec / ToolAnnotations / ToolPolicy / ToolListing（HttpTarget、McpTarget 在 P1/P2 加入）
│       │   │   ├── invocation.py    ToolInvocation / ToolOutput / ToolExecutionResult / ContentBlock
│       │   │   ├── context.py       ToolContext / Principal
│       │   │   └── errors.py        ③ ToolError 异常体系 + ToolErrorInfo
│       │   ├── binding.py           运行时绑定（dataclass）：ToolBinding / PythonTarget
│       │   ├── registry.py          ① 注册表、不可变快照、别名表、启用的版本
│       │   ├── router.py            ② 在快照上做 别名/名称/版本 → Route 的解析
│       │   ├── （P0 无 pipeline.py：执行链的固定顺序直接实现在 executor.py 中，见 §10）
│       │   ├── executor.py          ① execute / execute_many / list_tools / aclose
│       │   ├── runtime/
│       │   │   ├── clock.py         可注入的 Clock 和 sleep（方便测试）
│       │   │   └── threads.py       有上限的线程池，以及“孤儿线程”额度的统计
│       │   ├── governance/          ③ 截止时间、重试、并发和限流（中间件）
│       │   ├── security/            ⑤ 鉴权、参数校验、输出限制、密钥、网络防护（中间件和工具函数）
│       │   ├── observability/       ④ 语义规范属性名、观察者中间件、审计、脱敏
│       │   ├── adapters/
│       │   │   ├── base.py          ToolAdapter 协议
│       │   │   ├── python.py        P0
│       │   │   ├── http.py          P1（extra：http）
│       │   │   └── mcp.py           P2（extra：mcp）
│       │   └── integrations/
│       │       └── provider_sdk.py  桥接（extra：provider-sdk），是唯一允许 import provider_sdk 的模块
│       ├── tests/                   unit / integration / e2e / packaging
│       └── examples/agent_loop.py   用执行器改写 messages_tools.py 的循环
└── agent-tools-architecture.md      本文档（评审通过后移到 docs/）
```

**五组能力和代码的对应关系**：
- Protocol、Registry、Executor：`protocol/`、`registry.py`、`executor.py`；
- Router、Context：`router.py`、`protocol/context.py`；
- Error、Governance：`protocol/errors.py`、`governance/`；
- Observability：`observability/`；
- Security：`security/`。

这些是逻辑上的分组，不是物理上必须严格分层。治理、安全、观测都以中间件的形式挂在 `pipeline` 上，执行顺序由 pipeline 固定下来（§5.1），不允许宿主随意调整顺序，以免出现“先记录日志再脱敏”这类错误。

### 2.2 打包方式比较

| | A：同一个发行包，新增顶层包 | **B：uv workspace 子项目（推荐）** | C：独立仓库 |
|---|---|---|---|
| 安装依赖隔离 | ❌ `pip install provider-sdk` 一定会装上 `agent_tools`；只想用工具包的人也会被迫装上 `openai`、`httpx2` | ✅ 两个发行包，依赖各自声明 | ✅ |
| import 隔离 | 只能靠 AST 测试保证 | AST 测试，加上安装层面的天然隔离 | ✅ |
| 发布与版本 | 共用一个版本号，工具包的改动也会让 SDK 的版本号变化 | 各自有版本，可以单独发布 | 各自有版本 |
| 对现有仓库的改动 | 修改 wheel、sdist、mypy 配置 | 根 `pyproject.toml` 新增 `[tool.uv.workspace]` 和开发依赖；`uv.lock` 会更新 | 无 |
| 本地开发和端到端测试 | 最简单 | 共用一个 lock 和虚拟环境，也比较简单 | 需要跨仓库协作 |
| 部署 | 一个镜像同时包含两者 | 宿主按需安装 | 宿主按需安装 |

**推荐 B 的原因**：要求里的“独立”需要在安装层面成立，A 做不到。B 的代价只是根目录配置多了几行，不会碰 SDK 的代码。
- 如果选 A，必须同时修改：wheel 的 `packages`、sdist 的 `include`、mypy 的 `packages`，并且通过 extras 隔离 MCP 依赖。即便如此，`agent_tools` 的核心依赖仍然会被所有安装 SDK 的人装上。
- 方式 C 等到需要独立的发布节奏时再考虑。

### 2.3 B 方式下 `agent-tools` 的依赖设计

| 分组 | 依赖 | 说明 |
|---|---|---|
| 核心 | `pydantic>=2.14,<3`、`opentelemetry-api>=1.45,<2`、`structlog>=26.1,<27` | 和 SDK 的取值范围保持一致，避免在同一个 workspace 里冲突 |
| `[http]` | `httpx2>=2.16,<3`、`jsonschema>=4.20,<5` | P1 |
| `[mcp]` | `mcp>=2.3,<2.4`（会间接带上 `jsonschema`、`starlette` 等） | P2。版本锁到次版本，因为 SDK 的 2.x 还在快速变化 |
| `[provider-sdk]` | `provider-sdk>=1.0,<2` | 桥接模块使用 |

---

## 3. 核心接口与数据契约

### 3.1 契约和运行时绑定分开

| 类型 | 实现方式 | 是否可序列化 | 举例 |
|---|---|---|---|
| 契约 | Pydantic v2，`extra="forbid"`，`frozen=True` | ✅（可以写日志、跨进程传、做快照） | `ToolSpec`、`ToolPolicy`、`ToolAnnotations`、`HttpTarget`、`McpTarget`、`ToolInvocation`、`ToolExecutionResult`、`ToolErrorInfo`、`ToolContext` |
| 运行时绑定 | `@dataclass(frozen=True, slots=True)` 或普通类 | ❌ | `ToolBinding`、`PythonTarget(fn)`、适配器、`httpx2` 客户端、MCP 会话、线程池 |

`ToolContext.deadline` 使用 `time.monotonic()` 的绝对时刻，**只在同一个进程内有意义**。如果要跨进程传递，换算成剩余秒数后再传。

### 3.2 工具描述

```python
class ToolAnnotations(BaseModel):  # 描述工具的语义；只有当执行器的规则引用某一项时，它才会产生效果
    read_only: bool = False  # 不修改外部状态（用于审批和展示）
    concurrency_safe: bool = False  # 可以和同一批里的其他调用并发执行（见 §3.7）
    idempotent: bool = False  # 用相同参数重复调用，效果和调用一次相同（重试的前提，见 §5.2）
    destructive: bool = False  # 不可逆的操作（P0 默认禁止注册，见 §6.1）
    open_world: bool = False  # 会访问外部系统


class ToolPolicy(BaseModel):
    access: Literal["scoped", "public"]  # 必填，没有默认值：工具的访问方式必须显式声明（§6.1）
    required_scopes: frozenset[str] = frozenset()  # access="scoped" 时不能为空
    timeout_s: float = 30  # 单次尝试的超时上限
    max_retries: int = 0
    max_concurrency: int | None = None  # 每个工具在进程内的并发上限
    max_input_bytes: int = 64 * 1024  # 原始参数的 UTF-8 字节数上限
    output_limits: OutputLimits = OutputLimits()  # 见 §6.3
    idempotency_header: bool = False  # HTTP 专用：下游明确支持 Idempotency-Key 时才打开


class ToolSpec(BaseModel):
    name: str  # 内部名，匹配 ^[a-z0-9][a-z0-9_.\-]{0,127}$，可以带命名空间
    version: str = "1"  # 不透明字符串，P0 不支持语义化版本范围
    description: str = Field(max_length=4096)
    input_schema: dict[str, Any]  # JSON Schema 2020-12，顶层必须是 type=object
    output_schema: dict[str, Any] | None = None
    kind: Literal["python", "http", "mcp"]
    annotations: ToolAnnotations = ToolAnnotations()
    policy: ToolPolicy
    alias: str | None = None  # 显式指定模型侧名字；不填就按 §3.5 的规则生成


class HttpTarget(BaseModel): ...  # 见 §5.2


class McpTarget(BaseModel):  # 见 §5.3
    server_id: str
    remote_name: str  # 远端原始工具名（可能包含 .，最长 128）
```

```python
@dataclass(frozen=True, slots=True)
class PythonTarget:
    fn: Callable[..., Any]  # 同步函数或异步函数
    args_model: type[BaseModel]  # 由签名推导，或者显式传入


@dataclass(frozen=True, slots=True)
class ToolBinding:
    spec: ToolSpec
    target: PythonTarget | HttpTarget | McpTarget
```

### 3.3 调用、上下文与结果

```python
class Principal(BaseModel):
    user_id: str | None = None
    tenant_id: str | None = None
    scopes: frozenset[str] = frozenset()


class ToolContext(BaseModel):  # 不可变；派生时用 model_copy(update=...)
    request_id: str  # 规则和 provider_sdk 的 RequestContext 相同：[A-Za-z0-9._:-]{1,128}
    principal: Principal
    deadline: float | None = None  # 本进程内 monotonic 的绝对时刻；None 表示不设整体截止时间
    session_id: str | None = None
    attributes: Mapping[str, str] = {}  # 只写日志和 Span，不作为指标标签，不传给工具
    # Trace 上下文通过 OTel contextvars 隐式传递


class ToolInvocation(BaseModel):
    call_id: str  # 一般就是 ToolCall.id
    tool: str  # 模型侧别名，或者内部名
    version: str | None = None  # 只有宿主可以指定；None 表示使用当前启用的版本
    arguments: dict[str, Any] | str  # str 时由执行器解析（解析失败返回 InvalidArguments）


ContentBlock = TextBlock | JsonBlock | ImageBlock  # 按 type 区分的判别联合


class TextBlock(BaseModel):
    type: Literal["text"]
    text: str
    truncated: bool = False


class JsonBlock(BaseModel):
    type: Literal["json"]
    data: JsonValue


class ImageBlock(BaseModel):
    type: Literal["image"]
    mime_type: str
    data_b64: str | None
    url: str | None


class ToolOutput(BaseModel):  # 适配器返回的结构
    content: list[ContentBlock]
    structured: dict[str, Any] | None = None


class ToolErrorInfo(BaseModel):  # 可以安全给模型或客户端看的错误信息
    type: str  # 低基数，例如 "timeout"
    code: str  # 稳定的机器可读错误码
    message: str  # safe_message
    retryable: bool


class ToolExecutionResult(BaseModel):
    call_id: str
    tool: str  # 实际执行的内部名
    alias: str
    version: str | None  # 解析失败时为 None
    status: Literal["success", "error"]
    content: list[ContentBlock] = []
    structured: dict[str, Any] | None = None
    error: ToolErrorInfo | None = None
    trust: Literal["untrusted"] = "untrusted"  # 工具输出一律标记为不可信（§6.5）
    attempts: int
    duration_ms: float
    trace_id: str | None
    span_id: str | None

    def to_model_text(self) -> str: ...  # 回传给模型的文本；出错时只包含 code 和 message
```

### 3.4 错误模型

结构和 `provider_sdk.exceptions` 相同：每个错误都有 `safe_message`、`code`、`retryable`、`error_type`，以及 `to_info() -> ToolErrorInfo`。原始异常保存在 `__cause__` 里，只用于排查，不会暴露给模型。

| 异常类 | error_type | 是否可重试 | 在哪一步产生 |
|---|---|---|---|
| `ToolNotFoundError`、`ToolDisabledError`、`ToolVersionNotFoundError` | `not_found` | 否 | 解析 |
| `InvalidArgumentsError` | `invalid_arguments` | 否 | 大小检查、解析、参数校验 |
| `PermissionDeniedError` | `permission` | 否 | 鉴权 |
| `ApprovalRequiredError`、`ApprovalRejectedError`（P3） | `permission` | 否 | 审批 |
| `RateLimitedError`、`ConcurrencyLimitedError` | `throttled` | 是 | 治理 |
| `ToolTimeoutError`（`code` 为 `attempt_timeout` 或 `deadline_exceeded`） | `timeout` | 单次超时可重试；整体截止时间到了则不可重试 | 执行 |
| `ToolCancelledError`（`code` 为 `executor_closed` 或 `remote_cancelled`） | `cancelled` | 否 | 见 §3.8 |
| `CircuitOpenError`（P3） | `circuit_open` | 是 | 治理 |
| `UpstreamError(status_code)` | `upstream` | 5xx、429、连接错误可重试；其余不可重试 | HTTP / MCP 适配器 |
| `ToolExecutionError` | `execution` | 否（工具代码可以抛 `ToolError` 子类来自己声明） | 适配器 |
| `OutputLimitError`、`OutputValidationError` | `output` | 否 | 输出处理 |
| `ToolInfraError` | `internal` | 否 | 基础设施自身的 bug，`safe_message` 是固定文案 |

**会直接抛出、不包装成结果的情况**（都属于调用方用错了 API，或者流程控制）：
- `ExecutorClosedError`：执行器已经关闭后还在调用；
- 构造 `ToolContext` 或 `ToolInvocation` 时的 `pydantic.ValidationError`；
- `asyncio.CancelledError`（见 §3.8）。

### 3.5 注册表、快照、别名和版本

```python
class ToolRegistry:
    def register(self, binding: ToolBinding) -> None            # 同名同版本已存在时抛 RegistryConflictError
    def register_function(self, fn, *, name: str, policy: ToolPolicy, ...) -> ToolSpec
    def set_active(self, name: str, version: str) -> None       # 指定给模型使用的版本
    def enable(self, name: str, enabled: bool = True) -> None   # 启停是针对 name 的，影响它的所有版本
    def unregister(self, name: str, version: str) -> None       # 不能删除当前启用的版本，要先 set_active 切换
    def snapshot(self) -> RegistrySnapshot                      # O(1)，返回当前版本的不可变快照

@dataclass(frozen=True)
class RegistrySnapshot:
    id: str                                  # 单调递增的快照版本号
    def resolve(self, ref: str, version: str | None) -> ToolBinding   # 按 别名 → 名称 → 启用版本 的顺序查找
    def visible(self, principal: Principal) -> list[ToolSpec]         # 只列出启用的版本，并按访问策略过滤
```

规则：

1. **写时复制**：每次注册、启停、切换版本都会生成一个新的内部状态，并让快照 id 加 1；已经拿到的快照不会改变。
2. **一次批量执行只用一个快照**：`execute_many` 在开始时取一次快照，整批调用都按它解析，同一批里不会出现版本不一致。
3. **展示给模型的列表和实际执行之间允许有差异**：`list_tools()` 返回 `ToolListing(snapshot_id, specs)`。如果在模型回复之前注册表发生了变化，执行时用的是新快照；被停用的工具会返回 `ToolDisabledError` 结果。如果宿主需要严格一致，可以把 `snapshot_id` 传给 `execute_many(..., snapshot_id=...)`；快照已经被新版本替换时会返回 `ToolNotFoundError`。执行器只保留最近 N 个快照（可配置，默认 8）。
4. **别名的生成规则**（确定性的）：
   - 优先使用 `spec.alias`；
   - 否则把 `name` 中不在 `[A-Za-z0-9_-]` 内的字符替换成 `_`；
   - 超过 64 个字符时，截断到 55 个字符，再加上 `_` 和 `sha256(name)` 的前 8 位十六进制。
   - 别名**只和 name 有关，和 version 无关**，所以升级版本不会改变模型看到的名字。
5. **别名冲突**：两个不同的 name 生成了相同的别名时，在**注册阶段**直接拒绝（`RegistryConflictError`），不在运行时偷偷改名。
6. **版本**：每个 name 有且只有一个启用的版本，模型只能看到它。模型的调用不能指定版本；只有宿主可以通过 `ToolInvocation.version` 调用其他版本，而且仍然要经过鉴权。

### 3.6 路由

```python
class ToolRouter:
    def __init__(self, adapters: Mapping[str, ToolAdapter]) -> None   # kind -> 适配器
    def route(self, snapshot: RegistrySnapshot, inv: ToolInvocation) -> Route

@dataclass(frozen=True)
class Route:
    binding: ToolBinding
    adapter: ToolAdapter
```

Router 不保存任何状态，只负责“在给定快照上做解析，并选择适配器”。P0 只按 `kind` 选择适配器；“多实例选择”“按租户路由”都是后续扩展，现在不做。

### 3.7 执行和并发语义

```python
class ToolExecutor:
    def __init__(self, registry: ToolRegistry, router: ToolRouter, *, config: ExecutorConfig,
                 authorizer: Authorizer | None = None, audit: AuditSink | None = None,
                 clock: Clock | None = None) -> None
    async def execute(self, inv: ToolInvocation, ctx: ToolContext) -> ToolExecutionResult
    async def execute_many(self, invs: Sequence[ToolInvocation], ctx: ToolContext, *,
                           mode: Literal["auto", "sequential"] = "auto",
                           snapshot_id: str | None = None) -> list[ToolExecutionResult]
    def list_tools(self, principal: Principal) -> ToolListing
    async def aclose(self, *, grace_s: float = 5.0) -> None
```

**`execute_many` 的规则**（`mode="auto"`）：

1. 结果顺序**始终和输入顺序一致**。
2. 按输入顺序把调用切成若干段：**连续的 `concurrency_safe=True` 的调用**组成一个并发段；遇到 `concurrency_safe=False` 的调用，它自己单独成为一段（相当于一个屏障）。
3. 各段**严格按顺序执行**：一段里的所有调用都结束之后，才开始下一段。并发段内部的并行数不超过 `ExecutorConfig.max_parallel`（默认 8）。
4. 前一个调用失败**不会**中断后续调用。每个调用的结果是独立的；要不要因为失败而停止，由 Runtime 根据结果自己决定。
5. 无法解析的调用（找不到工具、工具已停用）当作 `concurrency_safe=False` 处理，并直接产生 error 结果。
6. `mode="sequential"` 时，所有调用都逐个执行。

**为什么不能把 `read_only` 当成 `concurrency_safe`**：只读工具也可能不适合并发，比如共享一个有状态的连接，或者下游有严格的速率限制；反过来，写操作如果作用在互不相干的资源上，也可以并发。所以这两个标注分开，默认都是 False。默认值最保守：全部按顺序执行。

**调用之间的依赖由谁负责**：执行器只保证上面这条“分段按顺序”的规则。模型在同一轮里返回的多个调用之间是否存在先后依赖（例如先创建、再查询），执行器无法知道。由**宿主负责**：要么给相关工具保持 `concurrency_safe=False`，要么使用 `mode="sequential"`，要么在 Runtime 层拆成多轮。

**不同请求之间的并发**（多个任务同时调用 `execute`）不受上面的规则约束，只受 `ToolPolicy.max_concurrency` 和全局的 `ExecutorConfig.max_inflight` 限制（§6.2）。

### 3.8 失败结果、超时与取消的边界

| 情况 | 调用方看到的结果 |
|---|---|
| 工具层面的任何失败（找不到工具、参数错误、权限不足、限流、单次超时、整体截止时间已过、上游错误、工具抛出异常、输出超限） | 返回 `status="error"` 的结果，**不抛异常** |
| **调用方自己的任务被取消**（例如 Runtime 取消了这一轮） | `asyncio.CancelledError` **原样向上抛出**，不会转换成结果。`execute_many` 里还没结束的子任务会被一起取消，**不会返回部分结果**。观察者会把 Span 状态记为 `cancelled`，并释放所有并发额度（同步工具除外，见 §3.9） |
| 执行器关闭（`aclose`） | 不再接受新调用（再调用会抛 `ExecutorClosedError`）；在途的调用最多等待 `grace_s`；超时后，执行器自己创建的内部任务会被取消，对应的调用返回 `ToolCancelledError(code="executor_closed")` 结果 |
| 远端主动取消（MCP 服务端返回已取消） | 返回 `ToolCancelledError(code="remote_cancelled")` 结果 |

实现上的约束：每个调用都在执行器自己创建的内部任务里运行，调用方 await 它。这样“调用方被取消”和“执行器内部取消”在代码上可以区分开：内部取消通过一个标记位识别，转换成结果；外部取消原样抛出。所有中间件都**不能吞掉** `CancelledError`，测试会检查这一点。

**截止时间、单次超时和退避的计算**：

```
remaining(now)  = ctx.deadline - now                        （没有 deadline 时视为无穷大）
attempt_timeout = min(policy.timeout_s, remaining(now))
开始前：        remaining <= 0                → ToolTimeoutError(deadline_exceeded)，不调用适配器
重试前：        backoff = min(base * 2^(n-1) * jitter, max_backoff, Retry-After)
                如果 remaining(now) - backoff < min_attempt_s（默认 0.05s），就不再重试，返回最后一次的错误
排队等待：      等待并发额度或限流令牌的时间也计入 deadline；等不到时返回 ConcurrencyLimited 或 RateLimited
```

### 3.9 同步 Python 工具和“取消的局限”

- 同步函数在执行器**专用的、有上限的** `ThreadPoolExecutor`（默认 8 个线程）里运行，不使用事件循环默认的线程池，避免影响宿主的其他代码。
- **线程无法被强制终止。** 超时或调用方取消后，结果会立即返回（或者抛出 `CancelledError`），但线程会继续执行到函数结束。
- **额度的处理**：同步工具占用的并发额度和线程，要等到**线程真正结束**才释放，而不是在超时那一刻释放。这样可以防止超时的任务越积越多，把线程耗尽。这类线程会被计入 `app.tool.sync.orphaned` 计量指标；它们最终的结果会被丢弃，抛出的异常只写日志。线程池满了以后，新的同步调用会等待（时间计入 deadline），等不到就返回 `ConcurrencyLimitedError`。
- **协作式取消**（可选）：工具函数如果声明了 `cancel_event: threading.Event` 参数，超时或取消时执行器会把这个事件设置为已触发。工具可以自己检查这个事件，尽早退出。
- **取消不能撤回已经发生的副作用**，同步工具和异步工具都一样。对异步工具来说，取消只能打断“下一次 await”，已经发出去的请求、已经写入的数据不会被撤销。基础设施**不对此做任何保证**。有副作用的工具要在工具本身或者下游实现幂等、补偿或者对账，需要时再配合审批（P3）。

---

## 4. 依赖方向

```
                     ┌──────────────────────────────┐
                     │ 宿主 / Agent Runtime（仓库外） │
                     └────────┬───────────────┬─────┘
                              │               │
                              ▼               ▼
              ┌──────────────────┐   ┌──────────────────────────────────┐
              │ provider_sdk      │◄──│ agent_tools.integrations.        │ extra: provider-sdk
              │（不改动，不知道    │   │   provider_sdk（唯一的桥接模块）   │
              │  agent_tools 存在）│   └───────────────┬──────────────────┘
              └──────────────────┘                   │
 ┌───────────────────────── agent_tools ─────────────▼───────────────────────────┐
 │ L5  executor                                                                   │
 │ L4  pipeline ──► 中间件：security / governance / observability                 │
 │ L3  router ──► adapters.base（协议）◄── adapters.python / http / mcp           │
 │ L2  registry                                                                   │
 │ L1  binding、runtime（clock / threads）                                         │
 │ L0  protocol（spec / invocation / context / errors）                           │
 └────────────────────────────────────────────────────────────────────────────────┘
```

**规则**（每一条都有对应的测试，见 §7）：

1. 模块只能依赖**同层或更低层**的模块。`protocol` 不依赖包内的任何其他模块。
2. `adapters.*` 只能依赖 L0、L1 和 `adapters.base`，**不能依赖** registry、router、pipeline、中间件和 executor。
3. 中间件只能依赖 L0、L1 和 `pipeline` 中定义的协议，中间件之间不能互相依赖。
4. 除了 `integrations/` 之外，任何模块都不能 import `provider_sdk`、`provider_gateway` 或 `mcp`；`mcp` 只能出现在 `adapters/mcp.py` 中，而且要延迟导入，缺少这个 extra 时给出明确的报错。
5. `provider_sdk` 不知道 `agent_tools` 的存在。在方式 B 下，这一点由安装关系保证：SDK 的发行包根本不依赖 `agent-tools`。

---

## 5. 执行时序与适配器

### 5.1 执行链的固定顺序

```
Runtime → execute_many(invs, ctx)
  │ 取一次快照；按 §3.7 分段
  │ 对每个调用（在内部任务中执行）：
  │  0. Observer.begin      Span "execute_tool {alias}"（INTERNAL）；日志 tool.call.start（只写元数据）
  │  1. 原始大小检查         arguments 的 UTF-8 字节数 > max_input_bytes → InvalidArguments
  │  2. 解析                 route(snapshot, inv) → NotFound / Disabled / VersionNotFound
  │  3. 鉴权（工具级）       Authorizer.check(principal, spec)          → PermissionDenied   【审计：决定】
  │  4. 参数解析与校验        JSON 解析 + Schema 校验                     → InvalidArguments
  │  5. 鉴权（参数级，可选）  Authorizer.check_args(principal, spec, args)，由宿主实现（P1）
  │  6. 审批（P3）
  │  7. 治理：限流 → 并发额度（等待时间计入 deadline）
  │  8. 重试循环 { 计算单次超时 → 子 Span "attempt N" → adapter.invoke() → 映射错误 → 是否重试、退避 }
  │  9. 输出处理             归一化成 ContentBlock → 用完整数据做 output_schema 校验 → 大小限制（§6.3）
  │ 10. Observer.finish      Span 状态和属性；指标；日志 tool.call.finish 或 tool.call.error（经过脱敏）
  │ 11. 审计：结果           【审计：结果】
  ▼
list[ToolExecutionResult] → 桥接 → 写回对话历史
```

**这个顺序的理由**：
- 先检查原始大小，用最低的成本挡住超大的输入；
- **先鉴权、再做参数校验**：没有权限的调用方拿不到 Schema 相关的错误细节，也不会消耗校验的资源；
- 输出 Schema 校验在截断**之前**、对完整数据进行，避免因为截断导致校验结果失真；
- 脱敏**只作用于遥测和审计**，不改动返回给模型的结果。如果需要过滤模型侧的内容，宿主可以提供 `ResultFilter`（P1）。

### 5.2 HTTP 适配器（P1）

```python
class HttpTarget(BaseModel):
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    base_url: AnyHttpUrl  # 注册时固定；必须在 ExecutorConfig.http_allow_hosts 白名单里
    path_template: str  # 例如 "/customers/{id}"；参数只能填进 path、query、body
    arg_mapping: ArgMapping  # 每个参数放到 path / query / body 的哪里
    auth: SecretRef | None  # 例如 SecretRef("crm_token", header="Authorization", scheme="Bearer")
    success_status: frozenset[int] = frozenset({200, 201, 204})
    max_response_bytes: int = 1 << 20
```

**SSRF 防护（分层，并标明首期的限制）**：

| 层次 | 做法 | 首期状态 |
|---|---|---|
| URL | `scheme`、`host`、`port` 在注册时固定，模型只能提供参数；path 参数做百分号编码并拒绝 `..`；默认只允许 `https`，使用 `http` 需要显式开启 | P1 实现 |
| DNS | 每次连接都解析域名；所有解析结果都要检查，拒绝回环、私有网段、链路本地、云元数据地址（`169.254.169.254` 等）、组播和保留地址；可以配置允许的 CIDR | P1 实现 |
| 实际连接地址 | **连接时检查，并直接连接已经校验过的 IP**（防止 DNS 重绑定）。可行路径：自定义 `httpcore2.AsyncNetworkBackend`，在 `connect_tcp` 中解析、校验后连接 IP；TLS 的 SNI 和证书校验仍然使用原始主机名。因为 `httpx2.AsyncHTTPTransport` 不能传入 network backend，需要包装一个基于 `httpcore2.AsyncConnectionPool` 的传输层 | **待 P1 spike 验证**。如果做不到可靠，就退回到“只在连接前解析检查”，同时**要求**部署层面配置出站网络策略，并在文档中注明剩余的 DNS 重绑定风险 |
| 重定向 | `follow_redirects=False`；遇到 3xx 返回 `UpstreamError`。如果要支持同源重定向，就手动跟随，并对新地址重新做全部检查 | P1 实现 |
| 代理 | `trust_env=False`，不读取环境变量中的代理；只能显式配置代理。**使用代理时，连接时的 IP 检查看到的是代理的地址**，目标地址的控制要靠代理自己的 ACL，这一点在配置项文档里注明 | P1 实现 |
| 响应 | 流式读取，超过 `max_response_bytes` 就中断，返回 `OutputLimitError` | P1 实现 |

**`Idempotency-Key` 的作用和限制**：
- 这**只是和下游协商的机制**：只有下游明确实现了基于这个键的去重，它才有意义。基础设施自己不能保证幂等。
- 只有 `policy.idempotency_header=True`（宿主确认下游支持）时才会发送。
- 键的计算：`sha256(tenant_id | request_id | call_id | tool name | version)`。同一个逻辑调用的多次重试**使用同一个键**；不同请求、不同租户之间不会冲突。因为模型的 `call_id` 可能在不同轮次中重复，所以键里一定要带上 `request_id`。
- **重试条件**：错误可重试 **且** `annotations.idempotent=True`。对 `POST` 和 `PATCH`，还必须同时满足 `idempotency_header=True`。不满足这些条件时，**一律不重试**，哪怕错误本身是可以重试的。

### 5.3 MCP 适配器（P2，基于已核实的 mcp 2.3.0）

| 方面 | 设计 | 依据 |
|---|---|---|
| 连接 | 每个 `server_id` 对应一个由 `McpServerManager` 管理的 `mcp.Client`（§14 起可选每个（服务端，租户）一个）；传输方式只支持 `StdioServerParameters`（命令由宿主配置）和 Streamable HTTP（URL 由宿主配置）。旧版 SSE 传输不支持 | 已核实 `Client` 支持这几种 `server` 参数 |
| 生命周期 | 启动时连接，或者第一次使用时连接；断开后按退避策略重连，重连期间的调用返回 `UpstreamError(retryable=True)`；`aclose` 时关闭所有会话（stdio 会按 SDK 自己的流程结束子进程） | 重连的行为**待 P2 测试验证** |
| 调用 | `client.call_tool(remote_name, args, read_timeout_seconds=单次超时)`；结果的 `isError=True` → `ToolExecutionError`；协议或传输错误 → `UpstreamError` | 已核实接口签名 |
| 取消 | 依赖 SDK 在放弃请求时发送 `notifications/cancelled` 的行为。服务端是否真的停止执行，由服务端决定，**不做保证** | 已核实是默认行为，asyncio 下的实际表现**待测试** |
| 发现工具 | `list_tools` 按 cursor 分页；只有**宿主配置的白名单**（`server_id → 允许的 remote_name`）里的工具才会被注册；新出现的工具默认**不会**自动注册 | 已核实支持分页 |
| 命名 | 内部名 `mcp.{server_id}.{remote_name}`，模型侧别名按 §3.5 生成（MCP 的名字允许 `.`，最长 128） | 已核实名字规则 |
| 回调 | **不提供** sampling、elicitation、roots 回调，否则服务端可以反过来驱动模型或者读取本地信息；如果服务端返回需要输入的结果，当作 `ToolExecutionError` 处理 | 已核实这些回调默认是 `None` |
| stdio 环境变量 | 只额外传入宿主显式列出的变量（SDK 默认只继承一组安全变量）；`cwd` 由宿主配置 | 已核实 |

**信任边界**：
- MCP 服务端视为**半可信的外部方**；
- 它提供的**工具描述和 Schema 都是不可信输入**（可能包含诱导模型的指令）：注册时限制描述长度（4096）、Schema 的大小和嵌套深度，宿主可以用自己的描述覆盖服务端的描述；
- **服务端身份**：stdio 方式由宿主配置的命令保证；HTTP 方式要求 `https`，凭证通过 SecretProvider 获取；
- 工具的输出按 §6.5 视为不可信内容。

### 5.4 三类适配器对比

| | Python（P0） | HTTP（P1） | MCP（P2） |
|---|---|---|---|
| 信任级别 | 可信（宿主代码） | 半可信（下游服务） | 半可信（第三方服务端） |
| Schema 来源 | 由 Pydantic 模型生成 | 注册时手写 | 远端提供，宿主用白名单筛选 |
| 参数校验 | Pydantic（有权威性） | `jsonschema` Draft 2020-12 | `jsonschema` Draft 2020-12（mcp 本身也依赖它） |
| 生命周期 | 无 | 一个共享连接池 | 每个服务端一个会话，需要重连 |
| 取消 | 异步函数：在 await 处中断；同步函数：线程继续运行（§3.9） | 关闭连接；请求可能已经被下游处理 | 发送取消通知，服务端可能忽略 |
| 主要风险 | 和宿主同等的权限 | SSRF、凭证泄露、非幂等的重试 | 指令注入、服务端被冒充、依赖较重 |

---

## 6. 安全、治理与可观测性

### 6.1 P0 的最低安全基线（P0 必须实现）

| 项 | P0 的默认行为 |
|---|---|
| 访问声明 | `ToolPolicy.access` **必填**，没有默认值。`scoped` 时 `required_scopes` 不能为空。`public` 表示任何 Principal 都可以调用，必须显式写出来 |
| 鉴权 | **列出工具和执行工具时都会检查**。列表过滤只是为了减少模型看到的工具，**真正的权限控制在执行时**；即使模型调用了一个没出现在列表里的工具名，也会在第 3 步被拒绝 |
| 不可逆操作 | `annotations.destructive=True` 的工具在 P0 **默认禁止注册**（因为审批要到 P3 才有）。宿主可以显式设置 `ExecutorConfig.allow_destructive_without_approval=True` 来放开，并且要自己承担这个风险 |
| 输入 | 原始参数默认最大 64 KiB；必须是 JSON 对象；用 Pydantic 校验 |
| 输出 | 按类型限制大小（§6.3） |
| 错误信息 | 工具抛出普通异常时，模型只能看到 `execution` 类型和固定的错误文案；原始异常信息只写进本地日志（经过脱敏） |
| 日志 | 默认模式是 `metadata`：只记录工具名、状态、耗时、参数和输出的**字节数与哈希**，**不记录内容**。`redacted` 和 `full` 模式需要显式开启 |
| 密钥 | P0 没有 SecretProvider，Python 工具通过宿主的闭包或者依赖注入拿到凭证，**不能**把凭证放进 `ToolSpec` 或参数。风险是基础设施无法对这些凭证做精确匹配清除，所以要求它们不能出现在工具输出里 |

**P0 推迟的能力，以及使用前提**：

| 推迟的能力 | 推迟到 | 在那之前的风险 | 使用前提 |
|---|---|---|---|
| HTTP、MCP 适配器（包括 SSRF 防护） | P1 / P2 | 无（P0 不能注册这两类工具） | — |
| 限流 | P1 | 单个工具可能被模型高频调用 | P0 依靠 `max_parallel`、`max_concurrency`，以及 Runtime 的轮次上限 |
| AuditSink | P1 | 只有日志，没有专门的审计记录 | 不用于有合规审计要求的场景 |
| 审批 | P3（已实现，§13） | 无法实现“人工确认” | 不可逆工具默认禁止注册；P3 起声明 `policy.approval` 即可注册 |
| 熔断 | P3（已实现，§13，默认关闭） | 下游故障时会持续失败 | 依靠超时和不重试的默认值 |
| 沙箱 | P3（部分：进程隔离，§13.2；不是完整沙箱） | Python 工具和宿主拥有同等权限 | 只注册可信代码；需要强隔离时用外部沙箱命令前缀 |

### 6.2 治理（P1 起完善）

| 项 | 做法 | 基础设施负责 | 宿主负责 |
|---|---|---|---|
| 超时、截止时间、退避 | 按 §3.8 计算 | ✅ | 设置 `ctx.deadline` 和各工具的策略 |
| 重试 | 只有“可重试 **且** 幂等（HTTP 的 POST/PATCH 还要求 idempotency_header）”才重试；默认 `max_retries=0` | ✅ | 声明幂等，确认下游的能力 |
| 并发 | 全局 `max_inflight`、每个工具的 `max_concurrency`、批内的 `max_parallel`；同步工具的额度要等线程结束才释放（§3.9） | ✅ 单进程 | 多实例部署时整体的容量规划 |
| 限流（P1） | 进程内的令牌桶，按 `(工具, 租户)` 分开计数；`RateLimiter` 是一个协议 | ✅ 单进程 | 分布式的实现（例如 Redis） |
| 熔断（P3） | 按工具或 endpoint 统计 | ✅ | 阈值 |

### 6.3 不同输出类型的大小限制

```python
class OutputLimits(BaseModel):
    max_total_bytes: int = 256 * 1024  # 所有内容块合计
    max_text_bytes: int = 64 * 1024  # 单个文本块
    max_json_bytes: int = 128 * 1024  # 单个 JSON 块序列化后的大小，以及 structured 的大小
    max_image_bytes: int = 2 * 1024 * 1024  # 单张图片（解码后），不计入 max_total_bytes
    max_images: int = 4
```

| 类型 | 超出限制时 | 理由 |
|---|---|---|
| 文本 | 在 UTF-8 字符边界处截断，末尾加上 `…[truncated N bytes]`，并设置 `TextBlock.truncated=True` | 截断后的文本仍然是合法文本；模型能看到它被截断了 |
| JSON、`structured` | **不截断**，直接返回 `OutputLimitError` | 截断会破坏 JSON 结构；output_schema 校验是对完整数据做的 |
| 图片 | **不截断，也不压缩**，直接返回 `OutputLimitError` | 截断的图片是无效数据 |
| 合计 | 超过 `max_total_bytes` 时返回 `OutputLimitError` | 防止单个块都没超、但合计很大的情况 |

宿主如果希望返回“摘要”而不是报错，可以配置 `on_oversize` 钩子（P3 已实现，见 §13.2）。默认仍只会报错或截断文本。

### 6.4 日志、脱敏和故障时的行为

- **脱敏的范围**：脱敏是**基于规则的、尽力而为的**（规则参考 SDK 的 `redaction.py`），**不承诺“绝不泄露”**。P1 增加一项可以验证的保证：本次调用中通过 SecretProvider 获取的密钥值，会在这次调用产生的日志、Span 和审计记录中**按原值精确匹配并清除**。如果工具在输出里自己返回了密钥，基础设施无法识别，这一点在文档中写明。
- **遥测出错**：观察者的每个钩子都用 `try/except Exception` 包起来，出错时吞掉异常并累加计数（`app.tool.telemetry.failures`），**不改变执行结果**。`BaseException`（例如 `CancelledError`）不会被吞掉。exporter 的阻塞和背压由 OTel SDK 和日志队列处理，基础设施不额外承诺。
- **审计出错**（P1）：通过 `ExecutorConfig.audit_failure_mode` 配置：
  - `fail_open`（**推荐的默认值**）：照常执行，记录失败的指标和本地错误日志；
  - `fail_closed`：如果第 3 步“授权决定”的审计写入失败，调用返回 `ToolInfraError`，**不执行工具**；结果的审计写入失败时，结果照常返回（因为工具已经执行完了），同时计数。
  - 审计写入有超时（默认 1 秒），超时按失败处理。

### 6.5 不可信输出与提示注入

| 基础设施提供的 | 宿主或模型侧负责的 |
|---|---|
| 所有结果都带 `trust="untrusted"` 和来源（工具名、kind） | 决定是否给工具输出加隔离包装，以及系统提示词怎么写 |
| 桥接模块提供可选的 `wrap_untrusted()` 辅助函数，用固定的分隔符包住内容，默认**关闭** | 判断工具输出能不能影响后续的工具调用（例如需要人工确认） |
| MCP 工具描述的长度和白名单控制（§5.3） | 选择哪些服务端值得信任 |

基础设施**不承诺**能阻止提示注入。

### 6.6 可观测性

- **Span**：`execute_tool {alias}`，kind 为 INTERNAL，父级是当前上下文。属性包括 `gen_ai.operation.name=execute_tool`、`gen_ai.tool.name`（别名）、`gen_ai.tool.call.id`、`app.tool.kind`、`app.tool.version`、`app.tool.status`、`app.tool.attempts`、`error.type`。`gen_ai.tool.type` 的取值待按规范核对。**默认不记录** `gen_ai.tool.call.arguments` 和 `gen_ai.tool.call.result`，只有在 `full` 日志模式下，并且经过脱敏后才记录。每次重试有子 Span `attempt N`。
- **指标**（`app.tool.*`，待和规范核对）：`app.tool.duration`（直方图）、`app.tool.calls`（计数器）、`app.tool.inflight`、`app.tool.sync.orphaned`、`app.tool.telemetry.failures`。标签只有：别名（未注册的统一记为 `_other`）、kind、status、error.type。
- **日志**：`tool.call.start`、`tool.call.finish`、`tool.call.error`，使用 structlog，带上 trace_id 和 span_id。
- **审计**（P1）：`AuditSink.emit(AuditEvent)`。事件类型有 `decision`（允许或拒绝，附原因）、`approval`（P3）、`result`。**不采样**；参数只记录哈希和大小。

---

## 7. 测试方案

### 7.1 测试分层

| 类别 | 关键用例 | 隔离方式 |
|---|---|---|
| 契约 | 模型序列化往返；`extra="forbid"`；错误码和 `error_type` 的完整列表（快照测试）；`to_model_text()` | 纯单元测试 |
| 注册表、别名、版本 | 别名生成（包括超长名字和哈希后缀）；**别名冲突在注册时被拒绝**；`set_active` 和 `unregister` 的约束；同一批调用使用同一个快照；`snapshot_id` 失效时的行为 | 纯单元测试 |
| 执行和并发 | §3.7 的分段规则（混合排列 `concurrency_safe`）；结果顺序；`max_parallel` 的峰值（用计数器检查）；一个失败不影响其他调用；`mode="sequential"`；100 个并发 `execute` 之间的 Span 和上下文互不干扰 | 注入 Clock |
| 取消 | **调用方取消 → 抛出 CancelledError，不返回部分结果，子任务全部取消，额度全部释放**；`aclose` 的宽限期和 `executor_closed` 结果；没有任何中间件吞掉 CancelledError（向每个中间件注入取消） | `asyncio` 事件和 Future 控制时序 |
| 超时和重试 | 单次超时和整体截止时间的区分；退避不会超过截止时间；不幂等的工具不重试；POST/PATCH 的额外条件；Retry-After | 注入 Clock 和 sleep，不真正等待 |
| 同步工具 | 超时后线程继续运行时，额度要等线程结束才释放；`orphaned` 指标；线程池满时返回 `ConcurrencyLimited`；`cancel_event` 被设置 | 用 `threading.Event` 控制线程 |
| 安全 | 必须显式声明 `access`；没在列表里的工具也会在执行时被拒绝；先鉴权再校验（没有权限时看不到校验错误）；输入大小限制；`destructive` 默认不能注册；`metadata` 日志模式下没有内容；（P1）密钥值的精确清除；异常信息不出现在模型看到的文本里 | 纯单元测试 |
| 输出 | 文本按 UTF-8 边界截断（包括多字节字符）；JSON 和图片超限时报错；合计超限；output_schema 在截断前校验 | 纯单元测试 |
| 可观测性 | Span 名称和属性与锁定的语义规范一致（仿照 SDK 的 semconv 测试）；指标标签都是低基数；遥测故障注入后结果不变 | 复用 InMemorySpanExporter 和 InMemoryMetricReader |
| 审计（P1） | `fail_open` 和 `fail_closed` 两种模式的行为；写入超时 | 假的 AuditSink |
| HTTP（P1） | 参数映射和转义；状态码到错误的映射；重定向不跟随；内网地址、元数据地址、DNS 重绑定（用假的解析器）；代理配置；响应大小上限；Idempotency-Key 的计算和发送条件 | `httpx2.MockTransport`、假的 network backend 和 DNS |
| MCP（P2） | 白名单发现和分页；`isError` 的映射；不提供 sampling 和 elicitation；取消时发送通知；断开后重连 | `mcp.Client` 的进程内 `Server` 模式（已核实支持）；stdio 用测试脚本子进程，打 `integration` 标记 |
| 依赖方向 | AST 扫描检查 §4 的规则 1–4；`import agent_tools` 之后 `sys.modules` 里没有 `provider_sdk` 和 `mcp` | 纯单元测试 |
| 桥接 | 工具名、参数、结果、错误的映射；`is_error` 时不会重复加 `[tool error]` 前缀；参数是非法 JSON 时得到 InvalidArguments 结果而不是异常 | 纯单元测试 |
| 端到端 | 用 `tests/e2e/fake_provider.py` 驱动“两轮工具调用 + 最终回答”；在同一个 Trace 里能看到 chat → execute_tool → chat | 本地的假 Provider |

### 7.2 外部服务隔离

真实的 HTTP 服务、MCP 服务端和 Provider 都打 `integration` 标记，默认不运行，沿用现有的 `addopts`。

### 7.3 打包测试（新增，打 `packaging` 标记）

1. `uv build packages/agent-tools` 生成 wheel 和 sdist；
2. 检查 wheel 的文件列表：包含 `agent_tools/**` 和 `py.typed`，**不包含** `tests/`、`provider_sdk/`；
3. 检查 sdist 包含 `pyproject.toml`、`README` 和源码；
4. 在**临时虚拟环境**里分别安装：
   - 不带 extras 的 wheel：`import agent_tools` 成功；`import agent_tools.adapters.mcp` 报错，并提示安装 `agent-tools[mcp]`；`provider_sdk` 和 `openai` **没有**被安装；
   - `[provider-sdk]`：桥接模块可以 import；
   - `[http]`、`[mcp]`：对应的适配器可以 import；
5. 对安装后的包跑 mypy，确认类型信息可以被使用方识别。

---

## 8. 分阶段实施

| 阶段 | 范围 | 新增依赖 | 验收标准 |
|---|---|---|---|
| **P0 最小可用链路** | `protocol`（全部契约和错误）、`binding`、`registry`（快照、别名、启用的版本）、`router`、`pipeline`（§5.1 的第 0–4、7（只有并发部分）、8（只执行一次，不重试；重试放到 P1）、9、10 步）、`executor`（`execute`、`execute_many`、`aclose`）、**Python 适配器**（同步和异步）、**§6.1 的最低安全基线**、按类型的输出限制、观察者（Span、指标、`metadata` 模式日志）、桥接模块、`examples/agent_loop.py`、打包骨架（方式 B） | 无新增运行时依赖（P0 只用核心依赖） | ① 端到端：假 Provider 驱动两轮工具调用（同一批里包含一个并发段和一个屏障调用）直到得到最终回答，一个 Trace 里能看到完整链路；② §7.1 中契约、注册表、执行和并发、取消、同步工具、安全、输出、可观测性、依赖方向、桥接这几类测试全部通过；③ §7.3 的打包测试通过（不带 extras 时不会装上 provider_sdk）；④ 在 `packages/agent-tools` 下 `ruff` 和 `mypy --strict` 通过；⑤ 仓库根目录 `make lint typecheck test` 的结果和改动前一致（SDK 不受影响） |
| **P1 HTTP 与治理** | HTTP 适配器、`jsonschema` 校验（Draft 2020-12，不加载远程 `$ref`，`format` 只作为注释）、SecretProvider 和密钥值精确清除、SSRF 分层防护（**连接时检查 IP 先做 spike**）、重试和 Idempotency-Key、限流、AuditSink 和故障模式、`redacted` / `full` 日志模式、参数级鉴权钩子、`ResultFilter` | `httpx2`、`jsonschema`（放在 `[http]` extra） | HTTP 测试全部通过（包括 DNS 重绑定的用例，或者 spike 失败时退回方案的文档和测试）；审计两种故障模式的测试通过；密钥精确清除的测试通过 |
| **P2 MCP** | `McpServerManager`、MCP 适配器、白名单发现、命名映射、取消通知、重连 | `mcp>=2.3,<2.4`（放在 `[mcp]` extra） | 用进程内 Server 的测试全部通过；不安装 `[mcp]` 时打包测试仍然通过；在 asyncio 下取消的实际表现和重连行为已经测试并写进文档 |
| **P3 扩展** | 熔断、审批（交互方式待定）、按 call_id 缓存结果、分布式限流的参考实现、子进程沙箱、`on_oversize` 钩子 | 按需 | 按功能逐项验收（实现记录见 §13；分布式限流参考实现于 2026-11-10 补充，见 §14.4，Lua 脚本未在真实 Redis 上验证） |

---

## 9. 需要你决策的问题

| # | 决策 | 推荐 | 理由 | 如果选其他方案的影响 |
|---|---|---|---|---|
| **D1** | 打包方式 | **B：uv workspace 子项目**（需要在根 `pyproject.toml` 增加 `[tool.uv.workspace]`，`uv.lock` 会更新） | 只有它能在安装层面做到独立，而且不改 SDK 代码 | 选 A：需要改 wheel、sdist、mypy 配置，SDK 的使用者会被迫装上工具包；选 C：需要另建仓库 |
| **D2** | P0 的访问控制默认值 | **`access` 必填，没有默认值** | 安全默认值是显式的，可以验证；代价是每个工具多写一行配置 | 默认 `public`：上手更快，但一不小心就会把工具暴露给所有人 |
| **D3** | `execute_many` 的默认并发规则 | **`concurrency_safe` 默认 False，按 §3.7 分段执行** | 默认最保守；需要并发的工具显式声明 | 默认全部并发：吞吐更高，但工具之间的依赖和副作用需要宿主自己保证 |
| **D4** | 不可逆工具在 P0 的处理 | **默认禁止注册，可以显式放开** | P0 没有审批能力 | 允许注册：需要宿主自己做确认 |
| **D5** | 审计故障的默认模式（P1） | **`fail_open`**，可以配置成 `fail_closed` | 审计不可用时不影响业务；有合规要求的场景可以切换 | 默认 `fail_closed`：更安全，但审计系统出问题时所有工具都无法执行 |
| **D6** | 是否接受 MCP extra 的依赖体积 | **接受，作为可选 extra，并锁定 `<2.4`** | 官方 SDK 已经使用 httpx2，和本仓库兼容；只用客户端也会带上 starlette、uvicorn 等服务端依赖，所以必须是可选的 | 不接受：需要自己实现 MCP 客户端协议，工作量和维护成本都很高 |
| **D7** | SSRF 连接时检查的 spike 失败时 | **退回到“连接前解析检查 + 要求部署层面配置出站网络策略”**，并在文档中写明剩余风险 | 对 httpx2 的自定义 network backend 目前只验证了接口存在，可靠性没有验证 | 不接受这个退回方案：HTTP 适配器要等到找到可靠方案才能交付 |

**以下细节已经直接决定，不需要你确认**：
- 发行包名 `agent-tools`，import 包名 `agent_tools`；
- 对外 API 只提供异步版本；
- 工具失败返回 error 结果，调用方被取消时抛出 `CancelledError`；
- 别名只和 name 有关，冲突时在注册阶段拒绝；
- 先鉴权、再做参数校验；
- JSON 和图片超限时报错而不是截断；
- 默认日志模式是 `metadata`；
- 脱敏代码复制一份，不复用 SDK 的；
- 不在网关上增加工具相关的接口。

---

## 10. P0 实现记录（2026-11-09）

P0 已按本文档实现在 `packages/agent-tools/`。实现过程中对文档做了以下**最小修正**，以代码为准：

| # | 文档原写法 | 实际实现 | 原因 |
|---|---|---|---|
| 1 | §2.1 有 `pipeline.py`，以通用 Middleware 链组装执行步骤 | 没有通用中间件链；§5.1 的固定顺序直接写在 `ToolExecutor._pipeline` 中，扩展点是具体协议（`Authorizer`、`ToolAdapter`），P1 再按需加入 `RateLimiter`、`AuditSink` 等 | 顺序本来就不允许宿主调整，通用中间件链在 P0 没有用处，反而增加出错面 |
| 2 | §5.1 第 1 步“原始大小检查”使用工具自己的 `max_input_bytes` | 解析前先用全局硬上限 `ExecutorConfig.max_raw_input_bytes`（默认 1 MiB）；鉴权之后再用工具的 `policy.max_input_bytes` 检查 | 解析工具之前还不知道是哪个工具，拿不到它的上限 |
| 3 | §6.1 `allow_destructive_without_approval` 放在 `ExecutorConfig` | 放在 `ToolRegistry(...)` 的构造参数中 | 检查发生在注册时，注册表才是做这个判断的地方 |
| 4 | §3.8 只区分“单次超时”和“整体截止时间” | 新增：超时发生时如果工具**还没开始执行**（例如同步工具一直等不到线程），返回 `ConcurrencyLimitedError(code="start_timeout")`（可重试）。适配器通过 `AttemptScope.mark_started()` 标记开始；线程池不再有自己的槽位超时 | 测试发现线程池等待与单次超时共用同一个截止时间，两者竞争会把“没有空闲线程”误报为“工具超时” |
| 5 | §3.2 `ToolSpec` 包含 `output_schema`，`ToolPolicy` 包含 `max_retries`、`idempotency_header` | P0 没有这些字段 | P0 不做输出 Schema 校验和重试；放着不生效的字段会误导使用者。P1 加入（新增字段向后兼容） |
| 6 | §6.6 每次尝试有子 Span `attempt N` | P0 没有子 Span，只在 `execute_tool` Span 上记录 `app.tool.attempts` | P0 不重试，只有一次尝试 |
| 7 | §3.7 `list_tools` 返回 `ToolListing(snapshot_id, specs)` | 返回 `ToolListing(snapshot_id, tools: list[ToolDescriptor])`，`ToolDescriptor` 带模型侧别名 | 桥接需要别名，而 `ToolSpec.alias` 在未显式指定时为空 |
| 8 | 未说明 | 一次 `execute_many` 超过 `max_batch_size`（默认 64）的调用直接返回 `ConcurrencyLimitedError(code="batch_too_large")` 结果，前面的调用照常执行 | 调用数量由模型输出决定，需要上限 |
| 9 | 未说明 | 授权实现抛出异常时失败即拒绝：列表中隐藏该工具，执行返回 `internal` 错误结果 | 安全默认值 |
| 10 | 未说明 | Python 工具的类型注解必须能在函数所在模块的全局作用域中解析（`from __future__ import annotations` 下，函数内部定义的类型无法解析） | `typing.get_type_hints` 的限制，注册时会明确报错 |
| 11 | §3.1 `Clock` 可注入以“不真正等待” | P0 的 `Clock` 只用于计算剩余时间；`asyncio.timeout` 使用事件循环自己的时钟，所以超时测试使用真实的短超时（几十毫秒） | P0 没有退避等待，没有必要引入可控事件循环 |

**P0 交付范围**：协议与错误模型、注册表（快照、别名、版本、启停）、路由、执行器（单次与批量、分段并发、超时、取消、关闭）、全局与单工具并发额度、同步工具专用线程池与孤儿线程额度、Python 适配器、§6.1 的安全基线、按类型的输出限制、Span / 指标 / 元数据日志、Provider SDK 桥接、端到端示例、打包测试。

**仍按原计划推迟到 P1 及以后**：HTTP 与 MCP 适配器、`jsonschema` 校验、重试与幂等键、限流、AuditSink、`redacted` / `full` 日志模式、SecretProvider、参数级鉴权钩子、审批、熔断、沙箱。

---

## 11. P1 实现记录（2026-11-09）

P1（HTTP 与治理）已实现在 `packages/agent-tools/`，与 P0 兼容（P0 的全部测试保留并通过）。
审计接收端与参数级授权在本阶段开始前已有未提交实现，本阶段在其基础上完善，没有重写。

### 11.1 交付内容

| 能力 | 位置 | 说明 |
|---|---|---|
| HTTP 适配器 | `adapters/http.py` | 参数映射到路径 / 查询 / JSON 体（不能映射到请求头）、共享连接池、流式响应大小限制、状态码与传输错误映射、`Retry-After` 解析 |
| 出站网络策略 | `security/net.py` | 主机白名单（含 `*.` 子域通配）、端口、内网网段放行、全部解析结果检查 |
| 连接时 IP 检查 | `adapters/http.py` `GuardedNetworkBackend` | 每次建立连接时解析、检查并直接连接已校验 IP；TLS SNI 仍用原始主机名 |
| JSON Schema | `security/schema.py` | Draft 2020-12、仅本地 `$ref`、拒绝远程检索、`format` 只作注释、大小与深度上限、错误不含值 |
| 重试与退避 | `governance/retry.py`、`executor.py` | 指数退避加抖动、`Retry-After`、截止时间检查；一次调用只占一份并发额度 |
| 幂等键 | `executor.py` `_idempotency_key` | 作用域：租户 + 请求 + 调用 + 工具 + 版本；只有 `idempotency_header=True` 时发送 |
| 速率限制 | `governance/ratelimit.py` | `RateLimiter` 协议 + 进程内令牌桶；按（工具，租户）计数；等待计入截止时间；取消时退还令牌 |
| 密钥 | `security/secrets.py` | `SecretProvider` 协议；本次调用的已知密钥值精确清除 |
| 日志模式 | `observability/observer.py`、`observability/redaction.py` | `redacted` / `full` 预览：精确清除 → 凭证格式清除 →（redacted）个人信息清除 → 截断 |
| ResultFilter | `security/result_filter.py` | 改写返回给模型的成功结果；失败即关闭 |
| 审计与参数级授权 | `observability/audit.py`、`executor.py` | 结果事件补充错误类型、错误码、尝试次数；补充拒绝、超时、取消、参数级审计失败的测试 |

### 11.2 SSRF 技术验证结论

“连接时检查并使用已校验 IP”**可以实现**，已作为默认模式：

* `httpcore2.AsyncConnectionPool` 支持自定义 `network_backend`；`connect_tcp` 收到的是原始主机名，自定义后端可以先解析、检查，再连接已校验 IP；
* TLS 握手时 httpcore 使用 `server_hostname=原始主机`，所以 SNI 与证书校验不受影响（已读源码确认，并在测试中对 httpcore 层断言；**没有**用真实 TLS 连接做端到端验证，因为当前环境不允许本地 TCP 连接）；
* `httpx2.AsyncHTTPTransport` 不能传入网络后端，实现方式是在子类中替换其私有属性 `_pool`。构造时检查该属性存在且类型符合预期，否则拒绝启动；`httpx2` 锁定在 `<3`；
* 已测试：解析到私有、回环、元数据、IPv4 映射回环地址被拒绝；公网与私有地址混合时整体拒绝；同一主机第二次连接解析到回环地址（DNS 重绑定）时被拒绝；显式放行的内网网段可以访问；重定向不跟随；
* **剩余风险**：依赖 `httpx2` 内部结构，升级时必须重新验证；每次连接只按解析顺序尝试已校验地址（没有 Happy Eyeballs）；**配置代理时退回到发送前预检，无法防止 DNS 重绑定**，必须依靠代理的访问控制。无论哪种模式，都建议在部署层面配置出站网络策略作为纵深防御。

### 11.3 与文档原写法不一致之处

| # | 原写法 | 实际实现 | 原因 |
|---|---|---|---|
| 1 | §5.2 主机白名单放在 `ExecutorConfig.http_allow_hosts` | 放在 `HttpAdapterConfig.network`（`NetworkPolicy`） | 白名单只与 HTTP 适配器有关，放在执行器配置里会让核心依赖 HTTP 概念 |
| 2 | §3.2 `ToolPolicy.idempotency_header` | 放在 `HttpTarget.idempotency_header` | 这是下游 HTTP 服务的能力，只对 HTTP 工具有意义 |
| 3 | §6.6 每次尝试一个子 Span `attempt N` | 在 `execute_tool` Span 上记录 `retry` 事件，外加 `app.tool.retries` 指标和 `tool.call.retry` 日志 | 子 Span 会让 Trace 层级变深而信息量相同；事件足以定位每次重试 |
| 4 | §6.4 精确清除只作用于日志、Span、审计 | **也作用于返回给模型的结果** | 下游回显请求头时，凭证会被直接交给模型；替换成本低、收益明确 |
| 5 | §5.1 第 4 步“参数级鉴权”在第 5 步，由 `Authorizer.check_args` 提供 | 由执行器参数 `authorize_args` 回调提供（沿用已有实现），回调抛出异常视为拒绝 | 保持已有接口；失败即拒绝是更安全的默认值 |
| 6 | 未说明 | HTTP 工具注册时要求输入 Schema 顶层 `additionalProperties: false`，映射到路径的参数必须必填 | 与 Python 工具的 `extra="forbid"` 一致，防止模型夹带未声明参数 |
| 7 | 未说明 | 本地限流错误（`rate_limited`）不触发重试 | 限流器已经按 `max_wait_s` 等待过，重试只会重复等待 |
| 8 | §3.4 未列出 | 新增错误：`UpstreamError`（`upstream`）、`RateLimitedError`（`throttled`）、`OutputValidationError`（`output`）；`ToolError` 新增可选的 `retry_after` | 区分下游失败、本地限流与输出不合规 |
| 9 | 未说明 | 执行租约改为等待**所有**登记的孤儿线程结束后才归还额度 | 重试会让一次调用产生多个超时的同步线程；只等一个会提前归还额度 |
| 10 | 未说明 | 密钥值短于 6 个字符时不做精确替换 | 太短的值会误伤正常文本，本身也不应作为密钥 |
| 11 | 未说明 | `jsonschema` 没有类型信息，mypy 对其设置 `ignore_missing_imports`，没有新增 `types-jsonschema` 依赖 | 避免为类型检查增加依赖 |

### 11.4 兼容性

* `ToolBinding.target` 从 `PythonTarget` 扩展为 `PythonTarget | HttpTarget`：运行时不变，读取 Python 专有属性前需要 `isinstance` 收窄（只影响类型检查）；
* `ToolPolicy.retry` / `rate_limit`、`ToolSpec.output_schema`、`AuditEvent.error_type` / `error_code` / `attempts`、`AttemptScope.secrets` / `idempotency_key`、`ToolError.retry_after` 都是带默认值的新增字段；
* `LogMode` 新增 `redacted`、`full`，默认仍为 `metadata`。

### 11.5 已知限制与后续

* HTTP：不支持 HTTP/2、多部分表单、请求体以外的二进制上传；响应只接受 JSON、文本和 PNG / JPEG / GIF / WebP 图片；
* 限流与并发额度只在进程内有效；
* 规则脱敏只能识别常见格式，不保证清除所有敏感信息；
* P2：MCP 适配器；P3：熔断、审批、结果缓存、子进程沙箱、`on_oversize` 钩子。

---

## 12. P2 实现记录（2026-11-09）

P2（MCP Client）已实现，与 P0、P1 兼容（之前的全部测试保留并通过）。代码：
`agent_tools/adapters/mcp.py`（连接与调用）、`agent_tools/integrations/mcp.py`（发现与注册）。

### 12.1 依赖与已核实的 SDK 行为

`[mcp]` extra：`mcp>=2.3,<2.4`（锁定 **2.3.0**，同时带入 `mcp-types 2.3.0`、`pyjwt`、`cryptography`、
`sse-starlette`、`python-multipart` 等；已有包版本不变）。以下行为均在 2.3.0 上通过 spike 或测试实际验证：

| 行为 | 结论 |
|---|---|
| `mcp.Client` 生命周期 | 只能进入一次；退出时关闭内部 anyio task group，**必须在进入它的同一任务中退出** → 每条连接由专属后台任务持有，调用方从任意任务并发请求（已验证） |
| 传输方式 | `StdioServerParameters`（stdio 子进程）与 URL / `streamable_http_client(url, http_client=...)`（Streamable HTTP）；后者可以传入自建的 `httpx2.AsyncClient` |
| 连接断开 | 服务端进程被杀后，持有任务**不会**自行结束，后续请求立即抛出 `MCPError(-32000 CONNECTION_CLOSED)` |
| 取消 | 调用方取消时 SDK 发送 `notifications/cancelled`；stdio 下服务端确实收到并停止了请求，会话仍可继续使用（已用标记文件验证）。HTTP 路径在内存 ASGI 测试中服务端也停止了，但那可能是进程内直接取消，**没有证明** HTTP 上的取消通知被处理 |
| 结果 | `CallToolResult.content` / `structured_content` / `is_error`；内容类型：文本、图片、音频、资源链接、内嵌资源（文本 / 二进制） |
| 工具名 | SDK 规则 `^[A-Za-z0-9._-]{1,128}$` |
| 参数 Schema | SDK 在协议层要求为对象类型；任一工具不满足时整个列表校验失败 |
| 回调 | sampling、elicitation、roots、logging 默认 `None`，不提供即不启用 |
| stdio 环境变量 | 总会继承固定的安全变量（POSIX：HOME、LOGNAME、PATH、SHELL、TERM、USER），再合并显式 `env`；父进程其他变量不会传入（已验证） |
| 重定向 | Streamable HTTP 只跟随同源重定向（SDK 自行处理，不看 `follow_redirects`） |

### 12.2 设计要点

* **会话管理（`McpServerManager`）**：每个服务端一条共享会话；首次使用时建立，并发首次调用单飞（已用计数验证只连接一次）；连接断开时本次调用返回可重试的 `mcp_connection_lost` 并回收连接，下一次调用重连；连续建立连接失败 `max_consecutive_failures` 次后进入 `failure_cooldown_s` 冷却期，期间直接返回 `mcp_server_unavailable`（带 `retry_after`）；**没有后台重连循环**。`aclose()` 关闭全部会话并终止 stdio 子进程，可重复调用，关闭后的调用返回 `mcp_manager_closed`。
* **发现与注册（`McpToolSync`）**：白名单制；分页（页数、工具数上限、游标循环检测）；名称规范化与撞名拒绝；远端描述清理截断并可覆盖；Schema 强制顶层禁止额外属性后按 P1 规则编译；远端语义提示不采信；定义指纹决定版本号；通过新增的 `ToolRegistry.apply()` 一次性原子生效；只替换自己注册过的工具。
* **执行**：`McpTarget(server_id, remote_name)` + 已编译的 JSON Schema 校验器进入统一执行链，复用授权（含参数级）、审计、超时、限流、并发额度、重试策略、输出限制、密钥清除、`ResultFilter` 和可观测性。
* **错误映射**：`isError` → `ToolExecutionError(mcp_tool_error)`，默认不交出远端文本（`expose_error_text=True` 时截断并清除已知密钥后交出）；`MCPError` 只暴露错误码（`mcp_protocol_error`）；连接类错误 → `mcp_connection_lost`（可重试）；结果格式非法 → `mcp_invalid_response`；服务端要求额外输入 → `mcp_input_required`。
* **HTTP 安全**：URL 必须通过 `NetworkPolicy`；建立连接前做 DNS 预检；默认客户端复用 P1 的连接时检查传输层（测试验证 TCP 连接校验过的 IP、SNI 为原始主机名）。

### 12.3 与文档原写法不一致之处

| # | 原写法 | 实际实现 | 原因 |
|---|---|---|---|
| 1 | §5.3 MCP 适配器一个模块 | 拆成 `adapters/mcp.py`（连接与调用）和 `integrations/mcp.py`（发现与注册） | 依赖分层规定适配器不能依赖注册表 |
| 2 | §5.3 发现时“按白名单注册” | 增加 `ToolRegistry.apply()` 原子批量变更，并要求同步器只能替换自己注册过的工具 | 测试发现：没有所有权检查时，与本地工具同名的 MCP 工具会被悄悄挂成本地工具的新版本 |
| 3 | §5.3 “重连期间返回 UpstreamError” | 不在后台重连；断开时本次调用失败，下次调用按需重连，加连续失败冷却期 | 行为可预测、没有无限重试 |
| 4 | 未说明 | 远端 Schema 顶层强制 `additionalProperties: false` | 与 Python、HTTP 工具一致，防止模型夹带未声明参数 |
| 5 | 未说明 | 远端的 readOnly / idempotent / destructive 提示不采信，远端输出 Schema 不采信（宿主可在白名单中提供） | 都是不可信输入，治理策略只能由宿主决定 |
| 6 | 未说明 | Streamable HTTP 不支持代理；凭证是服务端级别，不支持按租户区分（**2026-11-10 起两者都有可选实现，见 §14**） | 会话被所有调用共享；代理模式下无法做连接时检查 |
| 7 | 未说明 | `McpServerManager` 提供仅测试使用的 `http_client_factory`（会绕过连接时检查，URL 与 DNS 预检仍执行） | 当前环境不允许本地 TCP，HTTP 功能测试改用内存 ASGI 应用 |

### 12.4 兼容性

* `ToolTarget` 扩展为 `PythonTarget | HttpTarget | McpTarget`（只影响类型检查）；
* `ToolRegistry.register()` 内部改为调用新的 `apply()`，行为不变；
* `agent_tools.adapters.http` 新增公开函数 `create_http_client()`，`HttpAdapter` 改为使用它（行为不变）。

### 12.5 已知限制

* SDK 把整个响应读入内存后才交给适配器，输出大小限制在映射之后执行；stdio 没有传输层的大小上限；
* HTTP 上的取消通知是否被服务端处理取决于服务端实现，未在真实 HTTP 服务端上验证；
* 进程内 ASGI 测试无法覆盖真实 TCP / TLS；真实 TLS 握手仍未端到端验证（同 §11.2）；
* SDK 2.x 变化较快，版本锁定到次版本，升级时需要重新验证 §12.1 中的行为。

### 12.6 后续（P3）

熔断、人工审批、结果缓存、子进程沙箱（包括 MCP stdio 服务端的隔离）、`on_oversize` 钩子。
（已在 P3 实现，见 §13；§12.5 的限制在 §13.5 中逐项更新。）

---

## 13. P3 实现记录（2026-11-09）

P3（扩展能力）已实现在 `packages/agent-tools/`。P0–P2 的全部测试保留并通过；所有新能力**默认关闭**，
未配置时执行链与 P2 完全相同（唯一放宽的规则见 §13.4）。

### 13.1 实施顺序

1. **契约**（错误类型、`ToolPolicy.approval/cache/dedupe`、`ToolExecutionResult.reused`、审计字段）：其余能力都挂在它们上面；
2. **熔断**：独立的治理组件，只改动尝试循环；
3. **人工审批**：改变了不可逆工具的注册规则，必须先于依赖它的组合（审批 × 去重、审批 × 缓存）确定；
4. **结果缓存与调用去重**：位置取决于鉴权与审批，最后接入；
5. **`on_oversize`**：只影响输出阶段；
6. **进程隔离**（Python 子进程、MCP stdio 启动器）：依赖适配器与运行时层，和执行链其余部分正交；
7. **P2 限制的验证**：传输层大小上限、私有 CA、真实网络测试。

### 13.2 交付内容

| 能力 | 位置 | 说明 |
|---|---|---|
| 熔断 | `governance/circuit.py` | `CircuitBreaker` 异步协议 + `InProcessCircuitBreaker`；关闭 / 开启 / 半开；连续失败阈值、开启时长、半开探测名额与成功阈值；“代”机制使状态切换后的旧许可失效；可注入时钟；隔离键 `tool` / `target`；`default_failure_classifier`；状态切换回调 |
| 人工审批 | `governance/approval.py`、`executor.py` `_approve` | `ApprovalHandler` 协议、`ApprovalRequest`（参数视图 `none`/`redacted`/`full` + 摘要）、`ApprovalDecision`；超时 = `min(timeout_s, 剩余截止时间)`；缺少处理器安全失败；`approval` 审计事件 |
| 结果缓存 / 调用去重 | `governance/reuse.py`、`executor.py` | `ResultStore` 字节存储协议、`InMemoryResultStore`（TTL + LRU + 条目数 / 字节数上限）、作用域键、`SingleFlight`；缓存与去重使用不同存储实例 |
| 超大输出钩子 | `security/oversize.py`、`executor.py` `_process_output` | `OversizeHandler`；限时；失败回退原错误；改写结果重新校验、限制、清除密钥、过滤 |
| Python 进程隔离 | `runtime/isolation.py`、`runtime/process.py`、`runtime/subprocess_child.py` | `ProcessIsolation`、`ResourceLimits`；每次调用一个 `python -I` 子进程、独立进程组；结果大小上限；所有退出路径终止整个进程组；外部沙箱命令前缀 |
| MCP stdio 隔离 | `runtime/stdio_launcher.py`、`adapters/mcp.py` | `StdioIsolation`；启动器设置资源限制并按行转发 stdout，超长响应换成同 ID 的 JSON-RPC 错误（`-32090` → `mcp_response_too_large`） |
| MCP HTTP 响应上限 | `adapters/mcp.py` `_CappedTransport` | 非 SSE 响应流式计数，超限抛出 `httpx2.StreamError` 子类（SDK 转为该请求的 `PARSE_ERROR`）；SSE 使用 SDK 的 `max_sse_event_size`；两者映射为 `mcp_response_too_large`，连接不丢弃；请求 `Accept-Encoding: identity` |
| 私有 CA | `adapters/http.py` `_verify` | `HttpAdapterConfig.ca_bundle` / `HttpTransportConfig.ca_bundle` → `ssl.create_default_context(cafile=...)`（只信任该 CA，仍校验主机名） |

执行链（`executor.py` 模块文档为准）：大小检查 → 解析 → 工具级鉴权 → 参数校验 → 参数级鉴权 →
**调用去重** → **审批** → **结果缓存** → 并发额度 → 重试循环 { **熔断准入** → 限流 → 超时内调用适配器 →
**记入熔断** } → 输出 Schema 校验 → 大小限制（**on_oversize**）→ 密钥清除 →（写缓存）→ ResultFilter →
大小限制 → 审计（复用结果标明 `reused`）。

### 13.3 与文档原写法不一致之处

| # | 原写法 | 实际实现 | 原因 |
|---|---|---|---|
| 1 | §5.1 第 6 步“审批”紧跟参数级鉴权 | 去重在审批之前：重复提交直接重放，不再审批 | 重放不执行工具，再次打扰审批人没有意义；执行者路径仍然先审批再执行 |
| 2 | §3.4 `ApprovalRequiredError` 表示“需要审批” | 同一类型用错误码区分 `approval_unavailable` / `approval_timeout` / `approval_failed`；拒绝用 `ApprovalRejectedError` | 调用方需要区分“没配置”“没等到”“出错了”，但都属于 `permission` 类型、都不可重试 |
| 3 | §8 “按 call_id 缓存结果” | 拆成两个机制：按参数与身份的**结果缓存**（只读工具）和按 request_id + call_id 的**调用去重**（任意工具） | “按 call_id 缓存”实际是去重语义；把两者混在一起会把缓存误当成幂等保证 |
| 4 | §6.2 熔断“按工具或 endpoint 统计” | 隔离粒度 `tool`（工具名，跨版本）或 `target`（HTTP `scheme://host:port` / MCP 服务端 / Python 工具名）；不按租户 | 熔断描述下游健康，下游对所有租户相同 |
| 5 | §3.4 `CircuitOpenError` 可重试 | `retryable=True`（给调用方的提示），但执行器**不会**因它自动重试 | 熔断开启就是为了停止发送请求 |
| 6 | §8 “子进程沙箱” | 进程隔离 + POSIX 资源限制 + 外部沙箱命令前缀；**不是**完整沙箱 | 文件系统、网络、系统调用隔离依赖操作系统特性或容器运行时，无法在本包内可靠提供，只提供扩展接口 |
| 7 | §8 “分布式限流的参考实现” | 未实现 | 需要具体的共享存储（Redis 等），会引入强制基础设施依赖；`RateLimiter` 协议已足够宿主实现 |
| 8 | 未说明 | `ToolExecutionResult.reused`、`AuditEvent.approver/reused`、审计 `kind="approval"` | 复用的结果和审批决定需要可追溯 |
| 9 | 未说明 | 缓存的是 `ResultFilter` **之前**的输出，命中时重新过滤 | 过滤可能依赖调用上下文（例如会话、用户），缓存过滤后的结果会跨上下文泄露 |
| 10 | 未说明 | 去重只记录“工具已经开始执行”（以适配器的 `mark_started` 为准）的结果 | 没开始执行的失败（审批拒绝、熔断、等不到执行资源）可以安全地重新执行；记录它们会把暂时性失败永久化 |
| 11 | 未说明 | 声明了缓存的工具在没有配置缓存存储时照常执行；声明了去重的工具在没有去重存储时安全失败 | 缓存缺失只影响性能；去重缺失可能导致重复的副作用 |

### 13.4 兼容性

* 全部新增字段带默认值；未配置任何 P3 能力时行为与 P2 相同。P0–P2 已有测试的断言没有修改且全部通过；对已有测试文件
  的改动都是增量的：`test_mcp.py`、`test_packaging.py` 追加了 P3 用例，`mcp_stdio_server.py` 追加了 `big`/`spin`/`pids`
  工具，依赖方向测试允许 `adapters/mcp.py` 导入 `httpx2`（该模块原本就通过 `adapters.http` 依赖它，`[mcp]` extra 也包含它）；
* **规则放宽**：声明了 `policy.approval` 的不可逆工具在默认注册表中可以注册（P0–P2 一律拒绝）。未声明审批的仍被拒绝；
  `allow_destructive_without_approval=True` 含义不变；
* 审计接收端若对 `kind` 做穷举处理，需要接受 `approval`；
* `PythonAdapter` 新增可选参数 `processes`；`create_http_client` 新增可选参数 `wrap_transport`；均向后兼容。

### 13.5 P2 已知限制的处理结果

| 限制（§12.5） | 结果 | 证据 |
|---|---|---|
| HTTP 上的取消通知未在真实服务端验证 | **未验证**（测试已编写） | `tests/test_network_e2e.py::test_streamable_http_over_real_tcp_and_cancellation` 用真实 uvicorn + 生产传输层验证服务端收到取消；本次开发环境禁止本地 TCP 连接，且解除沙箱的请求被拒绝，测试被跳过 |
| 真实 TCP / TLS 握手未端到端验证 | **部分验证** | TLS 配置（私有 CA 信任、主机名校验、默认信任库拒绝私有 CA）用生产代码构造的 SSL 上下文做内存握手验证（`test_tls_memory.py`）；SNI 使用原始主机名已在 P1 验证。真实套接字上的握手测试已编写（`test_network_e2e.py`），未能运行 |
| SDK 先完整读入响应，输出限制在其后 | **已解决（可选，默认关闭）** | HTTP JSON 响应、SSE 事件（内存 ASGI 测试）、stdio 单条消息（真实子进程测试）在传输层被限制，超限得到 `mcp_response_too_large` 且连接继续可用 |
| stdio 没有传输层大小上限 | **已解决（可选，默认关闭）** | 同上，`StdioIsolation.max_line_bytes`；启动器内存占用不超过上限 |
| 凭证不能按租户区分 | **未解决**（2026-11-10 已提供可选实现，见 §14.2） | 会话按服务端共享；按租户区分需要每个（服务端，租户）一条会话及其生命周期管理，发现阶段也需要租户上下文，本阶段未做 |
| HTTP 不支持代理 | **未解决**（2026-11-10 已提供可选实现，见 §14.3；真实代理未验证） | 代理模式下连接时 IP 检查看到的是代理地址；HTTP 适配器的“预检 + 代理 ACL”退回方案尚未移植到 MCP |

### 13.6 已知限制

（本节是 2026-11-09 的记录，保留原文；逐项的当前状态见 §14.6。）

* 熔断、缓存、去重、单飞协调都只在**当前进程**有效；多实例需要宿主实现共享状态的协议；
* 去重不是事务：执行完成到写入记录之间崩溃、记录过期或被淘汰后会再次执行；
* 审批是同步等待，不支持跨进程或长时间暂停后恢复；
* 进程隔离不是完整沙箱：无文件系统 / 网络 / 系统调用隔离；`memory_bytes` 只在 Linux 可用（本次在 macOS 开发，Linux 内存限制测试被跳过，未实测）；`setsid` 脱离进程组的后代无法回收；不支持 Windows；
* 隔离 Python 工具的参数与返回值必须可 JSON 序列化，函数必须能按 `模块:限定名` 重新导入；注入的 `ToolContext` 不含截止时间；
* stdio 启动器只能从行首确认超长消息是否为响应；无法确认时（例如 `result` 键出现在上限之后）只能丢弃，对应请求会等到超时；
* `on_oversize` 不能减少传输阶段的内存占用；
* 真实网络端到端测试需要允许本地 TCP 的环境：`uv run pytest packages/agent-tools/tests -m network`。

---

## 14. 2026-11-10 补充实现记录（租户会话、代理、分布式限流参考实现）

本节记录 P3 之后对 §13.5、§13.6 遗留事项的处理。所有新能力**默认关闭**，未配置时行为与 P3 相同
（唯一的收紧见 §14.5）。P0–P3 的测试断言没有修改且全部通过。详细过程与逐项证据见
`docs/progress/2026-11-10_11-27-19_agent-tools-progress.md`。

### 14.1 开始前的核对（以代码和实际运行结果为准）

| 遗留事项 | 开始时的实际状态 |
|---|---|
| MCP 会话按服务端共享，不能按租户区分凭证 | 属实：`McpServerManager` 每个服务端一个 `_ServerState`，凭证用空 `Principal` 获取 |
| MCP Streamable HTTP 不支持代理 | 属实：`HttpTransportConfig` 没有代理字段；`create_http_client` 的代理分支只被 HTTP 适配器使用 |
| 真实网络端到端未验证 | 属实：`-m network` 仍在模块级跳过（回环 TCP 不被允许）。本次再次尝试，包括解除命令沙箱后，`connect()` 仍返回 `EPERM` |
| Linux 内存限制未实测 | 属实：开发机 macOS；Docker（OrbStack）套接字拒绝访问，无法启动 Linux 容器 |
| 分布式限流参考实现未做 | 属实：只有 `RateLimiter` 协议与进程内令牌桶 |
| 文档与代码不一致 | 未发现与 §10–§13 记录矛盾的实现；270 个测试通过、2 个跳过，与记录一致 |

### 14.2 MCP 租户级凭证与会话隔离

`McpServerConfig.session_scope`：`"server"`（默认，原行为）或 `"tenant"`。管理器的状态从“每个服务端一份”
改为“每个（服务端，租户）一份”（`_Session`），服务端范围时租户键恒为 `None`。

| 环节 | 行为 |
|---|---|
| 租户来源 | `ToolContext.principal.tenant_id`，由宿主认证后设置；适配器把 `ctx` 传给管理器，只读取租户，不发给服务端 |
| 缺少租户 | 租户范围的服务端直接拒绝（`PermissionDeniedError(mcp_tenant_required)`），不建立会话、不退回共享会话 |
| 会话创建与复用 | `_checkout` 是同步的（查找、回收、创建之间不让出事件循环），同一键不会并发创建两条会话；连接建立由每条会话自己的锁单飞 |
| 凭证 | 用合成上下文 `Principal(tenant_id=会话租户)` 调用 `SecretProvider`：不带用户、权限和请求 ID，凭证不取决于“谁先调用”。服务端范围仍用不带租户的上下文（与 P2 相同）。每次建立连接都重新获取 |
| stdio | 每个租户一个子进程；`StdioTransportConfig.secret_env` 把密钥作为环境变量传入（新增） |
| 密钥清除 | 管理器在请求结束时通过 `on_secrets` 把**本次使用的会话**的密钥交给适配器登记；其他租户的密钥不会进入这次调用 |
| 重连与冷却 | 按会话：断开后同一租户的下一次调用重连；连续失败计数与冷却期互不影响 |
| 凭证变更 | `McpServerManager.invalidate(server_id, tenant_id=None, force=False)`：会话从管理器移除，新调用建立新会话；默认等在途请求结束后关闭旧连接，`force=True` 立即关闭。管理器不会自动感知轮换 |
| 数量上限与空闲回收 | `max_tenant_sessions`（默认 64）达到时回收最久未使用的空闲会话，全部忙碌时 `ConcurrencyLimitedError(mcp_session_limit)`；`tenant_idle_ttl_s`（默认 900 秒）在下一次取会话时回收，或由宿主调用 `close_idle_sessions()`；没有后台任务 |
| 关闭 | `aclose()` 关闭所有租户的会话，包括已失效但仍有在途请求的会话，并等待后台关闭任务结束 |
| 发现 | `McpToolSync.sync(..., ctx=...)`：租户范围的服务端必须指定发现所用的租户，否则失败且注册表不变 |

**设计取舍**：

* **注册表不按租户区分**（同日第二次工作已提供可选的租户层，见 §15.2）。工具目录是服务端级别的，所有租户看到同一份定义；服务端按租户返回不同工具集的场景
  不支持（需要按租户的注册表视图，涉及 `ToolRegistry`、列表与别名规则，超出本次范围）。某个租户的服务端
  实际没有该工具时，调用得到 `mcp_protocol_error`；
* **凭证是租户级而不是用户级**：会话被同一租户的所有用户共享。需要用户级凭证时不应使用共享会话，
  本包目前不提供按用户的会话；
* **不自动重试、不自动感知凭证轮换**：鉴权失败只表现为连接失败并计入冷却；由宿主在轮换时调用 `invalidate`；
* 每个租户一条会话意味着多实例部署时每个实例各有一条，stdio 服务端的进程数 = 实例数 × 活跃租户数（受上限约束）。

### 14.3 MCP Streamable HTTP 代理

`HttpTransportConfig.proxy: McpProxyConfig | None`（默认 `None`，不读取环境变量中的代理）。

| 方面 | 行为 |
|---|---|
| 配置 | `McpProxyConfig(url, auth_secret)`：只接受 `http` / `https`，地址不能带用户信息、路径、查询、片段 |
| 代理凭证 | 密钥值格式 `用户名:密码`，通过 `SecretProvider` 用会话的租户上下文获取；只进入 `Proxy-Authorization`（`https` 目标时只在 `CONNECT` 中）。原值、密码、Base64 形式都登记为已知密钥。静态请求头不允许设置 `Proxy-Authorization` |
| 只连接代理 | `ProxyOnlyNetworkBackend`（`adapters/http.py`）：连接池发起的每条 TCP 连接都必须是配置的代理主机与端口，否则拒绝。HTTP 适配器的代理模式共用这一实现 |
| 预检 | 建立会话前检查 URL 并解析、检查全部 DNS 结果；`_PreflightTransport` 在**每个请求发送前**重复这一检查（覆盖 SDK 跟随的同源重定向）。被拒绝的请求不会发给代理，SDK 视为传输失败，调用返回 `mcp_connection_lost`，重连时得到 `destination_not_allowed` |
| 重定向 | 仍由 SDK 只跟随同源重定向；跟随的请求同样经过预检 |
| 目标 TLS | `https` 目标经 `CONNECT` 隧道，客户端端到端校验证书与主机名（含 `ca_bundle`）；代理看不到内容 |
| 明文目标 | 只有显式 `allow_http` 时才允许，以绝对地址形式转发，全部内容（含凭证）对代理可见 |

**剩余风险与使用前提**：代理模式下客户端无法验证目标的实际连接地址。预检与代理各解析一次 DNS，两次结果
可能不同，**不能防止 DNS 重绑定**；预检只能在 DNS 已经指向内网时尽早拒绝。使用前提是代理自身实施访问控制
（目标白名单、拒绝内网与元数据地址、限制 `CONNECT` 端口），并且代理是可信的。代理地址是部署配置，
不经过 `NetworkPolicy` 的内网检查。不支持 SOCKS。默认（无代理）模式的连接时检查没有任何变化。

### 14.4 分布式限流参考实现

`agent_tools.governance.redis_ratelimit.RedisTokenBucketRateLimiter`，满足 `RateLimiter` 协议。

* **不引入依赖**：`pyproject.toml` 的依赖与 extras 没有变化；客户端按 `RedisScriptClient` 协议（一个
  `eval` 方法）鸭子类型传入，`redis.asyncio.Redis` 满足；
* 两个 Lua 脚本（取令牌、退还），每个只访问一个键；时间取自 Redis `TIME`；键为 `prefix + sha256(key)`，
  带过期时间（不短于回满所需时间，过期不会放宽限制）；
* 语义与进程内实现一致：预留后等待、取消时后台尽力退还、等待受 `min(max_wait_s, 剩余截止时间)` 约束；
* Redis 不可用时默认 `fail_closed`（`rate_limiter_unavailable`），`fail_open` 需要显式选择并计数；
* **不是事务，不是严格计数**：回复丢失、退还失败会多扣；主从切换丢写入、键被淘汰会少扣；
* **验证状态**：Python 一侧用内存假客户端测试（假客户端按相同算法实现两个脚本）；**Lua 脚本没有在真实
  Redis 上执行过**。`tests/test_redis_ratelimit.py::test_lua_scripts_on_real_redis`（`-m redis`）用于真实验证，
  本次未运行。

### 14.5 兼容性与多实例作用范围

兼容性：

* 新增字段与参数全部带默认值：`McpServerConfig.session_scope / max_tenant_sessions / tenant_idle_ttl_s`、
  `HttpTransportConfig.proxy`、`StdioTransportConfig.secret_env`、`McpServerManager.list_tools / call_tool`
  与 `McpToolSync.sync` 的 `ctx`、`create_http_client(proxy_auth=...)`；
* `McpServerManager.known_secrets()` / `connected()` 新增仅关键字参数 `tenant_id`（默认服务端级会话）；
  适配器不再使用 `known_secrets()`，改为 `call_tool(on_secrets=...)`；
* 管理器内部的 `_ServerState` 改名为 `_Session`，`_open` 仍接受一个状态参数（私有接口）；
* **收紧 1**：`http` / `https` 代理模式下（HTTP 适配器与 MCP），连接池只能连接代理地址。配置方式不变；
  SOCKS 代理（需要宿主自行安装 `socksio`）仍走 `httpx2` 原有的代理传输层，没有这一限制；
* **收紧 2**：`HttpTransportConfig.headers` 不再接受 `Proxy-Authorization`（以前会作为普通请求头发给目标）。

多实例部署下的作用范围：

| 能力 | 内置实现 | 多实例时 | 不提供的保证 |
|---|---|---|---|
| 限流 | 进程内令牌桶 | 各实例各自计数；可换成 §14.4 的参考实现或宿主实现 | 参考实现不是严格计数 |
| 并发额度 | 进程内 | 各实例独立 | 无跨实例并发上限 |
| 熔断 | 进程内 | 各实例各自统计与开合；可实现 `CircuitBreaker` 协议共享 | 无全局一致状态 |
| 结果缓存 | `ResultStore` 协议，内置进程内实现 | 共享存储可跨实例命中 | 普通缓存，不保证一致性 |
| 调用去重 | `ResultStore` 协议 + 进程内单飞 | 共享存储可重放已完成的调用；并发到达不同实例的相同调用都会执行 | 不是跨实例事务、严格一次执行或下游幂等保证 |
| 单飞协调 | 进程内 | 只合并同一实例内的并发调用 | 无跨实例单飞 |
| MCP 会话与冷却 | 进程内 | 每个实例各有会话与冷却状态 | 无跨实例会话共享 |
| 审批 | 进程内同步等待 | 由发起调用的实例等待 | 不支持跨进程或暂停后恢复 |

### 14.6 已知限制的当前状态（取代 §13.5、§13.6 的状态列）

（本节是 2026-11-10 第一次记录的状态，保留原文；最新状态见 §15.6。）

| 事项 | 状态 | 说明与复现步骤 |
|---|---|---|
| MCP 凭证按租户区分 | 已解决（可选） | §14.2；`tests/test_mcp_tenancy.py`（内存 ASGI + 真实 stdio 子进程） |
| MCP 注册表按租户区分 | 未解决（设计边界） | 工具目录是服务端级别 |
| MCP HTTP 代理 | 已实现，**真实代理未验证** | §14.3；客户端一侧用内存网络验证（`tests/test_mcp_proxy.py`）；`test_network_e2e.py` 中的两个代理用例未运行 |
| 代理模式的 DNS 重绑定 | 未解决（无法在客户端解决） | 依赖代理的访问控制 |
| 真实网络上的 Streamable HTTP 取消通知、TCP 连接、TLS 握手 | **未验证** | 环境禁止回环 TCP（`connect()` 返回 `EPERM`，解除命令沙箱后仍然如此）。复现：在允许本地 TCP 的环境运行 `uv run pytest packages/agent-tools/tests -m network` |
| Linux 进程内存限制与资源清理 | **未验证** | macOS 开发机；Docker 套接字拒绝访问。复现：Linux 上运行 `uv run pytest packages/agent-tools/tests/test_isolation.py packages/agent-tools/tests/test_mcp.py`，确认 `test_memory_limit_enforced_on_linux` 不被跳过且通过 |
| 分布式限流 | 参考实现已提供，**Lua 未在真实 Redis 上验证** | §14.4；复现：`AGENT_TOOLS_REDIS_URL=redis://127.0.0.1:6379/15 uv run --with redis pytest packages/agent-tools/tests -m redis` |
| 熔断、缓存、去重、单飞、并发额度只在进程内 | 设计边界 | §14.5 的作用范围表；协议已允许宿主提供共享实现 |
| 去重不是事务 | 设计边界 | 同 §13.6 |
| 审批同步等待，不支持跨进程恢复 | 设计边界 | 需要持久化的调用状态机，属于 Agent Runtime 的职责 |
| 进程隔离不是完整沙箱；`setsid` 后代无法回收；不支持 Windows | 设计边界 | 需要强隔离时使用外部沙箱命令前缀 |
| 隔离工具的 `ToolContext` 不含截止时间 | 设计边界 | 单调时钟的绝对时刻跨进程无意义；超时由父进程强制执行 |
| stdio 启动器无法确认超长消息是否为响应时只能丢弃 | 设计边界 | 启动器不缓存超过上限的内容；对应请求等到超时 |
| `on_oversize` 不能减少传输阶段的内存占用 | 设计边界 | 使用传输层上限 |

---

## 15. 2026-11-10 第二次补充实现记录（租户注册表视图、共享状态参考实现、验证尝试）

本节记录同日第二次工作。所有新能力**默认不启用**：不使用租户层、不传入 Redis 参考实现时，行为与 §14
相同。P0–P3 与 §14 的测试断言没有修改且全部通过。过程与逐项证据见
`docs/progress/2026-11-10_15-20-52_agent-tools-progress.md`。

### 15.1 开始前的核对与高优先级验证的结果

| 事项 | 开始时的状态 | 本次结果 |
|---|---|---|
| 基线 | 319 通过、2 跳过、6 未选中，与上次记录一致；工作区未提交改动与上次相同 | — |
| 真实网络与代理（`-m network`） | 已实现、未验证 | **仍未验证**。回环 `connect()`、出站 TCP、Unix 域套接字都返回 `EPERM`；解除命令沙箱的请求被权限策略拒绝，没有再尝试绕过。对从未运行过的代理夹具做了静态检查并修复两处问题（§15.5），修复本身同样未运行 |
| 真实 Redis 上的限流 Lua | 已实现、未验证 | **仍未验证**。本机没有 `redis-server`；Docker（OrbStack，处于停止状态）套接字拒绝访问；离线环境无法安装 `redis` / `fakeredis` / `lupa`。只做了脚本的静态复查（§15.5） |
| Linux 进程隔离 | 已实现、未验证 | **仍未验证**。开发机是 macOS，没有可用的 Linux 主机或容器；`test_memory_limit_enforced_on_linux` 仍被跳过 |

没有用模拟结果替代上述任何一项。

### 15.2 按租户区分的注册表视图

**需求**：`session_scope="tenant"` 之后，凭证与会话已经按租户隔离，但目录仍是服务端级别的——服务端按
租户返回不同工具集（或同名但 Schema 不同的工具）时，其他租户会看到并调用到自己没有的工具，或者按别的
租户的 Schema 校验参数。

**方案**（已实现，`registry.py`、`executor.py`、`integrations/mcp.py`）：

| 方面 | 设计 |
|---|---|
| 结构 | 注册表 = 一个共享层 + 若干租户层。`RegistrySnapshot`（基础快照）只解析共享层，附带全部租户层；`for_tenant(tenant_id)` 返回“共享层 + 该租户层”的视图（两层只读叠加，O(1)，不复制） |
| 租户隔离 | 执行器的 `list_tools` / `execute` / `execute_many` 一律按 `principal.tenant_id` 取视图。其他租户的工具解析为 `ToolNotFoundError`，与不存在无法区分。安全默认值：任何忘记取视图的代码路径只能看到共享层，即更少的工具；租户视图不能再换成另一个租户（`ValueError`） |
| 权限检查 | 不变。租户层只决定目录，解析之后照常做工具级、参数级鉴权与审批。`ToolInvocation.version`（宿主指定版本）同样在视图内解析 |
| 名称冲突 | 一个视图内不允许歧义：租户级工具与共享工具之间的同名、别名与内部名交叉，在注册阶段拒绝，**双向**检查（注册共享工具时与每个租户层比较）。租户层不能遮盖共享工具。不同租户层之间不比较，可以同名同别名。别名规则（只由名字决定、与版本无关）不变 |
| 快照 | 所有层共用一个单调序号；任何一层的变更生成一个新快照。未变化的层对象在相邻快照之间共享。`snapshot_id` 只确定时刻，视图由 `ctx` 的租户决定 |
| 刷新 | `McpToolSync.sync(..., scope="tenant", ctx=...)`：用该租户的会话发现，只写入该租户层；记录与锁按（服务端，租户层）区分。只替换自己在这一层注册过的工具；与共享层或本层中非同步器管理的同名工具冲突时整体失败、注册表不变。`ToolRegistry.drop_tenant()` 删除整层 |
| 并发 | 注册表：所有修改（含跨层冲突检查）在同一把线程锁内，一次 `apply` 只修改一个作用域并原子发布。同步器：同一（服务端，租户层）串行，不同租户并发。读取方使用不可变快照 |
| 三类工具 | Python、HTTP、MCP 使用同一套作用域规则（`register_function` / `register_http` / `apply` 的 `tenant_id`）；执行链没有任何按工具类型的分支 |

**取舍与边界**：

* 冲突时拒绝而不是遮盖：遮盖会让同一个名字对不同租户解析到权限与策略完全不同的定义，而且宿主的本地
  工具可能被远端 MCP 工具替换；
* 目录按租户而不是按用户区分（与会话粒度一致）；
* 所有层共用快照历史（默认 8 个）：其他租户的变更也会让 `snapshot_id` 更快过期。没有改成按租户的序号，
  因为那会改变 `snapshot_id` 的含义（公共契约）；需要严格一致的宿主调大 `history`（层对象共享，成本低）；
* 租户层与 MCP 会话各自管理：`drop_tenant` 不关闭会话，`invalidate` 不修改目录；
* 结果缓存与去重的键本来就包含租户与版本（MCP 工具的版本是定义指纹），同名的租户级工具不会互相命中
  （已测试）；限流按（工具名，租户）计数，不受影响；
* **熔断的隔离键不含租户**（§13.3 第 4 条）：同名的租户级工具、同一服务端的不同租户共用一份熔断状态。
  一个租户的凭证或连接问题产生的可重试失败会影响其他租户。按租户扩展隔离键需要改动 `CircuitScope` 与
  执行器计算隔离键的方式，属于公共契约变更，列为待决事项（§15.7）。

### 15.3 共享状态的结果存储与熔断（参考实现）

先区分四个容易混淆的机制（实现与文档都按此划分）：

| 机制 | 回答的问题 | 共享存储能做到的 | 做不到的 |
|---|---|---|---|
| 结果缓存 | 只读调用能否不访问下游 | 跨实例命中 TTL 内的成功结果 | 一致性；不防重复执行 |
| 调用去重 | 同一逻辑调用被重复提交时是否再执行 | 重放任一实例**已完成并写入**的记录 | 并发到达不同实例时都会执行；写入前崩溃、记录丢失后会再次执行 |
| 并发协调（单飞） | 同一个键的并发调用是否只执行一次 | **不提供**：仍只在进程内 | 跨实例单飞需要“先占位再执行”的原子语义，现有 `ResultStore` 协议表达不了 |
| 下游幂等 | 副作用是否只发生一次 | **不提供**：只能由下游按幂等键实现 | — |

| | `governance/redis_store.py` `RedisResultStore` | `governance/redis_circuit.py` `RedisCircuitBreaker` |
|---|---|---|
| 协议 | `ResultStore`（未修改） | `CircuitBreaker`（未修改） |
| 依赖 | 无新增依赖；客户端鸭子类型（`get` / `set(px=)`） | 无新增依赖；客户端鸭子类型（`eval`） |
| 实现 | `GET` / `SET PX`，没有 Lua | 两个 Lua 脚本，每个一个键；时间取自 Redis `TIME`；许可带“代” |
| 多租户 | 键是执行器给出的摘要（含租户、用户等），存储不解析；值是明文结果，访问控制与加密由宿主负责；前缀必填 | 不按租户（同 §15.2 的边界） |
| 过期 | 缓存 `ttl_s` / 去重 `window_s`，`PX` 向上取整 | 状态哈希 `max(2 × open_s, idle_ttl_s)` 无写入即过期 = 恢复关闭；探测名额是 `probe_ttl_s` 的租约 |
| 故障模式 | 执行器处理：缓存按未命中；去重读失败不执行；超过 `max_value_bytes` 抛出异常而不是静默丢弃 | `on_error="local"`（默认，退回本实例的进程内熔断器）或 `"fail_closed"`（`circuit_unavailable`） |
| 竞争条件 | 两个实例同时未命中都会执行并各自写入（后写覆盖） | “连续失败”近似；记录在后台写入、可能丢失；旧代许可被忽略；租约过期后迟到的归还可能多放一个探测 |
| 不承诺 | 事务、严格一次执行、跨实例单飞、下游幂等 | 全局严格一致的状态 |

**验证状态：部分完成。** Python 一侧用内存假客户端验证（`tests/test_redis_state.py`，35 个用例）；熔断脚本
在假客户端中用 Python 按相同算法实现，所以这些测试**不能证明 Lua 脚本正确**。两个带 `redis` 标记的
真实用例已编写、未运行。因此本项不标记为完成。

### 15.4 兼容性

* 新增参数与字段全部带默认值：`ToolRegistry` 各修改方法的 `tenant_id`、`tenants()`、`drop_tenant()`；
  `RegistrySnapshot.tenants` / `tenant_id` / `for_tenant()`；`McpToolSync.sync(scope=)`、
  `McpToolSync.registered(tenant_id=)`、`McpSyncReport.tenant_id`；
* 不使用租户层时：注册、列表、解析、执行、快照编号与以前逐一相同（P0–P3 测试断言未修改）；
* 需要注意的一点：`registry.snapshot()` 返回的基础快照只解析共享层。宿主自己调用 `snapshot.resolve()`
  并希望看到租户级工具时，需要先 `for_tenant()`；
* 私有接口变化：`ToolRegistry._require` / `_add_to` 的签名、`McpToolSync` 内部记录与锁的键；
* `provider_sdk`、`provider_gateway` 未修改；核心包与各 extra 的依赖未变化。

### 15.5 夹具修复与静态复查（均未在真实环境中运行）

**夹具修复**（`tests/test_network_e2e.py` 的最小本地代理，不涉及产品代码）：

1. 明文转发只改写连接上的第一个请求，之后是盲转发；客户端复用连接发出的后续请求不会被改写（上游收到
   绝对地址形式的请求行）、不会被记录、也不会检查代理凭证。改为转发时去掉逐跳头并强制
   `Connection: close`，每个请求使用一条代理连接；
2. 连接上游失败时处理函数带着未关闭的连接异常退出，可能使 `server.wait_closed()` 等待不结束。
   改为回复 502 并关闭连接。

**产品代码**：本次没有因为验证而修改传输、代理、限流或隔离的产品代码。

**静态复查的观察**（没有修改）：限流脚本用 `tostring()` 保存时间戳，Lua 5.1 的数字转字符串只保留 14 位
有效数字，秒级时间戳的小数部分因此只精确到约 0.1 毫秒。影响是每次调用至多 `0.0001 × 速率` 个令牌的
误差，没有改动未经验证的脚本；新写的熔断脚本用毫秒整数并以 `string.format('%.0f', …)` 保存，避开了这一点。

### 15.6 已知限制的当前状态（取代 §14.6）

| 事项 | 状态 | 说明与复现步骤 |
|---|---|---|
| MCP 凭证按租户区分 | 已解决（可选） | §14.2 |
| MCP 注册表按租户区分 | **已解决（可选）** | §15.2；`tests/test_tenant_registry.py`（34 个用例：注册表、执行器、内存 ASGI 上的 MCP 同步） |
| 熔断隔离键不含租户 | 未解决（待决，§15.7） | 一个租户的连接失败会计入共用的熔断状态 |
| MCP HTTP 代理 | 已实现，**真实代理未验证** | §14.3；`uv run pytest packages/agent-tools/tests -m network` |
| 代理模式的 DNS 重绑定 | 未解决（无法在客户端解决） | 依赖代理的访问控制 |
| 真实网络上的 Streamable HTTP 取消通知、TCP 连接、TLS 握手 | **未验证** | 环境禁止回环 TCP、Unix 域套接字与出站连接；解除沙箱被拒绝。复现同上 |
| Linux 进程内存限制与资源清理 | **未验证** | 没有 Linux 主机或容器。复现：Linux 上 `uv sync && uv run pytest packages/agent-tools/tests/test_isolation.py packages/agent-tools/tests/test_mcp.py -rs`，确认 `test_memory_limit_enforced_on_linux` 不在跳过列表中 |
| 分布式限流 | 参考实现已提供，**Lua 未在真实 Redis 上验证** | `AGENT_TOOLS_REDIS_URL=redis://127.0.0.1:6379/15 uv run --with redis pytest packages/agent-tools/tests -m redis` |
| 共享状态的结果存储 | 参考实现已提供，**未在真实 Redis 上验证**（无 Lua） | 同上 |
| 共享状态的熔断 | 参考实现已提供，**Lua 未在真实 Redis 上验证** | 同上 |
| 跨实例单飞 / 并发协调 | 不提供（待决，§15.7） | 需要扩展 `ResultStore` 协议 |
| 并发额度、MCP 会话与冷却、审批只在进程内 | 设计边界 | §14.5 |
| 去重不是事务 | 设计边界 | §13.6 |
| 审批同步等待，不支持跨进程恢复 | 设计边界 | 属于 Agent Runtime 的职责 |
| 进程隔离不是完整沙箱；`setsid` 后代无法回收；不支持 Windows | 设计边界 | 没有文件系统、网络、系统调用隔离；需要强隔离时使用外部沙箱命令前缀 |
| 隔离工具的 `ToolContext` 不含截止时间 | 设计边界 | 超时由父进程强制执行 |
| stdio 启动器无法确认超长消息是否为响应时只能丢弃 | 设计边界 | 对应请求等到超时 |
| `on_oversize` 不能减少传输阶段的内存占用 | 设计边界 | 使用传输层上限 |

### 15.7 待决事项（需要产品决策或公共契约变更，本次没有实施）

| # | 事项 | 具体设计 | 影响 |
|---|---|---|---|
| 1 | 熔断按租户隔离 | `CircuitScope` 增加 `tenant_target`（键 = 目标 + 租户摘要），执行器计算隔离键时传入 `ctx` | `circuit_key()` 签名与 `CircuitScope` 取值变化；键数量 × 租户数；“下游整体不健康”时每个租户各自累计到阈值才开启 |
| 2 | 跨实例单飞（去重的并发协调） | `ResultStore` 增加可选的 `claim(key, ttl_s) -> bool`（`SET NX PX`）与释放；执行器在成为执行者之前占位，占位失败者轮询记录 | 协议新增方法（可以做成可选能力探测）；需要决定占位者崩溃后的等待上限，以及等待者超时后是执行还是失败——后者是“可能重复执行”与“可能不执行”之间的产品取舍 |
| 3 | 租户层允许遮盖共享工具 | 视图查找顺序改为租户层优先，注册时不再拒绝同名 | 改变“名字在注册阶段无歧义”的现有保证；本次选择拒绝 |
| 4 | 按租户的快照序号 | `snapshot_id` 由（共享序号，租户序号）组成 | 改变 `snapshot_id` 的格式与过期语义 |
