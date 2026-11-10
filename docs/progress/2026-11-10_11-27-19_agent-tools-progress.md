# Agent Tools 研发进度（2026-11-10）

## 1. 记录时间与工作范围

* 记录时间：2026-11-10 11:27:19（Asia/Shanghai）。
* 范围：`packages/agent-tools/`（工具调用基础设施）及其文档。处理 `agent-tools-architecture.md`
  §13.5、§13.6 中遗留的未完成、未验证事项。
* 不在范围内：`provider_sdk/`、`provider_gateway/`（未修改）。
* 架构层面的记录在 `agent-tools-architecture.md` §14；本文件是当日的过程与证据。

## 2. 开始前核对结果

以当前代码和实际运行结果为准，没有只依据昨天的文档。

| 昨天遗留 | 开始时的实际状态 | 依据 |
|---|---|---|
| MCP 会话按服务端共享，不能按租户区分凭证 | 属实 | `McpServerManager` 每个服务端一个 `_ServerState`；凭证用空 `Principal` 获取 |
| MCP Streamable HTTP 不支持代理 | 属实 | `HttpTransportConfig` 没有代理字段 |
| 真实网络端到端（取消通知、TCP、TLS）未验证 | 属实，且今天仍无法验证 | `-m network` 在模块级跳过；回环 `connect()` 返回 `EPERM`，IPv4 / IPv6 都一样，解除命令沙箱后仍然如此 |
| Linux 进程内存限制未实测 | 属实，且今天仍无法验证 | 开发机 macOS（Darwin 27，arm64）；Docker（OrbStack）套接字 `permission denied`，无法启动 Linux 容器 |
| 分布式限流参考实现未做 | 属实 | 只有 `RateLimiter` 协议和进程内令牌桶 |
| 熔断、缓存、去重、单飞只在进程内 | 属实（设计边界） | 代码与 §13.6 一致 |
| 进程隔离、stdio 超长消息、同步审批、去重的边界 | 属实（设计边界） | 代码与 §13.6 一致 |
| 文档与代码是否不一致 | 未发现矛盾 | 基线：270 通过、2 跳过、5 未选中，与 §13 的记录一致 |

工作区开始时：`pyproject.toml`、`uv.lock` 已修改未提交，`agent-tools-architecture.md`、`packages/`
未跟踪。全部保留。

## 3. 本次需求完成情况

| # | 需求 | 结论 | 实际行为与证据 |
|---|---|---|---|
| 1 | MCP 租户凭证与会话隔离 | **已完成**（可选能力，默认关闭）；注册表按租户区分**未做** | `session_scope="tenant"`：每个（服务端，租户）独立会话与凭证。19 个测试（`test_mcp_tenancy.py`）：内存 ASGI 的 HTTP 路径 + 真实 stdio 子进程。见 §3.1 |
| 2 | MCP HTTP 代理 | **部分完成**：已实现并在客户端一侧验证；**真实代理未验证** | `HttpTransportConfig.proxy`。16 个测试（`test_mcp_proxy.py`）用内存网络验证；2 个真实代理用例已编写但未运行。见 §3.2 |
| 3 | 真实网络与平台验证 | **未完成**（环境限制） | 回环 TCP、出站网络、Docker 均不可用。测试与复现步骤已就绪。见 §3.3 |
| 4 | 分布式治理能力 | **部分完成**：限流参考实现已提供并测试 Python 一侧；**Lua 未在真实 Redis 上运行**。其余能力只给出作用范围说明 | `RedisTokenBucketRateLimiter`，14 个测试用例（`test_redis_ratelimit.py`）+ 1 个真实 Redis 用例（未运行）。见 §3.4 |
| 5 | 其他已知限制 | **已审查**；没有可在本包内安全修复的项，保留限制说明 | 见 §7 |

### 3.1 租户隔离的实际行为

* 租户取自 `ToolContext.principal.tenant_id`；没有租户的调用被拒绝（`mcp_tenant_required`），不建立
  会话，也不会退回共享会话。
* 凭证用只带租户的合成上下文获取（不带用户、权限、请求 ID）。服务端范围仍使用不带租户的上下文，
  与之前相同。
* 已验证的场景：3 个租户各 8 个并发调用，每个请求只带自己租户的凭证，每个租户只建立一条会话；
  结果只清除本租户的凭证；重连留在本租户内并重新取凭证；冷却期按租户统计；`invalidate` 的默认
  方式与强制方式；会话数量上限（LRU 回收、全部忙碌时限流）；空闲回收；调用方取消后在途计数归还；
  关闭后所有租户的子进程被终止；发现阶段缺少租户时失败且注册表不变。
* stdio 新增 `secret_env`，每个租户的子进程通过环境变量拿到各自的凭证。

### 3.2 代理的实际行为

* 只支持 `http` / `https` 代理，地址不能内嵌凭证；凭证经 `SecretProvider`，只进入
  `Proxy-Authorization`。
* 已验证（内存网络）：`https` 目标只与代理建立 TCP 连接并发 `CONNECT`，代理凭证只在 `CONNECT` 中，
  目标凭证只在隧道内，TLS 主机名是目标主机；明文目标以绝对地址转发；凭证缺失或格式错误时不建立
  连接；环境变量中的代理不生效；预检在连接代理前拒绝内网解析结果；会话建立后 DNS 改指内网时请求
  不会发出；固定后端拒绝任何非代理地址的连接。
* **没有验证**：经真实代理的完整 MCP 会话（握手、调用、取消通知、TLS 隧道内的真实握手）。

### 3.3 尝试过的验证方法

| 尝试 | 结果 |
|---|---|
| `uv run pytest -m network`（沙箱内） | 模块级跳过：回环 TCP 不被允许 |
| 同一命令，解除命令沙箱 | 仍然跳过 |
| 直接探测：监听 127.0.0.1 / ::1 后连接 | 监听成功，`connect()` 返回 `EPERM` |
| 出站连接 1.1.1.1:443 | `EPERM` |
| `docker info`（OrbStack） | `permission denied`（Docker 套接字） |
| 通过 SSH 进入 OrbStack 的 Linux 机器 | 连接超时 |

没有尝试绕过这些限制。按要求，相关事项保持“未验证”。

### 3.4 分布式限流参考实现的实际行为

* 位置：`agent_tools/governance/redis_ratelimit.py`。核心包与 extras 的依赖**没有变化**，客户端按
  鸭子类型传入。
* Redis 不可用时默认拒绝调用；`fail_open` 需要显式选择。
* 测试中的假 Redis 用 Python 按相同算法实现两个脚本，所以这些测试证明的是 Python 一侧（参数约定、
  等待、取消退还、故障模式、两个实例合计限额），**不能证明 Lua 脚本正确**。

## 4. 主要代码与文档变更

| 文件 | 变更 |
|---|---|
| `agent_tools/adapters/mcp.py` | 会话管理从“每服务端”改为“每（服务端，租户）”（`_Session`、`_checkout` / `_checkin`、`invalidate`、`close_idle_sessions`、数量上限与空闲回收）；`McpProxyConfig`、`_PreflightTransport`；`StdioTransportConfig.secret_env`；适配器把 `ctx` 传给管理器，并通过 `on_secrets` 登记本次会话的密钥 |
| `agent_tools/adapters/http.py` | `ProxyOnlyNetworkBackend`、`_ProxyTransport`；`create_http_client(proxy_auth=...)` |
| `agent_tools/integrations/mcp.py` | `McpToolSync.sync(..., ctx=...)` |
| `agent_tools/governance/redis_ratelimit.py` | 新增：分布式限流参考实现 |
| `tests/test_mcp_tenancy.py` | 新增：19 个租户隔离测试 |
| `tests/test_mcp_proxy.py` | 新增：16 个代理测试 |
| `tests/test_redis_ratelimit.py` | 新增：14 个默认运行的用例 + 1 个 `redis` 标记用例 |
| `tests/test_network_e2e.py` | 新增 2 个真实代理用例与一个最小本地代理（未运行） |
| `packages/agent-tools/pyproject.toml` | 新增 `redis` 测试标记并默认排除；依赖未变 |
| `packages/agent-tools/README.md` | 租户会话、代理、分布式限流、多实例作用范围表、已知限制现状、兼容性说明 |
| `agent-tools-architecture.md` | 新增 §14；状态行、§5.3、§8、§12.3、§13.5、§13.6 加注指向 §14（原文保留） |

本次新增或修改的函数、方法、测试函数都写了中文文档字符串；用 AST 扫描上述 8 个 Python 文件，
缺失数为 0。

## 5. 设计取舍与兼容性

* **默认行为不变**：`session_scope` 默认 `"server"`，`proxy` 默认 `None`，执行器默认限流器不变。
  P0–P3 的 270 个测试未改断言，全部通过。
* **注册表不按租户区分**：工具目录是服务端级别的。按租户的工具集需要改动注册表、列表与别名规则，
  超出本次范围。
* **凭证是租户级而不是用户级**：会话被同一租户的所有用户共享。
* **不自动感知凭证轮换**：由宿主调用 `invalidate`。鉴权失败只表现为连接失败并计入冷却。
* **代理模式不能防止 DNS 重绑定**：这是客户端无法解决的问题，文档中写明了前提（代理自身的访问控制）。
* **与原架构的差异**：§12.2 的“每个服务端一条共享会话”改为可配置；§12.3 第 6 条的两项限制有了
  可选实现。原文保留并加注。
* **对已有使用者的影响**：
  * `McpServerManager.known_secrets()` / `connected()` 新增仅关键字参数 `tenant_id`，原有调用方式不变；
  * 收紧：`http` / `https` 代理模式下连接池只能连接代理地址（HTTP 适配器与 MCP 共用）；
  * 收紧：`HttpTransportConfig.headers` 不再接受 `Proxy-Authorization`；
  * 私有接口 `_ServerState` 改名为 `_Session`。

## 6. 测试与检查结果

全部为本次实际运行的结果。

| 检查 | 结果 |
|---|---|
| `packages/agent-tools`：`uv run pytest` | **319 通过，2 跳过，6 未选中**（基线 270 通过；新增 49） |
| 其中跳过的 2 项 | `test_network_e2e.py`（整个模块，回环 TCP 不被允许）；`test_memory_limit_enforced_on_linux`（非 Linux） |
| `uv run pytest -m packaging` | 5 通过 |
| `uv run pytest -m "network or redis"` | **0 运行，2 跳过**：网络模块（8 个用例，含 2 个新代理用例）与真实 Redis 用例都没有执行 |
| `uv run ruff check .` / `ruff format --check .` | 通过 |
| `uv run mypy`（strict，50 个源文件） | 通过 |
| 仓库根目录 `make lint` / `make typecheck` | 通过（37 个源文件） |
| 仓库根目录 `make test`（原有 SDK 测试） | 92 通过，1 跳过（`test_process_lifecycle.py`，回环 TCP），22 未选中；与改动前一致 |

未运行：真实网络测试、Linux 上的隔离测试、真实 Redis 测试、真实 Provider 集成测试（`-m integration`）、
容器测试（`-m container`）。

## 7. 已知限制与风险

**已解决**

* MCP 凭证与会话可以按租户隔离（可选）。

**已实现并部分验证，仍有限制**

* MCP HTTP 代理：客户端一侧已验证；真实代理未验证；代理模式不具备连接时 IP 检查。
* 分布式限流参考实现：Python 一侧已验证；Lua 脚本未在真实 Redis 上运行；不是事务或严格计数。
* 新写的真实代理测试中的最小本地代理本身也从未运行过，首次运行时可能需要调整。

**尚未解决（设计边界，保留说明）**

* MCP 注册表不按租户区分。
* 代理模式无法防止 DNS 重绑定。
* 熔断、缓存、去重、单飞、并发额度、MCP 会话只在进程内；去重不是事务；审批是同步等待。
* 进程隔离不是完整沙箱；`setsid` 脱离进程组的后代无法回收；不支持 Windows。
* stdio 启动器无法确认超长消息是否为响应时只能丢弃，对应请求等到超时。
* 隔离工具的 `ToolContext` 不含截止时间；`on_oversize` 不能减少传输阶段的内存占用。

**尚未验证**

* 真实网络上的 Streamable HTTP 调用、取消通知、TCP 连接、TLS 握手。
* Linux 上的进程内存限制与资源清理。
* 经真实代理的 MCP 会话。
* 真实 Redis 上的 Lua 脚本。

## 8. 后续工作

| 优先级 | 事项 | 原因 | 复现或验证步骤 |
|---|---|---|---|
| 高 | 在允许本地 TCP 的环境运行真实网络测试 | 两天都未能验证；含新的代理用例 | `uv run pytest packages/agent-tools/tests -m network` |
| 高 | 在真实 Redis 上验证限流 Lua 脚本 | 参考实现投入使用的前提 | `AGENT_TOOLS_REDIS_URL=redis://127.0.0.1:6379/15 uv run --with redis pytest packages/agent-tools/tests -m redis` |
| 高 | 在 Linux 上运行隔离测试 | 内存限制从未实测 | Linux 主机或容器中：`uv sync && uv run pytest packages/agent-tools/tests/test_isolation.py packages/agent-tools/tests/test_mcp.py`，确认 `test_memory_limit_enforced_on_linux` 未被跳过 |
| 中 | 按租户的注册表视图 | 服务端按租户提供不同工具集时需要 | 需要先做设计评审 |
| 中 | 共享状态的熔断与结果存储参考实现 | 多实例下目前各自为政 | 协议已有，可仿照限流参考实现 |
| 低 | SOCKS 代理、按用户的 MCP 会话 | 目前没有需求 | — |

## 9. 工作区状态

* `provider_sdk/`、`provider_gateway/`、根目录 `tests/` 与 `docs/` 中的原有文件：**未修改**。
* 开始时已有的未提交改动（`pyproject.toml`、`uv.lock`、`agent-tools-architecture.md`、`packages/`）：
  **全部保留**。根目录 `pyproject.toml` 与 `uv.lock` 今天没有再改动。
* 本次改动：**未提交**，没有执行任何 `git add` / `commit` / `reset`。
* 新增的未跟踪路径：`docs/progress/`（本文件）。
