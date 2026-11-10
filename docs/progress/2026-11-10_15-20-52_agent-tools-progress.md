# Agent Tools 研发进度（2026-11-10，第二次）

## 1. 记录时间、范围与开始前的实际状态

* 记录时间：2026-11-10 15:20:52（Asia/Shanghai）。
* 上一次记录：`docs/progress/2026-11-10_11-27-19_agent-tools-progress.md`（保留，未修改）。
* 范围：`packages/agent-tools/` 及其文档。高优先级：三项真实环境验证；中优先级：按租户区分的注册表
  视图、共享状态的熔断与结果存储。
* 不在范围内：`provider_sdk/`、`provider_gateway/`（未修改）；SOCKS 代理、按用户的 MCP 会话（未扩展，
  本次工作没有发现它们是必要条件）。
* 架构层面的记录在 `agent-tools-architecture.md` §15。

开始前核对（以代码和本次实际运行为准）：

| 上次记录的事项 | 开始时的实际状态 | 依据 |
|---|---|---|
| 基线测试 | 与记录一致：319 通过、2 跳过、6 未选中 | 本次运行 `uv run pytest` |
| MCP 租户会话、HTTP 代理、Redis 限流参考实现 | 已实现；真实代理、真实 Redis 未验证 | 代码与 `-m "network or redis"`（0 运行、2 跳过） |
| 真实网络端到端 | 已实现测试，从未运行 | 模块级跳过：回环 TCP 不可用 |
| Linux 内存限制 | 已实现测试，从未运行 | 非 Linux 被跳过 |
| 注册表按租户区分 | 未实现 | `ToolRegistry` 只有一层 |
| 共享状态的熔断、结果存储 | 未实现（只有协议与进程内实现） | `governance/circuit.py`、`governance/reuse.py` |
| 熔断、单飞、并发额度、审批只在进程内；去重不是事务；进程隔离不是沙箱 | 有意保留的设计边界 | 代码与架构文档 §13.6、§14.5 一致 |
| 工作区 | `pyproject.toml`、`uv.lock` 已修改未提交；`agent-tools-architecture.md`、`docs/progress/`、`packages/` 未跟踪 | `git status` |

## 2. 各项需求的状态

| # | 需求 | 状态 | 证据 |
|---|---|---|---|
| 1 | 真实网络与代理验证 | **未完成（环境受限，未验证）** | §2.1 |
| 2 | 真实 Redis 限流验证 | **未完成（环境受限，未验证）** | §2.1 |
| 3 | Linux 进程隔离验证 | **未完成（环境受限，未验证）** | §2.1 |
| 4 | 按租户区分的注册表视图 | **已完成**（可选能力，默认不使用） | §2.2；`tests/test_tenant_registry.py` 34 个用例通过 |
| 5 | 共享状态的结果存储 | **部分完成**：已实现，Python 一侧已验证；真实 Redis 未验证 | §2.3；`tests/test_redis_state.py` |
| 6 | 共享状态的熔断 | **部分完成**：已实现，Python 一侧已验证；**Lua 脚本未在真实 Redis 上运行** | §2.3；同上 |
| 7 | 函数注释规范 | **已完成** | AST 扫描本次改动的 8 个文件，260 个函数 / 方法 / 类，缺失 0，非中文 0 |
| 8 | 测试、文档、进度文件 | **已完成** | §5、§3 |

### 2.1 三项真实环境验证：尝试与阻碍

| 尝试 | 结果 |
|---|---|
| 监听 127.0.0.1 后 `connect()` | `EPERM`（Operation not permitted） |
| 出站连接 1.1.1.1:443 | `EPERM` |
| Unix 域套接字 `bind` / `connect` | `EPERM`（因此也无法通过套接字文件运行本地服务） |
| 同样的探测，解除命令沙箱 | 请求被权限策略拒绝，没有执行，也没有再用其他方式绕过 |
| `docker info`（OrbStack） | `permission denied`（Docker 套接字）；`orbctl status` 为 `Stopped` |
| 本机 Redis | 没有 `redis-server` / `redis-cli`，Homebrew 中也没有 Redis、Valkey 等 |
| 离线安装 `redis` / `fakeredis` / `lupa` | 失败：没有网络，uv 缓存中没有这些包 |
| `uv run pytest -m "network or redis"` | 0 运行，4 跳过（网络模块；3 个真实 Redis 用例） |
| Linux 主机或容器 | 没有；`test_memory_limit_enforced_on_linux` 仍被跳过 |

结论：三项都**没有在真实环境中验证**，状态保持“未验证”。没有用模拟结果替代，没有绕过安全策略。
本次在这三项上实际做了的只有：

* **夹具修复**（`tests/test_network_e2e.py` 的最小本地代理，静态检查发现，修复后同样未运行）：
  1. 明文转发只改写连接上的第一个请求，客户端复用连接发出的后续请求不会被改写、记录或检查代理凭证
     → 转发时去掉逐跳头并强制 `Connection: close`；
  2. 连接上游失败时处理函数带着未关闭的连接异常退出，可能让关闭过程等待不结束 → 回复 502 并关闭。
* **产品代码修复**：无。
* **静态复查的观察**（未修改）：限流 Lua 用 `tostring()` 保存时间戳，Lua 5.1 只保留 14 位有效数字，
  时间精度约 0.1 毫秒，误差至多 `0.0001 × 速率` 个令牌。没有改动未经验证的脚本。
* Linux 隔离的边界没有新的实测数据。按代码，现状仍是：普通子进程 + POSIX 资源限制，**没有**文件系统、
  网络、系统调用隔离；`setsid` 脱离进程组的后代无法回收；`memory_bytes` 只在 Linux 可用（macOS 上安全
  失败）。Python 工具与 MCP stdio 子进程各自适用的限制是否实际生效，仍需在 Linux 上确认。

### 2.2 按租户区分的注册表视图

* 注册表 = 共享层 + 租户层。`registry.snapshot()` 只解析共享层；`snapshot.for_tenant(t)` 是“共享层 +
  租户 t 的层”的只读叠加视图。
* 执行器的 `list_tools` / `execute` / `execute_many` 一律按 `principal.tenant_id` 取视图。其他租户的
  工具表现为 `not_found`，不建立会话、不访问下游；解析之后照常鉴权。
* 名称冲突：租户级工具与共享工具的同名、别名交叉在注册阶段拒绝（双向），租户层不能遮盖共享工具；
  不同租户可以同名同别名。
* `McpToolSync.sync(..., scope="tenant", ctx=...)`：用该租户的会话发现，只写入该租户层；没有租户时在
  连接之前拒绝；不同租户的同步可以并发。`ToolRegistry.drop_tenant()` 删除整层。
* Python、HTTP、MCP 三类工具使用同一套作用域规则。
* 已测试的场景：可见性与跨租户不可解析；四种名称 / 别名冲突（两种注册顺序）；各修改操作的作用域；
  租户层上 `apply` 的原子性；快照不可变与未变化层的对象共享；视图不能换租户；非法租户 ID；
  `drop_tenant`；不可逆工具规则不放宽；三线程并发注册（冲突名字恰好一方成功）；按租户的列表、执行与
  Schema 校验；`snapshot_id` 不能跨租户使用及其过期；在途批次保持视图；同名租户级工具的缓存互不命中；
  MCP 按租户发现（不同工具集、同名不同 Schema）、各用各的凭证、单租户刷新不影响其他租户、与共享层
  同步的双向冲突、不遮盖本地工具、`drop_tenant` 后重新同步、并发同步与调用、默认 `scope="shared"` 不变。

### 2.3 共享状态的结果存储与熔断

* `agent_tools/governance/redis_store.py`：`RedisResultStore`，只用 `GET` / `SET PX`，没有 Lua。
* `agent_tools/governance/redis_circuit.py`：`RedisCircuitBreaker`，两个 Lua 脚本；Redis 不可用时默认退回
  本实例的进程内熔断器，可选 `fail_closed`。
* 核心包与 extras 的依赖**没有变化**；两者都不被 `agent_tools/__init__.py` 导入。
* 验证范围：35 个默认运行的用例使用内存假客户端（熔断脚本在其中用 Python 按相同算法实现），证明的是
  Python 一侧与接入执行器后的行为，**不能证明 Lua 正确**，也不能证明真实 Redis 的过期与回复格式符合
  假设。2 个 `redis` 标记的真实用例已编写、未运行。
* 其中一个用例明确记录了**不提供的保证**：同一调用同时到达两个实例时两边都会执行
  （`test_concurrent_duplicates_on_two_instances_both_execute`）。

## 3. 主要代码、测试和文档变更

| 文件 | 变更 |
|---|---|
| `agent_tools/registry.py` | 新增 `_Layer`、`_Overlay`；`RegistrySnapshot` 新增 `tenants` / `tenant_id` / `for_tenant()`；`ToolRegistry` 各修改方法新增仅关键字的 `tenant_id`，新增 `tenants()`、`drop_tenant()`、跨作用域冲突检查；内部改为“工作副本 + 提交” |
| `agent_tools/executor.py` | `list_tools` / `execute` / `execute_many` 按调用方租户取视图（3 处，各一行逻辑）及对应文档 |
| `agent_tools/integrations/mcp.py` | `McpToolSync.sync(scope=)`、`registered(tenant_id=)`、`McpSyncReport.tenant_id`、`McpSyncScope`；记录与锁按（服务端，租户层）区分 |
| `agent_tools/governance/redis_store.py` | 新增：共享结果存储参考实现 |
| `agent_tools/governance/redis_circuit.py` | 新增：共享状态熔断参考实现 |
| `tests/test_tenant_registry.py` | 新增：34 个用例 |
| `tests/test_redis_state.py` | 新增：35 个默认运行的用例 + 2 个 `redis` 标记用例 |
| `tests/test_network_e2e.py` | 夹具修复（§2.1），未运行 |
| `packages/agent-tools/README.md` | 新增“按租户区分的工具目录”“共享状态的参考实现”；更新熔断、作用范围表、已知限制、兼容性、开发命令 |
| `agent-tools-architecture.md` | 新增 §15；状态行、§14.2、§14.6 加注（原文保留） |

## 4. 设计取舍、兼容性和安全边界

* **默认行为不变**：不使用租户层、不传入 Redis 参考实现时，与上次记录的行为相同；P0–P3 及上次新增的
  测试断言没有修改，全部通过。
* **安全默认值**：基础快照只解析共享层，忘记取视图只会看到更少的工具；租户视图不能换成另一个租户；
  空字符串不是合法的租户 ID；没有租户的租户级同步不会退回共享层。
* **冲突时拒绝而不是遮盖**：避免同一个名字对不同租户解析到权限不同的定义，避免远端 MCP 工具替换
  宿主的本地工具。
* **租户层不是授权**：只决定目录，鉴权照常执行。
* **缓存、去重、并发协调、下游幂等分开说明**：共享存储只做前两者；跨实例单飞与严格一次执行不提供。
* **结果存储的值是明文**：Redis 的访问控制与加密由宿主负责；前缀必填。
* **需要宿主注意的兼容点**：自行调用 `registry.snapshot().resolve()` 并希望看到租户级工具时，需要先
  `for_tenant()`（不使用租户层时没有区别）。私有接口 `ToolRegistry._require` / `_add_to` 的签名变化。
* **已知边界**：熔断隔离键不含租户（同名租户级工具、同一服务端的不同租户共用熔断状态）；所有层共用
  快照历史（其他租户的变更会让 `snapshot_id` 更快过期，可调大 `history`）；目录按租户而不是按用户；
  租户层与 MCP 会话各自管理。

## 5. 实际运行的检查

全部为本次最终代码上的实际结果。

| 检查 | 结果 |
|---|---|
| `packages/agent-tools`：`uv run pytest` | **通过**：388 通过，2 跳过，8 未选中（基线 319；新增 69） |
| 其中跳过的 2 项 | `test_network_e2e.py`（整个模块，回环 TCP 不可用）；`test_memory_limit_enforced_on_linux`（非 Linux） |
| `uv run pytest -m packaging` | **通过**：5 通过 |
| `uv run pytest -m "network or redis"` | **跳过**：0 运行，4 跳过（网络模块；限流、结果存储、熔断的真实 Redis 用例） |
| `uv run ruff check .` / `ruff format --check .` | **通过** |
| `uv run mypy`（strict，52 个源文件） | **通过** |
| 仓库根目录 `make lint` / `make typecheck` | **通过**（37 个源文件） |
| 仓库根目录 `make test`（原有 Provider SDK 测试） | **通过**：92 通过，1 跳过（`test_process_lifecycle.py`，回环 TCP），22 未选中；与改动前一致 |
| 真实网络测试、真实 Redis 测试、Linux 隔离测试 | **未运行**（§2.1） |
| 真实 Provider 集成测试（`-m integration`）、容器测试（`-m container`） | **未运行**（需要外网、密钥与 Docker） |

没有失败的检查。

## 6. 问题清单

**已解决**

* MCP 注册表可以按租户区分（可选）。

**已实现、部分验证**

* 共享结果存储、共享熔断：Python 一侧已验证；真实 Redis 未验证，熔断的 Lua 脚本从未执行过。
* 上次遗留：分布式限流的 Lua、MCP HTTP 代理，状态不变。

**尚未验证（连续三次记录）**

* 真实网络上的 Streamable HTTP 调用、取消通知、TCP 连接、TLS 握手与证书校验；经真实代理的会话与凭证流向。
* 真实 Redis 上的全部脚本。
* Linux 上的进程内存限制与资源清理。
* 网络测试夹具（含本次的修复）从未运行，首次运行时可能需要调整。

**仍存在（设计边界或待决）**

* 熔断隔离键不含租户；跨实例单飞不提供；去重不是事务；审批是同步等待；进程隔离不是完整沙箱；
  代理模式无法防止 DNS 重绑定；其余见架构文档 §15.6。

**待决事项**（需要产品决策或公共契约变更，设计见架构文档 §15.7）：熔断按租户隔离；`ResultStore` 增加
占位语义以支持跨实例单飞；是否允许租户层遮盖共享工具；按租户的快照序号。

## 7. 后续工作

| 优先级 | 事项 | 可复现的验证步骤 |
|---|---|---|
| 高 | 在允许本地 TCP 的环境运行真实网络测试 | `uv run pytest packages/agent-tools/tests -m network -rs`，确认不是模块级跳过 |
| 高 | 在真实 Redis 上验证全部参考实现 | `AGENT_TOOLS_REDIS_URL=redis://127.0.0.1:6379/15 uv run --with redis pytest packages/agent-tools/tests -m redis -rs`（只使用并清理带随机前缀的测试键）。现有真实用例尚未覆盖：Redis 不可用与连接中断、脚本执行错误、不同工具与租户的隔离——可用时需要补充 |
| 高 | 在 Linux 上运行隔离测试 | `uv sync && uv run pytest packages/agent-tools/tests/test_isolation.py packages/agent-tools/tests/test_mcp.py -rs`，确认 `test_memory_limit_enforced_on_linux` 不在跳过列表中 |
| 中 | 对 §15.7 的四个待决事项做决定 | — |
| 低 | SOCKS 代理、按用户的 MCP 会话 | 目前没有需求 |

环境方面需要的是以下任意一种：允许回环 TCP 的本机会话；可访问的 Docker / OrbStack（同时解决 Redis 与
Linux）；或一台 Linux 主机。

## 8. 工作区状态

* `provider_sdk/`、`provider_gateway/`、根目录 `tests/` 中的原有文件：**未修改**。
* 开始时已有的未提交改动（`pyproject.toml`、`uv.lock`、`agent-tools-architecture.md`、`docs/progress/`、
  `packages/`）：**全部保留**；根目录 `pyproject.toml` 与 `uv.lock` 本次没有改动。
* 上一次的进度文件：**保留，未修改**。
* 本次改动：**未提交**，没有执行任何 `git add` / `commit` / `reset` / `stash`。
