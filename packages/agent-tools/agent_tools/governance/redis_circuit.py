"""共享状态熔断的**参考实现**：基于 Redis 的 :class:`~agent_tools.governance.circuit.CircuitBreaker`
（可选，核心包不依赖 Redis）。

状态：给宿主参考和改造的实现，不是默认组件。只有宿主显式构造本类并通过
``ToolExecutor(circuit_breaker=...)`` 传入时才会生效。本包**不声明**对 ``redis`` 的依赖：客户端按
:class:`~agent_tools.governance.redis_ratelimit.RedisScriptClient` 协议（一个 ``eval`` 方法）
鸭子类型传入。

**验证范围（重要）**：Python 一侧（脚本参数与返回值约定、许可的“代”、探测名额的释放、后台记录、
Redis 不可用时的两种故障模式、多个熔断器实例共享状态）用内存中的假客户端测试，假客户端按与
Lua 脚本相同的算法用 Python 实现。**Lua 脚本本身没有在真实 Redis 上运行过**（开发环境没有
Redis，也不允许网络连接）；真实 Redis 上的验证步骤见 ``tests/test_redis_state.py`` 中带
``redis`` 标记的测试。投入生产前必须先在目标 Redis 版本上跑通该测试。

状态机与进程内实现（:mod:`agent_tools.governance.circuit`）相同，只是状态放在 Redis 的一个哈希
里，所有使用同一个 Redis、同一个 ``prefix`` 的实例共同统计失败、共同开合::

    closed ──连续失败达到 failure_threshold──► open
    open ──经过 open_s──► half_open（最多 half_open_max_calls 个探测同时进行，所有实例合计）
    half_open ──探测成功累计 success_threshold 次──► closed
    half_open ──任一探测失败──► open（重新计时）

每个许可记录发放时的“代”；状态每切换一次代加一，旧许可的结果被脚本忽略。时间取自 Redis 的
``TIME``，各实例的本地时钟不参与。

它**不是**什么（与进程内实现的差异）：

* **不是严格一致的全局状态**。“连续失败”在多个实例并发时只是近似：记录是尝试结束后在后台写入
  的（:meth:`CircuitPermit.record` 是同步、无 I/O 的协议方法），写入失败或进程在此之前退出时
  这次结果丢失；熔断开启之前已经放行的请求仍会到达下游；
* **探测名额是带有效期的租约**。占用名额的实例崩溃、或在 Redis 往返途中被取消时，名额不会被
  归还，要等 ``probe_ttl_s`` 过期后才重新可用；过期后原持有者迟到的归还可能让探测数暂时多出
  一个；
* **键过期等于恢复关闭**。哈希在 ``max(2 × open_s, idle_ttl_s)`` 内没有写入就会过期：失败计数
  被遗忘，长期没有调用的开启状态也会直接回到关闭（而不是先半开探测）。键被内存淘汰或主从切换
  丢失写入时同样如此（偏向放行）；
* **不按租户隔离**。隔离键由执行器按工具或下游目标计算
  （:func:`~agent_tools.governance.circuit.circuit_key`），不含租户：熔断描述下游健康状况。
  使用按租户的 MCP 会话与凭证时，一个租户的凭证或连接问题产生的可重试失败会计入所有租户
  共用的状态——这是现有协议的设计边界，本实现没有改变它；
* 只协调熔断。并发额度、单飞与审批仍然只在各自进程内有效。

Redis 不可用（异常、超时、回复格式错误）时的行为由 ``on_error`` 决定：

* ``"local"``（默认）：这次尝试改由本实例内的进程内熔断器判断。Redis 故障期间每个实例各自
  统计（退化为未共享的行为），不会因为 Redis 故障而拒绝所有工具调用，也不会完全失去保护；
* ``"fail_closed"``：拒绝这次尝试（``CircuitOpenError(code="circuit_unavailable")``）。Redis
  成为所有工具调用的单点，只在“宁可不调用”的场景使用。

要求：脚本在写入前后调用 ``TIME``，需要按“效果复制”执行脚本的 Redis（5.0 及以上的默认行为）。
每个脚本只访问一个键，可以用于 Redis Cluster。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import math
from collections.abc import Callable
from typing import Any, Literal

from ..protocol.errors import CircuitOpenError, ToolError
from .circuit import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitPermit,
    CircuitScope,
    CircuitState,
    InProcessCircuitBreaker,
    Outcome,
    default_failure_classifier,
)
from .redis_ratelimit import RedisScriptClient

__all__ = ["ACQUIRE_SCRIPT", "RECORD_SCRIPT", "RedisCircuitBreaker"]

#: 准入脚本。KEYS[1] = 状态哈希；ARGV = 开启时长（毫秒）、半开探测名额、探测租约有效期（毫秒）、
#: 键的过期时间（毫秒）。返回 ``{allowed(0/1), 代, 距离半开的毫秒数, 状态, 当前失败计数,
#: 本次是否发生 open→half_open 切换(0/1)}``。关闭状态下不写入任何内容。
ACQUIRE_SCRIPT = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local open_ms = tonumber(ARGV[1])
local max_probes = tonumber(ARGV[2])
local probe_ttl = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local d = redis.call('HMGET', KEYS[1], 'state', 'gen', 'opened_ms', 'probes', 'probe_ms', 'fail')
local state = d[1] or 'closed'
local gen = tonumber(d[2]) or 0
local opened = tonumber(d[3]) or 0
local probes = tonumber(d[4]) or 0
local probe_ms = tonumber(d[5]) or 0
local fail = tonumber(d[6]) or 0
local changed = 0
if state == 'open' then
  local elapsed = now - opened
  if elapsed < 0 then
    elapsed = 0
    redis.call('HSET', KEYS[1], 'opened_ms', string.format('%.0f', now))
    redis.call('PEXPIRE', KEYS[1], ttl)
  end
  if elapsed < open_ms then
    return {0, gen, open_ms - elapsed, 'open', 0, 0}
  end
  state = 'half_open'
  gen = gen + 1
  probes = 0
  changed = 1
  redis.call('HSET', KEYS[1], 'state', state, 'gen', gen, 'fail', 0, 'succ', 0, 'probes', 0)
  redis.call('PEXPIRE', KEYS[1], ttl)
end
if state == 'half_open' then
  if probes > 0 and now - probe_ms >= probe_ttl then
    probes = 0
  end
  if probes >= max_probes then
    return {0, gen, 0, 'half_open', 0, changed}
  end
  redis.call('HSET', KEYS[1], 'probes', probes + 1, 'probe_ms', string.format('%.0f', now))
  redis.call('PEXPIRE', KEYS[1], ttl)
  return {1, gen, 0, 'half_open', 0, changed}
end
return {1, gen, 0, 'closed', fail, 0}
"""

#: 记录脚本。KEYS[1] = 状态哈希；ARGV = 许可的代、是否占用探测名额（'1'/'0'）、结果
#: （'success' / 'failure' / 'ignored'）、失败阈值、成功阈值、键的过期时间（毫秒）。
#: 返回 ``{是否发生状态切换(0/1), 记录之后的状态}``。代不匹配（状态已经切换过，或键已过期重建）
#: 时什么都不做。
RECORD_SCRIPT = """
local d = redis.call('HMGET', KEYS[1], 'state', 'gen', 'fail', 'succ', 'probes')
local state = d[1] or 'closed'
local gen = tonumber(d[2]) or 0
if gen ~= tonumber(ARGV[1]) then
  return {0, state}
end
local fail = tonumber(d[3]) or 0
local succ = tonumber(d[4]) or 0
local probes = tonumber(d[5]) or 0
local outcome = ARGV[3]
local ttl = tonumber(ARGV[6])
if ARGV[2] == '1' and probes > 0 then
  redis.call('HSET', KEYS[1], 'probes', probes - 1)
end
if outcome == 'ignored' then
  return {0, state}
end
local function trip()
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  redis.call('HSET', KEYS[1], 'state', 'open', 'gen', gen + 1, 'fail', 0, 'succ', 0,
    'probes', 0, 'opened_ms', string.format('%.0f', now))
  redis.call('PEXPIRE', KEYS[1], ttl)
end
if state == 'half_open' then
  if outcome == 'failure' then
    trip()
    return {1, 'open'}
  end
  succ = succ + 1
  if succ >= tonumber(ARGV[5]) then
    redis.call('HSET', KEYS[1], 'state', 'closed', 'gen', gen + 1, 'fail', 0, 'succ', 0,
      'probes', 0)
    redis.call('PEXPIRE', KEYS[1], ttl)
    return {1, 'closed'}
  end
  redis.call('HSET', KEYS[1], 'succ', succ)
  return {0, state}
end
if state == 'closed' then
  if outcome == 'success' then
    if fail > 0 then
      redis.call('HSET', KEYS[1], 'fail', 0)
    end
    return {0, state}
  end
  fail = fail + 1
  if fail >= tonumber(ARGV[4]) then
    trip()
    return {1, 'open'}
  end
  redis.call('HSET', KEYS[1], 'fail', fail)
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return {0, state}
"""

_STATES: frozenset[str] = frozenset({"closed", "open", "half_open"})


class _RedisPermit:
    """共享熔断器发放的许可：记住发放时的代与状态，只记录一次结果。"""

    __slots__ = ("_breaker", "_done", "_fail", "_generation", "_key", "_state")

    def __init__(
        self,
        breaker: RedisCircuitBreaker,
        key: str,
        generation: int,
        state: CircuitState,
        fail: int,
    ) -> None:
        """创建许可。

        Args:
            breaker: 发放许可的熔断器。
            key: 隔离键（未加前缀、未哈希）。
            generation: 发放时的代。
            state: 发放时的状态（``half_open`` 表示占用了一个探测名额）。
            fail: 发放时的失败计数（用于跳过不必要的写入）。
        """
        self._breaker = breaker
        self._key = key
        self._generation = generation
        self._state = state
        self._fail = fail
        self._done = False

    def record(self, outcome: Outcome) -> None:
        """记录结果并释放探测名额；重复调用只生效一次。同步返回，写入在后台任务中进行。

        Args:
            outcome: 结果分类。
        """
        if self._done:
            return
        self._done = True
        self._breaker._record_later(self._key, self._generation, self._state, self._fail, outcome)


class RedisCircuitBreaker:
    """基于 Redis 的共享状态熔断器（参考实现，验证范围与限制见模块说明）。

    满足 :class:`~agent_tools.governance.circuit.CircuitBreaker` 协议。只在事件循环线程中使用；
    不拥有 Redis 客户端，客户端的连接、认证、TLS 与关闭都由宿主负责。
    """

    def __init__(
        self,
        client: RedisScriptClient,
        config: CircuitBreakerConfig | None = None,
        *,
        prefix: str = "agent-tools:circuit:",
        on_error: Literal["local", "fail_closed"] = "local",
        op_timeout_s: float = 0.5,
        probe_ttl_s: float = 120.0,
        idle_ttl_s: float = 3600.0,
        classifier: Callable[[ToolError], bool] = default_failure_classifier,
        on_state_change: Callable[[str, CircuitState, CircuitState], None] | None = None,
        fallback: CircuitBreaker | None = None,
    ) -> None:
        """创建熔断器。

        Args:
            client: Redis 客户端（鸭子类型）。
            config: 阈值与隔离粒度；默认 :class:`CircuitBreakerConfig`。``max_keys`` 只对本地
                回退熔断器有效（Redis 中的键靠过期回收）。
            prefix: 键前缀。同一个 Redis 上互不相干的部署应当使用不同的前缀；共用前缀的实例
                共享熔断状态。
            on_error: Redis 不可用时的行为：``local`` 改用本实例内的熔断器，``fail_closed``
                拒绝这次尝试。
            op_timeout_s: 单次 Redis 操作的超时（秒）；超时按不可用处理。准入发生在单次尝试的
                超时计时之前，所以这个值应当明显小于工具的超时。
            probe_ttl_s: 探测名额租约的有效期（秒）。应当大于单次尝试的最长耗时（工具超时），
                否则仍在进行的探测会被当成已丢失。
            idle_ttl_s: 状态哈希在没有写入时保留多久（秒）；实际过期时间不短于 ``2 × open_s``。
            classifier: 判断错误是否计入失败的函数。
            on_state_change: 状态切换回调 ``(隔离键, 旧状态, 新状态)``，只通知**由本实例的操作
                触发**的切换（其他实例触发的切换不会通知到这里）；回调抛出的异常被忽略。
            fallback: ``on_error="local"`` 时使用的本地熔断器；默认用相同的配置和分类函数创建
                一个 :class:`InProcessCircuitBreaker`。

        Raises:
            ValueError: 超时或有效期不是正数，或 ``on_error`` 取值非法。
        """
        if min(op_timeout_s, probe_ttl_s, idle_ttl_s) <= 0:
            raise ValueError("op_timeout_s, probe_ttl_s and idle_ttl_s must be positive")
        if on_error not in ("local", "fail_closed"):
            raise ValueError("on_error must be 'local' or 'fail_closed'")
        self._client = client
        self._config = config or CircuitBreakerConfig()
        self._prefix = prefix
        self._on_error = on_error
        self._op_timeout_s = op_timeout_s
        self._classifier = classifier
        self._on_state_change = on_state_change
        self._fallback: CircuitBreaker = fallback or InProcessCircuitBreaker(
            self._config, classifier=classifier
        )
        self._open_ms = math.ceil(self._config.open_s * 1000)
        self._probe_ttl_ms = math.ceil(probe_ttl_s * 1000)
        # 过期时间不短于开启时长的两倍：键过期等于恢复关闭，不能让开启状态因过期而提前结束
        self._ttl_ms = math.ceil(max(self._config.open_s * 2, idle_ttl_s) * 1000)
        self._records: set[asyncio.Task[None]] = set()
        #: Redis 操作失败（含超时、回复格式错误、后台记录失败）的累计次数，供宿主导出为指标。
        self.failures = 0

    @property
    def scope(self) -> CircuitScope:
        """隔离粒度，执行器据此计算隔离键。"""
        return self._config.scope

    def is_failure(self, error: ToolError) -> bool:
        """按配置的分类函数判断错误是否计入失败。

        Args:
            error: 尝试的错误。

        Returns:
            计入失败时为 True。
        """
        return self._classifier(error)

    def redis_key(self, key: str) -> str:
        """把隔离键映射成 Redis 键。

        隔离键来自工具名或下游地址（不含租户与参数）；哈希只是为了避免特殊字符和长度问题。

        Args:
            key: 执行器传入的隔离键。

        Returns:
            ``prefix`` + SHA-256 十六进制摘要。
        """
        return self._prefix + hashlib.sha256(key.encode()).hexdigest()

    async def acquire(self, key: str) -> CircuitPermit:
        """为一次尝试申请通行许可（一次 Redis 往返，不会等待状态变化）。

        Args:
            key: 隔离键。

        Returns:
            许可；尝试结束后必须调用 ``record``。Redis 不可用且 ``on_error="local"`` 时返回
            本地熔断器的许可。

        Raises:
            CircuitOpenError: 开启状态（``circuit_open``，带 ``retry_after``）；半开且探测名额
                已满（``circuit_half_open``）；或 ``on_error="fail_closed"`` 时 Redis 不可用
                （``circuit_unavailable``）。本地回退的熔断器也可能抛出前两种。
            asyncio.CancelledError: 被取消。在 Redis 往返途中被取消时，如果脚本已经发放了探测
                名额，这个名额要等租约过期才会释放（无法得知脚本是否已执行）。
        """
        try:
            async with asyncio.timeout(self._op_timeout_s):
                reply = await self._client.eval(
                    ACQUIRE_SCRIPT,
                    1,
                    self.redis_key(key),
                    self._open_ms,
                    self._config.half_open_max_calls,
                    self._probe_ttl_ms,
                    self._ttl_ms,
                )
            allowed, generation, retry_ms, state, fail, changed = _parse_acquire(reply)
        except Exception:
            # CancelledError 不是 Exception，会原样传播
            self.failures += 1
            if self._on_error == "local":
                return await self._fallback.acquire(key)
            raise CircuitOpenError(
                "the tool is temporarily unavailable (circuit state unavailable)",
                code="circuit_unavailable",
            ) from None
        if changed:
            self._notify(key, "open", "half_open")
        if allowed:
            return _RedisPermit(self, key, generation, state, fail)
        if state == "half_open":
            raise CircuitOpenError(
                "the tool is temporarily unavailable (circuit half-open, probe in progress)",
                code="circuit_half_open",
            )
        raise CircuitOpenError(
            "the tool is temporarily unavailable (circuit open)",
            retry_after=round(retry_ms / 1000, 3),
        )

    async def aclose(self) -> None:
        """等待还在进行的后台记录结束。**不会**关闭 Redis 客户端（由宿主负责）。可以重复调用。

        执行器不会调用本方法；宿主应在关闭执行器之后、关闭 Redis 客户端之前调用，否则最后几次
        尝试的结果可能来不及写入。
        """
        while self._records:
            await asyncio.wait(set(self._records))

    # -- 内部方法 ------------------------------------------------------------------
    def _record_later(
        self, key: str, generation: int, state: CircuitState, fail: int, outcome: Outcome
    ) -> None:
        """把一个许可的结果交给后台任务写入 Redis（同步返回，不抛出异常）。

        不需要写入的情况直接跳过，减少 Redis 往返：非探测许可的“不计入”；关闭状态下、发放时
        没有失败计数的“成功”（此时脚本什么都不会改；发放之后别的实例记录的失败不会被这次成功
        清零，偏向更严格）。

        Args:
            key: 隔离键。
            generation: 许可发放时的代。
            state: 许可发放时的状态。
            fail: 许可发放时的失败计数。
            outcome: 结果分类。
        """
        probe = state == "half_open"
        if not probe and (outcome == "ignored" or (outcome == "success" and fail == 0)):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 没有正在运行的事件循环（不应发生：许可只在执行器的协程中使用）；只计数
            self.failures += 1
            return
        task = loop.create_task(self._record(key, generation, probe, state, outcome))
        self._records.add(task)
        task.add_done_callback(self._records.discard)

    async def _record(
        self, key: str, generation: int, probe: bool, state: CircuitState, outcome: Outcome
    ) -> None:
        """执行记录脚本（尽力而为）；失败只累加 :attr:`failures`，不抛出异常。

        Args:
            key: 隔离键。
            generation: 许可发放时的代。
            probe: 许可是否占用了探测名额。
            state: 许可发放时的状态（发生切换时作为回调的旧状态）。
            outcome: 结果分类。
        """
        try:
            async with asyncio.timeout(self._op_timeout_s):
                reply = await self._client.eval(
                    RECORD_SCRIPT,
                    1,
                    self.redis_key(key),
                    generation,
                    "1" if probe else "0",
                    outcome,
                    self._config.failure_threshold,
                    self._config.success_threshold,
                    self._ttl_ms,
                )
            changed, new_state = _parse_record(reply)
        except Exception:
            self.failures += 1
            return
        if changed:
            self._notify(key, state, new_state)

    def _notify(self, key: str, old: CircuitState, new: CircuitState) -> None:
        """调用状态切换回调；回调抛出的异常被忽略，不影响熔断判断。

        Args:
            key: 隔离键。
            old: 旧状态。
            new: 新状态。
        """
        if self._on_state_change is not None:
            with contextlib.suppress(Exception):
                self._on_state_change(key, old, new)


def _text(raw: Any) -> str:
    """把脚本返回的字符串元素统一成 ``str``（客户端可能返回 ``bytes`` 或 ``str``）。

    Args:
        raw: 元素。

    Returns:
        字符串。
    """
    return raw.decode() if isinstance(raw, bytes | bytearray) else str(raw)


def _state(raw: Any) -> CircuitState:
    """解析并校验脚本返回的状态名。

    Args:
        raw: 状态元素。

    Returns:
        状态。

    Raises:
        ValueError: 不是已知的状态名（按 Redis 不可用处理）。
    """
    text = _text(raw)
    if text not in _STATES:
        raise ValueError("unexpected circuit state")
    return text  # type: ignore[return-value]


def _parse_acquire(reply: Any) -> tuple[bool, int, int, CircuitState, int, bool]:
    """解析准入脚本的返回值。

    Args:
        reply: ``[allowed, 代, 距离半开的毫秒数, 状态, 失败计数, 是否切换]``。

    Returns:
        ``(是否放行, 代, 距离半开的毫秒数, 状态, 失败计数, 本次是否发生 open→half_open 切换)``。

    Raises:
        ValueError: 返回值格式不符合约定（按 Redis 不可用处理）。
        TypeError: 元素类型不符合约定（同上）。
    """
    if not isinstance(reply, list | tuple) or len(reply) != 6:
        raise ValueError("unexpected circuit breaker reply")
    generation, retry_ms, fail = int(reply[1]), int(reply[2]), int(reply[4])
    if min(generation, retry_ms, fail) < 0:
        raise ValueError("unexpected circuit breaker reply")
    return int(reply[0]) == 1, generation, retry_ms, _state(reply[3]), fail, int(reply[5]) == 1


def _parse_record(reply: Any) -> tuple[bool, CircuitState]:
    """解析记录脚本的返回值。

    Args:
        reply: ``[是否切换, 状态]``。

    Returns:
        ``(是否发生状态切换, 记录之后的状态)``。

    Raises:
        ValueError: 返回值格式不符合约定（只计入失败次数）。
        TypeError: 元素类型不符合约定（同上）。
    """
    if not isinstance(reply, list | tuple) or len(reply) != 2:
        raise ValueError("unexpected circuit breaker reply")
    return int(reply[0]) == 1, _state(reply[1])
