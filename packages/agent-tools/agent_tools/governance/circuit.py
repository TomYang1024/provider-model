"""熔断（P3）：协议、错误分类、隔离键与进程内实现。

默认关闭：只有宿主把熔断器传给 ``ToolExecutor(circuit_breaker=...)`` 后才生效。

状态机（每个隔离键一份）::

    closed ──连续失败达到 failure_threshold──► open
    open ──经过 open_s──► half_open（允许最多 half_open_max_calls 个探测调用同时进行）
    half_open ──探测成功累计 success_threshold 次──► closed
    half_open ──任一探测失败──► open（重新计时）

执行顺序（由执行器固定）::

    并发额度（一次调用一份，跨所有重试）
      └─ 重试循环（每次尝试）
           ├─ 熔断准入  ← open 时直接 CircuitOpenError，不消耗限流令牌、不调用下游
           ├─ 速率限制令牌
           ├─ asyncio.timeout(单次超时) 内调用适配器
           └─ 把本次尝试的结果记入熔断器
    CircuitOpenError 不会触发重试。

计入失败的错误（:func:`default_failure_classifier`）：可重试的下游错误（HTTP 5xx / 429、
连接错误、MCP 连接丢失等 ``UpstreamError(retryable=True)``）和单次尝试超时（``attempt_timeout``）。
不计入：参数、权限、本地限流和并发受限、整体截止时间、工具自身业务错误
（``ToolExecutionError``）、输出超限。调用方取消时的探测名额会被释放，不计成功也不计失败。

并发竞争：每个许可记录它被发放时的“代”（状态每切换一次，代加一）。状态切换之后才完成的
旧许可，其结果被忽略——例如熔断开启前发出的慢请求事后成功，**不会**把刚开启的熔断关闭。

局限：进程内实现的状态只在当前进程有效；多实例部署时每个实例独立熔断，不会共享下游健康
状态。需要跨实例统一熔断时，宿主应实现共享状态的 :class:`CircuitBreaker`（协议是异步的）。
"""

from __future__ import annotations

import contextlib
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol
from urllib.parse import urlsplit

from ..binding import ToolBinding
from ..protocol.errors import CircuitOpenError, ToolError, ToolTimeoutError, UpstreamError
from ..protocol.spec import HttpTarget, McpTarget
from ..runtime.clock import Clock, MonotonicClock

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitPermit",
    "CircuitScope",
    "CircuitState",
    "InProcessCircuitBreaker",
    "circuit_key",
    "default_failure_classifier",
]

#: 熔断器状态。
CircuitState = Literal["closed", "open", "half_open"]
#: 隔离粒度：``tool`` 按工具名（所有版本共享）；``target`` 按下游目标——HTTP 为
#: ``scheme://host:port``，MCP 为服务端 ID，Python 工具没有共享下游，仍按工具名。
CircuitScope = Literal["tool", "target"]
#: 一次尝试的结果分类：计入成功、计入失败、不计入（例如参数错误、调用方取消）。
Outcome = Literal["success", "failure", "ignored"]


def default_failure_classifier(error: ToolError) -> bool:
    """判断一次失败的尝试是否说明下游“不健康”、应计入熔断失败。

    Args:
        error: 尝试的错误。

    Returns:
        计入失败时为 True：可重试的 ``UpstreamError``，或单次尝试超时（``attempt_timeout``）。
    """
    if isinstance(error, UpstreamError):
        return error.retryable
    return isinstance(error, ToolTimeoutError) and error.code == "attempt_timeout"


def circuit_key(binding: ToolBinding, scope: CircuitScope) -> str:
    """计算一次调用所属的熔断隔离键。

    键里不包含租户：熔断描述的是下游健康状况，下游对所有租户是同一个。

    Args:
        binding: 工具绑定。
        scope: 隔离粒度。

    Returns:
        隔离键，例如 ``tool:crm.get``、``http:https://api.example.com:443``、``mcp:files``。
    """
    target = binding.target
    if scope == "target":
        if isinstance(target, HttpTarget):
            parts = urlsplit(target.base_url)
            port = parts.port or (443 if parts.scheme == "https" else 80)
            return f"http:{parts.scheme}://{(parts.hostname or '').lower()}:{port}"
        if isinstance(target, McpTarget):
            return f"mcp:{target.server_id}"
    return f"tool:{binding.spec.name}"


class CircuitPermit(Protocol):
    """一次尝试的通行许可。执行器在尝试结束后**恰好调用一次** :meth:`record`。"""

    def record(self, outcome: Outcome) -> None:
        """记录本次尝试的结果并释放许可（同步、无 I/O；实现不得抛出异常）。

        Args:
            outcome: 结果分类。
        """
        ...


class CircuitBreaker(Protocol):
    """熔断器协议。宿主可以实现共享状态的版本（例如基于 Redis），以便多个实例统一熔断。"""

    async def acquire(self, key: str) -> CircuitPermit:
        """为一次尝试申请通行许可。

        Args:
            key: 隔离键（见 :func:`circuit_key`）。

        Returns:
            许可；尝试结束后必须调用 ``record``。

        Raises:
            CircuitOpenError: 熔断开启，或半开状态下探测名额已满。
            asyncio.CancelledError: 等待期间被取消（进程内实现不会等待）。
        """
        ...

    def is_failure(self, error: ToolError) -> bool:
        """判断一个尝试错误是否计入熔断失败。

        Args:
            error: 尝试的错误。

        Returns:
            计入失败时为 True。
        """
        ...

    @property
    def scope(self) -> CircuitScope:
        """隔离粒度，执行器据此计算隔离键。"""
        ...


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    """进程内熔断器的配置。

    Attributes:
        failure_threshold: 关闭状态下连续失败多少次后开启。
        open_s: 开启状态持续多久后进入半开（秒）。
        half_open_max_calls: 半开状态下允许同时进行的探测调用数。
        success_threshold: 半开状态下累计多少次探测成功后关闭。
        scope: 隔离粒度。
        max_keys: 最多跟踪多少个隔离键；超过时淘汰最久未使用的**关闭且无失败计数**的键。
    """

    failure_threshold: int = 5
    open_s: float = 30.0
    half_open_max_calls: int = 1
    success_threshold: int = 1
    scope: CircuitScope = "tool"
    max_keys: int = 10_000

    def __post_init__(self) -> None:
        """校验各项取值。

        Raises:
            ValueError: 阈值、时长或上限不是正数，或隔离粒度未知。
        """
        if min(self.failure_threshold, self.half_open_max_calls, self.success_threshold) <= 0:
            raise ValueError("circuit thresholds must be positive")
        if self.open_s <= 0 or self.max_keys <= 0:
            raise ValueError("open_s and max_keys must be positive")
        if self.scope not in ("tool", "target"):
            raise ValueError(f"unknown circuit scope {self.scope!r}")


@dataclass
class _Circuit:
    """一个隔离键的熔断状态。"""

    state: CircuitState = "closed"
    generation: int = 0
    failures: int = 0
    successes: int = 0
    opened_at: float = 0.0
    probes: int = 0


class _Permit:
    """进程内熔断器发放的许可：记住发放时的“代”，只记录一次结果。"""

    __slots__ = ("_breaker", "_circuit", "_done", "_generation", "_key", "_probe")

    def __init__(
        self, breaker: InProcessCircuitBreaker, key: str, circuit: _Circuit, probe: bool
    ) -> None:
        """创建许可。

        Args:
            breaker: 发放许可的熔断器。
            key: 隔离键。
            circuit: 该键的状态对象。
            probe: 是否占用了半开状态的探测名额。
        """
        self._breaker = breaker
        self._key = key
        self._circuit = circuit
        self._generation = circuit.generation
        self._probe = probe
        self._done = False

    def record(self, outcome: Outcome) -> None:
        """记录结果并释放探测名额；重复调用只生效一次。

        Args:
            outcome: 结果分类。
        """
        if self._done:
            return
        self._done = True
        self._breaker._record(self._key, self._circuit, self._generation, self._probe, outcome)


class InProcessCircuitBreaker:
    """进程内熔断器（只在事件循环线程中使用，不需要加锁）。"""

    def __init__(
        self,
        config: CircuitBreakerConfig | None = None,
        *,
        clock: Clock | None = None,
        classifier: Callable[[ToolError], bool] = default_failure_classifier,
        on_state_change: Callable[[str, CircuitState, CircuitState], None] | None = None,
    ) -> None:
        """创建熔断器。

        Args:
            config: 配置；默认 :class:`CircuitBreakerConfig`。
            clock: 单调时钟（测试注入，用于在不真正等待的情况下推进开启时长）。
            classifier: 判断错误是否计入失败的函数。
            on_state_change: 状态切换回调 ``(键, 旧状态, 新状态)``，用于上报遥测；回调抛出的
                异常会被忽略，不影响熔断判断。
        """
        self._config = config or CircuitBreakerConfig()
        self._clock = clock or MonotonicClock()
        self._classifier = classifier
        self._on_state_change = on_state_change
        self._circuits: OrderedDict[str, _Circuit] = OrderedDict()

    @property
    def scope(self) -> CircuitScope:
        """隔离粒度。"""
        return self._config.scope

    def is_failure(self, error: ToolError) -> bool:
        """按配置的分类函数判断错误是否计入失败。

        Args:
            error: 尝试的错误。

        Returns:
            计入失败时为 True。
        """
        return self._classifier(error)

    def state(self, key: str) -> CircuitState:
        """返回某个键当前的状态（开启时长已到会被视为半开，用于诊断和测试）。

        Args:
            key: 隔离键。

        Returns:
            状态；从未出现过的键为 ``closed``。
        """
        circuit = self._circuits.get(key)
        if circuit is None:
            return "closed"
        self._maybe_half_open(key, circuit)
        return circuit.state

    async def acquire(self, key: str) -> CircuitPermit:
        """申请通行许可（不会等待）。

        Args:
            key: 隔离键。

        Returns:
            许可。

        Raises:
            CircuitOpenError: 开启状态（``circuit_open``），或半开且探测名额已满
                （``circuit_half_open``）。
        """
        circuit = self._circuit(key)
        self._maybe_half_open(key, circuit)
        if circuit.state == "open":
            wait = max(circuit.opened_at + self._config.open_s - self._clock.monotonic(), 0.0)
            raise CircuitOpenError(
                "the tool is temporarily unavailable (circuit open)", retry_after=round(wait, 3)
            )
        if circuit.state == "half_open":
            if circuit.probes >= self._config.half_open_max_calls:
                raise CircuitOpenError(
                    "the tool is temporarily unavailable (circuit half-open, probe in progress)",
                    code="circuit_half_open",
                )
            circuit.probes += 1
            return _Permit(self, key, circuit, probe=True)
        return _Permit(self, key, circuit, probe=False)

    # -- 内部方法 ------------------------------------------------------------------
    def _circuit(self, key: str) -> _Circuit:
        """取得（必要时创建）某个键的状态，并维护最近使用顺序与数量上限。

        Args:
            key: 隔离键。

        Returns:
            状态对象。
        """
        circuit = self._circuits.get(key)
        if circuit is not None:
            self._circuits.move_to_end(key)
            return circuit
        if len(self._circuits) >= self._config.max_keys:
            self._evict()
        circuit = self._circuits[key] = _Circuit()
        return circuit

    def _evict(self) -> None:
        """淘汰最久未使用的、关闭且没有失败计数的键。

        淘汰这样的键不会放宽任何限制：新建的状态本来就是“关闭、无失败”。开启或半开的键
        不会被淘汰（否则熔断会被意外解除）；它们全部处于异常状态时允许暂时超出上限。
        """
        for key, circuit in self._circuits.items():
            if circuit.state == "closed" and circuit.failures == 0:
                del self._circuits[key]
                return

    def _maybe_half_open(self, key: str, circuit: _Circuit) -> None:
        """开启时长已到时把状态切换为半开。

        Args:
            key: 隔离键。
            circuit: 状态对象。
        """
        if circuit.state != "open":
            return
        if self._clock.monotonic() - circuit.opened_at >= self._config.open_s:
            self._transition(key, circuit, "half_open")

    def _transition(self, key: str, circuit: _Circuit, new: CircuitState) -> None:
        """切换状态：代加一（使旧许可的结果失效），重置计数，并通知回调。

        Args:
            key: 隔离键。
            circuit: 状态对象。
            new: 新状态。
        """
        old = circuit.state
        circuit.state = new
        circuit.generation += 1
        circuit.failures = 0
        circuit.successes = 0
        circuit.probes = 0
        if new == "open":
            circuit.opened_at = self._clock.monotonic()
        if self._on_state_change is not None:
            with contextlib.suppress(Exception):
                self._on_state_change(key, old, new)

    def _record(
        self, key: str, circuit: _Circuit, generation: int, probe: bool, outcome: Outcome
    ) -> None:
        """记录一个许可的结果。

        Args:
            key: 隔离键。
            circuit: 状态对象（许可发放时的对象；淘汰后重建的新对象不受影响）。
            generation: 许可发放时的代。
            probe: 许可是否占用了探测名额。
            outcome: 结果分类。
        """
        if generation != circuit.generation:
            return  # 状态已经切换过：旧许可的结果不能影响新状态
        if probe:
            circuit.probes = max(circuit.probes - 1, 0)
        if outcome == "ignored":
            return
        if circuit.state == "half_open":
            if outcome == "failure":
                self._transition(key, circuit, "open")
                return
            circuit.successes += 1
            if circuit.successes >= self._config.success_threshold:
                self._transition(key, circuit, "closed")
            return
        if circuit.state == "closed":
            if outcome == "success":
                circuit.failures = 0
                return
            circuit.failures += 1
            if circuit.failures >= self._config.failure_threshold:
                self._transition(key, circuit, "open")
