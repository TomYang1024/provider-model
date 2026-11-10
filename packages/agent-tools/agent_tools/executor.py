"""工具执行器：单次与批量执行、固定顺序的执行链、生命周期管理。

执行链的固定顺序（宿主不能调整，避免出现“先记录再脱敏”“先校验再鉴权”之类的错误）::

    0. 开始观察（Span、在途计数、开始日志，只含元数据）
    1. 全局原始大小检查（解析之前，用最低成本挡住超大输入）
    2. 解析：别名 → 内部名 → 版本 → 适配器
    3. 工具级鉴权（先于参数校验：无权限的调用方看不到参数校验细节）
    4. 单工具大小检查 → JSON 解析 → 参数校验（Python：Pydantic；HTTP：JSON Schema）
    5. 参数级鉴权（可选，宿主提供）
    6. 调用去重（P3，``policy.dedupe``）：重复提交直接重放第一次的结果（不再审批、不再执行）
    7. 人工审批（P3，``policy.approval``）：等待审批不占用执行额度；之后是结果缓存
       （P3，``policy.cache``，与审批互斥），同一个键同时只有一个执行者
    8. 获取并发额度（全局 + 单工具；等待时间计入截止时间；跨所有重试只占一份）
    9. 尝试循环：熔断准入（P3，默认关闭）→ 速率限制令牌 →
       asyncio.timeout(min(工具超时, 剩余时间)) 内调用适配器 → 结果记入熔断器；
       失败时只有“可重试且工具允许重试”才退避后重试，退避不能超过截止时间
    10. 输出处理：输出 Schema 校验（截断之前）→ 大小限制（超限且配置了 ``on_oversize`` 时
        交给钩子改写，改写结果重新校验和限制）→ 清除已知密钥值 →（写入结果缓存）→
        ResultFilter → 再次检查大小
    11. 结束观察（Span 状态、指标、结束日志）；写入结果审计（复用的结果标明来源）

失败与取消的边界：
* 工具层面的任何失败都转换成 ``status="error"`` 的结果，不抛异常；
* 调用方自己的任务被取消时，``asyncio.CancelledError`` 原样抛出，不返回部分结果；
* 执行器关闭时，超过宽限期仍未结束的调用返回 ``ToolCancelledError(executor_closed)`` 结果。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from opentelemetry import metrics, trace

from .adapters.base import AttemptScope, ToolAdapter
from .adapters.python import PythonAdapter
from .binding import ToolBinding
from .governance.approval import ApprovalDecision, ApprovalHandler, ApprovalRequest
from .governance.circuit import CircuitBreaker, CircuitPermit, Outcome, circuit_key
from .governance.deadline import attempt_window, wait_budget
from .governance.limits import ConcurrencyLimiter
from .governance.ratelimit import RateLimiter, TokenBucketRateLimiter
from .governance.retry import MIN_ATTEMPT_S, backoff_delay, should_retry
from .governance.reuse import (
    FlightOutcome,
    ResultStore,
    SingleFlight,
    arguments_digest,
    cache_key,
    canonical_json,
    dedupe_key,
)
from .observability.audit import AuditEvent, AuditSink
from .observability.observer import CallObservation, LogMode, ToolInstrumentation
from .observability.redaction import preview
from .protocol.context import Principal, ToolContext
from .protocol.errors import (
    ApprovalRejectedError,
    ApprovalRequiredError,
    CircuitOpenError,
    ConcurrencyLimitedError,
    ExecutorClosedError,
    OutputLimitError,
    OutputValidationError,
    PermissionDeniedError,
    ToolCancelledError,
    ToolError,
    ToolExecutionError,
    ToolInfraError,
    ToolNotFoundError,
    ToolTimeoutError,
)
from .protocol.invocation import (
    JsonBlock,
    TextBlock,
    ToolExecutionResult,
    ToolInvocation,
    ToolOutput,
)
from .protocol.spec import ToolDescriptor, ToolListing, ToolSpec
from .registry import RegistrySnapshot, ToolRegistry
from .router import Route, ToolRouter
from .runtime.clock import Clock, MonotonicClock
from .runtime.lease import ExecutionLease
from .runtime.process import ProcessRunner
from .runtime.threads import BoundedThreadRunner
from .security.authz import Authorizer, AuthzDecision, ScopeAuthorizer
from .security.output import enforce_output_limits, json_size
from .security.oversize import OversizeHandler
from .security.result_filter import ResultFilter
from .security.secrets import CallSecrets
from .security.validation import (
    argument_size,
    check_argument_size,
    parse_arguments,
    validate_arguments,
)

__all__ = ["ExecutionMode", "ExecutorConfig", "ToolExecutor"]

#: ``auto``：按 ``concurrency_safe`` 分段执行；``sequential``：全部逐个执行。
ExecutionMode = Literal["auto", "sequential"]

#: 结果复用时，等待同键执行者并重新查询存储的最多轮数（防止活锁）。
_MAX_FLIGHT_ROUNDS = 3
#: 审批请求中脱敏参数预览的单值最大字符数。
_APPROVAL_PREVIEW_CHARS = 256
#: 审批请求中按参数名整体隐藏的敏感字段（不区分大小写的子串匹配）。
_SENSITIVE_ARG_NAMES = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "cookie",
    "private_key",
)


@dataclass(frozen=True, slots=True)
class ExecutorConfig:
    """执行器配置。

    Attributes:
        max_parallel: 一个并发段内同时执行的调用上限。
        max_inflight: 进程内全局同时在途的调用上限（含等待孤儿线程结束的额度）。
        max_batch_size: 一次 ``execute_many`` 最多执行的调用数；超出部分直接返回错误结果。
        sync_workers: 同步工具专用线程池的线程数。
        max_raw_input_bytes: 解析之前对原始参数的全局硬上限（字节）；
            每个工具还有自己的 ``policy.max_input_bytes``。
        log_mode: 日志模式；默认 ``metadata``，不记录参数和输出内容；``redacted`` / ``full``
            记录经过脱敏和截断的内容预览（需要显式开启）。
        audit_failure_mode: 工具执行**之前**的审计写入失败时的策略：``fail_open`` 继续执行；
            ``fail_closed`` 拒绝执行（需要配置审计接收端）。执行之后的审计失败两种策略都只能
            计数和记录日志，无法撤回已经发生的执行。
        audit_timeout_s: 单条审计写入的超时秒数。
        store_timeout_s: 结果缓存 / 去重存储单次读写的超时秒数（P3）。
        oversize_timeout_s: ``on_oversize`` 钩子单次处理的超时秒数（P3）。
        max_subprocesses: 进程隔离的 Python 工具同时运行的子进程上限（P3）；等待子进程名额的
            时间计入单次尝试超时，超时前没能启动时返回 ``start_timeout``。
    """

    max_parallel: int = 8
    max_inflight: int = 64
    max_batch_size: int = 64
    sync_workers: int = 8
    max_raw_input_bytes: int = 1024 * 1024
    log_mode: LogMode = "metadata"
    audit_failure_mode: Literal["fail_open", "fail_closed"] = "fail_open"
    audit_timeout_s: float = 1.0
    store_timeout_s: float = 1.0
    oversize_timeout_s: float = 5.0
    max_subprocesses: int = 4

    def __post_init__(self) -> None:
        """校验各项上限都是正数，审计与日志模式的取值合法。

        Raises:
            ValueError: 某项上限不是正数，审计策略或超时非法，或日志模式未知。
        """
        if self.log_mode not in ("off", "metadata", "redacted", "full"):
            raise ValueError(f"unknown log_mode {self.log_mode!r}")
        if self.audit_failure_mode not in ("fail_open", "fail_closed") or self.audit_timeout_s <= 0:
            raise ValueError("invalid audit configuration")
        for name in (
            "max_parallel",
            "max_inflight",
            "max_batch_size",
            "sync_workers",
            "max_raw_input_bytes",
            "store_timeout_s",
            "oversize_timeout_s",
            "max_subprocesses",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass
class _CallState:
    """一次调用在执行过程中逐步补全的状态，用于构造结果（包括被取消时的结果）。"""

    invocation: ToolInvocation
    started: float = field(default_factory=time.perf_counter)
    alias: str = ""
    name: str = ""
    version: str | None = None
    attempts: int = 0
    trace_id: str | None = None
    span_id: str | None = None
    #: 是否已经把调用交给过适配器（工具可能已经产生副作用）；去重据此决定是否记录结果。
    invoked: bool = False
    #: 结果复用的来源（P3）。
    reused: Literal["cache", "dedupe"] | None = None

    def __post_init__(self) -> None:
        """解析成功之前，用调用方传入的名字（截断）作为工具名和别名。"""
        ref = self.invocation.tool[:64]
        self.alias = self.alias or ref
        self.name = self.name or ref

    def _duration_ms(self) -> float:
        """返回从开始到现在的毫秒数。"""
        return round((time.perf_counter() - self.started) * 1000, 3)

    def success(self, output: ToolOutput) -> ToolExecutionResult:
        """构造成功结果。

        Args:
            output: 已经应用过大小限制的输出。

        Returns:
            成功结果。
        """
        return ToolExecutionResult(
            call_id=self.invocation.call_id,
            tool=self.name,
            alias=self.alias,
            version=self.version,
            status="success",
            content=output.content,
            structured=output.structured,
            attempts=self.attempts,
            duration_ms=self._duration_ms(),
            trace_id=self.trace_id,
            span_id=self.span_id,
            reused=self.reused,
        )

    def replay(self, recorded: ToolExecutionResult) -> ToolExecutionResult:
        """把去重记录中第一次执行的结果改写成本次调用的结果（P3）。

        保留第一次的状态、内容、错误和尝试次数；耗时、Trace 关联换成本次调用的值，并标记
        ``reused="dedupe"``。

        Args:
            recorded: 第一次执行的结果。

        Returns:
            本次调用的结果。
        """
        return recorded.model_copy(
            update={
                "duration_ms": self._duration_ms(),
                "trace_id": self.trace_id,
                "span_id": self.span_id,
                "reused": "dedupe",
            }
        )

    def failure(self, error: ToolError) -> ToolExecutionResult:
        """构造失败结果。

        Args:
            error: 工具层错误。

        Returns:
            ``status="error"`` 的结果，只包含错误的安全信息。
        """
        return ToolExecutionResult(
            call_id=self.invocation.call_id,
            tool=self.name,
            alias=self.alias,
            version=self.version,
            status="error",
            error=error.to_info(),
            attempts=self.attempts,
            duration_ms=self._duration_ms(),
            trace_id=self.trace_id,
            span_id=self.span_id,
        )


@dataclass(frozen=True, slots=True)
class _Prepared:
    """执行链第 1–5 步（大小检查、解析、鉴权、参数校验）完成后的结果。

    Attributes:
        route: 路由结果。
        args: 已校验的参数（交给适配器）。
        raw_args: 模型给出的原始参数（用于生成审批请求中的参数视图）。
        args_sha256: 规范化原始参数的摘要；只有审批、缓存或去重需要时才计算。
        args_bytes: 规范化原始参数的字节数（与摘要一起计算）。
    """

    route: Route
    args: dict[str, Any]
    raw_args: dict[str, Any]
    args_sha256: str | None
    args_bytes: int


class ToolExecutor:
    """工具执行器。每个进程通常只创建一个，由所有任务共享。"""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        config: ExecutorConfig | None = None,
        authorizer: Authorizer | None = None,
        audit: AuditSink | None = None,
        authorize_args: Callable[[Principal, ToolSpec, dict[str, Any]], AuthzDecision]
        | None = None,
        rate_limiter: RateLimiter | None = None,
        result_filter: ResultFilter | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        approval_handler: ApprovalHandler | None = None,
        result_cache: ResultStore | None = None,
        dedupe_store: ResultStore | None = None,
        on_oversize: OversizeHandler | None = None,
        adapters: Mapping[str, ToolAdapter] | None = None,
        tracer: trace.Tracer | None = None,
        meter: metrics.Meter | None = None,
        logger: Any | None = None,
        clock: Clock | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """创建执行器。

        Args:
            registry: 工具注册表。
            config: 执行器配置。
            authorizer: 授权实现；默认 :class:`ScopeAuthorizer`。
            audit: 异步审计接收端；不配置时不写专门审计。
            authorize_args: 参数校验之后、工具执行之前调用的同步、无 I/O 的参数级授权回调；
                拒绝或抛出异常时工具都不会执行（失败即拒绝）。
            rate_limiter: 速率限制器；默认进程内令牌桶，只对声明了 ``policy.rate_limit``
                的工具生效。
            result_filter: 对返回给模型的成功输出做最后过滤的扩展点（与日志脱敏不同，见
                :mod:`agent_tools.security.result_filter`）。
            circuit_breaker: 熔断器（P3，默认不熔断），例如
                :class:`~agent_tools.governance.circuit.InProcessCircuitBreaker`。配置后对所有
                工具生效，隔离键与计入失败的错误见 :mod:`agent_tools.governance.circuit`。
            approval_handler: 人工审批处理器（P3）。声明了 ``policy.approval`` 的工具在没有
                处理器时一律安全失败（``approval_unavailable``），不会绕过审批执行。
            result_cache: 结果缓存存储（P3）。只对声明了 ``policy.cache`` 的只读工具生效；
                不配置时这些工具照常执行（只是没有缓存）。
            dedupe_store: 调用去重存储（P3）。只对声明了 ``policy.dedupe`` 的工具生效；
                不配置时这些工具安全失败（``dedupe_unavailable``），不会在没有去重的情况下执行。
                应与 ``result_cache`` 使用不同的实例。
            on_oversize: 超大输出处理钩子（P3，默认不配置：超限即失败），见
                :mod:`agent_tools.security.oversize`。
            adapters: 额外或替换的适配器（``kind`` → 适配器），例如
                ``{"http": HttpAdapter(...)}``；默认只有 Python 适配器。
            tracer: OTel tracer；默认使用全局 tracer。
            meter: OTel meter；默认使用全局 meter。
            logger: 兼容 structlog 接口的日志器。
            clock: 单调时钟；必须与 ``ToolContext.deadline`` 使用同一个时间基准。
            sleep: 重试退避使用的等待函数（测试可以注入）。
        """
        self._registry = registry
        self._config = config or ExecutorConfig()
        self._authorizer: Authorizer = authorizer or ScopeAuthorizer()
        if self._config.audit_failure_mode == "fail_closed" and audit is None:
            raise ValueError("fail_closed requires an audit sink")
        self._audit = audit
        self._authorize_args = authorize_args
        self._clock: Clock = clock or MonotonicClock()
        self._sleep = sleep
        self._rate_limiter: RateLimiter = rate_limiter or TokenBucketRateLimiter(clock=self._clock)
        self._result_filter = result_filter
        self._circuit = circuit_breaker
        self._approval = approval_handler
        if result_cache is not None and result_cache is dedupe_store:
            raise ValueError("result_cache and dedupe_store must be different store instances")
        self._result_cache = result_cache
        self._dedupe_store = dedupe_store
        self._on_oversize = on_oversize
        self._cache_flight = SingleFlight()
        self._dedupe_flight = SingleFlight()
        self._instr = ToolInstrumentation(
            tracer=tracer, meter=meter, logger=logger, log_mode=self._config.log_mode
        )
        self._runner = BoundedThreadRunner(
            self._config.sync_workers,
            on_orphan_change=self._instr.orphan_delta,
            on_orphan_error=self._instr.orphan_error,
        )
        # 进程隔离的 Python 工具（P3）使用的子进程运行器；只有注册了这类工具才会真正启动子进程
        self._processes = ProcessRunner(self._config.max_subprocesses)
        all_adapters: dict[str, ToolAdapter] = {
            "python": PythonAdapter(self._runner, processes=self._processes)
        }
        all_adapters.update(adapters or {})
        self._router = ToolRouter(all_adapters)
        self._limiter = ConcurrencyLimiter(self._config.max_inflight)
        self._tasks: set[asyncio.Task[ToolExecutionResult]] = set()
        # 被 aclose() 主动取消的内部任务，用来区分“执行器关闭”与“调用方取消”
        self._close_cancelled: set[asyncio.Task[ToolExecutionResult]] = set()
        self._closed = False

    # -- 只读属性 ------------------------------------------------------------------
    @property
    def registry(self) -> ToolRegistry:
        """执行器使用的注册表。"""
        return self._registry

    @property
    def closed(self) -> bool:
        """执行器是否已经关闭。"""
        return self._closed

    @property
    def instrumentation(self) -> ToolInstrumentation:
        """可观测性组件（用于诊断，例如查看遥测失败计数）。"""
        return self._instr

    @property
    def thread_runner(self) -> BoundedThreadRunner:
        """同步工具线程池（用于诊断，例如查看孤儿线程数）。"""
        return self._runner

    @property
    def limiter(self) -> ConcurrencyLimiter:
        """并发额度控制器（用于诊断，例如查看在途额度数）。"""
        return self._limiter

    @property
    def process_runner(self) -> ProcessRunner:
        """进程隔离工具的子进程运行器（用于诊断，例如查看存活的子进程数）。"""
        return self._processes

    # -- 公开 API ------------------------------------------------------------------
    def list_tools(self, principal: Principal) -> ToolListing:
        """列出当前快照上、该调用方有权使用的工具（只包含启用的版本）。

        列表过滤只是为了减少模型看到的工具；执行时会再次鉴权，隐藏工具不等于权限控制。
        授权实现抛出异常时，该工具不会出现在列表中（失败即拒绝）。

        租户：目录是“共享工具 + ``principal.tenant_id`` 的租户级工具”（注册表没有租户层时
        与以前完全相同）。其他租户的工具不会出现；没有租户的调用方只看到共享工具。

        Args:
            principal: 调用方身份与权限（``tenant_id`` 必须来自宿主认证，不来自模型）。

        Returns:
            带快照编号的工具列表，顺序稳定（按内部名排序）。
        """
        snapshot = self._registry.snapshot().for_tenant(principal.tenant_id)
        tools: list[ToolDescriptor] = []
        for alias, binding in snapshot.active_bindings():
            spec = binding.spec
            try:
                allowed = self._authorizer.authorize(principal, spec).allowed
            except Exception as exc:
                allowed = False
                self._instr.log_safe("error", "tool.authz.failed", {"exc_type": type(exc).__name__})
            if allowed:
                tools.append(
                    ToolDescriptor(
                        alias=alias,
                        name=spec.name,
                        version=spec.version,
                        description=spec.description,
                        input_schema=spec.input_schema,
                        annotations=spec.annotations,
                    )
                )
        return ToolListing(snapshot_id=snapshot.id, tools=tools)

    async def execute(self, invocation: ToolInvocation, ctx: ToolContext) -> ToolExecutionResult:
        """执行单个工具调用。

        工具在 ``ctx.principal.tenant_id`` 的目录（共享工具 + 该租户的租户级工具）中解析：
        其他租户的工具表现为不存在（``not_found``），解析成功后仍然执行鉴权。

        Args:
            invocation: 调用请求。
            ctx: 调用上下文。

        Returns:
            调用结果；工具层面的失败以 ``status="error"`` 返回。

        Raises:
            ExecutorClosedError: 执行器已关闭。
            asyncio.CancelledError: 调用方的任务被取消（原样传播，不返回部分结果）。
        """
        self._ensure_open()
        snapshot = self._registry.snapshot().for_tenant(ctx.principal.tenant_id)
        return await self._run_tracked(invocation, ctx, snapshot)

    async def execute_many(
        self,
        invocations: Sequence[ToolInvocation],
        ctx: ToolContext,
        *,
        mode: ExecutionMode = "auto",
        snapshot_id: str | None = None,
    ) -> list[ToolExecutionResult]:
        """批量执行工具调用（通常是模型在一轮中返回的全部调用）。

        规则（``mode="auto"``）：
        1. 结果顺序始终与输入顺序一致；
        2. 按输入顺序分段：连续的 ``concurrency_safe=True`` 调用组成一个并发段，
           其余调用各自单独成段；
        3. 各段严格按顺序执行，前一段全部结束后才开始下一段；并发段内最多同时执行
           ``max_parallel`` 个；
        4. 某个调用失败不会中断后续调用，是否停止由宿主根据结果决定；
        5. 无法解析的调用视为非并发安全。

        调用之间的业务依赖（例如先创建再查询）执行器无法知道，由宿主负责：保持相关工具
        ``concurrency_safe=False``，或使用 ``mode="sequential"``，或拆成多轮。

        整批调用使用同一个注册表快照，保证版本一致。快照按 ``ctx.principal.tenant_id`` 取
        租户视图：``snapshot_id`` 只确定“哪个时刻”，看到哪个租户的目录始终由 ``ctx`` 决定，
        拿到别的租户的 ``snapshot_id`` 也无法解析到对方的工具。

        Args:
            invocations: 调用请求列表。
            ctx: 调用上下文（整批共享同一个截止时间）。
            mode: 执行模式。
            snapshot_id: 指定快照编号（来自 ``list_tools``）；快照已失效时所有调用都返回
                ``snapshot_expired`` 错误。

        Returns:
            与输入顺序一致的结果列表。

        Raises:
            ExecutorClosedError: 执行器已关闭。
            asyncio.CancelledError: 调用方的任务被取消；未完成的调用会被一并取消，
                不返回部分结果。
        """
        self._ensure_open()
        invs = list(invocations)
        if not invs:
            return []
        snapshot = (
            self._registry.snapshot()
            if snapshot_id is None
            else self._registry.get_snapshot(snapshot_id)
        )
        if snapshot is None:
            expired = ToolNotFoundError(
                "the tool listing has expired; list the tools again", code="snapshot_expired"
            )
            return [_CallState(inv).failure(expired) for inv in invs]
        snapshot = snapshot.for_tenant(ctx.principal.tenant_id)

        limit = self._config.max_batch_size
        results: list[ToolExecutionResult | None] = [None] * len(invs)
        overflow = ConcurrencyLimitedError(
            f"too many tool calls in one batch (limit {limit})",
            code="batch_too_large",
            retryable=False,
        )
        for i in range(limit, len(invs)):
            results[i] = _CallState(invs[i]).failure(overflow)

        for segment in self._segments(invs[:limit], snapshot, mode):
            if len(segment) == 1:
                index = segment[0]
                results[index] = await self._run_tracked(invs[index], ctx, snapshot)
            else:
                await self._run_segment(segment, invs, ctx, snapshot, results)

        final: list[ToolExecutionResult] = []
        for item in results:
            assert item is not None  # 每个位置都已在上面填充
            final.append(item)
        return final

    async def aclose(self, *, grace_s: float = 5.0) -> None:
        """关闭执行器。

        关闭后不再接受新调用；在途调用最多等待 ``grace_s`` 秒，之后由执行器取消，
        其调用方收到 ``ToolCancelledError(executor_closed)`` 结果。最后关闭各适配器和线程池。
        同步工具的线程无法被终止，会继续运行到结束，本方法不会等待它们；进程隔离工具的子进程
        会随调用被取消而终止，适配器关闭时还会再终止一次残留的子进程（整个进程组）。

        Args:
            grace_s: 等待在途调用自然结束的宽限期（秒）。
        """
        if self._closed:
            return
        self._closed = True
        pending = {task for task in self._tasks if not task.done()}
        if pending:
            _, still_running = await asyncio.wait(pending, timeout=grace_s)
            for task in still_running:
                self._close_cancelled.add(task)
                task.cancel()
            if still_running:
                await asyncio.wait(still_running)
        for adapter in {id(a): a for a in self._router.adapters.values()}.values():
            try:
                await adapter.aclose()
            except Exception as exc:
                self._instr.log_safe(
                    "error", "tool.adapter.close_failed", {"exc_type": type(exc).__name__}
                )
        self._runner.shutdown()

    async def __aenter__(self) -> ToolExecutor:
        """进入异步上下文，返回执行器本身。"""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """退出异步上下文时关闭执行器。

        Args:
            *exc_info: 异常信息（未使用）。
        """
        await self.aclose()

    # -- 内部方法：调度 --------------------------------------------------------------
    def _ensure_open(self) -> None:
        """检查执行器是否仍然可用。

        Raises:
            ExecutorClosedError: 执行器已关闭。
        """
        if self._closed:
            raise ExecutorClosedError("the tool executor is closed")

    def _segments(
        self, invs: Sequence[ToolInvocation], snapshot: RegistrySnapshot, mode: ExecutionMode
    ) -> list[list[int]]:
        """按 ``concurrency_safe`` 把调用切分成按顺序执行的段。

        Args:
            invs: 调用请求列表。
            snapshot: 本批使用的注册表快照。
            mode: 执行模式。

        Returns:
            段列表，每段是调用下标的列表；多于一个下标的段可以并发执行。
        """
        segments: list[list[int]] = []
        current: list[int] = []
        for index, inv in enumerate(invs):
            safe = False
            if mode == "auto":
                try:
                    binding = snapshot.resolve(inv.tool, inv.version)
                    safe = binding.spec.annotations.concurrency_safe
                except ToolError:
                    safe = False
            if safe:
                current.append(index)
                continue
            if current:
                segments.append(current)
                current = []
            segments.append([index])
        if current:
            segments.append(current)
        return segments

    async def _run_segment(
        self,
        segment: list[int],
        invs: Sequence[ToolInvocation],
        ctx: ToolContext,
        snapshot: RegistrySnapshot,
        results: list[ToolExecutionResult | None],
    ) -> None:
        """并发执行一个段内的调用，最多同时 ``max_parallel`` 个。

        调用方被取消时，TaskGroup 会取消段内所有调用，``CancelledError`` 向上传播。

        Args:
            segment: 本段调用的下标。
            invs: 全部调用请求。
            ctx: 调用上下文。
            snapshot: 本批使用的注册表快照。
            results: 结果列表，按下标写入。
        """
        gate = asyncio.Semaphore(self._config.max_parallel)

        async def run_one(index: int) -> None:
            """在并发闸门内执行一个调用，并把结果写回对应位置。

            Args:
                index: 调用下标。
            """
            async with gate:
                results[index] = await self._run_tracked(invs[index], ctx, snapshot)

        async with asyncio.TaskGroup() as group:
            for index in segment:
                group.create_task(run_one(index))

    async def _run_tracked(
        self, invocation: ToolInvocation, ctx: ToolContext, snapshot: RegistrySnapshot
    ) -> ToolExecutionResult:
        """在执行器自己的内部任务中运行一次调用，并区分两种取消来源。

        * 调用方被取消（当前任务的 ``cancelling() > 0``）：确保内部任务也被取消并完成清理，
          然后原样抛出 ``CancelledError``；
        * 内部任务被 :meth:`aclose` 取消：转换为 ``executor_closed`` 错误结果。

        Args:
            invocation: 调用请求。
            ctx: 调用上下文。
            snapshot: 注册表快照。

        Returns:
            调用结果。

        Raises:
            asyncio.CancelledError: 调用方被取消。
        """
        state = _CallState(invocation)
        task = asyncio.create_task(
            self._run_call(state, ctx, snapshot), name=f"agent_tools.call.{invocation.call_id}"
        )
        self._tasks.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            caller_cancelled = current is not None and current.cancelling() > 0
            if not caller_cancelled and task in self._close_cancelled:
                return state.failure(
                    ToolCancelledError("the tool executor was closed", code="executor_closed")
                )
            if not task.done():
                task.cancel()
                # 等内部任务完成清理（归还额度、结束 Span），保证调用方拿到取消时状态已一致
                await asyncio.wait({task})
            raise
        finally:
            self._tasks.discard(task)
            self._close_cancelled.discard(task)

    # -- 内部方法：执行链 ------------------------------------------------------------
    async def _run_call(
        self, state: _CallState, ctx: ToolContext, snapshot: RegistrySnapshot
    ) -> ToolExecutionResult:
        """执行完整的执行链，并把任何工具层失败转换成结果。

        Args:
            state: 调用状态。
            ctx: 调用上下文。
            snapshot: 注册表快照。

        Returns:
            调用结果。

        Raises:
            asyncio.CancelledError: 本任务被取消（记录 ``cancelled`` 终态后原样抛出）。
        """
        obs = self._instr.begin(state.invocation, ctx)
        state.trace_id, state.span_id = obs.trace_ids
        lease = ExecutionLease(asyncio.get_running_loop())
        # 本次调用（跨所有尝试）取得的密钥值：在日志预览、Span、审计和结果中精确清除
        secrets = CallSecrets()
        obs.set_scrubber(secrets.scrub_text)
        output_bytes: int | None = None
        try:
            try:
                prepared = await self._prepare(state, ctx, snapshot, obs)
                if prepared.route.binding.spec.policy.dedupe is not None:
                    result, output_bytes = await self._run_deduped(
                        state, ctx, prepared, obs, lease, secrets
                    )
                else:
                    result, output_bytes = await self._run_once(
                        state, ctx, prepared, obs, lease, secrets
                    )
            except ToolError as exc:
                result = self._failure(state, ctx, exc)
            except asyncio.CancelledError:
                task = asyncio.current_task()
                closing = task is not None and task in self._close_cancelled
                obs.cancelled("executor_closed" if closing else "caller_cancelled")
                raise
            except Exception as exc:
                # 基础设施自身的意外错误：结果里只有固定文案，异常类型写本地日志
                self._instr.log_safe(
                    "error",
                    "tool.call.internal_error",
                    {
                        "request_id": ctx.request_id,
                        "call_id": state.invocation.call_id,
                        "exc_type": type(exc).__name__,
                    },
                )
                result = state.failure(ToolInfraError("internal error while running the tool"))
        finally:
            lease.finish()
        try:
            await self._emit_audit(
                AuditEvent(
                    "result",
                    ctx.request_id,
                    state.invocation.call_id,
                    state.name,
                    ctx.principal.tenant_id,
                    result.status,
                    error_type=result.error.type if result.error else None,
                    error_code=result.error.code if result.error else None,
                    attempts=result.attempts,
                    reused=result.reused,
                ),
                before_execution=False,
            )
        except asyncio.CancelledError:
            obs.cancelled()
            raise
        obs.finish(result, output_bytes=output_bytes)
        return result

    def _failure(self, state: _CallState, ctx: ToolContext, exc: ToolError) -> ToolExecutionResult:
        """把工具层错误转换成失败结果；工具代码抛出普通异常时把异常类型写本地日志。

        Args:
            state: 调用状态。
            ctx: 调用上下文。
            exc: 工具层错误。

        Returns:
            失败结果（只包含错误的安全信息）。
        """
        if isinstance(exc, ToolExecutionError) and exc.__cause__ is not None:
            self._instr.log_safe(
                "warning",
                "tool.call.tool_raised",
                {
                    "request_id": ctx.request_id,
                    "call_id": state.invocation.call_id,
                    "tool": state.alias,
                    "exc_type": type(exc.__cause__).__name__,
                },
            )
        return state.failure(exc)

    async def _emit_audit(self, event: AuditEvent, *, before_execution: bool) -> None:
        """限时写入审计；执行前失败时可选择拒绝调用。

        Args:
            event: 不包含调用内容的审计事件。
            before_execution: 是否处于工具执行之前。

        Raises:
            ToolInfraError: 执行前写入失败且配置为 fail_closed。
            asyncio.CancelledError: 调用方取消时向上传播。
        """
        if self._audit is None:
            return
        try:
            async with asyncio.timeout(self._config.audit_timeout_s):
                await self._audit.emit(event)
        except Exception as exc:
            # 只记录异常类型：接收端的异常消息可能包含审计后端的内部信息
            self._instr.safe(lambda: self._instr.audit_failures.add(1))
            self._instr.log_safe(
                "error",
                "tool.audit.failed",
                {
                    "exc_type": type(exc).__name__,
                    "audit_kind": event.kind,
                    "before_execution": before_execution,
                },
            )
            if before_execution and self._config.audit_failure_mode == "fail_closed":
                raise ToolInfraError("audit unavailable", code="audit_unavailable") from None

    async def _prepare(
        self,
        state: _CallState,
        ctx: ToolContext,
        snapshot: RegistrySnapshot,
        obs: CallObservation,
    ) -> _Prepared:
        """按固定顺序执行第 1–5 步：大小检查、解析、工具级鉴权、参数校验、参数级鉴权。

        这些步骤都发生在任何结果复用、审批和执行之前：没有权限或参数非法的调用既不会执行工具、
        不会打扰审批人，也拿不到缓存或去重记录中的结果。

        Args:
            state: 调用状态（解析成功后补全工具信息）。
            ctx: 调用上下文。
            snapshot: 注册表快照。
            obs: 本次调用的观察者。

        Returns:
            准备好的路由与参数。

        Raises:
            ToolError: 任何一步的工具层失败。
            asyncio.CancelledError: 被取消。
        """
        inv = state.invocation
        # 1. 解析前的全局硬上限
        raw_size = argument_size(inv.arguments)
        obs.set_arguments(raw_size, inv.arguments)
        check_argument_size(raw_size, self._config.max_raw_input_bytes)

        # 2. 解析
        route = self._router.route(snapshot, inv)
        spec = route.binding.spec
        state.alias, state.name, state.version = route.alias, spec.name, spec.version
        obs.resolved(alias=route.alias, name=spec.name, version=spec.version, kind=spec.kind)

        # 3. 工具级鉴权：先于参数校验，原因代码只写日志，不返回给模型
        decision = self._authorizer.authorize(ctx.principal, spec)
        await self._emit_audit(
            AuditEvent(
                "decision",
                ctx.request_id,
                inv.call_id,
                spec.name,
                ctx.principal.tenant_id,
                "allowed" if decision.allowed else "denied",
                decision.reason,
            ),
            before_execution=True,
        )
        if not decision.allowed:
            raise PermissionDeniedError("not permitted to call this tool")

        # 4. 单工具大小检查、解析与校验
        check_argument_size(raw_size, spec.policy.max_input_bytes)
        raw_args = parse_arguments(inv.arguments)
        args = validate_arguments(route.binding, raw_args)

        # 5. 参数级鉴权（可选）
        if self._authorize_args is not None:
            args_decision = self._authorize_args(ctx.principal, spec, args)
            await self._emit_audit(
                AuditEvent(
                    "decision",
                    ctx.request_id,
                    inv.call_id,
                    spec.name,
                    ctx.principal.tenant_id,
                    "allowed" if args_decision.allowed else "denied",
                    "arguments",
                ),
                before_execution=True,
            )
            if not args_decision.allowed:
                raise PermissionDeniedError("not permitted to call this tool with these arguments")

        policy = spec.policy
        digest: str | None = None
        size = 0
        if policy.approval is not None or policy.cache is not None or policy.dedupe is not None:
            # 用模型给出的原始参数（JSON 类型）计算摘要：与校验后的 Python 对象相比更稳定
            digest, size = arguments_digest(raw_args)
        return _Prepared(
            route=route, args=args, raw_args=raw_args, args_sha256=digest, args_bytes=size
        )

    async def _approve(
        self,
        state: _CallState,
        ctx: ToolContext,
        prepared: _Prepared,
        obs: CallObservation,
    ) -> None:
        """请求人工审批并按决定放行或拒绝（P3）。

        等待上限为 ``min(ApprovalPolicy.timeout_s, 剩余截止时间)``。所有结束方式（批准、拒绝、
        超时、处理器失败、没有处理器）都在工具执行之前写入 ``approval`` 审计事件；调用方取消
        时 ``CancelledError`` 原样传播，处理器协程随之被取消，不写审计。

        Args:
            state: 调用状态。
            ctx: 调用上下文。
            prepared: 已完成鉴权与校验的调用。
            obs: 本次调用的观察者。

        Raises:
            ApprovalRequiredError: 没有处理器（``approval_unavailable``）、超时
                （``approval_timeout``）或处理器失败（``approval_failed``）。
            ApprovalRejectedError: 审批人拒绝。
            ToolTimeoutError: 开始审批前整体截止时间已经过去。
            ToolInfraError: 审批审计写入失败且配置为 ``fail_closed``。
            asyncio.CancelledError: 调用方取消或执行器关闭。
        """
        spec = prepared.route.binding.spec
        policy = spec.policy.approval
        assert policy is not None  # 只在声明了审批时调用
        handler = self._approval
        decision: ApprovalDecision | None = None
        if handler is None:
            outcome = "unavailable"
        else:
            remaining = ctx.remaining(self._clock.monotonic())
            timeout_s = policy.timeout_s if remaining is None else min(policy.timeout_s, remaining)
            if timeout_s <= 0:
                raise ToolTimeoutError(
                    "deadline exceeded before approval", code="deadline_exceeded", retryable=False
                )
            request = ApprovalRequest(
                request_id=ctx.request_id,
                call_id=state.invocation.call_id,
                tool=spec.name,
                alias=prepared.route.alias,
                version=spec.version,
                description=spec.description,
                annotations=spec.annotations,
                principal=ctx.principal,
                arguments_view=policy.arguments,
                arguments=_approval_arguments(policy.arguments, prepared.raw_args),
                arguments_sha256=prepared.args_sha256 or "",
                arguments_bytes=prepared.args_bytes,
                timeout_s=timeout_s,
            )
            timeout_cm = asyncio.timeout(timeout_s)
            try:
                async with timeout_cm:
                    returned = await handler.request_approval(request)
                if isinstance(returned, ApprovalDecision):
                    decision = returned
                    outcome = "approved" if returned.approved else "rejected"
                else:
                    outcome = "error"
            except TimeoutError:
                outcome = "timeout" if timeout_cm.expired() else "error"
            except Exception as exc:
                outcome = "error"
                self._instr.log_safe(
                    "error",
                    "tool.approval.failed",
                    {
                        "request_id": ctx.request_id,
                        "call_id": state.invocation.call_id,
                        "exc_type": type(exc).__name__,
                    },
                )
        obs.event("approval", {"outcome": outcome})
        await self._emit_audit(
            AuditEvent(
                "approval",
                ctx.request_id,
                state.invocation.call_id,
                spec.name,
                ctx.principal.tenant_id,
                outcome,
                _clip(decision.reason if decision is not None else None),
                approver=_clip(decision.approver if decision is not None else None),
            ),
            before_execution=True,
        )
        if outcome == "approved":
            return
        if outcome == "rejected":
            raise ApprovalRejectedError("the tool call was rejected by the approver")
        if outcome == "unavailable":
            raise ApprovalRequiredError(
                "this tool requires human approval, but no approval handler is configured",
                code="approval_unavailable",
            )
        if outcome == "timeout":
            raise ApprovalRequiredError(
                "the tool call was not approved in time", code="approval_timeout"
            )
        raise ApprovalRequiredError("the approval request failed", code="approval_failed")

    async def _run_once(
        self,
        state: _CallState,
        ctx: ToolContext,
        prepared: _Prepared,
        obs: CallObservation,
        lease: ExecutionLease,
        secrets: CallSecrets,
    ) -> tuple[ToolExecutionResult, int | None]:
        """审批（工具声明了 ``policy.approval`` 时）后执行（或从结果缓存复用）一次调用，并构造结果。

        每次调用只审批一次，重试不会再次审批。审批失败（拒绝、超时、没有处理器）时工具不执行，
        返回对应的失败结果。

        Args:
            state: 调用状态。
            ctx: 调用上下文。
            prepared: 已完成鉴权与校验的调用。
            obs: 本次调用的观察者。
            lease: 本次调用的执行租约。
            secrets: 本次调用的已知密钥值集合。

        Returns:
            ``(结果, 输出字节数)``；失败时字节数为 ``None``。

        Raises:
            asyncio.CancelledError: 被取消。
        """
        try:
            if prepared.route.binding.spec.policy.approval is not None:
                await self._approve(state, ctx, prepared, obs)
            output = await self._execute(state, ctx, prepared, obs, lease, secrets)
        except ToolError as exc:
            return self._failure(state, ctx, exc), None
        return state.success(output), _output_bytes(output)

    async def _run_deduped(
        self,
        state: _CallState,
        ctx: ToolContext,
        prepared: _Prepared,
        obs: CallObservation,
        lease: ExecutionLease,
        secrets: CallSecrets,
    ) -> tuple[ToolExecutionResult, int | None]:
        """带调用去重地执行一次调用（P3，工具声明了 ``policy.dedupe``）。

        流程：查询去重记录 → 有记录则重放第一次的结果（重放不执行工具，因此**不会**再次请求
        审批）；没有记录则尝试成为该键的执行者（执行者按需审批后执行），同键的并发调用等待
        执行者结束后重新查询。执行者在工具**已经开始执行**（适配器标记了开始）时把最终结果
        （成功或失败）写入记录；工具没有开始执行（例如审批被拒绝、被限流或熔断拒绝、等不到
        执行资源）时不写，之后的重复提交可以重新执行。

        等待者看到执行者“已经开始执行但没有留下记录”（调用方取消、记录写入失败）时返回
        ``duplicate_call_aborted`` 错误，而不是再执行一次：此时第一次执行的效果未知。

        Args:
            state: 调用状态。
            ctx: 调用上下文。
            prepared: 已完成鉴权与校验的调用。
            obs: 本次调用的观察者。
            lease: 本次调用的执行租约。
            secrets: 本次调用的已知密钥值集合。

        Returns:
            ``(结果, 输出字节数)``；重放或失败时字节数为 ``None``。

        Raises:
            ToolInfraError: 没有配置去重存储，或读取存储失败（``dedupe_unavailable``），工具不执行。
            ToolCancelledError: 同一调用的第一次执行结果未知（``duplicate_call_aborted``）。
            ToolTimeoutError: 等待同一调用的第一次执行时截止时间已到。
            asyncio.CancelledError: 被取消。
        """
        store = self._dedupe_store
        if store is None:
            raise ToolInfraError(
                "call deduplication is required but no dedupe store is configured",
                code="dedupe_unavailable",
            )
        spec = prepared.route.binding.spec
        dedupe = spec.policy.dedupe
        assert dedupe is not None
        assert prepared.args_sha256 is not None
        key = dedupe_key(ctx, state.invocation.call_id, spec, prepared.args_sha256)
        previous: FlightOutcome | None = None
        for _ in range(_MAX_FLIGHT_ROUNDS):
            raw = await self._store_get(store, key, kind="dedupe")
            if raw is not None:
                try:
                    recorded = ToolExecutionResult.model_validate_json(raw)
                except ValueError:
                    raise ToolInfraError(
                        "the deduplication record is unreadable", code="dedupe_unavailable"
                    ) from None
                state.reused = "dedupe"
                obs.event("reused", {"source": "dedupe"})
                return state.replay(recorded), None
            if previous in ("stored", "aborted"):
                # 第一次执行已经开始，但现在查不到它的记录：不能再执行一次
                raise ToolCancelledError(
                    "an identical call already ran but its result was not recorded; "
                    "its effect is unknown",
                    code="duplicate_call_aborted",
                )
            waiter = self._dedupe_flight.join(key)
            if waiter is None:
                break
            previous = await self._await_flight(waiter, ctx)
        else:
            raise ConcurrencyLimitedError(
                "an identical call is still in progress", code="duplicate_in_progress"
            )

        outcome: FlightOutcome = "not_started"
        try:
            result, output_bytes = await self._run_once(state, ctx, prepared, obs, lease, secrets)
            if state.invoked:
                outcome = "aborted"
                stored = await self._store_set(
                    store, key, result.model_dump_json().encode(), dedupe.window_s, kind="dedupe"
                )
                if stored:
                    outcome = "stored"
            return result, output_bytes
        except BaseException:
            if state.invoked:
                outcome = "aborted"
            raise
        finally:
            self._dedupe_flight.finish(key, outcome)

    async def _execute(
        self,
        state: _CallState,
        ctx: ToolContext,
        prepared: _Prepared,
        obs: CallObservation,
        lease: ExecutionLease,
        secrets: CallSecrets,
    ) -> ToolOutput:
        """执行调用，或在声明了 ``policy.cache`` 且配置了缓存时复用缓存的成功结果（P3）。

        缓存命中时工具不执行，但结果仍会经过 ``ResultFilter`` 与大小限制（过滤可能依赖
        调用上下文，所以缓存的是过滤之前的输出）。同键的并发未命中只有一个执行者，其余等待；
        执行者没有写入缓存（失败、被取消）时，等待者各自直接执行，不再排队。读取缓存失败
        视为未命中。

        Args:
            state: 调用状态。
            ctx: 调用上下文。
            prepared: 已完成鉴权与校验的调用。
            obs: 本次调用的观察者。
            lease: 本次调用的执行租约。
            secrets: 本次调用的已知密钥值集合。

        Returns:
            最终返回给模型的输出。

        Raises:
            ToolError: 执行链中的工具层失败。
            asyncio.CancelledError: 被取消。
        """
        spec = prepared.route.binding.spec
        policy = spec.policy.cache
        store = self._result_cache
        key: str | None = None
        if policy is not None and store is not None and prepared.args_sha256 is not None:
            key = cache_key(policy, ctx.principal, spec, prepared.args_sha256)
        if key is None or store is None:
            return await self._produce(state, ctx, prepared, obs, lease, secrets, cache_to=None)

        for _ in range(_MAX_FLIGHT_ROUNDS):
            hit = await self._store_get(store, key, kind="cache")
            if hit is not None:
                try:
                    cached = ToolOutput.model_validate_json(hit)
                except ValueError:
                    cached = None  # 记录损坏：按未命中处理，执行后覆盖
                if cached is not None:
                    state.reused = "cache"
                    obs.event("reused", {"source": "cache"})
                    return self._filter_output(spec, cached, ctx)
            waiter = self._cache_flight.join(key)
            if waiter is None:
                break
            if await self._await_flight(waiter, ctx) != "stored":
                # 执行者没有留下结果：直接执行，避免同键的调用逐个排队
                return await self._produce(state, ctx, prepared, obs, lease, secrets, cache_to=None)
        else:
            return await self._produce(state, ctx, prepared, obs, lease, secrets, cache_to=None)

        outcome: FlightOutcome = "not_started"
        try:
            output = await self._produce(state, ctx, prepared, obs, lease, secrets, cache_to=key)
            outcome = "stored"
            return output
        finally:
            self._cache_flight.finish(key, outcome)

    async def _produce(
        self,
        state: _CallState,
        ctx: ToolContext,
        prepared: _Prepared,
        obs: CallObservation,
        lease: ExecutionLease,
        secrets: CallSecrets,
        *,
        cache_to: str | None,
    ) -> ToolOutput:
        """真正执行工具：并发额度 → 尝试循环 → 输出处理 →（写缓存）→ 结果过滤。

        Args:
            state: 调用状态。
            ctx: 调用上下文。
            prepared: 已完成鉴权与校验的调用。
            obs: 本次调用的观察者。
            lease: 本次调用的执行租约。
            secrets: 本次调用的已知密钥值集合。
            cache_to: 成功后写入结果缓存的键；``None`` 表示不写。

        Returns:
            最终返回给模型的输出。

        Raises:
            ToolError: 工具层失败。
            asyncio.CancelledError: 被取消。
        """
        route = prepared.route
        spec = route.binding.spec
        # 并发额度：排队时间计入截止时间；一次调用（包括所有重试）只占一份额度
        await self._limiter.acquire(
            f"{spec.name}@{spec.version}",
            spec.policy.max_concurrency,
            wait_s=wait_budget(ctx, spec.policy.timeout_s, self._clock.monotonic()),
            lease=lease,
        )
        output = await self._attempt_loop(state, ctx, route, prepared.args, obs, lease, secrets)
        processed = await self._process_output(route.binding, output, ctx, secrets, obs)
        if (
            cache_to is not None
            and spec.policy.cache is not None
            and self._result_cache is not None
        ):
            await self._store_set(
                self._result_cache,
                cache_to,
                processed.model_dump_json().encode(),
                spec.policy.cache.ttl_s,
                kind="cache",
            )
        return self._filter_output(spec, processed, ctx)

    async def _process_output(
        self,
        binding: ToolBinding,
        output: ToolOutput,
        ctx: ToolContext,
        secrets: CallSecrets,
        obs: CallObservation,
    ) -> ToolOutput:
        """输出处理（过滤之前的部分）：Schema 校验 → 大小限制（可交给 ``on_oversize``）→ 密钥清除。

        Args:
            binding: 工具绑定。
            output: 适配器返回的输出。
            ctx: 调用上下文。
            secrets: 本次调用的已知密钥值集合。
            obs: 本次调用的观察者。

        Returns:
            满足大小限制、已清除已知密钥值的输出。

        Raises:
            OutputValidationError: 输出（或钩子改写后的输出）不符合输出 Schema。
            OutputLimitError: 超限且没有钩子、钩子失败或超时（原错误），或钩子改写后仍超限
                （``output_too_large_after_hook``）。
            asyncio.CancelledError: 被取消。
        """
        spec = binding.spec
        limits = spec.policy.output_limits
        # 对完整数据校验（在任何截断之前），避免截断让校验结果失真
        _validate_output(binding, output)
        try:
            limited = enforce_output_limits(output, limits)
        except OutputLimitError as exc:
            if self._on_oversize is None:
                raise
            replaced = await self._handle_oversize(
                spec, _scrub_output(output, secrets), exc, ctx, obs
            )
            # 钩子的输出同样不可信：重新校验、重新限制，不能借钩子绕过任何检查
            _validate_output(binding, replaced)
            try:
                limited = enforce_output_limits(replaced, limits)
            except OutputLimitError:
                obs.event("oversize", {"outcome": "still_too_large"})
                raise OutputLimitError(
                    "tool output is still too large after on_oversize processing",
                    code="output_too_large_after_hook",
                ) from None
        return _scrub_output(limited, secrets)

    async def _handle_oversize(
        self,
        spec: ToolSpec,
        output: ToolOutput,
        error: OutputLimitError,
        ctx: ToolContext,
        obs: CallObservation,
    ) -> ToolOutput:
        """在限定时间内调用 ``on_oversize`` 钩子；任何失败都退回原来的大小限制错误。

        Args:
            spec: 工具规格。
            output: 已清除已知密钥值的完整输出。
            error: 触发钩子的大小限制错误。
            ctx: 调用上下文。
            obs: 本次调用的观察者。

        Returns:
            钩子改写后的输出（尚未校验）。

        Raises:
            OutputLimitError: 钩子超时、抛出异常或返回了非 ``ToolOutput``（即原来的 ``error``）。
            asyncio.CancelledError: 被取消。
        """
        handler = self._on_oversize
        assert handler is not None
        timeout_cm = asyncio.timeout(self._config.oversize_timeout_s)
        try:
            async with timeout_cm:
                replaced = await handler.on_oversize(spec, output, error, ctx)
        except Exception as exc:
            outcome = (
                "timeout" if isinstance(exc, TimeoutError) and timeout_cm.expired() else "error"
            )
            obs.event("oversize", {"outcome": outcome})
            if outcome == "error":
                self._instr.log_safe(
                    "error", "tool.oversize.failed", {"exc_type": type(exc).__name__}
                )
            raise error from None
        if not isinstance(replaced, ToolOutput):
            obs.event("oversize", {"outcome": "error"})
            raise error
        obs.event("oversize", {"outcome": "replaced"})
        return replaced

    def _filter_output(self, spec: ToolSpec, output: ToolOutput, ctx: ToolContext) -> ToolOutput:
        """应用 ``ResultFilter``（如果配置了），再检查一次大小限制。

        Args:
            spec: 工具规格。
            output: 已处理的输出。
            ctx: 调用上下文。

        Returns:
            最终返回给模型的输出。

        Raises:
            OutputLimitError: 过滤后的输出超限。
            Exception: 过滤器自身抛出的异常（由上层按内部错误处理，失败即关闭）。
        """
        if self._result_filter is None:
            return output
        filtered = self._result_filter.filter(spec, output, ctx)
        return enforce_output_limits(filtered, spec.policy.output_limits)

    async def _store_get(self, store: ResultStore, key: str, *, kind: str) -> bytes | None:
        """限时读取结果存储。

        Args:
            store: 存储。
            key: 键。
            kind: ``cache`` 或 ``dedupe``；决定失败时的策略。

        Returns:
            值；不存在时为 ``None``。缓存读取失败也返回 ``None``（按未命中处理）。

        Raises:
            ToolInfraError: 去重存储读取失败（``dedupe_unavailable``）：不知道是否执行过，
                所以不执行（失败即关闭）。
            asyncio.CancelledError: 被取消。
        """
        try:
            async with asyncio.timeout(self._config.store_timeout_s):
                return await store.get(key)
        except Exception as exc:
            self._instr.log_safe(
                "error",
                "tool.store.failed",
                {"store": kind, "op": "get", "exc_type": type(exc).__name__},
            )
            if kind == "dedupe":
                raise ToolInfraError(
                    "call deduplication is unavailable", code="dedupe_unavailable"
                ) from None
            return None

    async def _store_set(
        self, store: ResultStore, key: str, value: bytes, ttl_s: float, *, kind: str
    ) -> bool:
        """限时写入结果存储；失败只记录日志（此时工具已经执行完成）。

        Args:
            store: 存储。
            key: 键。
            value: 序列化后的值。
            ttl_s: 有效期（秒）。
            kind: ``cache`` 或 ``dedupe``（用于日志）。

        Returns:
            写入成功时为 True。

        Raises:
            asyncio.CancelledError: 被取消。
        """
        try:
            async with asyncio.timeout(self._config.store_timeout_s):
                await store.set(key, value, ttl_s)
        except Exception as exc:
            self._instr.log_safe(
                "error",
                "tool.store.failed",
                {"store": kind, "op": "set", "exc_type": type(exc).__name__},
            )
            return False
        return True

    async def _await_flight(
        self, waiter: asyncio.Future[FlightOutcome], ctx: ToolContext
    ) -> FlightOutcome:
        """等待同键的执行者结束；等待时间计入整体截止时间。

        使用 ``asyncio.shield``：等待者被取消（或超时）不会影响执行者。

        Args:
            waiter: 执行者的完成信号。
            ctx: 调用上下文。

        Returns:
            执行者结束时的状态。

        Raises:
            ToolTimeoutError: 截止时间已到（``deadline_exceeded``）。
            asyncio.CancelledError: 等待者被取消。
        """
        remaining = ctx.remaining(self._clock.monotonic())
        timeout_cm = asyncio.timeout(remaining)
        try:
            async with timeout_cm:
                return await asyncio.shield(waiter)
        except TimeoutError:
            if not timeout_cm.expired():
                raise
            raise ToolTimeoutError(
                "the request deadline was exceeded while waiting for an identical call",
                code="deadline_exceeded",
                retryable=False,
            ) from None

    async def _attempt_loop(
        self,
        state: _CallState,
        ctx: ToolContext,
        route: Route,
        args: dict[str, Any],
        obs: CallObservation,
        lease: ExecutionLease,
        secrets: CallSecrets,
    ) -> ToolOutput:
        """执行一次或多次尝试，直到成功、错误不可重试、达到尝试上限或时间不够。

        * 每次尝试先过熔断准入（P3，配置了熔断器时）：熔断开启时立即失败，不消耗限流令牌、
          不调用下游，也不会重试；
        * 再取速率限制令牌（等待计入截止时间；本地限流错误不会触发重试）；
        * 每次尝试结束后把结果记入熔断器：按熔断器的分类计入失败，其余失败不计入；成功计入
          成功；调用方取消时释放许可、不计入；
        * 重试条件见 :func:`agent_tools.governance.retry.should_retry`；
        * 退避等待之后距离截止时间至少还要剩 ``MIN_ATTEMPT_S`` 秒，否则放弃重试；
        * 退避等待期间被取消时，``CancelledError`` 原样传播；并发额度在整个循环期间持有，
          由租约在调用结束（以及所有孤儿线程结束）后统一归还；
        * 同一调用的所有尝试使用同一个幂等键和同一个密钥集合。

        Args:
            state: 调用状态（更新尝试次数）。
            ctx: 调用上下文。
            route: 路由结果。
            args: 已校验的参数。
            obs: 本次调用的观察者。
            lease: 本次调用的执行租约。
            secrets: 本次调用的已知密钥值集合。

        Returns:
            适配器返回的输出。

        Raises:
            ToolError: 最后一次尝试的错误（或熔断、限流、截止时间错误）。
            asyncio.CancelledError: 被取消。
        """
        spec = route.binding.spec
        idempotency_key = _idempotency_key(ctx, state.invocation, spec)
        breaker = self._circuit
        breaker_key = circuit_key(route.binding, breaker.scope) if breaker is not None else ""
        attempt = 0
        while True:
            attempt += 1
            state.attempts = attempt
            permit: CircuitPermit | None = None
            try:
                if breaker is not None:
                    try:
                        permit = await breaker.acquire(breaker_key)
                    except CircuitOpenError as exc:
                        obs.event("circuit_rejected", {"code": exc.code})
                        raise
                rate_limit = spec.policy.rate_limit
                if rate_limit is not None:
                    remaining = ctx.remaining(self._clock.monotonic())
                    await self._rate_limiter.acquire(
                        f"{spec.name}|{ctx.principal.tenant_id or '-'}",
                        rate_limit,
                        max_wait_s=float("inf") if remaining is None else remaining,
                    )
                output = await self._attempt(
                    state, route, args, ctx, lease, attempt, secrets, idempotency_key
                )
            except ToolError as exc:
                if permit is not None:
                    _record(permit, self._classify(exc))
                if not should_retry(exc, route.binding, attempt):
                    raise
                delay = backoff_delay(spec.policy.retry, attempt, exc.retry_after)
                remaining = ctx.remaining(self._clock.monotonic())
                if delay is None or (remaining is not None and remaining - delay < MIN_ATTEMPT_S):
                    raise
                obs.retrying(attempt=attempt + 1, error_type=exc.error_type, delay_s=delay)
                await self._sleep(delay)
            except BaseException:
                # 调用方取消、执行器关闭或内部错误：释放许可（尤其是半开状态的探测名额），不计入
                if permit is not None:
                    _record(permit, "ignored")
                raise
            else:
                if permit is not None:
                    _record(permit, "success")
                return output

    def _classify(self, error: ToolError) -> Outcome:
        """按熔断器的规则把尝试错误分类为“失败”或“不计入”；分类函数出错时不计入。

        Args:
            error: 尝试的错误。

        Returns:
            ``failure`` 或 ``ignored``。
        """
        breaker = self._circuit
        if breaker is None or isinstance(error, CircuitOpenError):
            return "ignored"
        try:
            return "failure" if breaker.is_failure(error) else "ignored"
        except Exception as exc:
            self._instr.log_safe(
                "error", "tool.circuit.classify_failed", {"exc_type": type(exc).__name__}
            )
            return "ignored"

    async def _attempt(
        self,
        state: _CallState,
        route: Route,
        args: dict[str, Any],
        ctx: ToolContext,
        lease: ExecutionLease,
        attempt: int,
        secrets: CallSecrets,
        idempotency_key: str,
    ) -> ToolOutput:
        """执行一次尝试：在 ``min(工具超时, 剩余时间)`` 内调用适配器，并映射超时。

        Args:
            state: 调用状态（适配器标记开始后记为“已执行”，供去重判断）。
            route: 路由结果。
            args: 已校验的参数。
            ctx: 调用上下文。
            lease: 执行租约。
            attempt: 尝试序号。
            secrets: 本次调用的已知密钥值集合。
            idempotency_key: 本次调用的幂等键。

        Returns:
            适配器返回的输出。

        Raises:
            ToolTimeoutError: 单次超时（``attempt_timeout``）或整体截止时间
                （``deadline_exceeded``）。
            ConcurrencyLimitedError: 超时前工具一直没能开始执行（``start_timeout``）。
            ToolError: 适配器映射出的其他错误。
            asyncio.CancelledError: 调用方取消。
        """
        spec = route.binding.spec
        window = attempt_window(ctx, spec.policy.timeout_s, self._clock.monotonic())
        scope = AttemptScope(
            attempt=attempt,
            deadline=window.deadline,
            lease=lease,
            secrets=secrets,
            idempotency_key=idempotency_key,
        )
        timeout_cm = asyncio.timeout(window.timeout_s)
        try:
            async with timeout_cm:
                return await route.adapter.invoke(route.binding, args, ctx, scope)
        except TimeoutError:
            if not timeout_cm.expired():
                raise  # 不是本次尝试的超时，交给上层按内部错误处理
            if not scope.started:
                # 超时前一直没拿到执行资源（例如线程全部被占用）：工具根本没有运行
                raise ConcurrencyLimitedError(
                    "the tool could not start before the timeout (no free capacity)",
                    code="start_timeout",
                ) from None
            if window.bounded_by_deadline:
                raise ToolTimeoutError(
                    "the request deadline was exceeded while the tool was running",
                    code="deadline_exceeded",
                    retryable=False,
                ) from None
            raise ToolTimeoutError(
                f"the tool did not finish within {spec.policy.timeout_s:g}s"
            ) from None
        finally:
            # 以适配器自己的“已开始”标记为准：等不到线程 / 子进程名额而超时的尝试没有运行工具
            if scope.started:
                state.invoked = True


def _clip(text: str | None, limit: int = 256) -> str | None:
    """截断来自审批处理器、要写入审计的文本；空字符串视为没有。

    Args:
        text: 原始文本。
        limit: 最多保留的字符数。

    Returns:
        截断后的文本或 ``None``。
    """
    return text[:limit] if text else None


def _record(permit: CircuitPermit, outcome: Outcome) -> None:
    """把尝试结果记入熔断许可；宿主实现的许可抛出异常时忽略（熔断不能影响调用结果）。

    Args:
        permit: 熔断许可。
        outcome: 结果分类。
    """
    with contextlib.suppress(Exception):
        permit.record(outcome)


def _validate_output(binding: ToolBinding, output: ToolOutput) -> None:
    """声明了输出 Schema 时，对结构化输出做完整校验。

    Args:
        binding: 工具绑定。
        output: 输出。

    Raises:
        OutputValidationError: 没有结构化输出，或结构化输出不符合 Schema。
    """
    if binding.output_validator is None:
        return
    if output.structured is None:
        raise OutputValidationError("tool output is not a JSON object")
    problems = binding.output_validator.errors(output.structured)
    if problems:
        raise OutputValidationError("tool output is invalid: " + "; ".join(problems))


def _approval_arguments(
    view: Literal["none", "redacted", "full"], raw_args: dict[str, Any]
) -> dict[str, Any] | None:
    """按审批策略生成交给审批处理器的参数视图。

    ``redacted``：参数名包含常见敏感词（password、token 等）的值整体替换为 ``[REDACTED]``；
    其余值序列化为 JSON 文本后做凭证格式与个人信息规则清除并截断。规则清除是尽力而为的，
    需要更严格控制时应使用 ``none``，并让审批界面只展示宿主自己生成的摘要。

    Args:
        view: 视图种类。
        raw_args: 模型给出的原始参数。

    Returns:
        参数视图；``none`` 时为 ``None``。
    """
    if view == "none":
        return None
    if view == "full":
        return dict(raw_args)
    redacted: dict[str, Any] = {}
    for name, value in raw_args.items():
        lowered = name.lower()
        if any(word in lowered for word in _SENSITIVE_ARG_NAMES):
            redacted[name] = "[REDACTED]"
            continue
        text = value if isinstance(value, str) else canonical_json(value)
        redacted[name] = preview(text, pii=True, max_chars=_APPROVAL_PREVIEW_CHARS)
    return redacted


def _idempotency_key(ctx: ToolContext, invocation: ToolInvocation, spec: ToolSpec) -> str:
    """计算本次调用的幂等键。

    作用域是“租户 + 请求 + 调用 + 工具 + 版本”：同一调用的所有重试使用同一个键；模型在不同
    轮次中可能重复使用相同的 ``call_id``，所以键里必须包含 ``request_id``。

    Args:
        ctx: 调用上下文。
        invocation: 调用请求。
        spec: 工具规格。

    Returns:
        64 位十六进制字符串。
    """
    raw = "|".join(
        (
            ctx.principal.tenant_id or "",
            ctx.request_id,
            invocation.call_id,
            spec.name,
            spec.version,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _scrub_output(output: ToolOutput, secrets: CallSecrets) -> ToolOutput:
    """把输出中出现的本次调用已知密钥值替换掉（例如下游把请求头回显到了响应里）。

    Args:
        output: 输出。
        secrets: 本次调用的已知密钥值集合。

    Returns:
        替换后的输出；没有已知密钥时原样返回。
    """
    if not secrets:
        return output
    blocks = []
    for block in output.content:
        if isinstance(block, TextBlock):
            block = TextBlock(text=secrets.scrub_text(block.text), truncated=block.truncated)
        elif isinstance(block, JsonBlock):
            block = JsonBlock(data=secrets.scrub_json(block.data))
        blocks.append(block)
    structured = secrets.scrub_json(output.structured) if output.structured is not None else None
    return ToolOutput(content=blocks, structured=structured)


def _output_bytes(output: ToolOutput) -> int:
    """估算输出的字节数，只用于日志（文本与 JSON 块合计，图片不计）。

    Args:
        output: 工具输出。

    Returns:
        字节数。
    """
    total = 0
    for block in output.content:
        if block.type == "text":
            total += len(block.text.encode("utf-8"))
        elif block.type == "json":
            total += json_size(block.data)
    return total
