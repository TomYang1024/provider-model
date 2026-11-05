"""``ChatStream``：可显式关闭的 :data:`StreamEvent` 异步流。

普通 ``async for`` 循环中途 ``break`` *不能*保证立即释放底层 HTTP 连接
（异步生成器的终结要等到垃圾回收）。因此 ``ChatStream`` 刻意不实现为生成器，
请用以下任一方式确定性地释放资源::

    async with sdk.open_stream(...) as stream:
        async for event in stream:
            ...

或者在 ``finally`` 中调用 ``await stream.aclose()``。作为最后一道防线，
``ProviderSDK.aclose()`` 会关闭所有仍处于打开状态的流。

状态机::

    new ──start()──► open ──message_end──► done
     │                │
     └──aclose()──►  closed ◄──失败 / 取消 / 超时 / 提前关闭
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Literal

from . import exceptions as errors
from .schemas.request import ChatRequest
from .schemas.response import ChatResponse
from .schemas.stream import MessageEndEvent, MessageStartEvent, StreamEvent

if TYPE_CHECKING:
    from types import TracebackType

    from .adapters.base import AdapterStream, BaseProviderAdapter
    from .client import ProviderSDK
    from .observability.middleware import CallObserver

State = Literal["new", "open", "done", "closed"]


class ChatStream:
    def __init__(self, sdk: ProviderSDK, request: ChatRequest) -> None:
        self._sdk = sdk
        self.request = request
        self._state: State = "new"
        self._adapter: BaseProviderAdapter | None = None
        self._inner: AdapterStream | None = None  # 适配器层的流（协议转换）
        self._observer: CallObserver | None = None  # 本次调用的 Trace / 指标 / 日志观察者
        self._deadline: float | None = None  # 整个流的截止时间（monotonic）
        self._seq = 0  # 事件序号，从 0 开始逐个递增
        self._start_sent = False  # message_start 是否已发出
        self._start_lock = asyncio.Lock()  # 防止并发调用 start() 重复建连
        #: 聚合后的完整响应，收到 ``message_end`` 后可用。
        self.response: ChatResponse | None = None
        #: 结束后的终态（``success``、``error``、``timeout``、``cancelled``、``closed_early``）。
        self.outcome: str | None = None

    @property
    def request_id(self) -> str:
        return self.request.context.request_id

    @property
    def closed(self) -> bool:
        return self._state in ("done", "closed")

    # -- 生命周期 -----------------------------------------------------------------------
    async def start(self) -> None:
        """连接 Provider。失败时在产生任何事件之前抛出异常（调用方可安全重试）。

        幂等；``async with`` 和第一次 ``__anext__`` 会自动调用。
        """
        async with self._start_lock:
            if self._state != "new":
                # 未启动就被关闭的流不能再使用
                if self._state == "closed" and self.outcome is None:
                    raise errors.StreamClosedError("stream was closed before it started")
                return
            sdk = self._sdk
            sdk._ensure_open()
            adapter = sdk.registry.get(self.request.provider)
            self._adapter = adapter
            # 截止时间从建连开始计算，覆盖建连 + 全部流式读取
            timeout = self.request.timeout or adapter.config.timeouts.stream_total
            self._deadline = time.monotonic() + timeout
            observer = sdk._begin(self.request, adapter)
            self._observer = observer
            # 登记到 SDK，以便 SDK 关闭时兜底关闭该流
            sdk._track(self)
            try:
                # 在请求 Span 上下文中建连，使 HTTP Span 成为其子 Span
                with observer.activate():
                    async with asyncio.timeout(self._remaining()):
                        self._inner = await adapter.open_stream(self.request)
            except BaseException as exc:
                self._state = "closed"
                sdk._untrack(self)
                out = self._fail(exc)
                if out is exc:
                    raise
                raise out from exc
            self._state = "open"
            observer.stream_connected()

    async def __aenter__(self) -> ChatStream:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # 因取消退出记为 cancelled；其他未读完就退出的情况记为 closed_early
        cancelled = exc_type is not None and issubclass(exc_type, asyncio.CancelledError)
        await self._close("cancelled" if cancelled else "closed_early")

    async def aclose(self) -> None:
        """释放连接。在 ``message_end`` 之前调用会把流记为 ``closed_early``
        （绝不记为成功）。幂等。"""
        await self._close("closed_early")

    # -- 迭代 ---------------------------------------------------------------------------
    def __aiter__(self) -> ChatStream:
        return self

    async def __anext__(self) -> StreamEvent:
        # 懒启动：允许直接 async for，而不必先调用 start()
        if self._state == "new":
            await self.start()
        if self._state in ("done", "closed"):
            raise StopAsyncIteration
        assert self._observer is not None
        assert self._inner is not None
        # 第一个事件固定是 message_start，此时 Provider 已接受请求
        if not self._start_sent:
            self._start_sent = True
            return self._number(
                MessageStartEvent(
                    request_id=self.request_id,
                    provider=self.request.provider,
                    model=self.request.model,
                    trace=self._observer.trace,
                )
            )
        observer = self._observer
        try:
            # 每次读取都在同一个 await 内挂载 / 卸载 Span 上下文，
            # 不会把上下文 token 泄漏到调用方的执行上下文中
            with observer.activate():
                # 每次读取都受剩余时间约束：截止时间是整个流的总时长，而非单次间隔
                async with asyncio.timeout(self._remaining()):
                    event = await self._inner.__anext__()
        except StopAsyncIteration:
            # 适配器未发出 message_end 就结束：上游协议异常
            err = errors.ProviderServerError(
                "stream ended without a terminal event",
                provider=self.request.provider,
                model=self.request.model,
                error_code="incomplete_stream",
            )
            raise self._fail(err) from None
        except BaseException as exc:
            out = self._fail(exc)
            if out is exc:
                raise
            raise out from exc

        # 记录首个内容事件（TTFT）等流式指标
        observer.stream_event(event)
        if isinstance(event, MessageEndEvent):
            # 唯一的成功终态：补充 latency / trace 后记录成功并释放连接
            response = self._sdk._finalize_response(event.response, observer, self.request)
            event = event.model_copy(update={"response": response})
            self.response = response
            self.outcome = "success"
            observer.finish("success", response=response, partial_output=False)
            self._state = "done"
            await self._release()
        return self._number(event)

    async def collect(self) -> ChatResponse:
        """消费完整个流并返回聚合后的响应。"""
        async with self:
            async for _ in self:
                pass
        if self.response is None:  # pragma: no cover - 协议保证不会发生
            raise errors.ProviderServerError("stream finished without a response")
        return self.response

    # -- 内部实现 -----------------------------------------------------------------------
    def _number(self, event: StreamEvent) -> StreamEvent:
        event.seq = self._seq
        self._seq += 1
        return event

    def _remaining(self) -> float:
        assert self._deadline is not None
        return max(0.0, self._deadline - time.monotonic())

    def _partial(self) -> bool:
        """是否已经向调用方输出过内容（决定失败后能否重试）。"""
        return bool(self._inner is not None and self._inner.emitted_output)

    def _fail(self, exc: BaseException) -> BaseException:
        """记录失败终态，并返回应当向调用方抛出的异常。"""
        observer = self._observer
        outcome: str
        out: BaseException
        if isinstance(exc, asyncio.CancelledError):
            # 取消必须原样向上传播，不能被吞掉或替换
            outcome, out = "cancelled", exc
        elif isinstance(exc, TimeoutError) and not isinstance(exc, errors.ProviderError):
            # asyncio.timeout 触发的内置 TimeoutError：转换为 SDK 的超时异常
            outcome = "timeout"
            out = errors.TimeoutError(
                "stream deadline exceeded",
                provider=self.request.provider,
                model=self.request.model,
                error_code="deadline_exceeded",
                # 可能已有部分内容交付给调用方：此时禁止透明重试
                retryable=not self._partial(),
            )
        elif isinstance(exc, errors.ProviderError):
            outcome = "timeout" if isinstance(exc, errors.TimeoutError) else "error"
            out = exc
            # 流式已输出后不允许透明重试
            if self._partial():
                exc.retryable = False
        elif self._adapter is not None and isinstance(exc, Exception):
            outcome, out = "error", self._adapter.map_error(exc, self.request)
        else:
            outcome, out = "error", exc
        self.outcome = outcome
        if observer is not None:
            observer.finish(
                outcome,  # type: ignore[arg-type]
                error=out,
                partial_output=self._partial(),
            )
        if self._state == "open":
            self._state = "closed"
            # 正在抛异常，不能在这里 await，改为后台任务释放连接
            self._schedule_release()
        return out

    async def _close(self, reason: Literal["closed_early", "cancelled"]) -> None:
        if self._state == "new":
            # 从未建连，无需释放资源
            self._state = "closed"
            return
        if self._state == "open":
            # 仍在进行中被关闭：记录非成功终态（done / 已失败的流不会重复记录）
            self._state = "closed"
            self.outcome = reason
            if self._observer is not None:
                self._observer.finish(reason, partial_output=self._partial())
        await self._release()

    async def _release(self) -> None:
        inner = self._inner
        self._sdk._untrack(self)
        if inner is None:
            return
        try:
            # 使用 shield：即使当前任务在关闭过程中再次被取消，
            # 关闭操作也会在后台继续完成，连接不会泄漏
            await asyncio.shield(inner.aclose())
        except asyncio.CancelledError:
            # 取消必须继续传播，否则任务会在被取消后继续运行
            raise
        except Exception:  # noqa: S110 - 关闭失败不能掩盖真实的终态
            pass

    def _schedule_release(self) -> None:
        """在失败路径上释放连接（此时正在抛异常，无法 await）。"""
        inner = self._inner
        self._sdk._untrack(self)
        if inner is None:
            return
        try:
            task = asyncio.get_running_loop().create_task(inner.aclose())
            # 交给 SDK 持有任务引用，避免被 GC 回收；SDK 关闭时会等待其完成
            self._sdk._background(task)
        except RuntimeError:  # pragma: no cover - 没有运行中的事件循环
            pass

    def __repr__(self) -> str:
        return (
            f"ChatStream(provider={self.request.provider!r}, model={self.request.model!r}, "
            f"request_id={self.request_id!r}, state={self._state!r})"
        )
