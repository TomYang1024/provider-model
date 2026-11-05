"""SDK 流的 Server-Sent Events 输出。

``SSEResponse`` 在流式输出期间始终监听 ``http.disconnect``。Starlette 只在 ASGI
spec < 2.4 时这样做，否则要等下一次写入失败才发现客户端已断开——在长时间推理阶段
这可能是几分钟之后。客户端断开时，响应任务被取消，进而取消上游 Provider 流，
并记录为 ``cancelled``。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Callable
from functools import partial
from typing import Any

import anyio
from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from provider_sdk import ChatStream, ProviderError
from provider_sdk.schemas import StreamEvent


class SSEResponse(StreamingResponse):
    """总是并发监听客户端断开的流式响应。"""

    media_type = "text/event-stream"

    def __init__(self, content: AsyncIterator[str], **kwargs: Any) -> None:
        headers = {
            "cache-control": "no-cache",
            "x-accel-buffering": "no",  # 关闭反向代理（nginx）缓冲，保证事件实时送达
            **kwargs.pop("headers", {}),
        }
        super().__init__(content, media_type=self.media_type, headers=headers, **kwargs)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # 两个任务并发：一个输出流，一个监听断开；任意一个结束就取消另一个
        async with anyio.create_task_group() as tg:

            async def run(func: Callable[[], Any]) -> None:
                await func()
                tg.cancel_scope.cancel()

            tg.start_soon(run, partial(self.stream_response, send))
            await run(partial(self.listen_for_disconnect, receive))
        if self.background is not None:
            await self.background()


def format_event(event: str, data: Any, event_id: int | None = None) -> str:
    """格式化一个 SSE 帧：可选的 id 行、event 行、单行 JSON 的 data 行。"""
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: {event}\ndata: {payload}\n\n"


async def sse_events(
    stream: ChatStream,
    *,
    ping_interval: float,
    on_done: Callable[[], None],
    request_id: str,
) -> AsyncIterator[str]:
    """把已启动的 :class:`ChatStream` 转换为 SSE 帧。

    终止帧：成功时为 ``message_end``，失败时为 ``error``。
    读取下一个事件放在独立任务中，这样等待期间可以按间隔发送 ping 保活，
    而不会因为超时取消正在进行的读取。
    """
    pending: asyncio.Task[StreamEvent] | None = None
    try:
        async with stream:
            try:
                while True:
                    if pending is None:
                        pending = asyncio.ensure_future(stream.__anext__())
                    # 在 ping 间隔内等待下一个事件；超时只发 ping，读取任务继续进行
                    done, _ = await asyncio.wait({pending}, timeout=ping_interval or None)
                    if not done:
                        yield ": ping\n\n"
                        continue
                    task, pending = pending, None
                    try:
                        event = task.result()
                    except StopAsyncIteration:
                        break
                    except ProviderError as exc:
                        # 流中途失败：已无法修改 HTTP 状态码，用 error 帧通知客户端
                        yield format_event(
                            "error", {"error": {**exc.to_dict(), "request_id": request_id}}
                        )
                        break
                    yield format_event(event.type, event.model_dump(mode="json"), event.seq)
            finally:
                # 先取消进行中的读取任务，再关闭流：保证上游终态被记录为 cancelled，
                # 避免读取任务与关闭操作竞争同一个 HTTP 流
                if pending is not None and not pending.done():
                    pending.cancel()
                    with contextlib.suppress(BaseException):
                        await asyncio.wait({pending}, timeout=1.0)
    finally:
        # 无论如何结束（完成、失败、客户端断开）都释放并发槽位
        on_done()
