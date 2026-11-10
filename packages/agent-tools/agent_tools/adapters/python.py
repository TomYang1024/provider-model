"""本地 Python 函数适配器（P0）。只用于宿主信任的代码：工具与宿主进程拥有相同的权限。"""

from __future__ import annotations

import sys
import threading
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError

from ..binding import PythonTarget, ToolBinding
from ..protocol.context import ToolContext
from ..protocol.errors import ToolError, ToolExecutionError, ToolInfraError
from ..protocol.invocation import JsonBlock, TextBlock, ToolOutput
from ..runtime.process import ProcessRunner
from ..runtime.threads import BoundedThreadRunner
from .base import AttemptScope

__all__ = ["PythonAdapter", "normalize_return_value"]

_JSON = TypeAdapter(JsonValue)


def normalize_return_value(value: Any) -> ToolOutput:
    """把工具函数的返回值归一化为 :class:`ToolOutput`。

    支持的返回值：``ToolOutput``（原样返回）、``str``（文本块）、Pydantic 模型（JSON 块，
    并填充 ``structured``）、可 JSON 序列化的 ``dict`` / ``list`` / 标量（JSON 块；``dict``
    同时填充 ``structured``）、``None``（空输出）。

    Args:
        value: 工具函数的返回值。

    Returns:
        归一化后的输出。

    Raises:
        ToolExecutionError: 返回值类型不受支持，或无法序列化为 JSON。
    """
    if isinstance(value, ToolOutput):
        return value
    if value is None:
        return ToolOutput()
    if isinstance(value, str):
        return ToolOutput(content=[TextBlock(text=value)])
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if isinstance(value, dict | list | int | float | bool):
        try:
            data = _JSON.validate_python(value)
        except ValidationError:
            raise ToolExecutionError(
                "tool returned a value that is not JSON serializable", code="invalid_tool_output"
            ) from None
        structured = data if isinstance(data, dict) else None
        return ToolOutput(content=[JsonBlock(data=data)], structured=structured)
    raise ToolExecutionError(
        f"tool returned unsupported type {type(value).__name__}", code="invalid_tool_output"
    )


class PythonAdapter:
    """执行 :class:`PythonTarget` 的适配器。

    * 异步函数直接在当前任务中 ``await``，取消在下一个 await 点生效；
    * 同步函数在专用的有上限线程池中运行，超时或取消后线程会继续运行到结束，
      其占用的额度通过执行租约推迟到线程结束后才归还（见 :mod:`agent_tools.runtime.threads`）；
    * 配置了进程隔离（P3）的函数在新的子进程中运行，超时或取消时子进程（整个进程组）被终止
      （见 :mod:`agent_tools.runtime.process`）。

    无论哪种方式，取消都不能撤回已经发生的副作用。
    """

    kind = "python"

    def __init__(
        self, runner: BoundedThreadRunner, *, processes: ProcessRunner | None = None
    ) -> None:
        """创建适配器。

        Args:
            runner: 运行同步工具的线程池。
            processes: 运行进程隔离工具的子进程运行器；``None`` 时隔离工具调用会失败
                （``isolation_unavailable``），不会退回到进程内运行。
        """
        self._runner = runner
        self._processes = processes

    async def invoke(
        self,
        binding: ToolBinding,
        args: Mapping[str, Any],
        ctx: ToolContext,
        attempt: AttemptScope,
    ) -> ToolOutput:
        """调用 Python 函数并归一化返回值。

        Args:
            binding: 工具绑定，其目标必须是 :class:`PythonTarget`。
            args: 已经通过参数模型校验的参数（字段名 → 值）。
            ctx: 调用上下文；函数声明了 :class:`ToolContext` 参数时会被注入。
            attempt: 本次尝试的执行范围。

        Returns:
            归一化后的输出。

        Raises:
            ToolError: 函数自己抛出的 :class:`ToolError` 原样传递（工具可借此给出安全的错误摘要）。
            ToolExecutionError: 函数抛出了其他异常，或返回值不受支持；原始异常保存在
                ``__cause__`` 中，不会出现在 ``safe_message`` 里。
            asyncio.CancelledError: 超时或调用方取消（原样传播）；同步函数在等待线程槽位时
                超时，执行器会根据 ``attempt.started`` 把它报告为并发受限。
        """
        target = binding.target
        if not isinstance(target, PythonTarget):
            raise ToolInfraError("binding target is not a Python function", code="adapter_mismatch")
        if target.isolation is not None:
            return await self._invoke_isolated(target, args, ctx, attempt)
        kwargs = dict(args)
        if target.context_param is not None:
            kwargs[target.context_param] = ctx
        try:
            if target.is_async:
                attempt.mark_started()
                value = await target.fn(**kwargs)
            else:
                cancel_event: threading.Event | None = None
                if target.cancel_event_param is not None:
                    cancel_event = threading.Event()
                    kwargs[target.cancel_event_param] = cancel_event
                value = await self._runner.run(
                    target.fn,
                    kwargs,
                    lease=attempt.lease,
                    cancel_event=cancel_event,
                    on_started=attempt.mark_started,
                )
        except ToolError:
            raise
        except Exception as exc:
            # 只暴露异常类型名：异常消息可能包含参数、路径或密钥，不能交给模型
            raise ToolExecutionError(
                f"tool raised {type(exc).__name__}", code="tool_raised"
            ) from exc
        return normalize_return_value(value)

    async def _invoke_isolated(
        self,
        target: PythonTarget,
        args: Mapping[str, Any],
        ctx: ToolContext,
        attempt: AttemptScope,
    ) -> ToolOutput:
        """在隔离子进程中调用函数（P3）。

        参数按 JSON 传给子进程（使用模型给出、已通过校验的 JSON 形式）；注入的
        :class:`ToolContext` 会清空 ``deadline``（单调时钟的绝对时刻跨进程没有意义，
        超时由父进程强制执行）。

        Args:
            target: 带隔离配置的执行目标。
            args: 已通过参数模型校验的参数。
            ctx: 调用上下文。
            attempt: 本次尝试的执行范围（子进程启动后标记开始）。

        Returns:
            归一化后的输出。

        Raises:
            ToolError: 子进程运行器映射出的错误（见 :meth:`ProcessRunner.run`）。
            ToolInfraError: 没有配置子进程运行器，或参数无法 JSON 序列化。
            asyncio.CancelledError: 超时或调用方取消（子进程已被终止）。
        """
        isolation = target.isolation
        assert isolation is not None
        if self._processes is None:
            raise ToolInfraError(
                "process isolation is not available in this adapter", code="isolation_unavailable"
            )
        try:
            kwargs = target.args_model.model_validate(dict(args)).model_dump(mode="json")
        except ValidationError:
            raise ToolInfraError("arguments cannot be passed to an isolated tool") from None
        module, qualname = target.reference
        request: dict[str, Any] = {
            "module": module,
            "qualname": qualname,
            "kwargs": kwargs,
            "sys_path": [p for p in sys.path if isinstance(p, str) and p],
        }
        if target.context_param is not None:
            request["context_param"] = target.context_param
            request["context"] = ctx.model_copy(update={"deadline": None}).model_dump_json()
        response = await self._processes.run(request, isolation, on_started=attempt.mark_started)
        if response.get("kind") == "output":
            try:
                return ToolOutput.model_validate(response.get("output"))
            except ValidationError:
                raise ToolExecutionError(
                    "isolated tool returned an invalid output", code="invalid_tool_output"
                ) from None
        return normalize_return_value(response.get("value"))

    async def aclose(self) -> None:
        """关闭线程池与子进程运行器。

        线程池不再接受新任务，正在运行的线程无法终止，会继续运行到结束；子进程运行器
        终止所有残留的隔离子进程。
        """
        self._runner.shutdown()
        if self._processes is not None:
            await self._processes.aclose()
