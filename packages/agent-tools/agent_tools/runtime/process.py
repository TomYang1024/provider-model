"""进程隔离 Python 工具的子进程运行器（P3）。

每次调用启动一个新的子进程（``python -I -m agent_tools.runtime.subprocess_child``，可选地放在
外部沙箱命令前缀之后），协议见 :mod:`agent_tools.runtime.subprocess_child`。隔离边界与平台
限制见 :mod:`agent_tools.runtime.isolation`。

进程生命周期：

* 子进程以 ``start_new_session=True`` 启动，自成一个进程组；
* 正常结束、工具失败、异常退出、输出超限、超时（执行器的 ``asyncio.timeout`` 取消本协程）、
  调用方取消、执行器关闭——所有路径都会在返回前终止**整个进程组**：先 SIGTERM，等待
  ``kill_grace_s`` 后 SIGKILL，并等待进程退出（最多再等 2 秒）；
* 进程组之外的后代（调用 ``setsid`` 脱离的进程）无法被回收；
* 同时运行的子进程数受 ``max_processes`` 限制，等待名额的时间计入单次尝试超时
  （执行器据 ``on_started`` 把这种超时报告为 ``start_timeout``）。

与线程不同，超时后子进程会被真正终止，因此不会产生“孤儿线程”，也不会推迟额度的归还。
终止不能撤回已经发生的副作用。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import sys
import tempfile
from collections.abc import Callable, Mapping
from typing import Any

from ..protocol import errors as tool_errors
from ..protocol.errors import (
    OutputLimitError,
    ToolError,
    ToolExecutionError,
    ToolInfraError,
)
from .isolation import ProcessIsolation

__all__ = ["CHILD_MODULE", "ProcessRunner"]

#: 子进程入口模块。
CHILD_MODULE = "agent_tools.runtime.subprocess_child"
_READ_CHUNK = 64 * 1024
_REAP_TIMEOUT_S = 2.0


def _tool_error(response: Mapping[str, Any]) -> ToolError:
    """把子进程传回的工具错误还原为对应的 :class:`ToolError` 子类（未知类型用执行错误）。

    Args:
        response: ``status="tool_error"`` 的响应。

    Returns:
        错误对象（只包含安全字段）。
    """
    cls = getattr(tool_errors, str(response.get("error_class", "")), None)
    if (
        not (isinstance(cls, type) and issubclass(cls, ToolError))
        or cls is tool_errors.UpstreamError
    ):
        cls = ToolExecutionError
    message = str(response.get("message", "tool failed"))[:1024]
    return cls(
        message,
        code=str(response.get("code", "tool_error"))[:64],
        retryable=bool(response.get("retryable", False)),
    )


class ProcessRunner:
    """在隔离子进程中运行 Python 工具，并负责子进程的全部清理。只在事件循环线程中使用。"""

    def __init__(self, max_processes: int) -> None:
        """创建运行器（不会启动任何进程）。

        Args:
            max_processes: 同时运行的子进程上限。

        Raises:
            ValueError: 上限不是正数。
        """
        if max_processes <= 0:
            raise ValueError("max_processes must be positive")
        self._slots = asyncio.Semaphore(max_processes)
        self._live: set[asyncio.subprocess.Process] = set()
        self._closed = False

    @property
    def live(self) -> int:
        """当前存活（尚未回收）的子进程数。"""
        return len(self._live)

    async def run(
        self,
        request: Mapping[str, Any],
        isolation: ProcessIsolation,
        *,
        on_started: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """启动子进程运行一次工具调用，返回成功响应。

        Args:
            request: 发给子进程的请求（模块、限定名、参数等，不含资源限制）。
            isolation: 隔离配置。
            on_started: 子进程已经启动、即将发送请求时调用（在事件循环线程中）。

        Returns:
            ``status="ok"`` 的响应（``kind`` 为 ``output`` 或 ``value``）。

        Raises:
            ToolError: 工具在子进程中抛出的 ``ToolError``（还原为对应子类）。
            ToolExecutionError: 工具抛出其他异常（``tool_raised``）、子进程异常退出
                （``subprocess_exited``）、被信号终止（``subprocess_killed``，例如 CPU 超限），
                或返回了无法解析的结果（``invalid_tool_output``）。
            OutputLimitError: 结果超过 ``max_output_bytes``（``subprocess_output_too_large``）。
            ToolInfraError: 运行器已关闭、子进程无法启动，或资源限制在本平台无法生效
                （``isolation_unavailable``）。
            asyncio.CancelledError: 超时或调用方取消（子进程会被终止后再传播）。
        """
        if self._closed:
            raise ToolInfraError("the process runner is closed", code="process_runner_closed")
        await self._slots.acquire()
        tmpdir: str | None = None
        proc: asyncio.subprocess.Process | None = None
        try:
            cwd = isolation.cwd
            if cwd is None:
                tmpdir = cwd = tempfile.mkdtemp(prefix="agent-tools-")
            payload = dict(request)
            payload["limits"] = isolation.limits.to_json()
            data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
            command = [*isolation.wrapper, sys.executable, "-I", "-m", CHILD_MODULE]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=dict(isolation.env),
                    cwd=cwd,
                    start_new_session=True,
                )
            except OSError:
                raise ToolInfraError(
                    "could not start the isolated tool process", code="process_start_failed"
                ) from None
            self._live.add(proc)
            if on_started is not None:
                on_started()
            assert proc.stdin is not None
            assert proc.stdout is not None
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                proc.stdin.write(data)
                await proc.stdin.drain()
            proc.stdin.close()
            raw = await self._read_capped(proc.stdout, isolation.max_output_bytes)
            returncode = await proc.wait()
            return self._parse(raw, returncode)
        finally:
            if proc is not None:
                await self._terminate(proc, isolation.kill_grace_s)
                self._live.discard(proc)
            self._slots.release()
            if tmpdir is not None:
                shutil.rmtree(tmpdir, ignore_errors=True)

    async def aclose(self) -> None:
        """关闭运行器：不再启动新进程，并终止所有残留的子进程（立即 SIGKILL 整个进程组）。"""
        self._closed = True
        for proc in list(self._live):
            _killpg(proc, signal.SIGKILL)
            with contextlib.suppress(Exception):
                async with asyncio.timeout(_REAP_TIMEOUT_S):
                    await proc.wait()
            self._live.discard(proc)

    # -- 内部方法 ------------------------------------------------------------------
    @staticmethod
    async def _read_capped(stream: asyncio.StreamReader, limit: int) -> bytes:
        """读取子进程 stdout 直到结束，超过上限时立即失败（调用方随后终止子进程）。

        Args:
            stream: 子进程的 stdout。
            limit: 最大字节数。

        Returns:
            读到的全部字节。

        Raises:
            OutputLimitError: 超过上限。
        """
        chunks: list[bytes] = []
        total = 0
        while chunk := await stream.read(_READ_CHUNK):
            total += len(chunk)
            if total > limit:
                raise OutputLimitError(
                    f"isolated tool output exceeded {limit} bytes",
                    code="subprocess_output_too_large",
                )
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _parse(raw: bytes, returncode: int) -> dict[str, Any]:
        """解析子进程的响应，把失败映射成统一错误。

        Args:
            raw: stdout 的全部内容。
            returncode: 退出码（负数表示被信号终止）。

        Returns:
            成功响应。

        Raises:
            ToolError: 见 :meth:`run`。
        """
        line = raw.strip()
        if not line:
            if returncode < 0:
                try:
                    name = signal.Signals(-returncode).name
                except ValueError:
                    name = str(-returncode)
                raise ToolExecutionError(
                    f"the isolated tool process was terminated by signal {name}",
                    code="subprocess_killed",
                )
            raise ToolExecutionError(
                f"the isolated tool process exited with status {returncode}",
                code="subprocess_exited",
            )
        try:
            response = json.loads(line)
        except ValueError:
            response = None
        if not isinstance(response, dict):
            raise ToolExecutionError(
                "the isolated tool process returned an invalid result", code="invalid_tool_output"
            )
        status = response.get("status")
        if status == "ok":
            return response
        if status == "tool_error":
            raise _tool_error(response)
        if status == "raised":
            exc_type = str(response.get("exc_type", "Exception"))[:64]
            raise ToolExecutionError(f"tool raised {exc_type}", code="tool_raised")
        if status == "isolation_error":
            raise ToolInfraError(
                "the configured isolation cannot be enforced on this platform",
                code="isolation_unavailable",
            )
        raise ToolExecutionError(
            "the isolated tool process returned an invalid result", code="invalid_tool_output"
        )

    @staticmethod
    async def _terminate(proc: asyncio.subprocess.Process, grace_s: float) -> None:
        """终止子进程所在的整个进程组并回收子进程。

        子进程已经退出时，仍向进程组发送 SIGKILL，清理它留下的后代进程。等待期间再次被
        取消时，立即 SIGKILL 后重新抛出取消。

        Args:
            proc: 子进程。
            grace_s: SIGTERM 之后等待的秒数。

        Raises:
            asyncio.CancelledError: 清理期间再次被取消（已发送 SIGKILL）。
        """
        if proc.returncode is None:
            _killpg(proc, signal.SIGTERM)
            try:
                async with asyncio.timeout(grace_s):
                    await proc.wait()
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                _killpg(proc, signal.SIGKILL)
                raise
        _killpg(proc, signal.SIGKILL)
        if proc.returncode is None:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(_REAP_TIMEOUT_S):
                    await proc.wait()


def _killpg(proc: asyncio.subprocess.Process, sig: signal.Signals) -> None:
    """向子进程的进程组发送信号；进程组已不存在时忽略。

    Args:
        proc: 子进程（以 ``start_new_session=True`` 启动，进程组 ID 等于其 PID）。
        sig: 信号。
    """
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, sig)
