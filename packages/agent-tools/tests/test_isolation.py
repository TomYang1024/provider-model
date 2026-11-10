"""进程隔离（P3）的测试：成功执行、环境变量与工作目录、异常退出、超时 / 取消 / 关闭时的进程
回收（含孙进程）、输出上限、资源限制、外部沙箱命令前缀，以及注册校验。

这些测试会真正启动子进程（每次调用一个 Python 解释器），因此超时设置得比纯单元测试宽松。
"""

from __future__ import annotations

import asyncio
import importlib
import os
import sys
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from agent_tools import (
    Principal,
    ProcessIsolation,
    RegistryError,
    ResourceLimits,
    ToolContext,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
)

TESTS_DIR = Path(__file__).resolve().parent
PUBLIC = ToolPolicy(access="public", timeout_s=10)


@pytest.fixture
def tools(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """让 ``isolated_tools`` 可以按模块名导入（父进程注册与子进程重新导入都需要）。"""
    monkeypatch.syspath_prepend(str(TESTS_DIR))
    return importlib.import_module("isolated_tools")


def alive(pid: int) -> bool:
    """判断进程是否仍然存在（僵尸进程视为已退出）。"""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        waited, _ = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return True  # 不是本进程的子进程，kill(0) 成功即视为存活
    return waited == 0


async def wait_dead(pid: int, timeout_s: float = 5.0) -> bool:
    """等待进程退出。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not alive(pid):
            return True
        await asyncio.sleep(0.05)
    return False


async def wait_file(path: Path, timeout_s: float = 10.0) -> str:
    """等待标记文件出现并返回其内容。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists() and path.read_text():
            return path.read_text()
        await asyncio.sleep(0.02)
    raise AssertionError(f"{path} was not written")


def register(
    registry: ToolRegistry, fn: Any, isolation: ProcessIsolation | None = None, **policy: Any
) -> None:
    """以函数名为工具名注册一个隔离工具。"""
    registry.register_function(
        fn,
        name=fn.__name__,
        policy=ToolPolicy(access="public", **({"timeout_s": 10} | policy)),
        isolation=isolation or ProcessIsolation(),
    )


async def run(executor: Any, tool: str, args: Any = None, **ctx: Any) -> Any:
    """执行一次调用。"""
    return await executor.execute(
        ToolInvocation(call_id="c", tool=tool, arguments=args or {}), ToolContext(**ctx)
    )


async def test_runs_in_separate_process(make_executor: Any, tools: ModuleType) -> None:
    """同步与异步工具在独立子进程中运行，结果按 Python 工具的规则归一化。"""
    registry = ToolRegistry()
    register(registry, tools.add)
    register(registry, tools.async_add)
    register(registry, tools.inspect_process)
    executor = make_executor(registry)
    assert (await run(executor, "add", {"a": 2, "b": 3})).to_model_text() == "5"
    assert (await run(executor, "async_add", {"a": 2, "b": 3})).to_model_text() == "5"
    info = await run(executor, "inspect_process", {"name": "HOME"})
    assert info.structured["pid"] != os.getpid()
    assert executor.process_runner.live == 0


async def test_environment_and_working_directory(
    make_executor: Any, tools: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """子进程只拿到显式配置的环境变量；默认工作目录是每次调用新建、结束后删除的临时目录。"""
    monkeypatch.setenv("AGENT_TOOLS_PARENT_SECRET", "do-not-leak")
    registry = ToolRegistry()
    register(registry, tools.inspect_process, ProcessIsolation(env={"ONLY": "this"}))
    executor = make_executor(registry)
    leaked = await run(executor, "inspect_process", {"name": "AGENT_TOOLS_PARENT_SECRET"})
    assert leaked.structured["env"] == "<unset>"
    only = await run(executor, "inspect_process", {"name": "ONLY"})
    assert only.structured["env"] == "this"
    cwd = Path(str(only.structured["cwd"]))
    assert cwd != Path.cwd() and cwd.name.startswith("agent-tools-")
    assert not cwd.exists()

    registry2 = ToolRegistry()
    register(registry2, tools.inspect_process, ProcessIsolation(cwd=str(tmp_path)))
    fixed = await run(make_executor(registry2), "inspect_process", {"name": "X"})
    assert Path(str(fixed.structured["cwd"])).resolve() == tmp_path.resolve()


async def test_abnormal_exit_and_errors(make_executor: Any, tools: ModuleType) -> None:
    """异常退出、工具错误、普通异常都映射成统一错误；普通异常的消息不会泄露。"""
    registry = ToolRegistry()
    for fn in (tools.crash, tools.raise_tool_error, tools.raise_plain, tools.noisy):
        register(registry, fn)
    executor = make_executor(registry)
    crashed = await run(executor, "crash")
    assert crashed.error is not None and crashed.error.code == "subprocess_exited"
    assert "status 3" in crashed.error.message
    custom = await run(executor, "raise_tool_error")
    assert custom.error is not None and (custom.error.code, custom.error.message) == (
        "quota_exhausted",
        "quota exhausted",
    )
    plain = await run(executor, "raise_plain", {"secret": "hunter2"})
    assert plain.error is not None and plain.error.code == "tool_raised"
    assert "hunter2" not in plain.to_model_text()
    assert (await run(executor, "noisy")).to_model_text() == "clean"
    assert executor.process_runner.live == 0


async def test_timeout_kills_process_and_grandchildren(
    make_executor: Any, tools: ModuleType, tmp_path: Path
) -> None:
    """超时后子进程及其同进程组的孙进程都被终止，运行器不再持有存活进程。"""
    registry = ToolRegistry()
    register(registry, tools.hang, ProcessIsolation(kill_grace_s=0.2), timeout_s=3)
    register(registry, tools.hang_with_grandchild, ProcessIsolation(kill_grace_s=0.2), timeout_s=3)
    executor = make_executor(registry)

    marker = tmp_path / "pid"
    result = await run(executor, "hang", {"marker": str(marker)})
    assert result.error is not None and result.error.code == "attempt_timeout"
    assert await wait_dead(int(marker.read_text()))

    grand = tmp_path / "grand"
    result = await run(executor, "hang_with_grandchild", {"marker": str(grand)})
    assert result.error is not None and result.error.code == "attempt_timeout"
    assert await wait_dead(int(grand.read_text()))
    assert executor.process_runner.live == 0


async def test_caller_cancel_and_executor_close_kill_process(
    make_executor: Any, tools: ModuleType, tmp_path: Path
) -> None:
    """调用方取消与执行器关闭都会终止子进程。"""
    registry = ToolRegistry()
    register(registry, tools.hang, ProcessIsolation(kill_grace_s=0.2))
    executor = make_executor(registry)

    marker = tmp_path / "a"
    task = asyncio.create_task(run(executor, "hang", {"marker": str(marker)}))
    pid = int(await wait_file(marker))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await wait_dead(pid)

    marker2 = tmp_path / "b"
    task = asyncio.create_task(run(executor, "hang", {"marker": str(marker2)}))
    pid = int(await wait_file(marker2))
    await executor.aclose(grace_s=0.05)
    result = await task
    assert result.error is not None and result.error.code == "executor_closed"
    assert await wait_dead(pid)
    assert executor.process_runner.live == 0


async def test_output_limit_kills_process(make_executor: Any, tools: ModuleType) -> None:
    """结果超过 ``max_output_bytes`` 时立即终止子进程并返回输出超限错误。"""
    registry = ToolRegistry()
    register(registry, tools.big_output, ProcessIsolation(max_output_bytes=10_000))
    executor = make_executor(registry)
    result = await run(executor, "big_output", {"n": 1_000_000})
    assert result.error is not None and result.error.code == "subprocess_output_too_large"
    assert (await run(executor, "big_output", {"n": 10})).ok


async def test_cpu_and_file_descriptor_limits(make_executor: Any, tools: ModuleType) -> None:
    """CPU 时间超限时子进程被内核终止（SIGXCPU / SIGKILL）；打开文件数受限。"""
    registry = ToolRegistry()
    register(registry, tools.burn_cpu, ProcessIsolation(limits=ResourceLimits(cpu_s=1)))
    register(registry, tools.open_many, ProcessIsolation(limits=ResourceLimits(open_files=64)))
    executor = make_executor(registry)
    burned = await run(executor, "burn_cpu")
    assert burned.error is not None and burned.error.code == "subprocess_killed"
    assert any(sig in burned.error.message for sig in ("SIGXCPU", "SIGKILL"))
    opened = await run(executor, "open_many", {"n": 500})
    assert opened.ok and int(opened.to_model_text()) < 64


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS 拒绝设置 RLIMIT_AS")
async def test_memory_limit_unavailable_on_macos_fails_closed(
    make_executor: Any, tools: ModuleType
) -> None:
    """macOS 上无法设置内存上限：拒绝运行（``isolation_unavailable``），而不是不受限制地执行。"""
    registry = ToolRegistry()
    register(registry, tools.add, ProcessIsolation(limits=ResourceLimits(memory_bytes=512 * 2**20)))
    result = await run(make_executor(registry), "add", {"a": 1, "b": 2})
    assert result.error is not None and result.error.code == "isolation_unavailable"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="RLIMIT_AS 只在 Linux 上生效")
async def test_memory_limit_enforced_on_linux(make_executor: Any, tools: ModuleType) -> None:
    """Linux 上内存上限生效：超额分配失败（``MemoryError`` → ``tool_raised``）。"""
    registry = ToolRegistry()
    register(
        registry, tools.allocate, ProcessIsolation(limits=ResourceLimits(memory_bytes=512 * 2**20))
    )
    result = await run(make_executor(registry), "allocate", {"mb": 2048})
    assert result.error is not None and result.error.code == "tool_raised"


async def test_wrapper_command_prefix(
    make_executor: Any, tools: ModuleType, tmp_path: Path
) -> None:
    """外部沙箱命令前缀会被用来启动子进程（这里用一个记录调用的 shell 脚本代替真实沙箱）。"""
    marker = tmp_path / "wrapped"
    wrapper = tmp_path / "wrapper.sh"
    wrapper.write_text(f'#!/bin/sh\necho wrapped > "{marker}"\nexec "$@"\n')
    wrapper.chmod(0o755)
    registry = ToolRegistry()
    register(registry, tools.add, ProcessIsolation(wrapper=(str(wrapper),)))
    result = await run(make_executor(registry), "add", {"a": 1, "b": 1})
    assert result.ok and marker.read_text().strip() == "wrapped"


async def test_waiting_for_a_process_slot_is_start_timeout(
    make_executor: Any, tools: ModuleType, tmp_path: Path
) -> None:
    """子进程名额用完时，等待名额超时报告为 ``start_timeout``（工具没有开始执行）。"""
    registry = ToolRegistry()
    isolation = ProcessIsolation(kill_grace_s=0.1)
    registry.register_function(
        tools.hang,
        name="hang_long",
        policy=ToolPolicy(access="public", timeout_s=30),
        isolation=isolation,
    )
    registry.register_function(
        tools.hang,
        name="hang_short",
        policy=ToolPolicy(access="public", timeout_s=0.5),
        isolation=isolation,
    )
    executor = make_executor(registry, max_subprocesses=1)
    holder = asyncio.create_task(run(executor, "hang_long", {"marker": str(tmp_path / "1")}))
    await wait_file(tmp_path / "1")
    second = await run(executor, "hang_short", {"marker": str(tmp_path / "2")})
    assert second.error is not None and second.error.code == "start_timeout"
    assert not (tmp_path / "2").exists()
    holder.cancel()
    with pytest.raises(asyncio.CancelledError):
        await holder
    assert executor.process_runner.live == 0


async def test_context_is_injected_without_deadline(make_executor: Any, tools: ModuleType) -> None:
    """注入子进程的上下文保留请求 ID 与身份，但清空截止时间（单调时钟跨进程没有意义）。"""
    registry = ToolRegistry()
    register(registry, tools.with_context)
    result = await run(
        make_executor(registry),
        "with_context",
        request_id="req-iso",
        principal=Principal(tenant_id="t9"),
        deadline=time.monotonic() + 30,
    )
    assert result.structured == {"request_id": "req-iso", "deadline": None, "tenant": "t9"}


def test_registration_rules(tools: ModuleType) -> None:
    """隔离工具必须能按模块名重新导入，不能声明协作式取消事件；配置项本身也会校验。"""

    def local_tool(x: int) -> int:
        """函数内部定义的工具。"""
        return x

    with pytest.raises(RegistryError, match="module level"):
        ToolRegistry().register_function(
            local_tool, name="local", policy=PUBLIC, isolation=ProcessIsolation()
        )

    def with_event(x: int, cancel: threading.Event) -> int:
        """声明了取消事件的工具。"""
        return x

    with_event.__qualname__ = "with_event"
    with pytest.raises(RegistryError, match="cancel event"):
        ToolRegistry().register_function(
            with_event, name="ev", policy=PUBLIC, isolation=ProcessIsolation()
        )
    with pytest.raises(ValueError, match="absolute"):
        ProcessIsolation(wrapper=("bwrap",))
    with pytest.raises(ValueError, match="absolute"):
        ProcessIsolation(cwd="relative/dir")
    with pytest.raises(ValueError):
        ResourceLimits(cpu_s=0)


async def test_in_process_tools_are_unaffected(make_executor: Any, tools: ModuleType) -> None:
    """没有配置隔离的 Python 工具仍在宿主进程内运行（P0 行为不变）。"""
    registry = ToolRegistry()
    registry.register_function(tools.inspect_process, name="inproc", policy=PUBLIC)
    result = await run(make_executor(registry), "inproc", {"name": "HOME"})
    assert result.structured["pid"] == os.getpid()
