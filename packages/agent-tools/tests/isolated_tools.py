"""进程隔离测试使用的工具函数（P3）。

这些函数会在隔离子进程中按 ``isolated_tools:<函数名>`` 重新导入，所以必须定义在模块顶层。
本模块名不以 ``test_`` 开头，不会被 pytest 收集。
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

from agent_tools import ToolContext, ToolExecutionError


def add(a: int, b: int) -> int:
    """返回两数之和。"""
    return a + b


async def async_add(a: int, b: int) -> int:
    """异步返回两数之和（子进程中用 ``asyncio.run`` 执行）。"""
    await asyncio.sleep(0)
    return a + b


def inspect_process(name: str) -> dict[str, object]:
    """返回子进程的 PID、某个环境变量、工作目录和环境变量数量。"""
    return {
        "pid": os.getpid(),
        "env": os.environ.get(name, "<unset>"),
        "cwd": os.getcwd(),
        "env_count": len(os.environ),
    }


def hang(marker: str) -> str:
    """把 PID 写入标记文件后一直等待（用于验证超时、取消和关闭时进程被终止）。"""
    Path(marker).write_text(str(os.getpid()))
    time.sleep(60)
    return "late"


def hang_with_grandchild(marker: str) -> str:
    """启动一个同进程组的孙进程，把它的 PID 写入标记文件后一直等待。"""
    child = subprocess.Popen(["/bin/sleep", "60"])
    Path(marker).write_text(str(child.pid))
    time.sleep(60)
    return "late"


def crash() -> str:
    """不写结果直接以退出码 3 退出。"""
    os._exit(3)


def big_output(n: int) -> str:
    """返回 n 个字符的文本。"""
    return "x" * n


def burn_cpu() -> str:
    """持续占用 CPU（用于验证 CPU 时间限制）。"""
    x = 0
    while True:
        x += 1


def open_many(n: int) -> int:
    """尝试同时打开 n 个文件，返回成功打开的数量。"""
    handles = []
    try:
        for _ in range(n):
            handles.append(open(os.devnull, "rb"))  # noqa: SIM115
    except OSError:
        pass
    count = len(handles)
    for handle in handles:
        handle.close()
    return count


def allocate(mb: int) -> int:
    """分配 mb 兆字节内存。"""
    data = bytearray(mb * 1024 * 1024)
    return len(data)


def raise_tool_error() -> str:
    """抛出带安全摘要的工具错误。"""
    raise ToolExecutionError("quota exhausted", code="quota_exhausted")


def raise_plain(secret: str) -> str:
    """抛出消息中包含敏感内容的普通异常。"""
    raise RuntimeError(f"failed with {secret}")


def noisy() -> str:
    """向 stdout 打印内容后返回（验证打印不会破坏结果协议）。"""
    print("noise that is not JSON")
    sys.stdout.flush()
    return "clean"


def with_context(ctx: ToolContext) -> dict[str, object]:
    """返回注入的上下文中的请求 ID、截止时间与租户。"""
    return {
        "request_id": ctx.request_id,
        "deadline": ctx.deadline,
        "tenant": ctx.principal.tenant_id,
    }
