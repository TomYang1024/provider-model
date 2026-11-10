"""测试用的 stdio MCP 服务端（作为受控子进程启动，不会被 pytest 收集）。

工具：
* ``pid``：返回进程号（用于验证进程复用、断开重连与关闭后的回收）；
* ``slow``：等待指定秒数；被取消时向 ``AGENT_TOOLS_MCP_MARKER`` 指定的文件写入 ``cancelled``
  （用于验证 SDK 确实向服务端发送了取消通知）；
* ``env``：返回某个环境变量的值（用于验证只有显式配置的环境变量被传给子进程）；
* ``secret_echo``：原样返回参数（用于验证结果中的已知密钥被清除）；
* ``big``：返回指定长度的文本（P3：验证隔离启动器的单条消息大小上限）；
* ``spin``：持续占用 CPU（P3：验证 CPU 时间限制）；
* ``pids``：返回 ``[进程号, 父进程号]``（P3：验证启动器与服务端在关闭后都被回收）。
"""

from __future__ import annotations

import asyncio
import os

from mcp.server.mcpserver import MCPServer

app = MCPServer("agent-tools-test")


@app.tool()
def pid() -> int:
    """返回服务端进程号。"""
    return os.getpid()


@app.tool()
async def slow(seconds: float) -> str:
    """等待指定秒数；被取消时记录到标记文件后重新抛出取消。"""
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        marker = os.environ.get("AGENT_TOOLS_MCP_MARKER")
        if marker:
            with open(marker, "a", encoding="utf-8") as f:
                f.write("cancelled\n")
        raise
    return "done"


@app.tool()
def env(name: str) -> str:
    """返回子进程中某个环境变量的值（不存在时返回 ``<unset>``）。"""
    return os.environ.get(name, "<unset>")


@app.tool()
def secret_echo(text: str) -> str:
    """原样返回参数。

    测试用：配合凭证清除测试，模拟服务端把敏感内容回显到结果里。
    """
    return text


@app.tool()
def big(n: int) -> str:
    """返回 n 个字符的文本。"""
    return "x" * n


@app.tool()
def spin() -> str:
    """持续占用 CPU，直到被资源限制终止。"""
    x = 0
    while True:
        x += 1


@app.tool()
def pids() -> list[int]:
    """返回服务端进程号与父进程号（经隔离启动器启动时，父进程就是启动器）。"""
    return [os.getpid(), os.getppid()]


if __name__ == "__main__":
    app.run("stdio")
