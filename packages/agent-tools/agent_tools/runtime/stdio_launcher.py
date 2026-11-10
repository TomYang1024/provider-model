"""MCP stdio 服务端的隔离启动器（P3）：``python -I -m agent_tools.runtime.stdio_launcher``。

用法（由 :class:`agent_tools.adapters.mcp.McpServerManager` 自动组装，宿主不需要直接调用）::

    python -I -m agent_tools.runtime.stdio_launcher --config '<json>' -- <服务端命令> [参数...]

启动器做三件事：

1. 设置资源限制（服务端及其子进程继承）；无法设置时立即以退出码 70 退出（安全失败）；
2. 启动服务端：stdin 直接继承（客户端写入的请求不经过启动器），stderr 直接继承，stdout 经过
   启动器转发；服务端与启动器在同一个进程组中，SDK 关闭连接时终止整个进程组；
3. 按行转发 stdout（MCP stdio 每条 JSON-RPC 消息占一行）。单行超过 ``max_line_bytes`` 时
   丢弃整行：如果从行首（上限之内）能确认它是一个**响应**（顶层有 ``id``，并且已经出现
   ``result`` 或 ``error`` 键），就改为发送一个同 ``id`` 的 JSON-RPC 错误
   （``code = -32090``），客户端调用得到“响应过大”的错误、连接继续可用；否则（通知、服务端
   发起的请求、或无法从行首确认）直接丢弃并在 stderr 记录。启动器自身最多缓冲
   ``max_line_bytes`` 字节。

服务端退出后启动器以相同的退出码退出；客户端关闭 stdout 管道时启动器终止服务端后退出。
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import subprocess
import sys
from typing import Any

#: 响应过大时合成的 JSON-RPC 错误码（JSON-RPC 保留给实现自定义服务端错误的区间内）。
RESPONSE_TOO_LARGE_CODE = -32090
_READ_CHUNK = 64 * 1024
_EXIT_ISOLATION_FAILED = 70

__all__ = ["RESPONSE_TOO_LARGE_CODE", "LineRelay", "main", "top_level_fields"]


def _skip_string(text: str, start: int) -> int:
    """从 ``start``（一个双引号）开始跳过 JSON 字符串。

    Args:
        text: 文本。
        start: 起始双引号的位置。

    Returns:
        结束双引号的位置；字符串在文本内没有结束时返回 ``-1``。
    """
    i = start + 1
    escaped = False
    while i < len(text):
        ch = text[i]
        if escaped:
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == '"':
            return i
        i += 1
    return -1


def top_level_fields(prefix: str) -> tuple[Any, set[str]]:
    """扫描一条（可能被截断的）JSON 对象的开头，找出顶层的 ``id`` 值和已经出现的顶层键。

    只跟踪嵌套深度与字符串边界，不做完整解析；嵌套对象里的 ``id`` 不会被误认。被截断的
    ``id`` 值（数字或字符串没有在前缀内结束）视为没有找到。

    Args:
        prefix: 行首的文本。

    Returns:
        ``(id 值或 None, 顶层键集合)``。
    """
    depth = 0
    expecting_key = False
    current_key: str | None = None
    keys: set[str] = set()
    found: Any = None
    i = 0
    n = len(prefix)
    while i < n:
        ch = prefix[i]
        if ch == '"':
            end = _skip_string(prefix, i)
            if end < 0:
                break
            token = prefix[i : end + 1]
            if depth == 1 and expecting_key:
                with contextlib.suppress(ValueError):
                    current_key = json.loads(token)
                    keys.add(current_key)
                expecting_key = False
            elif depth == 1 and current_key == "id" and found is None:
                found = json.loads(token)
            i = end + 1
            continue
        if ch in "{[":
            depth += 1
            if ch == "{" and depth == 1:
                expecting_key = True
        elif ch in "}]":
            depth -= 1
        elif ch == "," and depth == 1:
            expecting_key = True
            current_key = None
        elif depth == 1 and current_key == "id" and found is None and (ch.isdigit() or ch == "-"):
            j = i
            while j < n and (prefix[j].isdigit() or prefix[j] in "-+.eE"):
                j += 1
            if j >= n:
                break  # 数字在前缀末尾被截断：无法确认完整的 id
            with contextlib.suppress(ValueError):
                found = json.loads(prefix[i:j])
            i = j
            continue
        i += 1
    return found, keys


class LineRelay:
    """按行转发字节流，并对单行长度设上限（可单独测试，不依赖进程）。"""

    def __init__(self, write: Any, max_line_bytes: int | None) -> None:
        """创建转发器。

        Args:
            write: 写出字节的函数（接收 ``bytes``）。
            max_line_bytes: 单行（不含换行符）的最大字节数；``None`` 表示不限制。
        """
        self._write = write
        self._max = max_line_bytes
        self._buf = bytearray()
        self._dropping = False
        #: 被丢弃的行数（用于测试与诊断）。
        self.dropped = 0

    def feed(self, chunk: bytes) -> None:
        """处理一块数据：完整的行原样写出；超长行在越界时立即处理并丢弃剩余部分。

        Args:
            chunk: 数据块。
        """
        start = 0
        while start <= len(chunk):
            newline = chunk.find(b"\n", start)
            end = len(chunk) if newline < 0 else newline
            piece = chunk[start:end]
            if not self._dropping:
                if self._max is not None and len(self._buf) + len(piece) > self._max:
                    self._buf += piece[: self._max - len(self._buf)]
                    self._overflow(bytes(self._buf))
                    self._buf.clear()
                    self._dropping = True
                else:
                    self._buf += piece
            if newline < 0:
                return
            if self._dropping:
                self._dropping = False
            else:
                self._write(bytes(self._buf) + b"\n")
                self._buf.clear()
            start = newline + 1

    def flush(self) -> None:
        """输入结束时写出最后一段没有换行符的数据（超长行已被丢弃时不写）。"""
        if self._buf and not self._dropping:
            self._write(bytes(self._buf))
        self._buf.clear()

    def _overflow(self, prefix: bytes) -> None:
        """处理一条超长消息：能确认是响应时合成错误响应，否则只记录。

        Args:
            prefix: 消息的前 ``max_line_bytes`` 个字节。
        """
        self.dropped += 1
        msg_id, keys = top_level_fields(prefix.decode("utf-8", errors="ignore"))
        if msg_id is not None and isinstance(msg_id, int | str) and keys & {"result", "error"}:
            error = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {
                    "code": RESPONSE_TOO_LARGE_CODE,
                    "message": f"agent-tools: MCP message exceeded {self._max} bytes",
                },
            }
            self._write(json.dumps(error, separators=(",", ":")).encode() + b"\n")
            return
        with contextlib.suppress(OSError):
            os.write(2, f"agent-tools: dropped an MCP message over {self._max} bytes\n".encode())


def _write_all(fd: int, data: bytes) -> None:
    """把数据完整写入文件描述符。

    Args:
        fd: 文件描述符。
        data: 数据。

    Raises:
        OSError: 写入失败（例如客户端已关闭管道）。
    """
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def main(argv: list[str] | None = None) -> int:
    """启动器主函数。

    Args:
        argv: 命令行参数（不含程序名）；默认 ``sys.argv[1:]``。

    Returns:
        退出码：服务端的退出码（被信号终止时为 128 + 信号值）；资源限制无法设置时为 70。
    """
    args = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="agent_tools.runtime.stdio_launcher")
    parser.add_argument("--config", required=True)
    if "--" not in args:
        parser.error("usage: --config JSON -- COMMAND [ARGS...]")
    separator = args.index("--")
    ns = parser.parse_args(args[:separator])
    command = args[separator + 1 :]
    if not command:
        parser.error("missing server command")
    config = json.loads(ns.config)

    from agent_tools.runtime.isolation import (
        IsolationUnavailableError,
        ResourceLimits,
        apply_limits,
    )

    try:
        apply_limits(ResourceLimits.from_json(config.get("limits", {})))
    except IsolationUnavailableError as exc:
        with contextlib.suppress(OSError):
            os.write(2, f"agent-tools: isolation unavailable: {exc}\n".encode())
        return _EXIT_ISOLATION_FAILED

    child = subprocess.Popen(command, stdin=0, stdout=subprocess.PIPE, stderr=2)  # noqa: S603
    assert child.stdout is not None
    relay = LineRelay(lambda data: _write_all(1, data), config.get("max_line_bytes"))
    try:
        fd = child.stdout.fileno()
        while chunk := os.read(fd, _READ_CHUNK):
            relay.feed(chunk)
        relay.flush()
    except OSError:
        # 客户端关闭了管道：不再有人读取服务端输出，终止服务端
        child.kill()
    returncode = child.wait()
    return 128 - returncode if returncode < 0 else returncode


if __name__ == "__main__":
    sys.exit(main())
