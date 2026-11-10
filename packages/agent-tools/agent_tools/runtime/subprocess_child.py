"""进程隔离 Python 工具的子进程入口（P3）：``python -I -m agent_tools.runtime.subprocess_child``。

协议（由 :class:`agent_tools.runtime.process.ProcessRunner` 驱动）：

1. 从 stdin 读取**一个** JSON 请求：``module``、``qualname``、``kwargs``、可选的 ``context``
   （``ToolContext`` 的 JSON，截止时间已清空）与 ``context_param``、``sys_path``、``limits``；
2. 先把文件描述符 1 复制为私有的结果通道，再把 1 指向 2（stderr），这样工具代码的 ``print``
   不会破坏协议；
3. 在导入任何宿主代码**之前**设置资源限制；无法设置时返回 ``isolation_error`` 并退出；
4. 导入并调用工具函数（协程函数用 ``asyncio.run``）；
5. 在结果通道上写**一行** JSON 后退出：

   * ``{"status": "ok", "kind": "output", "output": {...}}``：工具返回了 ``ToolOutput``；
   * ``{"status": "ok", "kind": "value", "value": ...}``：其他返回值（Pydantic 模型已转换为 JSON）；
   * ``{"status": "tool_error", "error_class", "code", "message", "retryable"}``：工具抛出了
     ``ToolError``（只传回安全字段）；
   * ``{"status": "raised", "exc_type": ...}``：工具抛出了其他异常（只传回类型名）；
   * ``{"status": "isolation_error", "message": ...}``：资源限制无法生效。

本模块不会被 ``agent_tools`` 包自动导入。
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import os
import sys
from typing import Any


def _resolve(module_name: str, qualname: str) -> Any:
    """按 ``模块:限定名`` 找到工具函数。

    Args:
        module_name: 模块名。
        qualname: 限定名（可以是 ``类名.方法名`` 形式的静态函数）。

    Returns:
        函数对象。
    """
    obj: Any = importlib.import_module(module_name)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


def _run(request: dict[str, Any]) -> dict[str, Any]:
    """执行请求并生成响应（在已经设置好资源限制和输出重定向之后调用）。

    Args:
        request: 父进程发来的请求。

    Returns:
        响应对象。
    """
    from pydantic import BaseModel

    from agent_tools.protocol.context import ToolContext
    from agent_tools.protocol.errors import ToolError
    from agent_tools.protocol.invocation import ToolOutput

    for entry in reversed(request.get("sys_path", [])):
        if isinstance(entry, str) and entry not in sys.path:
            sys.path.insert(0, entry)
    try:
        fn = _resolve(request["module"], request["qualname"])
        kwargs = dict(request.get("kwargs", {}))
        if request.get("context_param"):
            kwargs[request["context_param"]] = ToolContext.model_validate_json(request["context"])
        value = fn(**kwargs)
        if inspect.isawaitable(value):
            value = asyncio.run(_await(value))
    except ToolError as exc:
        return {
            "status": "tool_error",
            "error_class": type(exc).__name__,
            "code": exc.code,
            "message": exc.safe_message,
            "retryable": exc.retryable,
        }
    except Exception as exc:
        # 只传回异常类型：异常消息可能包含参数、路径或密钥
        return {"status": "raised", "exc_type": type(exc).__name__}
    if isinstance(value, ToolOutput):
        return {"status": "ok", "kind": "output", "output": value.model_dump(mode="json")}
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return {"status": "ok", "kind": "value", "value": value}


async def _await(awaitable: Any) -> Any:
    """在新的事件循环中等待协程（``asyncio.run`` 需要协程对象）。

    Args:
        awaitable: 可等待对象。

    Returns:
        结果。
    """
    return await awaitable


def main() -> int:
    """子进程主函数。

    Returns:
        进程退出码：协议层面成功（包括工具失败）为 0；请求无法解析为 2。
    """
    # 私有结果通道：工具代码写到 stdout 的内容改为进入 stderr，不会混进协议
    result_fd = os.dup(1)
    os.dup2(2, 1)
    try:
        request = json.loads(sys.stdin.buffer.read())
    except ValueError:
        return 2
    from agent_tools.runtime.isolation import (
        IsolationUnavailableError,
        ResourceLimits,
        apply_limits,
    )

    try:
        apply_limits(ResourceLimits.from_json(request.get("limits", {})))
    except IsolationUnavailableError as exc:
        response: dict[str, Any] = {"status": "isolation_error", "message": str(exc)}
    else:
        response = _run(request)
    try:
        line = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        line = json.dumps({"status": "raised", "exc_type": "UnserializableResult"})
    data = (line + "\n").encode("utf-8")
    view = memoryview(data)
    while view:
        written = os.write(result_fd, view)
        view = view[written:]
    os.close(result_fd)
    return 0


if __name__ == "__main__":
    sys.exit(main())
