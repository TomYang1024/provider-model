"""工具适配器。P0 只提供本地 Python 函数适配器；HTTP、MCP 在后续阶段加入。"""

from .base import AttemptScope, ToolAdapter
from .python import PythonAdapter, normalize_return_value

__all__ = ["AttemptScope", "PythonAdapter", "ToolAdapter", "normalize_return_value"]
