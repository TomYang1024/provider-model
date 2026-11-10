"""输出大小限制。

不同类型的处理方式不同：
* 文本：在 UTF-8 字符边界处截断，追加截断标记，并把 ``TextBlock.truncated`` 置为 True；
* JSON（包括 ``structured``）、图片：**不截断**，超限时整体报错，因为截断会产生无效数据；
* 文本与 JSON 合计超过 ``max_total_bytes`` 时报错（在文本截断之后计算）。

URL 形式的图片不会被下载，因此只检查数量，不检查大小。
"""

from __future__ import annotations

import json
from typing import Any

from ..protocol.errors import OutputLimitError
from ..protocol.invocation import ContentBlock, ImageBlock, JsonBlock, TextBlock, ToolOutput
from ..protocol.spec import OutputLimits

__all__ = ["enforce_output_limits", "json_size", "truncate_utf8"]

# 为截断标记预留的字节数，保证截断后的文本（含标记）不超过上限
_MARKER_RESERVE = 48


def json_size(value: Any) -> int:
    """计算值按紧凑 JSON 序列化后的 UTF-8 字节数。

    Args:
        value: 可 JSON 序列化的值。

    Returns:
        字节数。
    """
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return len(text.encode("utf-8"))


def truncate_utf8(text: str, max_bytes: int) -> tuple[str, bool]:
    """把文本截断到不超过 ``max_bytes`` 个 UTF-8 字节，并且不会切断多字节字符。

    Args:
        text: 原始文本。
        max_bytes: 上限（字节）。

    Returns:
        ``(结果文本, 是否发生了截断)``。截断时结果末尾带有 ``…[truncated N bytes]`` 标记。
    """
    data = text.encode("utf-8")
    if len(data) <= max_bytes:
        return text, False
    budget = max(max_bytes - _MARKER_RESERVE, 0)
    # errors="ignore" 会丢掉被切断的半个多字节字符，保证结果是合法的 UTF-8 文本
    head = data[:budget].decode("utf-8", errors="ignore")
    dropped = len(data) - len(head.encode("utf-8"))
    return f"{head}…[truncated {dropped} bytes]", True


def _image_size(block: ImageBlock) -> int:
    """估算 Base64 图片解码后的字节数（URL 图片返回 0，因为不会被下载）。

    Args:
        block: 图片块。

    Returns:
        解码后的字节数。
    """
    if block.data_b64 is None:
        return 0
    data = block.data_b64.strip()
    padding = len(data) - len(data.rstrip("="))
    return max(len(data) * 3 // 4 - padding, 0)


def enforce_output_limits(output: ToolOutput, limits: OutputLimits) -> ToolOutput:
    """按类型对输出应用大小限制。

    Args:
        output: 适配器返回的输出。
        limits: 工具的输出限制。

    Returns:
        可能截断了文本块的新输出（其余内容保持不变）。

    Raises:
        OutputLimitError: JSON 块、``structured``、图片超限，图片数量超限，或合计超限。
    """
    blocks: list[ContentBlock] = []
    total = 0
    images = 0
    for block in output.content:
        if isinstance(block, TextBlock):
            text, truncated = truncate_utf8(block.text, limits.max_text_bytes)
            if truncated:
                block = TextBlock(text=text, truncated=True)
            total += len(block.text.encode("utf-8"))
        elif isinstance(block, JsonBlock):
            size = json_size(block.data)
            if size > limits.max_json_bytes:
                raise OutputLimitError(
                    f"JSON output is too large ({size} bytes, limit {limits.max_json_bytes})"
                )
            total += size
        else:
            images += 1
            if images > limits.max_images:
                raise OutputLimitError(
                    f"too many images (limit {limits.max_images})", code="too_many_images"
                )
            size = _image_size(block)
            if size > limits.max_image_bytes:
                raise OutputLimitError(
                    f"image is too large ({size} bytes, limit {limits.max_image_bytes})"
                )
        blocks.append(block)

    if output.structured is not None:
        size = json_size(output.structured)
        if size > limits.max_json_bytes:
            raise OutputLimitError(
                f"structured output is too large ({size} bytes, limit {limits.max_json_bytes})"
            )
    if total > limits.max_total_bytes:
        raise OutputLimitError(
            f"total output is too large ({total} bytes, limit {limits.max_total_bytes})"
        )
    return ToolOutput(content=blocks, structured=output.structured)
