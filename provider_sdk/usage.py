"""启发式 Usage 估算（需显式开启，``usage.source == "estimated"``）。

结果不可用于计费：中日韩字符约每个 1 Token，其他文本约每 4 个字符 1 Token。
仅在 Provider 未返回 Usage 且开启 ``ObservabilityConfig.estimate_usage`` 时使用。
"""

from __future__ import annotations

import math
import re

from .schemas.context import TokenUsage
from .schemas.message import Message

# 中日韩统一表意文字、假名、韩文音节等字符范围
_CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af\uf900-\ufaff]")
# 每条消息的格式开销（角色标记等）估算值
_PER_MESSAGE_OVERHEAD = 4


def estimate_text_tokens(text: str) -> int:
    if not text:
        return 0
    cjk = len(_CJK.findall(text))
    other = len(text) - cjk
    return cjk + math.ceil(other / 4)


def estimate_usage(messages: list[Message], output: Message | None) -> TokenUsage:
    input_tokens = sum(estimate_text_tokens(m.text) + _PER_MESSAGE_OVERHEAD for m in messages)
    output_tokens = 0
    if output is not None:
        output_tokens = estimate_text_tokens(output.text) + estimate_text_tokens(
            output.reasoning_content or ""
        )
        for tc in output.tool_calls or []:
            output_tokens += estimate_text_tokens(tc.name) + estimate_text_tokens(tc.arguments)
    return TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=input_tokens + output_tokens,
        source="estimated",
    )
