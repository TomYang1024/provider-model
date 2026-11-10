"""日志与 Span 内容的规则脱敏和截断（只用于 ``redacted`` / ``full`` 日志模式）。

规则复制自 ``provider_sdk.observability.redaction`` 的最小子集（核心包不能依赖 Provider SDK，
见架构文档 §1.4），属于有意的重复。规则脱敏是**尽力而为**：它只能识别常见格式的密钥和个人
信息，不能保证清除所有敏感内容。本次调用中已知的密钥值由
:class:`~agent_tools.security.secrets.CallSecrets` 另外做精确清除。
"""

from __future__ import annotations

import re

__all__ = ["preview", "scrub_pii", "scrub_secret_patterns", "truncate"]

# 凭证类格式
_SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+/=]{8,}"), "Bearer [REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}"), "[REDACTED_KEY]"),
    (
        re.compile(
            r"(?i)\b(api[_-]?key|access[_-]?token|secret|password|token)\b(\"?\s*[:=]\s*\"?)[^\s\",}]+"
        ),
        r"\1\2[REDACTED]",
    ),
    (
        re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"),
        "[REDACTED_JWT]",
    ),
]

# 个人信息类格式（``redacted`` 模式下清除）
_PII_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"), "[EMAIL]"),
    (re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)"), "[ID_NUMBER]"),
    (re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), "[PHONE]"),
    (re.compile(r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)"), "[CARD_NUMBER]"),
]


def scrub_secret_patterns(text: str) -> str:
    """按常见凭证格式清除文本中的密钥。

    Args:
        text: 原始文本。

    Returns:
        清除后的文本。
    """
    for pattern, repl in _SECRET_PATTERNS:
        text = pattern.sub(repl, text)
    return text


def scrub_pii(text: str) -> str:
    """按常见格式清除邮箱、身份证号、手机号、银行卡号。

    Args:
        text: 原始文本。

    Returns:
        清除后的文本。
    """
    for pattern, repl in _PII_PATTERNS:
        text = pattern.sub(repl, text)
    return text


def truncate(text: str, max_chars: int) -> str:
    """把文本截断到 ``max_chars`` 个字符，并注明截掉的长度。

    Args:
        text: 原始文本。
        max_chars: 最多保留的字符数。

    Returns:
        截断后的文本。
    """
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}...[truncated {len(text) - max_chars} chars]"


def preview(text: str, *, pii: bool, max_chars: int) -> str:
    """生成写入日志或 Span 的内容预览：先按凭证格式清除，可选清除个人信息，最后截断。

    调用方需要**先**用 ``CallSecrets.scrub_text`` 清除本次调用的已知密钥值。

    Args:
        text: 原始文本。
        pii: 是否同时清除个人信息。
        max_chars: 最多保留的字符数。

    Returns:
        预览文本。
    """
    text = scrub_secret_patterns(text)
    if pii:
        text = scrub_pii(text)
    return truncate(text, max_chars)
