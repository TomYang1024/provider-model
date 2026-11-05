"""用于日志、Span 属性和错误信息的密钥 / 个人信息清除与截断。"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

# 凭证类：任何日志模式下都会清除
_SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+/=]{8,}"), "Bearer [REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}"), "[REDACTED_KEY]"),
    # 智谱 API Key 格式为 "<32 位十六进制>.<字母数字>"
    (re.compile(r"\b[0-9a-f]{32}\.[A-Za-z0-9]{10,}\b"), "[REDACTED_KEY]"),
    (
        re.compile(r"(?i)\b(api[_-]?key|access[_-]?token|secret|password)\b(\s*[:=]\s*)\S+"),
        r"\1\2[REDACTED]",
    ),
    (
        re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"),
        "[REDACTED_JWT]",
    ),
]

# 个人信息类：在 ``redacted`` 模式下清除
_PII_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"), "[EMAIL]"),
    (re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)"), "[ID_NUMBER]"),  # 身份证号
    (re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), "[PHONE]"),  # 手机号
    (re.compile(r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)"), "[CARD_NUMBER]"),
]

# 需要整体遮蔽的敏感字段名（不区分大小写）
SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "api_key",
        "apikey",
        "x-api-key",
        "password",
        "secret",
        "token",
        "access_token",
        "refresh_token",
        "cookie",
        "set-cookie",
    }
)


def scrub_secrets(text: str) -> str:
    for pattern, repl in _SECRET_PATTERNS:
        text = pattern.sub(repl, text)
    return text


def scrub_pii(text: str) -> str:
    for pattern, repl in _PII_PATTERNS:
        text = pattern.sub(repl, text)
    return text


def truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}...[truncated {len(text) - max_chars} chars]"


def sanitize_text(text: str, *, max_chars: int, pii: bool = True) -> str:
    """清除密钥（始终）、个人信息（可选），最后截断。"""
    text = scrub_secrets(text)
    if pii:
        text = scrub_pii(text)
    return truncate(text, max_chars)


def redact_mapping(data: Mapping[str, Any], extra_keys: Iterable[str] = ()) -> dict[str, Any]:
    """返回 ``data`` 的浅拷贝，其中敏感字段的值被遮蔽。"""
    keys = SENSITIVE_KEYS | {k.lower() for k in extra_keys}
    return {k: ("[REDACTED]" if k.lower() in keys else v) for k, v in data.items()}
