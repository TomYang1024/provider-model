"""参数的大小检查、解析与校验。

错误信息只包含字段路径和错误类型，**不回显参数值**，因为参数来自模型输出，可能包含
用户的敏感信息，而错误信息会被返回给模型。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from ..binding import PythonTarget, ToolBinding
from ..protocol.errors import InvalidArgumentsError, ToolInfraError

__all__ = ["argument_size", "check_argument_size", "parse_arguments", "validate_arguments"]

_MAX_REPORTED_ERRORS = 5


def argument_size(arguments: Mapping[str, Any] | str) -> int:
    """计算参数的 UTF-8 字节数。

    字符串参数直接计算；字典参数按紧凑 JSON 序列化后计算（无法序列化的值用 ``str`` 兜底，
    只用于计算大小）。

    Args:
        arguments: 原始参数。

    Returns:
        字节数。
    """
    if isinstance(arguments, str):
        return len(arguments.encode("utf-8"))
    text = json.dumps(arguments, ensure_ascii=False, separators=(",", ":"), default=str)
    return len(text.encode("utf-8"))


def check_argument_size(size: int, limit: int) -> None:
    """检查参数大小是否超过上限。

    Args:
        size: 参数字节数（由 :func:`argument_size` 计算）。
        limit: 上限（字节）。

    Raises:
        InvalidArgumentsError: 超过上限。
    """
    if size > limit:
        raise InvalidArgumentsError(
            f"arguments are too large ({size} bytes, limit {limit})", code="arguments_too_large"
        )


def parse_arguments(arguments: Mapping[str, Any] | str) -> dict[str, Any]:
    """把原始参数解析为 JSON 对象。空字符串视为 ``{}``（与 Provider SDK 的约定一致）。

    Args:
        arguments: 原始参数（字典或 JSON 字符串）。

    Returns:
        参数字典（浅拷贝）。

    Raises:
        InvalidArgumentsError: 不是合法 JSON，或 JSON 的顶层不是对象。
    """
    if not isinstance(arguments, str):
        return dict(arguments)
    if not arguments.strip():
        return {}
    try:
        value = json.loads(arguments)
    except json.JSONDecodeError:
        raise InvalidArgumentsError(
            "arguments are not valid JSON", code="arguments_not_json"
        ) from None
    if not isinstance(value, dict):
        raise InvalidArgumentsError("arguments must be a JSON object", code="arguments_not_object")
    return value


def validate_arguments(binding: ToolBinding, arguments: dict[str, Any]) -> dict[str, Any]:
    """按工具的参数定义校验参数。

    * Python 工具：使用 Pydantic 参数模型校验（``extra="forbid"``，未声明的参数会被拒绝）；
      返回的值保留模型解析后的类型（例如嵌套模型对象），可以直接作为关键字参数传给函数。
    * 其他工具（HTTP）：使用注册时编译好的 JSON Schema（Draft 2020-12）校验，原样返回参数字典。

    两种方式的错误信息都只包含字段路径和校验关键字，不包含参数值。

    Args:
        binding: 工具绑定。
        arguments: 已解析的参数字典。

    Returns:
        字段名 → 校验后的值。

    Raises:
        InvalidArgumentsError: 参数没有通过校验。
        ToolInfraError: 工具没有可用的参数校验器（属于注册错误，正常情况下不会发生）。
    """
    target = binding.target
    if isinstance(target, PythonTarget):
        try:
            model = target.args_model.model_validate(arguments)
        except ValidationError as exc:
            raise InvalidArgumentsError(_summarize(exc), code="arguments_invalid") from None
        return {name: getattr(model, name) for name in type(model).model_fields}
    if binding.input_validator is None:
        raise ToolInfraError("no argument validator for this tool", code="validator_unavailable")
    problems = binding.input_validator.errors(arguments)
    if problems:
        raise InvalidArgumentsError(
            "invalid arguments: " + "; ".join(problems), code="arguments_invalid"
        )
    return dict(arguments)


def _summarize(exc: ValidationError) -> str:
    """把 Pydantic 校验错误压缩成不含输入值的摘要。

    Args:
        exc: 校验错误。

    Returns:
        形如 ``invalid arguments: city: missing; days: int_parsing`` 的文本，最多列出 5 项。
    """
    items = []
    for err in exc.errors(include_input=False, include_url=False)[:_MAX_REPORTED_ERRORS]:
        loc = ".".join(str(part) for part in err["loc"]) or "<root>"
        items.append(f"{loc}: {err['type']}")
    more = exc.error_count() - len(items)
    suffix = f" (+{more} more)" if more > 0 else ""
    return "invalid arguments: " + "; ".join(items) + suffix
