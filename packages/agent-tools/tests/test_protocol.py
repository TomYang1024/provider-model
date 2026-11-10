"""数据契约与错误模型的测试。"""

from __future__ import annotations

from typing import Any

import pydantic
import pytest

from agent_tools import (
    ImageBlock,
    JsonBlock,
    TextBlock,
    ToolContext,
    ToolExecutionResult,
    ToolInvocation,
    ToolPolicy,
    ToolSpec,
)
from agent_tools.protocol import errors


def test_policy_requires_explicit_access() -> None:
    """``ToolPolicy.access`` 没有默认值：不声明访问方式就无法构造策略。"""
    with pytest.raises(pydantic.ValidationError):
        ToolPolicy()  # type: ignore[call-arg]


def test_scoped_policy_requires_scopes_and_public_rejects_them() -> None:
    """``scoped`` 必须声明权限；``public`` 声明权限会被拒绝，避免误以为受保护。"""
    with pytest.raises(pydantic.ValidationError):
        ToolPolicy(access="scoped")
    with pytest.raises(pydantic.ValidationError):
        ToolPolicy(access="public", required_scopes=frozenset({"x"}))
    assert ToolPolicy(access="scoped", required_scopes=frozenset({"x"})).required_scopes == {"x"}


def test_spec_validates_name_alias_and_schema() -> None:
    """规格校验内部名格式、别名格式和顶层对象 Schema，并且禁止未知字段。"""
    policy = ToolPolicy(access="public")
    ok = ToolSpec(
        name="crm.get_customer", input_schema={"type": "object"}, kind="python", policy=policy
    )
    assert ok.version == "1"
    for bad in (
        {"name": "Bad.Name"},
        {"alias": "has.dot"},
        {"input_schema": {"type": "array"}},
        {"unknown": 1},
    ):
        data: dict[str, Any] = {
            "name": "ok",
            "input_schema": {"type": "object"},
            "kind": "python",
            "policy": policy,
        }
        data.update(bad)
        with pytest.raises(pydantic.ValidationError):
            ToolSpec.model_validate(data)


def test_context_request_id_and_deadline() -> None:
    """请求 ID 只允许安全字符；``with_timeout`` 把剩余秒数换算成绝对截止时间。"""
    with pytest.raises(pydantic.ValidationError):
        ToolContext(request_id="bad id\n")
    ctx = ToolContext.with_timeout(5, now=100.0)
    assert ctx.deadline == 105.0
    assert ctx.remaining(103.0) == 2.0
    assert ToolContext().remaining(1.0) is None


def test_image_block_requires_exactly_one_source() -> None:
    """图片块的 Base64 数据与 URL 必须恰好提供一个。"""
    with pytest.raises(pydantic.ValidationError):
        ImageBlock(mime_type="image/png")
    with pytest.raises(pydantic.ValidationError):
        ImageBlock(mime_type="image/png", data_b64="AA==", url="https://x/y.png")
    assert ImageBlock(mime_type="image/png", url="https://x/y.png").url


def test_result_round_trip_and_model_text() -> None:
    """结果可以序列化往返；``to_model_text`` 拼接文本与紧凑 JSON，图片用占位符。"""
    result = ToolExecutionResult(
        call_id="c1",
        tool="t",
        alias="t",
        version="1",
        status="success",
        content=[
            TextBlock(text="hi"),
            JsonBlock(data={"a": [1, "二"]}),
            ImageBlock(mime_type="image/png", url="https://x/y.png"),
        ],
        attempts=1,
    )
    assert ToolExecutionResult.model_validate_json(result.model_dump_json()) == result
    assert result.to_model_text() == 'hi\n{"a":[1,"二"]}\n[image]'
    assert result.trust == "untrusted"


def test_error_result_text_only_contains_safe_fields() -> None:
    """失败结果回传给模型的文本只包含错误码和安全摘要。"""
    info = errors.ToolTimeoutError("took too long").to_info()
    result = ToolExecutionResult(call_id="c", tool="t", alias="t", status="error", error=info)
    assert result.to_model_text() == "[attempt_timeout] took too long"
    assert info.retryable is True


@pytest.mark.parametrize(
    ("cls", "error_type", "code", "retryable"),
    [
        (errors.ToolNotFoundError, "not_found", "tool_not_found", False),
        (errors.ToolDisabledError, "not_found", "tool_disabled", False),
        (errors.ToolVersionNotFoundError, "not_found", "tool_version_not_found", False),
        (errors.InvalidArgumentsError, "invalid_arguments", "invalid_arguments", False),
        (errors.PermissionDeniedError, "permission", "permission_denied", False),
        (errors.ConcurrencyLimitedError, "throttled", "concurrency_limited", True),
        (errors.ToolTimeoutError, "timeout", "attempt_timeout", True),
        (errors.ToolCancelledError, "cancelled", "cancelled", False),
        (errors.ToolExecutionError, "execution", "tool_execution_failed", False),
        (errors.OutputLimitError, "output", "output_too_large", False),
        (errors.ToolInfraError, "internal", "internal_error", False),
    ],
)
def test_error_catalog_is_stable(
    cls: type[errors.ToolError], error_type: str, code: str, retryable: bool
) -> None:
    """错误类型、默认错误码和默认可重试标记是稳定的公开契约。"""
    err = cls("m")
    assert (err.error_type, err.code, err.retryable) == (error_type, code, retryable)


def test_invocation_defaults() -> None:
    """调用请求的参数默认为空字典，且禁止未知字段。"""
    assert ToolInvocation(call_id="c", tool="t").arguments == {}
    with pytest.raises(pydantic.ValidationError):
        ToolInvocation(call_id="c", tool="t", extra=1)  # type: ignore[call-arg]
