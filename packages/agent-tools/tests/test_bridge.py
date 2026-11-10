"""Provider SDK 桥接的测试：工具定义、调用、结果、错误与上下文的转换。"""

from __future__ import annotations

from collections.abc import Callable

from provider_sdk import ChatResponse, ImagePart, Message, RequestContext, TextPart, ToolCall

from agent_tools import (
    ImageBlock,
    JsonBlock,
    Principal,
    TextBlock,
    ToolContext,
    ToolErrorInfo,
    ToolExecutionResult,
    ToolExecutor,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.integrations.provider_sdk import (
    context_from_request,
    run_tool_calls,
    to_invocations,
    to_tool_definitions,
    to_tool_results,
)

PUBLIC = ToolPolicy(access="public")
ExecutorFactory = Callable[..., ToolExecutor]


async def lookup(customer_id: int) -> dict[str, int]:
    """查询客户。

    测试用工具：第一段会成为给模型看的描述，测试据此验证描述的转换。
    """
    return {"id": customer_id}


async def undocumented(x: int) -> int:
    """注册时显式传入空描述，用来验证空描述会转换为 None。"""
    return x


def test_tool_definitions_use_model_aliases(make_executor: ExecutorFactory) -> None:
    """工具定义使用模型侧别名，满足 Provider SDK 的命名规则；空描述转换为 None。"""
    registry = ToolRegistry()
    registry.register_function(lookup, name="crm.customer.lookup", policy=PUBLIC)
    registry.register_function(undocumented, name="undocumented", policy=PUBLIC, description="")
    definitions = to_tool_definitions(make_executor(registry).list_tools(Principal()))
    assert [d.name for d in definitions] == ["crm_customer_lookup", "undocumented"]
    assert definitions[0].description == "查询客户。"
    assert definitions[0].parameters["properties"]["customer_id"]["type"] == "integer"
    assert definitions[1].description is None


def test_invocations_keep_raw_arguments_and_fill_missing_ids() -> None:
    """调用转换保留原始参数字符串；Provider 返回空 ID 时用序号补齐。"""
    calls = [ToolCall(id="", name="a", arguments='{"x": 1}'), ToolCall(id="id2", name="b")]
    invs = to_invocations(calls)
    assert [(i.call_id, i.tool, i.arguments) for i in invs] == [
        ("call_0", "a", '{"x": 1}'),
        ("id2", "b", ""),
    ]


def test_results_errors_images_and_untrusted_wrapping() -> None:
    """成功结果转换为文本；错误只带错误码和摘要，前缀只由 SDK 添加一次；可选图片与不可信标记。"""
    ok = ToolExecutionResult(
        call_id="c1",
        tool="t",
        alias="t",
        status="success",
        content=[
            TextBlock(text="hi"),
            JsonBlock(data={"a": 1}),
            ImageBlock(mime_type="image/png", data_b64="AAAA"),
        ],
    )
    err = ToolExecutionResult(
        call_id="c2",
        tool="t",
        alias="t",
        status="error",
        error=ToolErrorInfo(type="timeout", code="attempt_timeout", message="slow", retryable=True),
    )
    plain, failed = to_tool_results([ok, err])
    assert plain.content == 'hi\n{"a":1}\n[image]'
    assert failed.is_error and failed.content == "attempt_timeout: slow"
    message = failed.to_message()
    assert message.content == "[tool error] attempt_timeout: slow"

    [wrapped] = to_tool_results([ok], wrap_untrusted=True)
    assert isinstance(wrapped.content, str) and wrapped.content.startswith("<tool_output untrusted")

    [multimodal] = to_tool_results([ok], include_images=True)
    assert isinstance(multimodal.content, list)
    assert isinstance(multimodal.content[0], TextPart)
    assert isinstance(multimodal.content[1], ImagePart)
    assert multimodal.content[1].url == "data:image/png;base64,AAAA"


def test_context_from_request_keeps_request_id() -> None:
    """上下文转换沿用同一个 request_id，权限只来自宿主显式提供的 scopes。"""
    request = RequestContext(request_id="req-1", user_id="u", tenant_id="t", session_id="s")
    ctx = context_from_request(request, scopes=["a"], timeout_s=5)
    assert (ctx.request_id, ctx.principal.user_id, ctx.principal.tenant_id, ctx.session_id) == (
        "req-1",
        "u",
        "t",
        "s",
    )
    assert ctx.principal.scopes == {"a"}
    assert ctx.deadline is not None
    assert context_from_request(request).principal.scopes == frozenset()


async def test_run_tool_calls_turns_bad_json_into_error_results(
    make_executor: ExecutorFactory,
) -> None:
    """模型给出非法 JSON 参数时得到 invalid_arguments 结果，而不是抛出异常。"""
    registry = ToolRegistry()
    registry.register_function(lookup, name="lookup", policy=PUBLIC)
    executor = make_executor(registry)
    response = ChatResponse(
        request_id="req-1",
        provider="deepseek",
        model="m",
        message=Message.assistant(
            tool_calls=[
                ToolCall(id="c1", name="lookup", arguments='{"customer_id": 7}'),
                ToolCall(id="c2", name="lookup", arguments="{broken"),
            ]
        ),
        finish_reason="tool_calls",
    )
    results, messages = await run_tool_calls(executor, response, ToolContext())
    assert [r.status for r in results] == ["success", "error"]
    assert results[1].error and results[1].error.code == "arguments_not_json"
    assert [m.tool_call_id for m in messages] == ["c1", "c2"]
    assert messages[1].is_error
