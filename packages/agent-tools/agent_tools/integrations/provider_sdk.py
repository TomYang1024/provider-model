"""与 Provider SDK 之间的双向转换（需要安装 ``agent-tools[provider-sdk]``）。

这是 ``agent_tools`` 中**唯一**允许 import ``provider_sdk`` 的模块；Provider SDK 不知道
本包的存在。转换内容：

* 工具定义：:class:`ToolListing` → ``provider_sdk.ToolDefinition``（使用模型侧别名）；
* 工具调用：``provider_sdk.ToolCall`` → :class:`ToolInvocation`（参数保持原始字符串，
  由执行器解析，非法 JSON 会得到 ``InvalidArgumentsError`` 结果而不是异常）；
* 工具结果：:class:`ToolExecutionResult` → ``provider_sdk.ToolResult``；错误只带错误码和安全摘要，
  ``[tool error]`` 前缀由 SDK 的 ``ToolResult.to_message()`` 添加，这里不重复添加；
* 上下文：``provider_sdk.RequestContext`` → :class:`ToolContext`（沿用同一个 ``request_id``）。

``ToolError`` 不会被转换成 ``ProviderError``，反之亦然：两套错误体系保持独立。
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence

try:
    from provider_sdk import (
        ChatResponse,
        ImagePart,
        RequestContext,
        TextPart,
        ToolCall,
        ToolDefinition,
        ToolResult,
    )
except ImportError as exc:  # pragma: no cover - 由打包测试覆盖
    raise ImportError(
        "agent_tools.integrations.provider_sdk requires the 'provider-sdk' extra: "
        "pip install 'agent-tools[provider-sdk]'"
    ) from exc

from ..executor import ExecutionMode, ToolExecutor
from ..protocol.context import Principal, ToolContext
from ..protocol.invocation import ImageBlock, ToolExecutionResult, ToolInvocation
from ..protocol.spec import ToolListing

__all__ = [
    "context_from_request",
    "run_tool_calls",
    "to_invocations",
    "to_tool_definitions",
    "to_tool_results",
]

_UNTRUSTED_OPEN = '<tool_output untrusted="true">\n'
_UNTRUSTED_CLOSE = "\n</tool_output>"


def to_tool_definitions(listing: ToolListing) -> list[ToolDefinition]:
    """把可见的工具列表转换成发给模型的工具定义。

    Args:
        listing: :meth:`ToolExecutor.list_tools` 的返回值。

    Returns:
        ``ToolDefinition`` 列表，``name`` 为模型侧别名（满足 Provider 的命名规则）。
    """
    return [
        ToolDefinition(
            name=tool.alias,
            description=tool.description or None,
            parameters=tool.input_schema,
        )
        for tool in listing.tools
    ]


def to_invocations(tool_calls: Iterable[ToolCall]) -> list[ToolInvocation]:
    """把模型返回的工具调用转换成调用请求。

    参数保持原始 JSON 字符串，交给执行器解析与校验。Provider 返回空的调用 ID 时，
    用 ``call_<序号>`` 补齐（ID 只需在本批内唯一）。

    Args:
        tool_calls: 模型返回的工具调用。

    Returns:
        调用请求列表，顺序与输入一致。
    """
    return [
        ToolInvocation(
            call_id=call.id or f"call_{index}",
            tool=call.name or "_unnamed",
            arguments=call.arguments,
        )
        for index, call in enumerate(tool_calls)
    ]


def to_tool_results(
    results: Sequence[ToolExecutionResult],
    *,
    include_images: bool = False,
    wrap_untrusted: bool = False,
) -> list[ToolResult]:
    """把执行结果转换成回传给模型的工具结果。

    Args:
        results: 执行结果，``call_id`` 必须与模型的 ``ToolCall.id`` 一致。
        include_images: 是否把图片作为多模态内容回传（模型需要支持图片输入）；
            默认用文本占位符代替。
        wrap_untrusted: 是否用固定的标记包住工具输出文本，提示模型这是不可信内容。
            这只是一个辅助手段，不能阻止提示注入；默认关闭，由宿主决定。

    Returns:
        ``ToolResult`` 列表，顺序与输入一致。
    """
    converted: list[ToolResult] = []
    for result in results:
        if result.error is not None:
            converted.append(
                ToolResult(
                    tool_call_id=result.call_id,
                    content=f"{result.error.code}: {result.error.message}",
                    is_error=True,
                )
            )
            continue
        text = result.to_model_text()
        if wrap_untrusted:
            text = f"{_UNTRUSTED_OPEN}{text}{_UNTRUSTED_CLOSE}"
        images = [b for b in result.content if isinstance(b, ImageBlock)]
        if include_images and images:
            parts: list[TextPart | ImagePart] = [TextPart(text=text)]
            for image in images:
                url = image.url or f"data:{image.mime_type};base64,{image.data_b64}"
                parts.append(ImagePart(url=url))
            converted.append(ToolResult(tool_call_id=result.call_id, content=parts))
        else:
            converted.append(ToolResult(tool_call_id=result.call_id, content=text))
    return converted


def context_from_request(
    request: RequestContext,
    *,
    scopes: Iterable[str] = (),
    timeout_s: float | None = None,
) -> ToolContext:
    """根据 Provider SDK 的请求上下文构造工具调用上下文。

    沿用同一个 ``request_id``，使模型调用与工具调用的日志可以直接关联。``scopes`` 必须由
    宿主在完成身份认证后提供，基础设施不会自行推断权限。

    Args:
        request: Provider SDK 的请求上下文。
        scopes: 调用方拥有的权限。
        timeout_s: 本批工具调用的总时长（秒）；``None`` 表示不设整体截止时间。

    Returns:
        工具调用上下文。
    """
    return ToolContext(
        request_id=request.request_id,
        principal=Principal(
            user_id=request.user_id, tenant_id=request.tenant_id, scopes=frozenset(scopes)
        ),
        deadline=None if timeout_s is None else time.monotonic() + timeout_s,
        session_id=request.session_id,
    )


async def run_tool_calls(
    executor: ToolExecutor,
    response: ChatResponse,
    ctx: ToolContext,
    *,
    mode: ExecutionMode = "auto",
    snapshot_id: str | None = None,
    include_images: bool = False,
    wrap_untrusted: bool = False,
) -> tuple[list[ToolExecutionResult], list[ToolResult]]:
    """便捷函数：执行模型一轮返回的全部工具调用，并转换成可回传的工具结果。

    Args:
        executor: 工具执行器。
        response: 模型响应。
        ctx: 工具调用上下文。
        mode: 批量执行模式。
        snapshot_id: 与发给模型的工具列表对应的快照编号。
        include_images: 见 :func:`to_tool_results`。
        wrap_untrusted: 见 :func:`to_tool_results`。

    Returns:
        ``(执行结果, 回传给模型的工具结果)``。

    Raises:
        asyncio.CancelledError: 调用方被取消（原样传播）。
        ExecutorClosedError: 执行器已关闭。
    """
    results = await executor.execute_many(
        to_invocations(response.tool_calls), ctx, mode=mode, snapshot_id=snapshot_id
    )
    return results, to_tool_results(
        results, include_images=include_images, wrap_untrusted=wrap_untrusted
    )
