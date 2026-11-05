"""适配器契约。

适配器只负责在 Provider 无关的数据模型与某个 Provider 协议之间做转换：
请求构建、响应 / 流式归一化、Usage 解析和异常映射。它不了解 Trace 后端、
日志、路由、计费或工具执行——这些都在 Client 和可观测性层中实现。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Protocol

import httpx2
from opentelemetry import trace

from .. import exceptions as errors
from ..schemas.message import ToolChoiceFunction
from ..schemas.provider import ModelCapabilities, ProviderConfig
from ..schemas.request import ChatRequest
from ..schemas.response import ChatResponse
from ..schemas.stream import StreamEvent


class AdapterStream(Protocol):
    """归一化为 SDK 事件的 Provider 流。

    产生除 ``message_start`` 之外的全部事件（message_start 由 Client 添加），
    并以 ``usage`` + ``message_end`` 结束。``aclose()`` 必须幂等，并释放底层 HTTP 连接。
    """

    def __aiter__(self) -> AdapterStream: ...

    async def __anext__(self) -> StreamEvent: ...

    async def aclose(self) -> None: ...

    @property
    def emitted_output(self) -> bool: ...


class BaseProviderAdapter(ABC):
    #: 注册表中的适配器类型键
    adapter_type: ClassVar[str]
    #: 是否支持 ``ReasoningConfig.budget_tokens``（思考 Token 预算）
    supports_reasoning_budget: ClassVar[bool] = False
    #: 是否支持按请求开关推理（否则只能通过模型 ID 选择）
    supports_reasoning_toggle: ClassVar[bool] = False
    #: 是否支持 ``ReasoningConfig.effort``（推理深度）
    supports_reasoning_effort: ClassVar[bool] = False

    def __init__(
        self,
        config: ProviderConfig,
        *,
        tracer: trace.Tracer,
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        self.tracer = tracer
        self._transport = transport

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def otel_provider_name(self) -> str:
        """``gen_ai.provider.name`` 属性值；未配置时使用注册名。"""
        return self.config.otel_provider_name or self.config.name

    # -- 请求校验 -----------------------------------------------------------------------
    def validate(self, request: ChatRequest) -> ModelCapabilities:
        """在任何 I/O 之前，按声明的模型能力校验请求。

        不支持的能力明确抛出 UnsupportedCapabilityError，而不是静默忽略参数，
        避免调用方误以为某项能力已生效。
        """
        caps = self.config.capabilities_for(request.model)
        common: dict[str, Any] = {"provider": self.name, "model": request.model}
        # strict_models 模式下，未登记的模型直接拒绝
        if caps is None:
            raise errors.InvalidRequestError(
                f"model {request.model!r} is not configured for provider {self.name!r}",
                error_code="model_not_found",
                **common,
            )

        def unsupported(what: str) -> errors.UnsupportedCapabilityError:
            return errors.UnsupportedCapabilityError(
                f"{what} is not supported by {self.name}/{request.model}", **common
            )

        if request.stream and not caps.streaming:
            raise unsupported("streaming")
        if request.tools and not caps.tools:
            raise unsupported("tool calling")
        if request.tool_choice is not None and not request.tools:
            raise errors.InvalidRequestError("tool_choice requires tools", **common)
        # "required" 和指定函数都属于强制工具选择（GLM / Qwen 只支持 auto）
        forced = request.tool_choice == "required" or isinstance(
            request.tool_choice, ToolChoiceFunction
        )
        if forced and not caps.tool_choice_required:
            raise unsupported("forced tool_choice")
        if request.parallel_tool_calls and not caps.parallel_tool_calls:
            raise unsupported("parallel tool calls")
        if any(m.has_images() for m in request.messages) and not caps.vision:
            raise unsupported("image input")
        if (
            request.response_format is not None
            and request.response_format.type == "json_object"
            and not caps.json_mode
        ):
            raise unsupported("JSON mode")
        if request.reasoning is not None:
            # 模型不支持推理却要求开启
            if request.reasoning.enabled and not caps.reasoning:
                raise unsupported("reasoning")
            # 模型始终思考，无法关闭
            if not request.reasoning.enabled and caps.reasoning_always_on:
                raise unsupported("disabling reasoning")
            # 混合思考模型，但 Provider 只能靠模型 ID 选择推理模式
            if (
                request.reasoning.enabled
                and caps.reasoning
                and not caps.reasoning_always_on
                and not self.supports_reasoning_toggle
            ):
                raise unsupported("toggling reasoning per request")
            if request.reasoning.budget_tokens is not None and not self.supports_reasoning_budget:
                raise unsupported("reasoning budget")
            if request.reasoning.effort is not None and not self.supports_reasoning_effort:
                raise unsupported("reasoning effort")
        # Provider 扩展参数白名单（可在配置中限制调用方能传哪些厂商参数）
        if self.config.allowed_options is not None:
            extra = set(request.provider_options) - set(self.config.allowed_options)
            if extra:
                raise errors.InvalidRequestError(
                    f"provider_options not allowed: {sorted(extra)}", **common
                )
        # 工具结果必须对应历史中某个已出现的 tool_call id（历史中有工具调用时才校验）
        known_ids = {tc.id for m in request.messages if m.tool_calls for tc in m.tool_calls}
        for m in request.messages:
            if m.role == "tool" and known_ids and m.tool_call_id not in known_ids:
                raise errors.InvalidRequestError(
                    "tool message references an unknown tool_call_id", **common
                )
        return caps

    # -- 调用接口 -----------------------------------------------------------------------
    @abstractmethod
    async def complete(self, request: ChatRequest) -> ChatResponse:
        """执行一次非流式调用。"""

    @abstractmethod
    async def open_stream(self, request: ChatRequest) -> AdapterStream:
        """开始流式调用；Provider 接受请求（收到响应头）后返回。"""

    @abstractmethod
    def map_error(self, exc: BaseException, request: ChatRequest) -> errors.ProviderError:
        """把厂商 / 传输层异常转换为 SDK 统一异常。"""

    async def aclose(self) -> None:  # noqa: B027 - 可选钩子
        """释放 HTTP 资源。"""
