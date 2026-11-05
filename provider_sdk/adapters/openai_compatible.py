"""OpenAI 兼容协议的通用适配器。

DeepSeek / GLM / Kimi / Qwen 都提供 OpenAI 兼容的 Chat Completions 接口，
本模块实现四者共用的部分：请求参数构建、响应归一化、流式事件转换、
Tool Calling 组装、Usage 解析和异常映射。各 Provider 的差异通过子类覆盖
``apply_reasoning`` / ``classify_error`` 钩子以及 ``ProviderConfig`` 开关处理。
"""

from __future__ import annotations

import re
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, ClassVar

import httpx2
import openai
from openai import AsyncOpenAI
from opentelemetry import trace

from .. import exceptions as errors
from ..observability.redaction import sanitize_text
from ..schemas.context import TokenUsage
from ..schemas.message import (
    ImagePart,
    Message,
    TextPart,
    ToolCall,
    ToolChoiceFunction,
)
from ..schemas.provider import ModelCapabilities, ProviderConfig
from ..schemas.request import ChatRequest
from ..schemas.response import ChatResponse, FinishReason
from ..schemas.stream import (
    MessageEndEvent,
    ReasoningDeltaEvent,
    StreamEvent,
    TextDeltaEvent,
    ToolCallDeltaEvent,
    ToolCallEndEvent,
    ToolCallStartEvent,
    UsageEvent,
)
from ..transport import TracingTransport, resolve_proxy
from .base import BaseProviderAdapter

# Provider 原始 finish_reason -> SDK 统一取值；未列出的值归为 "unknown"，
# 原始值仍保留在 ChatResponse.raw_finish_reason 中便于排查。
_FINISH_REASONS: dict[str, FinishReason] = {
    "stop": "stop",
    "eos": "stop",
    "length": "length",
    "max_tokens": "length",
    "tool_calls": "tool_calls",
    "function_call": "tool_calls",
    "content_filter": "content_filter",
    "sensitive": "content_filter",  # GLM：内容安全拦截
    "network_error": "error",  # GLM：推理过程网络异常
    "insufficient_system_resource": "error",  # DeepSeek：系统资源不足
}

# 以下正则用于在 Provider 错误信息中识别错误类别（各家措辞不统一，只能按文本匹配）。
# 上下文超长
_CONTEXT_LENGTH_RE = re.compile(
    r"context[ _]length|maximum context|context window|too long|token limit|"
    r"exceed(?:s|ed)?\b.*\b(?:token|length|context)|input length|range of input length",
    re.IGNORECASE,
)
# 余额 / 配额不足：属于不可重试的限流类错误
_QUOTA_RE = re.compile(
    r"insufficient[ _](?:balance|quota)|exceeded[ _].*quota|quota[ _]exceeded|"
    r"arrearage|欠费|余额不足",
    re.IGNORECASE,
)
# 内容安全拦截
_CONTENT_FILTER_RE = re.compile(
    r"data_inspection_failed|content[ _]filter|sensitive|high risk|inappropriate content",
    re.IGNORECASE,
)

# 错误摘要中保留的 Provider 原始错误信息最大长度（脱敏后截断）
_MAX_ERROR_DETAIL = 300


@dataclass
class _ErrorInfo:
    """从 openai 异常中提取的、与 Provider 无关的错误要素。"""

    status: int | None
    code: str | None
    type: str | None
    message: str
    request_id: str | None


@dataclass(frozen=True)
class ErrorRule:
    """Provider 业务错误码的分类规则：映射到哪个异常类、错误码、是否可重试。"""

    cls: type[errors.ProviderError]
    code: str | None = None
    retryable: bool | None = None


# 常用规则：配额不足（不可重试）与内容安全拦截
QUOTA = ErrorRule(errors.RateLimitError, "insufficient_quota", retryable=False)
CONTENT_FILTERED = ErrorRule(errors.InvalidRequestError, "content_filtered")


@dataclass
class _ToolCallBuilder:
    """流式 Tool Call 的拼装器：同一 index 的参数片段在此累积。"""

    index: int
    id: str
    name: str = ""
    arguments: list[str] = field(default_factory=list)
    started: bool = False  # 是否已发出 tool_call_start
    ended: bool = False  # 是否已发出 tool_call_end

    def tool_call(self) -> ToolCall:
        return ToolCall(id=self.id, name=self.name, arguments="".join(self.arguments))


def _gen_tool_call_id(index: int) -> str:
    """Provider 未返回 tool_call id 时生成一个本地唯一 id，保证后续 ToolResult 可关联。"""
    return f"call_{index}_{uuid.uuid4().hex[:12]}"


def _extra(obj: Any, key: str) -> Any:
    """读取 openai pydantic 对象上的厂商扩展字段（如 reasoning_content）。

    openai 的模型类配置为 ``extra="allow"``，非标准字段存放在 ``model_extra`` 中。
    """
    if obj is None:
        return None
    extra = getattr(obj, "model_extra", None) or {}
    if key in extra:
        return extra[key]
    return getattr(obj, key, None)


class OpenAICompatibleAdapter(BaseProviderAdapter):
    adapter_type: ClassVar[str] = "openai_compatible"

    def __init__(
        self,
        config: ProviderConfig,
        *,
        tracer: trace.Tracer,
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(config, tracer=tracer, transport=transport)
        # 启用的 Provider 必须有密钥；绝不让 openai SDK 回退读取 OPENAI_API_KEY
        if config.api_key is None or not config.api_key.get_secret_value():
            raise ValueError(f"provider {config.name!r} is enabled but has no API key")
        t = config.timeouts
        # 每个 Provider 独立的连接池，避免某个 Provider 变慢时占满其他 Provider 的连接
        limits = httpx2.Limits(
            max_connections=config.max_connections,
            max_keepalive_connections=config.max_keepalive_connections,
            keepalive_expiry=config.keepalive_expiry,
        )
        if transport is None:
            # 代理在初始化时解析一次，并由我们自己的传输层使用，
            # 确保经过代理的请求同样会产生 HTTP Span。
            proxy = resolve_proxy(config.base_url, proxy=config.proxy, trust_env=config.trust_env)
            transport = httpx2.AsyncHTTPTransport(limits=limits, proxy=proxy)
        self._http = httpx2.AsyncClient(
            transport=TracingTransport(
                transport, tracer=tracer, propagate_context=config.propagate_trace_context
            ),
            timeout=httpx2.Timeout(connect=t.connect, read=t.read, write=t.write, pool=t.pool),
            # 代理已在上面解析；若此处 trust_env=True，httpx2 会额外挂载代理传输层，
            # 这些请求将绕过 TracingTransport（丢失 Span 和连接池限制）。
            trust_env=False,
            follow_redirects=False,
        )
        self._client = AsyncOpenAI(
            api_key=config.api_key.get_secret_value(),
            base_url=config.base_url,
            # openai SDK 内置重试只发生在拿到响应之前，次数有上限（≤5），不会无限重试
            max_retries=config.max_retries,
            http_client=self._http,
            default_headers=config.extra_headers or None,
        )
        # openai SDK 会从环境变量读取 OPENAI_ORG_ID / OPENAI_PROJECT_ID 并作为请求头发送，
        # 这些 OpenAI 的内部标识绝不能泄露给第三方 Provider，因此强制清空。
        self._client.organization = None
        self._client.project = None

    # ------------------------------------------------------------------------------
    # 请求构建
    # ------------------------------------------------------------------------------
    def convert_message(self, message: Message) -> dict[str, Any]:
        """把 SDK 的 Message 转为 OpenAI Chat Completions 消息格式。"""
        out: dict[str, Any] = {"role": message.role}
        if message.role == "tool":
            # 工具结果：多数 Provider 只接受字符串 content，多段内容合并为纯文本
            out["tool_call_id"] = message.tool_call_id
            out["content"] = message.text if isinstance(message.content, list) else message.content
            if message.name:
                out["name"] = message.name
            return out
        if isinstance(message.content, list):
            # 多模态内容：文本段与图片段分别转换
            parts: list[dict[str, Any]] = []
            for part in message.content:
                if isinstance(part, TextPart):
                    parts.append({"type": "text", "text": part.text})
                elif isinstance(part, ImagePart):
                    image: dict[str, Any] = {"url": part.url}
                    if part.detail:
                        image["detail"] = part.detail
                    parts.append({"type": "image_url", "image_url": image})
            out["content"] = parts
        else:
            # 只有 tool_calls 的 assistant 消息 content 为 None；
            # 部分 Provider 不接受 null，统一发送空字符串
            out["content"] = message.content if message.content is not None else ""
        if message.name:
            out["name"] = message.name
        if message.role == "assistant":
            if message.tool_calls:
                out["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.name, "arguments": tc.arguments or "{}"},
                    }
                    for tc in message.tool_calls
                ]
            # DeepSeek（带 tools 时）和 Kimi 要求在历史中原样回传 reasoning_content，
            # 其他 Provider 默认不回传（由配置开关控制）
            if self.config.send_reasoning_content and message.reasoning_content:
                out["reasoning_content"] = message.reasoning_content
        return out

    def apply_reasoning(
        self, request: ChatRequest, caps: ModelCapabilities, body: dict[str, Any]
    ) -> None:
        """钩子：把 ``request.reasoning`` 转换为 Provider 请求体字段，默认不做任何事。"""

    @staticmethod
    def apply_thinking_object(
        request: ChatRequest, caps: ModelCapabilities, body: dict[str, Any]
    ) -> None:
        """``thinking: {"type": "enabled"|"disabled"}`` + ``reasoning_effort`` 风格的推理参数。

        DeepSeek、GLM、Kimi 使用此格式。对于始终开启思考的模型不发送 ``thinking``
        （部分 Provider 会拒绝该参数），只发送推理深度。
        """
        r = request.reasoning
        # 未指定推理配置时不发送任何参数，沿用 Provider 默认行为
        if r is None or not caps.reasoning:
            return
        if not caps.reasoning_always_on:
            body["thinking"] = {"type": "enabled" if r.enabled else "disabled"}
        if r.enabled and r.effort is not None:
            body["reasoning_effort"] = r.effort

    def build_params(self, request: ChatRequest, caps: ModelCapabilities) -> dict[str, Any]:
        """构建 ``chat.completions.create()`` 的关键字参数。"""
        params: dict[str, Any] = {
            "model": request.model,
            "messages": [self.convert_message(m) for m in request.messages],
        }
        if request.tools:
            tools = []
            for tool in request.tools:
                fn: dict[str, Any] = {"name": tool.name, "parameters": tool.parameters}
                if tool.description:
                    fn["description"] = tool.description
                if tool.strict is not None:
                    fn["strict"] = tool.strict
                tools.append({"type": "function", "function": fn})
            params["tools"] = tools
        if request.tool_choice is not None:
            if isinstance(request.tool_choice, ToolChoiceFunction):
                # 指定调用某个函数
                params["tool_choice"] = {
                    "type": "function",
                    "function": {"name": request.tool_choice.name},
                }
            else:
                params["tool_choice"] = request.tool_choice
        if request.parallel_tool_calls is not None:
            params["parallel_tool_calls"] = request.parallel_tool_calls
        # 只发送调用方显式设置的采样参数，其余使用 Provider 默认值
        for key in ("temperature", "top_p", "max_tokens", "stop", "seed"):
            value = getattr(request, key)
            if value is not None:
                params[key] = value
        if request.response_format is not None:
            params["response_format"] = {"type": request.response_format.type}
        if request.stream:
            params["stream"] = True
            # 需要显式请求才会在流末尾返回 Usage 的 Provider（DeepSeek / Kimi / Qwen）
            if self.config.stream_usage_option:
                params["stream_options"] = {"include_usage": True}
        # 扩展参数通过 extra_body 合并进 JSON 请求体：
        # Provider 默认参数 < 请求级 provider_options < 推理参数映射
        body = {**self.config.default_options, **request.provider_options}
        self.apply_reasoning(request, caps, body)
        if body:
            params["extra_body"] = body
        return params

    # ------------------------------------------------------------------------------
    # 响应归一化
    # ------------------------------------------------------------------------------
    @staticmethod
    def normalize_finish_reason(raw: str | None) -> FinishReason:
        if raw is None:
            return "unknown"
        return _FINISH_REASONS.get(raw, "unknown")

    @staticmethod
    def parse_usage(usage: Any) -> TokenUsage | None:
        """解析 Usage；Provider 未返回 Token 数时返回 None（绝不伪造数字）。

        同时兼容 openai 对象（非流式）和普通 dict（Kimi 旧版放在 choice 内的 usage）。
        """
        if usage is None:
            return None
        if isinstance(usage, dict):
            get = usage.get
        else:

            def get(key: str, default: Any = None) -> Any:
                value = _extra(usage, key)
                return default if value is None else value

        def details(name: str, key: str) -> int | None:
            # 读取 *_tokens_details 子对象中的字段（可能是 dict 也可能是对象）
            d = get(name)
            if d is None:
                return None
            v = d.get(key) if isinstance(d, dict) else getattr(d, key, None)
            return v if isinstance(v, int) else None

        input_tokens = get("prompt_tokens")
        output_tokens = get("completion_tokens")
        if input_tokens is None and output_tokens is None:
            return None
        total = get("total_tokens")
        if total is None and input_tokens is not None and output_tokens is not None:
            total = input_tokens + output_tokens
        # 缓存命中 Token：标准字段优先，再兼容各家私有字段
        cached = details("prompt_tokens_details", "cached_tokens")
        if cached is None:
            # DeepSeek：prompt_cache_hit_tokens；Moonshot 旧版：cached_tokens
            for key in ("prompt_cache_hit_tokens", "cached_tokens"):
                v = get(key)
                if isinstance(v, int):
                    cached = v
                    break
        return TokenUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total,
            reasoning_tokens=details("completion_tokens_details", "reasoning_tokens"),
            cached_input_tokens=cached,
            source="provider",
        )

    def provider_extensions(self, obj: Any) -> dict[str, Any]:
        """白名单方式保留的 Provider 扩展字段（不暴露原始厂商对象）。"""
        fingerprint = getattr(obj, "system_fingerprint", None)
        return {"system_fingerprint": fingerprint} if fingerprint else {}

    def parse_response(self, request: ChatRequest, completion: Any) -> ChatResponse:
        """把非流式 ChatCompletion 转为统一的 ChatResponse。"""
        if not getattr(completion, "choices", None):
            raise errors.ProviderServerError(
                f"{self.name} returned a response without choices",
                provider=self.name,
                model=request.model,
                error_code="empty_response",
            )
        # SDK 只支持 n=1，取第一个 choice
        choice = completion.choices[0]
        msg = choice.message
        tool_calls = None
        if getattr(msg, "tool_calls", None):
            tool_calls = [
                ToolCall(
                    id=tc.id or _gen_tool_call_id(i),
                    name=tc.function.name,
                    arguments=tc.function.arguments or "",
                )
                for i, tc in enumerate(msg.tool_calls)
            ]
        reasoning = _extra(msg, "reasoning_content") or None
        content = msg.content
        if content is None and not tool_calls:
            content = ""
        usage = self.parse_usage(getattr(completion, "usage", None)) or TokenUsage.unknown()
        raw_finish = choice.finish_reason
        return ChatResponse(
            id=getattr(completion, "id", None),
            request_id=request.context.request_id,
            provider=self.name,
            model=request.model,
            response_model=getattr(completion, "model", None),
            message=Message(
                role="assistant",
                content=content,
                tool_calls=tool_calls,
                reasoning_content=reasoning,
            ),
            finish_reason=self.normalize_finish_reason(raw_finish),
            raw_finish_reason=raw_finish,
            usage=usage,
            provider_extensions=self.provider_extensions(completion),
        )

    # ------------------------------------------------------------------------------
    # 调用入口
    # ------------------------------------------------------------------------------
    async def complete(self, request: ChatRequest) -> ChatResponse:
        # 先做能力校验（无 I/O），不支持的能力在发请求前就失败
        caps = self.validate(request)
        params = self.build_params(request, caps)
        try:
            completion = await self._client.chat.completions.create(**params)
        except errors.ProviderError:
            raise
        except Exception as exc:
            # 厂商异常统一映射，原异常通过 from 保留在异常链中
            raise self.map_error(exc, request) from exc
        return self.parse_response(request, completion)

    async def open_stream(self, request: ChatRequest) -> OpenAICompatibleStream:
        caps = self.validate(request)
        params = self.build_params(request, caps)
        try:
            # create() 在收到 HTTP 响应头后返回：此时 Provider 已接受请求，
            # 4xx/5xx 等错误在这里抛出，还没有产生任何流事件
            raw = await self._client.chat.completions.create(**params)
        except errors.ProviderError:
            raise
        except Exception as exc:
            raise self.map_error(exc, request) from exc
        return OpenAICompatibleStream(self, request, raw)

    async def aclose(self) -> None:
        # 关闭 openai 客户端会同时关闭底层 httpx2 连接池
        await self._client.close()

    # ------------------------------------------------------------------------------
    # 异常映射
    # ------------------------------------------------------------------------------
    @staticmethod
    def _error_info(exc: BaseException) -> _ErrorInfo:
        """从 openai 异常中提取状态码、错误码、错误类型和消息。

        各 Provider 的错误体结构不完全一致：有的是 ``{"error": {...}}``，有的直接平铺。
        """
        status = getattr(exc, "status_code", None)
        body = getattr(exc, "body", None)
        code = getattr(exc, "code", None)
        etype = getattr(exc, "type", None)
        message = getattr(exc, "message", None) or str(exc)
        if isinstance(body, dict):
            err = body.get("error", body)
            if isinstance(err, dict):
                code = err.get("code", code)
                etype = err.get("type", etype)
                message = err.get("message", message) or message
        return _ErrorInfo(
            status=status if isinstance(status, int) else None,
            code=str(code) if code is not None else None,
            type=str(etype) if etype is not None else None,
            message=str(message),
            request_id=getattr(exc, "request_id", None),
        )

    def classify_error(self, info: _ErrorInfo) -> ErrorRule | None:
        """钩子：Provider 业务错误码分类。返回 None 表示使用通用规则。"""
        return None

    def map_error(self, exc: BaseException, request: ChatRequest) -> errors.ProviderError:
        """把厂商 / 传输层异常转换为 SDK 统一异常。

        匹配顺序：传输层异常 → Provider 业务错误码规则 → 通用 HTTP 状态码 / 文本规则。
        生成的错误摘要会脱敏并截断，不包含密钥和完整 Prompt。
        """
        if isinstance(exc, errors.ProviderError):
            return exc
        base: dict[str, Any] = {"provider": self.name, "model": request.model}

        # 1. 传输层：超时必须先于连接错误判断（APITimeoutError 是 APIConnectionError 的子类）
        if isinstance(exc, openai.APITimeoutError | httpx2.TimeoutException):
            return errors.TimeoutError(f"{self.name} request timed out", **base)
        if isinstance(exc, openai.APIConnectionError | httpx2.TransportError):
            return errors.ConnectionError(
                f"{self.name} connection failed ({type(exc.__cause__ or exc).__name__})", **base
            )
        if isinstance(exc, openai.APIResponseValidationError):
            return errors.ProviderServerError(
                f"{self.name} returned an invalid response", error_code="invalid_response", **base
            )
        if not isinstance(exc, openai.APIError):
            # 非预期异常：只暴露异常类型名，不暴露内部信息
            return errors.ProviderError(
                f"unexpected error calling {self.name} ({type(exc).__name__})",
                error_code="internal_error",
                **base,
            )

        # 2. 构造安全的错误摘要（脱敏 + 截断）
        info = self._error_info(exc)
        detail = sanitize_text(info.message, max_chars=_MAX_ERROR_DETAIL)
        prefix = f"{self.name} returned HTTP {info.status}" if info.status else f"{self.name} error"
        summary = f"{prefix}: {detail}" if detail else prefix
        if info.code:
            summary += f" (code={sanitize_text(info.code, max_chars=64)})"
        base.update(status_code=info.status, provider_request_id=info.request_id)
        text = f"{info.code or ''} {info.type or ''} {info.message}"

        # 3. Provider 业务错误码优先
        rule = self.classify_error(info)
        if rule is not None:
            return rule.cls(summary, error_code=rule.code, retryable=rule.retryable, **base)

        # 4. 通用规则：先识别配额不足（重试无意义），再按状态码分类
        status = info.status
        if _QUOTA_RE.search(text) or status == 402:
            return errors.RateLimitError(
                summary, error_code="insufficient_quota", retryable=False, **base
            )
        if status in (401, 403):
            return errors.AuthenticationError(summary, **base)
        if status == 429:
            return errors.RateLimitError(summary, **base)
        if status == 408:
            return errors.TimeoutError(summary, **base)
        if status is not None and 400 <= status < 500:
            if _CONTEXT_LENGTH_RE.search(text):
                return errors.ContextLengthExceededError(summary, **base)
            if _CONTENT_FILTER_RE.search(text):
                return errors.InvalidRequestError(summary, error_code="content_filtered", **base)
            if status == 404:
                return errors.InvalidRequestError(summary, error_code="not_found", **base)
            return errors.InvalidRequestError(summary, **base)
        if status is not None and status >= 500:
            return errors.ProviderServerError(summary, **base)
        # 5. 没有 HTTP 状态码：错误是在 SSE 流内部下发的（此时可能已有输出，不可重试）
        if _CONTEXT_LENGTH_RE.search(text):
            return errors.ContextLengthExceededError(summary, **base)
        return errors.ProviderServerError(
            summary, error_code="stream_error", retryable=False, **base
        )


class OpenAICompatibleStream:
    """把 OpenAI 风格的 ``chat.completion.chunk`` SSE 流转换为 SDK 统一事件。

    一个 chunk 可能产生多个事件（例如同时含 reasoning 和 content），
    因此用 ``_pending`` 队列缓冲，``__anext__`` 每次只取出一个。
    本类只负责协议转换；message_start、序号、超时、Trace 由 ChatStream 负责。
    """

    def __init__(self, adapter: OpenAICompatibleAdapter, request: ChatRequest, raw: Any) -> None:
        self._adapter = adapter
        self._request = request
        self._raw = raw  # openai.AsyncStream
        self._pending: deque[StreamEvent] = deque()  # 待发出的事件
        self._tools: dict[int, _ToolCallBuilder] = {}  # 按 index 拼装中的工具调用
        self._text: list[str] = []  # 聚合文本，用于最终 ChatResponse
        self._reasoning: list[str] = []  # 聚合推理内容
        self._usage: TokenUsage | None = None
        self._finish_raw: str | None = None
        self._response_id: str | None = None
        self._response_model: str | None = None
        self._extensions: dict[str, Any] = {}
        self._exhausted = False  # 上游 SSE 已读完
        self._done = False  # 终态事件已入队，不再读取上游
        self._closed = False
        #: 是否已向调用方输出过内容；用于判定失败后能否安全重试
        self.emitted_output = False

    def __aiter__(self) -> OpenAICompatibleStream:
        return self

    async def __anext__(self) -> StreamEvent:
        # 队列为空时持续读取上游 chunk，直到产生至少一个事件或流结束
        while not self._pending:
            if self._done:
                raise StopAsyncIteration
            if self._exhausted:
                # 上游结束后补齐终态：未结束的工具调用、usage、message_end
                self._finish()
                continue
            try:
                chunk = await self._raw.__anext__()
            except StopAsyncIteration:
                self._exhausted = True
                continue
            except errors.ProviderError:
                raise
            except Exception as exc:
                raise self._adapter.map_error(exc, self._request) from exc
            self._handle_chunk(chunk)
        event = self._pending.popleft()
        if event.type in ("text_delta", "reasoning_delta", "tool_call_start"):
            self.emitted_output = True
        return event

    async def aclose(self) -> None:
        """幂等关闭：释放底层 HTTP 连接。"""
        if self._closed:
            return
        self._closed = True
        self._done = True
        close = getattr(self._raw, "close", None)
        if close is not None:
            await close()

    # -- chunk 处理 ---------------------------------------------------------------------
    def _handle_chunk(self, chunk: Any) -> None:
        if self._response_id is None:
            self._response_id = getattr(chunk, "id", None)
        if getattr(chunk, "model", None):
            self._response_model = chunk.model
        self._extensions.update(self._adapter.provider_extensions(chunk))
        # 顶层 usage：DeepSeek / Qwen / Kimi(新) 在最后一个 choices 为空的 chunk 中返回，
        # GLM 在 chunk 顶层返回；后到的值覆盖先到的值
        usage = self._adapter.parse_usage(getattr(chunk, "usage", None))
        if usage is not None:
            self._usage = usage
        for choice in getattr(chunk, "choices", None) or []:
            # 只处理第一个候选（n=1）
            if getattr(choice, "index", 0) not in (0, None):
                continue
            # Moonshot 旧版把 usage 放在最后一个 choice 内部，作为兼容兜底
            choice_usage = self._adapter.parse_usage(_extra(choice, "usage"))
            if choice_usage is not None:
                self._usage = choice_usage
            delta = getattr(choice, "delta", None)
            if delta is not None:
                # 同一个 chunk 内按 推理 → 文本 → 工具调用 的顺序产生事件
                reasoning = _extra(delta, "reasoning_content")
                if reasoning:
                    self._reasoning.append(reasoning)
                    self._pending.append(ReasoningDeltaEvent(text=reasoning))
                if delta.content:
                    self._text.append(delta.content)
                    self._pending.append(TextDeltaEvent(text=delta.content))
                for tc in delta.tool_calls or []:
                    self._handle_tool_delta(tc)
            if choice.finish_reason:
                # 收到 finish_reason 即结束所有工具调用；usage 可能在之后的 chunk 才到
                self._finish_raw = choice.finish_reason
                self._end_tool_calls()

    def _handle_tool_delta(self, tc: Any) -> None:
        """处理一个工具调用增量片段。

        OpenAI 协议中工具调用按 index 分片传输：首个片段通常带 id 和 name，
        后续片段只带 arguments。不同 index 的片段可能交错到达。
        """
        index = tc.index if tc.index is not None else len(self._tools)
        builder = self._tools.get(index)
        fn = tc.function
        if builder is None:
            builder = _ToolCallBuilder(index=index, id=tc.id or _gen_tool_call_id(index))
            self._tools[index] = builder
        elif builder.ended:
            return  # finish_reason 之后迟到的片段：忽略，保证 start/end 一一对应
        elif tc.id and not builder.started:
            builder.id = tc.id
        if fn is not None and fn.name and not builder.name:
            builder.name = fn.name
        args = (fn.arguments if fn is not None else None) or ""
        if not builder.started:
            # 还不知道函数名时先缓存参数片段，拿到名字后再发 start + 缓存的参数
            if args:
                builder.arguments.append(args)
            if builder.name:
                self._start_tool(builder)
            return
        if args:
            builder.arguments.append(args)
            self._pending.append(ToolCallDeltaEvent(index=index, arguments_delta=args))

    def _start_tool(self, builder: _ToolCallBuilder) -> None:
        builder.started = True
        self._pending.append(
            ToolCallStartEvent(index=builder.index, id=builder.id, name=builder.name)
        )
        # 把 start 之前缓存的参数作为一个 delta 补发
        buffered = "".join(builder.arguments)
        if buffered:
            self._pending.append(ToolCallDeltaEvent(index=builder.index, arguments_delta=buffered))

    def _end_tool_calls(self) -> None:
        """按 index 顺序结束所有未结束的工具调用（保证每个 start 都有对应 end）。"""
        for index in sorted(self._tools):
            builder = self._tools[index]
            if builder.ended:
                continue
            if not builder.started:
                self._start_tool(builder)
            builder.ended = True
            self._pending.append(ToolCallEndEvent(index=index, tool_call=builder.tool_call()))

    def _finish(self) -> None:
        """上游流结束：入队 usage 和 message_end 终态事件，并组装完整响应。"""
        # 上游未发 finish_reason 就结束时，也要补齐 tool_call_end
        self._end_tool_calls()
        # Provider 未返回 usage 时标记为 unknown，绝不伪造
        usage = self._usage or TokenUsage.unknown()
        tool_calls = [self._tools[i].tool_call() for i in sorted(self._tools)] or None
        text = "".join(self._text)
        message = Message(
            role="assistant",
            content=text if (text or not tool_calls) else None,
            tool_calls=tool_calls,
            reasoning_content="".join(self._reasoning) or None,
        )
        finish = self._adapter.normalize_finish_reason(self._finish_raw)
        response = ChatResponse(
            id=self._response_id,
            request_id=self._request.context.request_id,
            provider=self._adapter.name,
            model=self._request.model,
            response_model=self._response_model,
            message=message,
            finish_reason=finish,
            raw_finish_reason=self._finish_raw,
            usage=usage,
            provider_extensions=self._extensions,
        )
        self._pending.append(UsageEvent(usage=usage))
        self._pending.append(MessageEndEvent(finish_reason=finish, response=response))
        self._done = True
