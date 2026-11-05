"""统一异常体系。

SDK 在模型调用中抛出的所有错误都是 :class:`ProviderError`。原始厂商异常绝不会直接
抛给调用方，而是保存在 ``__cause__``（异常链）中用于排查；``str(error)`` 只包含
经过脱敏的摘要，可以安全地写入日志或返回给客户端。

``TimeoutError`` 和 ``ConnectionError`` 按规范要求命名，它们*仅在本模块内*遮蔽了
Python 内置异常；请通过限定名导入（``from provider_sdk import errors``），
或使用别名 ``ProviderTimeoutError`` / ``ProviderConnectionError``。

继承关系::

    ProviderError
    ├── AuthenticationError
    ├── RateLimitError
    ├── TimeoutError
    ├── ConnectionError
    ├── InvalidRequestError
    │   ├── ProviderNotFoundError
    │   ├── ContextLengthExceededError
    │   └── UnsupportedCapabilityError
    ├── ProviderServerError
    └── StreamClosedError
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "AuthenticationError",
    "ConnectionError",
    "ContextLengthExceededError",
    "InvalidRequestError",
    "ProviderConnectionError",
    "ProviderError",
    "ProviderNotFoundError",
    "ProviderServerError",
    "ProviderTimeoutError",
    "RateLimitError",
    "StreamClosedError",
    "TimeoutError",
    "UnsupportedCapabilityError",
]


class ProviderError(Exception):
    """所有 SDK 异常的基类。

    Attributes:
        provider: Provider 注册名（``deepseek``、``glm`` 等）。
        model: 请求的模型 ID。
        error_code: 稳定的、机器可读的错误码（如 ``rate_limited``）。
        retryable: 原样重试*同一个*请求是否可能成功。
        status_code: 上游 HTTP 状态码（如果有）。
        safe_message: 脱敏后的可读摘要（不含密钥和 Prompt）。
    """

    default_code = "provider_error"
    default_retryable = False
    #: 网关返回该异常时使用的 HTTP 状态码。
    http_status = 502

    def __init__(
        self,
        safe_message: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        error_code: str | None = None,
        retryable: bool | None = None,
        status_code: int | None = None,
        provider_request_id: str | None = None,
    ) -> None:
        super().__init__(safe_message)
        self.safe_message = safe_message
        self.provider = provider
        self.model = model
        self.error_code = error_code or self.default_code
        self.retryable = self.default_retryable if retryable is None else retryable
        self.status_code = status_code
        self.provider_request_id = provider_request_id

    @property
    def error_type(self) -> str:
        """低基数的错误类型，用于指标标签和 Span 的 ``error.type``。"""
        return _ERROR_TYPES.get(type(self), "provider_error")

    def to_dict(self) -> dict[str, Any]:
        """可安全返回给客户端的错误结构（网关错误响应 / SSE error 帧使用）。"""
        return {
            "type": self.error_type,
            "code": self.error_code,
            "message": self.safe_message,
            "provider": self.provider,
            "model": self.model,
            "retryable": self.retryable,
            "status_code": self.status_code,
        }

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(provider={self.provider!r}, model={self.model!r}, "
            f"error_code={self.error_code!r}, retryable={self.retryable!r}, "
            f"message={self.safe_message!r})"
        )


class AuthenticationError(ProviderError):
    default_code = "authentication_failed"
    http_status = 502  # 我们访问 Provider 的凭证失效，属于上游问题，不应向客户端返回 401


class RateLimitError(ProviderError):
    default_code = "rate_limited"
    default_retryable = True
    http_status = 429


class TimeoutError(ProviderError):  # noqa: A001 - name mandated by the spec
    default_code = "timeout"
    default_retryable = True
    http_status = 504


class ConnectionError(ProviderError):  # noqa: A001 - name mandated by the spec
    default_code = "connection_failed"
    default_retryable = True
    http_status = 502


class InvalidRequestError(ProviderError):
    default_code = "invalid_request"
    http_status = 400


class ProviderNotFoundError(InvalidRequestError):
    default_code = "provider_not_found"
    http_status = 404


class ContextLengthExceededError(InvalidRequestError):
    default_code = "context_length_exceeded"
    http_status = 400


class UnsupportedCapabilityError(InvalidRequestError):
    default_code = "unsupported_capability"
    http_status = 400


class ProviderServerError(ProviderError):
    default_code = "provider_server_error"
    default_retryable = True
    http_status = 502


class StreamClosedError(ProviderError):
    """在已关闭的流上继续操作时抛出。"""

    default_code = "stream_closed"
    http_status = 500


# 不与内置异常重名的别名，推荐在业务代码中使用
ProviderTimeoutError = TimeoutError
ProviderConnectionError = ConnectionError

# 异常类 -> 指标中的 error.type 取值（固定集合，保证低基数）
_ERROR_TYPES: dict[type[ProviderError], str] = {
    ProviderError: "provider_error",
    AuthenticationError: "authentication",
    RateLimitError: "rate_limit",
    TimeoutError: "timeout",
    ConnectionError: "connection",
    InvalidRequestError: "invalid_request",
    ProviderNotFoundError: "invalid_request",
    ContextLengthExceededError: "context_length_exceeded",
    UnsupportedCapabilityError: "unsupported_capability",
    ProviderServerError: "server_error",
    StreamClosedError: "stream_closed",
}
