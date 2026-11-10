"""工具调用的统一错误模型。

工具层面的失败（找不到工具、参数错误、权限不足、超时、工具抛出异常、输出超限……）都用
:class:`ToolError` 的子类表示，并由执行器转换成 ``status="error"`` 的
:class:`~agent_tools.protocol.invocation.ToolExecutionResult`，**不会**抛给调用方。

``safe_message`` 是可以安全展示给模型或客户端的摘要：不包含参数内容、密钥或原始异常信息；
原始异常保存在 ``__cause__`` 中，只用于本地排查。

调用方用错 API（例如执行器已关闭、注册了非法的工具）属于编程错误，使用
:class:`ExecutorClosedError`、:class:`RegistryError` 等直接抛出，不转换成结果。

继承关系::

    ToolError
    ├── ToolNotFoundError / ToolDisabledError / ToolVersionNotFoundError   (not_found)
    ├── InvalidArgumentsError                                              (invalid_arguments)
    ├── PermissionDeniedError                                              (permission)
    ├── ApprovalRequiredError / ApprovalRejectedError (P3)                 (permission)
    ├── ConcurrencyLimitedError / RateLimitedError                         (throttled)
    ├── CircuitOpenError (P3)                                              (circuit_open)
    ├── ToolTimeoutError                                                   (timeout)
    ├── ToolCancelledError                                                 (cancelled)
    ├── UpstreamError                                                      (upstream)
    ├── ToolExecutionError                                                 (execution)
    ├── OutputLimitError / OutputValidationError                           (output)
    └── ToolInfraError                                                     (internal)
"""

from __future__ import annotations

from typing import ClassVar

from .invocation import ToolErrorInfo

__all__ = [
    "ApprovalRejectedError",
    "ApprovalRequiredError",
    "CircuitOpenError",
    "ConcurrencyLimitedError",
    "ExecutorClosedError",
    "InvalidArgumentsError",
    "OutputLimitError",
    "OutputValidationError",
    "PermissionDeniedError",
    "RateLimitedError",
    "RegistryConflictError",
    "RegistryError",
    "ToolCancelledError",
    "ToolDisabledError",
    "ToolError",
    "ToolExecutionError",
    "ToolInfraError",
    "ToolNotFoundError",
    "ToolTimeoutError",
    "ToolVersionNotFoundError",
    "UpstreamError",
]


class ToolError(Exception):
    """所有工具层错误的基类。

    Attributes:
        safe_message: 脱敏后的可读摘要，可以展示给模型或客户端。
        code: 稳定的、机器可读的错误码（如 ``attempt_timeout``）。
        retryable: 原样重试同一个调用是否可能成功。执行器只有在它为 True **且**工具满足
            幂等条件时才会自动重试（见 ``ToolBinding.retry_permitted``）。
        retry_after: 下游或限流器建议的最早重试间隔（秒）；``None`` 表示没有建议。
    """

    #: 低基数的错误类型，用于指标标签和 Span 的 ``error.type``。
    error_type: ClassVar[str] = "internal"
    default_code: ClassVar[str] = "tool_error"
    default_retryable: ClassVar[bool] = False

    def __init__(
        self,
        safe_message: str,
        *,
        code: str | None = None,
        retryable: bool | None = None,
        retry_after: float | None = None,
    ) -> None:
        """创建一个工具层错误。

        Args:
            safe_message: 可以安全展示的摘要；调用方必须保证其中不含参数内容或密钥。
            code: 错误码；不传时使用子类的 ``default_code``。
            retryable: 是否可重试；不传时使用子类的 ``default_retryable``。
            retry_after: 建议的最早重试间隔（秒），例如来自 HTTP ``Retry-After``。
        """
        super().__init__(safe_message)
        self.safe_message = safe_message
        self.code = code or self.default_code
        self.retryable = self.default_retryable if retryable is None else retryable
        self.retry_after = retry_after

    def to_info(self) -> ToolErrorInfo:
        """转换成可以放进结果、返回给模型的错误信息。

        Returns:
            只包含类型、错误码、安全摘要和是否可重试的 :class:`ToolErrorInfo`。
        """
        return ToolErrorInfo(
            type=self.error_type,
            code=self.code,
            message=self.safe_message,
            retryable=self.retryable,
        )

    def __repr__(self) -> str:
        """返回便于调试的表示，只包含安全字段。"""
        return f"{type(self).__name__}(code={self.code!r}, message={self.safe_message!r})"


class ToolNotFoundError(ToolError):
    """工具名、别名不存在，或者指定的快照已经失效。"""

    error_type = "not_found"
    default_code = "tool_not_found"


class ToolDisabledError(ToolError):
    """工具存在但已被停用。"""

    error_type = "not_found"
    default_code = "tool_disabled"


class ToolVersionNotFoundError(ToolError):
    """宿主显式指定的版本不存在。"""

    error_type = "not_found"
    default_code = "tool_version_not_found"


class InvalidArgumentsError(ToolError):
    """参数过大、不是合法的 JSON 对象，或者没有通过参数校验。"""

    error_type = "invalid_arguments"
    default_code = "invalid_arguments"


class PermissionDeniedError(ToolError):
    """调用方没有权限使用该工具。"""

    error_type = "permission"
    default_code = "permission_denied"


class ApprovalRequiredError(ToolError):
    """工具需要人工审批，但没有得到有效的批准（P3）。

    错误码：``approval_unavailable``（没有配置审批处理器，安全失败）、``approval_timeout``
    （等待审批超时）、``approval_failed``（审批处理器出错）。这些情况下工具**都没有执行**。
    """

    error_type = "permission"
    default_code = "approval_required"


class ApprovalRejectedError(ToolError):
    """审批人明确拒绝了这次调用（P3）；工具没有执行。拒绝理由只进审计，不返回给模型。"""

    error_type = "permission"
    default_code = "approval_rejected"


class CircuitOpenError(ToolError):
    """熔断器处于开启（或半开且探测名额已满）状态，本次调用被快速拒绝，工具没有执行（P3）。

    ``retry_after`` 给出距离允许下一次探测的大致秒数。执行器**不会**因为这个错误自动重试。
    """

    error_type = "circuit_open"
    default_code = "circuit_open"
    default_retryable = True


class ConcurrencyLimitedError(ToolError):
    """在截止时间内拿不到并发额度（全局、单工具或线程池）。"""

    error_type = "throttled"
    default_code = "concurrency_limited"
    default_retryable = True


class RateLimitedError(ToolError):
    """调用频率超过了工具（按租户）的速率限制。``retry_after`` 给出下一个令牌的大致等待时间。"""

    error_type = "throttled"
    default_code = "rate_limited"
    default_retryable = True


class ToolTimeoutError(ToolError):
    """单次尝试超时（``attempt_timeout``，可重试）或整体截止时间已到（``deadline_exceeded``）。"""

    error_type = "timeout"
    default_code = "attempt_timeout"
    default_retryable = True


class ToolCancelledError(ToolError):
    """由基础设施一侧发起的取消，例如执行器关闭（``executor_closed``）。

    注意：调用方自己的任务被取消时，执行器会原样抛出 ``asyncio.CancelledError``，
    **不会**使用这个类型。
    """

    error_type = "cancelled"
    default_code = "cancelled"


class UpstreamError(ToolError):
    """下游服务（HTTP 等）返回了失败状态码，或者传输层出错。

    Attributes:
        status_code: 下游的 HTTP 状态码；传输层错误时为 ``None``。
    """

    error_type = "upstream"
    default_code = "upstream_error"

    def __init__(
        self,
        safe_message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        retryable: bool | None = None,
        retry_after: float | None = None,
    ) -> None:
        """创建下游错误。

        Args:
            safe_message: 可以安全展示的摘要；**不得**包含下游响应体（可能含有敏感数据）。
            status_code: 下游 HTTP 状态码。
            code: 错误码。
            retryable: 是否可重试。
            retry_after: 下游建议的重试间隔（秒）。
        """
        super().__init__(safe_message, code=code, retryable=retryable, retry_after=retry_after)
        self.status_code = status_code


class ToolExecutionError(ToolError):
    """工具代码自身执行失败（抛出了非 :class:`ToolError` 的异常、返回了不支持的类型等）。"""

    error_type = "execution"
    default_code = "tool_execution_failed"


class OutputLimitError(ToolError):
    """工具输出超过了大小限制，且该类型的内容不能安全截断（JSON、图片、合计超限）。"""

    error_type = "output"
    default_code = "output_too_large"


class OutputValidationError(ToolError):
    """工具输出没有通过声明的 ``output_schema`` 校验（在任何截断之前、对完整数据校验）。"""

    error_type = "output"
    default_code = "output_invalid"


class ToolInfraError(ToolError):
    """基础设施自身的意外错误；``safe_message`` 固定为通用文案，细节只写本地日志。"""

    error_type = "internal"
    default_code = "internal_error"


# ---------------------------------------------------------------------------------
# 调用方用错 API 时直接抛出的异常（不会被转换成结果）
# ---------------------------------------------------------------------------------
class RegistryError(ValueError):
    """注册表操作非法，例如注册了不满足安全基线的工具、删除当前启用的版本。"""


class RegistryConflictError(RegistryError):
    """名称、版本或模型侧别名与已注册的工具冲突。"""


class ExecutorClosedError(RuntimeError):
    """执行器已经关闭后仍然发起调用。"""
