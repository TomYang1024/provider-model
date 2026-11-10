"""重试条件与退避计算。

默认不重试（``RetryPolicy.max_attempts=1``）。只有同时满足以下条件才会重试：

1. 还没有达到 ``max_attempts``；
2. 错误本身可重试（``ToolError.retryable``）；
3. 工具允许重试（``ToolBinding.retry_permitted``：声明了幂等；HTTP POST/PATCH 还要求
   下游支持 ``Idempotency-Key``）；
4. 退避等待之后，距离整体截止时间还剩至少 ``MIN_ATTEMPT_S`` 秒（由执行器检查）。

本地速率限制（``RateLimitedError``）不会触发重试：限流器已经按 ``max_wait_s`` 等待过，
再重试只会重复等待。熔断拒绝（``CircuitOpenError``，P3）同样不会触发重试：熔断开启正是
为了停止向下游发送请求。
"""

from __future__ import annotations

import random
from collections.abc import Callable

from ..binding import ToolBinding
from ..protocol.errors import CircuitOpenError, RateLimitedError, ToolError
from ..protocol.spec import RetryPolicy

__all__ = ["MIN_ATTEMPT_S", "backoff_delay", "should_retry"]

#: 重试前，距离截止时间至少还要剩下的秒数；不够时放弃重试，返回最后一次的错误。
MIN_ATTEMPT_S = 0.05


def should_retry(error: ToolError, binding: ToolBinding, attempt: int) -> bool:
    """判断一次失败的尝试之后是否应该重试（不含截止时间检查）。

    本地限流与熔断拒绝（P3）不重试：它们说明“现在不应该再发请求”，而不是偶发失败。

    Args:
        error: 本次尝试的错误。
        binding: 工具绑定。
        attempt: 刚刚结束的尝试序号（从 1 开始）。

    Returns:
        应该重试时为 True。
    """
    policy = binding.spec.policy.retry
    if attempt >= policy.max_attempts or not error.retryable:
        return False
    if isinstance(error, RateLimitedError | CircuitOpenError):
        return False
    return binding.retry_permitted()


def backoff_delay(
    policy: RetryPolicy,
    retry_number: int,
    retry_after: float | None,
    *,
    rng: Callable[[], float] = random.random,
) -> float | None:
    """计算第 ``retry_number`` 次重试前的等待时间。

    指数退避：``min(max_delay_s, base_delay_s * 2**(n-1))``，再乘以 [0.5, 1.0] 的随机抖动，
    避免大量调用同时重试。下游给出 ``Retry-After`` 时取两者中较大的值；``Retry-After``
    超过 ``max_retry_after_s`` 时放弃重试。

    Args:
        policy: 重试策略。
        retry_number: 第几次重试（从 1 开始）。
        retry_after: 下游建议的等待时间（秒）。
        rng: 返回 [0, 1) 随机数的函数（测试可以注入）。

    Returns:
        等待秒数；``None`` 表示应该放弃重试。
    """
    if retry_after is not None and retry_after > policy.max_retry_after_s:
        return None
    exponential = min(policy.max_delay_s, policy.base_delay_s * 2.0 ** (retry_number - 1))
    delay = exponential * (0.5 + 0.5 * rng())
    if retry_after is not None:
        delay = max(delay, retry_after)
    return delay
