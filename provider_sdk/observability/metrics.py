"""模型调用的 OpenTelemetry 指标。

只使用低基数属性。规范中要求的标签与属性键的对应关系：

=============  ===========================================================
规范标签       属性键
=============  ===========================================================
provider       ``app.llm.provider``（注册名）+ ``gen_ai.provider.name``
model          ``gen_ai.request.model``（未登记的模型 ID -> ``_other``）
operation      ``app.llm.operation``（chat/messages/stream）+ ``gen_ai.operation.name``
status         ``app.llm.status``（success/error/timeout/cancelled/closed_early）
error_type     ``error.type``
=============  ===========================================================

绝不使用 trace_id、request_id、user_id 或 Prompt 文本作为属性。
"""

from __future__ import annotations

from collections.abc import Mapping

from opentelemetry import metrics

from . import semconv as sc

# 语义规范推荐的 gen_ai.client.operation.duration 分桶边界（秒）
_DURATION_BUCKETS = [
    0.01,
    0.02,
    0.04,
    0.08,
    0.16,
    0.32,
    0.64,
    1.28,
    2.56,
    5.12,
    10.24,
    20.48,
    40.96,
    81.92,
    163.84,
    327.68,
]
# 首 Token 时间分桶（秒）：覆盖从几十毫秒到长时间推理
_TTFT_BUCKETS = [0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 7.5, 10, 15, 30, 60]
# Token 数量分桶：按 4 倍递增，覆盖到百万级上下文
_TOKEN_BUCKETS = [1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304]

Attributes = Mapping[str, str]


class SDKMetrics:
    """SDK 使用的全部指标；Meter 为 no-op 时所有记录操作都是空操作。"""

    def __init__(self, meter: metrics.Meter) -> None:
        self.duration = meter.create_histogram(
            sc.METRIC_OPERATION_DURATION,
            unit="s",
            description="GenAI operation duration",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
        )
        self.ttft = meter.create_histogram(
            sc.METRIC_TIME_TO_FIRST_CHUNK,
            unit="s",
            description="Time from request start to the first content chunk of a stream",
            explicit_bucket_boundaries_advisory=_TTFT_BUCKETS,
        )
        self.tokens = meter.create_histogram(
            sc.METRIC_TOKEN_USAGE,
            unit="{token}",
            description="Provider reported input and output tokens",
            explicit_bucket_boundaries_advisory=_TOKEN_BUCKETS,
        )
        self.requests = meter.create_counter(
            "app.llm.requests", unit="{request}", description="Model calls by outcome"
        )
        self.errors = meter.create_counter(
            "app.llm.errors", unit="{error}", description="Failed model calls by error type"
        )
        self.timeouts = meter.create_counter(
            "app.llm.timeouts", unit="{timeout}", description="Model calls that timed out"
        )
        self.active_streams = meter.create_up_down_counter(
            "app.llm.active_streams", unit="{stream}", description="Currently open streams"
        )

    def record_call(
        self,
        base: Attributes,
        *,
        status: str,
        error_type: str | None,
        duration_s: float,
        input_tokens: int | None,
        output_tokens: int | None,
    ) -> None:
        """记录一次调用结束：耗时、请求数、错误 / 超时计数和 Token 用量。"""
        attrs = {**base, sc.APP_STATUS: status}
        if error_type:
            attrs[sc.ERROR_TYPE] = error_type
        self.duration.record(duration_s, attrs)
        self.requests.add(1, attrs)
        if status in ("error", "timeout"):
            self.errors.add(1, attrs)
        if status == "timeout":
            self.timeouts.add(1, attrs)
        # Token 直方图按 gen_ai.token.type 区分输入 / 输出（不带 status 标签，符合语义规范）
        if input_tokens is not None:
            self.tokens.record(input_tokens, {**base, sc.GEN_AI_TOKEN_TYPE: "input"})
        if output_tokens is not None:
            self.tokens.record(output_tokens, {**base, sc.GEN_AI_TOKEN_TYPE: "output"})

    def record_ttft(self, base: Attributes, seconds: float) -> None:
        self.ttft.record(seconds, base)

    # 活跃流数量：开启时 +1，结束时 -1，两次调用必须使用相同的标签
    def stream_opened(self, base: Attributes) -> None:
        self.active_streams.add(1, base)

    def stream_closed(self, base: Attributes) -> None:
        self.active_streams.add(-1, base)
