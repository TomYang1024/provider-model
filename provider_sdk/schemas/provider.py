"""Provider 配置与模型能力声明模型。"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr

Verification = Literal["documented", "tested", "unverified"]


class ModelCapabilities(BaseModel):
    """模型支持哪些能力；在发出任何 HTTP 请求之前用于校验请求。

    ``verification`` 记录这些能力值的来源：``documented``（来自 Provider 公开文档）、
    ``tested``（集成测试在真实 API 上验证过）或 ``unverified``（未验证的假设）。
    """

    model_config = ConfigDict(extra="forbid")

    streaming: bool = True  # 流式输出
    tools: bool = True  # 工具调用
    tool_choice_required: bool = True  # 强制工具选择（required / 指定函数）
    parallel_tool_calls: bool = True  # 并行工具调用
    reasoning: bool = False  # 支持推理 / 思考
    #: 该模型始终思考（无法通过请求关闭）。
    reasoning_always_on: bool = False
    vision: bool = False  # 图片输入
    json_mode: bool = True  # response_format=json_object
    max_context_tokens: int | None = None
    verification: Verification = "unverified"
    notes: str | None = None


class TimeoutConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: TCP / TLS 建连超时（秒）。
    connect: float = Field(default=10.0, gt=0)
    #: 两次收到数据之间的最长间隔（秒）；限制流内部的停顿时间。
    read: float = Field(default=120.0, gt=0)
    write: float = Field(default=30.0, gt=0)
    #: 等待连接池空闲连接的超时（秒）。
    pool: float = Field(default=10.0, gt=0)
    #: 非流式调用的默认总截止时间（秒）。
    total: float = Field(default=300.0, gt=0)
    #: 流式调用的默认总截止时间（秒）。
    stream_total: float = Field(default=900.0, gt=0)


class ProviderConfig(BaseModel):
    """注册表中一个 Provider 的配置。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z0-9][a-z0-9_\-]{0,63}$")
    #: 注册表中的适配器类型键（``deepseek``、``openai_compatible`` 等）。
    adapter: str
    enabled: bool = True
    api_key: SecretStr | None = None
    base_url: str
    #: OpenTelemetry ``gen_ai.provider.name`` 的取值。
    otel_provider_name: str | None = None
    #: 已登记模型 -> 能力声明。为空表示“接受任意模型 ID”。
    models: dict[str, ModelCapabilities] = Field(default_factory=dict)
    #: 未在 ``models`` 中登记的模型所使用的默认能力。
    default_capabilities: ModelCapabilities = Field(default_factory=ModelCapabilities)
    #: 拒绝未在 ``models`` 中登记的模型 ID。
    strict_models: bool = False
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)
    #: 在产生*任何输出之前*发生错误时的自动重试次数（有上限）。
    max_retries: int = Field(default=2, ge=0, le=5)
    max_connections: int = Field(default=100, gt=0)
    max_keepalive_connections: int = Field(default=20, ge=0)
    keepalive_expiry: float = Field(default=30.0, gt=0)
    #: 该 Provider 的显式出口代理地址（优先于环境变量）。
    proxy: str | None = None
    #: 未设置 ``proxy`` 时，是否遵循 HTTPS_PROXY / NO_PROXY（以及系统代理设置）。
    trust_env: bool = True
    #: 额外的固定 HTTP 请求头（绝不写入日志）。
    extra_headers: dict[str, str] = Field(default_factory=dict)
    #: 默认的 ``provider_options``，请求级参数会覆盖它。
    default_options: dict[str, Any] = Field(default_factory=dict)
    #: 设置后，只接受这些 ``provider_options`` 键（白名单）。
    allowed_options: list[str] | None = None
    #: 是否向 Provider 请求注入 W3C ``traceparent``
    #: （默认关闭：不向第三方泄露内部 Trace ID）。
    propagate_trace_context: bool = False
    #: 是否通过 ``stream_options.include_usage`` 请求流式 Usage。
    stream_usage_option: bool = True
    #: 是否在历史消息中把 assistant 的 ``reasoning_content`` 回传给 Provider。
    send_reasoning_content: bool = False
    #: 适配器特有的自由选项。
    adapter_options: dict[str, Any] = Field(default_factory=dict)

    def capabilities_for(self, model: str) -> ModelCapabilities | None:
        """返回 ``model`` 的能力声明；strict 模式下模型未登记时返回 ``None``。"""
        caps = self.models.get(model)
        if caps is not None:
            return caps
        return None if self.strict_models else self.default_capabilities

    def is_known_model(self, model: str) -> bool:
        return model in self.models
