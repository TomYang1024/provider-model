"""工具描述的数据契约：语义标注、访问与治理策略、输出限制、工具规格、HTTP 目标。"""

from __future__ import annotations

import re
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

__all__ = [
    "ALIAS_PATTERN",
    "NAME_PATTERN",
    "ApprovalPolicy",
    "ArgLocation",
    "CachePolicy",
    "DedupePolicy",
    "HttpMethod",
    "HttpTarget",
    "McpTarget",
    "OutputLimits",
    "RateLimit",
    "RetryPolicy",
    "SecretRef",
    "ToolAnnotations",
    "ToolDescriptor",
    "ToolKind",
    "ToolListing",
    "ToolPolicy",
    "ToolSpec",
]

#: 内部名：小写开头，可以带命名空间（``.``），最长 128（兼容 MCP 的名字长度）。
NAME_PATTERN = r"^[a-z0-9][a-z0-9_.\-]{0,127}$"
#: 模型侧别名：必须满足 Provider SDK ``ToolDefinition.name`` 的规则（不允许 ``.``，最长 64）。
ALIAS_PATTERN = r"^[A-Za-z0-9_\-]{1,64}$"

#: 工具类型：``python``（P0）、``http``（P1）、``mcp``（P2）。
ToolKind = Literal["python", "http", "mcp"]
#: HTTP 工具支持的方法。
HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
#: 参数在 HTTP 请求中的位置。参数不能映射到请求头，防止模型注入请求头。
ArgLocation = Literal["path", "query", "body"]

_HEADER_NAME_RE = re.compile(r"^[A-Za-z0-9!#$%&'*+.^_`|~\-]{1,64}$")
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
#: 由适配器自己管理、不允许在静态请求头里设置的头（不区分大小写）。
RESERVED_HEADERS = frozenset(
    {"host", "content-length", "transfer-encoding", "connection", "idempotency-key", "cookie"}
)


class _Frozen(BaseModel):
    """契约模型的公共配置：禁止未知字段、不可变。"""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ToolAnnotations(_Frozen):
    """工具的语义标注。只有被执行器规则引用的标注才会产生效果。"""

    #: 不修改外部状态（P0 只用于展示，不影响执行规则）。
    read_only: bool = False
    #: 可以和同一批里相邻的其他并发安全调用一起并发执行。与 ``read_only`` 是两回事。
    concurrency_safe: bool = False
    #: 用相同参数重复调用效果相同（P1 重试的前提，P0 不使用）。
    idempotent: bool = False
    #: 不可逆操作。只有声明了 ``policy.approval``（P3 人工审批）时才能注册；否则需要注册表显式
    #: 设置 ``allow_destructive_without_approval=True`` 并自行承担风险。
    destructive: bool = False
    #: 会访问外部系统（P0 只用于展示）。
    open_world: bool = False


class OutputLimits(_Frozen):
    """按内容类型区分的输出大小限制（字节）。"""

    #: 所有文本和 JSON 块合计（图片单独限制）。
    max_total_bytes: int = Field(default=256 * 1024, gt=0)
    #: 单个文本块；超出时在 UTF-8 字符边界处截断。
    max_text_bytes: int = Field(default=64 * 1024, gt=0)
    #: 单个 JSON 块序列化后的大小，以及 ``structured`` 的大小；超出时报错。
    max_json_bytes: int = Field(default=128 * 1024, gt=0)
    #: 单张图片解码后的大小；超出时报错。
    max_image_bytes: int = Field(default=2 * 1024 * 1024, gt=0)
    max_images: int = Field(default=4, ge=0)


class RetryPolicy(_Frozen):
    """重试策略。默认 ``max_attempts=1``，即不重试。

    即使配置了多次尝试，执行器也只在“错误可重试 **且** 工具满足幂等条件”时重试，
    并且所有尝试与退避等待都不能超过 ``ToolContext.deadline``。
    """

    #: 最多尝试次数（含第一次）；1 表示不重试。
    max_attempts: int = Field(default=1, ge=1, le=10)
    #: 第 n 次重试前的基础等待：``base_delay_s * 2**(n-1)``，再乘以 [0.5, 1.0] 的随机抖动。
    base_delay_s: float = Field(default=0.2, ge=0, le=60)
    #: 单次退避等待的上限（秒）。
    max_delay_s: float = Field(default=5.0, ge=0, le=300)
    #: 下游 ``Retry-After`` 超过这个值时放弃重试，直接返回错误。
    max_retry_after_s: float = Field(default=30.0, ge=0, le=3600)


class RateLimit(_Frozen):
    """速率限制：按（工具名，租户）分别计数的令牌桶。

    每次尝试（包括重试）消耗一个令牌。没有令牌时：如果下一个令牌在 ``max_wait_s`` 内、
    且不超过截止时间，就等待；否则立即返回 ``RateLimitedError``。
    """

    #: 每 ``per_s`` 秒补充的令牌数。
    requests: int = Field(gt=0)
    per_s: float = Field(gt=0)
    #: 令牌桶容量（允许的突发数）；不填时等于 ``requests``。
    burst: int | None = Field(default=None, gt=0)
    #: 没有令牌时最多等待多少秒；默认 0，表示直接失败。
    max_wait_s: float = Field(default=0.0, ge=0, le=60)


class ApprovalPolicy(_Frozen):
    """人工审批策略（P3）。在 ``ToolPolicy.approval`` 中声明即表示**每次调用**都必须先获批。

    审批发生在鉴权与参数校验之后、获取并发额度与执行之前（等待审批时不占用执行额度）；
    执行器上没有配置审批处理器时调用直接失败（``approval_unavailable``），不会绕过审批。
    """

    #: 等待审批的最长时间（秒）；实际等待还受 ``ToolContext.deadline`` 约束。
    timeout_s: float = Field(default=300.0, gt=0, le=3600)
    #: 交给审批处理器的参数视图：``none`` 只给大小和哈希；``redacted``（默认）给经过已知密钥
    #: 精确清除、凭证格式与个人信息规则脱敏的参数预览（尽力而为）；``full`` 给原始参数。
    arguments: Literal["none", "redacted", "full"] = "redacted"


class CachePolicy(_Frozen):
    """结果缓存策略（P3）：在 TTL 内对“同一身份、同一工具版本、相同参数”的调用复用成功结果。

    缓存只是性能优化：命中时工具不会再次执行，但鉴权、参数校验每次都会重新进行。它**不是**
    防重复执行或幂等机制，所以只允许用于 ``read_only=True`` 且非不可逆、无需审批的工具
    （注册时检查）。只有执行器配置了 ``result_store`` 时才生效，否则照常执行。
    """

    #: 结果的有效期（秒）。
    ttl_s: float = Field(gt=0, le=86400)
    #: 共享范围：``principal``（默认）按“租户 + 用户 + 权限集合”隔离；``tenant`` 在同一租户、
    #: 相同权限集合的用户之间共享。缺少对应身份字段（用户或租户为空）的调用不使用缓存。
    scope: Literal["principal", "tenant"] = "principal"


class DedupePolicy(_Frozen):
    """调用去重策略（P3）：防止**同一逻辑调用**被重复执行。

    同一逻辑调用指“租户 + 用户 + request_id + call_id + 工具版本 + 参数”都相同的调用，例如宿主
    重放了同一轮的工具调用。窗口期内重复提交时返回第一次的结果（包括失败结果，只要工具已经
    开始执行），并发的重复提交等待第一次完成。

    这只是进程内（或宿主提供的共享存储内）的尽力去重：它**不能**替代下游幂等，也不是事务——
    工具执行完成到结果写入存储之间进程崩溃，重放时仍会再次执行。需要执行器配置
    ``result_store``；未配置时调用安全失败（``dedupe_unavailable``），不会在没有去重的情况下执行。
    """

    #: 记录保留时间（秒），在这段时间内的重复提交会被识别。
    window_s: float = Field(default=3600.0, gt=0, le=7 * 86400)


class ToolPolicy(_Frozen):
    """访问与治理策略。``access`` 必填：工具的访问方式必须显式声明。"""

    #: ``scoped``：调用方必须拥有 ``required_scopes`` 中的全部权限；``public``：任何调用方可用。
    access: Literal["scoped", "public"]
    required_scopes: frozenset[str] = frozenset()
    #: 单次尝试的超时上限（秒），实际超时还会受 ``ToolContext.deadline`` 约束。
    timeout_s: float = Field(default=30.0, gt=0, le=3600)
    #: 该工具在进程内的并发上限；``None`` 表示只受全局上限约束。
    max_concurrency: int | None = Field(default=None, gt=0)
    #: 原始参数的 UTF-8 字节数上限。
    max_input_bytes: int = Field(default=64 * 1024, gt=0)
    output_limits: OutputLimits = Field(default_factory=OutputLimits)
    #: 重试策略（P1）；默认不重试。
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    #: 速率限制（P1）；``None`` 表示不限制。
    rate_limit: RateLimit | None = None
    #: 人工审批（P3）；``None`` 表示不需要审批。
    approval: ApprovalPolicy | None = None
    #: 结果缓存（P3）；``None`` 表示不缓存。
    cache: CachePolicy | None = None
    #: 调用去重（P3）；``None`` 表示不去重。
    dedupe: DedupePolicy | None = None

    @model_validator(mode="after")
    def _check_access(self) -> ToolPolicy:
        """校验访问声明与权限集合的组合，以及缓存、去重、审批之间的组合是否一致。

        Raises:
            ValueError: ``scoped`` 却没有声明权限，或 ``public`` 却声明了权限（容易误以为受保护）；
                同时声明了缓存与去重；需要审批的工具声明了缓存（缓存命中会跳过审批语义）。
        """
        if self.access == "scoped" and not self.required_scopes:
            raise ValueError("access='scoped' requires non-empty required_scopes")
        if self.access == "public" and self.required_scopes:
            raise ValueError("access='public' must not declare required_scopes")
        if self.cache is not None and self.dedupe is not None:
            raise ValueError("cache and dedupe are mutually exclusive")
        if self.cache is not None and self.approval is not None:
            raise ValueError("tools that require approval must not be cached")
        return self


class ToolSpec(_Frozen):
    """工具规格：可序列化、可展示给模型的部分。运行时对象放在 ``ToolBinding`` 中。"""

    name: str = Field(pattern=NAME_PATTERN)
    #: 不透明的版本字符串；P0 不支持语义化版本范围。
    version: str = Field(default="1", pattern=r"^[A-Za-z0-9._\-]{1,32}$")
    description: str = Field(default="", max_length=4096)
    #: 参数的 JSON Schema（Draft 2020-12），顶层必须是 ``type: object``。
    input_schema: dict[str, Any]
    #: 输出的 JSON Schema（P1，可选）：对结构化输出在任何截断之前校验。
    output_schema: dict[str, Any] | None = None
    kind: ToolKind
    annotations: ToolAnnotations = Field(default_factory=ToolAnnotations)
    policy: ToolPolicy
    #: 显式指定的模型侧名字；不填时由注册表按确定性规则生成。
    alias: str | None = Field(default=None, pattern=ALIAS_PATTERN)

    @field_validator("input_schema")
    @classmethod
    def _object_schema(cls, value: dict[str, Any]) -> dict[str, Any]:
        """校验参数 Schema 的顶层是对象类型。

        Args:
            value: 待校验的 JSON Schema。

        Returns:
            原样返回通过校验的 Schema。

        Raises:
            ValueError: 顶层 ``type`` 不是 ``object``。
        """
        if value.get("type") != "object":
            raise ValueError("input_schema must be a JSON Schema with type 'object'")
        return value

    @model_validator(mode="after")
    def _check_cache_semantics(self) -> ToolSpec:
        """结果缓存只能用于只读、非不可逆的工具。

        缓存命中时工具不会执行，对有副作用的工具这会悄悄跳过副作用，且容易被误当成幂等保证。

        Raises:
            ValueError: 声明了 ``policy.cache``，但工具不是 ``read_only`` 或者是 ``destructive``。
        """
        if self.policy.cache is not None and (
            not self.annotations.read_only or self.annotations.destructive
        ):
            raise ValueError("policy.cache requires annotations.read_only=True and not destructive")
        return self


class ToolDescriptor(_Frozen):
    """展示给模型（经由宿主）的工具描述：使用模型侧别名，只包含启用的版本。"""

    alias: str
    name: str
    version: str
    description: str
    input_schema: dict[str, Any]
    annotations: ToolAnnotations


class ToolListing(_Frozen):
    """某个注册表快照上、对某个调用方可见的工具列表。

    ``snapshot_id`` 可以传给 ``execute_many(..., snapshot_id=...)``，保证执行时使用
    与列表相同的工具版本。
    """

    snapshot_id: str
    tools: list[ToolDescriptor]


class SecretRef(_Frozen):
    """对密钥的引用。工具规格里只保存引用，真正的值在调用时由 ``SecretProvider`` 提供，
    不会出现在规格、日志、Span 或审计事件里。
    """

    #: 密钥名，交给 ``SecretProvider`` 查找（可以按租户返回不同的值）。
    name: str = Field(pattern=r"^[A-Za-z0-9_.:\-]{1,128}$")
    #: 放进哪个请求头。
    header: str = Field(default="Authorization", pattern=_HEADER_NAME_RE.pattern)
    #: 值的前缀，例如 ``Bearer``；``None`` 表示直接使用密钥值。
    scheme: str | None = Field(default="Bearer", pattern=r"^[A-Za-z0-9_\-]{1,32}$")

    @field_validator("header")
    @classmethod
    def _not_reserved(cls, value: str) -> str:
        """禁止把密钥放进由适配器管理的保留请求头。

        Args:
            value: 请求头名。

        Returns:
            原样返回通过校验的请求头名。

        Raises:
            ValueError: 是保留请求头。
        """
        if value.lower() in RESERVED_HEADERS:
            raise ValueError(f"header {value!r} is reserved")
        return value


class HttpTarget(_Frozen):
    """HTTP 工具的执行目标（可序列化，注册时固定）。

    安全约定：协议、主机和端口都在注册时固定，模型只能提供参数；参数只能映射到路径、
    查询字符串或请求体，不能映射到请求头或主机。
    """

    method: HttpMethod
    #: 注册时固定的基础地址；只允许 ``https``（``http`` 需要适配器显式允许），
    #: 不能带用户信息、查询字符串或片段。
    base_url: str = Field(max_length=2048)
    #: 路径模板，例如 ``/customers/{customer_id}``；占位符必须是映射到 ``path`` 的参数。
    path_template: str = Field(default="", max_length=1024)
    #: 参数名 → 位置。未列出的参数：GET/DELETE 放进查询字符串，其余方法放进 JSON 请求体。
    arg_mapping: dict[str, ArgLocation] = Field(default_factory=dict)
    #: 静态请求头（注册时固定，不能包含密钥，密钥用 ``auth``）。
    headers: dict[str, str] = Field(default_factory=dict, max_length=32)
    auth: SecretRef | None = None
    #: 视为成功的状态码。
    success_status: frozenset[int] = frozenset({200, 201, 202, 204})
    #: 响应体大小上限（字节），流式读取时超出即中断。
    max_response_bytes: int = Field(default=1024 * 1024, gt=0, le=64 * 1024 * 1024)
    #: 下游**明确支持** ``Idempotency-Key`` 去重时才设为 True；发送这个头本身不能保证幂等。
    idempotency_header: bool = False

    @field_validator("base_url")
    @classmethod
    def _check_base_url(cls, value: str) -> str:
        """校验基础地址的形式（主机白名单在适配器中按部署配置检查）。

        Args:
            value: 基础地址。

        Returns:
            去掉末尾 ``/`` 的基础地址。

        Raises:
            ValueError: 协议不是 http/https，缺少主机，或包含用户信息、查询字符串、片段。
        """
        parts = urlsplit(value)
        if parts.scheme not in ("http", "https"):
            raise ValueError("base_url must use http or https")
        if not parts.hostname:
            raise ValueError("base_url must contain a host")
        if parts.username or parts.password:
            raise ValueError("base_url must not contain credentials")
        if parts.query or parts.fragment:
            raise ValueError("base_url must not contain a query or fragment")
        return value.rstrip("/")

    @field_validator("headers")
    @classmethod
    def _check_headers(cls, value: dict[str, str]) -> dict[str, str]:
        """校验静态请求头：名字合法、值里没有换行、不覆盖保留请求头。

        Args:
            value: 请求头。

        Returns:
            原样返回通过校验的请求头。

        Raises:
            ValueError: 名字非法、值包含 CR/LF，或使用了保留请求头。
        """
        for name, header_value in value.items():
            if not _HEADER_NAME_RE.match(name) or name.lower() in RESERVED_HEADERS:
                raise ValueError(f"header {name!r} is not allowed")
            if "\r" in header_value or "\n" in header_value:
                raise ValueError(f"header {name!r} contains a line break")
        return value

    @model_validator(mode="after")
    def _check_template(self) -> HttpTarget:
        """校验路径模板与参数映射一致，并且鉴权头不与静态请求头冲突。

        Raises:
            ValueError: 模板占位符没有映射到 ``path``，``path`` 参数不在模板中，
                模板不是以 ``/`` 开头，或静态请求头与 ``auth.header`` 冲突。
        """
        if self.path_template and not self.path_template.startswith("/"):
            raise ValueError("path_template must start with '/'")
        placeholders = set(_PLACEHOLDER_RE.findall(self.path_template))
        path_args = {k for k, v in self.arg_mapping.items() if v == "path"}
        if placeholders != path_args:
            raise ValueError("path_template placeholders must equal the arguments mapped to 'path'")
        if self.auth is not None and self.auth.header.lower() in {h.lower() for h in self.headers}:
            raise ValueError("static headers must not override the auth header")
        return self

    def default_location(self) -> ArgLocation:
        """返回未在 ``arg_mapping`` 中列出的参数的默认位置。

        Returns:
            GET/DELETE 为 ``query``，其余方法为 ``body``。
        """
        return "query" if self.method in ("GET", "DELETE") else "body"


class McpTarget(_Frozen):
    """MCP 工具的执行目标（可序列化）：哪个服务端上的哪个远端工具。

    服务端的连接方式（命令、地址、凭证）是部署配置，放在 ``McpServerManager`` 中，
    不出现在工具规格里。
    """

    #: 宿主配置的服务端 ID。
    server_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_\-]{0,31}$")
    #: 远端原始工具名（MCP 允许 ``.``，最长 128）。
    remote_name: str = Field(pattern=r"^[A-Za-z0-9._\-]{1,128}$")
