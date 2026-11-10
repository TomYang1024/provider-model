"""HTTP 工具适配器的测试：参数映射、状态码、响应限制、网络安全边界、密钥与幂等键。

网络使用内存后端（``FakeNetwork``），它被包在真实的 ``GuardedNetworkBackend`` 里，
所以 DNS 检查、连接地址、TLS 主机名都走的是生产代码路径；只有最终的套接字是假的。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest

from agent_tools import (
    ExecutorConfig,
    HttpTarget,
    MappingSecretProvider,
    NetworkPolicy,
    Principal,
    SecretRef,
    ToolContext,
    ToolExecutionResult,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.adapters.http import HttpAdapter, HttpAdapterConfig, build_request

PUBLIC = ToolPolicy(access="public")
PUBLIC_IP = "93.184.216.34"
HOST = "api.example.com"
SECRET = "s3cr3t-token-value-123"


def schema(**props: Any) -> dict[str, Any]:
    """构造顶层禁止额外属性的对象 Schema。

    Args:
        **props: 属性名 → 属性 Schema；名字以 ``_`` 结尾的视为可选（去掉下划线）。

    Returns:
        JSON Schema。
    """
    properties = {k.rstrip("_"): v for k, v in props.items()}
    required = [k for k in props if not k.endswith("_")]
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def make_resolver(answers: dict[str, list[list[str]] | list[str]]) -> Callable[..., Any]:
    """构造假的 DNS 解析器；同一主机可以给出按次序变化的结果（模拟 DNS 重绑定）。

    Args:
        answers: 主机 → IP 列表，或“每次解析的 IP 列表”的列表。

    Returns:
        异步解析函数，并带有记录调用次数的 ``calls`` 属性。
    """
    calls: list[str] = []

    async def resolve(host: str, port: int) -> list[str]:
        """返回预设的解析结果。"""
        calls.append(host)
        value = answers[host]
        if value and isinstance(value[0], list):
            seq: list[list[str]] = value  # type: ignore[assignment]
            return seq[min(calls.count(host), len(seq)) - 1]
        return value  # type: ignore[return-value]

    resolve.calls = calls  # type: ignore[attr-defined]
    return resolve


def setup(
    fake_network: Any,
    target: HttpTarget,
    input_schema: dict[str, Any],
    *,
    policy: ToolPolicy = PUBLIC,
    resolver: Callable[..., Any] | None = None,
    network: NetworkPolicy | None = None,
    secrets: dict[str, str] | None = None,
    **executor_kwargs: Any,
) -> ToolExecutor:
    """注册一个 HTTP 工具，并创建连接到内存网络的执行器。

    Args:
        fake_network: 内存网络。
        target: HTTP 目标。
        input_schema: 参数 Schema。
        policy: 工具策略。
        resolver: DNS 解析器；默认把 ``api.example.com`` 解析到公网地址。
        network: 网络策略；默认只允许 ``api.example.com``。
        secrets: 密钥（名字 → 值）。
        **executor_kwargs: 传给执行器的其他参数。

    Returns:
        执行器。
    """
    registry = ToolRegistry()
    registry.register_http(target, name="api.call", input_schema=input_schema, policy=policy)
    adapter = HttpAdapter(
        HttpAdapterConfig(network=network or NetworkPolicy(allowed_hosts=frozenset({HOST}))),
        secrets=MappingSecretProvider(secrets) if secrets is not None else None,
        resolver=resolver or make_resolver({HOST: [PUBLIC_IP]}),
        network_backend=fake_network,
    )
    return ToolExecutor(registry, adapters={"http": adapter}, **executor_kwargs)


async def call(executor: ToolExecutor, args: dict[str, Any], **ctx: Any) -> ToolExecutionResult:
    """执行一次 ``api_call`` 调用并关闭执行器。

    Args:
        executor: 执行器。
        args: 参数。
        **ctx: ``ToolContext`` 字段。

    Returns:
        调用结果。
    """
    async with executor:
        return await executor.execute(
            ToolInvocation(call_id="c1", tool="api_call", arguments=args), ToolContext(**ctx)
        )


# ---------------------------------------------------------------------------------
# 参数映射
# ---------------------------------------------------------------------------------
async def test_get_maps_path_and_query_and_connects_to_validated_ip(
    fake_network: Any, make_response: Any
) -> None:
    """GET：路径参数被完整编码，其余参数进查询字符串；TCP 连接校验过的 IP，TLS 仍用原始主机名。"""
    target = HttpTarget(
        method="GET",
        base_url=f"https://{HOST}/v1",
        path_template="/customers/{cid}",
        arg_mapping={"cid": "path"},
        headers={"X-Client": "agent"},
    )
    fake_network.queue(make_response(200, b'{"name": "Ann"}'))
    executor = setup(
        fake_network,
        target,
        schema(
            cid={"type": "string"},
            tags_={"type": "array", "items": {"type": "string"}},
            active_={"type": "boolean"},
        ),
    )
    result = await call(executor, {"cid": "a/b c?#", "tags": ["x", "y"], "active": True})
    assert result.ok, result.error
    assert result.structured == {"name": "Ann"}
    method, path, headers, body = fake_network.request()
    assert method == "GET"
    assert path == "/v1/customers/a%2Fb%20c%3F%23?tags=x&tags=y&active=true"
    assert headers["host"] == HOST
    assert headers["x-client"] == "agent"
    assert body == b""
    assert fake_network.connections == [(PUBLIC_IP, 443)]
    assert fake_network.tls_hostnames == [HOST]


async def test_post_sends_json_body(fake_network: Any, make_response: Any) -> None:
    """POST：未映射的参数进入 JSON 请求体；文本响应成为文本块。"""
    target = HttpTarget(method="POST", base_url=f"https://{HOST}", path_template="/notes")
    fake_network.queue(
        make_response(201, "已保存".encode(), content_type="text/plain; charset=utf-8")
    )
    executor = setup(fake_network, target, schema(text={"type": "string"}, n_={"type": "integer"}))
    result = await call(executor, {"text": "你好", "n": 2})
    assert result.to_model_text() == "已保存"
    method, path, headers, body = fake_network.request()
    assert (method, path) == ("POST", "/notes")
    assert headers["content-type"] == "application/json"
    assert json.loads(body) == {"text": "你好", "n": 2}


@pytest.mark.parametrize("value", [".", "..", ""])
def test_path_traversal_values_are_rejected(value: str) -> None:
    """路径参数为空、``.``、``..`` 时拒绝，参数不能改变路径结构。"""
    target = HttpTarget(
        method="GET",
        base_url=f"https://{HOST}/v1",
        path_template="/f/{p}",
        arg_mapping={"p": "path"},
    )
    from agent_tools import InvalidArgumentsError

    with pytest.raises(InvalidArgumentsError):
        build_request(target, {"p": value})


async def test_schema_rejects_undeclared_arguments_without_echoing_values(
    fake_network: Any,
) -> None:
    """HTTP 工具用 JSON Schema 校验参数；未声明的参数被拒绝，错误中不出现参数值，也不会发出请求。"""
    target = HttpTarget(method="GET", base_url=f"https://{HOST}")
    executor = setup(fake_network, target, schema(q={"type": "string"}))
    result = await call(executor, {"q": "ok", "token": "LEAK-ME"})
    assert result.error and result.error.code == "arguments_invalid"
    assert "LEAK-ME" not in result.error.message and "token" not in result.error.message
    assert fake_network.connections == []


# ---------------------------------------------------------------------------------
# 状态码与响应
# ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (404, "upstream_not_found", False),
        (403, "upstream_auth_failed", False),
        (400, "upstream_client_error", False),
        (429, "upstream_rate_limited", True),
        (503, "upstream_server_error", True),
        (501, "upstream_server_error", False),
        (302, "redirect_not_followed", False),
    ],
)
async def test_status_mapping(
    fake_network: Any, make_response: Any, status: int, code: str, retryable: bool
) -> None:
    """失败状态码映射成统一的 upstream 错误；3xx 不会被跟随；错误信息不含响应体。"""
    target = HttpTarget(method="GET", base_url=f"https://{HOST}")
    fake_network.queue(
        make_response(
            status,
            b'{"internal": "stack trace with DB password"}',
            headers={"Location": "http://10.0.0.1/"},
        )
    )
    result = await call(setup(fake_network, target, schema()), {})
    assert result.error and (result.error.type, result.error.code, result.error.retryable) == (
        "upstream",
        code,
        retryable,
    )
    assert "password" not in result.error.message
    assert len(fake_network.connections) == 1  # 没有跟随重定向


async def test_response_size_limits(fake_network: Any, make_response: Any) -> None:
    """声明的 Content-Length 过大时直接拒绝；分块响应在读取过程中超限时中断。"""
    target = HttpTarget(method="GET", base_url=f"https://{HOST}", max_response_bytes=100)
    fake_network.queue(make_response(200, b"x" * 500, content_type="text/plain"))
    fake_network.queue(make_response(200, b"y" * 5000, content_type="text/plain", chunked=True))
    executor = setup(fake_network, target, schema())
    async with executor:
        for _ in range(2):
            result = await executor.execute(
                ToolInvocation(call_id="c", tool="api_call"), ToolContext()
            )
            assert result.error and (result.error.type, result.error.code) == (
                "output",
                "output_too_large",
            )


@pytest.mark.parametrize(
    ("content_type", "body", "expected"),
    [
        ("image/png", b"\x89PNG", "image"),
        ("application/octet-stream", b"\x00\x01", "unsupported_content_type"),
        ("application/json", b"{not json", "upstream_invalid_json"),
    ],
)
async def test_response_content_types(
    fake_network: Any, make_response: Any, content_type: str, body: bytes, expected: str
) -> None:
    """图片转成图片块；任意二进制和非法 JSON 报错，不会把原始字节交给模型。"""
    target = HttpTarget(method="GET", base_url=f"https://{HOST}")
    fake_network.queue(make_response(200, body, content_type=content_type))
    result = await call(setup(fake_network, target, schema()), {})
    if expected == "image":
        assert result.ok and result.content[0].type == "image"
    else:
        assert result.error and result.error.code == expected


async def test_transport_error_is_retryable_upstream_error(fake_network: Any) -> None:
    """连接失败映射为可重试的 connection_failed。"""
    fake_network.fail_connect = True
    target = HttpTarget(method="GET", base_url=f"https://{HOST}")
    result = await call(setup(fake_network, target, schema()), {})
    assert result.error and (result.error.code, result.error.retryable) == (
        "connection_failed",
        True,
    )


# ---------------------------------------------------------------------------------
# 网络安全边界
# ---------------------------------------------------------------------------------
async def test_host_not_in_allowlist_is_blocked_before_any_connection(fake_network: Any) -> None:
    """注册的主机不在部署白名单中：拒绝，且不发生任何 DNS 解析和连接。"""
    resolver = make_resolver({"other.example.com": [PUBLIC_IP]})
    target = HttpTarget(method="GET", base_url="https://other.example.com")
    result = await call(setup(fake_network, target, schema(), resolver=resolver), {})
    assert result.error and (result.error.type, result.error.code) == (
        "permission",
        "destination_not_allowed",
    )
    assert resolver.calls == [] and fake_network.connections == []  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "answers",
    [
        ["10.0.0.5"],
        ["127.0.0.1"],
        ["169.254.169.254"],
        ["::ffff:127.0.0.1"],
        [PUBLIC_IP, "10.0.0.5"],  # 混入一个内网地址：整体拒绝，而不是只连接“允许的那个”
    ],
)
async def test_private_resolutions_are_blocked(fake_network: Any, answers: list[str]) -> None:
    """解析到内网、回环、元数据或混合地址时拒绝，错误信息中不包含解析出的 IP。"""
    target = HttpTarget(method="GET", base_url=f"https://{HOST}")
    result = await call(
        setup(fake_network, target, schema(), resolver=make_resolver({HOST: answers})), {}
    )
    assert result.error and result.error.code == "destination_not_allowed"
    assert "10.0.0.5" not in result.error.message
    assert fake_network.connections == []


async def test_dns_rebinding_between_connections_is_blocked(
    fake_network: Any, make_response: Any
) -> None:
    """每次新建连接都重新解析并检查：第一次解析到公网、第二次解析到内网时，第二次被拒绝。"""
    resolver = make_resolver({HOST: [[PUBLIC_IP], ["127.0.0.1"]]})
    target = HttpTarget(method="GET", base_url=f"https://{HOST}")
    fake_network.queue(make_response(200, b"{}"))
    executor = setup(fake_network, target, schema(), resolver=resolver)
    async with executor:
        first = await executor.execute(ToolInvocation(call_id="a", tool="api_call"), ToolContext())
        second = await executor.execute(ToolInvocation(call_id="b", tool="api_call"), ToolContext())
    assert first.ok
    assert second.error and second.error.code == "destination_not_allowed"
    assert fake_network.connections == [(PUBLIC_IP, 443)]


async def test_explicitly_allowed_private_network(fake_network: Any, make_response: Any) -> None:
    """宿主显式放行的内网网段可以访问。"""
    fake_network.queue(make_response(200, b"{}"))
    target = HttpTarget(method="GET", base_url="https://svc.internal")
    network = NetworkPolicy(
        allowed_hosts=frozenset({"svc.internal"}), allowed_networks=("10.1.0.0/16",)
    )
    result = await call(
        setup(
            fake_network,
            target,
            schema(),
            network=network,
            resolver=make_resolver({"svc.internal": ["10.1.2.3"]}),
        ),
        {},
    )
    assert result.ok
    assert fake_network.connections == [("10.1.2.3", 443)]


async def test_plain_http_and_unlisted_ports_are_blocked_by_default(fake_network: Any) -> None:
    """默认只允许 https 与 443 端口。"""
    for base in (f"http://{HOST}", f"https://{HOST}:8443"):
        result = await call(
            setup(fake_network, HttpTarget(method="GET", base_url=base), schema()), {}
        )
        assert result.error and result.error.code == "destination_not_allowed", base
    assert fake_network.connections == []


async def test_proxy_mode_uses_preflight_and_blocks_private_targets(fake_network: Any) -> None:
    """代理模式：只能做发送前预检；解析到内网时在发送前拒绝。"""
    registry = ToolRegistry()
    registry.register_http(
        HttpTarget(method="GET", base_url=f"https://{HOST}"),
        name="api.call",
        input_schema=schema(),
        policy=PUBLIC,
    )
    adapter = HttpAdapter(
        HttpAdapterConfig(
            network=NetworkPolicy(allowed_hosts=frozenset({HOST})),
            proxy="http://proxy.example.com:3128",
        ),
        resolver=make_resolver({HOST: ["10.0.0.9"]}),
    )
    assert adapter.guard_mode == "preflight"
    result = await call(ToolExecutor(registry, adapters={"http": adapter}), {})
    assert result.error and result.error.code == "destination_not_allowed"


# ---------------------------------------------------------------------------------
# 密钥与幂等键
# ---------------------------------------------------------------------------------
async def test_secret_is_injected_and_scrubbed_from_result(
    fake_network: Any, make_response: Any, telemetry: Any
) -> None:
    """密钥通过 SecretProvider 注入请求头；下游把它回显到响应里时，结果和日志中都被替换。"""
    target = HttpTarget(method="GET", base_url=f"https://{HOST}", auth=SecretRef(name="crm"))
    echo = json.dumps({"echo": f"Bearer {SECRET}", "raw": SECRET}).encode()
    fake_network.queue(make_response(200, echo))
    executor = setup(
        fake_network,
        target,
        schema(),
        secrets={"crm": SECRET},
        logger=telemetry.logs,
        tracer=telemetry.tracer_provider.get_tracer("t"),
        config=ExecutorConfig(log_mode="full"),
    )
    result = await call(executor, {})
    _, _, headers, _ = fake_network.request()
    assert headers["authorization"] == f"Bearer {SECRET}"
    assert SECRET not in result.to_model_text()
    assert result.structured == {"echo": "[REDACTED_SECRET]", "raw": "[REDACTED_SECRET]"}
    assert SECRET not in repr(telemetry.logs.records)
    for span in telemetry.finished_spans():
        assert SECRET not in repr(dict(span.attributes or {}))


async def test_missing_secret_or_provider_fails_without_request(fake_network: Any) -> None:
    """取不到密钥或没有配置密钥提供者时，不发出请求。"""
    target = HttpTarget(method="GET", base_url=f"https://{HOST}", auth=SecretRef(name="crm"))
    options: list[dict[str, str] | None] = [{}, None]
    for secrets in options:
        result = await call(setup(fake_network, target, schema(), secrets=secrets), {})
        assert result.error and result.error.code == "secret_unavailable"
    assert fake_network.connections == []


async def test_idempotency_key_only_sent_when_downstream_supports_it(
    fake_network: Any, make_response: Any
) -> None:
    """只有 idempotency_header=True 时才发送 Idempotency-Key；同一请求、同一调用的键稳定，不同请求不同。"""
    plain = HttpTarget(method="POST", base_url=f"https://{HOST}")
    keyed = HttpTarget(method="POST", base_url=f"https://{HOST}", idempotency_header=True)
    fake_network.queue(*(make_response(200, b"{}") for _ in range(3)))
    await call(setup(fake_network, plain, schema()), {}, request_id="r1")
    await call(setup(fake_network, keyed, schema()), {}, request_id="r1")
    await call(setup(fake_network, keyed, schema()), {}, request_id="r2")
    assert "idempotency-key" not in fake_network.request(0)[2]
    key_r1 = fake_network.request(1)[2]["idempotency-key"]
    key_r2 = fake_network.request(2)[2]["idempotency-key"]
    assert len(key_r1) == 64 and key_r1 != key_r2


async def test_tenant_specific_secrets(fake_network: Any, make_response: Any) -> None:
    """SecretProvider 收到调用上下文，可以按租户返回不同的密钥。"""

    class TenantSecrets:
        """按租户返回密钥的提供者。"""

        async def get_secret(self, name: str, ctx: ToolContext) -> str:
            """返回租户专属密钥。"""
            return f"key-for-{ctx.principal.tenant_id}"

    registry = ToolRegistry()
    registry.register_http(
        HttpTarget(method="GET", base_url=f"https://{HOST}", auth=SecretRef(name="k", scheme=None)),
        name="api.call",
        input_schema=schema(),
        policy=PUBLIC,
    )
    adapter = HttpAdapter(
        HttpAdapterConfig(network=NetworkPolicy(allowed_hosts=frozenset({HOST}))),
        secrets=TenantSecrets(),
        resolver=make_resolver({HOST: [PUBLIC_IP]}),
        network_backend=fake_network,
    )
    fake_network.queue(make_response(200, b"{}"))
    await call(
        ToolExecutor(registry, adapters={"http": adapter}),
        {},
        principal=Principal(tenant_id="acme"),
    )
    assert fake_network.request()[2]["authorization"] == "key-for-acme"


def test_http_target_rejects_unsafe_configuration() -> None:
    """目标配置在注册时拒绝：非 http(s)、带凭证或查询的基础地址、请求头注入、保留请求头、模板不一致。"""
    import pydantic

    bad: list[dict[str, Any]] = [
        {"base_url": "ftp://x.example.com"},
        {"base_url": "https://u:p@x.example.com"},
        {"base_url": "https://x.example.com/?a=1"},
        {"base_url": "https://x.example.com", "headers": {"X": "a\r\nInjected: 1"}},
        {"base_url": "https://x.example.com", "headers": {"Host": "evil"}},
        {"base_url": "https://x.example.com", "path_template": "/a/{b}"},
    ]
    for fields in bad:
        with pytest.raises(pydantic.ValidationError):
            HttpTarget.model_validate({"method": "GET", **fields})
