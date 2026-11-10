"""JSON Schema 校验的测试：方言、本地 $ref、format、错误不含值、注册规则与输出校验。"""

from __future__ import annotations

import json
from typing import Any

import pytest

from agent_tools import (
    HttpTarget,
    NetworkPolicy,
    OutputLimits,
    RegistryError,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.adapters.http import HttpAdapter, HttpAdapterConfig
from agent_tools.security.schema import compile_schema

HOST = "api.example.com"
OBJ: dict[str, Any] = {"type": "object", "properties": {}, "additionalProperties": False}


def test_format_is_annotation_only_and_errors_omit_values() -> None:
    """``format`` 不做断言；错误只含路径和关键字，不含实例值或未声明的属性名。"""
    compiled = compile_schema(
        {
            "type": "object",
            "properties": {
                "email": {"type": "string", "format": "email"},
                "n": {"$ref": "#/$defs/pos"},
            },
            "required": ["email"],
            "additionalProperties": False,
            "$defs": {"pos": {"type": "integer", "minimum": 1}},
        }
    )
    assert compiled.errors({"email": "not-an-email", "n": 3}) == []
    problems = compiled.errors({"n": -5, "SECRET_FIELD": "SECRET_VALUE"})
    text = "; ".join(problems)
    assert "required email" in text and "n: minimum" in text and "additionalProperties" in text
    assert "SECRET" not in text and "-5" not in text


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        (
            {"type": "object", "properties": {"a": {"$ref": "https://evil.example/x.json"}}},
            r"local \$ref",
        ),
        ({"type": "object", "properties": {"a": {"$ref": "#/$defs/missing"}}}, "unresolvable"),
        ({"$schema": "http://json-schema.org/draft-07/schema#", "type": "object"}, "2020-12"),
        ({"type": "object", "minimum": "x"}, "invalid JSON Schema"),
        ({"type": "object", "description": "x" * 70_000}, "larger than"),
    ],
)
def test_unsafe_or_invalid_schemas_are_rejected(schema: dict[str, Any], message: str) -> None:
    """远程 $ref、无法解析的 $ref、其他方言、不合法的 Schema、超大的 Schema 都在编译时拒绝。"""
    with pytest.raises(RegistryError, match=message):
        compile_schema(schema)


def test_deeply_nested_schema_is_rejected() -> None:
    """嵌套过深的 Schema 被拒绝。"""
    node: dict[str, Any] = {"type": "object"}
    for _ in range(40):
        node = {"type": "object", "properties": {"x": node}}
    with pytest.raises(RegistryError, match="deeper"):
        compile_schema(node)


def test_register_http_enforces_schema_rules() -> None:
    """HTTP 工具：顶层必须禁止额外属性；映射的参数必须声明；路径参数必须必填。"""
    registry = ToolRegistry()
    policy = ToolPolicy(access="public")
    url = f"https://{HOST}"
    with pytest.raises(RegistryError, match="additionalProperties"):
        registry.register_http(
            HttpTarget(method="GET", base_url=url),
            name="a",
            input_schema={"type": "object"},
            policy=policy,
        )
    with pytest.raises(RegistryError, match="not declared"):
        registry.register_http(
            HttpTarget(method="GET", base_url=url, arg_mapping={"q": "query"}),
            name="b",
            input_schema=OBJ,
            policy=policy,
        )
    path_target = HttpTarget(
        method="GET", base_url=url, path_template="/{id}", arg_mapping={"id": "path"}
    )
    optional_id = {**OBJ, "properties": {"id": {"type": "string"}}}
    with pytest.raises(RegistryError, match="required"):
        registry.register_http(path_target, name="c", input_schema=optional_id, policy=policy)


async def test_output_schema_validates_full_data_before_truncation(
    fake_network: Any, make_response: Any
) -> None:
    """输出 Schema 在截断之前对完整数据校验：不符合时返回 output_invalid；非 JSON 输出也会被拒绝。"""
    registry = ToolRegistry()
    registry.register_http(
        HttpTarget(method="GET", base_url=f"https://{HOST}"),
        name="api.call",
        input_schema=OBJ,
        output_schema={
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "integer"}},
        },
        policy=ToolPolicy(access="public", output_limits=OutputLimits(max_json_bytes=10_000)),
    )

    async def resolve(host: str, port: int) -> list[str]:
        """解析到公网地址。"""
        return ["93.184.216.34"]

    adapter = HttpAdapter(
        HttpAdapterConfig(network=NetworkPolicy(allowed_hosts=frozenset({HOST}))),
        resolver=resolve,
        network_backend=fake_network,
    )
    fake_network.queue(
        make_response(200, json.dumps({"id": 7}).encode()),
        make_response(200, json.dumps({"id": "seven", "secret": "DO-NOT-ECHO"}).encode()),
        make_response(200, b"plain", content_type="text/plain"),
    )
    async with ToolExecutor(registry, adapters={"http": adapter}) as executor:
        results = [
            await executor.execute(ToolInvocation(call_id=str(i), tool="api_call"), ToolContext())
            for i in range(3)
        ]
    assert results[0].ok and results[0].structured == {"id": 7}
    for bad in results[1:]:
        assert bad.error and (bad.error.type, bad.error.code) == ("output", "output_invalid")
        assert "DO-NOT-ECHO" not in bad.error.message
