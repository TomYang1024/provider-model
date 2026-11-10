"""JSON Schema 校验（需要 ``agent-tools[http]`` extra 提供的 ``jsonschema``）。

约定（与架构文档一致）：

* 方言固定为 **Draft 2020-12**；``$schema`` 只能省略或声明为 2020-12；
* 只允许**本地** ``$ref`` / ``$dynamicRef``（以 ``#`` 开头），注册时检查它们都能解析；
  校验器使用一个拒绝任何检索的 ``referencing.Registry``，**绝不会联网加载远程 Schema**；
* ``format`` 只作为注释，不做断言（不传 ``format_checker``）；
* Schema 本身有大小（64 KiB）和嵌套深度（32）上限，防止恶意或失控的 Schema 拖垮校验；
* 校验错误只报告**路径和校验关键字**，不包含实例值（参数可能含有用户的敏感信息）。

``jsonschema`` 在函数内部延迟导入，所以没有安装 ``[http]`` 时导入本模块不会失败，
只有真正编译 Schema 时才会给出安装提示。
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any

from ..protocol.errors import RegistryError

__all__ = ["DIALECT", "CompiledSchema", "compile_schema"]

#: 唯一支持的方言。
DIALECT = "https://json-schema.org/draft/2020-12/schema"
_MAX_SCHEMA_BYTES = 64 * 1024
_MAX_DEPTH = 32
_MAX_REPORTED_ERRORS = 5
_REF_KEYS = ("$ref", "$dynamicRef")


class CompiledSchema:
    """已编译、可重复使用的校验器（线程安全，可在多个调用间共享）。"""

    def __init__(self, validator: Any) -> None:
        """包装一个 ``jsonschema`` 校验器。

        Args:
            validator: ``jsonschema.Draft202012Validator`` 实例。
        """
        self._validator = validator

    def errors(self, instance: Any) -> list[str]:
        """校验数据并返回不含实例值的错误摘要。

        Args:
            instance: 待校验的数据。

        Returns:
            错误摘要列表，最多 5 条（多出的用 ``(+N more)`` 表示）；通过校验时为空列表。
        """
        found = sorted(self._validator.iter_errors(instance), key=lambda e: list(e.absolute_path))
        items: list[str] = []
        for err in found[:_MAX_REPORTED_ERRORS]:
            path = "/".join(str(p) for p in err.absolute_path) or "<root>"
            detail = str(err.validator)
            if err.validator == "required" and isinstance(err.instance, dict):
                # 缺失的属性名来自 Schema（宿主定义），不是用户数据，可以报告
                missing = [n for n in err.validator_value if n not in err.instance]
                detail = f"required {','.join(map(str, missing))}"
            items.append(f"{path}: {detail}")
        if len(found) > _MAX_REPORTED_ERRORS:
            items.append(f"(+{len(found) - _MAX_REPORTED_ERRORS} more)")
        return items


def compile_schema(schema: Mapping[str, Any]) -> CompiledSchema:
    """检查并编译一个 JSON Schema。

    Args:
        schema: 宿主提供的 Schema。

    Returns:
        已编译的校验器。

    Raises:
        RegistryError: 未安装 ``[http]`` extra；Schema 过大、嵌套过深、方言不是 2020-12、
            含有非本地或无法解析的 ``$ref``，或者 Schema 本身不合法。
    """
    try:
        import jsonschema
        import referencing
        import referencing.exceptions
        import referencing.jsonschema
    except ImportError as exc:  # pragma: no cover - 由打包测试覆盖
        raise RegistryError(
            "JSON Schema validation requires the 'http' extra: pip install 'agent-tools[http]'"
        ) from exc

    raw = json.dumps(schema, ensure_ascii=False, separators=(",", ":"))
    if len(raw.encode("utf-8")) > _MAX_SCHEMA_BYTES:
        raise RegistryError(f"schema is larger than {_MAX_SCHEMA_BYTES} bytes")
    if _depth(schema) > _MAX_DEPTH:
        raise RegistryError(f"schema is nested deeper than {_MAX_DEPTH} levels")
    declared = schema.get("$schema")
    if declared is not None and declared.rstrip("#") != DIALECT:
        raise RegistryError(f"only JSON Schema Draft 2020-12 is supported (got {declared!r})")

    try:
        jsonschema.Draft202012Validator.check_schema(schema)
    except jsonschema.SchemaError as exc:
        raise RegistryError(f"invalid JSON Schema: {exc.message}") from None

    def deny(uri: str) -> referencing.Resource[Any]:
        """拒绝检索任何外部 Schema（防止注册或校验时访问网络）。

        Args:
            uri: 请求检索的地址。

        Raises:
            referencing.exceptions.NoSuchResource: 总是抛出。
        """
        # referencing 用 attrs 生成构造函数，mypy（未启用 attrs 插件）看不到这些参数
        raise referencing.exceptions.NoSuchResource(ref=uri)  # type: ignore[call-arg]

    registry: referencing.Registry[Any] = referencing.Registry(retrieve=deny)  # type: ignore[call-arg]
    resource = referencing.jsonschema.DRAFT202012.create_resource(dict(schema))
    resolver = registry.resolver_with_root(resource)
    for ref in _refs(schema):
        if not ref.startswith("#"):
            raise RegistryError(f"only local $ref values are allowed (got {ref[:64]!r})")
        try:
            resolver.lookup(ref)
        except referencing.exceptions.Unresolvable:
            raise RegistryError(f"unresolvable $ref {ref[:64]!r}") from None

    validator = jsonschema.Draft202012Validator(dict(schema), registry=registry)
    return CompiledSchema(validator)


def _refs(node: Any) -> Iterator[str]:
    """遍历 Schema 中所有 ``$ref`` / ``$dynamicRef`` 的值。

    Args:
        node: Schema 或其中的一部分。

    Yields:
        引用字符串。
    """
    if isinstance(node, Mapping):
        for key, value in node.items():
            if key in _REF_KEYS and isinstance(value, str):
                yield value
            else:
                yield from _refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from _refs(item)


def _depth(node: Any, level: int = 0) -> int:
    """计算 Schema 的最大嵌套深度；超过上限后立即返回，避免深度递归。

    Args:
        node: Schema 或其中的一部分。
        level: 当前深度。

    Returns:
        最大深度。
    """
    if level > _MAX_DEPTH:
        return level
    if isinstance(node, Mapping):
        return max((_depth(v, level + 1) for v in node.values()), default=level)
    if isinstance(node, list):
        return max((_depth(v, level + 1) for v in node), default=level)
    return level
