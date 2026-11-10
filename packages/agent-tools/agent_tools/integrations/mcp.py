"""MCP 工具的发现与注册（P2，需要 ``agent-tools[mcp]``）。

只有宿主**明确列入白名单**的远端工具才会进入注册表；服务端新出现的工具默认不开放。
每个白名单条目都必须由宿主给出访问策略（``ToolPolicy``）和语义标注：远端声明的
``readOnlyHint`` / ``idempotentHint`` / ``destructiveHint`` 等提示**不会**被采信，
是否可并发、可重试、无副作用、可公开访问，都只由宿主配置决定。

同步规则（:meth:`McpToolSync.sync`）：

1. 分页读取服务端的全部工具；
2. 白名单中的工具逐个检查：名称合法、没有与同批其他工具映射到同一个内部名、描述和
   Schema 通过不可信输入的限制（大小、嵌套深度、Draft 2020-12、只允许本地 ``$ref``）。
   顶层 Schema 会被强制设为 ``additionalProperties: false``，模型不能夹带未声明的参数；
3. 定义没变的工具保持不变；定义变化的工具以新版本号替换；白名单中但远端已不存在、或
   检查不通过的工具，之前注册过的会被移除；
4. 所有变更通过 :meth:`ToolRegistry.apply` **一次性原子生效**。同步器只会替换它自己注册过的
   工具；与其他工具的名称或别名冲突时，整个同步失败并抛出异常，注册表保持同步前的状态，
   不会出现部分刷新。

注意：SDK 在协议层要求每个工具的参数 Schema 都是对象类型；服务端只要有一个工具不满足，
整个 ``tools/list`` 响应都会校验失败，同步随之失败（注册表不变）。

按租户的目录（可选）：``sync(..., scope="tenant", ctx=...)`` 用 ``ctx`` 中租户的会话读取工具
列表，并只注册到**该租户的注册表层**（见 :mod:`agent_tools.registry`）。不同租户可以各自同步，
得到不同的工具集或同名但定义不同的工具；一个租户的同步不会读取、修改或移除其他租户和共享层
的工具。默认 ``scope="shared"`` 的行为与以前相同。

工具名映射：内部名为 ``mcp.<server_id>.<远端名>``（小写化，非法字符替换为 ``_``；超过 128 个
字符时截断并追加哈希）；模型侧别名沿用注册表的确定性规则（不含 ``.``，最长 64）。
宿主可以为单个工具显式指定 ``name`` / ``alias`` 来解决冲突。
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from ..adapters.mcp import McpServerManager
from ..binding import ToolBinding
from ..protocol.context import ToolContext
from ..protocol.errors import PermissionDeniedError, RegistryConflictError, RegistryError
from ..protocol.spec import McpTarget, ToolAnnotations, ToolPolicy, ToolSpec
from ..registry import ToolRegistry
from ..security.schema import compile_schema

__all__ = ["McpSyncReport", "McpSyncScope", "McpToolAllow", "McpToolSync", "mcp_internal_name"]

#: 同步结果写入注册表的哪个作用域：``shared`` 为共享层（所有租户可见，默认）；``tenant`` 为
#: 发现所用 ``ctx`` 中租户的租户层（只有该租户可见）。
McpSyncScope = Literal["shared", "tenant"]

_REMOTE_NAME_RE = re.compile(r"^[A-Za-z0-9._\-]{1,128}$")
_INVALID_NAME_CHARS = re.compile(r"[^a-z0-9_.\-]")
_MAX_REMOTE_DESCRIPTION = 1024


def mcp_internal_name(server_id: str, remote_name: str) -> str:
    """把远端工具名映射成注册表的内部名。

    Args:
        server_id: 服务端 ID。
        remote_name: 远端工具名。

    Returns:
        ``mcp.<server_id>.<远端名>`` 的规范化形式（小写，非法字符替换为 ``_``，最长 128）。
    """
    raw = f"mcp.{server_id}.{remote_name}"
    name = _INVALID_NAME_CHARS.sub("_", raw.lower())
    if len(name) <= 128:
        return name
    digest = hashlib.sha256(raw.encode()).hexdigest()[:8]
    return f"{name[:119]}_{digest}"


@dataclass(frozen=True, slots=True)
class McpToolAllow:
    """白名单条目：宿主明确开放的一个远端工具及其治理策略。

    Attributes:
        remote_name: 远端工具名（区分大小写，精确匹配）。
        policy: 访问与治理策略（必须由宿主给出，``access`` 必填）。
        annotations: 语义标注（是否并发安全、幂等等）；默认全部为 False，最保守。
        name: 显式指定的内部名；默认 ``mcp.<server_id>.<远端名>``。
        alias: 显式指定的模型侧别名。
        description: 覆盖远端描述（远端描述是不可信输入，可能包含诱导模型的指令）。
        version: 显式版本；默认由定义内容的哈希生成，定义变化时自动换新版本。
        output_schema: 宿主提供的输出 Schema（远端声明的输出 Schema 不被采信）。
    """

    remote_name: str
    policy: ToolPolicy
    annotations: ToolAnnotations = field(default_factory=ToolAnnotations)
    name: str | None = None
    alias: str | None = None
    description: str | None = None
    version: str | None = None
    output_schema: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class McpSyncReport:
    """一次同步的结果。

    Attributes:
        server_id: 服务端 ID。
        registered: 本次新注册或以新版本替换的内部名。
        unchanged: 定义没有变化、保持不变的内部名。
        removed: 本次从注册表移除的内部名。
        missing: 在白名单中、但远端已不存在的远端名。
        rejected: 检查未通过的远端名 → 原因代码（如 ``invalid_schema``、``name_collision``）。
        ignored: 远端存在但不在白名单中的工具数量（不会注册，也不列出名字）。
        tenant_id: 本次同步写入的租户层；``None`` 表示共享层。
    """

    server_id: str
    registered: tuple[str, ...] = ()
    unchanged: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()
    rejected: dict[str, str] = field(default_factory=dict)
    ignored: int = 0
    tenant_id: str | None = None


def _clean_description(text: str | None) -> str:
    """清理远端描述：去掉控制字符（保留换行和制表符），截断到 1024 个字符。

    Args:
        text: 远端描述（不可信）。

    Returns:
        清理后的描述。
    """
    if not text:
        return ""
    cleaned = "".join(ch for ch in text if ch in "\n\t" or ord(ch) >= 32)
    return cleaned[:_MAX_REMOTE_DESCRIPTION]


def _harden_schema(schema: Any) -> dict[str, Any]:
    """复制远端参数 Schema，并把顶层收紧为“对象、禁止额外属性”。

    Args:
        schema: 远端声明的参数 Schema（不可信）。

    Returns:
        收紧后的副本（原对象不变）。

    Raises:
        RegistryError: 顶层不是对象类型的 Schema。
    """
    if not isinstance(schema, dict):
        raise RegistryError("input schema is not an object")
    hardened = copy.deepcopy(schema)
    if hardened.get("type", "object") != "object":
        raise RegistryError("input schema must describe an object")
    hardened["type"] = "object"
    # 比远端更严格是安全的：模型不能传入远端没有声明的参数
    hardened["additionalProperties"] = False
    return hardened


class McpToolSync:
    """把 MCP 服务端的工具同步进注册表。同一（服务端，作用域）的同步串行执行。"""

    def __init__(self, registry: ToolRegistry, manager: McpServerManager) -> None:
        """创建同步器。

        Args:
            registry: 工具注册表。
            manager: MCP 服务端管理器。
        """
        self._registry = registry
        self._manager = manager
        # 键是（服务端，租户层）；租户层为 None 表示共享层。不同租户的同步互不阻塞
        self._locks: dict[tuple[str, str | None], asyncio.Lock] = {}
        # （服务端，租户层）→ {远端名: 已注册的内部名}，用于刷新时判断哪些需要移除
        self._registered: dict[tuple[str, str | None], dict[str, str]] = {}

    def registered(self, server_id: str, *, tenant_id: str | None = None) -> dict[str, str]:
        """返回某个服务端当前由本同步器注册的工具（远端名 → 内部名）。

        Args:
            server_id: 服务端 ID。
            tenant_id: 查看哪个租户层的记录；``None``（默认）为共享层。

        Returns:
            映射的副本。
        """
        return dict(self._registered.get((server_id, tenant_id), {}))

    async def sync(
        self,
        server_id: str,
        allow: Sequence[McpToolAllow],
        *,
        ctx: ToolContext | None = None,
        scope: McpSyncScope = "shared",
    ) -> McpSyncReport:
        """发现并同步一个服务端的工具（见模块说明中的同步规则）。

        租户上下文：``session_scope="tenant"`` 的服务端没有共享会话，发现时必须由宿主通过
        ``ctx`` 指定用哪个租户的会话与凭证读取工具列表。

        * ``scope="shared"``（默认，原有行为）：结果写入共享层，对所有租户生效；用哪个租户发现
          由宿主决定（通常是专门的运维 / 服务租户）。执行时仍按各自租户的会话调用，某个租户的
          服务端实际没有该工具时，调用会得到协议错误；
        * ``scope="tenant"``：结果只写入 ``ctx.principal.tenant_id`` 的租户层。发现所用的会话、
          凭证和写入的目录属于同一个租户，适用于服务端按租户返回不同工具集，或宿主希望按租户
          使用不同白名单的场景。每个租户需要各自同步和刷新；白名单传空列表即移除该租户在这个
          服务端上的全部工具。

        并发：同一（服务端，作用域）的同步串行；不同租户的同步可以并发，各自只修改自己的层，
        每次同步的注册表变更仍然原子生效。同一个服务端不应同时用两种作用域同步同名工具：
        租户级工具与共享工具撞名会被注册表拒绝（整次同步失败，注册表不变）。

        Args:
            server_id: 服务端 ID。
            allow: 白名单。
            ctx: 发现使用的上下文（只读取租户，不会发给服务端）；服务端范围的会话且
                ``scope="shared"`` 时可以省略。
            scope: 写入注册表的作用域。

        Returns:
            同步结果。

        Raises:
            UpstreamError: 连接或读取工具列表失败（注册表保持不变）。
            RegistryConflictError: 与其他工具冲突（注册表保持不变）。
            PermissionDeniedError: 租户范围的服务端、或 ``scope="tenant"`` 的同步没有得到带租户的
                ``ctx``（不连接服务端，注册表保持不变）。
            ValueError: 白名单中有重复的远端名，或 ``scope`` 取值非法。
        """
        names = [a.remote_name for a in allow]
        if len(set(names)) != len(names):
            raise ValueError("duplicate remote_name in the MCP allow list")
        if scope not in ("shared", "tenant"):
            raise ValueError("scope must be 'shared' or 'tenant'")
        layer: str | None = None
        if scope == "tenant":
            layer = ctx.principal.tenant_id if ctx is not None else None
            if not layer:
                # 在连接之前拒绝：不能退回共享层，否则一个租户的目录会泄露给所有租户
                raise PermissionDeniedError(
                    "a tenant is required to sync tenant-scoped MCP tools",
                    code="mcp_tenant_required",
                )
        lock = self._locks.setdefault((server_id, layer), asyncio.Lock())
        async with lock:
            remote = {
                tool.name: tool for tool in await self._manager.list_tools(server_id, ctx=ctx)
            }
            previous = self._registered.get((server_id, layer), {})
            base = self._registry.snapshot()
            # view：该作用域看到的目录（共享层 + 本租户层），用于冲突判断；
            # own：本次同步实际写入的那一层，用于判断“之前注册的工具是否还在”。两者必须分开：
            # 租户层的旧记录不能因为共享层恰好有同名工具就被当成“还在”，否则会去删一个不属于
            # 这一层的工具
            view = base.for_tenant(layer)
            if layer is None:
                own = base.entries
            else:
                tenant_layer = base.tenants.get(layer)
                own = tenant_layer.entries if tenant_layer is not None else {}

            rejected: dict[str, str] = {}
            candidates: dict[str, ToolBinding] = {}
            missing: list[str] = []
            for entry in allow:
                tool = remote.get(entry.remote_name)
                if tool is None:
                    missing.append(entry.remote_name)
                    continue
                try:
                    candidates[entry.remote_name] = self._binding(server_id, entry, tool)
                except (RegistryError, ValueError) as exc:
                    rejected[entry.remote_name] = _reason(exc)

            # 同一批中映射到同一个内部名的工具全部拒绝，由宿主显式指定 name 解决
            by_name: dict[str, list[str]] = {}
            for remote_name, binding in candidates.items():
                by_name.setdefault(binding.spec.name, []).append(remote_name)
            for remote_names in by_name.values():
                if len(remote_names) > 1:
                    for remote_name in remote_names:
                        candidates.pop(remote_name)
                        rejected[remote_name] = "name_collision"

            register: list[ToolBinding] = []
            unregister: list[str] = []
            unchanged: list[str] = []
            current: dict[str, str] = {}
            for remote_name, binding in candidates.items():
                spec = binding.spec
                old_name = previous.get(remote_name)
                existing = own.get(spec.name) if old_name == spec.name else None
                if existing is not None and spec.version in existing.versions:
                    unchanged.append(spec.name)
                else:
                    if old_name is not None and old_name in own:
                        unregister.append(old_name)
                    register.append(binding)
                current[remote_name] = spec.name
            for remote_name, old_name in previous.items():
                if remote_name not in current and old_name in own:
                    unregister.append(old_name)

            # 只能替换本同步器自己注册过的工具：同名的其他工具（例如宿主的本地工具）一律视为冲突，
            # 否则 MCP 绑定会被悄悄挂成那个工具的新版本
            # 租户级同步还会把共享层的同名工具视为冲突（租户层不能遮盖共享工具）
            owned = {name for name in previous.values() if name in own}
            for binding in register:
                name = binding.spec.name
                if name in view.entries and name not in owned:
                    raise RegistryConflictError(
                        f"tool {name!r} already exists and is not managed by this MCP sync; "
                        "set an explicit name for the MCP tool"
                    )

            self._registry.apply(register=register, unregister=unregister, tenant_id=layer)
            self._registered[(server_id, layer)] = current
            return McpSyncReport(
                server_id=server_id,
                registered=tuple(b.spec.name for b in register),
                unchanged=tuple(unchanged),
                removed=tuple(n for n in unregister if n not in {b.spec.name for b in register}),
                missing=tuple(missing),
                rejected=rejected,
                ignored=sum(1 for n in remote if n not in set(names)),
                tenant_id=layer,
            )

    def _binding(self, server_id: str, entry: McpToolAllow, tool: Any) -> ToolBinding:
        """把一个远端工具 + 白名单条目转换成运行时绑定。

        Args:
            server_id: 服务端 ID。
            entry: 白名单条目。
            tool: 远端工具定义（不可信）。

        Returns:
            工具绑定（含已编译的参数校验器）。

        Raises:
            RegistryError: 名称、描述或 Schema 不满足要求。
            ValueError: 规格字段不合法（例如显式指定的名字格式错误）。
        """
        if not _REMOTE_NAME_RE.match(entry.remote_name):
            raise RegistryError("invalid_name")
        input_schema = _harden_schema(tool.input_schema)
        try:
            input_validator = compile_schema(input_schema)
        except RegistryError as exc:
            raise RegistryError(f"invalid_schema: {exc}") from None
        output_validator = (
            compile_schema(entry.output_schema) if entry.output_schema is not None else None
        )
        description = (
            entry.description
            if entry.description is not None
            else _clean_description(tool.description)
        )
        fingerprint = json.dumps(
            {
                "schema": input_schema,
                "description": description,
                "policy": entry.policy.model_dump(mode="json"),
                "annotations": entry.annotations.model_dump(mode="json"),
                "output_schema": entry.output_schema,
                "alias": entry.alias,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        version = entry.version or "m" + hashlib.sha256(fingerprint.encode()).hexdigest()[:12]
        spec = ToolSpec(
            name=entry.name or mcp_internal_name(server_id, entry.remote_name),
            version=version,
            description=description,
            input_schema=input_schema,
            output_schema=entry.output_schema,
            kind="mcp",
            annotations=entry.annotations,
            policy=entry.policy,
            alias=entry.alias,
        )
        return ToolBinding(
            spec=spec,
            target=McpTarget(server_id=server_id, remote_name=entry.remote_name),
            input_validator=input_validator,
            output_validator=output_validator,
        )


def _reason(exc: Exception) -> str:
    """把拒绝原因压缩成低基数的代码（不包含远端提供的内容）。

    Args:
        exc: 检查失败的异常。

    Returns:
        原因代码，例如 ``invalid_name``、``invalid_schema``、``invalid_spec``。
    """
    text = str(exc)
    for code in ("invalid_name", "invalid_schema"):
        if text.startswith(code):
            return code
    if isinstance(exc, RegistryError):
        return "invalid_schema"
    return "invalid_spec"
