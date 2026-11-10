"""工具注册表：注册、启停、版本、模型侧别名，以及写时复制的不可变快照。

并发模型：所有修改操作在一把线程锁内进行，每次修改都生成一个新的不可变快照并替换当前快照；
读取方（执行器）拿到快照后不再受后续修改影响。因此一次批量执行内的所有调用看到的工具版本一致。

按租户区分的视图（可选，默认不使用）：

* 注册表有一个**共享层**（不带 ``tenant_id`` 注册的工具，所有租户可见，行为与以前完全相同）
  和若干**租户层**（带 ``tenant_id`` 注册的工具，只有该租户可见、可解析）；
* 快照本身只解析共享层；:meth:`RegistrySnapshot.for_tenant` 返回“共享层 + 该租户层”的视图。
  执行器按 ``principal.tenant_id`` 取视图，所以忘记取视图的代码路径只会看到**更少**的工具；
* 名称与别名：同一个租户看到的名字空间（共享层 + 自己的租户层）内不允许任何歧义，冲突在注册
  阶段拒绝，租户层**不能遮盖**共享层的工具。不同租户之间互不影响，可以用相同的名字和别名注册
  不同的定义；
* 租户层只决定“这个租户的目录里有什么”，不是授权：解析之后仍然执行工具级与参数级鉴权；
* 所有层共用一个快照序号：任何一层的变更都会生成新快照。未变化的层在新旧快照之间共享同一个
  不可变对象，保留历史快照的开销与变更的那一层成正比。
"""

from __future__ import annotations

import hashlib
import inspect
import re
import threading
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel

from .binding import PythonTarget, ToolBinding
from .protocol.errors import (
    RegistryConflictError,
    RegistryError,
    ToolDisabledError,
    ToolNotFoundError,
    ToolVersionNotFoundError,
)
from .protocol.spec import HttpTarget, ToolAnnotations, ToolPolicy, ToolSpec
from .runtime.isolation import ProcessIsolation
from .security.schema import compile_schema

__all__ = ["RegistrySnapshot", "ToolRegistry", "model_alias"]

_ALIAS_MAX = 64
_INVALID_ALIAS_CHARS = re.compile(r"[^A-Za-z0-9_\-]")


def model_alias(name: str) -> str:
    """按确定性规则，把内部名转换成满足 Provider 命名规则的模型侧别名。

    规则：把 ``[A-Za-z0-9_-]`` 以外的字符替换为 ``_``；超过 64 个字符时截断到 55 个字符，
    再追加 ``_`` 和内部名 SHA-256 的前 8 位十六进制，保证不同的长名字大概率得到不同别名。
    别名只由名字决定，与版本无关，所以升级版本不会改变模型看到的名字。

    Args:
        name: 工具的内部名。

    Returns:
        长度 1–64、只包含 ``[A-Za-z0-9_-]`` 的别名。
    """
    alias = _INVALID_ALIAS_CHARS.sub("_", name)
    if len(alias) <= _ALIAS_MAX:
        return alias
    digest = hashlib.sha256(name.encode()).hexdigest()[:8]
    return f"{alias[:55]}_{digest}"


@dataclass(frozen=True, slots=True)
class _Entry:
    """注册表中一个工具名对应的全部状态（不可变）。"""

    name: str
    alias: str
    versions: Mapping[str, ToolBinding]
    active: str
    enabled: bool


@dataclass(frozen=True, slots=True)
class _Layer:
    """一个作用域（共享层或某个租户层）在某一时刻的不可变内容。

    Attributes:
        entries: 工具名 → 工具状态。
        aliases: 模型侧别名 → 工具名。
    """

    entries: Mapping[str, _Entry]
    aliases: Mapping[str, str]

    @classmethod
    def of(cls, entries: Mapping[str, _Entry]) -> _Layer:
        """从条目字典生成不可变的层（复制一份，之后对原字典的修改不影响这一层）。

        Args:
            entries: 工具名 → 工具状态。

        Returns:
            新的层。
        """
        return cls(
            entries=MappingProxyType(dict(entries)),
            aliases=MappingProxyType({e.alias: e.name for e in entries.values()}),
        )


class _Overlay(Mapping[str, Any]):
    """把租户层叠在共享层之上的只读映射（不复制，创建成本 O(1)）。

    注册规则保证两层的键互不相交（见 :meth:`ToolRegistry._check_cross_scope`），所以查找顺序
    不影响结果，长度就是两层之和。
    """

    __slots__ = ("_base", "_top")

    def __init__(self, top: Mapping[str, Any], base: Mapping[str, Any]) -> None:
        """创建叠加映射。

        Args:
            top: 租户层的映射。
            base: 共享层的映射。
        """
        self._top = top
        self._base = base

    def __getitem__(self, key: str) -> Any:
        """先查租户层，再查共享层。

        Args:
            key: 键。

        Returns:
            对应的值。

        Raises:
            KeyError: 两层都没有这个键。
        """
        try:
            return self._top[key]
        except KeyError:
            return self._base[key]

    def __iter__(self) -> Iterator[str]:
        """依次遍历租户层与共享层的键（两层互不相交，不会重复）。"""
        yield from self._top
        yield from self._base

    def __len__(self) -> int:
        """两层的键数之和。"""
        return len(self._top) + len(self._base)


_NO_TENANTS: Mapping[str, _Layer] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class RegistrySnapshot:
    """注册表在某一时刻的不可变视图。

    直接从注册表取得的快照只包含**共享层**的工具；:meth:`for_tenant` 返回叠加了某个租户层的
    视图。视图与原快照的 ``id`` 相同。

    Attributes:
        id: 单调递增的快照编号（字符串形式）。
        entries: 工具名 → 工具状态（视图中为“共享层 + 该租户层”）。
        aliases: 模型侧别名 → 工具名（同上）。
        tenants: 租户 ID → 租户层。只在注册表直接给出的快照上有内容；租户视图中为空，
            因此不能从一个租户的视图再换到另一个租户。
        tenant_id: 这个视图叠加的租户；``None`` 表示只有共享层。
    """

    id: str
    entries: Mapping[str, _Entry]
    aliases: Mapping[str, str]
    tenants: Mapping[str, _Layer] = field(default=_NO_TENANTS, repr=False)
    tenant_id: str | None = None

    def for_tenant(self, tenant_id: str | None) -> RegistrySnapshot:
        """返回某个租户看到的视图：共享层加上这个租户自己的层。

        租户隔离边界：视图中只能解析共享工具和 ``tenant_id`` 自己的工具，其他租户的工具表现为
        不存在（``ToolNotFoundError``，与名字本来就不存在无法区分）。``tenant_id`` 必须来自宿主
        认证后的身份（``Principal.tenant_id``），不能来自模型输出。

        Args:
            tenant_id: 租户 ID；``None`` 或没有租户层的租户得到只含共享层的快照（即本对象）。

        Returns:
            视图（与本快照 ``id`` 相同，不可变，创建成本 O(1)）。

        Raises:
            ValueError: 本对象已经是某个租户的视图，却被要求换成另一个租户。
        """
        if self.tenant_id is not None:
            if tenant_id == self.tenant_id:
                return self
            # 视图里已经没有其他租户的层；静默返回会让调用方误以为拿到了对方的视图
            raise ValueError("a tenant view cannot be re-scoped to another tenant")
        layer = self.tenants.get(tenant_id) if tenant_id is not None else None
        if layer is None:
            return self
        return RegistrySnapshot(
            id=self.id,
            entries=MappingProxyType(_Overlay(layer.entries, self.entries)),
            aliases=MappingProxyType(_Overlay(layer.aliases, self.aliases)),
            tenant_id=tenant_id,
        )

    def resolve(self, ref: str, version: str | None = None) -> ToolBinding:
        """按 别名 → 内部名 → 版本 的顺序解析工具。

        Args:
            ref: 模型侧别名或内部名。
            version: 指定版本；``None`` 表示当前启用的版本。

        Returns:
            解析到的工具绑定。

        Raises:
            ToolNotFoundError: 名字不存在。
            ToolDisabledError: 工具已被停用。
            ToolVersionNotFoundError: 指定的版本不存在。
        """
        name = self.aliases.get(ref, ref)
        entry = self.entries.get(name)
        if entry is None:
            # 名字来自模型输出：只回显截断后的 repr，避免超长或控制字符进入错误信息
            raise ToolNotFoundError(f"unknown tool {ref[:64]!r}")
        if not entry.enabled:
            raise ToolDisabledError(f"tool {entry.alias!r} is disabled")
        chosen = entry.active if version is None else version
        binding = entry.versions.get(chosen)
        if binding is None:
            raise ToolVersionNotFoundError(f"tool {entry.alias!r} has no version {chosen[:32]!r}")
        return binding

    def alias_of(self, name: str) -> str:
        """返回内部名对应的模型侧别名。

        Args:
            name: 工具的内部名。

        Returns:
            模型侧别名。

        Raises:
            KeyError: 名字不存在。
        """
        return self.entries[name].alias

    def active_bindings(self) -> Iterator[tuple[str, ToolBinding]]:
        """遍历所有已启用工具的当前启用版本。

        Yields:
            ``(别名, 工具绑定)``，按工具名排序，保证给模型的列表顺序稳定。
        """
        for name in sorted(self.entries):
            entry = self.entries[name]
            if entry.enabled:
                yield entry.alias, entry.versions[entry.active]


class ToolRegistry:
    """线程安全的工具注册表。"""

    def __init__(
        self, *, allow_destructive_without_approval: bool = False, history: int = 8
    ) -> None:
        """创建空注册表。

        Args:
            allow_destructive_without_approval: 是否允许注册**没有声明审批**的
                ``destructive=True`` 工具。默认禁止；声明了 ``policy.approval`` 的不可逆工具
                不受这个开关影响，总是可以注册（执行时必须获批）。宿主显式放开时需要自行承担风险。
            history: 保留最近多少个快照，供 ``execute_many(..., snapshot_id=...)`` 查找。

        Raises:
            ValueError: ``history`` 不是正数。
        """
        if history <= 0:
            raise ValueError("history must be positive")
        self._allow_destructive = allow_destructive_without_approval
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}
        # 租户 ID → 该租户层的条目；没有任何工具的租户不保留空层
        self._tenant_entries: dict[str, dict[str, _Entry]] = {}
        # 已发布的不可变层：未变化的层在相邻快照之间共享同一个对象
        self._shared_layer = _Layer.of({})
        self._tenant_layers: dict[str, _Layer] = {}
        self._seq = 0
        self._history: deque[RegistrySnapshot] = deque(maxlen=history)
        self._current = self._publish()

    # -- 读取 ----------------------------------------------------------------------
    def snapshot(self) -> RegistrySnapshot:
        """返回当前的不可变快照（O(1)，不加锁）。

        返回的快照只解析共享层；需要某个租户的目录时调用
        :meth:`RegistrySnapshot.for_tenant`。
        """
        return self._current

    def tenants(self) -> tuple[str, ...]:
        """返回当前拥有租户层（至少注册了一个租户级工具）的租户 ID，按字典序排列。"""
        return tuple(sorted(self._current.tenants))

    def get_snapshot(self, snapshot_id: str) -> RegistrySnapshot | None:
        """按编号查找最近的快照。

        Args:
            snapshot_id: 快照编号。

        Returns:
            找到的快照；已经超出保留范围时返回 ``None``。任何一层（包括其他租户的层）的
            变更都会占用一个保留名额：租户很多且刷新频繁时，应调大构造参数 ``history``。
        """
        for snap in reversed(self._history):
            if snap.id == snapshot_id:
                return snap
        return None

    # -- 修改 ----------------------------------------------------------------------
    def register(self, binding: ToolBinding, *, tenant_id: str | None = None) -> ToolSpec:
        """注册一个工具版本。

        某个名字第一次注册时，这个版本自动成为启用版本；之后注册的新版本需要通过
        :meth:`set_active` 切换。

        Args:
            binding: 工具绑定。
            tenant_id: 注册到哪个租户层；``None``（默认）注册到共享层，所有租户可见。
                租户级工具只在该租户的视图中存在，其他租户无法列出或解析。

        Returns:
            已注册的工具规格。

        Raises:
            RegistryError: 工具是不可逆操作，但注册表未允许注册此类工具。
            RegistryConflictError: 同名同版本已存在，或者名称、别名与同一视图内的其他工具冲突
                （租户级工具与共享工具之间也不允许冲突）。
            ValueError: ``tenant_id`` 不合法。
        """
        self.apply(register=[binding], tenant_id=tenant_id)
        return binding.spec

    def apply(
        self,
        *,
        register: Sequence[ToolBinding] = (),
        unregister: Sequence[str] = (),
        tenant_id: str | None = None,
    ) -> RegistrySnapshot:
        """原子地删除和注册一批工具（P2：MCP 工具同步使用）。

        一次调用只作用于**一个**作用域：共享层（``tenant_id=None``）或某一个租户层。其他租户层
        和（操作租户层时的）共享层不会被修改，但会参与冲突检查。

        先按 ``unregister`` 删除整个工具（全部版本），再按顺序注册 ``register`` 中的绑定，
        规则与 :meth:`register` 相同。所有检查都在一个工作副本上完成：任何一步失败都会
        抛出异常，注册表**保持不变**；全部成功时只生成**一个**新快照，读取方不会看到
        “删了一半、加了一半”的中间状态。

        Args:
            register: 要注册的工具绑定。
            unregister: 要删除的工具内部名（在所选作用域内查找）。
            tenant_id: 作用域：``None`` 为共享层，否则为该租户的层。

        Returns:
            变更后的新快照（只含共享层的基础快照；租户目录用 ``for_tenant`` 取得）。

        Raises:
            RegistryError: 要删除的工具在该作用域内不存在，或者注册了没有声明审批、也未被允许
                的不可逆工具。
            RegistryConflictError: 名称、版本或别名冲突（含跨作用域冲突）。
            ValueError: ``tenant_id`` 不合法。
        """
        _check_tenant_id(tenant_id)
        for binding in register:
            spec = binding.spec
            # 不可逆工具必须经过人工审批（P3）；宿主显式放开时可以不审批（P0 起的兼容行为）
            if (
                spec.annotations.destructive
                and spec.policy.approval is None
                and not self._allow_destructive
            ):
                raise RegistryError(
                    f"tool {spec.name!r} is destructive; declare policy.approval to require "
                    "human approval, or pass allow_destructive_without_approval=True to the "
                    "registry to accept the risk"
                )
        with self._lock:
            work = self._working_copy(tenant_id)
            for name in unregister:
                if name not in work:
                    raise RegistryError(f"tool {name!r} is not registered{_where(tenant_id)}")
                del work[name]
            for binding in register:
                self._add_to(work, binding, tenant_id)
            return self._commit(tenant_id, work)

    def register_function(
        self,
        fn: Callable[..., Any],
        *,
        name: str,
        policy: ToolPolicy,
        description: str | None = None,
        version: str = "1",
        annotations: ToolAnnotations | None = None,
        alias: str | None = None,
        args_model: type[BaseModel] | None = None,
        isolation: ProcessIsolation | None = None,
        tenant_id: str | None = None,
    ) -> ToolSpec:
        """把一个 Python 函数注册为工具（宿主信任的代码）。

        参数 Schema 由函数签名或 ``args_model`` 生成，规则见
        :meth:`agent_tools.binding.PythonTarget.from_function`。

        Args:
            fn: 同步或异步函数。
            name: 内部名。
            policy: 访问与治理策略（``access`` 必须显式声明）。
            description: 给模型看的描述；不传时使用函数文档字符串的第一段。
            version: 版本字符串。
            annotations: 语义标注。
            alias: 显式指定的模型侧别名。
            args_model: 显式指定的参数模型。
            isolation: 进程隔离配置（P3）：每次调用在新的子进程中运行，超时或取消时终止
                子进程。不是完整的安全沙箱，边界见 :mod:`agent_tools.runtime.isolation`。
            tenant_id: 注册到哪个租户层；``None``（默认）为共享层。见 :meth:`register`。

        Returns:
            已注册的工具规格。

        Raises:
            RegistryError: 函数签名不受支持，违反注册规则，或配置了隔离但函数不能在子进程中
                重新导入。
            RegistryConflictError: 与已注册的工具冲突。
            pydantic.ValidationError: 规格字段不合法（例如名字格式错误）。
            ValueError: ``tenant_id`` 不合法。
        """
        target = PythonTarget.from_function(fn, args_model=args_model, isolation=isolation)
        if description is None:
            doc = inspect.getdoc(fn) or ""
            description = doc.split("\n\n", 1)[0].strip()
        spec = ToolSpec(
            name=name,
            version=version,
            description=description,
            input_schema=target.input_schema(),
            kind="python",
            annotations=annotations or ToolAnnotations(),
            policy=policy,
            alias=alias,
        )
        return self.register(ToolBinding(spec=spec, target=target), tenant_id=tenant_id)

    def register_http(
        self,
        target: HttpTarget,
        *,
        name: str,
        input_schema: dict[str, Any],
        policy: ToolPolicy,
        description: str = "",
        version: str = "1",
        annotations: ToolAnnotations | None = None,
        alias: str | None = None,
        output_schema: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> ToolSpec:
        """注册一个 HTTP 工具（需要 ``agent-tools[http]``）。

        注册规则（在注册阶段拒绝，而不是运行时才发现）：

        * ``input_schema`` 必须是合法的 Draft 2020-12 Schema，只含本地 ``$ref``，顶层
          ``additionalProperties`` 必须为 ``false``（模型不能传入未声明的参数，与 Python 工具的
          ``extra="forbid"`` 一致）；
        * ``arg_mapping`` 中的参数必须在 ``properties`` 中声明；映射到 ``path`` 的参数必须是必填；
        * 声明了 ``output_schema`` 时同样编译，调用时对结构化输出在截断之前校验。

        主机白名单不在这里检查：它属于部署配置（``HttpAdapterConfig.network``），每次请求时检查。

        Args:
            target: HTTP 目标。
            name: 内部名。
            input_schema: 参数的 JSON Schema。
            policy: 访问与治理策略。
            description: 给模型看的描述。
            version: 版本字符串。
            annotations: 语义标注。
            alias: 显式指定的模型侧别名。
            output_schema: 输出的 JSON Schema（可选）。
            tenant_id: 注册到哪个租户层；``None``（默认）为共享层。见 :meth:`register`。
                租户层只决定目录；凭证仍由 ``SecretProvider`` 按调用上下文提供。

        Returns:
            已注册的工具规格。

        Raises:
            RegistryError: Schema 或映射不满足上述规则，或未安装 ``[http]`` extra。
            RegistryConflictError: 与已注册的工具冲突。
            pydantic.ValidationError: 规格字段不合法。
            ValueError: ``tenant_id`` 不合法。
        """
        if input_schema.get("additionalProperties") is not False:
            raise RegistryError("HTTP tool input_schema must set additionalProperties to false")
        properties = input_schema.get("properties", {})
        required = set(input_schema.get("required", []))
        for arg, location in target.arg_mapping.items():
            if arg not in properties:
                raise RegistryError(f"mapped argument {arg!r} is not declared in input_schema")
            if location == "path" and arg not in required:
                raise RegistryError(f"path argument {arg!r} must be required in input_schema")
        input_validator = compile_schema(input_schema)
        output_validator = compile_schema(output_schema) if output_schema is not None else None
        spec = ToolSpec(
            name=name,
            version=version,
            description=description,
            input_schema=input_schema,
            output_schema=output_schema,
            kind="http",
            annotations=annotations or ToolAnnotations(),
            policy=policy,
            alias=alias,
        )
        return self.register(
            ToolBinding(
                spec=spec,
                target=target,
                input_validator=input_validator,
                output_validator=output_validator,
            ),
            tenant_id=tenant_id,
        )

    def set_active(self, name: str, version: str, *, tenant_id: str | None = None) -> None:
        """切换模型使用的版本。

        Args:
            name: 工具内部名。
            version: 要启用的版本，必须已经注册。
            tenant_id: 工具所在的作用域：``None`` 为共享层，否则为该租户的层。

        Raises:
            RegistryError: 工具或版本在该作用域内不存在。
            ValueError: ``tenant_id`` 不合法。
        """
        _check_tenant_id(tenant_id)
        with self._lock:
            work = self._working_copy(tenant_id)
            entry = self._require(work, name, tenant_id)
            if version not in entry.versions:
                raise RegistryError(f"tool {name!r} has no version {version!r}")
            work[name] = replace(entry, active=version)
            self._commit(tenant_id, work)

    def enable(self, name: str, enabled: bool = True, *, tenant_id: str | None = None) -> None:
        """启用或停用一个工具（作用于它的所有版本）。

        停用共享工具对所有租户生效；停用租户级工具只影响该租户。

        Args:
            name: 工具内部名。
            enabled: ``True`` 启用，``False`` 停用。
            tenant_id: 工具所在的作用域：``None`` 为共享层，否则为该租户的层。

        Raises:
            RegistryError: 工具在该作用域内不存在。
            ValueError: ``tenant_id`` 不合法。
        """
        _check_tenant_id(tenant_id)
        with self._lock:
            work = self._working_copy(tenant_id)
            entry = self._require(work, name, tenant_id)
            work[name] = replace(entry, enabled=enabled)
            self._commit(tenant_id, work)

    def disable(self, name: str, *, tenant_id: str | None = None) -> None:
        """停用一个工具，等价于 ``enable(name, False, tenant_id=tenant_id)``。

        Args:
            name: 工具内部名。
            tenant_id: 工具所在的作用域：``None`` 为共享层，否则为该租户的层。

        Raises:
            RegistryError: 工具在该作用域内不存在。
            ValueError: ``tenant_id`` 不合法。
        """
        self.enable(name, False, tenant_id=tenant_id)

    def unregister(
        self, name: str, version: str | None = None, *, tenant_id: str | None = None
    ) -> None:
        """删除一个版本，或者删除整个工具。

        Args:
            name: 工具内部名。
            version: 要删除的版本；``None`` 表示删除这个工具的全部版本。
                不能单独删除当前启用的版本，需要先用 :meth:`set_active` 切换。
            tenant_id: 工具所在的作用域：``None`` 为共享层，否则为该租户的层。

        Raises:
            RegistryError: 工具或版本在该作用域内不存在，或者试图单独删除启用版本。
            ValueError: ``tenant_id`` 不合法。
        """
        _check_tenant_id(tenant_id)
        with self._lock:
            work = self._working_copy(tenant_id)
            entry = self._require(work, name, tenant_id)
            if version is None:
                del work[name]
            else:
                if version not in entry.versions:
                    raise RegistryError(f"tool {name!r} has no version {version!r}")
                if version == entry.active:
                    raise RegistryError(
                        f"version {version!r} is active; call set_active() with another version "
                        "first, or unregister the whole tool"
                    )
                versions = {k: v for k, v in entry.versions.items() if k != version}
                work[name] = replace(entry, versions=MappingProxyType(versions))
            self._commit(tenant_id, work)

    def drop_tenant(self, tenant_id: str) -> tuple[str, ...]:
        """删除一个租户层中的全部工具（例如租户下线时）。共享层不受影响。

        只生成一个新快照；已经取得旧快照的在途调用仍按旧快照执行完。本方法只清理目录，
        不会关闭该租户的 MCP 会话或清除凭证（由 ``McpServerManager.invalidate`` 等负责）。

        Args:
            tenant_id: 租户 ID。

        Returns:
            被删除的工具内部名（按字典序）；该租户没有租户层时为空元组，注册表不变。

        Raises:
            ValueError: ``tenant_id`` 不合法。
        """
        if tenant_id is None:
            raise ValueError("tenant_id is required")
        _check_tenant_id(tenant_id)
        with self._lock:
            removed = tuple(sorted(self._tenant_entries.get(tenant_id, {})))
            if removed:
                self._commit(tenant_id, {})
            return removed

    # -- 内部方法（调用方必须持有锁，构造函数除外） --------------------------------
    def _working_copy(self, tenant_id: str | None) -> dict[str, _Entry]:
        """返回某个作用域当前条目的可修改副本（修改它不影响注册表，直到 :meth:`_commit`）。

        Args:
            tenant_id: ``None`` 为共享层，否则为该租户的层（不存在时得到空字典）。

        Returns:
            条目字典的浅拷贝。
        """
        if tenant_id is None:
            return dict(self._entries)
        return dict(self._tenant_entries.get(tenant_id, {}))

    def _commit(self, tenant_id: str | None, work: dict[str, _Entry]) -> RegistrySnapshot:
        """用工作副本替换某个作用域的内容，并发布新快照。

        只重建发生变化的那一层；其他层沿用已发布的不可变对象。租户层变空时整层移除，
        避免已下线的租户一直留在快照里。

        Args:
            tenant_id: ``None`` 为共享层，否则为该租户的层。
            work: 该作用域的新内容。

        Returns:
            新快照。
        """
        if tenant_id is None:
            self._entries = work
            self._shared_layer = _Layer.of(work)
        elif work:
            self._tenant_entries[tenant_id] = work
            self._tenant_layers[tenant_id] = _Layer.of(work)
        else:
            self._tenant_entries.pop(tenant_id, None)
            self._tenant_layers.pop(tenant_id, None)
        self._current = self._publish()
        return self._current

    def _require(self, entries: Mapping[str, _Entry], name: str, tenant_id: str | None) -> _Entry:
        """在给定作用域的条目中查找工具，不存在时抛出 :class:`RegistryError`。

        Args:
            entries: 该作用域的条目。
            name: 工具内部名。
            tenant_id: 作用域（只用于错误信息）。

        Returns:
            工具状态。

        Raises:
            RegistryError: 工具不存在。
        """
        entry = entries.get(name)
        if entry is None:
            raise RegistryError(f"tool {name!r} is not registered{_where(tenant_id)}")
        return entry

    def _add_to(
        self, entries: dict[str, _Entry], binding: ToolBinding, tenant_id: str | None
    ) -> None:
        """把一个工具版本加入给定的条目字典（工作副本），冲突时抛出异常。

        Args:
            entries: 所选作用域的条目字典（会被修改）。
            binding: 工具绑定。
            tenant_id: 所选作用域：``None`` 为共享层，否则为该租户的层。

        Raises:
            RegistryConflictError: 同名同版本已存在，版本间别名不一致，别名与同一作用域的其他
                工具冲突，或名称 / 别名与另一个作用域中同一视图可见的工具冲突。
        """
        spec = binding.spec
        existing = entries.get(spec.name)
        if existing is not None:
            if spec.version in existing.versions:
                raise RegistryConflictError(
                    f"tool {spec.name!r} version {spec.version!r} is already registered"
                )
            if spec.alias is not None and spec.alias != existing.alias:
                raise RegistryConflictError(
                    f"tool {spec.name!r} already uses alias {existing.alias!r}; "
                    "the alias must not change between versions"
                )
            versions = {**existing.versions, spec.version: binding}
            entries[spec.name] = replace(existing, versions=MappingProxyType(versions))
            return
        alias = spec.alias or model_alias(spec.name)
        self._check_alias_free(entries, spec.name, alias)
        # 只有新名字需要跨作用域检查：已有名字的名称与别名在第一次注册时已经检查过
        self._check_cross_scope(tenant_id, spec.name, alias)
        entries[spec.name] = _Entry(
            name=spec.name,
            alias=alias,
            versions=MappingProxyType({spec.version: binding}),
            active=spec.version,
            enabled=True,
        )

    def _check_alias_free(self, entries: Mapping[str, _Entry], name: str, alias: str) -> None:
        """检查新工具的名字和别名不会与同一作用域的已有工具产生歧义。

        解析顺序是“先别名、后内部名”，所以别名不能等于其他工具的内部名，
        内部名也不能等于其他工具的别名；冲突在注册阶段直接拒绝，而不是运行时悄悄改名。

        Args:
            entries: 当前（或工作副本中的）条目。
            name: 新工具的内部名。
            alias: 新工具的别名。

        Raises:
            RegistryConflictError: 存在冲突。
        """
        for other in entries.values():
            if alias in (other.alias, other.name) or name == other.alias:
                raise RegistryConflictError(
                    f"tool {name!r} (alias {alias!r}) conflicts with tool {other.name!r} "
                    f"(alias {other.alias!r}); set an explicit alias"
                )

    def _check_cross_scope(self, tenant_id: str | None, name: str, alias: str) -> None:
        """检查新工具不会与**另一个作用域**里、会出现在同一个租户视图中的工具产生歧义。

        一个租户的视图 = 共享层 + 自己的租户层，所以：

        * 注册租户级工具时，与共享层比较：同名或别名交叉都拒绝。租户层不能遮盖共享工具，
          否则同一个名字对不同租户会解析到权限、策略完全不同的定义，而宿主很难察觉；
        * 注册共享工具时，与**每一个**租户层比较：否则新的共享工具会与某个租户已有的工具
          在该租户的视图里撞名。

        不同租户层之间不比较：它们永远不会出现在同一个视图里。比较对象是其他作用域已提交的
        内容（本次 ``apply`` 只修改一个作用域，其他作用域在锁内不会变化）。

        Args:
            tenant_id: 新工具的作用域。
            name: 新工具的内部名。
            alias: 新工具的别名。

        Raises:
            RegistryConflictError: 存在冲突。
        """
        others: list[tuple[str | None, _Layer]]
        if tenant_id is None:
            others = list(self._tenant_layers.items())
        else:
            others = [(None, self._shared_layer)]
        for owner, layer in others:
            for token in (name, alias):
                other_name = token if token in layer.entries else layer.aliases.get(token)
                if other_name is not None:
                    raise RegistryConflictError(
                        f"tool {name!r} (alias {alias!r}){_where(tenant_id)} conflicts with "
                        f"tool {other_name!r}{_where(owner)}; tenant-scoped and shared tools "
                        "must not share names or aliases, set an explicit name or alias"
                    )

    def _publish(self) -> RegistrySnapshot:
        """根据已发布的各层生成新的不可变快照，并记入历史。

        Returns:
            新快照（只解析共享层，附带全部租户层供 ``for_tenant`` 使用）。
        """
        self._seq += 1
        # 只复制“租户 → 层”的指针；层本身不可变，在快照之间共享
        tenants = (
            MappingProxyType(dict(self._tenant_layers)) if self._tenant_layers else _NO_TENANTS
        )
        snap = RegistrySnapshot(
            id=str(self._seq),
            entries=self._shared_layer.entries,
            aliases=self._shared_layer.aliases,
            tenants=tenants,
        )
        self._history.append(snap)
        return snap


def _check_tenant_id(tenant_id: str | None) -> None:
    """校验租户 ID（与 ``Principal.tenant_id`` 的约束一致：非空、不超过 256 个字符）。

    空字符串被拒绝：它很容易来自“没有租户”的误传，一旦被当成真实租户，就会出现一个所有
    匿名调用共享的租户层。

    Args:
        tenant_id: 租户 ID；``None`` 表示共享层，总是合法。

    Raises:
        ValueError: 不是字符串、为空或超长。
    """
    if tenant_id is None:
        return
    if not isinstance(tenant_id, str) or not tenant_id or len(tenant_id) > 256:
        raise ValueError("tenant_id must be a non-empty string of at most 256 characters")


def _where(tenant_id: str | None) -> str:
    """生成错误信息中的作用域说明（错误信息面向宿主，不会返回给模型）。

    Args:
        tenant_id: 作用域。

    Returns:
        共享层为空字符串，租户层为 `` for tenant '<id>'``（ID 截断到 64 个字符）。
    """
    return "" if tenant_id is None else f" for tenant {tenant_id[:64]!r}"
