"""按租户区分的注册表视图的测试。

分三层验证：

* 注册表：租户层与共享层的可见性、名称与别名冲突规则、各修改操作的作用域、快照不可变与层共享、
  多线程并发注册；
* 执行器：``list_tools`` / ``execute`` / ``execute_many`` 按调用方租户取视图，其他租户的工具表现为
  不存在，解析之后仍然鉴权，``snapshot_id`` 不能跨租户使用；
* MCP 同步：``McpToolSync.sync(..., scope="tenant")`` 用租户自己的会话发现并只写入该租户的层。
  服务端是内存中的 ASGI 应用（与 ``test_mcp_tenancy.py`` 的隔离方式相同），按请求携带的凭证返回
  **不同的工具集**；除最终的 HTTP 传输外都是生产代码。

没有租户层时的默认行为由已有的 P0–P3 测试覆盖（它们的断言没有修改）。
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import threading
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx2
import mcp.types as mt
import pytest
from mcp.server.lowlevel import Server

from agent_tools import (
    CachePolicy,
    HttpTarget,
    InMemoryResultStore,
    NetworkPolicy,
    PermissionDeniedError,
    Principal,
    RegistryConflictError,
    RegistryError,
    SecretRef,
    ToolAnnotations,
    ToolContext,
    ToolDisabledError,
    ToolExecutor,
    ToolInvocation,
    ToolNotFoundError,
    ToolPolicy,
    ToolRegistry,
)
from agent_tools.adapters.mcp import (
    HttpTransportConfig,
    McpAdapter,
    McpServerConfig,
    McpServerManager,
)
from agent_tools.integrations.mcp import McpToolAllow, McpToolSync

PUBLIC = ToolPolicy(access="public")


async def _one() -> str:
    """测试用工具：返回固定文本 ``one``。"""
    return "one"


async def _two() -> str:
    """测试用工具：返回固定文本 ``two``。"""
    return "two"


def ctx_of(tenant: str | None, **principal: Any) -> ToolContext:
    """构造某个租户的调用上下文。

    Args:
        tenant: 租户 ID；``None`` 表示没有租户。
        **principal: ``Principal`` 的其他字段。

    Returns:
        调用上下文。
    """
    return ToolContext(principal=Principal(tenant_id=tenant, **principal))


def invoke(tool: str, call_id: str = "c", **arguments: Any) -> ToolInvocation:
    """构造一次调用请求。

    Args:
        tool: 模型侧别名或内部名。
        call_id: 调用 ID。
        **arguments: 工具参数。

    Returns:
        调用请求。
    """
    return ToolInvocation(call_id=call_id, tool=tool, arguments=arguments)


# ---------------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------------
def test_tenant_tools_resolve_only_in_their_own_view() -> None:
    """租户级工具只存在于自己租户的视图：基础快照、其他租户、未知租户都解析不到；共享工具
    在所有视图中都能解析。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="shared.tool", policy=PUBLIC)
    registry.register_function(_one, name="crm.search", policy=PUBLIC, tenant_id="acme")
    snapshot = registry.snapshot()

    assert registry.tenants() == ("acme",)
    assert snapshot.for_tenant("acme").resolve("crm_search").spec.name == "crm.search"
    assert snapshot.for_tenant("acme").resolve("shared_tool").spec.name == "shared.tool"
    for view in (snapshot, snapshot.for_tenant(None), snapshot.for_tenant("globex")):
        assert view.resolve("shared_tool").spec.name == "shared.tool"
        with pytest.raises(ToolNotFoundError):
            view.resolve("crm_search")
        with pytest.raises(ToolNotFoundError):
            view.resolve("crm.search")  # 用内部名也解析不到
        assert [alias for alias, _ in view.active_bindings()] == ["shared_tool"]
    assert [alias for alias, _ in snapshot.for_tenant("acme").active_bindings()] == [
        "crm_search",
        "shared_tool",
    ]


def test_tenants_can_reuse_names_and_aliases_with_different_definitions() -> None:
    """不同租户可以用相同的内部名和别名注册不同的定义，各自解析到自己的那一个。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="crm.search", policy=PUBLIC, tenant_id="acme")
    registry.register_function(
        _two, name="crm.search", policy=PUBLIC, tenant_id="globex", version="7"
    )
    snapshot = registry.snapshot()
    acme = snapshot.for_tenant("acme").resolve("crm_search")
    globex = snapshot.for_tenant("globex").resolve("crm_search")
    assert (acme.spec.version, globex.spec.version) == ("1", "7")
    assert acme.target is not globex.target
    assert snapshot.for_tenant("acme").alias_of("crm.search") == "crm_search"


@pytest.mark.parametrize(
    ("shared", "tenant"),
    [
        ({"name": "a.b"}, {"name": "a.b"}),  # 同名：租户层不能遮盖共享工具
        ({"name": "a.b"}, {"name": "a_b"}),  # 租户工具的内部名等于共享工具的别名
        ({"name": "a_b"}, {"name": "a.b"}),  # 租户工具的别名等于共享工具的内部名
        ({"name": "x", "alias": "pick"}, {"name": "y", "alias": "pick"}),  # 别名相同
    ],
)
def test_tenant_and_shared_tools_must_not_collide(
    shared: dict[str, str], tenant: dict[str, str]
) -> None:
    """租户级工具与共享工具之间的名称、别名冲突，无论先注册哪一方都在注册阶段被拒绝，
    失败的注册不产生新快照。"""
    first = ToolRegistry()
    first.register_function(_one, policy=PUBLIC, **shared)
    before = first.snapshot().id
    with pytest.raises(RegistryConflictError):
        first.register_function(_two, policy=PUBLIC, tenant_id="acme", **tenant)
    assert first.snapshot().id == before
    assert first.tenants() == ()

    second = ToolRegistry()
    second.register_function(_two, policy=PUBLIC, tenant_id="acme", **tenant)
    before = second.snapshot().id
    with pytest.raises(RegistryConflictError):
        second.register_function(_one, policy=PUBLIC, **shared)
    assert second.snapshot().id == before
    # 另一个租户不受这次冲突影响，可以正常使用相同的名字
    second.register_function(_one, policy=PUBLIC, tenant_id="globex", **tenant)


def test_adding_a_version_to_a_shared_tool_ignores_tenant_layers() -> None:
    """给已有的共享工具增加版本不做跨层检查（名字和别名在首次注册时已经检查过），也不会
    影响租户层。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="t", policy=PUBLIC)
    registry.register_function(_one, name="own", policy=PUBLIC, tenant_id="acme")
    registry.register_function(_two, name="t", policy=PUBLIC, version="2")
    registry.set_active("t", "2")
    assert registry.snapshot().for_tenant("acme").resolve("t").spec.version == "2"
    assert registry.snapshot().for_tenant("acme").resolve("own").spec.version == "1"


def test_mutations_are_scoped_to_one_layer() -> None:
    """启停、切换版本、删除都只作用于指定的作用域；在错误的作用域里找不到工具时报错，
    注册表不变。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="t", policy=PUBLIC, tenant_id="acme")
    registry.register_function(_two, name="t", policy=PUBLIC, tenant_id="acme", version="2")
    registry.register_function(_one, name="t", policy=PUBLIC, tenant_id="globex")

    before = registry.snapshot().id
    for call in (
        lambda: registry.disable("t"),  # 共享层没有这个工具
        lambda: registry.set_active("t", "2", tenant_id="globex"),  # globex 没有版本 2
        lambda: registry.unregister("t", tenant_id="initech"),  # 没有这个租户层
        lambda: registry.unregister("t", "1", tenant_id="acme"),  # 不能删除启用的版本
    ):
        with pytest.raises(RegistryError):
            call()
    assert registry.snapshot().id == before

    registry.set_active("t", "2", tenant_id="acme")
    registry.disable("t", tenant_id="globex")
    snapshot = registry.snapshot()
    assert snapshot.for_tenant("acme").resolve("t").spec.version == "2"
    with pytest.raises(ToolDisabledError):
        snapshot.for_tenant("globex").resolve("t")

    registry.unregister("t", "1", tenant_id="acme")
    registry.unregister("t", tenant_id="globex")
    assert registry.tenants() == ("acme",)  # 变空的租户层被移除
    registry.enable("t", False, tenant_id="acme")
    with pytest.raises(ToolDisabledError):
        registry.snapshot().for_tenant("acme").resolve("t")


def test_apply_on_a_tenant_layer_is_atomic() -> None:
    """租户层上的批量变更要么全部生效要么都不生效：其中一个绑定与共享工具冲突时，
    同批的删除和其他注册都不会发生。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="shared", policy=PUBLIC)
    registry.register_function(_one, name="old", policy=PUBLIC, tenant_id="acme")
    donor = ToolRegistry()
    donor.register_function(_two, name="fresh", policy=PUBLIC)
    donor.register_function(_two, name="shared", policy=PUBLIC, version="9")
    fresh = donor.snapshot().resolve("fresh")
    clash = donor.snapshot().resolve("shared", "9")

    before = registry.snapshot()
    with pytest.raises(RegistryConflictError):
        registry.apply(register=[fresh, clash], unregister=["old"], tenant_id="acme")
    assert registry.snapshot() is before

    after = registry.apply(register=[fresh], unregister=["old"], tenant_id="acme")
    assert int(after.id) == int(before.id) + 1  # 只生成一个新快照
    view = after.for_tenant("acme")
    assert sorted(view.entries) == ["fresh", "shared"]
    assert len(view.entries) == 2
    assert len(view.aliases) == 2


def test_snapshots_are_immutable_and_share_unchanged_layers() -> None:
    """已取得的租户视图不受后续修改影响；一个租户的变更不会重建其他租户层和共享层
    （新旧快照共享同一个不可变对象）。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="shared", policy=PUBLIC)
    registry.register_function(_one, name="a", policy=PUBLIC, tenant_id="acme")
    registry.register_function(_one, name="g", policy=PUBLIC, tenant_id="globex")
    old = registry.snapshot()
    old_view = old.for_tenant("acme")

    registry.register_function(_two, name="a2", policy=PUBLIC, tenant_id="acme")
    registry.unregister("a", tenant_id="acme")
    new = registry.snapshot()

    assert sorted(old_view.entries) == ["a", "shared"]
    assert old_view.resolve("a").spec.name == "a"
    assert sorted(new.for_tenant("acme").entries) == ["a2", "shared"]
    assert new.tenants["globex"] is old.tenants["globex"]
    assert new.entries is old.entries
    assert new.tenants["acme"] is not old.tenants["acme"]
    with pytest.raises(TypeError):
        new.tenants["x"] = old.tenants["acme"]  # type: ignore[index]
    with pytest.raises(TypeError):
        old_view.entries["z"] = old_view.entries["a"]  # type: ignore[index]


def test_tenant_view_cannot_be_rescoped() -> None:
    """租户视图不能再换成另一个租户（否则可能被误用来越权）；换成同一个租户返回自身，
    基础快照对没有租户层的租户返回自身。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="a", policy=PUBLIC, tenant_id="acme")
    registry.register_function(_one, name="g", policy=PUBLIC, tenant_id="globex")
    base = registry.snapshot()
    view = base.for_tenant("acme")
    assert view.tenant_id == "acme"
    assert view.id == base.id
    assert view.for_tenant("acme") is view
    assert not view.tenants
    with pytest.raises(ValueError):
        view.for_tenant("globex")
    with pytest.raises(ValueError):
        view.for_tenant(None)
    assert base.for_tenant(None) is base
    assert base.for_tenant("initech") is base
    assert base.for_tenant("") is base


@pytest.mark.parametrize("tenant_id", ["", "x" * 257, 7])
def test_invalid_tenant_ids_are_rejected(tenant_id: Any) -> None:
    """空字符串、超长或非字符串的租户 ID 在所有修改操作上都被拒绝，不会形成“匿名租户层”。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="t", policy=PUBLIC)
    for call in (
        lambda: registry.register_function(_one, name="u", policy=PUBLIC, tenant_id=tenant_id),
        lambda: registry.apply(tenant_id=tenant_id),
        lambda: registry.enable("t", tenant_id=tenant_id),
        lambda: registry.disable("t", tenant_id=tenant_id),
        lambda: registry.set_active("t", "1", tenant_id=tenant_id),
        lambda: registry.unregister("t", tenant_id=tenant_id),
        lambda: registry.drop_tenant(tenant_id),
    ):
        with pytest.raises(ValueError):
            call()
    with pytest.raises(ValueError):
        registry.drop_tenant(None)  # type: ignore[arg-type]
    assert registry.tenants() == ()


def test_drop_tenant_removes_only_that_layer() -> None:
    """删除租户层：返回被删除的名字，其他租户和共享层不变；重复删除不产生新快照。"""
    registry = ToolRegistry()
    registry.register_function(_one, name="shared", policy=PUBLIC)
    registry.register_function(_one, name="b", policy=PUBLIC, tenant_id="acme")
    registry.register_function(_one, name="a", policy=PUBLIC, tenant_id="acme")
    registry.register_function(_one, name="g", policy=PUBLIC, tenant_id="globex")

    assert registry.drop_tenant("acme") == ("a", "b")
    snapshot = registry.snapshot()
    assert registry.tenants() == ("globex",)
    assert sorted(snapshot.for_tenant("acme").entries) == ["shared"]
    assert sorted(snapshot.for_tenant("globex").entries) == ["g", "shared"]
    assert registry.drop_tenant("acme") == ()
    assert registry.snapshot() is snapshot
    # 层删除后，之前因它而冲突的共享名字可以注册
    registry.register_function(_one, name="a", policy=PUBLIC)


def test_destructive_rule_applies_to_tenant_tools() -> None:
    """不可逆工具的注册规则不因注册到租户层而放宽。"""
    registry = ToolRegistry()
    with pytest.raises(RegistryError):
        registry.register_function(
            _one,
            name="drop",
            policy=PUBLIC,
            annotations=ToolAnnotations(destructive=True),
            tenant_id="acme",
        )
    assert registry.tenants() == ()


def test_register_http_supports_tenant_scope() -> None:
    """HTTP 工具同样可以注册到租户层，三类工具使用同一套作用域规则。"""
    registry = ToolRegistry()
    target = HttpTarget(method="GET", base_url="https://api.example.com", path_template="/ping")
    schema = {"type": "object", "properties": {}, "additionalProperties": False}
    spec = registry.register_http(
        target, name="api.ping", input_schema=schema, policy=PUBLIC, tenant_id="acme"
    )
    assert spec.kind == "http"
    assert registry.snapshot().for_tenant("acme").resolve("api_ping").spec is spec
    with pytest.raises(ToolNotFoundError):
        registry.snapshot().resolve("api_ping")


def test_concurrent_registration_keeps_every_view_unambiguous() -> None:
    """多线程并发注册：每个名字同时被注册为共享工具和某个租户的工具时恰好一方成功；不相干的
    租户全部成功；最终每个租户视图中没有重名（冲突检查与发布在同一把锁内）。"""
    registry = ToolRegistry(history=4)
    names = [f"dup{i}" for i in range(40)]
    outcomes: dict[tuple[str, str | None], bool] = {}
    start = threading.Barrier(3)

    def worker(tenant_id: str | None) -> None:
        """在一个作用域里依次注册全部名字，记录每次是否成功。"""
        start.wait()
        for name in names:
            try:
                registry.register_function(_one, name=name, policy=PUBLIC, tenant_id=tenant_id)
                outcomes[(name, tenant_id)] = True
            except RegistryConflictError:
                outcomes[(name, tenant_id)] = False

    threads = [threading.Thread(target=worker, args=(t,)) for t in (None, "acme", "globex")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    snapshot = registry.snapshot()
    for name in names:
        shared_won = outcomes[(name, None)]
        # 共享层成功 ⇔ 两个租户都失败；否则至少一个租户先注册成功
        assert shared_won != (outcomes[(name, "acme")] or outcomes[(name, "globex")])
        assert (name in snapshot.entries) == shared_won
    for tenant_id in ("acme", "globex"):
        view = snapshot.for_tenant(tenant_id)
        keys = list(view.entries)
        assert len(keys) == len(set(keys)) == len(view.entries)
        assert set(keys) == set(names)
        assert all(view.resolve(name).spec.name == name for name in names)
    assert int(snapshot.id) == 1 + sum(outcomes.values())


# ---------------------------------------------------------------------------------
# 执行器
# ---------------------------------------------------------------------------------
def tenant_executor(**kw: Any) -> tuple[ToolExecutor, dict[str, int]]:
    """创建带共享工具和两个租户同名工具的执行器。

    Args:
        **kw: 传给 ``ToolExecutor`` 的其他参数。

    Returns:
        ``(执行器, 各工具的执行次数)``；``acme`` 与 ``globex`` 的 ``crm.search`` 同名同别名，
        返回值与策略不同（``globex`` 的需要 ``crm`` 权限）。
    """
    calls = {"shared": 0, "acme": 0, "globex": 0}

    async def shared() -> str:
        """共享工具。"""
        calls["shared"] += 1
        return "shared"

    async def acme_search(q: str) -> str:
        """acme 的搜索。"""
        calls["acme"] += 1
        return f"acme:{q}"

    async def globex_search(query: str, limit: int = 5) -> str:
        """globex 的搜索（参数与 acme 不同）。"""
        calls["globex"] += 1
        return f"globex:{query}:{limit}"

    registry = ToolRegistry()
    registry.register_function(shared, name="common.ping", policy=PUBLIC)
    registry.register_function(acme_search, name="crm.search", policy=PUBLIC, tenant_id="acme")
    registry.register_function(
        globex_search,
        name="crm.search",
        policy=ToolPolicy(access="scoped", required_scopes=frozenset({"crm"})),
        tenant_id="globex",
    )
    return ToolExecutor(registry, **kw), calls


async def test_list_tools_shows_each_tenant_its_own_catalog() -> None:
    """列表按调用方租户取视图：同一个别名对不同租户是不同的定义；没有租户或未知租户只看到
    共享工具；列表仍按权限过滤。"""
    executor, _ = tenant_executor()

    def listing(principal: Principal) -> dict[str, list[str]]:
        """返回 ``别名 → 参数名`` 的列表摘要。"""
        return {
            t.alias: sorted(t.input_schema["properties"])
            for t in executor.list_tools(principal).tools
        }

    assert listing(Principal(tenant_id="acme")) == {"common_ping": [], "crm_search": ["q"]}
    assert listing(Principal(tenant_id="globex", scopes=frozenset({"crm"}))) == {
        "common_ping": [],
        "crm_search": ["limit", "query"],
    }
    assert listing(Principal(tenant_id="globex")) == {"common_ping": []}  # 没有 crm 权限
    assert listing(Principal(tenant_id="initech")) == {"common_ping": []}
    assert listing(Principal()) == {"common_ping": []}
    await executor.aclose()


async def test_execute_resolves_in_the_callers_tenant_view() -> None:
    """执行按调用方租户解析：各租户得到自己的实现并按自己的 Schema 校验；其他租户和无租户的
    调用得到 not_found（与工具不存在无法区分），工具不执行；解析成功后仍然鉴权。"""
    executor, calls = tenant_executor()

    acme = await executor.execute(invoke("crm_search", q="x"), ctx_of("acme"))
    assert acme.to_model_text() == "acme:x"
    globex = await executor.execute(
        invoke("crm_search", query="y"), ctx_of("globex", scopes=frozenset({"crm"}))
    )
    assert globex.to_model_text() == "globex:y:5"

    wrong_schema = await executor.execute(invoke("crm_search", query="y"), ctx_of("acme"))
    assert wrong_schema.error is not None and wrong_schema.error.type == "invalid_arguments"
    denied = await executor.execute(invoke("crm_search", query="y"), ctx_of("globex"))
    assert denied.error is not None and denied.error.type == "permission"

    for ctx in (ctx_of("initech"), ctx_of(None)):
        for ref in ("crm_search", "crm.search"):
            missing = await executor.execute(invoke(ref, q="x"), ctx)
            assert missing.error is not None and missing.error.type == "not_found"
        assert (await executor.execute(invoke("common_ping"), ctx)).ok
    assert calls == {"shared": 2, "acme": 1, "globex": 1}
    await executor.aclose()


async def test_snapshot_id_cannot_be_used_across_tenants() -> None:
    """``snapshot_id`` 只确定时刻：用 acme 列表的编号执行时，目录仍由 ``ctx`` 的租户决定；
    之后另一个租户的变更不影响这个编号对应的版本，直到超出保留范围后才失效。"""
    executor, calls = tenant_executor()
    registry = executor.registry
    listing = executor.list_tools(Principal(tenant_id="acme"))

    # 列表之后：acme 的工具换成新版本，globex 也发生变更
    async def acme_v2(q: str) -> str:
        """acme 搜索的新版本。"""
        return f"acme-v2:{q}"

    registry.register_function(
        acme_v2, name="crm.search", policy=PUBLIC, tenant_id="acme", version="2"
    )
    registry.set_active("crm.search", "2", tenant_id="acme")
    registry.disable("crm.search", tenant_id="globex")

    batch = [invoke("crm_search", "c1", q="a"), invoke("common_ping", "c2")]
    pinned = await executor.execute_many(batch, ctx_of("acme"), snapshot_id=listing.snapshot_id)
    assert [r.to_model_text() for r in pinned] == ["acme:a", "shared"]
    assert pinned[0].version == "1"
    current = await executor.execute_many(batch, ctx_of("acme"))
    assert current[0].to_model_text() == "acme-v2:a"

    stolen = await executor.execute_many(batch, ctx_of("initech"), snapshot_id=listing.snapshot_id)
    assert stolen[0].error is not None and stolen[0].error.type == "not_found"
    assert stolen[1].ok
    assert calls["acme"] == 1

    for i in range(8):  # 默认保留 8 个快照：其他租户的变更也会占用名额
        registry.enable("crm.search", i % 2 == 0, tenant_id="globex")
    expired = await executor.execute_many(batch, ctx_of("acme"), snapshot_id=listing.snapshot_id)
    assert all(r.error is not None and r.error.code == "snapshot_expired" for r in expired)
    await executor.aclose()


async def test_inflight_batch_keeps_its_tenant_view() -> None:
    """批量执行开始后租户层被删除：这一批仍按开始时的视图执行完，之后的调用看不到该工具。"""
    registry = ToolRegistry()
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow() -> str:
        """等到测试放行。"""
        entered.set()
        await release.wait()
        return "slow"

    registry.register_function(slow, name="slow", policy=PUBLIC, tenant_id="acme")
    registry.register_function(_one, name="after", policy=PUBLIC, tenant_id="acme")
    executor = ToolExecutor(registry)

    task = asyncio.create_task(
        executor.execute_many([invoke("slow", "c1"), invoke("after", "c2")], ctx_of("acme"))
    )
    await entered.wait()
    assert registry.drop_tenant("acme") == ("after", "slow")
    release.set()
    results = await task
    assert [r.to_model_text() for r in results] == ["slow", "one"]
    gone = await executor.execute(invoke("after"), ctx_of("acme"))
    assert gone.error is not None and gone.error.type == "not_found"
    await executor.aclose()


async def test_result_cache_is_not_shared_between_same_named_tenant_tools() -> None:
    """两个租户的同名同版本工具使用租户范围的结果缓存：缓存键含租户，互不命中对方的结果。"""
    registry = ToolRegistry()
    calls: list[str] = []
    policy = ToolPolicy(access="public", cache=CachePolicy(ttl_s=60, scope="tenant"))
    read_only = ToolAnnotations(read_only=True)

    async def acme_report() -> str:
        """acme 的报表。"""
        calls.append("acme")
        return "acme-data"

    async def globex_report() -> str:
        """globex 的报表。"""
        calls.append("globex")
        return "globex-data"

    for tenant_id, fn in (("acme", acme_report), ("globex", globex_report)):
        registry.register_function(
            fn, name="report", policy=policy, annotations=read_only, tenant_id=tenant_id
        )
    executor = ToolExecutor(registry, result_cache=InMemoryResultStore())
    texts = [
        (await executor.execute(invoke("report"), ctx_of(tenant))).to_model_text()
        for tenant in ("acme", "globex", "acme", "globex")
    ]
    assert texts == ["acme-data", "globex-data", "acme-data", "globex-data"]
    assert calls == ["acme", "globex"]  # 各执行一次，第二轮各自命中自己的缓存
    await executor.aclose()


# ---------------------------------------------------------------------------------
# MCP：按租户发现并写入租户层
# ---------------------------------------------------------------------------------
#: 当前 HTTP 请求带来的 Authorization 请求头（由 ASGI 包装层设置，服务端处理函数读取）。
REQUEST_AUTH: contextvars.ContextVar[str] = contextvars.ContextVar("catalog_auth", default="")


def token(tenant: str | None) -> str:
    """生成某个租户的测试凭证（足够长，会被精确清除）。

    Args:
        tenant: 租户 ID；``None`` 表示服务端级凭证。

    Returns:
        凭证值。
    """
    return f"token-{tenant or 'server'}-0123456789abcdef"


def _schema(**properties: str) -> dict[str, Any]:
    """生成远端工具的参数 Schema。

    Args:
        **properties: 参数名 → JSON 类型。

    Returns:
        对象类型的 Schema。
    """
    return {"type": "object", "properties": {k: {"type": v} for k, v in properties.items()}}


@dataclass
class Catalogs:
    """按租户返回不同工具集的内存 MCP 服务端的状态。

    Attributes:
        tools: ``Authorization`` 值 → {远端工具名: 参数 Schema}；测试可以修改它来模拟远端变化。
        listed: 每次 ``tools/list`` 请求携带的 Authorization。
        called: 每次工具调用的 ``(Authorization, 工具名, 参数 JSON)``。
        opened: 客户端工厂每次建立会话时收到的 Authorization。
    """

    tools: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    listed: list[str] = field(default_factory=list)
    called: list[tuple[str, str, str]] = field(default_factory=list)
    opened: list[str] = field(default_factory=list)


class TenantTokens:
    """按调用上下文的租户返回凭证的密钥提供者。"""

    async def get_secret(self, name: str, ctx: ToolContext) -> str:
        """返回上下文所属租户的凭证。

        Args:
            name: 密钥名（未使用）。
            ctx: 调用上下文。

        Returns:
            凭证值。
        """
        return token(ctx.principal.tenant_id)


@pytest.fixture
async def catalogs() -> AsyncIterator[tuple[Catalogs, Any]]:
    """启动内存中的 Streamable HTTP 服务端：按请求的凭证返回对应租户的工具集。

    Yields:
        ``(服务端状态, http_client_factory)``。初始目录：``acme`` 有 ``search(q)`` 与
        ``acme_only``；``globex`` 有 ``search(query, limit)`` 与 ``export``；``ops`` 有全部四个名字。
    """
    state = Catalogs(
        tools={
            f"Bearer {token('acme')}": {"search": _schema(q="string"), "acme_only": _schema()},
            f"Bearer {token('globex')}": {
                "search": _schema(query="string", limit="integer"),
                "export": _schema(),
            },
            f"Bearer {token('ops')}": {
                "search": _schema(q="string"),
                "acme_only": _schema(),
                "export": _schema(),
            },
            f"Bearer {token(None)}": {"search": _schema(q="string")},
        }
    )

    async def list_tools(ctx: Any, params: Any) -> mt.ListToolsResult:
        """返回当前请求凭证对应的工具集。"""
        auth = REQUEST_AUTH.get()
        state.listed.append(auth)
        return mt.ListToolsResult(
            tools=[
                mt.Tool(name=name, description=f"{name} tool", input_schema=schema)
                for name, schema in state.tools.get(auth, {}).items()
            ]
        )

    async def call_tool(ctx: Any, params: mt.CallToolRequestParams) -> mt.CallToolResult:
        """记录调用；凭证对应的目录里没有这个工具时返回工具错误。"""
        auth = REQUEST_AUTH.get()
        arguments = json.dumps(params.arguments or {}, sort_keys=True)
        state.called.append((auth, params.name, arguments))
        known = params.name in state.tools.get(auth, {})
        return mt.CallToolResult(
            content=[mt.TextContent(type="text", text=f"{params.name}:{arguments}")],
            is_error=not known,
        )

    server = Server("catalogs", on_list_tools=list_tools, on_call_tool=call_tool)
    app = server.streamable_http_app(stateless_http=True, json_response=True)

    async def capture(scope: Any, receive: Any, send: Any) -> None:
        """把请求的 Authorization 放进上下文变量后转交给 MCP 应用。"""
        if scope["type"] == "http":
            headers = {k.decode(): v.decode() for k, v in scope["headers"]}
            REQUEST_AUTH.set(headers.get("authorization", ""))
        await app(scope, receive, send)

    def factory(cfg: HttpTransportConfig, headers: Any) -> httpx2.AsyncClient:
        """创建连接内存应用的 HTTP 客户端，并记录这条会话使用的凭证。"""
        state.opened.append(dict(headers).get("Authorization", ""))
        return httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=capture),
            base_url="http://localhost:8000",
            headers=dict(headers),
        )

    ready, stop = asyncio.Event(), asyncio.Event()

    async def serve() -> None:
        """在后台任务中运行 ASGI 应用的 lifespan，直到测试结束。"""
        async with app.router.lifespan_context(app):
            ready.set()
            await stop.wait()

    task = asyncio.create_task(serve())
    await ready.wait()
    yield state, factory
    stop.set()
    await task


async def _loopback(host: str, port: int) -> list[str]:
    """测试 DNS：解析到回环地址（网络策略显式放行 127.0.0.0/8）。"""
    return ["127.0.0.1"]


ALLOW = [McpToolAllow(remote_name=n, policy=PUBLIC) for n in ("search", "acme_only", "export")]


@dataclass
class McpEnv:
    """一套 MCP 测试环境。

    Attributes:
        state: 服务端状态。
        registry: 注册表。
        manager: MCP 服务端管理器。
        sync: 同步器。
        executor: 执行器。
    """

    state: Catalogs
    registry: ToolRegistry
    manager: McpServerManager
    sync: McpToolSync
    executor: ToolExecutor

    async def aclose(self) -> None:
        """关闭执行器（连带关闭 MCP 适配器与全部会话）。"""
        await self.executor.aclose()

    def aliases(self, tenant: str | None) -> list[str]:
        """返回某个租户通过执行器看到的工具别名。

        Args:
            tenant: 租户 ID。

        Returns:
            别名列表（按内部名排序）。
        """
        return [t.alias for t in self.executor.list_tools(Principal(tenant_id=tenant)).tools]

    async def call(self, tenant: str | None, tool: str, **arguments: Any) -> Any:
        """以某个租户的身份调用一个远端工具。

        Args:
            tenant: 租户 ID。
            tool: 远端工具名。
            **arguments: 工具参数。

        Returns:
            调用结果。
        """
        return await self.executor.execute(
            invoke(f"mcp_remote_{tool}", **arguments), ctx_of(tenant)
        )


def mcp_env(catalogs: tuple[Catalogs, Any], session_scope: str = "tenant") -> McpEnv:
    """创建指向内存服务端的管理器、同步器与执行器（不做任何发现）。

    Args:
        catalogs: ``catalogs`` 夹具的返回值。
        session_scope: MCP 会话范围。

    Returns:
        测试环境。
    """
    state, factory = catalogs
    network = NetworkPolicy(
        allowed_hosts=frozenset({"localhost"}),
        allowed_ports=frozenset({8000}),
        allow_http=True,
        allowed_networks=("127.0.0.0/8",),
    )
    server = McpServerConfig(
        server_id="remote",
        transport=HttpTransportConfig(
            url="http://localhost:8000/mcp", network=network, auth=SecretRef(name="api")
        ),
        session_scope=session_scope,  # type: ignore[arg-type]
    )
    manager = McpServerManager(
        [server], secrets=TenantTokens(), resolver=_loopback, http_client_factory=factory
    )
    registry = ToolRegistry()
    executor = ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)})
    return McpEnv(state, registry, manager, McpToolSync(registry, manager), executor)


async def test_tenant_sync_registers_each_tenants_own_tool_set(catalogs: Any) -> None:
    """按租户同步：每个租户用自己的凭证发现，目录只含自己服务端返回的工具；同名工具的 Schema
    按租户不同；共享层和未同步的租户没有这些工具。"""
    env = mcp_env(catalogs)
    acme = await env.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="tenant")
    globex = await env.sync.sync("remote", ALLOW, ctx=ctx_of("globex"), scope="tenant")

    assert (acme.tenant_id, globex.tenant_id) == ("acme", "globex")
    assert set(acme.registered) == {"mcp.remote.search", "mcp.remote.acme_only"}
    assert acme.missing == ("export",)
    assert set(globex.registered) == {"mcp.remote.search", "mcp.remote.export"}
    assert env.state.listed == [f"Bearer {token('acme')}", f"Bearer {token('globex')}"]

    assert env.aliases("acme") == ["mcp_remote_acme_only", "mcp_remote_search"]
    assert env.aliases("globex") == ["mcp_remote_export", "mcp_remote_search"]
    assert env.aliases("initech") == []
    assert env.aliases(None) == []
    assert not env.registry.snapshot().entries
    assert env.sync.registered("remote") == {}
    assert env.sync.registered("remote", tenant_id="acme") == {
        "search": "mcp.remote.search",
        "acme_only": "mcp.remote.acme_only",
    }

    snapshot = env.registry.snapshot()
    a = snapshot.for_tenant("acme").resolve("mcp_remote_search").spec
    g = snapshot.for_tenant("globex").resolve("mcp_remote_search").spec
    assert set(a.input_schema["properties"]) == {"q"}
    assert set(g.input_schema["properties"]) == {"query", "limit"}
    assert a.version != g.version  # 定义不同 → 指纹版本不同（缓存与去重键随之不同）
    await env.aclose()


async def test_tenant_tools_execute_with_own_schema_session_and_credentials(catalogs: Any) -> None:
    """执行：每个租户按自己的 Schema 校验，用自己的会话和凭证调用；调用其他租户才有的工具得到
    not_found，且不会为此建立会话或向服务端发出请求；没有租户的调用同样解析不到。"""
    env = mcp_env(catalogs)
    for tenant in ("acme", "globex"):
        await env.sync.sync("remote", ALLOW, ctx=ctx_of(tenant), scope="tenant")
    opened = len(env.state.opened)

    assert (await env.call("acme", "search", q="a")).ok
    assert (await env.call("globex", "search", query="g", limit=3)).ok
    bad = await env.call("acme", "search", query="g")
    assert bad.error is not None and bad.error.type == "invalid_arguments"

    for tenant, tool in (("acme", "export"), ("globex", "acme_only"), (None, "search")):
        missing = await env.call(tenant, tool)
        assert missing.error is not None and missing.error.type == "not_found"
    stranger = await env.call("initech", "search", q="a")
    assert stranger.error is not None and stranger.error.type == "not_found"

    assert env.state.called == [
        (f"Bearer {token('acme')}", "search", '{"q": "a"}'),
        (f"Bearer {token('globex')}", "search", '{"limit": 3, "query": "g"}'),
    ]
    assert len(env.state.opened) == opened  # 被拒绝的调用没有建立新会话
    assert env.manager.connected("remote", tenant_id="initech") is False
    await env.aclose()


async def test_tenant_refresh_does_not_touch_other_tenants(catalogs: Any) -> None:
    """刷新一个租户：远端删除和变更的工具只在该租户的层中更新；其他租户的层对象保持不变；
    空白名单移除该租户在这个服务端上的全部工具。"""
    env = mcp_env(catalogs)
    for tenant in ("acme", "globex"):
        await env.sync.sync("remote", ALLOW, ctx=ctx_of(tenant), scope="tenant")
    globex_layer = env.registry.snapshot().tenants["globex"]
    old_version = env.registry.snapshot().for_tenant("acme").resolve("mcp_remote_search").spec

    acme_tools = env.state.tools[f"Bearer {token('acme')}"]
    del acme_tools["acme_only"]
    acme_tools["search"] = _schema(q="string", page="integer")
    report = await env.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="tenant")

    assert report.registered == ("mcp.remote.search",)
    assert report.removed == ("mcp.remote.acme_only",)
    assert env.aliases("acme") == ["mcp_remote_search"]
    new_version = env.registry.snapshot().for_tenant("acme").resolve("mcp_remote_search").spec
    assert new_version.version != old_version.version
    assert env.registry.snapshot().tenants["globex"] is globex_layer
    assert env.aliases("globex") == ["mcp_remote_export", "mcp_remote_search"]

    unchanged = await env.sync.sync("remote", ALLOW, ctx=ctx_of("globex"), scope="tenant")
    assert set(unchanged.unchanged) == {"mcp.remote.search", "mcp.remote.export"}
    assert unchanged.registered == unchanged.removed == ()

    cleared = await env.sync.sync("remote", [], ctx=ctx_of("acme"), scope="tenant")
    assert cleared.removed == ("mcp.remote.search",)
    assert env.registry.tenants() == ("globex",)
    await env.aclose()


@pytest.mark.parametrize("ctx", [None, ctx_of(None), ctx_of("")])
async def test_tenant_sync_without_a_tenant_fails_closed(catalogs: Any, ctx: Any) -> None:
    """租户级同步没有租户时在连接之前拒绝：不建立会话、不读取工具列表、注册表不变，
    也不会退回共享层。服务端范围的会话同样如此。"""
    env = mcp_env(catalogs, session_scope="server")
    before = env.registry.snapshot()
    with pytest.raises(PermissionDeniedError) as info:
        await env.sync.sync("remote", ALLOW, ctx=ctx, scope="tenant")
    assert info.value.code == "mcp_tenant_required"
    assert env.registry.snapshot() is before
    assert env.state.opened == [] and env.state.listed == []
    with pytest.raises(ValueError):
        await env.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="user")  # type: ignore[arg-type]
    await env.aclose()


async def test_shared_and_tenant_sync_of_the_same_tool_conflict(catalogs: Any) -> None:
    """同一个工具不能既在共享层又在租户层：无论先后，后一次同步整体失败、注册表不变；
    为租户级工具显式指定名字和别名后可以并存。"""
    env = mcp_env(catalogs)
    await env.sync.sync("remote", ALLOW, ctx=ctx_of("ops"))  # 默认 scope="shared"
    before = env.registry.snapshot()
    with pytest.raises(RegistryConflictError):
        await env.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="tenant")
    assert env.registry.snapshot() is before
    assert env.sync.registered("remote", tenant_id="acme") == {}

    renamed = [
        McpToolAllow(remote_name="search", policy=PUBLIC, name="acme.search", alias="acme_search")
    ]
    await env.sync.sync("remote", renamed, ctx=ctx_of("acme"), scope="tenant")
    assert "acme_search" in env.aliases("acme")
    assert "mcp_remote_search" in env.aliases("acme")  # 共享层的那个仍然可见
    assert "acme_search" not in env.aliases("globex")
    await env.aclose()

    reverse = mcp_env(catalogs)
    await reverse.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="tenant")
    before = reverse.registry.snapshot()
    with pytest.raises(RegistryConflictError):
        await reverse.sync.sync("remote", ALLOW, ctx=ctx_of("ops"))
    assert reverse.registry.snapshot() is before
    await reverse.aclose()


async def test_tenant_sync_does_not_shadow_local_tools(catalogs: Any) -> None:
    """租户级同步不能替换宿主的本地工具：共享层或本租户层里已有同名的非 MCP 工具时同步失败，
    本地工具保持原样。"""
    env = mcp_env(catalogs)
    env.registry.register_function(_one, name="mcp.remote.search", policy=PUBLIC)
    env.registry.register_function(
        _two, name="mcp.remote.export", policy=PUBLIC, tenant_id="globex"
    )
    before = env.registry.snapshot()
    for tenant in ("acme", "globex"):
        with pytest.raises(RegistryConflictError):
            await env.sync.sync("remote", ALLOW, ctx=ctx_of(tenant), scope="tenant")
    assert env.registry.snapshot() is before
    assert (await env.call("globex", "export")).to_model_text() == "two"
    await env.aclose()


async def test_resync_after_drop_tenant(catalogs: Any) -> None:
    """租户层被整体删除后，同步器里的旧记录不会导致误删：再次同步重新注册全部工具；即使此时
    共享层出现了同名工具，也只报告冲突而不会去删除共享工具。"""
    env = mcp_env(catalogs)
    await env.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="tenant")
    assert env.registry.drop_tenant("acme") == ("mcp.remote.acme_only", "mcp.remote.search")
    assert env.aliases("acme") == []

    again = await env.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="tenant")
    assert set(again.registered) == {"mcp.remote.search", "mcp.remote.acme_only"}
    assert again.removed == ()

    env.registry.drop_tenant("acme")
    env.registry.register_function(_one, name="mcp.remote.search", policy=PUBLIC)
    with pytest.raises(RegistryConflictError):
        await env.sync.sync("remote", ALLOW, ctx=ctx_of("acme"), scope="tenant")
    assert env.registry.snapshot().resolve("mcp.remote.search").spec.kind == "python"
    await env.aclose()


async def test_concurrent_tenant_syncs_and_calls(catalogs: Any) -> None:
    """并发：多个租户同时同步（含同一租户的重复同步）并同时发起调用。每个租户最终只看到自己的
    目录，每次调用只使用调用方租户的凭证，没有任何调用落到其他租户的工具上。"""
    env = mcp_env(catalogs)
    tenants = ("acme", "globex")
    await asyncio.gather(
        *(
            env.sync.sync("remote", ALLOW, ctx=ctx_of(tenant), scope="tenant")
            for tenant in tenants
            for _ in range(3)
        )
    )
    assert env.aliases("acme") == ["mcp_remote_acme_only", "mcp_remote_search"]
    assert env.aliases("globex") == ["mcp_remote_export", "mcp_remote_search"]

    async def churn(tenant: str) -> None:
        """反复刷新一个租户的目录。"""
        for _ in range(3):
            await env.sync.sync("remote", ALLOW, ctx=ctx_of(tenant), scope="tenant")

    calls = [
        env.call("acme", "search", q=f"a{i}") if i % 2 == 0 else env.call("globex", "export")
        for i in range(12)
    ]
    cross = [env.call("acme", "export"), env.call("globex", "acme_only")]
    results = await asyncio.gather(*calls, *cross, churn("acme"), churn("globex"))
    assert all(r.ok for r in results[:12])
    assert all(r.error is not None and r.error.type == "not_found" for r in results[12:14])
    for auth, name, _ in env.state.called:
        assert (auth, name) in {
            (f"Bearer {token('acme')}", "search"),
            (f"Bearer {token('globex')}", "export"),
        }
    assert len(env.state.called) == 12
    await env.aclose()


async def test_shared_sync_default_is_unchanged(catalogs: Any) -> None:
    """默认作用域仍是共享层：用运维租户发现的目录对所有租户可见（原有行为），报告不带租户；
    某个租户的服务端实际没有该工具时，调用到达服务端并得到工具错误。"""
    env = mcp_env(catalogs)
    report = await env.sync.sync("remote", ALLOW, ctx=ctx_of("ops"))
    assert report.tenant_id is None
    assert env.registry.tenants() == ()
    assert env.aliases("acme") == env.aliases("globex") == env.aliases("initech")
    assert len(env.aliases("acme")) == 3
    result = await env.call("acme", "export")
    assert result.error is not None and result.error.type == "execution"
    assert env.state.called[-1][:2] == (f"Bearer {token('acme')}", "export")
    await env.aclose()
