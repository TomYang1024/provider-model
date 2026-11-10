"""验证审计事件与故障策略对执行的影响。"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agent_tools import (
    AuthzDecision,
    ExecutorConfig,
    Principal,
    ToolContext,
    ToolExecutor,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
    ToolSpec,
)
from agent_tools.observability.audit import AuditEvent


class Sink:
    """记录事件并可模拟指定阶段失败。"""

    def __init__(self, fail: str = "") -> None:
        """设置需要模拟失败的阶段。"""
        self.fail = fail
        self.events: list[AuditEvent] = []

    async def emit(self, event: AuditEvent) -> None:
        """记录事件；匹配阶段时模拟写入失败。"""
        self.events.append(event)
        if event.kind == self.fail:
            raise RuntimeError("sensitive diagnostic")


@pytest.mark.parametrize(
    ("mode", "fail", "expected", "calls"),
    [
        ("fail_open", "decision", "success", 1),
        ("fail_closed", "decision", "error", 0),
        ("fail_closed", "result", "success", 1),
    ],
)
async def test_audit_failure_modes(mode: str, fail: str, expected: str, calls: int) -> None:
    """验证执行前与执行后的审计故障遵循不同策略。"""
    executed: list[bool] = []

    def operation() -> str:
        """记录实际执行次数。"""
        executed.append(True)
        return "ok"

    registry = ToolRegistry()
    registry.register_function(operation, name="op", policy=ToolPolicy(access="public"))
    sink = Sink(fail)
    config = ExecutorConfig(audit_failure_mode=mode)  # type: ignore[arg-type]
    async with ToolExecutor(registry, config=config, audit=sink) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="op", arguments={}), ToolContext()
        )
    assert result.status == expected
    assert len(executed) == calls
    assert [e.kind for e in sink.events] == ["decision", "result"]


def test_fail_closed_requires_sink() -> None:
    """缺少审计接收端时禁止伪装成 fail_closed。"""
    with pytest.raises(ValueError, match="requires an audit sink"):
        ToolExecutor(ToolRegistry(), config=ExecutorConfig(audit_failure_mode="fail_closed"))


async def test_audit_timeout_fails_closed() -> None:
    """授权审计超时后不能执行工具。"""

    class SlowSink:
        """模拟无限等待的审计接收端。"""

        async def emit(self, event: AuditEvent) -> None:
            """等待直至超时或取消。"""
            await asyncio.Event().wait()

    registry = ToolRegistry()

    def operation() -> str:
        """如果执行则暴露测试失败。"""
        raise AssertionError("must not execute")

    registry.register_function(operation, name="op", policy=ToolPolicy(access="public"))
    async with ToolExecutor(
        registry,
        config=ExecutorConfig(audit_failure_mode="fail_closed", audit_timeout_s=0.01),
        audit=SlowSink(),
    ) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="op", arguments={}), ToolContext()
        )
    assert result.error is not None and result.error.code == "audit_unavailable"


async def test_argument_authorization_blocks_execution() -> None:
    """参数级授权拒绝时记录审计并且不调用工具。"""
    registry = ToolRegistry()
    called: list[int] = []

    def operation(value: int) -> str:
        """记录执行并返回结果。"""
        called.append(value)
        return "ok"

    def check_args(principal: Principal, spec: ToolSpec, args: dict[str, Any]) -> AuthzDecision:
        """拒绝值为 42 的调用。"""
        return AuthzDecision(args["value"] != 42, "restricted")

    registry.register_function(operation, name="op", policy=ToolPolicy(access="public"))
    sink = Sink()
    async with ToolExecutor(registry, audit=sink, authorize_args=check_args) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="op", arguments={"value": 42}), ToolContext()
        )
    assert result.error is not None and result.error.code == "permission_denied"
    assert called == []
    assert [(e.kind, e.outcome) for e in sink.events] == [
        ("decision", "allowed"),
        ("decision", "denied"),
        ("result", "error"),
    ]


class Recorder:
    """记录全部事件的审计接收端，可按授权原因模拟失败。"""

    def __init__(self, fail_reason: str | None = None) -> None:
        """创建接收端。

        Args:
            fail_reason: ``decision`` 事件的 ``reason`` 等于它时模拟写入失败。
        """
        self.fail_reason = fail_reason
        self.events: list[AuditEvent] = []

    async def emit(self, event: AuditEvent) -> None:
        """记录事件；匹配原因时模拟失败。

        Args:
            event: 审计事件。
        """
        self.events.append(event)
        if self.fail_reason is not None and event.reason == self.fail_reason:
            raise RuntimeError("audit backend down")


async def test_denied_decision_and_result_metadata_without_content() -> None:
    """工具级授权拒绝：记录拒绝原因和带错误类型的结果；事件中没有参数、结果内容或密钥。"""
    registry = ToolRegistry()

    async def secret_op(token: str) -> str:
        """测试用：需要权限、参数里带敏感值的工具。"""
        return f"used {token}"

    registry.register_function(
        secret_op,
        name="op",
        policy=ToolPolicy(access="scoped", required_scopes=frozenset({"admin"})),
    )
    sink = Recorder()
    async with ToolExecutor(registry, audit=sink) as executor:
        denied = await executor.execute(
            ToolInvocation(call_id="c1", tool="op", arguments={"token": "SECRET-ARG"}),
            ToolContext(principal=Principal(tenant_id="t1")),
        )
        allowed = await executor.execute(
            ToolInvocation(call_id="c2", tool="op", arguments={"token": "SECRET-ARG"}),
            ToolContext(principal=Principal(scopes=frozenset({"admin"}))),
        )
    assert denied.error and allowed.ok
    first, second, third, fourth = sink.events
    assert (first.kind, first.outcome, first.reason, first.tenant_id) == (
        "decision",
        "denied",
        "missing_scopes",
        "t1",
    )
    assert (second.kind, second.outcome, second.error_type, second.error_code, second.attempts) == (
        "result",
        "error",
        "permission",
        "permission_denied",
        0,
    )
    assert (third.outcome, third.reason) == ("allowed", "scopes_satisfied")
    assert (fourth.outcome, fourth.attempts, fourth.error_type) == ("success", 1, None)
    assert "SECRET-ARG" not in repr(sink.events)


async def test_fail_closed_blocks_when_argument_decision_audit_fails() -> None:
    """fail_closed：参数级授权决定的审计写入失败时，工具不会执行。"""
    executed: list[bool] = []

    def op(value: int) -> str:
        """测试用：记录是否被执行。"""
        executed.append(True)
        return "ok"

    def allow_all(principal: Principal, spec: ToolSpec, args: dict[str, Any]) -> AuthzDecision:
        """测试用：放行所有参数。"""
        return AuthzDecision(True, "ok")

    registry = ToolRegistry()
    registry.register_function(op, name="op", policy=ToolPolicy(access="public"))
    sink = Recorder(fail_reason="arguments")
    async with ToolExecutor(
        registry,
        audit=sink,
        authorize_args=allow_all,
        config=ExecutorConfig(audit_failure_mode="fail_closed"),
    ) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="op", arguments={"value": 1}), ToolContext()
        )
    assert result.error and result.error.code == "audit_unavailable"
    assert executed == []


async def test_argument_authorizer_exception_fails_closed() -> None:
    """参数级授权回调抛出异常时视为拒绝（内部错误），工具不会执行，异常信息不会出现在结果中。"""
    executed: list[bool] = []

    def op(value: int) -> str:
        """测试用：记录是否被执行。"""
        executed.append(True)
        return "ok"

    def broken(principal: Principal, spec: ToolSpec, args: dict[str, Any]) -> AuthzDecision:
        """测试用：模拟授权系统故障。"""
        raise RuntimeError("policy engine exploded")

    registry = ToolRegistry()
    registry.register_function(op, name="op", policy=ToolPolicy(access="public"))
    async with ToolExecutor(registry, authorize_args=broken) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="op", arguments={"value": 1}), ToolContext()
        )
    assert result.error and result.error.type == "internal"
    assert "exploded" not in result.error.message
    assert executed == []


@pytest.mark.parametrize("block_kind", ["decision", "result"])
async def test_cancellation_during_audit_write_is_not_swallowed(block_kind: str) -> None:
    """审计写入期间调用方取消：CancelledError 原样传播；在授权决定阶段取消时工具不会执行。"""
    started = asyncio.Event()
    executed: list[bool] = []

    class BlockingSink:
        """在指定阶段一直阻塞的审计接收端。"""

        async def emit(self, event: AuditEvent) -> None:
            """在指定阶段阻塞，等待被取消。"""
            if event.kind == block_kind:
                started.set()
                await asyncio.Event().wait()

    def op() -> str:
        """测试用：记录是否被执行。"""
        executed.append(True)
        return "ok"

    registry = ToolRegistry()
    registry.register_function(op, name="op", policy=ToolPolicy(access="public"))
    async with ToolExecutor(
        registry, audit=BlockingSink(), config=ExecutorConfig(audit_timeout_s=30)
    ) as executor:
        task = asyncio.create_task(
            executor.execute(ToolInvocation(call_id="c", tool="op"), ToolContext())
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert executor.limiter.inflight == 0
    assert executed == ([] if block_kind == "decision" else [True])
