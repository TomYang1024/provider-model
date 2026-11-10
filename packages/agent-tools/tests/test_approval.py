"""人工审批（P3）的测试：批准、拒绝、缺少处理器、超时、取消、处理器失败、审计及其失败，
审批时点（鉴权与校验之后）、参数暴露，以及不可逆工具注册规则的兼容性。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest
from pydantic import ValidationError

from agent_tools import (
    ApprovalDecision,
    ApprovalPolicy,
    ApprovalRequest,
    AuditEvent,
    CachePolicy,
    DedupePolicy,
    InMemoryResultStore,
    Principal,
    RegistryError,
    RetryPolicy,
    ToolAnnotations,
    ToolContext,
    ToolInvocation,
    ToolPolicy,
    ToolRegistry,
    UpstreamError,
)

DESTRUCTIVE = ToolAnnotations(destructive=True)


@dataclass
class Handler:
    """可编程的审批处理器：记录收到的请求，按设定返回决定、等待或抛出异常。"""

    decision: Any = field(default_factory=lambda: ApprovalDecision(True, approver="alice"))
    delay_s: float = 0.0
    raises: Exception | None = None
    requests: list[ApprovalRequest] = field(default_factory=list)
    cleaned_up: int = 0
    entered: asyncio.Event = field(default_factory=asyncio.Event)

    async def request_approval(self, request: ApprovalRequest) -> Any:
        """记录请求；按设定等待、抛出异常或返回决定；无论如何都执行清理。"""
        self.requests.append(request)
        self.entered.set()
        try:
            if self.delay_s:
                await asyncio.sleep(self.delay_s)
            if self.raises is not None:
                raise self.raises
            return self.decision
        finally:
            self.cleaned_up += 1


class Audit:
    """收集审计事件；可以让指定种类的事件写入失败。"""

    def __init__(self, fail_kind: str | None = None) -> None:
        """创建接收端。

        Args:
            fail_kind: 写入失败的事件种类。
        """
        self.events: list[AuditEvent] = []
        self.fail_kind = fail_kind

    async def emit(self, event: AuditEvent) -> None:
        """记录事件，或者模拟写入失败。"""
        if event.kind == self.fail_kind:
            raise ConnectionError("audit backend down")
        self.events.append(event)

    def of(self, kind: str) -> list[AuditEvent]:
        """按种类筛选事件。"""
        return [e for e in self.events if e.kind == kind]


def make_registry(calls: list[dict[str, Any]], **approval: Any) -> ToolRegistry:
    """注册一个需要审批的不可逆工具 ``delete_user``。

    Args:
        calls: 记录真实执行的参数。
        **approval: ``ApprovalPolicy`` 字段。

    Returns:
        注册表。
    """
    registry = ToolRegistry()

    async def delete_user(user: str, password: str = "", note: str = "") -> str:
        """删除用户（测试替身）。"""
        calls.append({"user": user})
        return f"deleted {user}"

    registry.register_function(
        delete_user,
        name="delete_user",
        policy=ToolPolicy(
            access="scoped",
            required_scopes=frozenset({"admin"}),
            approval=ApprovalPolicy(**approval),
        ),
        annotations=DESTRUCTIVE,
    )
    return registry


ADMIN = ToolContext(principal=Principal(user_id="u1", tenant_id="t1", scopes=frozenset({"admin"})))
ARGS = {"user": "bob", "password": "hunter2-secret", "note": "contact bob@example.com"}


def call(args: Any = None, call_id: str = "c1") -> ToolInvocation:
    """构造对 ``delete_user`` 的调用。"""
    return ToolInvocation(
        call_id=call_id, tool="delete_user", arguments=ARGS if args is None else args
    )


async def test_approved_call_runs_and_is_audited(make_executor: Any) -> None:
    """批准后工具执行；审批事件写入审计（含审批人），参数视图默认脱敏，带参数摘要。"""
    calls: list[dict[str, Any]] = []
    handler, audit = Handler(), Audit()
    executor = make_executor(make_registry(calls), approval_handler=handler, audit=audit)
    result = await executor.execute(call(), ADMIN)
    assert result.ok and calls == [{"user": "bob"}]
    (request,) = handler.requests
    assert request.tool == "delete_user" and request.annotations.destructive
    assert request.principal.user_id == "u1"
    assert request.arguments_view == "redacted"
    assert request.arguments is not None
    assert request.arguments["password"] == "[REDACTED]"
    assert "bob@example.com" not in request.arguments["note"]
    assert request.arguments["user"] == "bob"
    assert len(request.arguments_sha256) == 64 and request.arguments_bytes > 0
    (event,) = audit.of("approval")
    assert (event.outcome, event.approver, event.tool) == ("approved", "alice", "delete_user")
    kinds = [e.kind for e in audit.events]
    assert kinds.index("approval") < kinds.index("result")


async def test_rejection_does_not_execute_and_hides_reason(make_executor: Any) -> None:
    """拒绝时工具不执行；拒绝理由只进审计，不出现在返回给模型的结果里。"""
    calls: list[dict[str, Any]] = []
    handler = Handler(decision=ApprovalDecision(False, approver="bob", reason="customer is VIP"))
    audit = Audit()
    executor = make_executor(make_registry(calls), approval_handler=handler, audit=audit)
    result = await executor.execute(call(), ADMIN)
    assert calls == []
    assert result.error is not None
    assert (result.error.type, result.error.code) == ("permission", "approval_rejected")
    assert "VIP" not in result.to_model_text()
    (event,) = audit.of("approval")
    assert (event.outcome, event.reason, event.approver) == ("rejected", "customer is VIP", "bob")


async def test_missing_handler_fails_safe(make_executor: Any) -> None:
    """需要审批但没有配置处理器时安全失败，不会绕过审批执行。"""
    calls: list[dict[str, Any]] = []
    audit = Audit()
    executor = make_executor(make_registry(calls), audit=audit)
    result = await executor.execute(call(), ADMIN)
    assert calls == []
    assert result.error is not None and result.error.code == "approval_unavailable"
    assert audit.of("approval")[0].outcome == "unavailable"


async def test_timeout_is_bounded_by_policy_and_deadline(make_executor: Any) -> None:
    """等待审批超过 ``timeout_s`` 时失败；整体截止时间更短时以截止时间为准；处理器被取消并清理。"""
    calls: list[dict[str, Any]] = []
    handler = Handler(delay_s=5)
    executor = make_executor(make_registry(calls, timeout_s=0.05), approval_handler=handler)
    result = await executor.execute(call(), ADMIN)
    assert result.error is not None and result.error.code == "approval_timeout"
    assert calls == [] and handler.cleaned_up == 1

    executor = make_executor(make_registry(calls, timeout_s=60), approval_handler=handler)
    ctx = ToolContext.with_timeout(0.05, principal=ADMIN.principal)
    result = await executor.execute(call(), ctx)
    assert result.error is not None and result.error.code == "approval_timeout"
    assert handler.requests[-1].timeout_s <= 0.05


@pytest.mark.parametrize(
    "handler", [Handler(raises=RuntimeError("ui crashed")), Handler(decision="yes")]
)
async def test_handler_failure_fails_closed(make_executor: Any, handler: Handler) -> None:
    """处理器抛出异常或返回非法值时视为审批失败，工具不执行。"""
    calls: list[dict[str, Any]] = []
    audit = Audit()
    executor = make_executor(make_registry(calls), approval_handler=handler, audit=audit)
    result = await executor.execute(call(), ADMIN)
    assert calls == []
    assert result.error is not None and result.error.code == "approval_failed"
    assert audit.of("approval")[0].outcome == "error"


async def test_handler_timeout_error_is_not_mistaken_for_approval_timeout(
    make_executor: Any,
) -> None:
    """处理器自己抛出的 ``TimeoutError`` 属于处理器失败，而不是“等待审批超时”。"""
    calls: list[dict[str, Any]] = []
    handler = Handler(raises=TimeoutError("backend timed out"))
    executor = make_executor(make_registry(calls), approval_handler=handler)
    result = await executor.execute(call(), ADMIN)
    assert result.error is not None and result.error.code == "approval_failed"


async def test_caller_cancellation_while_waiting(make_executor: Any) -> None:
    """等待审批时调用方取消：``CancelledError`` 原样传播，处理器被取消并清理，工具不执行。"""
    calls: list[dict[str, Any]] = []
    handler, audit = Handler(delay_s=30), Audit()
    executor = make_executor(make_registry(calls), approval_handler=handler, audit=audit)
    task = asyncio.create_task(executor.execute(call(), ADMIN))
    await handler.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == [] and handler.cleaned_up == 1
    assert audit.of("approval") == []


async def test_executor_close_while_waiting(make_executor: Any) -> None:
    """等待审批时执行器关闭：调用返回 ``executor_closed``，工具不执行。"""
    calls: list[dict[str, Any]] = []
    handler = Handler(delay_s=30)
    executor = make_executor(make_registry(calls), approval_handler=handler)
    task = asyncio.create_task(executor.execute(call(), ADMIN))
    await handler.entered.wait()
    await executor.aclose(grace_s=0.01)
    result = await task
    assert result.error is not None and result.error.code == "executor_closed"
    assert calls == []


async def test_audit_failure_fail_closed_blocks_execution(make_executor: Any) -> None:
    """``fail_closed`` 下审批事件写入失败时，即使已经批准，工具也不执行。"""
    calls: list[dict[str, Any]] = []
    executor = make_executor(
        make_registry(calls),
        approval_handler=Handler(),
        audit=Audit(fail_kind="approval"),
        audit_failure_mode="fail_closed",
    )
    result = await executor.execute(call(), ADMIN)
    assert result.error is not None and result.error.code == "audit_unavailable"
    assert calls == []


async def test_audit_failure_fail_open_still_executes(make_executor: Any) -> None:
    """``fail_open`` 下审批事件写入失败只计数，批准的调用照常执行。"""
    calls: list[dict[str, Any]] = []
    executor = make_executor(
        make_registry(calls), approval_handler=Handler(), audit=Audit(fail_kind="approval")
    )
    result = await executor.execute(call(), ADMIN)
    assert result.ok and calls


async def test_approval_happens_after_authorization_and_validation(make_executor: Any) -> None:
    """没有权限或参数非法的调用不会打扰审批人。"""
    calls: list[dict[str, Any]] = []
    handler = Handler()
    executor = make_executor(make_registry(calls), approval_handler=handler)
    denied = await executor.execute(call(), ToolContext())
    assert denied.error is not None and denied.error.code == "permission_denied"
    invalid = await executor.execute(call({"nope": 1}), ADMIN)
    assert invalid.error is not None and invalid.error.type == "invalid_arguments"
    assert handler.requests == []


@pytest.mark.parametrize(("view", "expect_args"), [("none", False), ("full", True)])
async def test_argument_views(make_executor: Any, view: str, expect_args: bool) -> None:
    """``arguments="none"`` 只给摘要；``"full"`` 给原始参数（需要宿主显式选择）。"""
    calls: list[dict[str, Any]] = []
    handler = Handler()
    executor = make_executor(make_registry(calls, arguments=view), approval_handler=handler)
    await executor.execute(call(), ADMIN)
    (request,) = handler.requests
    if expect_args:
        assert request.arguments == ARGS
    else:
        assert request.arguments is None
        assert len(request.arguments_sha256) == 64


async def test_retries_do_not_request_approval_again(make_executor: Any) -> None:
    """一次调用只审批一次：允许重试的工具在重试时不会再次请求审批。"""
    attempts: list[int] = []
    registry = ToolRegistry()

    async def flaky() -> str:
        """前两次失败，第三次成功。"""
        attempts.append(1)
        if len(attempts) < 3:
            raise UpstreamError("busy", retryable=True)
        return "ok"

    registry.register_function(
        flaky,
        name="flaky",
        policy=ToolPolicy(
            access="public",
            approval=ApprovalPolicy(),
            retry=RetryPolicy(max_attempts=3, base_delay_s=0),
        ),
        annotations=ToolAnnotations(idempotent=True),
    )
    handler = Handler()
    executor = make_executor(registry, approval_handler=handler)
    result = await executor.execute(ToolInvocation(call_id="c", tool="flaky"), ToolContext())
    assert result.ok and result.attempts == 3
    assert len(handler.requests) == 1


def test_destructive_registration_rules() -> None:
    """不可逆工具：声明审批即可注册；不声明审批时默认拒绝；P0 的显式放开开关仍然有效。"""

    async def drop() -> str:
        """删除数据（测试替身）。"""
        return "dropped"

    with pytest.raises(RegistryError, match="approval"):
        ToolRegistry().register_function(
            drop, name="drop", policy=ToolPolicy(access="public"), annotations=DESTRUCTIVE
        )
    ToolRegistry().register_function(
        drop,
        name="drop",
        policy=ToolPolicy(access="public", approval=ApprovalPolicy()),
        annotations=DESTRUCTIVE,
    )
    ToolRegistry(allow_destructive_without_approval=True).register_function(
        drop, name="drop", policy=ToolPolicy(access="public"), annotations=DESTRUCTIVE
    )


def test_policy_combinations_are_validated() -> None:
    """需要审批的工具不能声明结果缓存；审批超时必须为正数。"""
    with pytest.raises(ValidationError):
        ToolPolicy(access="public", approval=ApprovalPolicy(), cache=CachePolicy(ttl_s=10))
    with pytest.raises(ValidationError):
        ApprovalPolicy(timeout_s=0)


async def test_dedupe_replay_does_not_request_approval_again(make_executor: Any) -> None:
    """同一逻辑调用的重复提交直接重放第一次的结果：不执行工具，也不再打扰审批人。"""
    calls: list[str] = []
    registry = ToolRegistry()

    async def wire(amount: int) -> str:
        """转账（测试替身）。"""
        calls.append("wire")
        return f"sent {amount}"

    registry.register_function(
        wire,
        name="wire",
        policy=ToolPolicy(access="public", approval=ApprovalPolicy(), dedupe=DedupePolicy()),
        annotations=DESTRUCTIVE,
    )
    handler = Handler()
    executor = make_executor(registry, approval_handler=handler, dedupe_store=InMemoryResultStore())
    ctx = ToolContext(request_id="req-w", principal=Principal(user_id="u", tenant_id="t"))
    invocation = ToolInvocation(call_id="c", tool="wire", arguments={"amount": 10})
    first = await executor.execute(invocation, ctx)
    again = await executor.execute(invocation, ctx)
    assert first.ok and again.reused == "dedupe"
    assert calls == ["wire"] and len(handler.requests) == 1


async def test_rejected_call_is_not_recorded_for_dedupe(make_executor: Any) -> None:
    """审批被拒绝时工具没有执行，不写去重记录：之后的重复提交会重新请求审批。"""
    calls: list[str] = []
    registry = ToolRegistry()

    async def wire(amount: int) -> str:
        """转账（测试替身）。"""
        calls.append("wire")
        return "sent"

    registry.register_function(
        wire,
        name="wire",
        policy=ToolPolicy(access="public", approval=ApprovalPolicy(), dedupe=DedupePolicy()),
        annotations=DESTRUCTIVE,
    )
    handler = Handler(decision=ApprovalDecision(False))
    executor = make_executor(registry, approval_handler=handler, dedupe_store=InMemoryResultStore())
    invocation = ToolInvocation(call_id="c", tool="wire", arguments={"amount": 10})
    ctx = ToolContext(request_id="req-r")
    for _ in range(2):
        result = await executor.execute(invocation, ctx)
        assert result.error is not None and result.error.code == "approval_rejected"
    assert calls == [] and len(handler.requests) == 2
