"""人工审批（P3）：宿主实现的 :class:`ApprovalHandler` 协议与请求、决定的数据结构。

审批模型（由执行器固定）：

* **何时需要**：工具在 ``ToolPolicy.approval`` 中声明了
  :class:`~agent_tools.protocol.spec.ApprovalPolicy`。
  不可逆工具（``destructive=True``）只有声明了审批才能注册，除非注册表显式设置了
  ``allow_destructive_without_approval=True``（P0 起的兼容开关，风险由宿主承担）。
* **时点**：工具级鉴权 → 参数解析与校验 → 参数级鉴权 → 调用去重（重复提交直接重放第一次的
  结果，不执行、因此也不再审批）→ **审批** → 结果缓存（与审批互斥，不会同时生效）→ 熔断、
  并发额度、限流 → 执行。未经授权或参数非法的调用不会打扰审批人；等待审批时不占用执行额度。
  每次调用只审批一次，重试不会再次审批。
* **结果**（除“批准”外，工具都**没有执行**）：

  - 批准：继续执行；
  - 拒绝：``ApprovalRejectedError(approval_rejected)``；
  - 执行器没有审批处理器：``ApprovalRequiredError(approval_unavailable)``（安全失败）；
  - 超过 ``min(timeout_s, 剩余截止时间)``：``ApprovalRequiredError(approval_timeout)``；
  - 处理器抛出异常或返回非法值：``ApprovalRequiredError(approval_failed)``；
  - 调用方取消：``CancelledError`` 原样传播（处理器协程被取消）；
  - 执行器关闭：``ToolCancelledError(executor_closed)`` 结果。

  批准、拒绝、超时、失败都会写一条 ``approval`` 审计事件（在执行之前写入，``fail_closed`` 时
  写入失败会阻止执行）；调用方取消时不写（取消期间不再等待审计写入）。

* **同步等待的适用范围**：处理器在当前调用的任务里被 ``await``，适合审批人在线、几分钟内就能
  响应的交互式场景（例如聊天界面里的确认按钮）。**不支持**跨进程或长时间的暂停与恢复：
  进程重启、执行器关闭、调用方取消或截止时间到达都会结束等待，审批状态不会被保存。需要
  “先挂起、几小时后再继续”的流程，应由宿主在 Runtime 层拆成多轮（例如先返回待审批状态，
  审批完成后以新的请求重新发起调用）。
* **参数暴露**：由 ``ApprovalPolicy.arguments`` 决定处理器能看到什么（默认经过脱敏的预览，
  见 :class:`ApprovalRequest`）。拒绝理由、审批人只进审计，不会出现在返回给模型的结果里。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from ..protocol.context import Principal
from ..protocol.spec import ToolAnnotations

__all__ = ["ApprovalDecision", "ApprovalHandler", "ApprovalRequest"]


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    """交给审批处理器的一次审批请求（只读）。

    Attributes:
        request_id: 请求 ID。
        call_id: 调用 ID。
        tool: 工具内部名。
        alias: 模型侧别名。
        version: 工具版本。
        description: 工具描述（宿主注册时提供的文本）。
        annotations: 工具的语义标注（例如 ``destructive``）。
        principal: 发起调用的身份（含权限集合，审批人据此判断）。
        arguments_view: 参数视图的种类（来自 ``ApprovalPolicy.arguments``）。
        arguments: ``full`` 时为原始参数；``redacted`` 时为脱敏后的参数（值为字符串预览，
            尽力而为，不保证清除全部敏感信息）；``none`` 时为 ``None``。
        arguments_sha256: 规范化参数 JSON 的 SHA-256，可用于把决定与具体参数绑定。
        arguments_bytes: 规范化参数 JSON 的字节数。
        timeout_s: 本次等待审批的实际上限（秒）。
    """

    request_id: str
    call_id: str
    tool: str
    alias: str
    version: str
    description: str
    annotations: ToolAnnotations
    principal: Principal
    arguments_view: Literal["none", "redacted", "full"]
    arguments: Mapping[str, Any] | None
    arguments_sha256: str
    arguments_bytes: int
    timeout_s: float


@dataclass(frozen=True, slots=True)
class ApprovalDecision:
    """审批决定。

    Attributes:
        approved: 是否批准。
        approver: 审批人标识（写入审计）。
        reason: 低基数的原因代码或简短说明（写入审计，不返回给模型）。
        metadata: 写入审计的少量附加信息（宿主自定义，不要包含敏感内容）。
    """

    approved: bool
    approver: str | None = None
    reason: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)


class ApprovalHandler(Protocol):
    """宿主实现的审批处理器（例如把请求推送到界面并等待用户点击）。"""

    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        """请求审批并等待决定。

        执行器用 ``asyncio.timeout`` 限制等待时间；超时或调用方取消时本协程会被取消，
        实现应在 ``finally`` 中清理（例如撤回界面上的待审批条目），并且**不得**吞掉
        ``asyncio.CancelledError``。

        Args:
            request: 审批请求。

        Returns:
            审批决定。

        Raises:
            Exception: 任何异常都会被视为审批失败（``approval_failed``），工具不会执行。
        """
        ...
