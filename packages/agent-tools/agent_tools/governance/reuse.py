"""结果复用（P3）：结果缓存与调用去重共用的存储协议、键计算与单飞协调。

三种机制的区别（互相不能替代）：

* **结果缓存**：TTL 内，相同身份、相同参数的只读调用不再访问下游。它不防重复执行，也不是幂等
  机制，只允许用于只读工具；
* **调用去重**：同一逻辑调用（同一 request_id + call_id + 参数）被重复提交时不再执行，返回第一次
  的结果。它不是事务：执行完成到写入记录之间进程崩溃，重放时仍会再次执行；记录被淘汰或过期后
  也会再次执行；
* **下游幂等**：下游按 ``Idempotency-Key`` 自行去重（P1）。需要下游明确支持，基础设施无法验证。

两者都默认关闭：工具必须声明 ``policy.cache`` / ``policy.dedupe``，执行器还必须分别配置
``result_cache`` / ``dedupe_store``。两者使用不同的存储实例，避免缓存条目挤占去重记录。

键的作用域（全部经过 SHA-256，存储里看不到明文身份和参数）：

* 缓存：``principal`` 范围 = 租户 + 用户 + 排序后的权限集合 + 工具名 + 版本 + 规范化参数；
  ``tenant`` 范围去掉用户但保留权限集合，因此权限不同的用户之间不会共享结果。缺少所需的
  身份字段时不使用缓存（返回 ``None``）；
* 去重：租户 + 用户 + request_id + call_id + 工具名 + 版本 + 规范化参数。参数不同的同 ID 调用
  视为不同调用。

查找发生在工具级与参数级鉴权**之后**：没有权限的调用拿不到缓存结果；每次命中都会重新鉴权。

并发：同一个键同时只有一个“执行者”，其余调用等待执行者结束后再查存储（:class:`SingleFlight`）。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from ..protocol.context import Principal, ToolContext
from ..protocol.spec import CachePolicy, ToolSpec
from ..runtime.clock import Clock, MonotonicClock

__all__ = [
    "FlightOutcome",
    "InMemoryResultStore",
    "ResultStore",
    "SingleFlight",
    "arguments_digest",
    "cache_key",
    "canonical_json",
    "dedupe_key",
]

#: 执行者结束时的状态：``stored`` 已写入记录；``not_started`` 工具没有执行（可以由等待者
#: 重新执行）；``aborted`` 工具已经开始执行但没有得到可记录的结果（例如调用方取消）。
FlightOutcome = Literal["stored", "not_started", "aborted"]


class ResultStore(Protocol):
    """结果存储协议。值是不透明的字节串（执行器负责序列化）。

    宿主可以提供共享存储（例如 Redis）的实现，以便在多个实例之间复用或去重；实现负责
    访问控制、加密与持久化策略。执行器为每次读写设置超时（``ExecutorConfig.store_timeout_s``）。
    """

    async def get(self, key: str) -> bytes | None:
        """读取未过期的值。

        Args:
            key: 键（带 ``cache:`` / ``dedupe:`` 前缀的 SHA-256 十六进制串）。

        Returns:
            值；不存在或已过期时为 ``None``。
        """
        ...

    async def set(self, key: str, value: bytes, ttl_s: float) -> None:
        """写入值。

        Args:
            key: 键。
            value: 值。
            ttl_s: 有效期（秒）。
        """
        ...


def canonical_json(value: Any) -> str:
    """把参数序列化为规范化 JSON（键排序、紧凑），用于计算摘要。

    Args:
        value: 解析后的 JSON 参数（来自模型的原始参数，已经是 JSON 类型）。

    Returns:
        规范化的 JSON 字符串。
    """
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def arguments_digest(arguments: Mapping[str, Any]) -> tuple[str, int]:
    """计算参数的 SHA-256 摘要与字节数。

    Args:
        arguments: 解析后的 JSON 参数。

    Returns:
        ``(十六进制摘要, 规范化 JSON 的 UTF-8 字节数)``。
    """
    data = canonical_json(arguments).encode("utf-8")
    return hashlib.sha256(data).hexdigest(), len(data)


def _digest(namespace: str, parts: list[str]) -> str:
    """把若干字段用 JSON 数组编码后求 SHA-256（避免分隔符拼接造成的歧义）。

    Args:
        namespace: 键前缀。
        parts: 字段。

    Returns:
        ``前缀:十六进制摘要``。
    """
    raw = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return f"{namespace}:{hashlib.sha256(raw).hexdigest()}"


def cache_key(
    policy: CachePolicy, principal: Principal, spec: ToolSpec, args_sha256: str
) -> str | None:
    """计算结果缓存的键。

    Args:
        policy: 缓存策略。
        principal: 调用方身份。
        spec: 工具规格。
        args_sha256: 参数摘要。

    Returns:
        键；所需的身份字段缺失时返回 ``None``（不使用缓存，防止匿名调用之间共享结果）。
    """
    if policy.scope == "principal":
        if principal.user_id is None:
            return None
        user = principal.user_id
    else:
        if principal.tenant_id is None:
            return None
        user = "*"
    return _digest(
        "cache",
        [
            policy.scope,
            principal.tenant_id or "",
            user,
            *sorted(principal.scopes),
            "|",
            spec.name,
            spec.version,
            args_sha256,
        ],
    )


def dedupe_key(ctx: ToolContext, call_id: str, spec: ToolSpec, args_sha256: str) -> str:
    """计算调用去重的键。

    Args:
        ctx: 调用上下文。
        call_id: 调用 ID。
        spec: 工具规格。
        args_sha256: 参数摘要。

    Returns:
        键。
    """
    principal = ctx.principal
    return _digest(
        "dedupe",
        [
            principal.tenant_id or "",
            principal.user_id or "",
            ctx.request_id,
            call_id,
            spec.name,
            spec.version,
            args_sha256,
        ],
    )


@dataclass
class _Entry:
    """内存存储中的一条记录。"""

    value: bytes
    expires: float


class InMemoryResultStore:
    """进程内的结果存储：TTL + 最近最少使用淘汰 + 条目数与总字节数上限。

    只在事件循环线程中使用。只对当前进程有效：进程重启后全部丢失，多实例之间不共享。
    容量不足时会淘汰最久未使用的条目——对去重存储而言，被淘汰的记录意味着之后的重复提交
    会再次执行，因此容量应按“窗口期内的调用量”规划。
    """

    def __init__(
        self,
        *,
        max_entries: int = 10_000,
        max_bytes: int = 64 * 1024 * 1024,
        clock: Clock | None = None,
    ) -> None:
        """创建存储。

        Args:
            max_entries: 最多保存的条目数。
            max_bytes: 所有值合计的最大字节数；单个值超过它时不保存。
            clock: 单调时钟（测试注入，用于在不等待的情况下推进过期）。

        Raises:
            ValueError: 上限不是正数。
        """
        if max_entries <= 0 or max_bytes <= 0:
            raise ValueError("max_entries and max_bytes must be positive")
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._clock = clock or MonotonicClock()
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        self._bytes = 0

    def __len__(self) -> int:
        """当前保存的条目数（含尚未清理的过期条目）。"""
        return len(self._entries)

    @property
    def size_bytes(self) -> int:
        """当前所有值合计的字节数。"""
        return self._bytes

    async def get(self, key: str) -> bytes | None:
        """读取未过期的值，并把它标记为最近使用。

        Args:
            key: 键。

        Returns:
            值；不存在或已过期（同时删除）时为 ``None``。
        """
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires <= self._clock.monotonic():
            self._remove(key)
            return None
        self._entries.move_to_end(key)
        return entry.value

    async def set(self, key: str, value: bytes, ttl_s: float) -> None:
        """写入值；必要时先清理过期条目，再按最近最少使用淘汰，直到满足上限。

        Args:
            key: 键。
            value: 值。
            ttl_s: 有效期（秒）；不是正数时不保存。
        """
        if ttl_s <= 0 or len(value) > self._max_bytes:
            return
        if key in self._entries:
            self._remove(key)
        now = self._clock.monotonic()
        if len(self._entries) >= self._max_entries or self._bytes + len(value) > self._max_bytes:
            for stale in [k for k, e in self._entries.items() if e.expires <= now]:
                self._remove(stale)
        while self._entries and (
            len(self._entries) >= self._max_entries or self._bytes + len(value) > self._max_bytes
        ):
            self._remove(next(iter(self._entries)))
        self._entries[key] = _Entry(value, now + ttl_s)
        self._bytes += len(value)

    def _remove(self, key: str) -> None:
        """删除一条记录并更新字节数。

        Args:
            key: 键。
        """
        entry = self._entries.pop(key)
        self._bytes -= len(entry.value)


class SingleFlight:
    """同一个键同时只允许一个执行者；其余调用等待执行者结束（进程内，事件循环线程中使用）。"""

    def __init__(self) -> None:
        """创建空的协调器。"""
        self._flights: dict[str, asyncio.Future[FlightOutcome]] = {}

    def __len__(self) -> int:
        """当前正在执行的键的数量（用于测试与诊断）。"""
        return len(self._flights)

    def join(self, key: str) -> asyncio.Future[FlightOutcome] | None:
        """尝试成为某个键的执行者。

        Args:
            key: 键。

        Returns:
            ``None`` 表示当前调用成为执行者（结束时必须调用 :meth:`finish`）；否则返回执行者的
            完成信号，调用方应等待它之后重新查询存储。
        """
        existing = self._flights.get(key)
        if existing is not None:
            return existing
        self._flights[key] = asyncio.get_running_loop().create_future()
        return None

    def finish(self, key: str, outcome: FlightOutcome) -> None:
        """执行者结束：唤醒所有等待者并移除这个键。可以在取消处理中同步调用。

        Args:
            key: 键。
            outcome: 执行者结束时的状态。
        """
        future = self._flights.pop(key, None)
        if future is not None and not future.done():
            future.set_result(outcome)
