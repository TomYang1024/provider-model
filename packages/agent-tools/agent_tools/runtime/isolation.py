"""执行隔离的配置与资源限制（P3）。

本模块提供的是**进程隔离**，不是完整的安全沙箱：

==========================  ==================================================================
能力                          实际提供的
==========================  ==================================================================
地址空间                       工具在独立子进程中运行，不能直接读写宿主进程的内存和对象
生命周期                       超时、调用方取消、执行器关闭时终止整个进程组（SIGTERM → 宽限期 →
                             SIGKILL），线程做不到这一点
环境变量                       子进程只拿到显式配置的变量（Python 工具：没有任何继承的变量）
工作目录                       默认每次调用一个新建的临时目录，结束后删除；也可以显式指定
输入输出                       参数经 stdin 以 JSON 传入；结果经 stdout 以 JSON 传回，超过
                             ``max_output_bytes`` 立即终止子进程
资源限制（POSIX rlimit）         CPU 秒数、打开文件数、单文件大小在 Linux 与 macOS 上可用；
                             地址空间（内存）只在 Linux 上可用，macOS 拒绝设置 ``RLIMIT_AS``——
                             配置了却无法设置时**拒绝运行**（``isolation_unavailable``），不会
                             在没有限制的情况下静默执行
==========================  ==================================================================

**不提供**：文件系统隔离（子进程以宿主用户身份运行，能读写宿主用户能读写的所有文件）、
网络隔离、系统调用过滤、按调用的用户或权限隔离、Windows 支持。调用 ``setsid`` 脱离进程组的
后代进程无法被回收。需要这些能力时，通过 ``wrapper`` 把子进程放进外部沙箱或容器运行时
（例如 ``bwrap``、``nsjail``、``firejail``、``sandbox-exec``、``docker run -i --rm``），由部署方
负责该沙箱的配置与验证；基础设施只保证按配置的前缀启动命令，并在其外层执行上面的生命周期管理。
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "IsolationUnavailableError",
    "ProcessIsolation",
    "ResourceLimits",
    "StdioIsolation",
    "apply_limits",
]


class IsolationUnavailableError(RuntimeError):
    """当前平台无法提供某项已配置的资源限制（子进程中抛出，导致拒绝运行）。"""


@dataclass(frozen=True, slots=True)
class ResourceLimits:
    """POSIX 资源限制（在子进程中、运行工具代码之前设置；软限制与硬限制设为同一个值，
    工具代码无法再调高）。``None`` 表示不限制。

    Attributes:
        cpu_s: CPU 时间（秒）。超过时内核发送 ``SIGXCPU``，进程被终止。
        memory_bytes: 地址空间上限（``RLIMIT_AS``）。**只在 Linux 上可用**。
        open_files: 打开文件描述符数上限（``RLIMIT_NOFILE``）。
        file_size_bytes: 进程可以写出的单个文件大小上限（``RLIMIT_FSIZE``）。
    """

    cpu_s: int | None = None
    memory_bytes: int | None = None
    open_files: int | None = None
    file_size_bytes: int | None = None

    def __post_init__(self) -> None:
        """校验配置的上限都是正数。

        Raises:
            ValueError: 某项上限不是正数。
        """
        for name in ("cpu_s", "memory_bytes", "open_files", "file_size_bytes"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive")

    def to_json(self) -> dict[str, int]:
        """序列化为传给子进程的 JSON 对象（只包含设置了的项）。

        Returns:
            名称 → 上限。
        """
        return {
            name: value
            for name in ("cpu_s", "memory_bytes", "open_files", "file_size_bytes")
            if (value := getattr(self, name)) is not None
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> ResourceLimits:
        """从 :meth:`to_json` 的结果还原。

        Args:
            data: 名称 → 上限。

        Returns:
            资源限制。
        """
        return cls(**{k: int(v) for k, v in data.items()})


def apply_limits(limits: ResourceLimits) -> None:
    """在**当前进程**中设置资源限制（只应在隔离子进程里调用）。

    Args:
        limits: 资源限制。

    Raises:
        IsolationUnavailableError: 平台不支持或拒绝设置某项限制（例如 macOS 的 ``RLIMIT_AS``）。
    """
    if not limits.to_json():
        return
    try:
        import resource
    except ImportError as exc:  # pragma: no cover - 非 POSIX 平台
        raise IsolationUnavailableError("resource limits require a POSIX platform") from exc

    plan = [
        ("cpu_s", "RLIMIT_CPU", limits.cpu_s),
        ("memory_bytes", "RLIMIT_AS", limits.memory_bytes),
        ("open_files", "RLIMIT_NOFILE", limits.open_files),
        ("file_size_bytes", "RLIMIT_FSIZE", limits.file_size_bytes),
    ]
    for name, constant, value in plan:
        if value is None:
            continue
        which = getattr(resource, constant, None)
        if which is None:
            raise IsolationUnavailableError(f"{name} is not supported on this platform")
        _soft, hard = resource.getrlimit(which)
        target = value if hard == resource.RLIM_INFINITY else min(value, hard)
        # CPU：硬限制比软限制多 1 秒，先收到可捕获的 SIGXCPU，再被内核强制终止
        new_hard = (
            target + 1 if constant == "RLIMIT_CPU" and hard == resource.RLIM_INFINITY else target
        )
        try:
            resource.setrlimit(which, (target, new_hard))
        except (ValueError, OSError) as exc:
            raise IsolationUnavailableError(f"{name} cannot be enforced on this platform") from exc


def _check_wrapper(wrapper: tuple[str, ...]) -> None:
    """校验外部沙箱命令前缀：第一个元素必须是绝对路径，不能包含空字节。

    子进程只拿到显式配置的环境变量（通常没有 ``PATH``），所以命令必须写绝对路径。

    Args:
        wrapper: 命令前缀。

    Raises:
        ValueError: 前缀非法。
    """
    if wrapper and not os.path.isabs(wrapper[0]):
        raise ValueError("wrapper[0] must be an absolute path")
    if any("\0" in part for part in wrapper):
        raise ValueError("wrapper must not contain NUL bytes")


def _require_posix() -> None:
    """进程隔离依赖 POSIX 的进程组与资源限制。

    Raises:
        ValueError: 当前平台不是 POSIX。
    """
    if os.name != "posix":  # pragma: no cover - 测试只在 POSIX 上运行
        raise ValueError("process isolation is only supported on POSIX platforms")


@dataclass(frozen=True, slots=True)
class ProcessIsolation:
    """进程隔离的 Python 工具配置（P3）：每次调用启动一个新的子进程运行工具函数。

    工具函数必须能在子进程中按 ``模块:限定名`` 重新导入（模块顶层定义，不能是 ``__main__``
    或函数内部定义的函数），参数和返回值必须能 JSON 序列化。

    Attributes:
        limits: 资源限制。
        env: 传给子进程的环境变量（**只有**这些变量，不继承宿主的任何环境变量）。
        cwd: 工作目录；``None`` 表示每次调用新建一个临时目录，结束后删除。
        wrapper: 外部沙箱命令前缀，例如 ``("/usr/bin/bwrap", "--unshare-net", ..., "--")``；
            第一个元素必须是绝对路径。
        max_output_bytes: 子进程 stdout 上返回结果的最大字节数，超过时立即终止子进程。
        kill_grace_s: 终止时 SIGTERM 之后等待多久再 SIGKILL（秒）。
    """

    limits: ResourceLimits = field(default_factory=ResourceLimits)
    env: Mapping[str, str] = field(default_factory=dict)
    cwd: str | None = None
    wrapper: tuple[str, ...] = ()
    max_output_bytes: int = 4 * 1024 * 1024
    kill_grace_s: float = 1.0

    def __post_init__(self) -> None:
        """校验平台、命令前缀、工作目录与各项上限。

        Raises:
            ValueError: 不是 POSIX 平台，命令前缀非法，工作目录不是绝对路径，或上限不是正数。
        """
        _require_posix()
        _check_wrapper(self.wrapper)
        if self.cwd is not None and not os.path.isabs(self.cwd):
            raise ValueError("cwd must be an absolute path")
        if self.max_output_bytes <= 0 or self.kill_grace_s <= 0:
            raise ValueError("max_output_bytes and kill_grace_s must be positive")
        if any("\0" in k or "\0" in v or "=" in k for k, v in self.env.items()):
            raise ValueError("invalid environment variable")


@dataclass(frozen=True, slots=True)
class StdioIsolation:
    """MCP stdio 服务端的隔离配置（P3）：通过 ``agent_tools.runtime.stdio_launcher`` 启动服务端。

    启动器与服务端在同一个进程组里，SDK 关闭连接时会终止整个进程组。启动器负责：

    * 在启动服务端之前设置资源限制（由服务端及其子进程继承）；
    * 转发服务端的 stdout，单条 JSON-RPC 消息（一行）超过 ``max_line_bytes`` 时丢弃该消息：
      如果它是某个请求的响应，就改为向客户端发送一个错误响应（调用返回
      ``mcp_response_too_large``，连接继续可用）；否则（通知或服务端请求）直接丢弃。
      这样客户端缓冲的单条消息不会超过上限，弥补 SDK 在 stdio 上没有大小上限的问题。

    Attributes:
        limits: 资源限制。
        max_line_bytes: 单条消息的最大字节数；``None`` 表示不限制（只转发）。
        wrapper: 外部沙箱命令前缀（第一个元素必须是绝对路径），放在服务端命令之前。
    """

    limits: ResourceLimits = field(default_factory=ResourceLimits)
    max_line_bytes: int | None = 4 * 1024 * 1024
    wrapper: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """校验平台、命令前缀与行长度上限。

        Raises:
            ValueError: 不是 POSIX 平台，命令前缀非法，或行长度上限过小。
        """
        _require_posix()
        _check_wrapper(self.wrapper)
        if self.max_line_bytes is not None and self.max_line_bytes < 1024:
            raise ValueError("max_line_bytes must be at least 1024")
