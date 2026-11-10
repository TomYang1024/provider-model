"""共享结果存储的**参考实现**：基于 Redis 的 :class:`~agent_tools.governance.reuse.ResultStore`
（可选，核心包不依赖 Redis）。

状态：给宿主参考和改造的实现，不是默认组件。只有宿主显式构造本类并通过
``ToolExecutor(result_cache=...)`` 或 ``ToolExecutor(dedupe_store=...)`` 传入时才会生效。本包
**不声明**对 ``redis`` 的依赖：客户端按 :class:`RedisKeyValueClient` 协议鸭子类型传入
（``redis.asyncio.Redis`` 满足该协议，需要宿主自行安装，并且**不能**开启
``decode_responses``）。

**验证范围（重要）**：本实现只使用 ``GET`` 与 ``SET ... PX`` 两条普通命令，没有 Lua 脚本。
Python 一侧（键与前缀、过期时间换算、大小上限、返回值类型检查、接入执行器后跨实例的缓存命中与
去重重放、存储故障时执行器的行为）用内存中的假客户端测试。**没有在真实 Redis 上运行过**
（开发环境没有 Redis，也不允许网络连接）；真实 Redis 上的验证步骤见
``tests/test_redis_state.py`` 中带 ``redis`` 标记的测试。

它用在两个**不同**的机制上，含义不能混淆（见 :mod:`agent_tools.governance.reuse`）：

* **结果缓存**（``result_cache``）：多个实例共享只读工具的成功结果。普通缓存：不保证一致性，
  读到的可能是其他实例稍早写入的结果；读写失败按未命中处理，工具照常执行；
* **调用去重**（``dedupe_store``）：一个实例完成并记录的调用，在窗口期内被重复提交到**任何**
  实例时重放记录的结果。读失败时调用不执行（``dedupe_unavailable``，失败即关闭）。

它**不提供**的保证：

* **不是并发协调**。单飞（同一个键只执行一次）仍然只在各自进程内：同一个调用**同时**到达两个
  实例时，两边都读不到记录，都会执行。跨实例单飞需要“先占位、再执行”的原子语义，现有的
  ``ResultStore`` 协议（只有 ``get`` / ``set``）表达不了，本实现没有偷偷加锁；
* **不是事务，也不是严格一次执行**。工具执行完成到记录写入之间进程崩溃、写入失败、记录过期、
  或 Redis 因内存淘汰 / 主从切换丢失记录后，重复提交会再次执行；
* **不是下游幂等**。需要“副作用只发生一次”时，必须由下游按幂等键去重（HTTP 工具的
  ``Idempotency-Key``），本存储只能减少重复执行，不能消除。

多租户隔离与安全边界：

* 执行器给出的键已经是 ``cache:`` / ``dedupe:`` + SHA-256 摘要，摘要的输入包含租户、用户、权限集合
  （缓存）或租户、用户、请求与调用 ID（去重）、工具名、版本和参数。不同租户、不同用户的调用得到
  不同的键，本存储不解析也不放宽这个作用域；键里没有明文身份或参数；
* **值是工具结果的明文序列化**（密钥清除之后、``ResultFilter`` 之前的输出）。Redis 的访问控制、
  传输加密（TLS）与静态加密由宿主负责；能读这个 Redis 的任何人都能读到这些结果。需要应用层加密
  时在宿主侧包装本类；
* 不同部署、不同环境必须使用不同的 ``prefix``（或不同的 Redis 数据库）；缓存与去重必须使用
  不同的存储实例（执行器会检查）和不同的前缀；
* 去重存储应放在**不淘汰键**（``maxmemory-policy noeviction``）的 Redis 上：被淘汰的去重记录
  意味着重复提交会再次执行。缓存可以放在允许淘汰的实例上。

过期：使用 Redis 的 ``PX``（毫秒）。有效期由执行器按工具策略给出（缓存 ``ttl_s``、去重
``window_s``），以 Redis 服务端的时钟为准，各实例的本地时钟不参与。
"""

from __future__ import annotations

import math
from typing import Any, Protocol

__all__ = ["RedisKeyValueClient", "RedisResultStore"]


class RedisKeyValueClient(Protocol):
    """本实现需要的最小 Redis 客户端接口（``redis.asyncio.Redis`` 满足）。"""

    async def get(self, name: str) -> Any:
        """读取一个键。

        Args:
            name: Redis 键。

        Returns:
            值（``bytes``）；不存在或已过期时为 ``None``。

        Raises:
            Exception: 连接或执行失败（执行器按缓存未命中 / 去重不可用处理）。
        """
        ...

    async def set(self, name: str, value: bytes, *, px: int) -> Any:
        """写入一个键并设置过期时间（整体替换，原子）。

        Args:
            name: Redis 键。
            value: 值。
            px: 过期时间（毫秒）。

        Returns:
            客户端的原始回复（不使用）。

        Raises:
            Exception: 连接或执行失败（执行器只记录日志）。
        """
        ...


class RedisResultStore:
    """基于 Redis 的结果存储（参考实现，验证范围与不提供的保证见模块说明）。

    满足 :class:`~agent_tools.governance.reuse.ResultStore` 协议。不拥有 Redis 客户端：连接、认证、
    TLS、超时与关闭都由宿主负责；执行器另外为每次读写设置 ``ExecutorConfig.store_timeout_s``。
    本类没有内部状态，可以被多个执行器共用，也可以在取消时安全中断（一次读写只有一条命令）。
    """

    def __init__(
        self,
        client: RedisKeyValueClient,
        *,
        prefix: str,
        max_value_bytes: int = 1024 * 1024,
    ) -> None:
        """创建存储。

        Args:
            client: Redis 客户端（鸭子类型，不能开启 ``decode_responses``）。
            prefix: 键前缀，必填且不能为空：没有默认值是为了让宿主为每个部署、每种用途（缓存 /
                去重）显式选择互不相同的前缀，避免不同环境或不同机制的记录互相读取。
            max_value_bytes: 单个值的大小上限；超过时 :meth:`set` 抛出异常而不是写入。

        Raises:
            ValueError: 前缀为空，或上限不是正数。
        """
        if not prefix:
            raise ValueError("prefix must not be empty")
        if max_value_bytes <= 0:
            raise ValueError("max_value_bytes must be positive")
        self._client = client
        self._prefix = prefix
        self._max_value_bytes = max_value_bytes

    def redis_key(self, key: str) -> str:
        """把执行器给出的键映射成 Redis 键。

        Args:
            key: 执行器的键（``cache:`` / ``dedupe:`` + SHA-256 十六进制串，不含明文身份）。

        Returns:
            ``prefix`` + 原键。
        """
        return self._prefix + key

    async def get(self, key: str) -> bytes | None:
        """读取未过期的值（过期由 Redis 负责）。

        Args:
            key: 键。

        Returns:
            值；不存在或已过期时为 ``None``。

        Raises:
            TypeError: 客户端返回了字符串等非字节类型（通常是开启了 ``decode_responses``）。
                不做猜测性的转码：执行器会把它当作存储故障处理（缓存按未命中、去重不执行）。
            Exception: 客户端的连接或执行错误，原样抛出，由执行器处理。
            asyncio.CancelledError: 被取消（原样传播）。
        """
        raw = await self._client.get(self.redis_key(key))
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw
        if isinstance(raw, bytearray | memoryview):
            return bytes(raw)
        raise TypeError("the Redis client must return bytes (disable decode_responses)")

    async def set(self, key: str, value: bytes, ttl_s: float) -> None:
        """写入值并设置过期时间。

        Args:
            key: 键。
            value: 值。
            ttl_s: 有效期（秒）；不是正的有限数时不写入（与进程内实现一致）。向上取整到毫秒，
                不会比要求的更早过期超过 1 毫秒。

        Raises:
            ValueError: 值超过 ``max_value_bytes``。抛出而不是静默丢弃：执行器会记录
                ``tool.store.failed``，去重场景下等待者得到 ``duplicate_call_aborted``，
                而不是在没有记录的情况下悄悄允许重复执行。
            Exception: 客户端的连接或执行错误，原样抛出，由执行器记录。
            asyncio.CancelledError: 被取消（原样传播；命令可能已经到达 Redis）。
        """
        if not math.isfinite(ttl_s) or ttl_s <= 0:
            return
        if len(value) > self._max_value_bytes:
            raise ValueError("result is too large for the shared store")
        await self._client.set(self.redis_key(key), value, px=max(math.ceil(ttl_s * 1000), 1))
