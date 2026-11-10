"""超大输出钩子 ``on_oversize``（P3）的测试：默认行为不变、成功改写、失败与超时退回原错误、
改写后仍超限、改写结果重新校验与过滤、钩子输入已清除已知密钥。"""

from __future__ import annotations

import asyncio
from typing import Any

from agent_tools import (
    JsonBlock,
    OutputLimitError,
    OutputLimits,
    TextBlock,
    ToolContext,
    ToolInvocation,
    ToolOutput,
    ToolPolicy,
    ToolRegistry,
    ToolSpec,
)
from agent_tools.binding import ToolBinding
from agent_tools.security.schema import compile_schema

LIMITS = OutputLimits(max_json_bytes=1024, max_total_bytes=2048)


class Hook:
    """可编程的钩子：记录输入，按设定返回、等待或抛出异常。"""

    def __init__(self, result: Any = None, delay_s: float = 0, raises: Exception | None = None):
        """创建钩子。

        Args:
            result: 返回值；``None`` 时返回一段摘要文本。
            delay_s: 返回前等待的秒数。
            raises: 要抛出的异常。
        """
        self.result, self.delay_s, self.raises = result, delay_s, raises
        self.seen: list[tuple[str, ToolOutput, OutputLimitError]] = []

    async def on_oversize(
        self, spec: ToolSpec, output: ToolOutput, error: OutputLimitError, ctx: ToolContext
    ) -> Any:
        """记录输入后按设定处理。"""
        self.seen.append((spec.name, output, error))
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.raises is not None:
            raise self.raises
        if self.result is not None:
            return self.result
        return ToolOutput(content=[TextBlock(text="summary: 5000 rows omitted")])


def big_registry() -> ToolRegistry:
    """注册返回超限 JSON 的工具 ``big``。"""
    registry = ToolRegistry()

    async def big() -> dict[str, Any]:
        """返回约 5 KB 的 JSON。"""
        return {"rows": ["x" * 50] * 100}

    registry.register_function(
        big, name="big", policy=ToolPolicy(access="public", output_limits=LIMITS)
    )
    return registry


async def run(executor: Any, tool: str = "big") -> Any:
    """执行一次调用。"""
    return await executor.execute(ToolInvocation(call_id="c", tool=tool), ToolContext())


async def test_default_behavior_is_unchanged(make_executor: Any) -> None:
    """没有配置钩子时，超限仍然失败（``output_too_large``）。"""
    result = await run(make_executor(big_registry()))
    assert result.error is not None and result.error.code == "output_too_large"


async def test_hook_replaces_oversized_output(make_executor: Any, telemetry: Any) -> None:
    """钩子收到完整输出与触发的错误，返回的摘要成为结果。"""
    hook = Hook()
    result = await run(make_executor(big_registry(), on_oversize=hook))
    assert result.ok and result.to_model_text() == "summary: 5000 rows omitted"
    ((name, output, error),) = hook.seen
    assert name == "big" and error.code == "output_too_large"
    assert isinstance(output.content[0], JsonBlock)
    assert any(r.get("outcome") == "replaced" for r in telemetry.logs.events("tool.call.oversize"))


async def test_hook_not_called_for_text_truncation(make_executor: Any) -> None:
    """文本超限只截断，不会触发钩子。"""
    registry = ToolRegistry()

    async def long_text() -> str:
        """返回超长文本。"""
        return "y" * 10_000

    registry.register_function(
        long_text,
        name="long_text",
        policy=ToolPolicy(access="public", output_limits=OutputLimits(max_text_bytes=1024)),
    )
    hook = Hook()
    result = await run(make_executor(registry, on_oversize=hook), "long_text")
    assert result.ok and result.content[0].truncated  # type: ignore[union-attr]
    assert hook.seen == []


async def test_failure_and_timeout_fall_back_to_original_error(make_executor: Any) -> None:
    """钩子抛出异常、超时或返回非 ``ToolOutput`` 时，返回原来的大小限制错误。"""
    for hook, kwargs in (
        (Hook(raises=RuntimeError("summarizer down")), {}),
        (Hook(delay_s=5), {"oversize_timeout_s": 0.05}),
        (Hook(result="not an output"), {}),
    ):
        result = await run(make_executor(big_registry(), on_oversize=hook, **kwargs))
        assert result.error is not None and result.error.code == "output_too_large"


async def test_hook_output_is_limited_again(make_executor: Any) -> None:
    """改写后的输出仍超限时失败（``output_too_large_after_hook``），钩子不能绕过限制。"""
    hook = Hook(result=ToolOutput(content=[JsonBlock(data={"still": "z" * 5000})]))
    result = await run(make_executor(big_registry(), on_oversize=hook))
    assert result.error is not None and result.error.code == "output_too_large_after_hook"


async def test_hook_output_is_validated_and_filtered(make_executor: Any) -> None:
    """声明了输出 Schema 的工具，改写结果必须仍然符合 Schema；``ResultFilter`` 对改写结果生效。"""
    registry = ToolRegistry()
    schema = {"type": "object", "properties": {"rows": {"type": "array"}}, "required": ["rows"]}

    async def big() -> dict[str, Any]:
        """返回超限的结构化结果。"""
        return {"rows": ["x" * 50] * 100}

    spec = ToolSpec(
        name="big",
        input_schema={"type": "object", "properties": {}},
        output_schema=schema,
        kind="python",
        policy=ToolPolicy(access="public", output_limits=LIMITS),
    )
    from agent_tools.binding import PythonTarget

    registry.register(
        ToolBinding(
            spec=spec,
            target=PythonTarget.from_function(big),
            output_validator=compile_schema(schema),
        )
    )
    bad = Hook(result=ToolOutput(content=[TextBlock(text="summary")], structured={"n": 1}))
    result = await run(make_executor(registry, on_oversize=bad))
    assert result.error is not None and result.error.code == "output_invalid"

    good = Hook(
        result=ToolOutput(content=[TextBlock(text="first rows")], structured={"rows": ["x"]})
    )

    class Upper:
        """把文本改成大写的过滤器。"""

        def filter(self, spec: ToolSpec, output: ToolOutput, ctx: ToolContext) -> ToolOutput:
            """把文本块改成大写。"""
            blocks = [
                TextBlock(text=b.text.upper()) if isinstance(b, TextBlock) else b
                for b in output.content
            ]
            return ToolOutput(content=blocks, structured=output.structured)

    result = await run(make_executor(registry, on_oversize=good, result_filter=Upper()))
    assert result.ok and result.to_model_text() == "FIRST ROWS"


async def test_hook_receives_scrubbed_output_and_result_is_scrubbed(make_executor: Any) -> None:
    """钩子拿到的输出已清除本次调用的已知密钥；钩子把密钥写回结果时也会被再次清除。"""
    secret = "sk-test-secret-value-123456"
    registry = ToolRegistry()

    async def leaky() -> dict[str, Any]:
        """返回带有已知密钥的超限输出。"""
        return {"token": secret, "rows": ["x" * 50] * 100}

    registry.register_function(
        leaky, name="leaky", policy=ToolPolicy(access="public", output_limits=LIMITS)
    )

    from agent_tools.adapters.python import PythonAdapter

    class SecretAdapter(PythonAdapter):
        """在调用时登记一个已知密钥的 Python 适配器。"""

        async def invoke(self, binding: Any, args: Any, ctx: Any, attempt: Any) -> ToolOutput:
            """登记密钥后照常调用。"""
            attempt.secrets.add(secret)
            return await super().invoke(binding, args, ctx, attempt)

    class Echo:
        """把输入中 token 的值写回结果。"""

        def __init__(self) -> None:
            """创建钩子。"""
            self.seen = ""

        async def on_oversize(
            self, spec: ToolSpec, output: ToolOutput, error: OutputLimitError, ctx: ToolContext
        ) -> ToolOutput:
            """记录输入，再把（试图）带密钥的文本写回。"""
            self.seen = output.model_dump_json()
            return ToolOutput(content=[TextBlock(text=f"leak {secret}")])

    from agent_tools.runtime.threads import BoundedThreadRunner

    hook = Echo()
    executor = make_executor(
        registry, on_oversize=hook, adapters={"python": SecretAdapter(BoundedThreadRunner(1))}
    )
    result = await run(executor, "leaky")
    assert secret not in hook.seen
    assert result.ok and secret not in result.to_model_text()
