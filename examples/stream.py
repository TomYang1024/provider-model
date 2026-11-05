"""流式输出示例：确定性地释放资源。

运行：QWEN_API_KEY=... python examples/stream.py
"""

import asyncio

from provider_sdk import Message, ProviderError, ProviderSDK


async def main() -> None:
    async with ProviderSDK.from_env() as sdk:
        try:
            # 使用 async with：无论正常结束、break 还是异常，连接都会被立即释放
            async with sdk.open_stream(
                provider="qwen",
                model="qwen-plus",
                messages=[Message.user("介绍一下 Python")],
                reasoning=True,  # Qwen 对应 enable_thinking
            ) as stream:
                async for event in stream:
                    # 按事件类型分别处理（StreamEvent 是可判别联合类型）
                    match event.type:
                        case "reasoning_delta":
                            print(f"\033[2m{event.text}\033[0m", end="", flush=True)
                        case "text_delta":
                            print(event.text, end="", flush=True)
                        case "tool_call_end":
                            print(f"\n[tool] {event.tool_call.name}({event.tool_call.arguments})")
                        case "usage":
                            print(f"\n[usage] {event.usage.model_dump(exclude_none=True)}")
                        case "message_end":
                            print(f"[done] finish_reason={event.finish_reason}")
        except ProviderError as exc:
            # 一旦已有输出，retryable 就是 False：不要盲目重试
            print(f"\n[error] {exc.error_code} retryable={exc.retryable}")


asyncio.run(main())
