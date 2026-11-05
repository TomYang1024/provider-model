"""多轮工具调用示例。SDK 从不执行工具：由应用自己执行。

运行：KIMI_API_KEY=... python examples/messages_tools.py
"""

import asyncio
import json

from provider_sdk import Message, ProviderSDK, ToolDefinition, ToolResult

WEATHER = ToolDefinition(
    name="get_weather",
    description="Get the current weather for a city",
    parameters={
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
    },
)


def get_weather(city: str) -> dict:
    return {"city": city, "condition": "sunny", "temp_c": 25}  # 替换为真实的工具实现


async def main() -> None:
    provider, model = "kimi", "kimi-k2.6"
    history: list[Message | ToolResult] = [
        Message.system("你是天气助手"),
        Message.user("北京和上海今天天气怎么样？"),
    ]
    async with ProviderSDK.from_env() as sdk:
        for _ in range(5):  # 有上限的 Agent 循环，防止模型无限调用工具
            resp = await sdk.messages(
                provider=provider, model=model, messages=history, tools=[WEATHER]
            )
            # 把模型回复（含 tool_calls / reasoning_content）加入历史，下一轮回传给模型
            history.append(resp.message)
            if resp.finish_reason != "tool_calls":
                print(resp.text)
                return
            # 执行每个工具调用，把结果以 ToolResult 形式加入历史
            for call in resp.tool_calls:
                try:
                    result = json.dumps(get_weather(**call.parsed_arguments()), ensure_ascii=False)
                    history.append(ToolResult(tool_call_id=call.id, content=result))
                except (ValueError, TypeError) as exc:
                    history.append(
                        ToolResult(tool_call_id=call.id, content=str(exc), is_error=True)
                    )


asyncio.run(main())
