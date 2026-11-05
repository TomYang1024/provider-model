"""单轮对话示例。运行：DEEPSEEK_API_KEY=... python examples/chat.py"""

import asyncio

from provider_sdk import ProviderError, ProviderSDK


async def main() -> None:
    # 从环境变量读取所有 Provider 配置；async with 退出时自动关闭连接池
    async with ProviderSDK.from_env() as sdk:
        try:
            resp = await sdk.chat(
                provider="deepseek",
                model="deepseek-flash",
                prompt="用一句话介绍 Python",
                system="你是专业助手",
                max_tokens=200,
            )
        except ProviderError as exc:
            # 统一异常：可根据 retryable 决定是否重试
            print(f"failed: {exc.error_type}/{exc.error_code} retryable={exc.retryable}")
            return
        print(resp.text)
        print(resp.usage.model_dump(), resp.request_id, resp.trace.trace_id)


asyncio.run(main())
