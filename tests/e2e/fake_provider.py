"""用于进程级 / 容器测试的最小化 OpenAI 兼容假 Provider。

运行：python -m tests.e2e.fake_provider --port 18081
POST /chat/completions：返回 JSON，或较慢的 SSE 流（每 CHUNK_DELAY 秒一个 chunk）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

DELAY = float(os.environ.get("CHUNK_DELAY", "0.2"))
CHUNKS = int(os.environ.get("CHUNKS", "50"))


def _chunk(delta: dict, finish: str | None = None, usage: dict | None = None) -> str:
    body = {
        "id": "fake-1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "fake",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    if usage:
        body["usage"] = usage
    return f"data: {json.dumps(body)}\n\n"


async def completions(request: Request):  # type: ignore[no-untyped-def]
    body = await request.json()
    if not body.get("stream"):
        return JSONResponse(
            {
                "id": "fake-1",
                "object": "chat.completion",
                "created": 1,
                "model": "fake",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "pong"},
                    }
                ],
                "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
            }
        )

    async def gen():  # type: ignore[no-untyped-def]
        for i in range(CHUNKS):
            yield _chunk({"content": f"t{i} "})
            await asyncio.sleep(DELAY)
        yield _chunk({}, "stop", {"prompt_tokens": 3, "completion_tokens": CHUNKS})
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


app = Starlette(routes=[Route("/chat/completions", completions, methods=["POST"])])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
