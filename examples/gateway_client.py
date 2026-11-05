"""从任意服务通过 HTTP / SSE 调用 Docker 网关的示例。

运行：GATEWAY_URL=http://127.0.0.1:8080 GATEWAY_TOKEN=... python examples/gateway_client.py
"""

import json
import os

import httpx2

URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8080")
HEADERS = {
    "authorization": f"Bearer {os.environ['GATEWAY_TOKEN']}",
    # 可选：延续调用方的 Trace
    # "traceparent": "00-<trace-id>-<span-id>-01",
}

with httpx2.Client(base_url=URL, headers=HEADERS, timeout=120) as client:
    r = client.post(
        "/v1/chat", json={"provider": "deepseek", "model": "deepseek-flash", "prompt": "你好"}
    )
    r.raise_for_status()
    print(r.json()["message"]["content"])

    body = {
        "provider": "glm",
        "model": "glm-4.6",
        "messages": [{"role": "user", "content": "写一首短诗"}],
    }
    # 逐行解析 SSE：event 行给出事件类型，data 行是该事件的 JSON
    with client.stream("POST", "/v1/stream", json=body) as resp:
        event = None
        for line in resp.iter_lines():
            if line.startswith("event: "):
                event = line[7:]
            elif line.startswith("data: "):
                data = json.loads(line[6:])
                if event == "text_delta":
                    print(data["text"], end="", flush=True)
                elif event == "error":
                    print("\n[error]", data["error"]["code"])
                elif event == "message_end":
                    print("\n[usage]", data["response"]["usage"])
