"""启动网关：``python -m provider_gateway``。

收到 SIGTERM / SIGINT 后的优雅停机流程：

1. Readiness 变为 503，新的模型请求被拒绝（draining）；
2. uvicorn 停止接受新连接，最多等待 ``GATEWAY_SHUTDOWN_GRACE_SECONDS``
   让进行中的请求和流完成；
3. 取消剩余的请求任务（其中的流记录为 ``cancelled``）；
4. lifespan 关闭阶段关闭 SDK（Provider HTTP 客户端、剩余的流），在
   ``GATEWAY_TELEMETRY_FLUSH_TIMEOUT`` 内 Flush 并关闭 OpenTelemetry，最后排空日志队列；
5. 进程以退出码 0 退出。
"""

from __future__ import annotations

import sys
from types import FrameType

import uvicorn

from .app import LIFECYCLE, create_app
from .settings import GatewaySettings


class DrainingServer(uvicorn.Server):
    """收到停机信号时，先把网关标记为 draining，再交给 uvicorn 执行标准停机流程。"""

    def handle_exit(self, sig: int, frame: FrameType | None) -> None:
        LIFECYCLE.start_draining()
        super().handle_exit(sig, frame)


def main() -> None:
    try:
        settings = GatewaySettings()
    except Exception as exc:  # 配置错误：以非零退出码退出，并给出清晰提示
        print(f"provider-gateway: invalid configuration: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    config = uvicorn.Config(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_config=None,  # 日志由应用配置（JSON 输出到 stdout），uvicorn 不再单独配置
        access_log=False,  # 访问日志由中间件输出结构化的 http.request 事件替代
        lifespan="on",
        # SIGTERM 后等待进行中请求完成的最长时间
        timeout_graceful_shutdown=int(settings.shutdown_grace_seconds),
        timeout_keep_alive=5,
        server_header=False,  # 不暴露服务器软件信息
        proxy_headers=False,  # 不信任 X-Forwarded-*（需要时在可信代理后显式开启）
    )
    DrainingServer(config).run()


if __name__ == "__main__":
    main()
