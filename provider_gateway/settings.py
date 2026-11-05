"""网关配置（环境变量前缀 ``GATEWAY_``）。"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_", extra="ignore")

    host: str = "0.0.0.0"  # noqa: S104 - 容器内监听所有网卡，对外暴露由 Compose 端口映射控制
    port: int = Field(default=8080, gt=0, lt=65536)
    log_level: str = "INFO"
    log_queue_size: int = Field(default=10_000, gt=0)
    service_name: str = "provider-gateway"

    #: 网关接受的 Bearer 令牌，多个用逗号分隔。
    api_keys: SecretStr | None = None
    #: 每行一个令牌的文件（Docker Secret）。
    api_keys_file: Path | None = None
    #: 显式允许无认证访问（仅限本地开发）。
    auth_disabled: bool = False

    #: 客户端传入 ``timeout`` 的上限（秒）。
    max_request_timeout: float = Field(default=300.0, gt=0)
    max_stream_timeout: float = Field(default=900.0, gt=0)
    #: 同时进行的模型调用上限（包括流式），超出返回 503。
    max_concurrency: int = Field(default=256, gt=0)
    max_body_bytes: int = Field(default=4 * 1024 * 1024, gt=0)
    #: SSE 保活注释（ping）的发送间隔（秒），0 表示关闭。
    sse_ping_interval: float = Field(default=15.0, ge=0)
    #: 是否接受客户端传入的 ``provider_options``。
    allow_provider_options: bool = True

    #: 收到 SIGTERM 后 uvicorn 等待进行中请求的时间（秒）。
    #: 必须小于编排平台的停机宽限期减去 telemetry_flush_timeout。
    shutdown_grace_seconds: float = Field(default=20.0, ge=0)
    telemetry_flush_timeout: float = Field(default=5.0, gt=0)

    @model_validator(mode="after")
    def _require_auth(self) -> GatewaySettings:
        # 安全默认：未配置令牌且未显式关闭认证时拒绝启动
        if not self.auth_disabled and not self.tokens():
            raise ValueError(
                "no gateway API keys configured: set GATEWAY_API_KEYS or "
                "GATEWAY_API_KEYS_FILE (or GATEWAY_AUTH_DISABLED=true for local dev)"
            )
        return self

    def tokens(self) -> list[str]:
        """合并环境变量和文件中的令牌，去除空白行。"""
        raw: list[str] = []
        if self.api_keys is not None:
            raw.extend(self.api_keys.get_secret_value().split(","))
        if self.api_keys_file is not None and self.api_keys_file.is_file():
            raw.extend(self.api_keys_file.read_text(encoding="utf-8").splitlines())
        return [t.strip() for t in raw if t.strip()]
