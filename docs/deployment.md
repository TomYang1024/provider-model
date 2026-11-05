# 部署指南

## 部署模式

* **模式 A：Python 库。** `pip install provider-sdk`，在进程内直接调用 `ProviderSDK`。
* **模式 B：Docker 网关。** 使用下文的镜像，通过 HTTP/SSE 供其他语言和服务调用。网关是独立的包
  （`provider_gateway`），SDK 核心不会 import 它（有测试保证）。

## 镜像（`Dockerfile`）

* 多阶段构建：Builder 阶段把 `uv.lock` 导出为带哈希的依赖清单，使用 `pip --require-hashes` 安装，
  构建项目 wheel（设置 `SOURCE_DATE_EPOCH`，wheel 字节级可复现），并执行 `pip check`。
  Runtime 阶段只复制 `/opt/venv`。
* 固定 `python:3.12.13-slim-bookworm` 和 `uv:0.12.1` 版本。CI 中建议按 digest 固定：
  `--build-arg PYTHON_IMAGE=python@sha256:…`。
* 以非 root 用户 `10001:10001` 运行；镜像中没有源码目录、`.env`、测试代码和构建工具，
  `.dockerignore` 采用白名单方式。
* exec 形式的 `CMD ["python", "-m", "provider_gateway"]`、`STOPSIGNAL SIGTERM`，以及基于
  `/health/live` 的 `HEALTHCHECK`。
* 镜像扫描：`make scan`（`docker scout cves` 或 `trivy image`）。

## Compose（`compose.yaml`）

| 服务 | 网络 | 暴露方式 |
|---|---|---|
| `provider-gateway` | `edge`（访问 Provider 的出口）、`telemetry` | `127.0.0.1:8080`（在防火墙 / 负载均衡之后可设置 `GATEWAY_BIND=0.0.0.0`） |
| `otel-collector`（contrib 0.128.0） | `telemetry`（内部网络） | 不暴露 |
| `jaeger`（2.7.0） | `telemetry`、`jaeger-ui` | 仅 `127.0.0.1:16686` |

网关通过 `otel-collector:4317` 访问 Collector，绝不使用 `localhost` 访问其他容器。其他配置包括：

* 只读根文件系统 + `tmpfs` 挂载的 `/tmp`、`cap_drop: [ALL]`、`no-new-privileges`。
* `init: true`、`restart: unless-stopped`、CPU / 内存限制。
* json-file 日志按 10 MB × 5 份轮转。
* `stop_grace_period: 35s`，必须大于 `GATEWAY_SHUTDOWN_GRACE_SECONDS`（20）+
  `GATEWAY_TELEMETRY_FLUSH_TIMEOUT`（5）+ 余量。
* `depends_on` 设置 `required: false`：Collector 不可用时网关依然可以启动并提供服务。

### 密钥管理

可使用环境变量（`.env`，绝不提交到仓库，也不会复制进镜像），或使用 Docker Secrets：

```bash
mkdir -p secrets && chmod 700 secrets
printf '%s' "$DEEPSEEK_KEY" > secrets/deepseek_api_key      # 空文件 = 禁用该 Provider
printf '%s\n' "$TOKEN1" "$TOKEN2" > secrets/gateway_api_keys
docker compose -f compose.yaml -f compose.secrets.yaml up -d
```

### 跨网络传输 OTLP

内置编排使用明文 OTLP，因为流量只在 `internal: true` 的 Docker 内部网络中传输。
**不要在不可信网络上传输明文 OTLP。** 连接远程 Collector 时：

```env
OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.example.com:4317
OTEL_EXPORTER_OTLP_CERTIFICATE=/run/secrets/otel_ca.pem
OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE=/run/secrets/otel_client.pem   # mTLS
OTEL_EXPORTER_OTLP_CLIENT_KEY=/run/secrets/otel_client.key
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer%20<token>          # 或使用 Collector 认证扩展
```

同时在 Collector 的 OTLP Receiver 上启用 `tls`（以及认证扩展）。

## 生命周期

启动顺序：日志 → Telemetry → SDK → `ready`。Liveness 绝不依赖 Provider 或 Collector；
Readiness 反映初始化状态和是否正在停机。

收到 SIGTERM 后：

1. Readiness 变为 503，新的模型请求返回 `503 draining`。
2. uvicorn 停止接受新连接，最多等待 `GATEWAY_SHUTDOWN_GRACE_SECONDS` 让进行中的请求和流完成。
3. 取消剩余的请求任务，其中的流记录为 `cancelled`（Span + `llm.stream.cancelled` 日志）。
4. `sdk.aclose()` 关闭剩余的流以及所有 Provider HTTP Client。
5. Flush OpenTelemetry 并关闭 Exporter，耗时上限为 `GATEWAY_TELEMETRY_FLUSH_TIMEOUT`。
6. 排空日志队列，进程以退出码 0 退出。

`tests/e2e` 在真实进程上验证该流程，`tests/container` 在真实容器上验证。

## 扩容

网关每个容器运行一个进程，通过在负载均衡后增加容器实现扩容。每个容器有独立的 TracerProvider，
多副本之间通过 W3C 上下文和资源属性关联 Trace，例如
`OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=prod,service.instance.id=$HOSTNAME`。
SSE 需要关闭代理缓冲（网关会发送 `X-Accel-Buffering: no`），并且代理的空闲超时要大于
`GATEWAY_SSE_PING_INTERVAL`。

## 生产上线检查清单

- [ ] 网关令牌已轮换并以 Secret 方式存储；未设置 `GATEWAY_AUTH_DISABLED`
- [ ] 基础镜像按 digest 固定，CI 中执行镜像扫描
- [ ] `PROVIDER_SDK_LOG_MODE=metadata_only`（或经过数据保护评审后使用 `redacted`）
- [ ] 已确定采样率（`OTEL_TRACES_SAMPLER_ARG`）；OTLP 离开主机网络时启用 TLS
- [ ] 设置 `strict_models=true` 并显式列出允许使用的模型
- [ ] 每个启用的 Provider 和模型都通过了 `pytest -m integration`
- [ ] `stop_grace_period` > 停机等待时间 + Flush 超时
