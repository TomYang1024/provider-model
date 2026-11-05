# syntax=docker/dockerfile:1.7
#
# Provider Gateway 镜像（FastAPI + provider_sdk）。
#
# 可复现构建：
#   * 基础镜像按标签固定（CI 中建议按 digest 固定：PYTHON_IMAGE=python@sha256:...）
#   * 依赖从 uv.lock 安装，并使用 --require-hashes 校验哈希
#   * 通过 SOURCE_DATE_EPOCH 使项目 wheel 字节级可复现
#
# 构建：docker build -t provider-gateway:1.0.0 .
# 扫描：docker scout cves provider-gateway:1.0.0   （或：trivy image provider-gateway:1.0.0）

ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.12.1

FROM ${UV_IMAGE} AS uv

# ---------------------------------------------------------------------------------------
# builder 阶段：把锁定的依赖安装到 venv 中，并构建项目 wheel
# ---------------------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS builder
ARG SOURCE_DATE_EPOCH=1767225600
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /src

# 1) 依赖层：只有锁文件变化时才会失效，充分利用构建缓存
COPY pyproject.toml uv.lock README.md ./
RUN uv export --frozen --no-dev --extra gateway --no-emit-project \
        --format requirements-txt --output-file /tmp/requirements.txt \
 && python -m venv /opt/venv \
 && /opt/venv/bin/pip install --require-hashes --no-deps -r /tmp/requirements.txt

# 2) 项目层：安装项目 wheel，执行依赖一致性检查，并预编译字节码
COPY provider_sdk ./provider_sdk
COPY provider_gateway ./provider_gateway
RUN uv build --wheel --out-dir /dist \
 && /opt/venv/bin/pip install --no-deps /dist/*.whl \
 && /opt/venv/bin/pip check \
 && /opt/venv/bin/python -m compileall -q /opt/venv/lib

# ---------------------------------------------------------------------------------------
# runtime 阶段：不含构建工具和源码目录，以非 root 用户运行
# ---------------------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime
LABEL org.opencontainers.image.title="provider-gateway" \
      org.opencontainers.image.description="Multi-provider LLM gateway (DeepSeek, GLM, Kimi, Qwen)" \
      org.opencontainers.image.version="1.0.0"
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GATEWAY_HOST=0.0.0.0 \
    GATEWAY_PORT=8080
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin app
COPY --from=builder /opt/venv /opt/venv
USER 10001:10001
WORKDIR /tmp
EXPOSE 8080
# 只检查存活状态：绝不依赖 Collector 或 Provider 的可用性
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=3 \
  CMD ["python", "-c", "import sys,urllib.request as u; sys.exit(0 if u.urlopen('http://127.0.0.1:8080/health/live', timeout=2).status == 200 else 1)"]
STOPSIGNAL SIGTERM
# exec 形式：配合 `init: true`，python 进程能直接收到 SIGTERM 并执行优雅停机
CMD ["python", "-m", "provider_gateway"]
