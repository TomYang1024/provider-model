.PHONY: install lint typecheck test test-integration test-container test-all image scan up down

install:
	uv sync --frozen

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run mypy

test:            ## 单元 + 网关 + 进程级测试（无需外网和密钥）
	uv run pytest

test-integration: ## 调用真实 Provider API（需要 *_API_KEY）
	uv run pytest -m integration

test-container:  ## 构建镜像并运行 Compose 编排
	uv run pytest -m container

test-all: lint typecheck test test-container

image:
	docker build -t provider-gateway:1.0.0 .

scan: image      ## 镜像漏洞扫描（docker scout 或 trivy）
	docker scout cves provider-gateway:1.0.0 || trivy image provider-gateway:1.0.0

up:
	docker compose up -d --build --wait

down:
	docker compose down
