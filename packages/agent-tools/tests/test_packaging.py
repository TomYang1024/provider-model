"""打包测试（``-m packaging``）：构建 wheel / sdist，并在临时虚拟环境中安装验证。

验证点：
* wheel 只包含 ``agent_tools``（含 ``py.typed``），不包含测试、示例或 Provider SDK；
* sdist 包含源码与 ``pyproject.toml``；
* 不带 extras 安装时，不会装上 ``provider_sdk``、``openai``、``httpx2``、``jsonschema``、``mcp``；
  导入桥接模块、HTTP 适配器、编译 Schema 时都会给出明确的安装提示；核心功能可以独立使用；
* 安装 ``[http]`` extra 后可以注册并校验 HTTP 工具，仍然不会装上 Provider SDK；
* 安装 ``[mcp]`` extra 后可以通过 stdio 发现、注册并调用 MCP 工具，仍然不会装上 Provider SDK。

这些检查需要从包索引下载依赖（或使用本地 uv 缓存）。
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

CORE_SCRIPT = """
import asyncio, importlib.util
for name in ("provider_sdk", "openai", "httpx2", "jsonschema", "mcp"):
    assert importlib.util.find_spec(name) is None, f"{name} must not be installed"

def expect_hint(action, hint):
    try:
        action()
    except Exception as exc:  # ImportError 或 RegistryError
        assert hint in str(exc), str(exc)
    else:
        raise AssertionError(f"expected a hint mentioning {hint}")

expect_hint(lambda: __import__("agent_tools.integrations.provider_sdk"), "agent-tools[provider-sdk]")
expect_hint(lambda: __import__("agent_tools.adapters.http"), "agent-tools[http]")
expect_hint(lambda: __import__("agent_tools.adapters.mcp"), "agent-tools[mcp]")
expect_hint(lambda: __import__("agent_tools.integrations.mcp"), "agent-tools[mcp]")
from agent_tools.security.schema import compile_schema
expect_hint(lambda: compile_schema({"type": "object"}), "agent-tools[http]")

from agent_tools import ToolContext, ToolExecutor, ToolInvocation, ToolPolicy, ToolRegistry

async def ping() -> str:
    return "pong"

async def main() -> None:
    registry = ToolRegistry()
    registry.register_function(ping, name="ping", policy=ToolPolicy(access="public"))
    async with ToolExecutor(registry) as executor:
        result = await executor.execute(ToolInvocation(call_id="c", tool="ping"), ToolContext())
        assert result.to_model_text() == "pong", result

asyncio.run(main())
print("OK")
"""

HTTP_SCRIPT = """
import asyncio, importlib.util
assert importlib.util.find_spec("provider_sdk") is None, "provider_sdk must not be installed"
from agent_tools import (
    HttpTarget, NetworkPolicy, ToolContext, ToolExecutor, ToolInvocation, ToolPolicy, ToolRegistry,
)
from agent_tools.adapters.http import HttpAdapter, HttpAdapterConfig

async def main() -> None:
    registry = ToolRegistry()
    registry.register_http(
        HttpTarget(method="GET", base_url="https://api.example.com"),
        name="api.call",
        input_schema={"type": "object", "properties": {"q": {"type": "string"}},
                      "additionalProperties": False},
        policy=ToolPolicy(access="public"),
    )
    adapter = HttpAdapter(HttpAdapterConfig(network=NetworkPolicy(allowed_hosts=frozenset())))
    async with ToolExecutor(registry, adapters={"http": adapter}) as executor:
        bad = await executor.execute(
            ToolInvocation(call_id="c", tool="api_call", arguments={"q": 1}), ToolContext()
        )
        assert bad.error and bad.error.code == "arguments_invalid", bad
        blocked = await executor.execute(
            ToolInvocation(call_id="d", tool="api_call", arguments={"q": "x"}), ToolContext()
        )
        assert blocked.error and blocked.error.code == "destination_not_allowed", blocked

asyncio.run(main())
print("OK")
"""


MCP_SCRIPT = """
import asyncio, importlib.util, sys
assert importlib.util.find_spec("provider_sdk") is None, "provider_sdk must not be installed"
from agent_tools import ToolContext, ToolExecutor, ToolInvocation, ToolPolicy, ToolRegistry
from agent_tools.adapters.mcp import McpAdapter, McpServerConfig, McpServerManager, StdioTransportConfig
from agent_tools.integrations.mcp import McpToolAllow, McpToolSync

async def main() -> None:
    server = McpServerConfig(
        server_id="local",
        transport=StdioTransportConfig(command=sys.executable, args=("-I", sys.argv[1])),
    )
    manager = McpServerManager([server])
    registry = ToolRegistry()
    report = await McpToolSync(registry, manager).sync(
        "local", [McpToolAllow(remote_name="secret_echo", policy=ToolPolicy(access="public"))]
    )
    assert report.registered == ("mcp.local.secret_echo",), report
    async with ToolExecutor(registry, adapters={"mcp": McpAdapter(manager)}) as executor:
        result = await executor.execute(
            ToolInvocation(call_id="c", tool="mcp_local_secret_echo", arguments={"text": "hi"}),
            ToolContext(),
        )
        assert result.to_model_text() == "hi", result

asyncio.run(main())
print("OK")
"""


def _run(cmd: list[str], cwd: Path | None = None) -> str:
    """运行命令，失败时把输出带进断言信息。

    Args:
        cmd: 命令。
        cwd: 工作目录。

    Returns:
        标准输出。
    """
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    assert proc.returncode == 0, f"{cmd}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    return proc.stdout


def _uv() -> str:
    """返回 uv 可执行文件路径；没有安装 uv 时跳过测试。"""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not installed")
    return uv


@pytest.fixture(scope="module")
def dist(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """构建一次 wheel 和 sdist，供本模块的测试共用。

    Returns:
        构建产物所在的目录。
    """
    out = tmp_path_factory.mktemp("dist")
    _run([_uv(), "build", "--package", "agent-tools", "--out-dir", str(out)], cwd=REPO_ROOT)
    return out


def _install(tmp_path: Path, requirement: str) -> tuple[Path, str]:
    """创建临时虚拟环境并安装指定的需求。

    Args:
        tmp_path: 临时目录。
        requirement: 需求字符串（wheel 路径，可以带 extras）。

    Returns:
        ``(解释器路径, 小写的 pip freeze 输出)``。
    """
    uv = _uv()
    venv = tmp_path / "venv"
    _run([uv, "venv", str(venv), "--python", sys.executable])
    python = venv / "bin" / "python"
    _run([uv, "pip", "install", "--python", str(python), requirement])
    return python, _run([uv, "pip", "freeze", "--python", str(python)]).lower()


def _check(python: Path, script: str, cwd: Path, *args: str) -> None:
    """在隔离模式下运行检查脚本。

    ``-I`` 并在仓库外运行，避免把当前目录（仓库根目录下的 provider_sdk 源码）加入 ``sys.path``；
    structlog 未配置时会把元数据日志打印到标准输出，所以只检查最后一行。

    Args:
        python: 解释器路径。
        script: 检查脚本。
        cwd: 工作目录。
        *args: 传给脚本的命令行参数。
    """
    output = _run([str(python), "-I", "-c", script, *args], cwd=cwd)
    assert output.strip().splitlines()[-1] == "OK"


@pytest.mark.packaging
def test_artifacts_contain_only_the_package(dist: Path) -> None:
    """wheel 与 sdist 只包含 agent_tools 源码、类型标记和元数据。"""
    wheel = next(dist.glob("agent_tools-*.whl"))
    names = zipfile.ZipFile(wheel).namelist()
    assert "agent_tools/py.typed" in names
    assert "agent_tools/adapters/http.py" in names
    assert "agent_tools/integrations/provider_sdk.py" in names
    # P3：隔离子进程入口与 stdio 启动器必须随 wheel 发布
    assert "agent_tools/runtime/subprocess_child.py" in names
    assert "agent_tools/runtime/stdio_launcher.py" in names
    assert not [n for n in names if n.startswith(("tests/", "examples/", "provider_sdk/"))]
    metadata = next(n for n in names if n.endswith("METADATA"))
    text = zipfile.ZipFile(wheel).read(metadata).decode()
    assert "Provides-Extra: http" in text and "Provides-Extra: provider-sdk" in text

    sdist = next(dist.glob("agent_tools-*.tar.gz"))
    with tarfile.open(sdist) as tar:
        members = tar.getnames()
    assert any(m.endswith("/pyproject.toml") for m in members)
    assert any(m.endswith("/agent_tools/__init__.py") for m in members)
    assert not any("/tests/" in m for m in members)


@pytest.mark.packaging
def test_core_install_has_no_optional_dependencies(dist: Path, tmp_path: Path) -> None:
    """不带 extras 安装：没有 Provider SDK 与 HTTP 依赖，缺少 extra 时给出提示，核心功能可用。"""
    wheel = next(dist.glob("agent_tools-*.whl"))
    python, installed = _install(tmp_path, str(wheel))
    assert "agent-tools" in installed  # 本地 wheel 显示为 ``agent-tools @ file://...``
    for name in ("provider-sdk", "openai", "httpx2", "jsonschema"):
        assert name not in installed, name
    _check(python, CORE_SCRIPT, tmp_path)


@pytest.mark.packaging
def test_http_extra_install(dist: Path, tmp_path: Path) -> None:
    """安装 [http] extra：可以注册 HTTP 工具、做 Schema 校验和目标地址检查，仍然没有 Provider SDK。"""
    wheel = next(dist.glob("agent_tools-*.whl"))
    python, installed = _install(tmp_path, f"{wheel}[http]")
    assert "httpx2" in installed and "jsonschema" in installed
    assert "provider-sdk" not in installed and "openai" not in installed
    _check(python, HTTP_SCRIPT, tmp_path)


@pytest.mark.packaging
def test_mcp_extra_install(dist: Path, tmp_path: Path) -> None:
    """安装 [mcp] extra：可以发现、注册并通过 stdio 调用 MCP 工具，仍然没有 Provider SDK。"""
    wheel = next(dist.glob("agent_tools-*.whl"))
    python, installed = _install(tmp_path, f"{wheel}[mcp]")
    assert "mcp==" in installed and "jsonschema" in installed
    assert "provider-sdk" not in installed and "openai" not in installed
    server = tmp_path / "mcp_stdio_server.py"
    server.write_text((Path(__file__).parent / "mcp_stdio_server.py").read_text(encoding="utf-8"))
    _check(python, MCP_SCRIPT, tmp_path, str(server))


ISOLATION_SCRIPT = """
import asyncio, sys
sys.path.insert(0, sys.argv[1])
import iso_tool
from agent_tools import (ProcessIsolation, ResourceLimits, ToolContext, ToolExecutor,
                         ToolInvocation, ToolPolicy, ToolRegistry)

async def main():
    registry = ToolRegistry()
    registry.register_function(iso_tool.double, name="double",
                               policy=ToolPolicy(access="public", timeout_s=20),
                               isolation=ProcessIsolation(limits=ResourceLimits(cpu_s=5)))
    async with ToolExecutor(registry) as executor:
        result = await executor.execute(ToolInvocation(call_id="c", tool="double",
                                                       arguments={"x": 21}), ToolContext())
        assert result.ok and result.to_model_text() == "42", result
        assert result.structured is None
    print("OK")

asyncio.run(main())
"""


@pytest.mark.packaging
def test_process_isolation_from_core_install(dist: Path, tmp_path: Path) -> None:
    """不带 extras 安装后，进程隔离可用：子进程入口来自已安装的 wheel，宿主模块按 sys.path 重新导入。"""
    wheel = next(dist.glob("agent_tools-*.whl"))
    python, _ = _install(tmp_path, str(wheel))
    (tmp_path / "iso_tool.py").write_text(
        "def double(x: int) -> int:\n    return x * 2\n", encoding="utf-8"
    )
    _check(python, ISOLATION_SCRIPT, tmp_path, str(tmp_path))
