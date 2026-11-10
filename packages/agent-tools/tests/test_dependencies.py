"""依赖方向的测试：按架构文档 §4 的分层规则扫描 import，防止出现反向依赖。"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import agent_tools

PACKAGE_ROOT = Path(agent_tools.__file__).resolve().parent

# 核心模块（integrations 之外）禁止依赖的外部包
FORBIDDEN_FOR_CORE = (
    "provider_sdk",
    "provider_gateway",
    "mcp",
    "openai",
    "httpx2",
    "httpcore2",
    "jsonschema",
    "referencing",
    "mcp_types",
    "anyio",
)
# 可选依赖只能出现在对应的模块中（[http] extra）；这些模块要么延迟导入，要么不被核心导入
OPTIONAL_IMPORTS: dict[str, set[str]] = {
    "security.schema": {"jsonschema", "referencing"},
    "adapters.http": {"httpx2", "httpcore2"},
    "adapters.mcp": {"mcp", "mcp_types", "httpcore2", "httpx2", "anyio"},
}


def _group(module: str) -> str:
    """把模块名映射到架构分层中的分组。

    Args:
        module: ``agent_tools`` 下的模块名（不含 ``agent_tools.`` 前缀），例如 ``protocol.spec``。

    Returns:
        分组名。
    """
    head = module.split(".")[0]
    if head in ("binding", "runtime", "version"):
        return "L1"
    if head == "adapters":
        return "adapters.base" if module == "adapters.base" else "adapters"
    return head


# 每个分组允许依赖的分组（本分组内部的相互引用总是允许）
ALLOWED: dict[str, set[str]] = {
    "protocol": set(),
    "L1": {"protocol"},
    "registry": {"protocol", "L1", "security"},
    "adapters.base": {"protocol", "L1", "security"},
    "adapters": {"protocol", "L1", "adapters.base", "security"},
    "router": {"protocol", "L1", "registry", "adapters.base"},
    "security": {"protocol", "L1"},
    "governance": {"protocol", "L1"},
    "observability": {"protocol", "L1"},
    "executor": {
        "protocol",
        "L1",
        "registry",
        "adapters.base",
        "adapters",
        "router",
        "security",
        "governance",
        "observability",
    },
}


def _modules() -> list[tuple[str, Path]]:
    """列出 ``agent_tools`` 下的全部模块。

    Returns:
        ``(模块名, 文件路径)`` 列表；包的 ``__init__`` 用包名表示。
    """
    found = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        rel = path.relative_to(PACKAGE_ROOT).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        found.append((".".join(parts), path))
    return found


def _imports(module: str, path: Path) -> set[str]:
    """解析一个模块的全部 import，相对导入会被换算成绝对模块名。

    Args:
        module: 模块名（不含 ``agent_tools.`` 前缀）。
        path: 文件路径。

    Returns:
        被导入的绝对模块名集合。
    """
    is_package = path.name == "__init__.py"
    package_parts = ["agent_tools", *module.split(".")] if module else ["agent_tools"]
    if not is_package:
        package_parts = package_parts[:-1]
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package_parts[: len(package_parts) - node.level + 1]
                target = ".".join([*base, node.module] if node.module else base)
            else:
                target = node.module or ""
            names.add(target)
    return names


def test_core_never_imports_provider_sdk_or_optional_dependencies() -> None:
    """除 integrations 外，任何模块都不能 import Provider SDK、网关；可选依赖只能出现在指定模块中。"""
    violations = []
    for module, path in _modules():
        if module.startswith("integrations"):
            continue
        allowed = OPTIONAL_IMPORTS.get(module, set())
        for name in _imports(module, path):
            head = name.split(".")[0]
            if head in FORBIDDEN_FOR_CORE and head not in allowed:
                violations.append(f"{module} imports {name}")
    assert violations == []


def test_layering_rules() -> None:
    """模块只能依赖同层或下层：protocol 不依赖其他模块，适配器、中间件不依赖执行器等上层模块。"""
    violations = []
    for module, path in _modules():
        if module == "" or module.startswith("integrations"):
            continue  # 顶层 __init__ 负责汇总导出；integrations 可以依赖任何模块
        group = _group(module)
        allowed = ALLOWED[group] | {group}
        for name in _imports(module, path):
            if not name.startswith("agent_tools"):
                continue
            target = name.removeprefix("agent_tools").lstrip(".")
            if not target:
                violations.append(f"{module} imports the top-level package")
                continue
            if target.startswith("integrations"):
                violations.append(f"{module} imports {name}")
                continue
            if _group(target) not in allowed:
                violations.append(f"{module} ({group}) imports {name} ({_group(target)})")
    assert violations == []


def test_importing_agent_tools_does_not_load_provider_sdk() -> None:
    """在全新的解释器中导入 agent_tools，不会加载 Provider SDK、openai 或 [http] 可选依赖。"""
    code = (
        "import sys, agent_tools; "
        f"bad = [m for m in {FORBIDDEN_FOR_CORE!r} if m in sys.modules]; "
        "print(','.join(bad))"
    )
    out = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)
    assert out.stdout.strip() == ""
