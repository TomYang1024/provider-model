"""注册表、别名、版本、快照与 Python 函数签名推导的测试。"""

from __future__ import annotations

import threading

import pydantic
import pytest

from agent_tools import (
    PythonTarget,
    RegistryConflictError,
    RegistryError,
    ToolAnnotations,
    ToolContext,
    ToolDisabledError,
    ToolNotFoundError,
    ToolPolicy,
    ToolRegistry,
    ToolVersionNotFoundError,
    model_alias,
)

PUBLIC = ToolPolicy(access="public")


async def _noop(x: int) -> int:
    """测试用工具：原样返回参数。"""
    return x


def test_model_alias_rules() -> None:
    """别名替换非法字符；超长名字截断并追加哈希，结果满足 Provider 的命名规则。"""
    assert model_alias("weather.get") == "weather_get"
    long_name = "a" * 100
    alias = model_alias(long_name)
    assert len(alias) == 64
    assert alias.startswith("a" * 55 + "_")
    assert model_alias("b" * 100) != model_alias("c" * 100)


def test_register_function_derives_schema_and_special_params() -> None:
    """从签名推导参数 Schema；ToolContext 与取消事件参数不会出现在 Schema 中。"""

    def tool(city: str, ctx: ToolContext, cancel: threading.Event, days: int = 1) -> str:
        """查询天气。

        第二段不会进入描述。
        """
        return city

    registry = ToolRegistry()
    spec = registry.register_function(tool, name="weather.get", policy=PUBLIC)
    assert spec.description == "查询天气。"
    assert set(spec.input_schema["properties"]) == {"city", "days"}
    assert spec.input_schema["required"] == ["city"]
    binding = registry.snapshot().resolve("weather_get")
    # P1 起 target 是 PythonTarget | HttpTarget 的联合类型，读取 Python 专有属性前要先收窄
    assert isinstance(binding.target, PythonTarget)
    assert binding.target.context_param == "ctx"
    assert binding.target.cancel_event_param == "cancel"


@pytest.mark.parametrize(
    "fn",
    [
        lambda *args: None,
        lambda a, /: None,
    ],
)
def test_register_function_rejects_unsupported_signatures(fn: object) -> None:
    """不支持 ``*args``、仅限位置参数以及没有类型注解的参数。"""
    with pytest.raises(RegistryError):
        ToolRegistry().register_function(fn, name="bad", policy=PUBLIC)  # type: ignore[arg-type]


def test_register_function_rejects_missing_annotation_and_async_cancel_event() -> None:
    """缺少类型注解的参数，以及异步函数上的取消事件参数，都会在注册时被拒绝。"""

    def untyped(x):  # type: ignore[no-untyped-def]
        """缺少注解。

        测试用：参数没有类型注解，注册时应当被拒绝。
        """
        return x

    async def async_with_event(ev: threading.Event) -> None:
        """异步函数不支持取消事件参数。"""

    with pytest.raises(RegistryError):
        ToolRegistry().register_function(untyped, name="a", policy=PUBLIC)
    with pytest.raises(RegistryError):
        ToolRegistry().register_function(async_with_event, name="b", policy=PUBLIC)


def test_destructive_tools_blocked_by_default() -> None:
    """不可逆工具默认禁止注册，宿主显式放开后才允许。"""
    destructive = ToolAnnotations(destructive=True)
    with pytest.raises(RegistryError):
        ToolRegistry().register_function(_noop, name="drop", policy=PUBLIC, annotations=destructive)
    allowed = ToolRegistry(allow_destructive_without_approval=True)
    assert allowed.register_function(_noop, name="drop", policy=PUBLIC, annotations=destructive)


def test_alias_conflicts_are_rejected_at_registration() -> None:
    """别名与其他工具的别名或内部名冲突时，在注册阶段拒绝。"""
    registry = ToolRegistry()
    registry.register_function(_noop, name="a.b", policy=PUBLIC)  # 别名 a_b
    with pytest.raises(RegistryConflictError):
        registry.register_function(_noop, name="a_b", policy=PUBLIC)  # 内部名等于已有别名
    with pytest.raises(RegistryConflictError):
        registry.register_function(_noop, name="x", policy=PUBLIC, alias="a_b")
    with pytest.raises(RegistryConflictError):
        registry.register_function(_noop, name="a.b", policy=PUBLIC)  # 同名同版本


def test_versions_active_and_alias_stability() -> None:
    """新版本需要显式切换；别名在版本间保持不变；宿主可以指定版本调用。"""
    registry = ToolRegistry()
    registry.register_function(_noop, name="calc.add", policy=PUBLIC)
    registry.register_function(_noop, name="calc.add", policy=PUBLIC, version="2")
    snap = registry.snapshot()
    assert snap.resolve("calc_add").spec.version == "1"
    assert snap.resolve("calc.add", "2").spec.version == "2"
    with pytest.raises(ToolVersionNotFoundError):
        snap.resolve("calc_add", "9")
    with pytest.raises(RegistryConflictError):
        registry.register_function(
            _noop, name="calc.add", policy=PUBLIC, version="3", alias="other"
        )

    registry.set_active("calc.add", "2")
    assert registry.snapshot().resolve("calc_add").spec.version == "2"
    assert registry.snapshot().alias_of("calc.add") == "calc_add"


def test_unregister_rules() -> None:
    """不能单独删除启用版本；可以删除非启用版本或整个工具。"""
    registry = ToolRegistry()
    registry.register_function(_noop, name="t", policy=PUBLIC)
    registry.register_function(_noop, name="t", policy=PUBLIC, version="2")
    with pytest.raises(RegistryError):
        registry.unregister("t", "1")
    registry.unregister("t", "2")
    registry.unregister("t")
    with pytest.raises(ToolNotFoundError):
        registry.snapshot().resolve("t")


def test_snapshots_are_immutable_and_tracked() -> None:
    """快照不受之后修改的影响；最近的快照可以按编号找回，超出保留范围后找不到。"""
    registry = ToolRegistry(history=2)
    registry.register_function(_noop, name="t", policy=PUBLIC)
    before = registry.snapshot()
    registry.disable("t")
    after = registry.snapshot()
    assert before.resolve("t").spec.name == "t"
    with pytest.raises(ToolDisabledError):
        after.resolve("t")
    assert registry.get_snapshot(after.id) is after
    registry.enable("t")
    registry.enable("t")
    assert registry.get_snapshot(before.id) is None


def test_unknown_tool_message_is_truncated() -> None:
    """未知工具名来自模型输出，错误信息中只回显截断后的 repr。"""
    with pytest.raises(ToolNotFoundError) as info:
        ToolRegistry().snapshot().resolve("x" * 500)
    assert len(info.value.safe_message) < 100


def test_spec_validation_errors_surface_at_registration() -> None:
    """非法的内部名在注册时就会报错。"""
    with pytest.raises(pydantic.ValidationError):
        ToolRegistry().register_function(_noop, name="Bad Name", policy=PUBLIC)
