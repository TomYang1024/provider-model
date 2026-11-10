"""运行时绑定：把可序列化的 :class:`~agent_tools.protocol.spec.ToolSpec` 和不可序列化的
执行目标（函数对象等）组合在一起。

这里的对象使用 ``dataclass`` 而不是 Pydantic 模型，因为它们持有函数、客户端等运行时资源，
不能也不应该被序列化。
"""

from __future__ import annotations

import importlib
import inspect
import threading
import typing
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, create_model

from .protocol.context import ToolContext
from .protocol.errors import RegistryError
from .protocol.spec import HttpTarget, McpTarget, ToolSpec
from .runtime.isolation import ProcessIsolation

__all__ = ["PythonTarget", "SchemaValidator", "ToolBinding", "ToolTarget"]


class SchemaValidator(Protocol):
    """已编译的 JSON Schema 校验器（实现见 ``agent_tools.security.schema``）。

    定义在这里而不是安全模块中，是为了让绑定层不依赖 ``jsonschema`` 这个可选依赖。
    """

    def errors(self, instance: Any) -> list[str]:
        """返回不含实例值的错误摘要列表；通过校验时返回空列表。

        Args:
            instance: 待校验的数据。

        Returns:
            形如 ``city: required`` 的错误摘要（只含路径和校验关键字）。
        """
        ...


@dataclass(frozen=True, slots=True)
class PythonTarget:
    """本地 Python 函数执行目标。

    默认在宿主进程内运行，只能用于宿主信任的代码：它与宿主进程拥有相同的权限。配置了
    ``isolation``（P3）时，每次调用在新的子进程中运行（独立地址空间、可被终止、资源限制、
    最小环境变量），但仍以宿主用户身份运行，不是完整的安全沙箱
    （见 :mod:`agent_tools.runtime.isolation`）。

    Attributes:
        fn: 同步或异步函数。
        args_model: 参数模型，用于校验模型给出的参数，并生成 ``input_schema``。
        is_async: ``fn`` 是否是协程函数。
        context_param: 需要注入 :class:`ToolContext` 的参数名（不出现在参数 Schema 中）。
        cancel_event_param: 需要注入 ``threading.Event`` 协作式取消事件的参数名（仅同步函数）。
        isolation: 进程隔离配置（P3）；``None`` 表示在宿主进程内运行。
    """

    fn: Callable[..., Any]
    args_model: type[BaseModel]
    is_async: bool
    context_param: str | None = None
    cancel_event_param: str | None = None
    isolation: ProcessIsolation | None = None

    @classmethod
    def from_function(
        cls,
        fn: Callable[..., Any],
        *,
        args_model: type[BaseModel] | None = None,
        isolation: ProcessIsolation | None = None,
    ) -> PythonTarget:
        """根据函数签名构造执行目标。

        规则：
        * 只支持普通参数和仅限关键字参数，不支持 ``*args``、``**kwargs`` 和仅限位置参数；
        * 每个参数都必须有类型注解，用于生成 Schema；
        * 类型为 :class:`ToolContext` 的参数会被注入上下文；
        * 同步函数中类型为 ``threading.Event`` 的参数会被注入协作式取消事件；
        * 其余参数组成参数模型（``extra="forbid"``，模型不能传入未声明的参数）；
        * 类型注解必须能在函数所在模块的全局作用域中解析（使用
          ``from __future__ import annotations`` 时，函数内部定义的类型无法解析）；
        * 配置了 ``isolation`` 时，函数必须能在子进程中按 ``模块:限定名`` 重新导入
          （模块顶层定义、模块不是 ``__main__``），并且不能声明协作式取消事件参数
          （子进程会被直接终止）。

        Args:
            fn: 工具函数。
            args_model: 显式指定的参数模型；其字段必须与函数的普通参数一一对应。
            isolation: 进程隔离配置（P3）。

        Returns:
            构造好的 :class:`PythonTarget`。

        Raises:
            RegistryError: 签名不满足上述规则，``args_model`` 与签名不一致，或函数不能在
                隔离子进程中重新导入。
        """
        if not callable(fn):
            raise RegistryError("tool function must be callable")
        is_async = inspect.iscoroutinefunction(fn)
        try:
            hints = typing.get_type_hints(fn)
        except Exception as exc:
            raise RegistryError(f"cannot resolve type hints of {fn!r}: {exc}") from exc

        context_param: str | None = None
        cancel_event_param: str | None = None
        fields: dict[str, Any] = {}
        for param in inspect.signature(fn).parameters.values():
            if param.kind not in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY):
                raise RegistryError(
                    f"parameter {param.name!r}: *args, **kwargs and positional-only "
                    "parameters are not supported"
                )
            annotation = hints.get(param.name)
            if annotation is None:
                raise RegistryError(f"parameter {param.name!r} must have a type annotation")
            if annotation is ToolContext:
                context_param = param.name
                continue
            if annotation is threading.Event:
                if is_async:
                    # 异步函数靠 asyncio 取消即可中断，不需要（也不应依赖）线程事件
                    raise RegistryError("cancel event parameters are only supported on sync tools")
                cancel_event_param = param.name
                continue
            default = ... if param.default is inspect.Parameter.empty else param.default
            fields[param.name] = (annotation, default)

        if args_model is None:
            model_name = f"{getattr(fn, '__name__', 'tool')}_args"
            args_model = create_model(  # type: ignore[call-overload]
                model_name, __config__=ConfigDict(extra="forbid"), **fields
            )
        elif set(args_model.model_fields) != set(fields):
            raise RegistryError("args_model fields must match the function parameters")

        assert args_model is not None  # 供类型检查器收窄类型
        if isolation is not None:
            if cancel_event_param is not None:
                raise RegistryError(
                    "isolated tools are terminated on timeout; cancel event parameters are "
                    "not supported"
                )
            _check_importable(fn)
        return cls(
            fn=fn,
            args_model=args_model,
            is_async=is_async,
            context_param=context_param,
            cancel_event_param=cancel_event_param,
            isolation=isolation,
        )

    @property
    def reference(self) -> tuple[str, str]:
        """函数的 ``(模块名, 限定名)``，隔离子进程据此重新导入函数。"""
        return self.fn.__module__, self.fn.__qualname__

    def input_schema(self) -> dict[str, Any]:
        """生成参数的 JSON Schema（Pydantic 生成的是 Draft 2020-12）。

        Returns:
            顶层为 ``type: object`` 的 JSON Schema。
        """
        return self.args_model.model_json_schema()


def _check_importable(fn: Callable[..., Any]) -> None:
    """确认函数能在另一个进程中按 ``模块:限定名`` 重新导入，并且得到的是同一个函数。

    Args:
        fn: 工具函数。

    Raises:
        RegistryError: 函数定义在 ``__main__``、函数内部，或按名字找不到同一个对象。
    """
    module_name = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", "")
    if not module_name or module_name == "__main__" or "<locals>" in qualname:
        raise RegistryError(
            "isolated tools must be defined at module level in an importable module "
            "(not __main__ and not inside a function)"
        )
    try:
        obj: Any = importlib.import_module(module_name)
        for part in qualname.split("."):
            obj = getattr(obj, part)
    except (ImportError, AttributeError) as exc:
        raise RegistryError(f"cannot re-import {module_name}:{qualname}") from exc
    if obj is not fn:
        raise RegistryError(f"{module_name}:{qualname} does not refer to the registered function")


#: 执行目标的联合类型：Python 函数（P0）、HTTP 请求（P1）、MCP 远端工具（P2）。
ToolTarget = PythonTarget | HttpTarget | McpTarget


@dataclass(frozen=True, slots=True)
class ToolBinding:
    """工具规格与执行目标的组合，是注册表中保存的单元。

    Attributes:
        spec: 工具规格。
        target: 执行目标。
        input_validator: 参数的 JSON Schema 校验器（HTTP、MCP 工具必有；Python 工具用
            Pydantic，为空）。
        output_validator: 输出的 JSON Schema 校验器（声明了 ``output_schema`` 时才有）。
    """

    spec: ToolSpec
    target: ToolTarget
    input_validator: SchemaValidator | None = None
    output_validator: SchemaValidator | None = None

    def __post_init__(self) -> None:
        """校验规格中的 ``kind`` 与执行目标的类型一致，并且需要的校验器已经提供。

        Raises:
            RegistryError: 二者不一致；HTTP / MCP 工具缺少参数校验器；声明了输出 Schema
                却没有输出校验器。
        """
        if isinstance(self.target, McpTarget):
            if self.spec.kind != "mcp":
                raise RegistryError("an McpTarget requires spec.kind == 'mcp'")
            if self.input_validator is None:
                raise RegistryError("MCP tools require a compiled input schema validator")
        if isinstance(self.target, PythonTarget) and self.spec.kind != "python":
            raise RegistryError("a PythonTarget requires spec.kind == 'python'")
        if isinstance(self.target, HttpTarget):
            if self.spec.kind != "http":
                raise RegistryError("an HttpTarget requires spec.kind == 'http'")
            if self.input_validator is None:
                raise RegistryError("HTTP tools require a compiled input schema validator")
        if self.spec.output_schema is not None and self.output_validator is None:
            raise RegistryError("output_schema requires a compiled output validator")

    def retry_permitted(self) -> bool:
        """判断这个工具在“错误可重试”时，是否**允许**自动重试。

        规则：工具必须声明 ``idempotent=True``；HTTP 的 ``POST`` / ``PATCH`` 默认不幂等，
        还要求 ``idempotency_header=True``（宿主确认下游支持 ``Idempotency-Key`` 去重）。
        注意：这里只决定“允许重试”，幂等性本身由工具或下游保证，基础设施无法验证。

        Returns:
            允许重试时为 True。
        """
        if not self.spec.annotations.idempotent:
            return False
        target = self.target
        if isinstance(target, HttpTarget) and target.method in ("POST", "PATCH"):
            return target.idempotency_header
        return True
