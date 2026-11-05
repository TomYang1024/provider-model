"""SDK 配置：内置 Provider 默认值、环境变量加载和 TOML 覆盖配置。

密钥只能来自环境变量或文件（``*_API_KEY_FILE``，例如 Docker Secrets），
绝不允许直接写在配置文件中。

加载优先级（后者覆盖前者）：内置默认值 < TOML 配置文件 < 环境变量。
"""

from __future__ import annotations

import copy
import os
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from .schemas.provider import ModelCapabilities, ProviderConfig

LogMode = Literal["metadata_only", "redacted", "full"]


class LoggingConfig(BaseModel):
    """SDK 输入输出日志策略。

    ``full`` 模式只有在 ``full_content_acknowledged`` 同时为 true
    （环境变量 ``PROVIDER_SDK_ALLOW_FULL_LOGGING=true``）时才生效；否则 SDK
    会降级为 ``redacted`` 并输出警告。任何模式下都会清除密钥。
    """

    model_config = ConfigDict(extra="forbid")

    mode: LogMode = "metadata_only"
    full_content_acknowledged: bool = False
    #: *成功*调用的日志采样比例（错误日志总是记录）。
    sample_rate: float = Field(default=1.0, ge=0, le=1)
    #: redacted / full 模式下每个内容字段的最大字符数。
    max_field_chars: int = Field(default=2000, ge=16, le=1_000_000)
    #: 日志中最多记录的输入消息条数（取最近的若干条）。
    max_messages: int = Field(default=20, ge=1, le=10_000)

    @property
    def effective_mode(self) -> LogMode:
        """实际生效的日志模式：未显式授权的 full 模式降级为 redacted。"""
        if self.mode == "full" and not self.full_content_acknowledged:
            return "redacted"
        return self.mode


class ObservabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: 把脱敏后的输入 / 输出记录为 ``gen_ai.input/output.messages`` Span 属性，默认关闭。
    capture_content_in_spans: bool = False
    #: Provider 未返回 Usage 时进行启发式估算（``usage.source == "estimated"``），默认关闭。
    estimate_usage: bool = False


class SDKConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    providers: dict[str, ProviderConfig] = Field(default_factory=dict)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        config_file: str | Path | None = None,
    ) -> SDKConfig:
        """由内置默认值 + 可选的 TOML 文件 + 环境变量构建配置。"""
        env = dict(os.environ if environ is None else environ)
        # 深拷贝内置默认值，避免修改模块级常量
        raw: dict[str, Any] = {"providers": copy.deepcopy(BUILTIN_PROVIDERS)}

        path = config_file or env.get("PROVIDER_SDK_CONFIG_FILE")
        if path:
            with open(path, "rb") as fh:
                overlay = tomllib.load(fh)
            _merge_overlay(raw, overlay)

        providers: dict[str, ProviderConfig] = {}
        for name, pdata in raw["providers"].items():
            pdata = dict(pdata)
            # 环境变量前缀：deepseek -> DEEPSEEK；别名如 GLM 的 ZHIPUAI
            prefix = _env_prefix(name)
            aliases = pdata.pop("env_aliases", [])
            # 这两个字段只在加载阶段使用，不属于 ProviderConfig
            key_env = pdata.pop("api_key_env", None)
            key_file = pdata.pop("api_key_file", None)
            # 安全约束：配置文件中禁止出现明文密钥
            if "api_key" in pdata:
                raise ValueError(
                    f"providers.{name}.api_key must not be set in a config file; "
                    "use api_key_env / api_key_file or the environment"
                )
            api_key = _read_secret(env, [prefix, *aliases], key_env, key_file)
            if api_key:
                pdata["api_key"] = SecretStr(api_key)
            for env_name in (f"{p}_BASE_URL" for p in [prefix, *aliases]):
                if env.get(env_name):
                    pdata["base_url"] = env[env_name]
                    break
            if env.get(f"PROVIDER_SDK_{prefix}_PROXY"):
                pdata["proxy"] = env[f"PROVIDER_SDK_{prefix}_PROXY"]
            if env.get("PROVIDER_SDK_TRUST_ENV"):
                pdata.setdefault("trust_env", _truthy(env["PROVIDER_SDK_TRUST_ENV"]))
            # 启用规则：显式环境变量 > 配置文件 > 是否有密钥（没有密钥自动禁用）
            enabled_env = env.get(f"PROVIDER_SDK_{prefix}_ENABLED")
            if enabled_env is not None:
                pdata["enabled"] = _truthy(enabled_env)
            elif "enabled" not in pdata:
                pdata["enabled"] = bool(api_key)
            pdata.setdefault("name", name)
            providers[name] = ProviderConfig.model_validate(pdata)

        logging_raw = dict(raw.get("logging", {}))
        if env.get("PROVIDER_SDK_LOG_MODE"):
            logging_raw["mode"] = env["PROVIDER_SDK_LOG_MODE"]
        if env.get("PROVIDER_SDK_LOG_SAMPLE_RATE"):
            logging_raw["sample_rate"] = float(env["PROVIDER_SDK_LOG_SAMPLE_RATE"])
        if env.get("PROVIDER_SDK_LOG_MAX_CHARS"):
            logging_raw["max_field_chars"] = int(env["PROVIDER_SDK_LOG_MAX_CHARS"])
        if env.get("PROVIDER_SDK_ALLOW_FULL_LOGGING"):
            logging_raw["full_content_acknowledged"] = _truthy(
                env["PROVIDER_SDK_ALLOW_FULL_LOGGING"]
            )

        obs_raw = dict(raw.get("observability", {}))
        if env.get("PROVIDER_SDK_CAPTURE_CONTENT"):
            obs_raw["capture_content_in_spans"] = _truthy(env["PROVIDER_SDK_CAPTURE_CONTENT"])
        if env.get("PROVIDER_SDK_ESTIMATE_USAGE"):
            obs_raw["estimate_usage"] = _truthy(env["PROVIDER_SDK_ESTIMATE_USAGE"])

        return cls(
            providers=providers,
            logging=LoggingConfig.model_validate(logging_raw),
            observability=ObservabilityConfig.model_validate(obs_raw),
        )


# --------------------------------------------------------------------------------------
# 辅助函数
# --------------------------------------------------------------------------------------
def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_prefix(name: str) -> str:
    return name.upper().replace("-", "_")


def _read_secret(
    env: Mapping[str, str],
    prefixes: list[str],
    key_env: str | None,
    key_file: str | None,
) -> str | None:
    """解析 API Key，顺序：显式指定的环境变量名 → ``<P>_API_KEY`` → ``*_FILE`` 文件。"""
    names = ([key_env] if key_env else []) + [f"{p}_API_KEY" for p in prefixes]
    for name in names:
        value = env.get(name)
        if value:
            return value.strip()
    files = ([key_file] if key_file else []) + [
        f for f in (env.get(f"{n}_FILE") for n in names) if f
    ]
    for file in files:
        # 读取 Docker Secret 等文件；空文件视为未配置（对应 Provider 会被禁用）
        p = Path(file)
        if p.is_file():
            value = p.read_text(encoding="utf-8").strip()
            if value:
                return value
    return None


def _merge_overlay(raw: dict[str, Any], overlay: Mapping[str, Any]) -> None:
    """把 TOML 覆盖配置合并到内置默认值上。

    models 按模型逐个合并能力字段（只覆盖写出的字段），其他字典字段浅合并，
    标量字段直接覆盖；TOML 中新出现的 Provider 原样添加。
    """
    for section in ("logging", "observability"):
        if section in overlay:
            raw.setdefault(section, {}).update(overlay[section])
    for name, pdata in overlay.get("providers", {}).items():
        base = raw["providers"].get(name)
        if base is None:
            raw["providers"][name] = dict(pdata)
            continue
        for key, value in pdata.items():
            if key == "models" and isinstance(value, dict):
                models = base.setdefault("models", {})
                for model_id, caps in value.items():
                    models[model_id] = {**models.get(model_id, {}), **caps}
            elif isinstance(value, dict) and isinstance(base.get(key), dict):
                base[key] = {**base[key], **value}
            else:
                base[key] = value
    # 拼写错误的配置段直接报错，而不是被静默忽略
    unknown = set(overlay) - {"providers", "logging", "observability"}
    if unknown:
        raise ValueError(f"unknown config sections: {sorted(unknown)}")


def _caps(**kwargs: Any) -> dict[str, Any]:
    """构造能力声明字典，只保留与默认值不同的字段，便于 TOML 按字段合并。"""
    return ModelCapabilities(**kwargs).model_dump(exclude_defaults=True)


# --------------------------------------------------------------------------------------
# 内置 Provider 默认配置：2026-11-05 对照各 Provider 官方 API 文档核实
# （来源与验证矩阵见 docs/providers.md）。``documented`` = 文档明确说明；
# ``unverified`` = 文档未说明，属于最佳假设。在真实 API 上通过
# pytest -m integration 之前，没有任何一项标记为 ``tested``。
#
# 模型目录变化频繁：通过 TOML（PROVIDER_SDK_CONFIG_FILE）覆盖。
# strict_models 默认关闭，未列出的模型 ID 使用 default_capabilities。
# --------------------------------------------------------------------------------------
_DS_MODEL = _caps(
    reasoning=True,  # 混合思考，默认开启
    max_context_tokens=1_000_000,
    verification="documented",
    notes="thinking on by default; tool_choice='required' not stated in docs",
)
_GLM_HYBRID = _caps(reasoning=True, tool_choice_required=False, verification="documented")
_KIMI_K3 = _caps(
    reasoning=True,
    reasoning_always_on=True,
    verification="documented",
    notes="always thinks; depth via reasoning_effort",
)
_QWEN_HYBRID = _caps(
    reasoning=True,
    tool_choice_required=False,
    verification="documented",
    notes="tool_choice values other than auto are not documented",
)

BUILTIN_PROVIDERS: dict[str, dict[str, Any]] = {
    "deepseek": {
        "adapter": "deepseek",
        "base_url": "https://api.deepseek.com",
        "otel_provider_name": "deepseek",  # 语义规范中的标准取值
        "stream_usage_option": True,
        # 必需：请求带 tools 时必须回传历史 reasoning_content（否则返回 400）
        "send_reasoning_content": True,
        "default_capabilities": _caps(reasoning=True),
        "models": {
            "deepseek-flash": _DS_MODEL,
            "deepseek-v4-pro": _DS_MODEL,
        },
    },
    "glm": {
        "adapter": "glm",
        "env_aliases": ["ZHIPUAI"],
        "base_url": "https://open.bigmodel.cn/api/paas/v4/",
        "otel_provider_name": "zhipu",
        # 流式 chunk 顶层自带 usage；stream_options 未在文档中说明，因此不发送
        "stream_usage_option": False,
        "default_capabilities": _caps(tool_choice_required=False),
        "models": {
            "glm-5.3": _caps(
                reasoning=True,
                reasoning_always_on=True,
                tool_choice_required=False,
                verification="documented",
            ),
            "glm-5.2": _GLM_HYBRID,
            "glm-5.1": _GLM_HYBRID,
            "glm-5": _GLM_HYBRID,
            "glm-5-turbo": _GLM_HYBRID,
            "glm-4.7": _GLM_HYBRID,
            "glm-4.7-flash": _GLM_HYBRID,
            "glm-4.6": _GLM_HYBRID,
            "glm-4.5-air": _GLM_HYBRID,
            "glm-4.6v": _caps(
                reasoning=True,
                vision=True,
                tool_choice_required=False,
                verification="documented",
            ),
        },
    },
    "kimi": {
        "adapter": "kimi",
        "env_aliases": ["MOONSHOT"],
        "base_url": "https://api.moonshot.cn/v1",
        "otel_provider_name": "moonshot",
        # 文档说明：只有设置 stream_options.include_usage 才会在最后一个 chunk 返回 usage
        "stream_usage_option": True,
        # 文档说明：多轮对话中需原样回传 reasoning_content
        "send_reasoning_content": True,
        "models": {
            "kimi-k3.1-pro": _KIMI_K3,
            "kimi-k3.1-pro-highspeed": _KIMI_K3,
            "kimi-k3": _KIMI_K3,
            "kimi-k2.7-code": _caps(
                reasoning=True,
                reasoning_always_on=True,
                verification="documented",
                notes="thinking.type must be enabled",
            ),
            "kimi-k2.6": _caps(
                reasoning=True,
                verification="documented",
                notes="thinking toggle; thinking.keep='all' via provider_options",
            ),
        },
    },
    "qwen": {
        "adapter": "qwen",
        "env_aliases": ["DASHSCOPE"],
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "otel_provider_name": "alibaba.dashscope",
        "stream_usage_option": True,  # 文档已说明
        "default_capabilities": _caps(tool_choice_required=False),
        "models": {
            "qwen3.8-max": _QWEN_HYBRID,
            "qwen3.7-plus": _QWEN_HYBRID,
            "qwen3.5-flash": _QWEN_HYBRID,
            "qwen-plus": _QWEN_HYBRID,
            "qwen-flash": _QWEN_HYBRID,
            "qwen-max": _caps(
                tool_choice_required=False,
                verification="unverified",
                notes="function calling documented; thinking support not stated",
            ),
        },
    },
}
