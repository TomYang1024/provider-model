"""锁定版本的 OpenTelemetry 属性名。

GenAI 规范的 ``execute_tool`` 相关属性来自 ``opentelemetry-semantic-conventions==0.66b2``
（schema 1.44.0），已核实存在；该规范仍处于孵化阶段，所以把名字复制到这里而不是从私有模块导入，
并由单元测试与已安装的规范包逐一比对。

规范中没有确认到工具专用的指标，指标统一使用 ``app.tool.*`` 命名空间。
"""

SCHEMA_URL = "https://opentelemetry.io/schemas/1.44.0"

# GenAI 语义规范
GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
GEN_AI_TOOL_NAME = "gen_ai.tool.name"
GEN_AI_TOOL_CALL_ID = "gen_ai.tool.call.id"
#: 只在 ``redacted`` / ``full`` 日志模式下、经过脱敏和截断后才写入。
GEN_AI_TOOL_CALL_ARGUMENTS = "gen_ai.tool.call.arguments"
GEN_AI_TOOL_CALL_RESULT = "gen_ai.tool.call.result"
ERROR_TYPE = "error.type"
#: ``gen_ai.operation.name`` 的取值。
OPERATION_EXECUTE_TOOL = "execute_tool"

# 应用自定义属性（``app.tool.`` 命名空间）
APP_TOOL_NAME = "app.tool.name"
APP_TOOL_VERSION = "app.tool.version"
APP_TOOL_KIND = "app.tool.kind"
APP_TOOL_STATUS = "app.tool.status"
APP_TOOL_ERROR_CODE = "app.tool.error_code"
APP_TOOL_ATTEMPTS = "app.tool.attempts"
APP_REQUEST_ID = "app.tool.request_id"

# 指标名
METRIC_DURATION = "app.tool.duration"
METRIC_CALLS = "app.tool.calls"
METRIC_INFLIGHT = "app.tool.inflight"
METRIC_SYNC_ORPHANED = "app.tool.sync.orphaned"
METRIC_TELEMETRY_FAILURES = "app.tool.telemetry.failures"
METRIC_AUDIT_FAILURES = "app.tool.audit.failures"
METRIC_RETRIES = "app.tool.retries"

#: 未解析到已注册工具时，指标中工具名标签的统一取值，防止模型随意编造的名字造成标签基数爆炸。
UNRESOLVED_TOOL = "_other"
