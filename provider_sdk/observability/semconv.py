"""锁定版本的 OpenTelemetry 语义规范属性名。

基于 ``opentelemetry-semantic-conventions==0.66b2``（schema
``https://opentelemetry.io/schemas/1.44.0``）；GenAI 规范仍处于*孵化*阶段，
因此把属性名复制到这里，而不是从私有模块 ``opentelemetry.semconv._incubating`` 导入。
单元测试会将这些常量与已安装的语义规范包逐一比对。
"""

SCHEMA_URL = "https://opentelemetry.io/schemas/1.44.0"

# GenAI 语义规范属性
GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
GEN_AI_PROVIDER_NAME = "gen_ai.provider.name"
GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_REQUEST_MAX_TOKENS = "gen_ai.request.max_tokens"
GEN_AI_REQUEST_TEMPERATURE = "gen_ai.request.temperature"
GEN_AI_REQUEST_TOP_P = "gen_ai.request.top_p"
GEN_AI_REQUEST_SEED = "gen_ai.request.seed"
GEN_AI_REQUEST_STREAM = "gen_ai.request.stream"
GEN_AI_RESPONSE_ID = "gen_ai.response.id"
GEN_AI_RESPONSE_MODEL = "gen_ai.response.model"
GEN_AI_RESPONSE_FINISH_REASONS = "gen_ai.response.finish_reasons"
GEN_AI_RESPONSE_TIME_TO_FIRST_CHUNK = "gen_ai.response.time_to_first_chunk"
GEN_AI_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS = "gen_ai.usage.cache_read.input_tokens"
GEN_AI_USAGE_REASONING_OUTPUT_TOKENS = "gen_ai.usage.reasoning.output_tokens"
GEN_AI_INPUT_MESSAGES = "gen_ai.input.messages"
GEN_AI_OUTPUT_MESSAGES = "gen_ai.output.messages"
GEN_AI_TOKEN_TYPE = "gen_ai.token.type"
SERVER_ADDRESS = "server.address"
SERVER_PORT = "server.port"
ERROR_TYPE = "error.type"

# GenAI 语义规范指标名
METRIC_OPERATION_DURATION = "gen_ai.client.operation.duration"
METRIC_TOKEN_USAGE = "gen_ai.client.token.usage"
METRIC_TIME_TO_FIRST_CHUNK = "gen_ai.client.operation.time_to_first_chunk"

# 应用自定义属性（``app.`` 命名空间），语义规范中没有对应项
APP_REQUEST_ID = "app.llm.request_id"
APP_OUTCOME = "app.llm.outcome"
APP_STREAM = "app.llm.stream"
APP_PROVIDER = "app.llm.provider"
APP_OPERATION = "app.llm.operation"
APP_STATUS = "app.llm.status"
APP_USAGE_SOURCE = "app.llm.usage_source"
APP_TTFT_MS = "app.llm.ttft_ms"
APP_ERROR_CODE = "app.llm.error_code"
APP_RETRYABLE = "app.llm.retryable"
APP_STREAM_EVENTS = "app.llm.stream.events"
APP_PARTIAL_OUTPUT = "app.llm.partial_output"

#: 对话补全的 ``gen_ai.operation.name`` 取值。
OPERATION_CHAT = "chat"
