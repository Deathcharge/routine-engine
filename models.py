"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🌀 Helix Spirals Data Models
Pydantic models matching frontend TypeScript types
"""

import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# Enums matching TypeScript types
class TriggerType(str, Enum):
    WEBHOOK = "webhook"
    SCHEDULE = "schedule"
    MANUAL = "manual"
    AGENT_EVENT = "agent_event"
    UCF_THRESHOLD = "ucf_threshold"
    DISCORD_MESSAGE = "discord_message"
    SYSTEM_EVENT = "system_event"


class ActionType(str, Enum):
    SEND_WEBHOOK = "send_webhook"
    STORE_DATA = "store_data"
    SEND_DISCORD = "send_discord"
    TRIGGER_CYCLE = "trigger_cycle"
    ALERT_AGENT = "alert_agent"
    UPDATE_UCF = "update_uc"
    LOG_EVENT = "log_event"
    TRANSFORM_DATA = "transform_data"
    CONDITIONAL_BRANCH = "conditional_branch"
    DELAY = "delay"
    PARALLEL_EXECUTE = "parallel_execute"
    SEND_EMAIL = "send_email"
    ROUTER = "router"
    STOP_AND_ERROR = "stop_and_error"
    HUMAN_INPUT = "human_input"
    LLM_ROUTER = "llm_router"
    EXECUTE_SPIRAL = "execute_spiral"
    FOREACH = "foreach"
    NODE_EXECUTION = "node_execution"
    DIGEST = "digest"
    KV_STORAGE = "kv_storage"


class ExecutionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    PAUSED = "paused"
    WAITING_INPUT = "waiting_input"


class ConditionOperator(str, Enum):
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX_MATCH = "regex_match"
    IN_LIST = "in_list"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class LogicalOperator(str, Enum):
    AND = "AND"
    OR = "OR"


class UCFMetric(str, Enum):
    HARMONY = "harmony"
    RESILIENCE = "resilience"
    THROUGHPUT = "throughput"
    FOCUS = "focus"
    FRICTION = "friction"
    VELOCITY = "velocity"


class PerformanceScore(int, Enum):
    """User's coordination level system (1-10)"""

    DORMANT = 1
    STIRRING = 2
    AWAKENING = 3
    AWARE = 4
    CONSCIOUS = 5
    EXPANDING = 6
    FLOWING = 7
    UNIFIED = 8
    TRANSCENDENT = 9
    OMNISCIENT = 10


# Condition Model
class Condition(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    field: str
    operator: ConditionOperator
    value: Any
    logical_operator: LogicalOperator | None = None
    nested_conditions: list["Condition"] | None = []


# Trigger Configuration Models
class WebhookTriggerConfig(BaseModel):
    type: str = "webhook"
    endpoint: str | None = None
    method: str | None = "POST"
    headers: dict[str, str] | None = {}
    signature_key: str | None = None
    allowed_ips: list[str] | None = []


class ScheduleTriggerConfig(BaseModel):
    type: str = "schedule"
    cron: str | None = None
    interval: int | None = None  # milliseconds
    timezone: str | None = "UTC"
    start_date: str | None = None
    end_date: str | None = None


class AgentEventTriggerConfig(BaseModel):
    type: str = "agent_event"
    agent_name: str
    event_types: list[str]
    filters: dict[str, Any] | None = {}


class UCFThresholdTriggerConfig(BaseModel):
    type: str = "ucf_threshold"
    metric: UCFMetric
    operator: str  # above, below, equals
    threshold: float
    check_interval: int | None = 60000  # milliseconds


class DiscordMessageTriggerConfig(BaseModel):
    type: str = "discord_message"
    channel_id: str | None = None
    user_id: str | None = None
    role_id: str | None = None
    message_pattern: str | None = None


class SystemEventTriggerConfig(BaseModel):
    type: str = "system_event"
    event_name: str
    source: str | None = None


class ManualTriggerConfig(BaseModel):
    type: str = "manual"
    requires_auth: bool | None = False
    allowed_users: list[str] | None = []


# Trigger Model
class Trigger(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: TriggerType
    name: str
    description: str | None = None
    enabled: bool = True
    config: (
        WebhookTriggerConfig
        | ScheduleTriggerConfig
        | AgentEventTriggerConfig
        | UCFThresholdTriggerConfig
        | DiscordMessageTriggerConfig
        | SystemEventTriggerConfig
        | ManualTriggerConfig
    )
    conditions: list[Condition] | None = []
    metadata: dict[str, Any] | None = {}


# Action Configuration Models
class SendWebhookConfig(BaseModel):
    type: str = "send_webhook"
    url: str
    method: str = "POST"
    headers: dict[str, str] | None = {}
    body: Any | None = None
    authentication: dict[str, str] | None = None


class StoreDataConfig(BaseModel):
    type: str = "store_data"
    storage_type: str  # local, database, cache, file
    key: str
    value: Any | None = None
    ttl: int | None = None  # seconds
    encrypt: bool | None = False


class SendDiscordConfig(BaseModel):
    type: str = "send_discord"
    webhook_url: str | None = None
    channel_id: str | None = None
    message_type: str = "text"  # text, embed, file
    content: str | None = None
    embed: dict[str, Any] | None = None


class TriggerCycleConfig(BaseModel):
    type: str = "trigger_cycle"
    cycle_name: str
    parameters: dict[str, Any] | None = {}
    wait_for_completion: bool | None = False


class AlertAgentConfig(BaseModel):
    type: str = "alert_agent"
    agent_name: str
    alert_level: str  # info, warning, error, critical
    message: str
    metadata: dict[str, Any] | None = {}


class UpdateUCFConfig(BaseModel):
    type: str = "update_uc"
    metric: UCFMetric
    operation: str  # set, increment, decrement, multiply
    value: float


class LogEventConfig(BaseModel):
    type: str = "log_event"
    level: str  # debug, info, warning, error
    message: str
    category: str | None = None
    metadata: dict[str, Any] | None = {}


class TransformDataConfig(BaseModel):
    type: str = "transform_data"
    transformations: list[dict[str, Any]]


class ConditionalBranchConfig(BaseModel):
    type: str = "conditional_branch"
    conditions: list[Condition]
    true_branch: list["Action"]
    false_branch: list["Action"] | None = []


class DelayConfig(BaseModel):
    type: str = "delay"
    duration: int  # milliseconds


class ParallelExecuteConfig(BaseModel):
    type: str = "parallel_execute"
    actions: list["Action"]
    wait_for_all: bool | None = True


class ForeachConfig(BaseModel):
    """Iterate over a list and execute one action per item.

    ``items`` is a context-variable reference (e.g. ``"$my_list"``) or an
    inline list of values.  Each iteration receives the item bound under
    ``item_var`` in a copy of the context variables.

    Set ``max_concurrency > 1`` to fan out in parallel (bounded by the
    semaphore).  All results are collected into ``output_var``.
    """

    type: str = "foreach"
    items: Any = Field(description="Context variable ref (e.g. '$results') or literal list to iterate over")
    item_var: str = Field(default="item", description="Name bound to the current element inside the action config")
    action: dict[str, Any] = Field(description="Action dict to execute for each item")
    max_concurrency: int = Field(default=1, ge=1, le=50, description="1 = sequential; >1 = parallel fan-out")
    output_var: str = Field(default="foreach_results", description="Context variable to store the results list")
    continue_on_item_error: bool = Field(
        default=False, description="If True, errors on individual items are recorded but do not abort the loop"
    )


class SendEmailConfig(BaseModel):
    type: str = "send_email"
    to: list[str]
    subject: str
    body: str
    is_html: bool | None = False
    cc: list[str] | None = []
    bcc: list[str] | None = []


class RouterConfig(BaseModel):
    """Multi-way routing based on previous step output.

    Examines the specified ``field`` in the previous action's output
    (or evaluates ``expression``) and selects a route key. Downstream
    actions connected with a matching condition are executed; others
    are skipped.
    """

    type: str = "router"
    field: str = Field("route", description="Key to inspect in previous step output")
    routes: dict[str, str] = Field(
        default_factory=dict,
        description="Map of route_key → description (e.g. {'success': 'Happy path', 'error': 'Handle errors'})",
    )
    default_route: str = Field("default", description="Fallback if value doesn't match any route")
    expression: str | None = Field(None, description="Python expression to compute route key (overrides field)")
    route_actions: dict[str, list["Action"]] | None = Field(
        default=None,
        description="Optional inline actions per route (for Spiral sequential engine)",
    )


class StopAndErrorConfig(BaseModel):
    """Intentionally halt execution with an error message.

    Useful for validation-driven failures: if data doesn't meet criteria,
    stop the spiral and trigger the error workflow (if configured).
    """

    type: str = "stop_and_error"
    message: str = Field("Execution stopped", description="Error message to report")
    error_code: str | None = Field(None, description="Application-specific error code")
    condition: str | None = Field(
        None,
        description="Expression — only stop if this evaluates truthy. If None, always stop.",
    )


class HumanInputConfig(BaseModel):
    """Pause execution and wait for human approval or input.

    Execution checkpoints to durable storage and resumes when a human
    submits their decision via the resume API.
    """

    type: str = "human_input"
    prompt: str = Field("Please review and approve", description="Prompt shown to the human reviewer")
    timeout_minutes: int = Field(1440, description="Auto-reject after this many minutes (default 24h)")
    actions: list[str] = Field(
        default_factory=lambda: ["approve", "reject"],
        description="Available actions the human can choose",
    )
    require_feedback: bool = Field(False, description="Require the human to provide text feedback")
    assignee: str | None = Field(None, description="User ID of the assigned reviewer")


class LLMRouterConfig(BaseModel):
    """LLM-as-judge routing: use AI to classify input into named scenarios.

    More flexible than rule-based routing for natural-language inputs.
    A cheap/fast model examines the input and selects the best-matching
    scenario, which determines which downstream branch executes.
    """

    type: str = "llm_router"
    scenarios: dict[str, str] = Field(
        ...,
        description="Map of scenario_name → description (e.g. {'sales': 'Sales inquiry', 'support': 'Support request'})",
    )
    instructions: str = Field(
        "Classify the input into one of the defined scenarios.",
        description="System instructions for the classification task",
    )
    model: str | None = Field(None, description="Override model (default: gpt-4o-mini)")
    default_scenario: str = Field("other", description="Fallback when no scenario matches")


class ExecuteSpiralConfig(BaseModel):
    """Invoke another spiral as a sub-workflow.

    Enables modular, reusable workflow composition. The child spiral
    runs to completion and its output is returned to the parent.
    """

    type: str = "execute_spiral"
    spiral_id: str = Field(..., description="ID of the spiral to execute")
    input_mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Map parent variables to child trigger data (e.g. {'parent_var': 'child_field'})",
    )
    wait_for_completion: bool = Field(True, description="Wait for child spiral to finish")
    max_depth: int = Field(3, description="Maximum nesting depth to prevent loops")


# Retry Configuration
class RetryConfig(BaseModel):
    max_attempts: int = 3
    backoff_strategy: str = "exponential"  # fixed, exponential, linear
    initial_delay: int = 1000  # milliseconds
    max_delay: int | None = 60000
    retry_on: list[str] | None = []  # error codes


# Quality Gate Configuration (per-step output validation)
class QualityGateConfig(BaseModel):
    """Validates a step's output before the next step runs.

    Supports three validation modes:
    - **schema**: Checks that required keys exist in the output dict.
    - **llm**: Sends the output to an LLM for quality assessment.
    - **expression**: Evaluates a Python-like expression against the output.

    When a gate fails and retries are configured, the failure reason is
    injected into the execution context so the retried action can see
    WHY it failed and produce a corrected output.
    """

    mode: str = Field(
        default="schema",
        description="Validation mode: schema, llm, or expression",
    )
    # Schema mode: ensure these keys exist in the output
    required_keys: list[str] | None = Field(
        default=None,
        description="Keys that must exist in the action output (schema mode)",
    )
    # Expression mode: e.g. "len(output) > 0" or "output.get('status') == 'ok'"
    expression: str | None = Field(
        default=None,
        description="Python expression evaluated against output (expression mode)",
    )
    # LLM mode: prompt the LLM to validate
    llm_prompt: str | None = Field(
        default=None,
        description="LLM prompt for quality assessment (llm mode)",
    )
    # Shared settings
    max_retries: int = Field(default=1, ge=0, le=5, description="Times to retry the action if validation fails")
    on_failure: str = Field(
        default="fail",
        description="Behavior on validation failure: fail, skip, or escalate",
    )
    feedback_on_retry: bool = Field(
        default=True,
        description="Inject failure reason into context before retry so the action can self-correct",
    )


class NodeExecutionConfig(BaseModel):
    """Execute any node in the NodeRegistry by node_type.

    All fields beyond ``node_type`` are passed as ``inputs`` to the node.
    """

    model_config = ConfigDict(extra="allow")

    type: str = "node_execution"
    node_type: str


class DigestConfig(BaseModel):
    """Batch-aggregate events over a time window or count, then release."""

    type: str = "digest"
    digest_key: str = Field(description="Unique key for this digest bucket")
    release_mode: str = Field(default="time", description="'time' or 'count'")
    window_seconds: int = Field(default=300, description="Release after N seconds (time mode)")
    count_threshold: int = Field(default=10, description="Release after N items (count mode)")
    aggregate_field: str | None = Field(default=None, description="Optional field to aggregate on")
    dedup_field: str | None = Field(default=None, description="Optional field for deduplication")


class KVStorageConfig(BaseModel):
    """Cross-run key-value storage operations."""

    type: str = "kv_storage"
    operation: str = Field(description="get, set, delete, exists, increment, list_keys")
    key: str = Field(description="Storage key")
    value: Any = Field(default=None, description="Value for set/increment operations")
    ttl: int | None = Field(default=None, description="TTL in seconds (set operation)")
    storage_type: str = Field(default="database", description="'database' (PostgreSQL) or 'cache' (Redis)")
    output_var: str = Field(default="_kv_result", description="Variable name to store result")


# Action Model
class Action(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: ActionType
    name: str
    description: str | None = None
    config: (
        SendWebhookConfig
        | StoreDataConfig
        | SendDiscordConfig
        | TriggerCycleConfig
        | AlertAgentConfig
        | UpdateUCFConfig
        | LogEventConfig
        | TransformDataConfig
        | ConditionalBranchConfig
        | DelayConfig
        | ParallelExecuteConfig
        | ForeachConfig
        | SendEmailConfig
        | RouterConfig
        | StopAndErrorConfig
        | HumanInputConfig
        | LLMRouterConfig
        | ExecuteSpiralConfig
        | NodeExecutionConfig
        | DigestConfig
        | KVStorageConfig
    )
    conditions: list[Condition] | None = []
    retry_config: RetryConfig | None = None
    quality_gate: QualityGateConfig | None = Field(
        default=None, description="Optional quality validation after this action"
    )
    timeout: int | None = 30000  # milliseconds
    continue_on_error: bool | None = False
    metadata: dict[str, Any] | None = {}


# Variable Definition
class Variable(BaseModel):
    name: str
    type: str  # string, number, boolean, object, array
    default_value: Any | None = None
    required: bool | None = False
    description: str | None = None
    source: str | None = "static"  # trigger, action, environment, static


# Rate Limiting
class RateLimitConfig(BaseModel):
    max_executions: int
    window_ms: int
    strategy: str = "sliding"  # sliding, fixed


# Scheduling
class SchedulingConfig(BaseModel):
    priority: str = "normal"  # low, normal, high, critical
    max_concurrent: int | None = None
    queue_strategy: str | None = "fifo"  # fifo, lifo, priority


# Security
class SecurityConfig(BaseModel):
    requires_auth: bool | None = False
    allowed_users: list[str] | None = []
    allowed_roles: list[str] | None = []
    webhook_signature: dict[str, Any] | None = None
    ip_whitelist: list[str] | None = []
    rate_limit: RateLimitConfig | None = None


# Main Spiral Model
class Spiral(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str | None = None
    version: str = "1.0.0"
    enabled: bool = True
    tags: list[str] | None = []
    trigger: Trigger
    actions: list[Action]
    variables: list[Variable] | None = []
    rate_limiting: RateLimitConfig | None = None
    scheduling: SchedulingConfig | None = None
    security: SecurityConfig | None = None
    metadata: dict[str, Any] | None = Field(
        default_factory=lambda: {
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
    )

    # Support for user's coordination level system
    performance_score: PerformanceScore | None = None

    # Support for 14-agent system
    assigned_agents: list[str] | None = []

    # Owner tracking — set at creation, None for anonymous/webhook spirals
    user_id: str | None = None

    # Error workflow: triggers a separate spiral on execution failure
    error_workflow_id: str | None = Field(None, description="ID of a spiral to trigger when this spiral fails")


# Execution Models
class ExecutionLog(BaseModel):
    timestamp: str
    level: str  # info, warning, error
    message: str
    action_id: str | None = None
    metadata: dict[str, Any] | None = {}


class ExecutionError(BaseModel):
    message: str
    stack: str | None = None
    action_id: str | None = None


class ExecutionContext(BaseModel):
    spiral_id: str
    execution_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trigger: dict[str, Any]
    variables: dict[str, Any] = {}
    logs: list[ExecutionLog] = []
    status: ExecutionStatus = ExecutionStatus.PENDING
    started_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    completed_at: str | None = None
    current_action: str | None = None
    error: ExecutionError | None = None
    metrics: dict[str, Any] | None = {}

    # UCF tracking
    ucf_impact: dict[str, float] | None = {}

    # Custom execution data: user-attachable key-value metadata, searchable/filterable
    custom_data: dict[str, str] = Field(
        default_factory=dict,
        description="Up to 10 user-defined key-value pairs for business context (max 50 char key, 255 char value)",
    )

    # Human-in-the-loop: tracks pending input requests
    pending_human_input: dict[str, Any] | None = Field(
        None,
        description="Active human input request (action_id, prompt, actions, assignee)",
    )


# API Request/Response Models
class WebhookPayload(BaseModel):
    spiral_id: str
    method: str
    headers: dict[str, str]
    body: Any
    query_params: dict[str, str]
    client_ip: str | None = None


class SpiralCreateRequest(BaseModel):
    name: str
    description: str | None = None
    trigger: dict[str, Any]
    actions: list[dict[str, Any]]
    enabled: bool = True
    tags: list[str] | None = []
    variables: list[dict[str, Any]] | None = []
    performance_score: int | None = None


class SpiralUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    enabled: bool | None = None
    trigger: dict[str, Any] | None = None
    actions: list[dict[str, Any]] | None = None
    tags: list[str] | None = None


class ExecutionRequest(BaseModel):
    trigger_data: dict[str, Any] | None = {}
    variables: dict[str, Any] | None = {}


class ExecutionResponse(BaseModel):
    execution_id: str
    spiral_id: str
    status: ExecutionStatus
    started_at: str
    completed_at: str | None = None
    logs: list[ExecutionLog] | None = []


class SpiralStatistics(BaseModel):
    total_spirals: int
    enabled_spirals: int
    total_executions: int
    successful_executions: int
    failed_executions: int
    average_execution_time_ms: float
    last_execution: str | None = None
    top_spirals: list[dict[str, Any]] = []


# Template Models for Zapier Replacement
class SpiralTemplate(BaseModel):
    id: str
    name: str
    description: str
    category: str
    icon: str | None = None
    spiral: Spiral
    zapier_equivalent: str | None = None
    steps_consolidated: int | None = None


# Forward reference update
Condition.model_rebuild()
Action.model_rebuild()
ConditionalBranchConfig.model_rebuild()
ParallelExecuteConfig.model_rebuild()
RouterConfig.model_rebuild()
