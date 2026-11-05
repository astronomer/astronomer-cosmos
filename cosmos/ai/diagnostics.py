"""LLM-assisted diagnosis of dbt task failures, built on apache-airflow-providers-common-ai."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from airflow.exceptions import AirflowException, AirflowOptionalProviderFeatureException
from pydantic import BaseModel, Field

try:
    from airflow.sdk.bases.hook import BaseHook  # Airflow 3
except ImportError:
    from airflow.hooks.base import BaseHook  # Airflow 2

from cosmos.ai.formatting import format_diagnosis
from cosmos.log import get_logger

# The provider is optional. A dependency of its own that is missing raises ``AirflowOptionalProviderFeatureException``,
# which is an ``AirflowException`` and not an ``ImportError``.
try:
    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.providers.common.ai.utils.masking import mask_secrets  # needs apache-airflow-providers-common-ai>=1.0
    from pydantic_ai import CancellationToken, UsageLimits
    from pydantic_ai.exceptions import AgentRunError, FallbackExceptionGroup, RunCancelled, UserError
except (ImportError, AirflowOptionalProviderFeatureException):
    PydanticAIHook = None
    mask_secrets = None
    CancellationToken = None
    UsageLimits = None
    AgentRunError = None
    FallbackExceptionGroup = None
    RunCancelled = None
    UserError = None

if TYPE_CHECKING:
    from cosmos.ai.context import FailureContext
    from cosmos.config import AIConfig, ProfileConfig

logger = get_logger(__name__)

# SQLToolset's ``query`` tool can read arbitrary rows and hand them to the LLM; introspection only needs metadata.
_SCHEMA_ONLY_TOOLS = frozenset({"list_tables", "get_schema"})

_DEFAULT_DIAGNOSIS_INSTRUCTIONS = (
    "You are a dbt/warehouse failure diagnosis assistant embedded in Apache Airflow's "
    "astronomer-cosmos. Given the error and the compiled SQL of the dbt nodes that failed, "
    "identify the underlying root cause and a concrete, actionable fix. Keep `root_cause` strictly "
    "to *why* the failure happened (e.g. a missing/renamed column, a bad join, a permissions issue) "
    "-- do not describe the fix there. Keep `suggested_fix` to the concrete next step. Each field "
    "should be 1-3 short sentences; do not restate the raw error text verbatim."
)

# For a custom ``diagnosis_output_type``: the field names are the user's, so the model is steered by the ``Field``
# descriptions of that type instead.
_CUSTOM_OUTPUT_TYPE_INSTRUCTIONS = (
    "You are a dbt/warehouse failure diagnosis assistant embedded in Apache Airflow's "
    "astronomer-cosmos. Given the error and the compiled SQL of the dbt nodes that failed, "
    "populate the requested structured output as accurately and concisely as possible."
)


class DbtFailureDiagnosis(BaseModel):
    root_cause: str = Field(description="Why the failure happened, only -- not how to fix it. 1-3 short sentences.")
    suggested_fix: str = Field(description="The concrete next step to resolve it. 1-3 short sentences.")
    confidence: Literal["low", "medium", "high"]


class DiagnosisHandle:
    """Lets another thread cancel a diagnosis in flight, e.g. an operator's ``on_kill``."""

    def __init__(self) -> None:
        # No token without the provider: ``diagnose_dbt_failure`` then says so and does nothing.
        self.token: Any = CancellationToken() if CancellationToken else None

    def cancel(self) -> None:
        # ``on_kill`` runs in a signal handler on the thread that drives the agent, where a cancellation would only take
        # effect when the pending call returns. From another thread it interrupts the call.
        if self.token:
            threading.Thread(target=self.token.cancel, daemon=True).start()


def diagnose_dbt_failure(
    *,
    ai_config: AIConfig,
    profile_config: ProfileConfig | None,
    failure_context: FailureContext,
    handle: DiagnosisHandle | None = None,
) -> str | None:
    """
    Ask an LLM for the root cause of a dbt failure, and return its answer as text to append to the exception message.

    Returns None, and logs why, when the diagnosis can't be had for an expected reason: the provider is missing, the
    connection or the LLM call fails, the time limit is reached or the task is killed. Any other error is a bug, or
    an incompatible provider version: it propagates so it isn't hidden, and the caller keeps the dbt failure.
    """
    if PydanticAIHook is None:
        logger.warning(
            "AI diagnosis requires apache-airflow-providers-common-ai>=1.0.0, which is not installed or not usable; "
            "skipping. Install it with: pip install 'astronomer-cosmos[ai]'"
        )
        return None

    try:
        output_type = ai_config.diagnosis_output_type or DbtFailureDiagnosis
        default_instructions = (
            _DEFAULT_DIAGNOSIS_INSTRUCTIONS if output_type is DbtFailureDiagnosis else _CUSTOM_OUTPUT_TYPE_INSTRUCTIONS
        )
        toolsets = _build_toolsets(ai_config, profile_config, failure_context)
        # ``get_hook`` picks the hook class that matches the connection type (Azure, Bedrock, Vertex, ...).
        agent = PydanticAIHook.get_hook(ai_config.llm_conn_id).create_agent(
            output_type=output_type,
            instructions=ai_config.diagnosis_instructions or default_instructions,
            toolsets=toolsets,
        )

        # The tools only accept the exact names of the allow-list: a model that guesses another spelling of the
        # same table is refused, so it is told the names.
        prompt = redact(failure_context.to_prompt(failure_context.allowed_tables if toolsets else None), ai_config)
        result = _run_agent(agent, prompt, ai_config, handle or DiagnosisHandle())
        if result is None:
            return None

        usage = result.usage
        logger.info(
            "AI diagnosis sent %s characters, and used %s model request(s), %s input and %s output tokens.",
            len(prompt),
            usage.requests,
            usage.input_tokens,
            usage.output_tokens,
        )
        return redact(format_diagnosis(result.output), ai_config)
    except (AgentRunError, FallbackExceptionGroup, UserError, AirflowException, ImportError, OSError, ValueError):
        # ``ImportError``: pydantic-ai asks for the SDK of the model's provider (e.g. ``openai``) when it is missing.
        # ``ValueError``: the hook raises it for a connection that is not a Pydantic AI one.
        # ``FallbackExceptionGroup``: every connection of a ``fallback_conn_ids`` chain failed.
        logger.warning("AI diagnosis failed; falling back to the standard error message.", exc_info=True)
        return None


def redact(text: str, ai_config: AIConfig) -> str:
    """Mask the secrets Airflow knows about, then apply ``AIConfig.redactor``: what Airflow can't know is up to it."""
    # Without the provider nothing is sent, so there is nothing to mask: ``diagnose_dbt_failure`` returns before that.
    text = mask_secrets(text) if mask_secrets else text
    return ai_config.redactor(text) if ai_config.redactor else text


def _run_agent(agent: Any, prompt: str, ai_config: AIConfig, handle: DiagnosisHandle) -> Any:
    """Run the agent on this thread. Returns None if the time limit is reached or the task is killed."""
    timed_out = threading.Event()

    def cancel_on_timeout() -> None:
        timed_out.set()
        handle.token.cancel()

    timer = threading.Timer(ai_config.timeout_seconds, cancel_on_timeout)
    timer.daemon = True
    timer.start()
    try:
        return agent.run_sync(
            prompt, cancellation_token=handle.token, usage_limits=UsageLimits(request_limit=ai_config.max_requests)
        )
    except RunCancelled:
        if timed_out.is_set():
            logger.warning("AI diagnosis timed out after %s seconds; skipping.", ai_config.timeout_seconds)
        else:
            logger.info("AI diagnosis cancelled because the task was killed.")
        return None
    finally:
        timer.cancel()


def _build_toolsets(
    ai_config: AIConfig, profile_config: ProfileConfig | None, failure_context: FailureContext
) -> list[Any]:
    """
    The tools the agent can use to check the warehouse, for the tables of the failed node and its parents only.

    Introspection is a best-effort extra: when it can't be set up safely, the diagnosis runs without it.
    """
    if not ai_config.introspect_schema:
        return []

    profile_mapping = profile_config.profile_mapping if profile_config else None
    if profile_mapping is None:
        logger.warning("AIConfig.introspect_schema needs ProfileConfig.profile_mapping; skipping schema introspection.")
        return []

    if not failure_context.allowed_tables:
        logger.warning("The failure has no related tables to inspect; skipping schema introspection.")
        return []

    try:
        # Imported here: the SQL toolset needs the ``sql`` extra of the provider.
        from airflow.providers.common.ai.toolsets.sql import SQLToolset
        from airflow.providers.common.sql.hooks.sql import DbApiHook
    except (ImportError, AirflowOptionalProviderFeatureException):
        logger.warning(
            "Schema introspection needs apache-airflow-providers-common-ai[sql]; skipping schema introspection.",
            exc_info=True,
        )
        return []

    if not _is_dbapi_connection(profile_mapping.conn_id, DbApiHook):
        return []

    allowed_tables = failure_context.allowed_tables
    # The toolset only reads the connection when a tool runs, so nothing here depends on the warehouse.
    toolset = SQLToolset(db_conn_id=profile_mapping.conn_id, allowed_tables=allowed_tables)
    metadata_tools = toolset.filtered(lambda _ctx, tool_def: tool_def.name in _SCHEMA_ONLY_TOOLS)
    return [_redacting(metadata_tools, ai_config, allowed_tables)]


def _redacting(toolset: Any, ai_config: AIConfig, allowed_tables: list[str]) -> Any:
    """Wrap ``toolset`` to log what the agent asks of its tools, and to redact what they return like the prompt."""
    # The provider, so pydantic-ai, is there by now.
    from pydantic_ai.exceptions import ModelRetry
    from pydantic_ai.toolsets import WrapperToolset

    allowed = {table.casefold() for table in allowed_tables}

    @dataclass
    class RedactingToolset(WrapperToolset):  # type: ignore[misc]
        async def call_tool(self, name: str, tool_args: dict[str, Any], ctx: Any, tool: Any) -> Any:
            # Only a table of the failed node is logged. The rest of what the model writes in a tool call could be
            # anything, such as what a prompt injection asked for, so it is not.
            table = str(tool_args.get("table_name", ""))
            if not table or table.casefold() in allowed:
                logger.info("AI diagnosis tool call: %s %s", name, table)
            else:
                logger.warning("AI diagnosis asked for a table that is not one of the tables of the failed node.")
            try:
                result = await self.wrapped.call_tool(name, tool_args, ctx, tool)
            except ModelRetry as retry:
                # The provider turns the error of a tool into a retry for the model, such as a database error with a
                # host and a user. It is not a result, so it is redacted here.
                raise ModelRetry(redact(retry.message, ai_config)) from retry
            return redact(result if isinstance(result, str) else json.dumps(result, default=str), ai_config)

    return RedactingToolset(wrapped=toolset)


def _is_dbapi_connection(conn_id: str, dbapi_hook_class: type) -> bool:
    """SQLToolset only works with SQL hooks. Several profile mappings (BigQuery, Databricks, ...) use other hooks."""
    try:
        hook = BaseHook.get_connection(conn_id).get_hook()
    except Exception as exc:
        # Deliberately broad, as it is Airflow loading code of a provider: a missing connection and a missing Airflow
        # provider are an ``AirflowException`` ("Unknown hook type"), but a ``generic`` connection is an
        # ``AttributeError``. Introspection is optional, so none of them should lose the diagnosis.
        logger.warning(
            "Could not load a hook for connection %r (%s); schema introspection needs the Airflow provider of your "
            "warehouse, such as apache-airflow-providers-postgres. Skipping schema introspection.",
            conn_id,
            exc,
        )
        return False
    if not isinstance(hook, dbapi_hook_class):
        logger.warning(
            "Connection %r uses %s, which is not a DbApiHook; schema introspection only supports SQL connections, "
            "so it is skipped.",
            conn_id,
            type(hook).__name__,
        )
        return False
    return True
