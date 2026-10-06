"""LLM-assisted diagnosis of dbt task failures, built on apache-airflow-providers-common-ai."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field

from cosmos.log import get_logger

try:
    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
    from pydantic_ai import CancellationToken
except ImportError:
    PydanticAIHook = None
    SQLToolset = None
    CancellationToken = None

if TYPE_CHECKING:
    from cosmos.config import AiConfig, ProfileConfig

logger = get_logger(__name__)

# dbt output and compiled SQL (all models, in WATCHER mode) can be huge; bound what we send to the LLM.
_MAX_RAW_OUTPUT_CHARS = 8000
_MAX_COMPILED_SQL_CHARS = 8000

# SQLToolset's `query` tool can read arbitrary rows and hand them to the LLM; introspection only needs metadata.
_SCHEMA_ONLY_TOOLS = frozenset({"list_tables", "get_schema"})

# After timing out, how long to wait for agent.run_sync to unwind once cancelled before giving up
# and leaking the thread. CancellationToken.cancel() is thread-safe and interrupts the run almost
# immediately in the common case (verified against a real LLM call); this is just a safety margin.
_CANCELLATION_GRACE_SECONDS = 5.0

_DEFAULT_DIAGNOSIS_INSTRUCTIONS = (
    "You are a dbt/warehouse failure diagnosis assistant embedded in Apache Airflow's "
    "astronomer-cosmos. Given the compiled SQL for a dbt model and the raw dbt error output, "
    "identify the underlying root cause and a concrete, actionable fix. Keep `root_cause` strictly "
    "to *why* the failure happened (e.g. a missing/renamed column, a bad join, a permissions issue) "
    "-- do not describe the fix there. Keep `suggested_fix` to the concrete next step. Each field "
    "should be 1-3 short sentences; do not restate the raw error text verbatim."
)

# Used when AiConfig.diagnosis_output_type overrides the default model: field-specific guidance
# above wouldn't make sense for arbitrary fields, so rely on the custom model's own Field
# descriptions (surfaced to the LLM via its generated schema) to steer the output instead.
_CUSTOM_OUTPUT_TYPE_INSTRUCTIONS = (
    "You are a dbt/warehouse failure diagnosis assistant embedded in Apache Airflow's "
    "astronomer-cosmos. Given the compiled SQL for a dbt model and the raw dbt error output, "
    "populate the requested structured output as accurately and concisely as possible."
)


class DbtFailureDiagnosis(BaseModel):
    root_cause: str = Field(description="Why the failure happened, only -- not how to fix it. 1-3 short sentences.")
    suggested_fix: str = Field(description="The concrete next step to resolve it. 1-3 short sentences.")
    confidence: Literal["low", "medium", "high"]


def _truncate_middle(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    half = max_chars // 2
    return f"{text[:half]}\n... [truncated {len(text) - 2 * half} characters] ...\n{text[-half:]}"


def _build_prompt(compiled_sql: str, raw_output: str) -> str:
    truncated_output = raw_output[-_MAX_RAW_OUTPUT_CHARS:]
    truncated_sql = _truncate_middle(compiled_sql, _MAX_COMPILED_SQL_CHARS)
    return (
        "A dbt task failed to run. Diagnose the root cause and suggest a fix.\n\n"
        f"Compiled SQL:\n{truncated_sql or '<not available>'}\n\n"
        f"dbt output (tail):\n{truncated_output}\n"
    )


def diagnose_dbt_failure(
    *,
    ai_config: AiConfig,
    profile_config: ProfileConfig | None,
    compiled_sql: str,
    raw_output: str,
) -> BaseModel | None:
    """Best-effort diagnosis of a dbt task failure using an LLM.

    Never raises: returns None and logs a warning on any failure of the diagnosis path itself
    (missing optional dependency, LLM/timeout/API error, etc). The original dbt failure must
    always surface to the user regardless of what happens here.
    """
    if PydanticAIHook is None:
        logger.warning(
            "AI diagnosis requires apache-airflow-providers-common-ai, which is not installed; "
            "skipping. Install it with: pip install apache-airflow-providers-common-ai"
        )
        return None

    # pydantic-ai prints an ASCII-art banner to stderr on first agent creation, which Airflow
    # logs at ERROR level, making a successful diagnosis look like a failure. Suppress it.
    os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")

    try:
        toolsets = []
        if ai_config.introspect_schema and profile_config and profile_config.profile_mapping:
            sql_toolset = SQLToolset(db_conn_id=profile_config.profile_mapping.conn_id)
            toolsets.append(sql_toolset.filtered(lambda _ctx, tool_def: tool_def.name in _SCHEMA_ONLY_TOOLS))

        output_type = ai_config.diagnosis_output_type or DbtFailureDiagnosis
        default_instructions = (
            _DEFAULT_DIAGNOSIS_INSTRUCTIONS if output_type is DbtFailureDiagnosis else _CUSTOM_OUTPUT_TYPE_INSTRUCTIONS
        )
        instructions = ai_config.diagnosis_instructions or default_instructions

        hook = PydanticAIHook(llm_conn_id=ai_config.llm_conn_id)
        agent = hook.create_agent(
            output_type=output_type,
            instructions=instructions,
            toolsets=toolsets,
        )

        prompt = _build_prompt(compiled_sql, raw_output)
        cancellation_token = CancellationToken()
        # Not using ThreadPoolExecutor as a context manager: its __exit__ calls
        # shutdown(wait=True), which would block until agent.run_sync actually returns even
        # after future.result() below times out -- silently turning timeout_seconds into a
        # no-op. shutdown(wait=False) here lets us return promptly.
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(agent.run_sync, prompt, cancellation_token=cancellation_token)
            try:
                result = future.result(timeout=ai_config.timeout_seconds)
            except FutureTimeoutError:
                # CancellationToken.cancel() is thread-safe and asks the run to stop cooperatively,
                # which interrupts an in-flight LLM call almost immediately in practice -- much
                # better than leaking a thread that keeps running until the call finishes on its
                # own. Still fall back to leaking it if cancellation doesn't unblock it in time
                # (e.g. it's stuck in a non-cooperative call outside of pydantic-ai's control).
                cancellation_token.cancel()
                try:
                    future.result(timeout=_CANCELLATION_GRACE_SECONDS)
                except Exception:
                    # Either cancellation succeeded (raises RunCancelled) or the grace period
                    # also elapsed (thread gets leaked below); both are expected here. Re-raise
                    # the original timeout so callers get one consistent, expected exception type.
                    pass
                raise
        finally:
            executor.shutdown(wait=False)

        output: BaseModel = result.output
        return output
    except FutureTimeoutError:
        logger.warning("AI diagnosis timed out after %s seconds; skipping.", ai_config.timeout_seconds)
        return None
    except Exception:
        logger.warning("AI diagnosis failed; falling back to the standard error message.", exc_info=True)
        return None
