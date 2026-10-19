"""LLM-assisted diagnosis of dbt task failures, built on apache-airflow-providers-common-ai."""

from __future__ import annotations

import importlib
import os
import threading
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field

try:
    from airflow.sdk.bases.hook import BaseHook  # Airflow 3
except ImportError:
    from airflow.hooks.base import BaseHook  # Airflow 2

from cosmos.ai.formatting import format_diagnosis
from cosmos.log import get_logger


def _airflow_exceptions(name: str) -> tuple[type[Exception], ...]:
    """The exception ``name`` of Airflow core and of the Task SDK. Airflow 3 has both, and they are different types."""
    classes = []
    for module_name in ("airflow.exceptions", "airflow.sdk.exceptions"):
        try:
            classes.append(getattr(importlib.import_module(module_name), name))
        except (ImportError, AttributeError):
            continue
    return tuple(classes)


# The provider or one of its own dependencies is not usable.
_PROVIDER_IMPORT_ERRORS: tuple[type[BaseException], ...] = (
    ImportError,
    *_airflow_exceptions("AirflowOptionalProviderFeatureException"),
)
# How the diagnosis is expected to fail, as opposed to a bug: pydantic-ai's own errors (added where they are imported)
# plus Airflow's (e.g. connection not found), the network and the SDK of the model's provider (``ImportError``, which
# pydantic-ai raises when e.g. ``openai`` is missing), and the ``ValueError`` of the hook for a wrong connection type
# (also the base of pydantic's validation and serialization errors).
_EXPECTED_ERRORS: tuple[type[BaseException], ...] = (
    *_airflow_exceptions("AirflowException"),
    ImportError,
    OSError,
    ValueError,
)

# The provider is optional. Importing it fails with ``ImportError``, or with ``AirflowOptionalProviderFeatureException``
# (an ``AirflowException``, not an ``ImportError``) when one of its own dependencies is missing.
try:
    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.providers.common.ai.utils.masking import mask_secrets  # needs apache-airflow-providers-common-ai>=1.0
    from pydantic_ai import CancellationToken, UsageLimits
    from pydantic_ai.exceptions import AgentRunError, RunCancelled, UserError
except _PROVIDER_IMPORT_ERRORS:
    PydanticAIHook = None
    mask_secrets = None
    CancellationToken = None
    UsageLimits = None
    AgentRunError = None
    RunCancelled = None
    UserError = None

if TYPE_CHECKING:
    from cosmos.ai.context import FailureContext
    from cosmos.config import AiConfig, ProfileConfig

logger = get_logger(__name__)

# Model requests a single diagnosis can make: the answer plus a few schema checks.
_REQUEST_LIMIT = 8

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

# Used when AiConfig.diagnosis_output_type overrides the default model: field-specific guidance
# above wouldn't make sense for arbitrary fields, so rely on the custom model's own Field
# descriptions (surfaced to the LLM via its generated schema) to steer the output instead.
_CUSTOM_OUTPUT_TYPE_INSTRUCTIONS = (
    "You are a dbt/warehouse failure diagnosis assistant embedded in Apache Airflow's "
    "astronomer-cosmos. Given the error and the compiled SQL of the dbt nodes that failed, "
    "populate the requested structured output as accurately and concisely as possible."
)


class _DiagnosisTimeoutError(Exception):
    """The diagnosis did not finish within ``AiConfig.timeout_seconds``."""


class _RedactionError(Exception):
    """``AiConfig.redactor`` raised, so the text can't be proven safe to send."""


class DbtFailureDiagnosis(BaseModel):
    root_cause: str = Field(description="Why the failure happened, only -- not how to fix it. 1-3 short sentences.")
    suggested_fix: str = Field(description="The concrete next step to resolve it. 1-3 short sentences.")
    confidence: Literal["low", "medium", "high"]


class DiagnosisHandle:
    """
    Lets another thread cancel a diagnosis in flight, e.g. an operator's ``on_kill``.

    The cancellation is requested from a new thread: ``on_kill`` runs in a signal handler on the thread that drives
    the agent, and a cancellation issued from that same thread would only take effect when the pending call returns.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._token: Any = None
        self._cancelled = False

    def bind(self, token: Any) -> None:
        with self._lock:
            self._token = token
            already_cancelled = self._cancelled
        if already_cancelled:
            token.cancel()

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            token = self._token
        if token is not None:
            threading.Thread(target=token.cancel, name="cosmos-ai-diagnosis-cancel", daemon=True).start()


def diagnose_dbt_failure(
    *,
    ai_config: AiConfig,
    profile_config: ProfileConfig | None,
    failure_context: FailureContext,
    handle: DiagnosisHandle | None = None,
) -> str | None:
    """Best-effort diagnosis of a dbt task failure using an LLM.

    Returns the diagnosis rendered as text, ready to append to the task's exception message.

    Returns None and logs a warning when the diagnosis can't be had for an expected reason: the optional dependency is
    missing, the connection or the LLM call fails (``pydantic-ai`` and Airflow errors, ``OSError``, and the
    ``ValueError`` the hook raises for a wrong connection), the redactor fails, or it is cancelled or times out.
    Anything else is a bug or an incompatible provider version, and propagates so it isn't hidden: the caller must
    make sure that the original dbt failure still surfaces.
    """
    if PydanticAIHook is None:
        logger.warning(
            "AI diagnosis requires apache-airflow-providers-common-ai>=1.0.0, which is not installed or not usable; "
            "skipping. Install it with: pip install 'astronomer-cosmos[ai]'"
        )
        return None

    # pydantic-ai prints an ASCII-art banner to stderr on first agent creation, which Airflow
    # logs at ERROR level, making a successful diagnosis look like a failure. Suppress it.
    os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")

    try:
        output_type = ai_config.diagnosis_output_type or DbtFailureDiagnosis
        default_instructions = (
            _DEFAULT_DIAGNOSIS_INSTRUCTIONS if output_type is DbtFailureDiagnosis else _CUSTOM_OUTPUT_TYPE_INSTRUCTIONS
        )

        # ``get_hook`` picks the hook class that matches the connection type (Azure, Bedrock, Vertex, ...).
        hook = PydanticAIHook.get_hook(ai_config.llm_conn_id)
        toolsets = _build_toolsets(ai_config, profile_config, failure_context)
        agent = hook.create_agent(
            output_type=output_type,
            instructions=ai_config.diagnosis_instructions or default_instructions,
            toolsets=toolsets,
        )

        # The tools only accept the exact names of the allow-list, and a model that guesses another spelling of the same
        # table (unqualified, or with the database) is refused. So tell it the names.
        inspectable_tables = failure_context.allowed_tables if toolsets else None
        prompt = redact(failure_context.to_prompt(inspectable_tables), ai_config)
        result = _run_agent(agent, prompt, ai_config.timeout_seconds, handle)

        usage = result.usage
        logger.info(
            "AI diagnosis used %s model request(s), %s input and %s output tokens.",
            usage.requests,
            usage.input_tokens,
            usage.output_tokens,
        )
        _log_tool_calls(result, ai_config)
        return redact(format_diagnosis(result.output), ai_config)
    except _DiagnosisTimeoutError:
        logger.warning("AI diagnosis timed out after %s seconds; skipping.", ai_config.timeout_seconds)
        return None
    except RunCancelled:
        logger.info("AI diagnosis cancelled because the task was killed.")
        return None
    except _RedactionError:
        logger.warning("AiConfig.redactor failed, so nothing was sent to the LLM; skipping.", exc_info=True)
        return None
    except (AgentRunError, UserError, *_EXPECTED_ERRORS):
        logger.warning("AI diagnosis failed; falling back to the standard error message.", exc_info=True)
        return None


def _log_tool_calls(result: Any, ai_config: AiConfig) -> None:
    """Log what the agent asked of its tools, and what was refused: otherwise a diagnosis made blind looks the same."""
    try:
        messages = result.all_messages()
    except AttributeError:
        return
    for message in messages:
        for part in getattr(message, "parts", ()):
            kind = getattr(part, "part_kind", "")
            if kind == "tool-call" and part.tool_name in _SCHEMA_ONLY_TOOLS:
                logger.info("AI diagnosis tool call: %s", redact(f"{part.tool_name}({part.args})", ai_config))
            elif kind in ("tool-return", "retry-prompt") and part.tool_name in _SCHEMA_ONLY_TOOLS:
                content = redact(str(part.content), ai_config)
                if "not in the allowed tables" in content:
                    logger.warning("AI diagnosis was refused a table: %s", content)
                else:
                    logger.debug("AI diagnosis tool result: %s", content)


def redact(text: str, ai_config: AiConfig) -> str:
    """Mask the secrets Airflow knows about and then apply ``AiConfig.redactor``, so none go to the LLM or the logs."""
    text = mask_secrets(text)
    if ai_config.redactor is not None:
        try:
            text = ai_config.redactor(text)
        except Exception as exc:  # user code: any failure means the text can't be trusted
            raise _RedactionError from exc
    return text


def _run_agent(agent: Any, prompt: str, timeout_seconds: float, handle: DiagnosisHandle | None) -> Any:
    """
    Run the agent on this thread, cancelling it cooperatively when ``timeout_seconds`` pass.

    :raises _DiagnosisTimeoutError: when the deadline is reached before the agent finishes.
    """
    token = CancellationToken()
    if handle is not None:
        handle.bind(token)

    timed_out = threading.Event()

    def _cancel_on_timeout() -> None:
        timed_out.set()
        token.cancel()

    timer = threading.Timer(timeout_seconds, _cancel_on_timeout)
    timer.daemon = True
    timer.start()
    try:
        return agent.run_sync(prompt, cancellation_token=token, usage_limits=UsageLimits(request_limit=_REQUEST_LIMIT))
    except RunCancelled as exc:
        if timed_out.is_set():
            raise _DiagnosisTimeoutError from exc
        raise
    finally:
        timer.cancel()


def _build_toolsets(
    ai_config: AiConfig, profile_config: ProfileConfig | None, failure_context: FailureContext
) -> list[Any]:
    """
    The tools the agent can use to confirm a hypothesis in the warehouse.

    Introspection is a best-effort extra: whenever it can't be set up safely, the diagnosis still runs without it. That
    is why the clauses below are broad: hooks of any warehouse run their own code here, and none of their failures
    should cost the user the diagnosis itself.
    """
    if not ai_config.introspect_schema:
        return []

    profile_mapping = profile_config.profile_mapping if profile_config else None
    if profile_mapping is None:
        logger.warning("AiConfig.introspect_schema needs ProfileConfig.profile_mapping; skipping schema introspection.")
        return []

    allowed_tables = failure_context.allowed_tables
    if not allowed_tables:
        logger.warning("The failure has no related tables to inspect; skipping schema introspection.")
        return []

    try:
        # Imported only here: importing the SQL toolset needs the optional ``sql`` extra of the provider, which fails
        # with ``AirflowOptionalProviderFeatureException`` if it is missing.
        from airflow.providers.common.ai.toolsets.sql import SQLToolset
        from airflow.providers.common.sql.hooks.sql import DbApiHook
    except _PROVIDER_IMPORT_ERRORS:
        logger.warning(
            "Schema introspection needs apache-airflow-providers-common-ai[sql]; skipping schema introspection.",
            exc_info=True,
        )
        return []

    conn_id = profile_mapping.conn_id
    if not _is_dbapi_connection(conn_id, DbApiHook):
        return []

    try:
        # Only the tables of the failed node and its parents: enough to check a column, no more of the warehouse.
        sql_toolset = SQLToolset(db_conn_id=conn_id, allowed_tables=allowed_tables)
        return [sql_toolset.filtered(lambda _ctx, tool_def: tool_def.name in _SCHEMA_ONLY_TOOLS)]
    except Exception:
        logger.warning("Could not set up schema introspection; skipping it.", exc_info=True)
        return []


def _is_dbapi_connection(conn_id: str, dbapi_hook_class: type) -> bool:
    """SQLToolset only works with SQL hooks. Several profile mappings (BigQuery, Databricks, ...) use other hooks."""
    try:
        hook = BaseHook.get_connection(conn_id).get_hook()
    except Exception as exc:
        # E.g. ``Unknown hook type "postgres"`` when the Airflow provider of the warehouse is not installed.
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
