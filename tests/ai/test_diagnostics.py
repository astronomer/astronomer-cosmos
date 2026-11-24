from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException
from pydantic import BaseModel

import cosmos.ai.diagnostics as diagnostics
from cosmos.ai.context import FailedNode, FailureContext
from cosmos.ai.diagnostics import DbtFailureDiagnosis, DiagnosisHandle, diagnose_dbt_failure
from cosmos.config import AIConfig

try:
    from pydantic_ai import Agent
    from pydantic_ai.exceptions import UsageLimitExceeded
    from pydantic_ai.messages import ModelResponse, ToolCallPart
    from pydantic_ai.models.function import AgentInfo, FunctionModel

    PROVIDER_AVAILABLE = diagnostics.PydanticAIHook is not None
except Exception:  # pragma: no cover - depends on the optional provider
    PROVIDER_AVAILABLE = False

requires_provider = pytest.mark.skipif(
    not PROVIDER_AVAILABLE, reason="apache-airflow-providers-common-ai>=1.0.0 is not importable"
)

FAILURE_CONTEXT = FailureContext(
    failed_nodes=(
        FailedNode(
            unique_id="model.p.orders",
            status="error",
            message="Database Error: column foo does not exist, password=SECRET-VALUE",
            compiled_code="select foo from raw.orders",
            relations=("analytics.orders", "raw.orders"),
        ),
    )
)
DIAGNOSIS = DbtFailureDiagnosis(root_cause="Column foo is missing.", suggested_fix="Rename it.", confidence="high")
AI_CONFIG = AIConfig(llm_conn_id="my_llm_conn", diagnose_on_failure=True)


# --- A fake provider: lets the diagnosis logic run, and be covered, without the optional dependencies ---------------


class FakeRunCancelled(Exception):
    pass


class FakeFallbackExceptionGroup(Exception):
    pass


class FakeAgentRunError(Exception):
    pass


class FakeUserError(Exception):
    pass


class FakeToken:
    def __init__(self):
        self.cancelled = threading.Event()

    def cancel(self):
        self.cancelled.set()


class FakeAgent:
    def __init__(self, behaviour):
        self.behaviour = behaviour
        self.runs = []

    def run_sync(self, prompt, cancellation_token, usage_limits):
        self.runs.append(SimpleNamespace(prompt=prompt, token=cancellation_token, usage_limits=usage_limits))
        return self.behaviour(prompt, cancellation_token)


@pytest.fixture
def fake_provider(monkeypatch):
    """Swap the provider API for fakes. ``.behaviour`` decides what a run does; ``.agent`` is the agent it ran on."""
    state = SimpleNamespace(
        behaviour=lambda prompt, token: SimpleNamespace(
            output=DIAGNOSIS,
            usage=SimpleNamespace(requests=1, input_tokens=10, output_tokens=5),
        ),
        agent=None,
        agent_kwargs=None,
        conn_ids=[],
    )

    class FakeHook:
        @classmethod
        def get_hook(cls, conn_id, hook_params=None):
            state.conn_ids.append(conn_id)
            return cls()

        def create_agent(self, **kwargs):
            state.agent_kwargs = kwargs
            state.agent = FakeAgent(lambda prompt, token: state.behaviour(prompt, token))
            return state.agent

    monkeypatch.setattr(diagnostics, "PydanticAIHook", FakeHook)
    monkeypatch.setattr(diagnostics, "CancellationToken", FakeToken)
    monkeypatch.setattr(diagnostics, "UsageLimits", lambda request_limit: SimpleNamespace(request_limit=request_limit))
    monkeypatch.setattr(diagnostics, "RunCancelled", FakeRunCancelled)
    monkeypatch.setattr(diagnostics, "AgentRunError", FakeAgentRunError)
    monkeypatch.setattr(diagnostics, "FallbackExceptionGroup", FakeFallbackExceptionGroup)
    monkeypatch.setattr(diagnostics, "UserError", FakeUserError)
    monkeypatch.setattr(diagnostics, "mask_secrets", lambda text: text.replace("SECRET-VALUE", "***"))
    return state


def _block_until_cancelled(prompt, token):
    token.cancelled.wait(timeout=10)
    raise FakeRunCancelled


def test_diagnose_dbt_failure_returns_the_rendered_diagnosis(fake_provider):
    diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is not None
    assert "AI diagnosis (confidence: high)" in diagnosis
    assert "Root cause:\n  Column foo is missing." in diagnosis
    assert "Suggested fix:\n  Rename it." in diagnosis


def test_diagnose_dbt_failure_resolves_the_hook_class_from_the_connection_type(fake_provider):
    diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    # ``PydanticAIHook.get_hook`` returns the Azure, Bedrock or Vertex subclass that matches the connection.
    assert fake_provider.conn_ids == ["my_llm_conn"]


def test_diagnose_dbt_failure_uses_the_default_model_and_instructions(fake_provider):
    diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert fake_provider.agent_kwargs["output_type"] is DbtFailureDiagnosis
    assert "`root_cause` strictly" in fake_provider.agent_kwargs["instructions"]
    assert fake_provider.agent_kwargs["toolsets"] == []


def test_diagnose_dbt_failure_uses_a_custom_output_type_and_generic_instructions(fake_provider):
    class Custom(BaseModel):
        summary: str

    fake_provider.behaviour = lambda prompt, token: SimpleNamespace(
        output=Custom(summary="It broke."),
        usage=SimpleNamespace(requests=1, input_tokens=1, output_tokens=1),
    )
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, diagnosis_output_type=Custom)

    diagnosis = diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert fake_provider.agent_kwargs["output_type"] is Custom
    assert "populate the requested structured output" in fake_provider.agent_kwargs["instructions"]
    assert "Summary:\n  It broke." in diagnosis


def test_diagnose_dbt_failure_custom_instructions_replace_the_default(fake_provider):
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, diagnosis_instructions="Be terse.")

    diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert fake_provider.agent_kwargs["instructions"] == "Be terse."


def test_diagnose_dbt_failure_bounds_the_number_of_model_requests_as_the_user_says(fake_provider):
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, max_requests=3)

    diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert AI_CONFIG.max_requests == 8  # the default
    assert fake_provider.agent.runs[0].usage_limits.request_limit == 3


def test_diagnose_dbt_failure_masks_secrets_before_calling_the_model(fake_provider):
    diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    prompt = fake_provider.agent.runs[0].prompt
    assert "SECRET-VALUE" not in prompt
    assert "password=***" in prompt
    assert "select foo from raw.orders" in prompt


def test_diagnose_dbt_failure_applies_the_user_redactor_after_the_masker(fake_provider):
    config = AIConfig(
        llm_conn_id="c", diagnose_on_failure=True, redactor=lambda text: text.replace("raw.orders", "<table>")
    )

    diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    prompt = fake_provider.agent.runs[0].prompt
    assert "raw.orders" not in prompt and "<table>" in prompt
    assert "SECRET-VALUE" not in prompt


def test_diagnose_dbt_failure_redacts_the_diagnosis_too(fake_provider):
    fake_provider.behaviour = lambda prompt, token: SimpleNamespace(
        output=DbtFailureDiagnosis(root_cause="Leaked SECRET-VALUE.", suggested_fix="Fix.", confidence="low"),
        usage=SimpleNamespace(requests=1, input_tokens=1, output_tokens=1),
    )

    diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert "SECRET-VALUE" not in diagnosis and "Leaked ***." in diagnosis


def test_diagnose_dbt_failure_sends_nothing_if_the_redactor_fails(fake_provider):
    """A bug in the user's redactor is not an expected failure: it surfaces, and nothing has been sent."""

    def broken_redactor(text):
        raise RuntimeError("redactor bug")

    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, redactor=broken_redactor)

    with pytest.raises(RuntimeError, match="redactor bug"):
        diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert fake_provider.agent is None or fake_provider.agent.runs == []


def test_diagnose_dbt_failure_without_the_provider_skips_with_a_warning(monkeypatch, caplog):
    monkeypatch.setattr(diagnostics, "PydanticAIHook", None)

    with caplog.at_level(logging.WARNING):
        diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    assert "apache-airflow-providers-common-ai>=1.0.0" in caplog.text
    assert "astronomer-cosmos[ai]" in caplog.text


def test_diagnose_dbt_failure_returns_none_when_the_model_call_fails(fake_provider, caplog):
    def fail(prompt, token):
        raise ConnectionError("LLM unreachable")

    fake_provider.behaviour = fail

    with caplog.at_level(logging.WARNING):
        diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    assert "AI diagnosis failed; falling back" in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        FakeAgentRunError("rate limited"),
        FakeFallbackExceptionGroup("all connections failed"),
        FakeUserError("bad model name"),
        AirflowException("connection not found"),
        ImportError("Please install `openai`"),
        TimeoutError("socket timeout"),
        ConnectionError("LLM unreachable"),
        ValueError("not a Pydantic AI connection"),
    ],
    ids=lambda error: type(error).__name__,
)
def test_diagnose_dbt_failure_logs_the_failures_it_expects(fake_provider, caplog, error):
    def fail(prompt, token):
        raise error

    fake_provider.behaviour = fail

    with caplog.at_level(logging.WARNING):
        diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    assert "AI diagnosis failed; falling back" in caplog.text


@pytest.mark.parametrize("error", [TypeError("create_agent() got an unexpected keyword"), AttributeError("gone")])
def test_diagnose_dbt_failure_does_not_hide_bugs(fake_provider, error):
    """A programming error, or an incompatible provider, must surface instead of passing as a normal LLM failure."""

    def fail(prompt, token):
        raise error

    fake_provider.behaviour = fail

    with pytest.raises(type(error)):
        diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)


def test_diagnose_dbt_failure_does_not_report_a_timeout_error_of_the_call_as_the_time_limit(fake_provider, caplog):
    """On Python 3.11+, a socket timeout is a ``TimeoutError`` too, and the time limit was not reached."""

    def socket_timeout(prompt, token):
        raise TimeoutError("read timed out")

    fake_provider.behaviour = socket_timeout

    with caplog.at_level(logging.WARNING):
        diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    assert "timed out after" not in caplog.text
    assert "AI diagnosis failed; falling back" in caplog.text


def test_diagnose_dbt_failure_returns_none_when_the_diagnosis_can_not_be_rendered(fake_provider, caplog):
    class Unrenderable(BaseModel):
        value: str = "x"

        def model_dump(self, *args, **kwargs):
            raise ValueError("cannot serialize")

    fake_provider.behaviour = lambda prompt, token: SimpleNamespace(
        output=Unrenderable(),
        usage=SimpleNamespace(requests=1, input_tokens=1, output_tokens=1),
    )

    with caplog.at_level(logging.WARNING):
        diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    assert "AI diagnosis failed; falling back" in caplog.text


def test_diagnose_dbt_failure_returns_none_when_the_connection_is_wrong(fake_provider, monkeypatch, caplog):
    def get_hook(cls, conn_id, hook_params=None):
        raise ValueError("Connection 'x' has type 'postgres', but a Pydantic AI hook needs one of [...]")

    monkeypatch.setattr(diagnostics.PydanticAIHook, "get_hook", classmethod(get_hook))

    with caplog.at_level(logging.WARNING):
        diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    assert "needs one of" in caplog.text


def test_diagnose_dbt_failure_times_out_without_leaving_threads_behind(fake_provider, caplog):
    fake_provider.behaviour = _block_until_cancelled
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, timeout_seconds=0.05)
    threads_before = threading.active_count()

    started = time.monotonic()
    with caplog.at_level(logging.WARNING):
        diagnosis = diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    assert time.monotonic() - started < 5
    assert "timed out after 0.05 seconds" in caplog.text
    # The run happens on the task's own thread and the timer is cancelled: nothing outlives the call.
    assert threading.active_count() == threads_before


def test_diagnose_dbt_failure_stops_when_the_task_is_killed(fake_provider, caplog):
    fake_provider.behaviour = _block_until_cancelled
    handle = DiagnosisHandle()
    threading.Timer(0.05, handle.cancel).start()

    with caplog.at_level(logging.INFO):
        diagnosis = diagnose_dbt_failure(
            ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT, handle=handle
        )

    assert diagnosis is None
    assert "cancelled because the task was killed" in caplog.text


def test_diagnose_dbt_failure_stops_if_the_task_was_killed_before_the_run(fake_provider, caplog):
    fake_provider.behaviour = _block_until_cancelled
    handle = DiagnosisHandle()
    handle.cancel()

    with caplog.at_level(logging.INFO):
        diagnosis = diagnose_dbt_failure(
            ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT, handle=handle
        )

    assert diagnosis is None
    assert "cancelled because the task was killed" in caplog.text


def test_diagnosis_handle_cancels_its_token_from_another_thread(fake_provider):
    handle = DiagnosisHandle()
    cancelled_by = []
    handle.token.cancel = lambda: cancelled_by.append(threading.current_thread())

    handle.cancel()

    deadline = time.monotonic() + 5
    while not cancelled_by and time.monotonic() < deadline:
        time.sleep(0.01)
    assert cancelled_by and cancelled_by[0] is not threading.current_thread()


def test_diagnosis_handle_without_the_provider_has_nothing_to_cancel(monkeypatch):
    monkeypatch.setattr(diagnostics, "CancellationToken", None)

    handle = DiagnosisHandle()
    handle.cancel()

    assert handle.token is None


# --- Schema introspection ------------------------------------------------------------------------------------------


def _profile_config(conn_id="my_db_conn"):
    return SimpleNamespace(profile_mapping=SimpleNamespace(conn_id=conn_id))


def test_build_toolsets_is_empty_unless_introspection_is_enabled():
    assert diagnostics._build_toolsets(AI_CONFIG, _profile_config(), FAILURE_CONTEXT) == []


@pytest.mark.parametrize(
    "profile_config, context, message",
    [
        (None, FAILURE_CONTEXT, "needs ProfileConfig.profile_mapping"),
        (SimpleNamespace(profile_mapping=None), FAILURE_CONTEXT, "needs ProfileConfig.profile_mapping"),
        (_profile_config(), FailureContext(), "no related tables"),
    ],
)
def test_build_toolsets_skips_introspection_without_what_it_needs(profile_config, context, message, caplog):
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, introspect_schema=True)

    with caplog.at_level(logging.WARNING):
        assert diagnostics._build_toolsets(config, profile_config, context) == []

    assert message in caplog.text


def test_build_toolsets_skips_introspection_when_the_sql_extra_is_missing(monkeypatch, caplog):
    # Importing the toolset without the ``sql`` extra of the provider fails with ``AirflowOptionalProviderFeatureException``.
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    class MissingSqlToolset:
        def __getattr__(self, name):
            raise AirflowOptionalProviderFeatureException("No module named 'sqlglot'")

    monkeypatch.setitem(__import__("sys").modules, "airflow.providers.common.ai.toolsets.sql", MissingSqlToolset())
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, introspect_schema=True)

    with caplog.at_level(logging.WARNING):
        assert diagnostics._build_toolsets(config, _profile_config(), FAILURE_CONTEXT) == []

    assert "needs apache-airflow-providers-common-ai[sql]" in caplog.text


class FakeDbApiHook:
    pass


def test_is_dbapi_connection_accepts_sql_hooks(monkeypatch):
    monkeypatch.setattr(
        diagnostics.BaseHook, "get_connection", lambda conn_id: SimpleNamespace(get_hook=lambda: FakeDbApiHook())
    )

    assert diagnostics._is_dbapi_connection("my_db_conn", FakeDbApiHook) is True


@pytest.mark.parametrize("conn_type", ["google_cloud_platform", "databricks", "aws", "spark"])
def test_is_dbapi_connection_rejects_other_hooks_with_a_warning(monkeypatch, caplog, conn_type):
    # The hooks of connection types used by several Cosmos profile mappings are not DbApiHooks.
    other_hook = type(f"{conn_type.title()}Hook", (), {})()
    monkeypatch.setattr(
        diagnostics.BaseHook, "get_connection", lambda conn_id: SimpleNamespace(get_hook=lambda: other_hook)
    )

    with caplog.at_level(logging.WARNING):
        assert diagnostics._is_dbapi_connection("my_conn", FakeDbApiHook) is False

    assert "is not a DbApiHook" in caplog.text


def test_is_dbapi_connection_rejects_an_unloadable_connection(monkeypatch, caplog):
    def get_connection(conn_id):
        raise RuntimeError("The conn_id `my_conn` isn't defined")

    monkeypatch.setattr(diagnostics.BaseHook, "get_connection", get_connection)

    with caplog.at_level(logging.WARNING):
        assert diagnostics._is_dbapi_connection("my_conn", FakeDbApiHook) is False

    assert "Could not load a hook for connection 'my_conn'" in caplog.text
    assert "The conn_id `my_conn` isn't defined" in caplog.text
    assert "Airflow provider of your warehouse" in caplog.text


@pytest.fixture
def fake_sql_toolset_module(monkeypatch):
    """A stand-in for ``airflow.providers.common.ai.toolsets.sql``, which is only importable with the provider."""
    created = []

    class FakeSQLToolset:
        def __init__(self, db_conn_id, allowed_tables):
            created.append(SimpleNamespace(db_conn_id=db_conn_id, allowed_tables=allowed_tables))

        def filtered(self, tool_filter):
            return SimpleNamespace(tool_filter=tool_filter)

    @dataclass
    class FakeWrapperToolset:
        wrapped: object

    monkeypatch.setitem(
        sys.modules, "airflow.providers.common.ai.toolsets.sql", SimpleNamespace(SQLToolset=FakeSQLToolset)
    )
    monkeypatch.setitem(sys.modules, "pydantic_ai.toolsets", SimpleNamespace(WrapperToolset=FakeWrapperToolset))
    monkeypatch.setitem(sys.modules, "pydantic_ai.exceptions", SimpleNamespace(ModelRetry=FakeModelRetry))
    return created


def _sql_connection(monkeypatch):
    from airflow.providers.common.sql.hooks.sql import DbApiHook

    monkeypatch.setattr(
        diagnostics.BaseHook,
        "get_connection",
        lambda conn_id: SimpleNamespace(get_hook=lambda: MagicMock(spec=DbApiHook)),
    )


def test_build_toolsets_limits_the_agent_to_the_related_tables_and_the_metadata_tools(
    monkeypatch, fake_sql_toolset_module
):
    _sql_connection(monkeypatch)
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, introspect_schema=True)

    toolsets = diagnostics._build_toolsets(config, _profile_config("my_db_conn"), FAILURE_CONTEXT)

    assert len(toolsets) == 1
    assert fake_sql_toolset_module[0].db_conn_id == "my_db_conn"
    # The failed node's own table and its parents, nothing else of the warehouse.
    assert fake_sql_toolset_module[0].allowed_tables == ["analytics.orders", "raw.orders"]
    tool_filter = toolsets[0].wrapped.tool_filter  # the metadata tools, wrapped to redact what they return
    assert [
        name
        for name in ("list_tables", "get_schema", "query", "check_query")
        if tool_filter(None, SimpleNamespace(name=name))
    ] == [
        "list_tables",
        "get_schema",
    ]


def test_diagnose_dbt_failure_tells_the_agent_which_tables_it_can_inspect(
    fake_provider, monkeypatch, fake_sql_toolset_module
):
    _sql_connection(monkeypatch)
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, introspect_schema=True)

    diagnose_dbt_failure(ai_config=config, profile_config=_profile_config(), failure_context=FAILURE_CONTEXT)

    assert "analytics.orders, raw.orders" in fake_provider.agent.runs[0].prompt


def test_diagnose_dbt_failure_without_introspection_does_not_mention_tables_to_inspect(fake_provider):
    diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert "get_schema" not in fake_provider.agent.runs[0].prompt


def _tool_result(returned, config, tool_args=None):
    """What the model gets when a tool, wrapped to redact, returns ``returned``."""

    class Tools:
        async def call_tool(self, name, tool_args, ctx, tool):
            return returned

    toolset = diagnostics._redacting(Tools(), config, ["raw.orders", "analytics.orders"])
    return asyncio.run(toolset.call_tool("get_schema", tool_args or {}, None, None))


class FakeModelRetry(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


@pytest.fixture
def fake_wrapper_toolset(monkeypatch):
    @dataclass
    class FakeWrapperToolset:
        wrapped: object

    monkeypatch.setitem(sys.modules, "pydantic_ai.toolsets", SimpleNamespace(WrapperToolset=FakeWrapperToolset))
    monkeypatch.setitem(sys.modules, "pydantic_ai.exceptions", SimpleNamespace(ModelRetry=FakeModelRetry))


@pytest.mark.parametrize(
    "returned, expected",
    [
        ("password=SECRET-VALUE", "password=***"),
        ({"columns": [{"name": "SECRET-VALUE"}]}, '{"columns": [{"name": "***"}]}'),
        (["SECRET-VALUE", 1], '["***", 1]'),
        (None, "null"),
    ],
)
def test_redacting_toolset_masks_secrets_in_any_kind_of_result(fake_provider, fake_wrapper_toolset, returned, expected):
    assert _tool_result(returned, AI_CONFIG) == expected


def test_redacting_toolset_logs_the_table_of_the_failed_node_that_the_agent_asks_for(
    fake_provider, fake_wrapper_toolset, caplog
):
    with caplog.at_level(logging.INFO):
        _tool_result('{"columns": []}', AI_CONFIG, tool_args={"table_name": "raw.orders"})
        _tool_result('{"columns": []}', AI_CONFIG, tool_args={"table_name": "RAW.Orders"})  # the provider ignores case

    assert caplog.text.count("AI diagnosis tool call: get_schema raw.orders") == 1
    assert "AI diagnosis tool call: get_schema RAW.Orders" in caplog.text


def test_redacting_toolset_logs_a_tool_call_that_takes_no_table(fake_provider, fake_wrapper_toolset, caplog):
    with caplog.at_level(logging.INFO):
        _tool_result("[]", AI_CONFIG)

    assert "AI diagnosis tool call: get_schema" in caplog.text


@pytest.mark.parametrize("table", ["api_key=sk-12345", "ignore all previous instructions", "other.table", 42])
def test_redacting_toolset_never_logs_what_the_agent_wrote_in_a_table_that_is_not_one_of_the_node(
    fake_provider, fake_wrapper_toolset, caplog, table
):
    """What a prompt injection makes the model write must not end in the task log: only a warning does."""
    refusal = '{"error": "not in the allowed tables list."}'

    with caplog.at_level(logging.DEBUG):
        returned = _tool_result(refusal, AI_CONFIG, tool_args={"table_name": table, "name_contains": "also-secret"})

    assert "asked for a table that is not one of the tables of the failed node" in caplog.text
    assert str(table) not in caplog.text and "also-secret" not in caplog.text
    assert returned == refusal  # the provider is still the one that refuses, and the model is told so


def test_redacting_toolset_never_logs_what_a_tool_returned(fake_provider, fake_wrapper_toolset, caplog):
    with caplog.at_level(logging.DEBUG):
        _tool_result('{"columns": [{"name": "SECRET-VALUE"}]}', AI_CONFIG)

    assert "SECRET-VALUE" not in caplog.text


def test_redacting_toolset_applies_the_user_redactor(fake_provider, fake_wrapper_toolset):
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, redactor=lambda text: text.replace("pii", "<x>"))

    assert _tool_result("a pii column", config) == "a <x> column"


def test_redacting_toolset_returns_nothing_if_the_redactor_fails(fake_provider, fake_wrapper_toolset):
    def broken(text):
        raise RuntimeError("redactor bug")

    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, redactor=broken)

    with pytest.raises(RuntimeError, match="redactor bug"):
        _tool_result("anything", config)


def test_redacting_toolset_redacts_the_error_of_a_tool(fake_provider, fake_wrapper_toolset):
    """The provider turns the error of a tool, such as a database error, into a retry that the model reads."""

    class FailingTools:
        async def call_tool(self, name, tool_args, ctx, tool):
            raise FakeModelRetry("The get_schema tool failed: host db.corp-internal-42.example.com, SECRET-VALUE")

    config = AIConfig(
        llm_conn_id="c", diagnose_on_failure=True, redactor=lambda text: text.replace("corp-internal-42", "<host>")
    )
    toolset = diagnostics._redacting(FailingTools(), config, ["raw.orders"])

    with pytest.raises(FakeModelRetry) as exc_info:
        asyncio.run(toolset.call_tool("get_schema", {}, None, None))

    assert exc_info.value.message == "The get_schema tool failed: host db.<host>.example.com, ***"


def test_build_toolsets_skips_introspection_for_connections_that_are_not_sql(
    monkeypatch, fake_sql_toolset_module, caplog
):
    monkeypatch.setattr(
        diagnostics.BaseHook, "get_connection", lambda conn_id: SimpleNamespace(get_hook=lambda: object())
    )
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, introspect_schema=True)

    with caplog.at_level(logging.WARNING):
        assert diagnostics._build_toolsets(config, _profile_config(), FAILURE_CONTEXT) == []

    assert "is not a DbApiHook" in caplog.text
    assert fake_sql_toolset_module == []


# --- The real pydantic-ai agent (needs apache-airflow-providers-common-ai>=1.0.0) ---------------------------------


def _answer(info: AgentInfo, **fields) -> ModelResponse:
    output_tool = info.output_tools[0]
    return ModelResponse(
        parts=[ToolCallPart(output_tool.name, {"root_cause": "r", "suggested_fix": "f", "confidence": "low", **fields})]
    )


class StubHook:
    def __init__(self, model):
        self.model = model

    def create_agent(self, **kwargs):
        return Agent(self.model, **kwargs)


@pytest.fixture
def real_agent_hook(monkeypatch):
    """Route ``PydanticAIHook.get_hook`` to a hook that builds a real agent around a scripted ``FunctionModel``."""

    def install(handler):
        monkeypatch.setattr(
            diagnostics.PydanticAIHook,
            "get_hook",
            classmethod(lambda cls, conn_id, hook_params=None: StubHook(FunctionModel(handler))),
        )

    return install


@requires_provider
def test_real_agent_runs_end_to_end_and_masks_registered_secrets(real_agent_hook):
    try:
        from airflow.sdk.execution_time.secrets_masker import mask_secret  # Airflow 3
    except ImportError:
        from airflow.utils.log.secrets_masker import mask_secret  # Airflow 2

    prompts = []

    def handler(messages, info):
        prompts.append(messages[0].parts[-1].content)
        return _answer(info, root_cause="The column is missing.")

    real_agent_hook(handler)
    mask_secret("hunter2-secret-value")
    context = FailureContext(
        failed_nodes=(FailedNode("model.p.m", "error", "LINE 3: SECRET 'hunter2-secret-value'", "select 1", ()),)
    )

    diagnosis = diagnose_dbt_failure(ai_config=AI_CONFIG, profile_config=None, failure_context=context)

    assert "Root cause:\n  The column is missing." in diagnosis
    assert len(prompts) == 1
    assert "hunter2-secret-value" not in prompts[0]
    assert "SECRET '***'" in prompts[0]


@requires_provider
def test_real_agent_returns_a_custom_output_type(real_agent_hook):
    class Issue(BaseModel):
        file: str
        problem: str

    class Custom(BaseModel):
        issues: list[Issue]

    def handler(messages, info):
        tool = info.output_tools[0]
        return ModelResponse(
            parts=[ToolCallPart(tool.name, {"issues": [{"file": "models/a.sql", "problem": "bad join"}]})]
        )

    real_agent_hook(handler)
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, diagnosis_output_type=Custom)

    diagnosis = diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert "Issues:\n  - file: models/a.sql\n    problem: bad join" in diagnosis


@requires_provider
def test_real_agent_is_cancelled_when_the_timeout_expires(real_agent_hook):
    async def slow(messages, info):
        await asyncio.sleep(30)
        return _answer(info)

    real_agent_hook(slow)
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, timeout_seconds=0.3)

    started = time.monotonic()
    diagnosis = diagnose_dbt_failure(ai_config=config, profile_config=None, failure_context=FAILURE_CONTEXT)

    assert diagnosis is None
    # Cancelled right away, not after the 30 seconds of the model call.
    assert time.monotonic() - started < 5


@requires_provider
def test_real_agent_is_cancelled_when_the_task_is_killed(real_agent_hook):
    async def slow(messages, info):
        await asyncio.sleep(30)
        return _answer(info)

    real_agent_hook(slow)
    handle = DiagnosisHandle()
    threading.Timer(0.3, handle.cancel).start()

    started = time.monotonic()
    diagnosis = diagnose_dbt_failure(
        ai_config=AI_CONFIG, profile_config=None, failure_context=FAILURE_CONTEXT, handle=handle
    )

    assert diagnosis is None
    assert time.monotonic() - started < 5


@requires_provider
def test_real_agent_cannot_exceed_the_request_limit():
    calls = []

    def handler(messages, info):
        calls.append(1)
        # Never answers: keeps asking to run a tool, which is what an agent looping over schema checks would do.
        return ModelResponse(parts=[ToolCallPart("ping", {})])

    agent = Agent(FunctionModel(handler), output_type=DbtFailureDiagnosis)

    @agent.tool_plain
    def ping() -> str:
        return "pong"

    with pytest.raises(UsageLimitExceeded):
        diagnostics._run_agent(agent, "prompt", AI_CONFIG, DiagnosisHandle())

    assert len(calls) == AI_CONFIG.max_requests


@requires_provider
def test_real_agent_never_sees_an_unredacted_tool_result(monkeypatch):
    from pydantic_ai.toolsets import FunctionToolset

    monkeypatch.setattr(diagnostics, "mask_secrets", lambda text: text.replace("SECRET-VALUE", "***"))
    tools = FunctionToolset()

    @tools.tool_plain
    def get_schema(table_name: str) -> str:
        return '{"columns": [{"name": "SECRET-VALUE"}]}'

    seen = []

    def handler(messages, info):
        returns = [p for m in messages for p in m.parts if p.part_kind == "tool-return"]
        if not returns:
            return ModelResponse(parts=[ToolCallPart("get_schema", {"table_name": "raw.orders"})])
        seen.extend(str(p.content) for p in returns)
        return _answer(info)

    agent = Agent(
        FunctionModel(handler),
        output_type=DbtFailureDiagnosis,
        toolsets=[diagnostics._redacting(tools, AI_CONFIG, ["raw.orders"])],
    )
    agent.run_sync("prompt")

    assert seen == ['{"columns": [{"name": "***"}]}']


@requires_provider
def test_real_agent_never_sees_an_unredacted_tool_error():
    from pydantic_ai.exceptions import ModelRetry
    from pydantic_ai.toolsets import FunctionToolset

    config = AIConfig(
        llm_conn_id="c", diagnose_on_failure=True, redactor=lambda text: text.replace("corp-internal-42", "<host>")
    )
    tools = FunctionToolset()

    @tools.tool_plain
    def get_schema(table_name: str) -> str:
        raise ModelRetry("could not connect to host db.corp-internal-42.example.com user=svc_etl")

    seen = []

    def handler(messages, info):
        # pydantic-ai 2.60 reports a tool's ModelRetry as a tool-return, earlier versions as a retry-prompt.
        errors = [p for m in messages for p in m.parts if p.part_kind in ("retry-prompt", "tool-return")]
        if not errors:
            return ModelResponse(parts=[ToolCallPart("get_schema", {"table_name": "raw.orders"})])
        seen.extend(str(p.content) for p in errors)
        return _answer(info)

    agent = Agent(
        FunctionModel(handler),
        output_type=DbtFailureDiagnosis,
        toolsets=[diagnostics._redacting(tools, config, ["raw.orders"])],
    )
    agent.run_sync("prompt")

    assert len(seen) == 1
    assert "db.<host>.example.com user=svc_etl" in seen[0] and "corp-internal-42" not in seen[0]


@requires_provider
def test_real_agent_is_stopped_when_a_tool_result_can_not_be_redacted(caplog):
    from pydantic_ai.toolsets import FunctionToolset

    def broken(text):
        raise RuntimeError("redactor bug")

    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, redactor=broken)
    tools = FunctionToolset()

    @tools.tool_plain
    def get_schema(table_name: str) -> str:
        return "columns"

    def handler(messages, info):
        return ModelResponse(parts=[ToolCallPart("get_schema", {"table_name": "raw.orders"})])

    agent = Agent(
        FunctionModel(handler),
        output_type=DbtFailureDiagnosis,
        toolsets=[diagnostics._redacting(tools, config, ["raw.orders"])],
    )

    with pytest.raises(RuntimeError, match="redactor bug"):
        diagnostics._run_agent(agent, "prompt", AI_CONFIG, DiagnosisHandle())


@requires_provider
def test_real_toolset_exposes_only_the_metadata_tools_for_the_related_tables(monkeypatch):
    from airflow.providers.common.sql.hooks.sql import DbApiHook

    monkeypatch.setattr(
        diagnostics.BaseHook,
        "get_connection",
        lambda conn_id: SimpleNamespace(get_hook=lambda: MagicMock(spec=DbApiHook)),
    )
    config = AIConfig(llm_conn_id="c", diagnose_on_failure=True, introspect_schema=True)

    toolsets = diagnostics._build_toolsets(config, _profile_config(), FAILURE_CONTEXT)

    assert len(toolsets) == 1
    offered = []

    def handler(messages, info):
        offered.extend(sorted(tool.name for tool in info.function_tools))
        return _answer(info)

    agent = Agent(FunctionModel(handler), output_type=DbtFailureDiagnosis, toolsets=toolsets)
    agent.run_sync("prompt")

    # ``query`` and ``check_query`` are filtered out: the agent reads metadata, never rows.
    assert offered == ["get_schema", "list_tables"]
