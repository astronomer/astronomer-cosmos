import time
from unittest.mock import MagicMock, patch

from pydantic import BaseModel, Field

from cosmos.ai.diagnostics import DbtFailureDiagnosis, diagnose_dbt_failure
from cosmos.config import AiConfig, ProfileConfig


def _ai_config(**overrides):
    return AiConfig(llm_conn_id="my_llm_conn", diagnose_on_failure=True, **overrides)


@patch("cosmos.ai.diagnostics.PydanticAIHook", None)
def test_diagnose_dbt_failure_missing_dependency_returns_none(caplog):
    result = diagnose_dbt_failure(
        ai_config=_ai_config(), profile_config=None, compiled_sql="select 1", raw_output="boom"
    )
    assert result is None
    assert "apache-airflow-providers-common-ai" in caplog.text


@patch("cosmos.ai.diagnostics.CancellationToken", MagicMock())
@patch("cosmos.ai.diagnostics.PydanticAIHook")
def test_diagnose_dbt_failure_happy_path_default_output_type(mock_hook_cls):
    expected = DbtFailureDiagnosis(root_cause="Missing column.", suggested_fix="Add it back.", confidence="high")
    mock_agent = MagicMock()
    mock_agent.run_sync.return_value = MagicMock(output=expected)
    mock_hook_cls.return_value.create_agent.return_value = mock_agent

    result = diagnose_dbt_failure(
        ai_config=_ai_config(), profile_config=None, compiled_sql="select 1", raw_output="boom"
    )

    assert result == expected
    _, kwargs = mock_hook_cls.return_value.create_agent.call_args
    assert kwargs["output_type"] is DbtFailureDiagnosis
    assert kwargs["toolsets"] == []


@patch("cosmos.ai.diagnostics.CancellationToken", MagicMock())
@patch("cosmos.ai.diagnostics.PydanticAIHook")
def test_diagnose_dbt_failure_custom_output_type(mock_hook_cls):
    class CustomDiagnosis(BaseModel):
        summary: str = Field(description="What happened.")

    expected = CustomDiagnosis(summary="It broke.")
    mock_agent = MagicMock()
    mock_agent.run_sync.return_value = MagicMock(output=expected)
    mock_hook_cls.return_value.create_agent.return_value = mock_agent

    result = diagnose_dbt_failure(
        ai_config=_ai_config(diagnosis_output_type=CustomDiagnosis),
        profile_config=None,
        compiled_sql="select 1",
        raw_output="boom",
    )

    assert result == expected
    _, kwargs = mock_hook_cls.return_value.create_agent.call_args
    assert kwargs["output_type"] is CustomDiagnosis
    # A custom output type without explicit instructions falls back to the generic prompt,
    # not the default DbtFailureDiagnosis-specific one.
    assert "root_cause" not in kwargs["instructions"]


@patch("cosmos.ai.diagnostics.CancellationToken", MagicMock())
@patch("cosmos.ai.diagnostics.PydanticAIHook")
def test_diagnose_dbt_failure_custom_instructions_passed_through(mock_hook_cls):
    mock_agent = MagicMock()
    mock_agent.run_sync.return_value = MagicMock(
        output=DbtFailureDiagnosis(root_cause="x", suggested_fix="y", confidence="low")
    )
    mock_hook_cls.return_value.create_agent.return_value = mock_agent

    diagnose_dbt_failure(
        ai_config=_ai_config(diagnosis_instructions="Custom instructions."),
        profile_config=None,
        compiled_sql="select 1",
        raw_output="boom",
    )

    _, kwargs = mock_hook_cls.return_value.create_agent.call_args
    assert kwargs["instructions"] == "Custom instructions."


@patch("cosmos.ai.diagnostics.CancellationToken", MagicMock())
@patch("cosmos.ai.diagnostics.SQLToolset")
@patch("cosmos.ai.diagnostics.PydanticAIHook")
def test_diagnose_dbt_failure_introspect_schema_adds_sql_toolset(mock_hook_cls, mock_sql_toolset_cls):
    mock_agent = MagicMock()
    mock_agent.run_sync.return_value = MagicMock(
        output=DbtFailureDiagnosis(root_cause="x", suggested_fix="y", confidence="low")
    )
    mock_hook_cls.return_value.create_agent.return_value = mock_agent
    profile_mapping = MagicMock(conn_id="my_warehouse_conn")
    profile_config = MagicMock(spec=ProfileConfig, profile_mapping=profile_mapping)

    diagnose_dbt_failure(
        ai_config=_ai_config(introspect_schema=True),
        profile_config=profile_config,
        compiled_sql="select 1",
        raw_output="boom",
    )

    mock_sql_toolset_cls.assert_called_once_with(db_conn_id="my_warehouse_conn")


@patch("cosmos.ai.diagnostics.CancellationToken", MagicMock())
@patch("cosmos.ai.diagnostics.PydanticAIHook")
def test_diagnose_dbt_failure_timeout_returns_none(mock_hook_cls, caplog):
    def slow_run_sync(prompt, cancellation_token=None):
        time.sleep(0.3)
        return MagicMock(output=DbtFailureDiagnosis(root_cause="x", suggested_fix="y", confidence="low"))

    mock_agent = MagicMock()
    mock_agent.run_sync.side_effect = slow_run_sync
    mock_hook_cls.return_value.create_agent.return_value = mock_agent

    result = diagnose_dbt_failure(
        ai_config=_ai_config(timeout_seconds=0.05),
        profile_config=None,
        compiled_sql="select 1",
        raw_output="boom",
    )

    assert result is None
    assert "timed out" in caplog.text


@patch("cosmos.ai.diagnostics.PydanticAIHook")
def test_diagnose_dbt_failure_timeout_cancels_the_run(mock_hook_cls):
    mock_token = MagicMock()

    def slow_run_sync(prompt, cancellation_token=None):
        time.sleep(0.3)
        return MagicMock(output=DbtFailureDiagnosis(root_cause="x", suggested_fix="y", confidence="low"))

    mock_agent = MagicMock()
    mock_agent.run_sync.side_effect = slow_run_sync
    mock_hook_cls.return_value.create_agent.return_value = mock_agent

    with patch("cosmos.ai.diagnostics.CancellationToken", return_value=mock_token):
        result = diagnose_dbt_failure(
            ai_config=_ai_config(timeout_seconds=0.05),
            profile_config=None,
            compiled_sql="select 1",
            raw_output="boom",
        )

    assert result is None
    mock_token.cancel.assert_called_once()


@patch("cosmos.ai.diagnostics.PydanticAIHook")
def test_diagnose_dbt_failure_unexpected_error_returns_none(mock_hook_cls, caplog):
    mock_hook_cls.side_effect = RuntimeError("boom")

    result = diagnose_dbt_failure(
        ai_config=_ai_config(), profile_config=None, compiled_sql="select 1", raw_output="boom"
    )

    assert result is None
    assert "AI diagnosis failed" in caplog.text
