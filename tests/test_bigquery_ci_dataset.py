import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("google.cloud.bigquery")
from google.api_core import exceptions  # noqa: E402

SCRIPT = Path(__file__).parent.parent / "scripts/test/bigquery_ci_dataset.py"
spec = importlib.util.spec_from_file_location("bigquery_ci_dataset", SCRIPT)
bigquery_ci_dataset = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bigquery_ci_dataset)


def test_dataset_name_must_be_set(monkeypatch):
    monkeypatch.delenv("COSMOS_TEST_BIGQUERY_DATASET", raising=False)
    with pytest.raises(SystemExit, match="not set"):
        bigquery_ci_dataset._dataset_name()


def test_dataset_name_must_start_with_the_ci_prefix(monkeypatch):
    monkeypatch.setenv("COSMOS_TEST_BIGQUERY_DATASET", "prod_data")
    with pytest.raises(SystemExit, match="Refusing"):
        bigquery_ci_dataset._dataset_name()


def test_dataset_name_with_the_ci_prefix_is_accepted(monkeypatch):
    monkeypatch.setenv("COSMOS_TEST_BIGQUERY_DATASET", "cosmos_ci_1_1_oss_0")
    assert bigquery_ci_dataset._dataset_name() == "cosmos_ci_1_1_oss_0"


@pytest.mark.parametrize(
    "exc",
    [
        exceptions.TooManyRequests("slow down"),
        exceptions.ServiceUnavailable("try later"),
        exceptions.InternalServerError("oops"),
        exceptions.Forbidden("rate limited", errors=[{"reason": "rateLimitExceeded"}]),
    ],
)
def test_transient_errors_are_retried(exc):
    assert bigquery_ci_dataset._is_transient(exc)


@pytest.mark.parametrize(
    "exc",
    [
        exceptions.Forbidden("no access", errors=[{"reason": "accessDenied"}]),
        exceptions.Forbidden("no details"),
        exceptions.NotFound("gone"),
        ValueError("not an API error"),
    ],
)
def test_other_errors_are_not_retried(exc):
    assert not bigquery_ci_dataset._is_transient(exc)


def test_create_labels_the_dataset_and_makes_its_tables_expire(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    client = MagicMock(project="my-project")
    monkeypatch.setattr(bigquery_ci_dataset, "_client", lambda: client)

    bigquery_ci_dataset.create("cosmos_ci_42_1_oss_0", max_start_jitter=0)

    dataset = client.create_dataset.call_args.args[0]
    assert dataset.dataset_id == "cosmos_ci_42_1_oss_0"
    assert dataset.project == "my-project"
    assert dataset.labels == {"ci": "cosmos", "workflow": "test-dbt-oss", "run-id": "42"}
    assert dataset.default_table_expiration_ms == 6 * 60 * 60 * 1000
    assert client.create_dataset.call_args.kwargs["exists_ok"] is True
    assert client.create_dataset.call_args.kwargs["retry"] is bigquery_ci_dataset.RETRY
    client.get_dataset.assert_called_once_with("cosmos_ci_42_1_oss_0")


def test_create_waits_until_the_dataset_is_visible(monkeypatch):
    client = MagicMock(project="my-project")
    client.get_dataset.side_effect = [exceptions.NotFound("not yet"), exceptions.NotFound("not yet"), MagicMock()]
    monkeypatch.setattr(bigquery_ci_dataset, "_client", lambda: client)
    monkeypatch.setattr(bigquery_ci_dataset.time, "sleep", lambda _seconds: None)

    bigquery_ci_dataset.create("cosmos_ci_42_1_oss_0", max_start_jitter=0)

    assert client.get_dataset.call_count == 3


def test_create_gives_up_when_the_dataset_never_becomes_visible(monkeypatch):
    client = MagicMock(project="my-project")
    client.get_dataset.side_effect = exceptions.NotFound("never")
    monkeypatch.setattr(bigquery_ci_dataset, "_client", lambda: client)
    monkeypatch.setattr(bigquery_ci_dataset.time, "sleep", lambda _seconds: None)
    clock = iter(range(0, 1000, 30))
    monkeypatch.setattr(bigquery_ci_dataset.time, "monotonic", lambda: next(clock))

    with pytest.raises(SystemExit, match="still not visible"):
        bigquery_ci_dataset.create("cosmos_ci_42_1_oss_0", max_start_jitter=0)


def test_delete_removes_the_dataset_and_its_contents(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(bigquery_ci_dataset, "_client", lambda: client)

    bigquery_ci_dataset.delete("cosmos_ci_42_1_oss_0")

    kwargs = client.delete_dataset.call_args.kwargs
    assert client.delete_dataset.call_args.args == ("cosmos_ci_42_1_oss_0",)
    assert kwargs["delete_contents"] is True
    assert kwargs["not_found_ok"] is True
    assert kwargs["retry"] is bigquery_ci_dataset.RETRY


def test_delete_failure_is_a_warning_not_an_error(monkeypatch, capsys):
    client = MagicMock()
    client.delete_dataset.side_effect = exceptions.Forbidden("no access")
    monkeypatch.setattr(bigquery_ci_dataset, "_client", lambda: client)

    bigquery_ci_dataset.delete("cosmos_ci_42_1_oss_0")

    assert "::warning::Could not delete BigQuery dataset cosmos_ci_42_1_oss_0" in capsys.readouterr().out


def test_delete_retry_error_is_a_warning_not_an_error(monkeypatch, capsys):
    client = MagicMock()
    client.delete_dataset.side_effect = exceptions.RetryError("gave up", cause=exceptions.ServiceUnavailable("down"))
    monkeypatch.setattr(bigquery_ci_dataset, "_client", lambda: client)

    bigquery_ci_dataset.delete("cosmos_ci_42_1_oss_0")

    assert "::warning::Could not delete BigQuery dataset cosmos_ci_42_1_oss_0" in capsys.readouterr().out


def test_delete_does_not_hide_a_bad_connection(monkeypatch):
    def broken_client():
        raise KeyError("AIRFLOW_CONN_GCP_GS_CONN")

    monkeypatch.setattr(bigquery_ci_dataset, "_client", broken_client)

    with pytest.raises(KeyError):
        bigquery_ci_dataset.delete("cosmos_ci_42_1_oss_0")


def test_client_without_a_keyfile_dict_says_so(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_GCP_GS_CONN", '{"conn_type": "google_cloud_platform", "extra": {"key_path": "/k"}}'
    )

    with pytest.raises(SystemExit, match="no keyfile_dict"):
        bigquery_ci_dataset._client()
