from unittest.mock import patch

from cosmos.dbt.seed import (
    _compute_csv_checksum,
    _evaluate_seed_change,
    _persist_seed_checksum,
    _resolve_seed_checksum,
    _seed_variable_key,
)


def test_seed_variable_key_is_bounded_and_stable():
    key = _seed_variable_key("my_dag", "seed.pkg.my_seed")
    assert key == _seed_variable_key("my_dag", "seed.pkg.my_seed")
    assert key.startswith("cosmos_seed_checksum__")
    assert len(key) < 250


def test_seed_variable_key_is_scoped_per_identifier():
    assert _seed_variable_key("dag_a", "seed.pkg.s") != _seed_variable_key("dag_b", "seed.pkg.s")


def test_compute_csv_checksum_is_deterministic_sha256(tmp_path):
    seed_file = tmp_path / "seed.csv"
    seed_file.write_text("id,name\n1,alice\n")
    checksum = _compute_csv_checksum(seed_file)
    assert checksum == _compute_csv_checksum(seed_file)
    assert len(checksum) == 64


def test_compute_csv_checksum_returns_none_when_unreadable(tmp_path):
    assert _compute_csv_checksum(tmp_path / "missing.csv") is None


def test_resolve_seed_checksum_prefers_manifest(tmp_path):
    seed_file = tmp_path / "seed.csv"
    seed_file.write_text("x")
    assert _resolve_seed_checksum("manifest-checksum", str(seed_file)) == "manifest-checksum"


def test_resolve_seed_checksum_falls_back_to_csv(tmp_path):
    seed_file = tmp_path / "seed.csv"
    seed_file.write_text("x")
    assert _resolve_seed_checksum(None, str(seed_file)) == _compute_csv_checksum(seed_file)


def test_resolve_seed_checksum_none_without_source():
    assert _resolve_seed_checksum(None, None) is None


@patch("cosmos.dbt.seed.Variable")
def test_evaluate_seed_change_unchanged(mock_variable):
    mock_variable.get.return_value = "abc"
    should_run, checksum, key = _evaluate_seed_change("dag", "seed.pkg.s", "abc", None)
    assert should_run is False
    assert checksum == "abc"
    assert key == _seed_variable_key("dag", "seed.pkg.s")


@patch("cosmos.dbt.seed.Variable")
def test_evaluate_seed_change_changed(mock_variable):
    mock_variable.get.return_value = "old"
    should_run, checksum, _ = _evaluate_seed_change("dag", "seed.pkg.s", "new", None)
    assert should_run is True
    assert checksum == "new"


@patch("cosmos.dbt.seed.Variable")
def test_evaluate_seed_change_runs_when_no_stored_value(mock_variable):
    mock_variable.get.return_value = None
    should_run, _, _ = _evaluate_seed_change("dag", "seed.pkg.s", "new", None)
    assert should_run is True


def test_evaluate_seed_change_runs_when_checksum_undeterminable():
    should_run, checksum, key = _evaluate_seed_change("dag", "seed.pkg.s", None, None)
    assert should_run is True
    assert checksum is None
    assert key is None


@patch("cosmos.dbt.seed.Variable")
def test_persist_seed_checksum_sets_variable(mock_variable):
    _persist_seed_checksum("key", "abc")
    mock_variable.set.assert_called_once_with("key", "abc")


@patch("cosmos.dbt.seed.Variable")
def test_persist_seed_checksum_noop_when_missing(mock_variable):
    _persist_seed_checksum(None, "abc")
    _persist_seed_checksum("key", None)
    mock_variable.set.assert_not_called()


@patch("cosmos.dbt.seed.Variable")
def test_persist_seed_checksum_is_best_effort(mock_variable):
    from airflow.exceptions import AirflowException

    mock_variable.set.side_effect = AirflowException("boom")
    _persist_seed_checksum("key", "abc")  # must not raise
