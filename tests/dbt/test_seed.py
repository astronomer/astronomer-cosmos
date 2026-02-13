"""Tests for the seed change detection module."""

from unittest.mock import patch

import pytest

from cosmos.dbt.seed import (
    SEED_HASH_VARIABLE_PREFIX,
    _compute_file_hash,
    _get_variable_key,
    get_stored_seed_hash,
    has_seed_changed,
    store_seed_hash,
    update_seed_hash_after_run,
)


def test_compute_hash_for_existing_file(tmp_path):
    """Test that hash is computed correctly for an existing file."""
    test_file = tmp_path / "test.csv"
    test_file.write_text("id,name\n1,Alice\n2,Bob\n")

    hash_result = _compute_file_hash(test_file)

    assert isinstance(hash_result, str)
    assert len(hash_result) == 64


def test_same_content_same_hash(tmp_path):
    """Test that same content produces the same hash."""
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    content = "id,name\n1,Alice\n"
    file1.write_text(content)
    file2.write_text(content)

    assert _compute_file_hash(file1) == _compute_file_hash(file2)


def test_different_content_different_hash(tmp_path):
    """Test that different content produces different hashes."""
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    file1.write_text("id,name\n1,Alice\n")
    file2.write_text("id,name\n1,Bob\n")

    assert _compute_file_hash(file1) != _compute_file_hash(file2)


def test_file_not_found_raises_exception(tmp_path):
    """Test that FileNotFoundError is raised for non-existent file."""
    non_existent_file = tmp_path / "non_existent.csv"

    with pytest.raises(FileNotFoundError):
        _compute_file_hash(non_existent_file)


def test_generates_valid_key():
    """Test that a valid variable key is generated."""
    key = _get_variable_key("my_dag__my_task_group", "seed.my_project.my_seed")

    assert key.startswith(SEED_HASH_VARIABLE_PREFIX)
    assert "my_dag" in key
    assert "my_seed" in key
    assert "." not in key.replace(SEED_HASH_VARIABLE_PREFIX, "")


def test_sanitizes_special_characters():
    """Test that special characters are sanitized."""
    key = _get_variable_key("dag-with-dashes", "seed.project.seed-name")

    assert "-" not in key
    assert "." not in key.replace(SEED_HASH_VARIABLE_PREFIX, "")


@patch("cosmos.dbt.seed.Variable")
def test_get_stored_seed_hash_returns_hash_when_exists(mock_variable):
    """Test that stored hash is returned when it exists."""
    mock_variable.get.return_value = "abc123"

    result = get_stored_seed_hash("my_dag", "seed.project.my_seed")

    assert result == "abc123"
    mock_variable.get.assert_called_once()


@patch("cosmos.dbt.seed.Variable")
def test_get_stored_seed_hash_returns_none_when_not_exists(mock_variable):
    """Test that None is returned when hash doesn't exist."""
    mock_variable.get.return_value = None

    result = get_stored_seed_hash("my_dag", "seed.project.my_seed")

    assert result is None


@patch("cosmos.dbt.seed.Variable")
def test_get_stored_seed_hash_returns_none_on_exception(mock_variable):
    """Test that None is returned when an exception occurs."""
    mock_variable.get.side_effect = Exception("DB error")

    result = get_stored_seed_hash("my_dag", "seed.project.my_seed")

    assert result is None


@patch("cosmos.dbt.seed.Variable")
def test_store_seed_hash_stores_hash_successfully(mock_variable):
    """Test that hash is stored successfully."""
    store_seed_hash("my_dag", "seed.project.my_seed", "abc123")

    mock_variable.set.assert_called_once()
    call_args = mock_variable.set.call_args
    assert call_args[0][1] == "abc123"


@patch("cosmos.dbt.seed.Variable")
def test_store_seed_hash_raises_exception_on_failure(mock_variable):
    """Test that exception is raised when storage fails."""
    mock_variable.set.side_effect = Exception("DB error")

    with pytest.raises(Exception, match="DB error"):
        store_seed_hash("my_dag", "seed.project.my_seed", "abc123")


@patch("cosmos.dbt.seed.get_stored_seed_hash")
def test_has_seed_changed_returns_true_when_no_stored_hash(mock_get_stored, tmp_path):
    """Test that True is returned when no previous hash exists."""
    test_file = tmp_path / "seed.csv"
    test_file.write_text("id,name\n1,Alice\n")
    mock_get_stored.return_value = None

    result = has_seed_changed("my_dag", "seed.project.my_seed", test_file)

    assert result is True


@patch("cosmos.dbt.seed.get_stored_seed_hash")
def test_has_seed_changed_returns_false_when_hash_matches(mock_get_stored, tmp_path):
    """Test that False is returned when hash matches."""
    test_file = tmp_path / "seed.csv"
    test_file.write_text("id,name\n1,Alice\n")
    current_hash = _compute_file_hash(test_file)
    mock_get_stored.return_value = current_hash

    result = has_seed_changed("my_dag", "seed.project.my_seed", test_file)

    assert result is False


@patch("cosmos.dbt.seed.get_stored_seed_hash")
def test_has_seed_changed_returns_true_when_hash_differs(mock_get_stored, tmp_path):
    """Test that True is returned when hash differs."""
    test_file = tmp_path / "seed.csv"
    test_file.write_text("id,name\n1,Alice\n")
    mock_get_stored.return_value = "different_hash"

    result = has_seed_changed("my_dag", "seed.project.my_seed", test_file)

    assert result is True


@patch("cosmos.dbt.seed.get_stored_seed_hash")
def test_has_seed_changed_returns_true_when_file_not_found(mock_get_stored, tmp_path):
    """Test that True is returned when file doesn't exist."""
    non_existent_file = tmp_path / "non_existent.csv"

    result = has_seed_changed("my_dag", "seed.project.my_seed", non_existent_file)

    assert result is True


@patch("cosmos.dbt.seed.get_stored_seed_hash")
def test_has_seed_changed_accepts_string_path(mock_get_stored, tmp_path):
    """Test that string paths are accepted."""
    test_file = tmp_path / "seed.csv"
    test_file.write_text("id,name\n1,Alice\n")
    mock_get_stored.return_value = None

    result = has_seed_changed("my_dag", "seed.project.my_seed", str(test_file))

    assert result is True


@patch("cosmos.dbt.seed.store_seed_hash")
def test_update_seed_hash_after_run_updates_hash_successfully(mock_store, tmp_path):
    """Test that hash is updated after successful run."""
    test_file = tmp_path / "seed.csv"
    test_file.write_text("id,name\n1,Alice\n")
    expected_hash = _compute_file_hash(test_file)

    update_seed_hash_after_run("my_dag", "seed.project.my_seed", test_file)

    mock_store.assert_called_once_with("my_dag", "seed.project.my_seed", expected_hash)


@patch("cosmos.dbt.seed.store_seed_hash")
def test_update_seed_hash_after_run_handles_non_existent_file(mock_store, tmp_path):
    """Test that non-existent file doesn't cause crash."""
    non_existent_file = tmp_path / "non_existent.csv"

    update_seed_hash_after_run("my_dag", "seed.project.my_seed", non_existent_file)

    mock_store.assert_not_called()


@patch("cosmos.dbt.seed.store_seed_hash")
def test_update_seed_hash_after_run_raises_on_store_failure(mock_store, tmp_path):
    """Test that exception is raised when store fails."""
    test_file = tmp_path / "seed.csv"
    test_file.write_text("id,name\n1,Alice\n")
    mock_store.side_effect = Exception("DB error")

    with pytest.raises(Exception, match="DB error"):
        update_seed_hash_after_run("my_dag", "seed.project.my_seed", test_file)
