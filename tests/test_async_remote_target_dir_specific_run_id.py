from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cosmos.config import ProfileConfig
from cosmos.constants import InvocationMode
from cosmos.io import _construct_dest_file_path
from cosmos.operators.local import AbstractDbtLocalBase


class ConcreteDbtLocalBaseOperator(AbstractDbtLocalBase):
    """Concrete implementation of AbstractDbtLocalBase for testing."""

    base_cmd = ["cmd"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.invocation_mode = InvocationMode.SUBPROCESS


@pytest.fixture
def dummy_context():
    """Fixture for reusable test context."""
    return {
        "dag": MagicMock(dag_id="test_dag"),
        "run_id": "test_run_id",
        "task_instance": MagicMock(task_id="test_task", _try_number=1),
    }


@pytest.fixture
def profile_config():
    """Fixture for profile config."""
    return ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=MagicMock(),
    )


@pytest.fixture
def dummy_kwargs():
    """Fixture for reusable test kwargs."""
    return {
        "context": {
            "dag": MagicMock(dag_id="test_dag"),
            "run_id": "test_run_id",
            "task_instance": MagicMock(task_id="test_task", _try_number=1),
        },
        "bucket_name": "test_bucket",
        "container_name": "test_container",
    }


def test_construct_dest_file_path_with_run_id(dummy_kwargs):
    """Test _construct_dest_file_path uses run_id correctly."""
    dest_target_dir = Path("/dest")
    source_target_dir = Path("/project_dir/target")
    file_path = "/project_dir/target/subdir/file.txt"
    source_subpath = "target"

    expected_path = "/dest/test_dag/test_run_id/test_task/1/target/subdir/file.txt"
    result = _construct_dest_file_path(
        dest_target_dir, file_path, source_target_dir, source_subpath, context=dummy_kwargs["context"]
    )

    assert result == expected_path
    assert "test_run_id" in result


def test_operator_construct_dest_file_path_with_run_id(profile_config):
    """Test that the operator's _construct_dest_file_path method uses run_id correctly."""
    operator = ConcreteDbtLocalBaseOperator(
        task_id="test_task", profile_config=profile_config, project_dir="/project_dir"
    )

    operator.extra_context = {"run_id": "test_run_id", "dbt_dag_task_group_identifier": "test_task_group"}

    dest_target_dir = Path("/dest")
    source_compiled_dir = Path("/project_dir/target/compiled")
    file_path = "/project_dir/target/compiled/models/my_model.sql"
    resource_type = "compiled"

    expected_path = "/dest/test_task_group/test_run_id/compiled/models/my_model.sql"
    result = operator._construct_dest_file_path(dest_target_dir, file_path, source_compiled_dir, resource_type)

    assert result == expected_path
    assert "test_run_id" in result


def test_construct_dest_file_path_in_operator(profile_config):
    """Test that the operator's _construct_dest_file_path method uses run_id correctly."""
    operator = ConcreteDbtLocalBaseOperator(
        task_id="test_task", profile_config=profile_config, project_dir="/project_dir"
    )

    operator.extra_context = {"run_id": "test_run_id", "dbt_dag_task_group_identifier": "test_task_group"}

    dest_target_dir = Path("/dest")
    source_compiled_dir = Path("/project_dir/target/compiled")
    file_path = "/project_dir/target/compiled/models/my_model.sql"
    resource_type = "compiled"

    expected_path = "/dest/test_task_group/test_run_id/compiled/models/my_model.sql"

    with patch.object(
        operator, "_construct_dest_file_path", wraps=operator._construct_dest_file_path
    ) as mock_construct:
        result = operator._construct_dest_file_path(dest_target_dir, file_path, source_compiled_dir, resource_type)

        assert result == expected_path
        assert "test_run_id" in result

        mock_construct.assert_called_once_with(dest_target_dir, file_path, source_compiled_dir, resource_type)
