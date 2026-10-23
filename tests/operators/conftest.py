from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def dbt_v1_global_flag_format():
    """Keep the operator tests' expected ``--vars`` YAML string the same on dbt 1 and dbt 2 (JSON is covered in test_base)."""
    with patch("cosmos.operators.base.is_dbt_v2", return_value=False):
        yield
