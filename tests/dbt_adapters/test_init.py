from __future__ import annotations

from unittest.mock import Mock

import pytest

from cosmos.dbt_adapters import associate_async_operator_args


def test_associate_async_operator_args_invalid_profile():
    """Test associate_async_operator_args raises KeyError for an invalid profile type."""
    async_operator_mock = Mock()

    with pytest.raises(KeyError):
        associate_async_operator_args(async_operator_mock, "invalid_profile")
