"Tests for the base profile mapping."

from unittest.mock import MagicMock

from airflow.models.connection import Connection

from cosmos.providers.dbt.core.profiles.base import BaseProfileMapping


def test_profile_file_contents() -> None:
    """
    Tests that the profile file contents are correct.
    """
    profile_mapping = BaseProfileMapping(
        Connection(conn_type="generic"),
    )

    # mock the profile_mapping.get_profile() method
    profile_mapping.get_profile = MagicMock(  # type: ignore
        return_value={
            "type": "conn_type",
            "host": "my_host",
            "user": "my_user",
        }
    )

    profile_contents = profile_mapping.get_profile_file_contents(
        profile_name="my_profile",
        target_name="my_target",
    )

    assert (
        profile_contents
        == """my_profile:
    outputs:
        my_target:
            host: my_host
            type: conn_type
            user: my_user
    target: my_target
"""
    )
