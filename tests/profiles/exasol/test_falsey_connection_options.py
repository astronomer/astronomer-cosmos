"""Profile generation must retain explicitly configured false and zero values."""

from __future__ import annotations

import json

import pytest
from airflow.models.connection import Connection

from cosmos.profiles.exasol.user_pass import ExasolUserPasswordProfileMapping


@pytest.mark.parametrize(
    "field,value",
    [("encryption", False), ("compression", False), ("socket_timeout", 0), ("connection_timeout", 0)],
)
def test_profile_retains_false_and_zero_connection_options(field: str, value: bool | int) -> None:
    mapping = ExasolUserPasswordProfileMapping(conn_id="offline")
    mapping._conn = Connection(conn_type="exasol", extra=json.dumps({field: value}))

    assert mapping.profile[field] == value
    assert type(mapping.profile[field]) is type(value)


@pytest.mark.parametrize("value", [None, "", True, 5])
def test_profile_optional_connection_values(value: bool | int | str | None) -> None:
    mapping = ExasolUserPasswordProfileMapping(conn_id="offline")
    mapping._conn = Connection(conn_type="exasol", extra=json.dumps({"encryption": value}))

    if value is None or value == "":
        assert "encryption" not in mapping.profile
    else:
        assert mapping.profile["encryption"] == value
