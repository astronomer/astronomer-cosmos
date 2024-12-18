from unittest.mock import patch

from cosmos.profiles.snowflake.base import SnowflakeBaseProfileMapping


@patch("cosmos.profiles.snowflake.base.SnowflakeBaseProfileMapping.conn.extra_dejson", {"region": "us-west-2"})
@patch("cosmos.profiles.snowflake.base.SnowflakeBaseProfileMapping.conn")
def test_default_region(mock_conn):
    profile_mapping = SnowflakeBaseProfileMapping(conn_id="fake-conn")
    response = profile_mapping.transform_account("myaccount")
    assert response == "myaccount"


@patch("cosmos.profiles.snowflake.base.SnowflakeBaseProfileMapping.conn.extra_dejson", {"region": "us-east-1"})
@patch("cosmos.profiles.snowflake.base.SnowflakeBaseProfileMapping.conn")
def test_non_default_region(mock_conn):
    profile_mapping = SnowflakeBaseProfileMapping(conn_id="fake-conn")
    response = profile_mapping.transform_account("myaccount")
    assert response == "myaccount.us-east-1"
