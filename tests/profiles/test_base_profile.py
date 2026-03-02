from __future__ import annotations

from typing import Any

import pytest
import yaml

from cosmos.exceptions import CosmosValueError
from cosmos.profiles.base import BaseProfileMapping, DbtProfileConfigVars


class TestProfileMapping(BaseProfileMapping):
    __test__ = False
    dbt_profile_method: str = "fake-method"
    dbt_profile_type: str = "fake-type"

    @property
    def profile(self) -> dict[str, str]:
        return {"some-profile-key": "some-profile-value"}


@pytest.mark.parametrize("profile_arg", ["type", "method"])
def test_validate_profile_args(profile_arg: str):
    """
    An error should be raised if the profile_args contains a key that should not be overridden from the class variables.
    """
    profile_args = {profile_arg: "fake-value"}
    dbt_profile_value = getattr(TestProfileMapping, f"dbt_profile_{profile_arg}")

    expected_cosmos_error = (
        f"`profile_args` for TestProfileMapping has {profile_arg}='fake-value' that will override the dbt profile required value of "
        f"'{dbt_profile_value}'. To fix this, remove {profile_arg} from `profile_args`."
    )

    with pytest.raises(CosmosValueError, match=expected_cosmos_error):
        TestProfileMapping(
            conn_id="fake_conn_id",
            profile_args=profile_args,
        )


@pytest.mark.parametrize("disable_event_tracking", [True, False])
def test_disable_event_tracking(disable_event_tracking: bool):
    """
    Tests the config block in the profile is set correctly if disable_event_tracking is set.
    """
    test_profile = TestProfileMapping(
        conn_id="fake_conn_id",
        disable_event_tracking=disable_event_tracking,
    )
    profile_contents = yaml.safe_load(test_profile.get_profile_file_contents(profile_name="fake-profile-name"))

    assert "config" not in profile_contents
    if disable_event_tracking:
        assert test_profile.env_vars["DO_NOT_TRACK"] == "1"
        assert test_profile.env_vars["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "False"


def test_disable_event_tracking_and_send_anonymous_usage_stats():
    expected_cosmos_error = (
        "Cannot set both disable_event_tracking and "
        "dbt_config_vars=DbtProfileConfigVars(send_anonymous_usage_stats ..."
    )

    with pytest.raises(CosmosValueError) as err_info:
        TestProfileMapping(
            conn_id="fake_conn_id",
            dbt_config_vars=DbtProfileConfigVars(send_anonymous_usage_stats=False),
            disable_event_tracking=True,
        )
    assert err_info.value.args[0] == expected_cosmos_error


def test_dbt_profile_config_vars_none():
    """
    Tests the DbtProfileConfigVars return None.
    """
    dbt_config_vars = DbtProfileConfigVars(
        send_anonymous_usage_stats=None,
        partial_parse=None,
        use_experimental_parser=None,
        static_parser=None,
        printer_width=None,
        write_json=None,
        warn_error=None,
        warn_error_options=None,
        log_format=None,
        debug=None,
        version_check=None,
    )
    assert dbt_config_vars.as_dict() is None


@pytest.mark.parametrize("config", [True, False])
def test_dbt_config_vars_config(config: bool):
    """
    Tests the config block in the profile is set correctly.
    """

    dbt_config_vars = None
    if config:
        dbt_config_vars = DbtProfileConfigVars(debug=False)

    test_profile = TestProfileMapping(
        conn_id="fake_conn_id",
        dbt_config_vars=dbt_config_vars,
    )
    profile_contents = yaml.safe_load(test_profile.get_profile_file_contents(profile_name="fake-profile-name"))

    assert ("config" in profile_contents["fake-profile-name"]) == config


@pytest.mark.parametrize("dbt_config_var,dbt_config_value", [("debug", True), ("debug", False)])
def test_validate_dbt_config_vars(dbt_config_var: str, dbt_config_value: Any):
    """
    Tests the config block in the profile is set correctly.
    """
    dbt_config_vars = {dbt_config_var: dbt_config_value}
    test_profile = TestProfileMapping(
        conn_id="fake_conn_id",
        dbt_config_vars=DbtProfileConfigVars(**dbt_config_vars),
    )
    profile_contents = yaml.safe_load(test_profile.get_profile_file_contents(profile_name="fake-profile-name"))

    assert "config" in profile_contents["fake-profile-name"]
    assert profile_contents["fake-profile-name"]["config"][dbt_config_var] == dbt_config_value


@pytest.mark.parametrize(
    "dbt_config_var,dbt_config_value",
    [("send_anonymous_usage_stats", 2), ("send_anonymous_usage_stats", "aaa")],
)
def test_profile_config_validate_dbt_config_vars_check_unexpected_types(dbt_config_var: str, dbt_config_value: Any):
    dbt_config_vars = {dbt_config_var: dbt_config_value}

    with pytest.raises(ValueError):
        TestProfileMapping(
            conn_id="fake_conn_id",
            dbt_config_vars=DbtProfileConfigVars(**dbt_config_vars),
        )


@pytest.mark.parametrize("dbt_config_var,dbt_config_value", [("send_anonymous_usage_stats", True)])
def test_profile_config_validate_dbt_config_vars_check_expected_types(dbt_config_var: str, dbt_config_value: Any):
    dbt_config_vars = {dbt_config_var: dbt_config_value}

    profile_config = TestProfileMapping(
        conn_id="fake_conn_id",
        dbt_config_vars=DbtProfileConfigVars(**dbt_config_vars),
    )
    assert profile_config.dbt_config_vars.as_dict() == dbt_config_vars


@pytest.mark.parametrize(
    "dbt_config_var,dbt_config_value",
    [("log_format", "text2")],
)
def test_profile_config_validate_dbt_config_vars_check_values(dbt_config_var: str, dbt_config_value: Any):
    dbt_config_vars = {dbt_config_var: dbt_config_value}

    with pytest.raises(ValueError):
        TestProfileMapping(
            conn_id="fake_conn_id",
            dbt_config_vars=DbtProfileConfigVars(**dbt_config_vars),
        )


def test_profile_version_sha_consistency():
    profile_mapping = TestProfileMapping(conn_id="fake_conn_id")
    version = profile_mapping.version(profile_name="dev", target_name="dev")
    assert version == "ea3bf1f70b033405ba9ff9cafe65af873fd7a868cac840cdbfd5e8e9a1da9650"
