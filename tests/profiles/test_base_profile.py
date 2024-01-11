from __future__ import annotations

from typing import Any

import pytest
import yaml

from cosmos.profiles.base import BaseProfileMapping, DbtConfigVars
from cosmos.exceptions import CosmosValueError


class TestProfileMapping(BaseProfileMapping):
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


@pytest.mark.parametrize("dbt_config_var,dbt_config_value", [("debug", True), ("debug", False)])
def test_validate_dbt_config_vars(dbt_config_var: str, dbt_config_value: Any):
    """
    Tests the config block in the profile is set correctly.
    """
    dbt_config_vars = {dbt_config_var: dbt_config_value}
    test_profile = TestProfileMapping(
        conn_id="fake_conn_id",
        dbt_config_vars=DbtConfigVars(**dbt_config_vars),
    )
    profile_contents = yaml.safe_load(test_profile.get_profile_file_contents(profile_name="fake-profile-name"))

    assert "config" in profile_contents
    assert profile_contents["config"] == dbt_config_vars


def test_profile_config_validate_dbt_config_vars_empty():
    test_profile = TestProfileMapping(
        conn_id="fake_conn_id",
        dbt_config_vars=None,
    )
    assert test_profile.dbt_config_vars is None


def test_profile_config_validate_dbt_config_vars_check_unexpected_var():
    dbt_config_var = "unexpected_var"
    dbt_config_value = True
    dbt_config_vars = {dbt_config_var: dbt_config_value}

    with pytest.raises(CosmosValueError) as err_info:
        TestProfileMapping(
            conn_id="fake_conn_id",
            dbt_config_vars=DbtConfigVars(**dbt_config_vars),
        )
    assert err_info.value.args[0] == f"dbt config var {dbt_config_var}: {dbt_config_value} is not supported"


@pytest.mark.parametrize(
    "dbt_config_var,dbt_config_value",
    [("send_anonymous_usage_stats", 2), ("send_anonymous_usage_stats", None)],
)
def test_profile_config_validate_dbt_config_vars_check_unexpected_types(dbt_config_var: str, dbt_config_value: Any):
    dbt_config_vars = {dbt_config_var: dbt_config_value}

    with pytest.raises(CosmosValueError) as err_info:
        TestProfileMapping(
            conn_id="fake_conn_id",
            dbt_config_vars=DbtConfigVars(**dbt_config_vars),
        )
    assert err_info.value.args[0].startswith(f"dbt config var {dbt_config_var}: {dbt_config_value} must be a ")


@pytest.mark.parametrize("dbt_config_var,dbt_config_value", [("send_anonymous_usage_stats", True)])
def test_profile_config_validate_dbt_config_vars_check_expected_types(dbt_config_var: str, dbt_config_value: Any):
    dbt_config_vars = {dbt_config_var: dbt_config_value}

    profile_config = TestProfileMapping(
        conn_id="fake_conn_id",
        dbt_config_vars=DbtConfigVars(**dbt_config_vars),
    )
    assert profile_config.dbt_config_vars == dbt_config_vars


@pytest.mark.parametrize(
    "dbt_config_var,dbt_config_value",
    [("log_format", "text2")],
)
def test_profile_config_validate_dbt_config_vars_check_values(dbt_config_var: str, dbt_config_value: Any):
    dbt_config_vars = {dbt_config_var: dbt_config_value}

    with pytest.raises(CosmosValueError) as err_info:
        TestProfileMapping(
            conn_id="fake_conn_id",
            dbt_config_vars=DbtConfigVars(**dbt_config_vars),
        )
    assert err_info.value.args[0].startswith(f"dbt config var {dbt_config_var}: {dbt_config_value} must be one of ")
