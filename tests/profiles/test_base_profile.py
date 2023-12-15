import pytest
import yaml

from cosmos.profiles.base import BaseProfileMapping
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


@pytest.mark.parametrize("disable_event_tracking", [True, False])
def test_disable_event_tracking(disable_event_tracking: str):
    """
    Tests the config block in the profile is set correctly if disable_event_tracking is set.
    """
    test_profile = TestProfileMapping(
        conn_id="fake_conn_id",
        disable_event_tracking=disable_event_tracking,
    )
    profile_contents = yaml.safe_load(test_profile.get_profile_file_contents(profile_name="fake-profile-name"))

    assert ("config" in profile_contents) == disable_event_tracking
    if disable_event_tracking:
        assert profile_contents["config"]["send_anonymous_usage_stats"] == "False"
