import pytest
from cosmos.profiles.base import BaseProfileMapping
from cosmos.exceptions import CosmosValueError


class TestProfileMapping(BaseProfileMapping):
    dbt_profile_method: str = "fake-method"
    dbt_profile_type: str = "fake-type"

    def profile(self):
        raise NotImplementedError


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
