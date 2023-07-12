import pytest
from airflow.exceptions import AirflowException

from cosmos.converter import validate_arguments


@pytest.mark.parametrize("argument_key", ["tags", "paths"])
def test_validate_arguments_tags(argument_key):
    select = {argument_key: ["a", "b"]}
    exclude = {argument_key: ["b", "c"]}
    profile_args = {}
    task_args = {}
    with pytest.raises(AirflowException) as err:
        validate_arguments(select, exclude, profile_args, task_args)
    expected = f"Can't specify the same {argument_key[:-1]} in `select` and `include`: {{'b'}}"
    assert err.value.args[0] == expected
