import pytest
from cosmos.exceptions import CosmosValueError


from cosmos.converter import validate_arguments


@pytest.mark.parametrize("argument_key", ["tags", "paths"])
def test_validate_arguments_tags(argument_key):
    selector_name = argument_key[:-1]
    select = [f"{selector_name}:a,{selector_name}:b"]
    exclude = [f"{selector_name}:b,{selector_name}:c"]
    profile_args = {}
    task_args = {}
    with pytest.raises(CosmosValueError) as err:
        validate_arguments(select, exclude, profile_args, task_args)
    expected = f"Can't specify the same {selector_name} in `select` and `exclude`: {{'b'}}"
    assert err.value.args[0] == expected
