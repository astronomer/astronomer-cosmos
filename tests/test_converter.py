import pytest
from airflow.exceptions import AirflowException


from cosmos.constants import TestBehavior
from cosmos.converter import convert_value_to_enum, validate_arguments, UserInputError


def test_convert_value_to_enum():
    assert convert_value_to_enum(TestBehavior.AFTER_ALL, TestBehavior) == TestBehavior.AFTER_ALL
    assert convert_value_to_enum("none", TestBehavior) == TestBehavior.NONE
    with pytest.raises(UserInputError) as err:
        convert_value_to_enum("unavailable", TestBehavior)
    expected = "The given value unavailable is not compatible with the type TestBehavior"
    assert err.value.args[0] == expected


@pytest.mark.parametrize("argument_key", ["tags", "paths"])
def test_validate_arguments_tags(argument_key):
    selector_name = argument_key[:-1]
    select = [f"{selector_name}:a,{selector_name}:b"]
    exclude = [f"{selector_name}:b,{selector_name}:c"]
    profile_args = {}
    task_args = {}
    with pytest.raises(AirflowException) as err:
        validate_arguments(select, exclude, profile_args, task_args)
    expected = f"Can't specify the same {selector_name} in `select` and `exclude`: {{'b'}}"
    assert err.value.args[0] == expected
