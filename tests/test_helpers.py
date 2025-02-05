import sys
import types

import pytest

from cosmos.helpers import load_method_from_module

dummy_module_text = """
def dummy_method():
    return "Hello from dummy_method"
"""


@pytest.fixture
def create_dummy_module():
    """Creates a temporary in-memory module with a dummy method."""
    module_name = "test_dummy_module"
    method_name = "dummy_method"

    module = types.ModuleType(module_name)
    exec(dummy_module_text, module.__dict__)

    sys.modules[module_name] = module

    yield module_name, method_name

    del sys.modules[module_name]


def test_load_valid_method(create_dummy_module):
    """Test that a valid method is loaded successfully."""
    module_name, method_name = create_dummy_module
    method = load_method_from_module(module_name, method_name)
    assert callable(method)
    assert method() == "Hello from dummy_method"


def test_load_invalid_module():
    """Test that ModuleNotFoundError is raised for an invalid module."""
    with pytest.raises(ModuleNotFoundError, match="Module invalid_module not found"):
        load_method_from_module("invalid_module", "dummy_method")


def test_load_invalid_method(create_dummy_module):
    """Test that AttributeError is raised for a missing method in a valid module."""
    module_name, _ = create_dummy_module
    with pytest.raises(AttributeError, match=f"Method invalid_method not found in module {module_name}"):
        load_method_from_module(module_name, "invalid_method")
