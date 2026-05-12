from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

import pytest

import cosmos


class TestLazyImports:
    """Tests for the lazy import dispatch in cosmos/__init__.py."""

    def test_lazy_import_resolves_known_name(self):
        """Known names in _LAZY_IMPORTS are resolved on attribute access."""
        assert cosmos.DbtDag is not None
        assert cosmos.ExecutionMode is not None

    def test_optional_dependency_returns_missing_package(self):
        """When an optional-dep module fails to import, a MissingPackage sentinel is returned."""
        # Clear any cached value from a prior import
        cosmos.__dict__.pop("DbtBuildDockerOperator", None)

        with patch.dict(sys.modules, {"docker": None}):
            with patch("importlib.import_module", side_effect=ImportError("No module named 'docker'")):
                operator = cosmos.DbtBuildDockerOperator  # noqa: F841
                # MissingPackage is a callable that raises RuntimeError when invoked
                with pytest.raises(RuntimeError, match="optional dependencies"):
                    operator()

    def test_unknown_attribute_raises_attribute_error(self):
        """Accessing a non-existent attribute raises AttributeError."""
        with pytest.raises(AttributeError, match="has no attribute"):
            _ = cosmos.non_existent_attribute_xyz

    def test_submodule_access(self):
        """Submodule access via attribute (e.g. cosmos.settings) works."""
        assert cosmos.settings is not None

    def test_non_optional_import_error_propagates(self):
        """ImportError for a non-optional module re-raises instead of returning MissingPackage."""
        cosmos.__dict__.pop("DbtDag", None)

        def fake_import(name, *args, **kwargs):
            if name == "cosmos.airflow.dag":
                raise ImportError("broken import")
            return original_import(name, *args, **kwargs)

        original_import = importlib.import_module
        with patch("importlib.import_module", side_effect=fake_import):
            with pytest.raises(ImportError, match="broken import"):
                _ = cosmos.DbtDag

    def test_submodule_internal_import_error_propagates(self):
        """If an existing submodule has an internal ImportError, it must propagate."""

        def fake_import(name, *args, **kwargs):
            if name == "cosmos.settings":
                raise ModuleNotFoundError("No module named 'some_internal_dep'", name="some_internal_dep")
            return importlib.__import__(name, *args, **kwargs)

        with patch("importlib.import_module", side_effect=fake_import):
            # Clear cached submodule so __getattr__ is triggered
            cosmos.__dict__.pop("settings", None)
            with pytest.raises(ModuleNotFoundError, match="some_internal_dep"):
                _ = cosmos.settings
