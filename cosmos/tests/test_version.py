"""
Tests related to the package version.
"""

import re

import cosmos


def test_version() -> None:
    """
    Test that the version is set correctly.
    """
    assert cosmos.__version__ == "0.6.2"


def test_version_format() -> None:
    """
    Test that the version is set correctly.
    """
    assert re.match(r"\d+\.\d+\.\d+", cosmos.__version__)
