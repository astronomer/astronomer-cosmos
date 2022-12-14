from typing import Optional

import os


def validate_directory(
    path: str,
    param_name: Optional[str],
):
    """
    Validates that the given path exists and is a directory. If not, raises a
    ValueError.
    """
    if not param_name:
        param_name = "Path"

    if not path:
        raise ValueError(f"{param_name} must be provided.")

    if not os.path.exists(path):
        raise ValueError(f"{param_name} {path} does not exist.")

    if not os.path.isdir(path):
        raise ValueError(f"{param_name} {path} is not a directory.")
