"""Class for setting profile config."""

from __future__ import annotations

import contextlib
import tempfile
from typing import Iterator
from dataclasses import dataclass
from pathlib import Path

from cosmos.profiles.base import BaseProfileMapping


@dataclass
class ProfileConfig:
    """
    Class for setting profile config. Supports two modes of operation:
    1. Using a user-supplied profiles.yml file. If using this mode, set path_to_profiles_yml to the
    path to the file.
    2. Using cosmos to map Airflow connections to dbt profiles. If using this mode, set profile_mapping
    to a subclass of BaseProfileMapping.

    :param profile_name: The name of the dbt profile to use.
    :param target_name: The name of the dbt target to use.
    :param path_to_profiles_yml: The path to a profiles.yml file to use.
    :param profile_mapping: A mapping of Airflow connections to dbt profiles.
    """

    # should always be set to be explicit
    profile_name: str
    target_name: str

    # should be set if using a user-supplied profiles.yml
    path_to_profiles_yml: str | None = None

    # should be set if using cosmos to map Airflow connections to dbt profiles
    profile_mapping: BaseProfileMapping | None = None

    def __post_init__(self) -> None:
        "Validates that we have enough information to render a profile."
        if not self.profile_mapping and not self.path_to_profiles_yml:
            raise ValueError("Either a profile_mapping or path_to_profiles_yml must be set")

        if self.profile_mapping and self.path_to_profiles_yml:
            raise ValueError("Only one of profile_mapping or path_to_profiles_yml can be set")

        # if using a user-supplied profiles.yml, validate that it exists
        if self.path_to_profiles_yml:
            profiles_path = Path(self.path_to_profiles_yml)
            if not profiles_path.exists():
                raise ValueError(f"Could not find profiles.yml at {self.path_to_profiles_yml}")

    @contextlib.contextmanager
    def ensure_profile(self) -> Iterator[str]:
        "Context manager to ensure that there is a profile. If not, create one."
        if self.path_to_profiles_yml:
            yield self.path_to_profiles_yml
        elif self.profile_mapping:
            profile_contents = self.profile_mapping.get_profile_file_contents(
                profile_name=self.profile_name, target_name=self.target_name
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".yml") as temp_file:
                temp_file.write(profile_contents)
                temp_file.flush()
                yield temp_file.name
