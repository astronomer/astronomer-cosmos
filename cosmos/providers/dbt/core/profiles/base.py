"""
This module contains a base class that other profile mappings should
inherit from to ensure consistency.
"""
from __future__ import annotations

from logging import getLogger
from typing import Any

import yaml
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

logger = getLogger(__name__)


class BaseProfileMapping:
    """
    A base class that other profile mappings should inherit from to ensure consistency.
    Responsible for mapping Airflow connections to dbt profiles.
    """

    airflow_connection_type: str = "generic"
    is_community: bool = False

    required_fields: list[str] = []

    def __init__(self, conn: Connection, profile_args: dict[str, Any] | None = None):
        self.conn = conn
        self.profile_args = profile_args or {}

    def can_claim_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.
        """
        if self.conn.conn_type != self.airflow_connection_type:
            return False

        for field in self.required_fields:
            if not getattr(self, field):
                logger.info(
                    "Not using mapping %s because %s is not set",
                    self.__class__.__name__,
                    field,
                )
                return False

        return True

    def get_profile(self) -> dict[str, Any]:
        """
        Return a dbt profile based on the Airflow connection.
        """
        raise NotImplementedError

    def get_env_vars(self) -> dict[str, str]:
        """
        Return a dictionary of environment variables that should be set.
        """
        return {}

    def get_profile_file_contents(
        self, profile_name: str, target_name: str = "cosmos_target"
    ) -> str:
        """
        Translates the profile into a string that can be written to a profiles.yml file.
        """
        profile_vars = self.get_profile()

        # filter out any null values
        profile_vars = {k: v for k, v in profile_vars.items() if v is not None}

        profile_contents = {
            profile_name: {
                "target": target_name,
                "outputs": {target_name: profile_vars},
            }
        }

        return str(yaml.dump(profile_contents, indent=4))

    @classmethod
    def filter_null(cls, args: dict[str, Any]) -> dict[str, Any]:
        """
        Filters out null values from a dictionary.
        """
        return {k: v for k, v in args.items() if v is not None}

    @classmethod
    def get_env_var_name(cls, field_name: str) -> str:
        """
        Return the name of an environment variable.
        """
        return f"COSMOS_CONN_{cls.airflow_connection_type.upper()}_{field_name.upper()}"

    @classmethod
    def get_env_var_format(cls, field_name: str) -> str:
        """
        Return the format for an environment variable name.
        """
        env_var_name = cls.get_env_var_name(field_name)
        # need to double the brackets to escape them in the template
        return f"{{{{ env_var('{env_var_name}') }}}}"
