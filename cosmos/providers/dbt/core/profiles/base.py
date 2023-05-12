"""
This module contains a base class that other profile mappings should
inherit from to ensure consistency.
"""
from __future__ import annotations

from typing import Any

import yaml
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection


class InvalidMappingException(Exception):
    "Raised when a connection is not valid for a profile mapping."
    pass


class BaseProfileMapping:
    """
    A base class that other profile mappings should inherit from to ensure consistency.
    Responsible for mapping Airflow connections to dbt profiles.
    """

    connection_type: str = "generic"

    def __init__(self, conn: Connection, profile_args: dict[str, Any] = {}):
        self.conn = conn
        self.profile_args = profile_args

        if not self.validate_connection():
            raise InvalidMappingException

    def validate_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.
        """
        return self.conn.conn_type == self.connection_type

    def get_profile(self) -> dict[str, Any]:
        """
        Return a dbt profile based on the Airflow connection.
        """
        raise NotImplementedError

    def get_env_vars(self) -> dict[str, str]:
        """
        Return a dictionary of environment variables that should be set.
        """
        raise NotImplementedError

    def get_profile_file_contents(
        self, profile_name: str, target_name: str = "cosmos_target"
    ):
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

        return yaml.dump(profile_contents)

    @property
    def database(self) -> str:
        """
        In most cases, Airflow uses the `schema` field for the database name.
        Exceptions to this are handled by the profile mapping subclasses.
        """
        return str(self.conn.schema)

    @property
    def schema(self) -> str:
        """
        In most cases, Airflow connections don't have a `schema` field.
        Exceptions to this are handled by the profile mapping subclasses.
        """
        schema = self.profile_args.get("schema")
        if not schema:
            raise Exception("You must provide a schema in the profile_args.")

        return schema

    @classmethod
    def get_env_var_name(cls, field_name: str) -> str:
        """
        Return the name of an environment variable.
        """
        return f"COSMOS_CONN_{cls.connection_type.upper()}_{field_name.upper()}"

    @classmethod
    def get_env_var_format(cls, field_name: str) -> str:
        """
        Return the format for an environment variable name.
        """
        env_var_name = cls.get_env_var_name(field_name)
        # need to double the brackets to escape them in the template
        return f"{{{{ env_var('{env_var_name}') }}}}"
