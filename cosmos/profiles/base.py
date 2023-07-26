"""
This module contains a base class that other profile mappings should
inherit from to ensure consistency.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any

from typing import TYPE_CHECKING
import yaml

from airflow.hooks.base import BaseHook
from cosmos.exceptions import CosmosValueError

if TYPE_CHECKING:
    from airflow.models import Connection

logger = getLogger(__name__)


class BaseProfileMapping(ABC):
    """
    A base class that other profile mappings should inherit from to ensure consistency.
    Responsible for mapping Airflow connections to dbt profiles.
    """

    airflow_connection_type: str = "generic"
    is_community: bool = False

    required_fields: list[str] = []
    secret_fields: list[str] = []
    airflow_param_mapping: dict[str, str | list[str]] = {}

    _conn: Connection | None = None

    def __init__(self, conn_id: str, profile_args: dict[str, Any] | None = None):
        self.conn_id = conn_id
        self.profile_args = profile_args or {}

    @property
    def conn(self) -> Connection:
        "Returns the Airflow connection."
        if not self._conn:
            conn = BaseHook.get_connection(self.conn_id)
            if not conn:
                raise CosmosValueError(f"Could not find connection {self.conn_id}.")

            self._conn = conn

        return self._conn

    def can_claim_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.
        """
        if self.conn.conn_type != self.airflow_connection_type:
            return False

        logger.info(dir(self.conn))
        logger.info(self.conn.__dict__)

        for field in self.required_fields:
            try:
                if not getattr(self, field):
                    logger.info(
                        "1 Not using mapping %s because %s is not set",
                        self.__class__.__name__,
                        field,
                    )
                    return False
            except AttributeError:
                logger.info(
                    "2 Not using mapping %s because %s is not set",
                    self.__class__.__name__,
                    field,
                )
                return False

        return True

    @property
    @abstractmethod
    def profile(self) -> dict[str, Any]:
        """
        Return a dbt profile based on the Airflow connection.
        """
        raise NotImplementedError

    @property
    def env_vars(self) -> dict[str, str]:
        "Returns a dictionary of environment variables that should be set based on self.secret_fields."
        env_vars = {}

        for field in self.secret_fields:
            env_var_name = self.get_env_var_name(field)
            value = self.get_dbt_value(field)
            if value is not None:
                env_vars[env_var_name] = str(value)

        return env_vars

    def get_profile_file_contents(self, profile_name: str, target_name: str = "cosmos_target") -> str:
        """
        Translates the profile into a string that can be written to a profiles.yml file.
        """
        profile_vars = self.profile

        # filter out any null values
        profile_vars = {k: v for k, v in profile_vars.items() if v is not None}

        profile_contents = {
            profile_name: {
                "target": target_name,
                "outputs": {target_name: profile_vars},
            }
        }
        return str(yaml.dump(profile_contents, indent=4))

    def get_dbt_value(self, name: str) -> Any:
        """
        Gets values for the dbt profile based on the required_by_dbt and required_in_profile_args lists.
        Precedence is:
        1. profile_args
        2. conn
        """
        # if it's in profile_args, return that
        if self.profile_args.get(name):
            return self.profile_args[name]

        # if it's has an entry in airflow_param_mapping, we can get it from conn
        if name in self.airflow_param_mapping:
            airflow_fields = self.airflow_param_mapping[name]

            if isinstance(airflow_fields, str):
                airflow_fields = [airflow_fields]

            for airflow_field in airflow_fields:
                # make sure there's no "extra." prefix
                if airflow_field.startswith("extra."):
                    airflow_field = airflow_field.replace("extra.", "", 1)
                    value = self.conn.extra_dejson.get(airflow_field)
                else:
                    value = getattr(self.conn, airflow_field)

                if not value:
                    continue

                # if there's a transform method, use it
                if hasattr(self, f"transform_{name}"):
                    return getattr(self, f"transform_{name}")(value)

                return value

        # otherwise, we don't have it - return None
        return None

    def __getattr__(self, name: str) -> Any:
        "If the attribute doesn't exist, try to get it from profile_args or the Airflow connection."
        attempted_value = self.get_dbt_value(name)
        if attempted_value is not None:
            return attempted_value

        raise AttributeError

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
