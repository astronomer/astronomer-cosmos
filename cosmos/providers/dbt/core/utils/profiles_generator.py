import fcntl
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple

import pkg_resources
import yaml
from airflow.hooks.base import BaseHook

from cosmos.providers.dbt.core.profiles import get_available_adapters

logger = logging.getLogger(__name__)


def create_default_profiles(profile_path: Path) -> None:
    """
    Write all the available profiles out to the profile path.
    :param profile_path: The path location to write all the profiles to.
    :return: Nothing
    """
    # get installed version of astronomer-cosmos
    try:
        package = pkg_resources.get_distribution("astronomer-cosmos")
    except pkg_resources.DistributionNotFound:
        package = None
    profiles = {}
    for adapter_config in get_available_adapters().values():
        profiles[adapter_config.profile_name] = adapter_config.profile
    write_file = False
    package_comment_line = f"# {package}\n"
    if profile_path.exists():
        # check the version of cosmos when it was created
        with open(profile_path) as f:
            first_line = next(f)
        if first_line != package_comment_line:
            # if version of cosmos has been updated - re-write the profiles.yml file
            write_file = True
    else:
        write_file = True
        # make the parent dir
        profile_path.parent.mkdir(parents=True, exist_ok=True)
    if write_file:
        # if file doesn't exist - write the profiles.yml file
        with open(profile_path, "w") as file:
            fcntl.flock(file, fcntl.LOCK_SH)
            file.write(package_comment_line)
            yaml.dump(profiles, file)
            fcntl.flock(file, fcntl.LOCK_UN)


def map_profile(
    conn_id: str,
    db_override: Optional[str] = None,
    schema_override: Optional[str] = None,
) -> Tuple[str, dict]:
    conn = BaseHook().get_connection(conn_id)
    connection_type = conn.conn_type
    adapters = get_available_adapters()
    if connection_type in adapters:
        return adapters[connection_type].create_profile_function(
            conn, db_override, schema_override
        )

    logging.getLogger().setLevel(logging.ERROR)
    logging.error(f"This connection type is currently not supported {connection_type}.")
    sys.exit(1)
