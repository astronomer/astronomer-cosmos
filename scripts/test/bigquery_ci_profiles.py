"""Point the Postgres profiles of the example dbt projects at BigQuery, for the dbt-oss workflow.

Usage, from ``.github/workflows/test-dbt-oss.yml`` (``-I`` keeps the working directory and ``PYTHONPATH`` off ``sys.path``)::

    python -I scripts/test/bigquery_ci_profiles.py PROFILES_YML [PROFILES_YML ...]

dbt-oss has no Postgres adapter, so a test that reads a project's own ``profiles.yml`` cannot run with it, even with the
``POSTGRES_*`` variables set. Every output of ``type: postgres`` in the given files is replaced with a BigQuery output
that reads the project and the dataset from ``POSTGRES_DB`` and ``POSTGRES_SCHEMA`` (the variables the Postgres profiles
already read) and authenticates with the service account key of ``AIRFLOW_CONN_GCP_GS_CONN``, with the
``service-account-json`` method that Cosmos' own BigQuery profile mapping uses (the one the rest of the job already runs
with). The key ends up in the rewritten files, so run this on the CI runner only: the files are rewritten in place.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import yaml
from airflow.models.connection import Connection


def _keyfile() -> dict[str, Any]:
    raw = os.environ["AIRFLOW_CONN_GCP_GS_CONN"]
    conn = Connection.from_json(raw) if raw.lstrip().startswith("{") else Connection(uri=raw)
    extra = conn.extra_dejson
    keyfile = extra.get("keyfile_dict") or extra.get("extra__google_cloud_platform__keyfile_dict")
    if isinstance(keyfile, str):
        keyfile = json.loads(keyfile)
    if not keyfile:
        raise SystemExit("AIRFLOW_CONN_GCP_GS_CONN has no keyfile_dict")
    return dict(keyfile)


def _bigquery_output(keyfile: dict[str, Any], threads: Any) -> dict[str, Any]:
    return {
        "type": "bigquery",
        "method": "service-account-json",
        "keyfile_json": keyfile,
        "project": "{{ env_var('POSTGRES_DB') }}",
        "dataset": "{{ env_var('POSTGRES_SCHEMA') }}",
        "threads": threads,
    }


def rewrite_profiles(profiles_yml: Path, keyfile: dict[str, Any]) -> int:
    """Replace the Postgres outputs of ``profiles_yml`` with BigQuery ones; return how many were replaced."""
    profiles = yaml.safe_load(profiles_yml.read_text())
    replaced = 0
    for profile in profiles.values():
        for name, output in profile.get("outputs", {}).items():
            if output.get("type") == "postgres":
                profile["outputs"][name] = _bigquery_output(keyfile, output.get("threads", 1))
                replaced += 1
    profiles_yml.write_text(yaml.safe_dump(profiles, sort_keys=False))
    return replaced


def main(argv: list[str]) -> int:
    if not argv:
        raise SystemExit(__doc__)
    keyfile = _keyfile()
    for profiles_yml in map(Path, argv):
        print(f"{profiles_yml}: replaced {rewrite_profiles(profiles_yml, keyfile)} Postgres output(s) with BigQuery")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
