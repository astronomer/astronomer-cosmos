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

import sys
from pathlib import Path
from typing import Any

import yaml

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from bigquery_ci_common import keyfile

_keyfile = keyfile


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
    key = keyfile()
    for profiles_yml in map(Path, argv):
        print(f"{profiles_yml}: replaced {rewrite_profiles(profiles_yml, key)} Postgres output(s) with BigQuery")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
