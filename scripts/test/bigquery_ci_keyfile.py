"""Write the service account key of ``AIRFLOW_CONN_GCP_GS_CONN`` to a file that only the current user can read.

Usage, from ``.github/workflows/test-dbt-oss.yml`` (``-I`` keeps the working directory and ``PYTHONPATH`` off ``sys.path``)::

    python -I scripts/test/bigquery_ci_keyfile.py KEYFILE

The Kubernetes job stores the file in a Kubernetes secret with ``kubectl create secret --from-file``, so the key is never
on a command line or in the output of a step. Run this on the CI runner only.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from airflow.models.connection import Connection


def keyfile() -> dict:
    raw = os.environ["AIRFLOW_CONN_GCP_GS_CONN"]
    conn = Connection.from_json(raw) if raw.lstrip().startswith("{") else Connection(uri=raw)
    extra = conn.extra_dejson
    key = extra.get("keyfile_dict") or extra.get("extra__google_cloud_platform__keyfile_dict")
    if isinstance(key, str):
        key = json.loads(key)
    if not key:
        raise SystemExit("AIRFLOW_CONN_GCP_GS_CONN has no keyfile_dict")
    return dict(key)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        raise SystemExit(__doc__)
    path = Path(argv[0])
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as file:
        json.dump(keyfile(), file)
    print(f"{path}: wrote the service account key")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
