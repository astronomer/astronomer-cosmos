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

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from bigquery_ci_common import keyfile


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        raise SystemExit(__doc__)
    path = Path(argv[0])
    key = keyfile()
    # The key is a plain environment variable in the pods, and GitHub masks only the whole secret value, so mask the
    # parts of it that a log could print on their own. The runner consumes these commands and does not show them.
    for value in (key.get("private_key_id"), *str(key.get("private_key", "")).splitlines()):
        if value and value.strip():
            print(f"::add-mask::{value.strip()}")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as file:
        json.dump(key, file)
    print(f"{path}: wrote the service account key")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
