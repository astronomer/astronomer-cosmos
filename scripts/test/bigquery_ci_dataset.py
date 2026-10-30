"""Create or delete the BigQuery dataset that one job of the dbt-oss workflow writes to.

Usage, from ``.github/workflows/test-dbt-oss.yml`` (``-I`` keeps the working directory and ``PYTHONPATH`` off ``sys.path``)::

    python -I scripts/test/bigquery_ci_dataset.py create
    python -I scripts/test/bigquery_ci_dataset.py delete

Environment: ``COSMOS_TEST_BIGQUERY_DATASET`` (must start with ``cosmos_ci_``), ``BIGQUERY_PROJECT`` and
``AIRFLOW_CONN_GCP_GS_CONN``, an Airflow connection with the service account key. ``GITHUB_RUN_ID`` is added as a label.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from pathlib import Path

import requests
from google import auth
from google.api_core import exceptions, retry
from google.cloud import bigquery
from google.oauth2 import service_account

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from bigquery_ci_common import keyfile

DATASET_PREFIX = "cosmos_ci_"
# Tables the job forgets to clean up (for example when the runner dies) expire on their own.
TABLE_EXPIRATION_MS = 6 * 60 * 60 * 1000
# Dataset operations are rate limited per project, and a run starts many jobs at once: spread the first call out.
MAX_START_JITTER_SECONDS = 20
VISIBLE_AFTER_CREATE_SECONDS = 60

_RETRYABLE_REASONS = {"rateLimitExceeded", "backendError", "internalError"}


def _is_transient(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            exceptions.TooManyRequests,
            exceptions.InternalServerError,
            exceptions.BadGateway,
            exceptions.ServiceUnavailable,
            exceptions.GatewayTimeout,
        ),
    ):
        return True
    # Passing ``retry=`` replaces the library's default retry, which also retries a dropped connection.
    if isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            auth.exceptions.TransportError,
            ConnectionResetError,
        ),
    ):
        return True
    # BigQuery reports its rate limits as 403 rateLimitExceeded.
    return isinstance(exc, exceptions.Forbidden) and any(
        error.get("reason") in _RETRYABLE_REASONS for error in (exc.errors or [])
    )


RETRY = retry.Retry(predicate=_is_transient, initial=2, maximum=60, multiplier=2, timeout=300)


def _dataset_name() -> str:
    dataset = os.environ.get("COSMOS_TEST_BIGQUERY_DATASET")
    if not dataset:
        raise SystemExit("COSMOS_TEST_BIGQUERY_DATASET is not set")
    if not dataset.startswith(DATASET_PREFIX):
        raise SystemExit(f"Refusing to touch dataset {dataset!r}: it does not start with {DATASET_PREFIX!r}")
    return dataset


def _client() -> bigquery.Client:
    key = keyfile()
    credentials = service_account.Credentials.from_service_account_info(key)
    return bigquery.Client(project=os.environ["BIGQUERY_PROJECT"], credentials=credentials)


def create(dataset: str, max_start_jitter: float = MAX_START_JITTER_SECONDS) -> None:
    client = _client()
    time.sleep(random.uniform(0, max_start_jitter))
    new_dataset = bigquery.Dataset(f"{client.project}.{dataset}")
    new_dataset.default_table_expiration_ms = TABLE_EXPIRATION_MS
    new_dataset.labels = {
        "ci": "cosmos",
        "workflow": "test-dbt-oss",
        "run-id": os.environ.get("GITHUB_RUN_ID", "local"),
    }
    print(f"Creating BigQuery dataset {dataset}")
    client.create_dataset(new_dataset, exists_ok=True, retry=RETRY)
    # dbt would create a missing dataset itself, but BigQuery does not always make it visible straight away, which made
    # the first seed fail with "Not found: Dataset".
    deadline = time.monotonic() + VISIBLE_AFTER_CREATE_SECONDS
    while True:
        try:
            client.get_dataset(dataset)
            return
        except exceptions.NotFound:
            if time.monotonic() > deadline:
                raise SystemExit(
                    f"BigQuery dataset {dataset} is still not visible after {VISIBLE_AFTER_CREATE_SECONDS} seconds"
                )
            time.sleep(2)


_API_FAILURES = (
    exceptions.GoogleAPICallError,
    exceptions.RetryError,
    requests.exceptions.RequestException,
    auth.exceptions.GoogleAuthError,
)


def _delete_quietly(client: bigquery.Client, dataset: str) -> None:
    print(f"Deleting BigQuery dataset {dataset}")
    try:
        client.delete_dataset(dataset, delete_contents=True, not_found_ok=True, retry=RETRY)
    except _API_FAILURES as exc:  # the job's result must not depend on cleanup
        print(f"::warning::Could not delete BigQuery dataset {dataset}: {exc}")


def delete(dataset: str) -> None:
    """Delete the dataset, then the per-test ones created for it (``<dataset>_<test>``, labelled with this run's id).

    A failure to reach the API is a warning, not an error: the tables of the datasets expire on their own.
    """
    # A bad connection is a setup mistake and fails the step; only the API calls, including a network or token
    # failure while making them, are allowed to fail quietly.
    client = _client()
    _delete_quietly(client, dataset)
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    try:
        per_test = [
            d.dataset_id
            for d in client.list_datasets(filter=f"labels.run-id:{run_id}", retry=RETRY)
            if d.dataset_id.startswith(f"{dataset}_")
        ]
    except _API_FAILURES as exc:
        print(f"::warning::Could not list the per-test BigQuery datasets of {dataset}: {exc}")
        return
    for name in per_test:
        _delete_quietly(client, name)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["create", "delete"])
    parser.add_argument("--max-start-jitter", type=float, default=MAX_START_JITTER_SECONDS)
    args = parser.parse_args(argv)
    dataset = _dataset_name()
    if args.action == "create":
        create(dataset, args.max_start_jitter)
    else:
        delete(dataset)
    return 0


if __name__ == "__main__":
    sys.exit(main())
