"""Delete the BigQuery datasets that CI jobs leave behind (``cosmos_ci_*``) once they are older than a given age.

Each CI job deletes its own dataset when it finishes. This is the safety net for the jobs that could not, such as a
timeout or a runner crash. It runs only from the default branch, by the scheduled ``cleanup-bigquery-ci-datasets``
workflow.
"""

import datetime
import json
import os
from urllib.parse import parse_qsl, urlsplit

from google.cloud import bigquery
from google.oauth2 import service_account

DATASET_PREFIX = "cosmos_ci_"
MAX_AGE = datetime.timedelta(hours=int(os.getenv("MAX_AGE_HOURS", "24")))
KEYFILE_FIELDS = ("keyfile_dict", "extra__google_cloud_platform__keyfile_dict")


def _keyfile_from_connection(raw: str) -> dict:
    """Read the service account key from an Airflow connection given as JSON or as a URI."""
    if raw.lstrip().startswith("{"):
        extra = json.loads(raw).get("extra") or {}
    else:
        params = dict(parse_qsl(urlsplit(raw).query))
        extra = json.loads(params["__extra__"]) if "__extra__" in params else params
    if isinstance(extra, str):
        extra = json.loads(extra)
    keyfile = next((extra[field] for field in KEYFILE_FIELDS if extra.get(field)), None)
    return json.loads(keyfile) if isinstance(keyfile, str) else keyfile


def delete_stale_datasets() -> None:
    project = os.environ["BIGQUERY_PROJECT"]
    credentials = service_account.Credentials.from_service_account_info(
        _keyfile_from_connection(os.environ["AIRFLOW_CONN_GCP_GS_CONN"])
    )
    client = bigquery.Client(project=project, credentials=credentials)
    now = datetime.datetime.now(datetime.timezone.utc)

    for item in client.list_datasets():
        if not item.dataset_id.startswith(DATASET_PREFIX):
            continue
        created = client.get_dataset(item.reference).created
        if created is None or now - created < MAX_AGE:
            continue
        print(f"Deleting stale BigQuery dataset {project}.{item.dataset_id} (created {created.isoformat()})")
        client.delete_dataset(item.reference, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    delete_stale_datasets()
