import json

from airflow.hooks.base import BaseHook
from google.cloud import bigquery
from google.oauth2 import service_account

DATASET_PREFIX = "cosmos_ci_"


def delete_bigquery_ci_dataset() -> None:
    """
    Delete the BigQuery dataset (and its tables) that ``set_bigquery_ci_connection.py`` configured for this CI job.

    The project and dataset are read from the ``gcp_gs_conn`` Airflow connection. Only datasets whose name starts
    with ``cosmos_ci_`` are deleted.
    """
    extra = BaseHook.get_connection("gcp_gs_conn").extra_dejson
    project, dataset = extra["project"], extra["dataset"]
    if not dataset.startswith(DATASET_PREFIX):
        raise ValueError(f"Refusing to delete dataset {dataset!r}: it does not start with {DATASET_PREFIX!r}")

    keyfile = extra.get("keyfile_dict") or extra.get("extra__google_cloud_platform__keyfile_dict")
    if isinstance(keyfile, str):
        keyfile = json.loads(keyfile)

    client = bigquery.Client(
        project=project, credentials=service_account.Credentials.from_service_account_info(keyfile)
    )
    print(f"Deleting BigQuery dataset {project}.{dataset}")
    client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    delete_bigquery_ci_dataset()
