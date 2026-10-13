import os

from airflow.hooks.base import BaseHook
from google.cloud import bigquery
from google.oauth2 import service_account

DATASET_PREFIX = "cosmos_ci_"


def delete_bigquery_dataset() -> None:
    """
    Delete the BigQuery dataset (and its tables) that a CI job wrote to.

    The dataset name is read from COSMOS_TEST_BIGQUERY_DATASET and must start with ``cosmos_ci_``.
    The credentials come from the Airflow connection ``gcp_gs_conn``.
    """
    dataset = os.environ["COSMOS_TEST_BIGQUERY_DATASET"]
    if not dataset.startswith(DATASET_PREFIX):
        raise ValueError(f"Refusing to delete dataset {dataset!r}: it does not start with {DATASET_PREFIX!r}")

    extra = BaseHook.get_connection("gcp_gs_conn").extra_dejson
    keyfile_dict = extra.get("keyfile_dict") or extra.get("extra__google_cloud_platform__keyfile_dict")
    if isinstance(keyfile_dict, str):
        import json

        keyfile_dict = json.loads(keyfile_dict)
    project = os.getenv("COSMOS_TEST_BIGQUERY_PROJECT", "astronomer-dag-authoring")

    credentials = service_account.Credentials.from_service_account_info(keyfile_dict)
    client = bigquery.Client(project=project, credentials=credentials)
    print(f"Deleting BigQuery dataset {project}.{dataset}")
    client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    delete_bigquery_dataset()
