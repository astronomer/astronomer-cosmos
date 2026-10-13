import json
import os

from airflow.hooks.base import BaseHook

DEFAULT_PROJECT = "astronomer-dag-authoring"


def set_bigquery_ci_connection() -> None:
    """
    Add the BigQuery ``project`` and a dataset specific to this CI job to the ``gcp_gs_conn`` Airflow connection,
    so that ``GoogleCloudServiceAccountDictProfileMapping(conn_id="gcp_gs_conn")`` writes to that dataset.

    The updated connection is exported to the following steps of the job through ``GITHUB_ENV``.
    """
    conn = BaseHook.get_connection("gcp_gs_conn")
    extra = conn.extra_dejson
    extra["project"] = (
        os.getenv("COSMOS_TEST_BIGQUERY_PROJECT")
        or extra.get("project")
        or extra.get("extra__google_cloud_platform__project")
        or DEFAULT_PROJECT
    )
    extra["dataset"] = os.environ["COSMOS_TEST_BIGQUERY_DATASET"]

    keyfile = extra.get("keyfile_dict") or extra.get("extra__google_cloud_platform__keyfile_dict")
    if isinstance(keyfile, str):
        keyfile = json.loads(keyfile)
    for secret_field in ("private_key", "private_key_id"):
        if keyfile and keyfile.get(secret_field):
            # GitHub masks single lines, so mask the JSON-escaped form that appears in the connection string.
            print(f"::add-mask::{json.dumps(keyfile[secret_field])[1:-1]}")

    value = json.dumps({"conn_type": conn.conn_type, "extra": extra})
    with open(os.environ["GITHUB_ENV"], "a") as github_env:
        github_env.write(f"AIRFLOW_CONN_GCP_GS_CONN={value}\n")
    print(f"Configured gcp_gs_conn for BigQuery dataset {extra['project']}.{extra['dataset']}")


if __name__ == "__main__":
    set_bigquery_ci_connection()
