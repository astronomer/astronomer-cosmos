"""Pytest plugin for the dbt v2 (dbt-oss) integration job, ``.github/workflows/test-dbt-v2.yml``.

dbt v2 does not support the Postgres adapter the integration tests are written for, so that job runs them against
BigQuery. It loads this plugin through ``PYTEST_PLUGINS=tests.dbt_v2_plugin``, so no other job imports it: every
``PostgresUserPasswordProfileMapping`` built by the tests and the example DAGs becomes a BigQuery mapping that writes
to ``COSMOS_TEST_BIGQUERY_DATASET``, and the tests that need Postgres itself are skipped.

Which tests and example DAGs run is controlled by ``tests/dbt_v2_enabled.py``.
"""

from __future__ import annotations

import os

import pytest

from tests.dbt_v2_enabled import ENABLED_TEST_FILES, SKIPPED_TESTS

# Tests that depend on Postgres itself (and not only on the profile) and so cannot run against BigQuery.
POSTGRES_ONLY_TEST_FILES = {
    "tests/listeners/test_dag_run_listener.py": "asserts on the Postgres profile mapping",
    "tests/operators/test_virtualenv.py": "installs dbt-postgres in a virtualenv",
}
POSTGRES_ONLY_FIXTURES = {
    "reset_fail_once_sequence": "uses psycopg2 and Postgres sequences",
}


def _use_bigquery_in_place_of_postgres() -> None:
    import cosmos.profiles
    import cosmos.profiles.postgres
    from cosmos.profiles.bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping

    class BigQueryInPlaceOfPostgresProfileMapping(GoogleCloudServiceAccountDictProfileMapping):
        def __init__(self, conn_id, profile_args=None, disable_event_tracking=None, dbt_config_vars=None):
            bigquery_profile_args = {k: v for k, v in (profile_args or {}).items() if k != "schema"}
            bigquery_profile_args["dataset"] = os.environ["COSMOS_TEST_BIGQUERY_DATASET"]
            bigquery_profile_args["project"] = os.getenv("COSMOS_TEST_BIGQUERY_PROJECT", "astronomer-dag-authoring")
            super().__init__(
                conn_id="gcp_gs_conn",
                profile_args=bigquery_profile_args,
                disable_event_tracking=disable_event_tracking,
                dbt_config_vars=dbt_config_vars,
            )

    cosmos.profiles.PostgresUserPasswordProfileMapping = BigQueryInPlaceOfPostgresProfileMapping
    cosmos.profiles.postgres.PostgresUserPasswordProfileMapping = BigQueryInPlaceOfPostgresProfileMapping


_use_bigquery_in_place_of_postgres()


def pytest_collection_modifyitems(items):
    for item in items:
        file_path = item.nodeid.split("::")[0]
        reason = POSTGRES_ONLY_TEST_FILES.get(file_path) or next(
            (why for fixture, why in POSTGRES_ONLY_FIXTURES.items() if fixture in item.fixturenames), None
        )
        if reason:
            reason = f"Requires Postgres: {reason}"
        elif item.nodeid in SKIPPED_TESTS:
            reason = f"Not working with dbt v2 yet: {SKIPPED_TESTS[item.nodeid]}"
        elif ENABLED_TEST_FILES is not None and file_path not in ENABLED_TEST_FILES:
            reason = "Not enabled for dbt v2 yet (see tests/dbt_v2_enabled.py)"
        if reason:
            item.add_marker(pytest.mark.skip(reason=reason))
