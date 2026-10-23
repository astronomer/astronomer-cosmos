"""Pytest plugin for the dbt-oss integration job, ``.github/workflows/test-dbt-oss.yml``.

dbt-oss does not support the Postgres adapter the integration tests are written for, so that job runs them against
BigQuery. It loads this plugin through ``PYTEST_PLUGINS=tests.dbt_oss_plugin``, so no other job imports it: every
``PostgresUserPasswordProfileMapping`` built by the tests and the example DAGs becomes a BigQuery mapping that writes
to ``COSMOS_TEST_BIGQUERY_DATASET``, and the tests that need Postgres itself are skipped. The example projects that
have their own Postgres ``profiles.yml`` are covered by a second mechanism: the workflow rewrites those files to
BigQuery with ``scripts/test/bigquery_ci_profiles.py``, which this plugin cannot do because they are not Python mappings.
The tests in ``PER_TEST_DATASET_TESTS`` get a dataset of their own, named after the job's one, so they do not depend on
what another test left.

Which example DAGs run, and which tests are skipped for dbt-oss, is listed in ``tests/dbt_oss_enabled.py``.
"""

from __future__ import annotations

import os
import re

import pytest

from tests.dbt_oss_enabled import SKIPPED_TESTS

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
            bigquery_profile_args = dict(profile_args or {})
            bigquery_profile_args["dataset"] = os.environ["COSMOS_TEST_BIGQUERY_DATASET"]
            bigquery_profile_args["project"] = os.getenv("COSMOS_TEST_BIGQUERY_PROJECT", "astronomer-dag-authoring")
            super().__init__(
                conn_id="gcp_gs_conn",
                profile_args=bigquery_profile_args,
                disable_event_tracking=disable_event_tracking,
                dbt_config_vars=dbt_config_vars,
            )

        @property
        def profile(self):
            # BigQuery has no schema: the dataset is used instead. The caller's `schema` stays in `profile_args` so the
            # tests that read it still see it, but it never reaches the dbt profile. The dataset is read here, not in
            # __init__, because the DAGs are parsed before the per-test dataset is set.
            profile = {key: value for key, value in super().profile.items() if key != "schema"}
            profile["dataset"] = os.environ["COSMOS_TEST_BIGQUERY_DATASET"]
            return profile

    cosmos.profiles.PostgresUserPasswordProfileMapping = BigQueryInPlaceOfPostgresProfileMapping
    cosmos.profiles.postgres.PostgresUserPasswordProfileMapping = BigQueryInPlaceOfPostgresProfileMapping


_use_bigquery_in_place_of_postgres()

# Other example DAGs still share the job's dataset: some of them rely on tables that an earlier test seeded there.
PER_TEST_DATASET_TESTS = {"tests/test_example_dags.py::test_example_dag[example_multiple_parents_test]"}
_NON_WORD = re.compile(r"\W")


@pytest.fixture(autouse=True)
def per_test_bigquery_dataset(request, monkeypatch):
    """Give the tests in PER_TEST_DATASET_TESTS their own dataset, which dbt creates on first use.

    The job's cleanup step deletes every dataset that starts with the job's one. The sources of altered_jaffle_shop
    read POSTGRES_SCHEMA, so it follows the dataset.
    """
    if request.node.nodeid in PER_TEST_DATASET_TESTS:
        job_dataset = os.environ["COSMOS_TEST_BIGQUERY_DATASET"]
        dataset = f"{job_dataset}_{_NON_WORD.sub('_', request.node.name)}"
        monkeypatch.setenv("COSMOS_TEST_BIGQUERY_DATASET", dataset)
        monkeypatch.setenv("POSTGRES_SCHEMA", dataset)


def pytest_collection_modifyitems(items):
    for item in items:
        file_path = item.nodeid.split("::")[0]
        reason = POSTGRES_ONLY_TEST_FILES.get(file_path) or next(
            (why for fixture, why in POSTGRES_ONLY_FIXTURES.items() if fixture in item.fixturenames), None
        )
        if reason:
            reason = f"Requires Postgres: {reason}"
        elif item.nodeid in SKIPPED_TESTS:
            reason = f"Not working with dbt-oss yet: {SKIPPED_TESTS[item.nodeid]}"
        if reason:
            item.add_marker(pytest.mark.skip(reason=reason))
