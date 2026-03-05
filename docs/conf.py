import os
import sys

# Add the project root to the path so we can import the package
sys.path.insert(0, os.path.abspath("../"))

from docs.generate_mappings import generate_mapping_docs  # noqa: E402

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Astronomer Cosmos"
copyright = "2023, Astronomer"
author = "Astronomer"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    # "autoapi.extension",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_reredirects",
]

add_module_names = False
autodoc_mock_imports = ["airflow"]
autoapi_dirs = ["../cosmos"]
autoapi_ignore = ["*/tests/*"]
templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "**/tests/*"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = [
    "css/custom.css",
]
html_theme_options = {
    "logo": {
        "image_light": "cosmos-icon.svg",
        "image_dark": "cosmos-icon.svg",
    },
    "footer_start": ["copyright"],
}

generate_mapping_docs()

# -- Begin docs redirect section
# -- To test redirects in a local build, paste the redirect source, and append .html to the end.
# -- For example, "airflow3_compatibility/index" redirect must be tested using "airflow3_compatibility/index.html"
# --  https://documatt.com/sphinx-reredirects/usage/
redirects = {
    "configuration/caching": "../optimize_performance/caching.html",
    "configuration/cosmos-conf": "../reference/configs/cosmos-conf.html",
    "configuration/execution-config": "../reference/configs/execution-config.html",
    "configuration/memory_optimization": "../optimize_performance/memory_optimization.html",
    "configuration/partial-parsing": "../optimize_performance/partial-parsing.html",
    "configuration/profile-config": "../reference/configs/profile-config.html",
    "configuration/project-config": "../reference/configs/project-config.html",
    "configuration/selecting-excluding": "../optimize_performance/selecting-excluding.html",
    "getting_started/async-execution-mode": "../guides/run_dbt/airflow-worker/async-execution-mode.html",
    "getting_started/aws-container-run-job": "../guides/run_dbt/airflow-worker/async-execution-mode.html",
    "getting_started/azure-container-instance": "../guides/run_dbt/container/azure-container-instance.html",
    "getting_started/custom-airflow-properties": "../run_dbt/airflow-worker/custom-airflow-properties.html",
    "getting_started/docker": "../guides/run_dbt/container/docker.html",
    "getting_started/execution-modes-local-conflicts": "../guides/run_dbt/airflow-worker/execution-modes-local-conflicts.html",
    "getting_started/execution-modes": "../guides/run_dbt/execution-modes.html",
    "getting_started/gcp-cloud-run-job": "../guides/run_dbt/container/gcp-cloud-run-job.html",
    "getting_started/kubernetes": "../guides/run_dbt/container/kubernetes.html",
    "getting_started/operators": "../guides/run_dbt/operators/operators.html",
    "getting_started/watcher-execution-mode": "../guides/run_dbt/airflow-worker/watcher-execution-mode.html",
    "getting_started/watcher-kubernetes-execution-mode": "../guides/run_dbt/container/watcher-kubernetes-execution-mode.html",
    "profiles/AthenaAccessKey": "../reference/profiles/AthenaAccessKey.html",
    "profiles/ClickhouseUserPassword": "../reference/profiles/ClickhouseUserPassword.html",
    "profiles/DatabricksOauth": "../reference/profiles/DatabricksOauth.html",
    "profiles/DatabricksToken": "../reference/profiles/DatabricksToken.html",
    "profiles/DuckDBUserPassword": "../reference/profiles/DuckDBUserPassword.html",
    "profiles/ExasolUserPassword": "../reference/profiles/ExasolUserPassword.html",
    "profiles/GoogleCloudOauth": "../reference/profiles/GoogleCloudOauth.html",
    "profiles/GoogleCloudServiceAccountDict": "../reference/profiles/GoogleCloudServiceAccountDict.html",
    "profiles/GoogleCloudServiceAccountFile": "../reference/profiles/GoogleCloudServiceAccountFile.html",
    "profiles/index": "../reference/profiles/index.html",
    "profiles/MysqlUserPassword": "../reference/profiles/MysqlUserPassword.html",
    "profiles/OracleUserPassword": "../reference/profiles/OracleUserPassword.html",
    "profiles/PostgresUserPassword": "../reference/profiles/PostgresUserPassword.html",
    "profiles/RedshiftUserPassword": "../reference/profiles/RedshiftUserPassword.html",
    "profiles/SnowflakeEncryptedPrivateKeyFilePem": "../reference/profiles/SnowflakeEncryptedPrivateKeyFilePem.html",
    "profiles/SnowflakeEncryptedPrivateKeyPem": "../reference/profiles/SnowflakeEncryptedPrivateKeyPem.html",
    "profiles/SnowflakePrivateKeyPem": "../reference/profiles/SnowflakePrivateKeyPem.html",
    "profiles/SnowflakeUserPassword": "../reference/profiles/SnowflakeUserPassword.html",
    "profiles/SparkThrift": "../reference/profiles/SparkThrift.html",
    "profiles/StandardSQLServerAuth": "../reference/profiles/StandardSQLServerAuth.html",
    "profiles/StarrocksUserPassword": "../reference/profiles/StarrocksUserPassword.html",
    "profiles/TeradataUserPassword": "../reference/profiles/TeradataUserPassword.html",
    "profiles/TrinoCertificate": "../reference/profiles/TrinoCertificate.html",
    "profiles/TrinoJWT": "../reference/profiles/TrinoJWT.html",
    "profiles/TrinoLDAP": "../reference/profiles/TrinoLDAP.html",
    "profiles/VerticaUserPassword": "../reference/profiles/VerticaUserPassword.html",
}
