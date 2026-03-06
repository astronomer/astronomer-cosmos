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
    "airflow3_compatibility/index": "../policy/airflow3-compatibility.html",
    "compatibility-policy": "../policy/compatibility-policy.html",
    "configuration/caching": "../optimize_performance/caching.html",
    "configuration/cosmos-conf": "../reference/configs/cosmos-conf.html"
    "configuration/dag-customization": "../guides/translate_dbt_to_airflow/dag-customization.html",
    "configuration/execution-config": "../reference/configs/execution-config.html",
    "configuration/memory_optimization": "../optimize_performance/memory_optimization.html",
    "configuration/parsing-methods": "../guides/translate_dbt_to_airflow/parsing-methods.html",
    "configuration/partial-parsing": "../guides/run_dbt/customization/partial-parsing.html",
    "configuratoin/profile-config": "../profiles/index.html",
    "configuration/project-config": "../reference/configs/project-config.html",
    "configuration/render-config": "../guides/translate_dbt_to_airflow/render-config.html",
    "configuration/selecting-excluding": "../guides/translate_dbt_to_airflow/selecting-excluding.html",
    "configuration/source-nodes-rendering": "../guides/translate_dbt_to_airflow/managing-sources.html",
    "configuration/testing-behavior": "../guides/translate_dbt_to_airflow/testing-behavior.html",
    "contributing": "../policy/contributing.html",
    "contributors": "../policy/contributors.html",
    "contributors-roles": "../policy/contributors-roles.html",
    "getting_started/async-execution-mode": "../guides/run_dbt/airflow-worker/async-execution-mode.html",
    "getting_started/aws-container-run-job": "../guides/run_dbt/airflow-worker/async-execution-mode.html",
    "getting_started/azure-container-instance": "../guides/run_dbt/container/azure-container-instance.html",
    "getting_started/custom-airflow-properties": "../guides/run_dbt/customization/custom-airflow-properties.html",
    "getting_started/docker": "../guides/run_dbt/container/docker.html",
    "getting_started/execution-modes-local-conflicts": "../guides/run_dbt/airflow-worker/execution-modes-local-conflicts.html",
    "getting_started/execution-modes": "../guides/run_dbt/execution-modes.html",
    "getting_started/gcp-cloud-run-job": "../guides/run_dbt/container/gcp-cloud-run-job.html",
    "getting_started/kubernetes": "../guides/run_dbt/container/kubernetes.html",
    "getting_started/operators": "../guides/run_dbt/operators/operators.html",
    "getting_started/watcher-execution-mode": "../guides/run_dbt/airflow-worker/watcher-execution-mode.html",
    "getting_started/watcher-kubernetes-execution-mode": "../guides/run_dbt/container/watcher-kubernetes-execution-mode.html",
}
