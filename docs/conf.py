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
# - https://documatt.com/sphinx-reredirects/usage/
redirects = {
     "airflow3_compatibility/index": "../policy/airflow3-compatibility.html",
     "compatibility-policy": "../policy/compatibility-policy.html",
     "contributing": "../policy/contributing.html",
     "contributors-roles": "../policy/contributors-roles.html"
}

