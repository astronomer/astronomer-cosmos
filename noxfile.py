"""Nox automation definitions."""
import os
from pathlib import Path

import nox

nox.options.sessions = ["dev"]
nox.options.reuse_existing_virtualenvs = True

os.environ[
    "AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES"
] = "airflow.* astro.* cosmos.*"


@nox.session(python="3.10")
def dev(session: nox.Session) -> None:
    """Create a dev environment with everything installed.

    This is useful for setting up IDE for autocompletion etc. Point the
    development environment to ``.nox/dev``.
    """
    session.install("nox")
    session.install("-e", ".[all,tests]")


def _expand_env_vars(file_path: Path):
    """Expand environment variables in the given file."""
    with file_path.open() as fp:
        yaml_with_env = os.path.expandvars(fp.read())
    with file_path.open("w") as fp:
        fp.write(yaml_with_env)


@nox.session(python=["3.8", "3.9", "3.10"])
@nox.parametrize("airflow", ["2.2.4", "2.3", "2.4", "2.5"])
def test(session: nox.Session, airflow) -> None:
    """Run both unit and integration tests."""
    env = {
        "AIRFLOW_HOME": f"~/airflow-{airflow}-python-{session.python}",
        "AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES": "airflow\\.* astro\\.* cosmos\\.*",
    }

    session.install("-e", ".[all,tests]")
    session.install(f"apache-airflow=={airflow}")

    # Starting with Airflow 2.0.0, the pickle type for XCom messages has been replaced to JSON by default to prevent
    # RCE attacks and the default value for `[core] enable_xom_pickling` has been set to False.
    # Read more here: http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/latest/release_notes.html#the-default-value-for-core-enable-xcom-pickling-has-been-changed-to-false.
    # However, serialization of objects that contain attr, dataclass or custom serializer into JSON got supported only
    # after Airflow 2.5.0 with the inclusion of PR: https://github.com/apache/airflow/pull/27540 and hence, for older
    # versions of Airflow we enable pickling to support serde of such objects that the Databricks tasks push to
    # XCOM (e.g. DatabricksMetaData).
    if airflow in ("2.2.4", "2.3", "2.4"):
        env["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"] = "True"

    # Log all the installed dependencies
    session.log("Installed Dependencies:")
    session.run("pip3", "freeze")

    test_connections_file = Path("test-connections.yaml")
    if test_connections_file.exists():
        _expand_env_vars(test_connections_file)

    session.run("airflow", "db", "init", env=env)

    # Since pytest is not installed in the nox session directly, we need to set `external=true`.
    session.run(
        "pytest",
        "-vv",
        *session.posargs,
        env=env,
        external=True,
    )


@nox.session(python=["3.8"])
def type_check(session: nox.Session) -> None:
    """Run MyPy checks."""
    session.install("-e", ".[all,tests]")
    session.run("mypy", "--version")
    session.run("mypy", "cosmos")


@nox.session(python="3.9")
def build_docs(session: nox.Session) -> None:
    """Build release artifacts."""
    session.install("-e", ".[docs]")
    session.chdir("./docs")
    session.run("make", "html")
