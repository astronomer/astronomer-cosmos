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


def _expand_env_vars(file_path: str):
    """Expand environment variables in the given file."""
    file_path = Path(file_path)
    with file_path.open() as fp:
        yaml_with_env = os.path.expandvars(fp.read())
    with file_path.open("w") as fp:
        fp.write(yaml_with_env)


@nox.session(python=["3.8", "3.9", "3.10"])
@nox.parametrize("airflow", ["2.4", "2.5"])
def test(session: nox.Session, airflow) -> None:
    """Run both unit and integration tests."""
    session.run("echo", "$PWD")
    env = {
        "AIRFLOW_HOME": f"~/airflow-{airflow}-python-{session.python}",
        "AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES": "airflow\\.* astro\\.* cosmos\\.*",
    }

    session.install(f"apache-airflow=={airflow}")
    session.install("-e", ".[all,tests]")

    # Log all the installed dependencies
    session.log("Installed Dependencies:")
    session.run("pip3", "freeze")

    _expand_env_vars("test-connections.yaml")

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
