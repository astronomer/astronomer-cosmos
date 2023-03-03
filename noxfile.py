"""Nox automation definitions."""


import nox

nox.options.sessions = ["dev"]
nox.options.reuse_existing_virtualenvs = True


@nox.session(python="3.10")
def dev(session: nox.Session) -> None:
    """Create a dev environment with everything installed.

    This is useful for setting up IDE for autocompletion etc. Point the
    development environment to ``.nox/dev``.
    """
    session.install("nox")
    session.install("-e", ".[all,tests]")


@nox.session(python=["3.8", "3.9", "3.10"])
@nox.parametrize("airflow", ["2.4", "2.5.0"])
def test(session: nox.Session, airflow) -> None:
    """Run both unit and integration tests."""
    env = {
        "AIRFLOW_HOME": f"~/airflow-{airflow}-python-{session.python}",
        "AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES": "airflow\\.* astro\\.* cosmos\\.*",
    }

    session.install(f"apache-airflow=={airflow}")
    session.install("-e", ".[all,tests]")

    # Log all the installed dependencies
    session.log("Installed Dependencies:")
    session.run("pip3", "freeze")

    session.run("airflow", "db", "init", env=env)

    # Since pytest is not installed in the nox session directly, we need to set `external=true`.
    session.run(
        "pytest",
        "-vv",
        "tests",
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


@nox.session()
@nox.parametrize(
    "extras",
    [
        ("postgres-amazon", {"include": ["postgres", "amazon"]}),
        ("snowflake-amazon", {"include": ["snowflake", "amazon"]}),
        ("sqlite", {"include": ["sqlite"]}),
    ],
)
def test_examples_by_dependency(session: nox.Session, extras):
    _, extras = extras
    pypi_deps = ",".join(extras["include"])
    pytest_options = " and ".join(extras["include"])
    pytest_options = " and not ".join([pytest_options, *extras.get("exclude", [])])
    pytest_args = ["-k", pytest_options]

    env = {
        "AIRFLOW_HOME": "~/airflow-latest-python-latest",
        "AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES": "airflow\\.* astro\\.*",
    }

    session.install("-e", f".[{pypi_deps}]")
    session.install("-e", ".[tests]")

    # Log all the installed dependencies
    session.log("Installed Dependencies:")
    session.run("pip3", "freeze")

    session.run("airflow", "db", "init", env=env)

    # Since pytest is not installed in the nox session directly, we need to set `external=true`.
    session.run(
        "pytest",
        "tests_integration/test_example_dags.py",
        *pytest_args,
        *session.posargs,
        env=env,
        external=True,
    )


@nox.session(python="3.9")
def build_docs(session: nox.Session) -> None:
    """Build release artifacts."""
    session.install("-e", ".[docs]")
    session.chdir("./docs")
    session.run("make", "html")
