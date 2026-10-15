#!/bin/bash
# Prepare an Astronomer Runtime image's own Python environment to run the Cosmos test suite
# AGAINST the Airflow that ships in that image.
#
# Unlike scripts/test/pre-install-airflow.sh (which pip-pins a specific Airflow), this installs
# NO Airflow: the Runtime image already provides it. We only add cosmos (from the checked-out
# source), its dbt adapters, and the pytest tooling on top. The downstream run scripts
# (integration-setup.sh / integration.sh / unit.sh) already use whatever `airflow` is on PATH,
# so they run unchanged against the image's Airflow build.
#
# Usage: runtime-alpha-setup.sh [COSMOS_EXTRAS]
#   COSMOS_EXTRAS  comma-separated cosmos optional-dependency extras (default: dbt-postgres)
set -euxo pipefail

COSMOS_EXTRAS="${1:-dbt-postgres}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Astronomer Runtime images ship /pyproject.toml carrying [tool.uv].constraint-dependencies, so
# that `uv pip install` stays pinned to the image's Airflow build. uv discovers this file by
# walking up from the install directory. Some alpha images write an invalid PEP440
# [project].version there (e.g. "3.3-7-alpha2" -- valid TOML, but not a valid version), which
# makes uv abort parsing the file and therefore abort EVERY install run under it. We never use
# that version, so neutralise it to a valid placeholder, scoped to the [project] table so only that
# key is touched and the constraint-dependencies below stay intact. Guarded on existence so older
# images are a no-op.
RUNTIME_UV_PYPROJECT="${RUNTIME_UV_PYPROJECT:-/pyproject.toml}"
if [ -f "$RUNTIME_UV_PYPROJECT" ]; then
  echo "Neutralising [project].version in ${RUNTIME_UV_PYPROJECT} (Runtime image ships an invalid PEP440 version that breaks uv)."
  sed -i -E '/^\[project\]/,/^\[/{s/^version = .*/version = "0.0.0"/}' "$RUNTIME_UV_PYPROJECT"
  # Confirm the neutralisation took. tomllib reads the version regardless of how the line is
  # formatted, so this catches a future image whose version line the sed did not match, which would
  # otherwise leave an invalid PEP440 version for uv to reject later with a confusing error.
  python - "$RUNTIME_UV_PYPROJECT" <<'PY'
import sys, tomllib
from packaging.version import InvalidVersion, Version

path = sys.argv[1]
with open(path, "rb") as f:
    version = tomllib.load(f).get("project", {}).get("version")
if version is not None:
    try:
        Version(version)
    except InvalidVersion:
        raise SystemExit(
            f"ERROR: {path} still has an invalid PEP440 [project].version ({version!r}) after "
            "neutralisation. The version-line format likely changed. Update the sed in "
            "runtime-alpha-setup.sh."
        )
PY
fi

pip install -U uv

# Install cosmos (editable, from this checkout) + requested dbt adapters. Providers come from the
# image's own /pyproject.toml constraint-dependencies (the versions the alpha ships), so the suite
# runs against the alpha's real provider set rather than an overridden one. cosmos only floors
# apache-airflow>=2.9.0, so uv won't reinstall the image's Airflow.
uv pip install --system -e "${REPO_ROOT}[${COSMOS_EXTRAS}]"

# Pytest tooling needed to collect and run the suite.
uv pip install --system -r "${SCRIPT_DIR}/requirements-test-tools.txt"

# Some integration tests execute dbt in a separate virtualenv (subprocess invocation mode).
# pre-install-airflow.sh normally creates this; reproduce it here since we skip that script.
python -m venv "${REPO_ROOT}/venv-subprocess"
"${REPO_ROOT}/venv-subprocess/bin/pip" install -U "dbt-core<2.0" dbt-postgres

# Compatibility signal: show the resolved Airflow / dbt / cosmos versions actually in play.
uv pip freeze | grep -Ei 'airflow|dbt|cosmos' || true
airflow version
