"""Version-aware imports for Airflow objects whose import path differs across Airflow 2 and 3.

``EmptyOperator`` moved to the standard provider in Airflow 3; the legacy
``airflow.operators.empty`` path still resolves there but emits a ``DeprecatedImportWarning``,
so on Airflow 3 we import it from the standard provider. Compare on the major version so that
Airflow 3 pre-releases (e.g. 3.0.0rc1) are treated as Airflow 3.
"""

from __future__ import annotations

from typing import Any

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION, AIRFLOW_VERSION

if AIRFLOW_VERSION.major >= _AIRFLOW3_MAJOR_VERSION:
    try:
        from airflow.providers.standard.operators.empty import EmptyOperator as EmptyOperator
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "Cosmos on Airflow 3 requires `apache-airflow-providers-standard` to import `EmptyOperator`."
        ) from exc
else:
    # The redundant ``as EmptyOperator`` alias marks the name as an explicit re-export for type
    # checkers; the ``no-redef`` ignore silences the duplicate binding mypy sees across branches.
    from airflow.operators.empty import EmptyOperator as EmptyOperator  # type: ignore[no-redef]

# Dotted import path for the version-appropriate EmptyOperator, for the places that store the
# operator as a string for later dynamic import (e.g. ``Task.operator_class``) rather than
# referencing the class. Derived from the class itself, so it always matches the imported one.
EMPTY_OPERATOR_CLASS_PATH = f"{EmptyOperator.__module__}.{EmptyOperator.__name__}"


# ``Variable`` moved to the Task SDK in Airflow 3. ``airflow.sdk.Variable`` avoids the
# deprecation warning that ``airflow.models.Variable`` emits per access on Airflow 3, but it only
# works inside the task supervisor; outside it (e.g. ``dag.test()``) it raises ImportError. These
# helpers prefer the SDK and fall back to ``airflow.models.Variable`` when it is not usable.
def get_variable(key: str, default: Any = None) -> Any:
    try:
        from airflow.sdk import Variable

        return Variable.get(key, default=default)
    except ImportError:
        from airflow.models import Variable as LegacyVariable

        return LegacyVariable.get(key, default_var=default)


def set_variable(key: str, value: str) -> None:
    try:
        from airflow.sdk import Variable

        Variable.set(key, value)
    except ImportError:
        from airflow.models import Variable

        Variable.set(key, value)


def delete_variable(key: str) -> None:
    try:
        from airflow.sdk import Variable

        Variable.delete(key)
    except ImportError:
        from airflow.models import Variable

        Variable.delete(key)
