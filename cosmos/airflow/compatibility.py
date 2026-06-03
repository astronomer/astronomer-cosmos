"""Version-aware imports for Airflow objects whose import path differs across Airflow 2 and 3.

Names exported here are resolved lazily on first attribute access (PEP 562), so importing
this module is free: the underlying Airflow object is only imported when the name is actually
used. Use sites keep referencing the object by its real name (e.g. ``EmptyOperator``) rather
than a version-specific dotted path.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION, AIRFLOW_VERSION

if TYPE_CHECKING:
    # Resolved for type checkers / IDEs only; no import happens at runtime here. The standard
    # provider path is tried first so the type resolves to the real class on Airflow 3 (the
    # legacy module is a deprecated shim typed as ``Any`` there).
    try:
        from airflow.providers.standard.operators.empty import EmptyOperator as EmptyOperator
    except ImportError:
        from airflow.operators.empty import EmptyOperator as EmptyOperator  # type: ignore[no-redef]

# Single source of truth for where ``EmptyOperator`` lives. The operator moved to the standard
# provider in Airflow 3; the legacy ``airflow.operators.empty`` path still resolves there but
# emits a ``DeprecatedImportWarning``, so on Airflow 3 we select the standard provider path.
# Compare on the major version so Airflow 3 pre-releases (e.g. 3.0.0rc1) are treated as Airflow 3.
_EMPTY_OPERATOR_MODULE = (
    "airflow.operators.empty"
    if AIRFLOW_VERSION.major < _AIRFLOW3_MAJOR_VERSION
    else "airflow.providers.standard.operators.empty"
)

# Maps the exported name to the module it should be imported from for this Airflow version.
_LAZY_IMPORTS = {
    "EmptyOperator": _EMPTY_OPERATOR_MODULE,
}


def __getattr__(name: str) -> object:
    module_path = _LAZY_IMPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    # Cache the resolved symbol in the module namespace so subsequent attribute access skips
    # __getattr__ entirely (PEP 562 only invokes it for names missing from globals()).
    resolved = getattr(importlib.import_module(module_path), name)
    globals()[name] = resolved
    return resolved


def get_version_aware_operator_class_path(operator: type) -> str:
    """Return the fully qualified import path for the given operator class.

    The path is read from the class itself (``__module__`` + ``__name__``), so it reflects
    the actual module the class lives in for the running Airflow version. Pair it with the
    version-aware classes exported from this module, e.g.::

        get_version_aware_operator_class_path(EmptyOperator)

    Used where Cosmos stores the operator as a dotted string for later dynamic import
    (e.g. ``Task.operator_class``) instead of referencing the class directly.
    """
    return f"{operator.__module__}.{operator.__name__}"
