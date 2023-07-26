from typing import Any


def MissingPackage(module_name: str, optional_dependency_name: str) -> Any:
    def raise_error(**kwargs: Any) -> None:
        raise RuntimeError(
            f"Error loading the module {module_name},"
            f" please make sure the right optional dependencies are installed."
            f" try - pip install astronomer-cosmos[...,{optional_dependency_name}]"
        )

    return raise_error
