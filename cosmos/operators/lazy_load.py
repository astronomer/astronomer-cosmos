def MissingPackage(module_name, optional_dependency_name):
    def raise_error(**kwargs):
        raise RuntimeError(
            f"Error loading the module {module_name},"
            f" please make sure the right optional dependencies are installed."
            f" try - pip install astronomer-cosmos[...,{optional_dependency_name}]"
        )

    return raise_error
