class MissingPackage:
    def __init__(self, module_name, optional_dependency_name):
        raise RuntimeError(
            f"Error loading the module {module_name},"
            f" please make sure the right optional dependencies are installed."
            f" try - pip install astronomer-cosmos[...,{optional_dependency_name}]"
        )
