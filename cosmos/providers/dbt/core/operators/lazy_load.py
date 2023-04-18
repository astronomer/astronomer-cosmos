class MissingPackage:
    def __init__(self, module_name, optional_dependency_name):
        self.module_name = module_name
        self.optional_dependency_name = optional_dependency_name

    def __getattr__(self, item):
        raise RuntimeError(
            f"Error loading the module {self.module_name},"
            f" please make sure the right optional dependencies are installed."
            f" try - pip install astronomer-cosmos[...,{self.optional_dependency_name}]"
        )
