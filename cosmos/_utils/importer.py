import importlib
from typing import Any, Callable


def load_method_from_module(module_path: str, method_name: str) -> Callable[..., Any]:
    try:
        module = importlib.import_module(module_path)
        method = getattr(module, method_name)
        return method  # type: ignore
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f"Module {module_path} not found")
    except AttributeError:
        raise AttributeError(f"Method {method_name} not found in module {module_path}")
