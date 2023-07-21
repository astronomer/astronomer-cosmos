from typing import Any, Tuple


try:
    from airflow.datasets import Dataset
except (ImportError, ModuleNotFoundError):
    from logging import getLogger

    logger = getLogger(__name__)

    class Dataset:  # type: ignore[no-redef]
        cosmos_override = True

        def __init__(self, id: str, *args: Tuple[Any], **kwargs: str):
            self.id = id
            logger.warning("Datasets are not supported in Airflow < 2.5.0")

        def __eq__(self, other: "Dataset") -> bool:
            return bool(self.id == other.id)


def get_dbt_dataset(connection_id: str, project_name: str, model_name: str) -> Dataset:
    return Dataset(f"DBT://{connection_id.upper()}/{project_name.upper()}/{model_name.upper()}")


__all__ = ["Dataset"]
