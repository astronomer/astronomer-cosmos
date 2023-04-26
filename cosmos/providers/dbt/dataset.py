try:
    from airflow.datasets import Dataset
except ImportError:
    from logging import getLogger

    logger = getLogger(__name__)

    class Dataset:
        cosmos_override = True

        def __init__(self, id: str, *args, **kwargs):
            self.id = id
            logger.warning("Datasets are not supported in Airflow < 2.5.0")

        def __eq__(self, other) -> bool:
            return self.id == other.id
