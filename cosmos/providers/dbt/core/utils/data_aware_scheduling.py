from cosmos.providers.dbt.dataset import Dataset


def get_dbt_dataset(connection_id: str, project_name: str, model_name: str):
    return Dataset(f"DBT://{connection_id.upper()}/{project_name.upper()}/{model_name.upper()}")
