from datetime import datetime

from airflow import DAG, Dataset
from airflow.datasets import DatasetAlias
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


ALIAS_NAME = "basic_cosmos_dag__orders__run"
DATASET_URI = 'postgres://0.0.0.0:5434/postgres.public.orders'


class CustomOperator(BaseOperator):

    def __init__(self, *args, **kwargs):
        kwargs["outlets"] = [DatasetAlias(name=ALIAS_NAME)]
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        new_outlets = [Dataset(DATASET_URI)]
        for outlet in new_outlets:
            context["outlet_events"][ALIAS_NAME].add(outlet)


with DAG("artificial_dataset_creator", start_date=datetime(2023, 4, 20), schedule=None) as dag:
    do_something = CustomOperator(task_id="do_something")
    do_something
