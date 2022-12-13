from airflow.operators.empty import EmptyOperator


class MyCustomOperator(EmptyOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
