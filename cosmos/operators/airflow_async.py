from airflow.providers.google

class BigqueryRunAsync(BigQueryInsertJobOperator):
    def __init__(
            self,
            gcp_conn_id: str = "",
            project_id: str = "",
            location: str = "",
            job_id: str = ""
    ):


    def execute(self, context):
        # configuration: Sql query
        # 1. get remote conn
        # 2. get sql file name
        pass



