from typing import Any

from cosmos.operators.base import DbtCompileMixin
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtDepsLocalOperator,
    DbtDocsAzureStorageLocalOperator,
    DbtDocsCloudLocalOperator,
    DbtDocsGCSLocalOperator,
    DbtDocsLocalOperator,
    DbtDocsS3LocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
)


class DbtBuildAirflowAsyncOperator(DbtBuildLocalOperator):
    pass


class DbtLSAirflowAsyncOperator(DbtLSLocalOperator):
    pass


class DbtSeedAirflowAsyncOperator(DbtSeedLocalOperator):
    pass


class DbtSnapshotAirflowAsyncOperator(DbtSnapshotLocalOperator):
    pass


class DbtSourceAirflowAsyncOperator(DbtSourceLocalOperator):
    pass


class DbtRunAirflowAsyncOperator(DbtRunLocalOperator):
    pass


class DbtTestAirflowAsyncOperator(DbtTestLocalOperator):
    pass


class DbtRunOperationAirflowAsyncOperator(DbtRunOperationLocalOperator):
    pass


class DbtDocsAirflowAsyncOperator(DbtDocsLocalOperator):
    pass


class DbtDocsCloudAirflowAsyncOperator(DbtDocsCloudLocalOperator):
    pass


class DbtDocsS3AirflowAsyncOperator(DbtDocsS3LocalOperator):
    pass


class DbtDocsAzureStorageAirflowAsyncOperator(DbtDocsAzureStorageLocalOperator):
    pass


class DbtDocsGCSAirflowAsyncOperator(DbtDocsGCSLocalOperator):
    pass


class DbtDepsAirflowAsyncOperator(DbtDepsLocalOperator):
    pass


class DbtCompileAirflowAsyncOperator(DbtCompileMixin, DbtLocalBaseOperator):
    """
    Executes a dbt core build command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["should_upload_compiled_sql"] = True
        super().__init__(*args, **kwargs)
