from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCompileLocalOperator,
    DbtDocsAzureStorageLocalOperator,
    DbtDocsGCSLocalOperator,
    DbtDocsLocalOperator,
    DbtDocsS3LocalOperator,
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


class DbtDocsS3AirflowAsyncOperator(DbtDocsS3LocalOperator):
    pass


class DbtDocsAzureStorageAirflowAsyncOperator(DbtDocsAzureStorageLocalOperator):
    pass


class DbtDocsGCSAirflowAsyncOperator(DbtDocsGCSLocalOperator):
    pass


class DbtCompileAirflowAsyncOperator(DbtCompileLocalOperator):
    pass
