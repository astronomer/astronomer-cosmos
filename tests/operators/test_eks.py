from unittest.mock import MagicMock

from cosmos.operators.eks import DbtLSEksOperator, DbtSeedEksOperator, DbtBuildEksOperator, DbtTestEksOperator, \
    DbtRunEksOperator

base_kwargs = {
    "conn_id": "my_airflow_connection",
    "cluster_name": "my-cluster",
    "task_id": "my-task",
    "image": "my_image",
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}
result_map = {
    "ls": DbtLSEksOperator(**base_kwargs),
    "run": DbtRunEksOperator(**base_kwargs),
    "test": DbtTestEksOperator(**base_kwargs),
    "build": DbtBuildEksOperator(**base_kwargs),
    "seed": DbtSeedEksOperator(**base_kwargs),
}


def test_dbt_kubernetes_build_command():
    """
    Since we know that the KubernetesOperator is tested, we can just test that the
    command is built correctly and added to the "arguments" parameter.
    """
    for command_name, command_operator in result_map.items():
        command_operator.build_kube_args(context=MagicMock(), cmd_flags=MagicMock())
        assert command_operator.arguments == [
            "dbt",
            command_name,
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
            "--project-dir",
            "my/dir",
        ]
