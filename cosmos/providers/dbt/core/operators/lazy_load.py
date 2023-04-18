class MissingPackage:
    def __init__(self, module_name, optional_dependency_name):
        self.module_name = module_name
        self.optional_dependency_name = optional_dependency_name

    def __getattr__(self, item):
        raise RuntimeError(
            f"Error loading the module {self.module_name},"
            f" please make sure the right optional dependencies are installed."
            f" try - pip install astronomer-cosmos[...,{self.optional_dependency_name}]"
        )


try:
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError:
    DockerOperator = MissingPackage(
        "airflow.providers.docker.operators.docker.DockerOperator", "docker"
    )

try:
    from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
        convert_env_vars,
    )
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
        KubernetesPodOperator,
    )
    from kubernetes.client import models as k8s
except ImportError:
    KubernetesPodOperator = MissingPackage(
        "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator",
        "kubernetes",
    )
    convert_env_vars = MissingPackage(
        "airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters.convert_env_vars",
        "kubernetes",
    )
    k8s = MissingPackage("kubernetes.client.models", "kubernetes")
