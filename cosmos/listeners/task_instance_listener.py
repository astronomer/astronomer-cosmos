from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cosmos import ProfileConfig


try:
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as TaskInstance
except ImportError:
    from airflow.models.taskinstance import TaskInstance  # type: ignore[assignment]


def get_profile_metrics(task_instance: TaskInstance) -> tuple[str, str, str]:
    profile_config: ProfileConfig = task_instance.task.profile_config

    # Determine strategy
    profile_strategy = "yaml_file" if profile_config.profiles_yml_filepath is not None else "mapping"

    # Default
    profile_mapping_class = ""

    # Populate mapping class only when strategy is "mapping"
    if profile_strategy == "mapping":
        profile_mapping_class = str(profile_config.profile_mapping)

    # Get database or profile type
    database = profile_config.get_profile_type()

    return profile_strategy, profile_mapping_class, database
