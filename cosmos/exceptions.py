"Contains exceptions that Cosmos uses"


class CosmosValueError(ValueError):
    """Raised when a Cosmos config value is invalid."""


class AirflowCompatibilityError(Exception):
    """Raised when Cosmos features are limited for Airflow version being used."""
