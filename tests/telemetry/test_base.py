from cosmos.telemetry.base import TelemetryMetric, MetricType, sanitize_metric_name, sanitize_tags


def test_metric_creation_defaults():
    metric = TelemetryMetric(name="MyMetric", metric_type=MetricType.COUNTER)
    assert metric.name == "MyMetric"
    assert metric.metric_type is MetricType.COUNTER
    assert metric.value == 1.0
    assert metric.tags is None


def test_sanitize_metric_name_converts_to_snake_case():
    assert sanitize_metric_name("DbtRunLocalOperator") == "dbtrunlocaloperator"
    assert sanitize_metric_name("dbt-run-local-operator") == "dbt_run_local_operator"
    assert sanitize_metric_name("dbt run.local") == "dbt_run_local"


def test_sanitize_tags_handles_none():
    assert sanitize_tags(None) == {}


def test_sanitize_tags_normalizes_keys_to_string_values():
    tags = {"Operator-Name": "DbtRunLocalOperator", "Attempts": 3}
    assert sanitize_tags(tags) == {
        "operator_name": "DbtRunLocalOperator",
        "attempts": "3",
    }
