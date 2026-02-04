"""
Tests for the experimental orjson parser feature.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos import settings
from cosmos.config import ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.dbt.graph import CosmosLoadDbtException, DbtGraph

SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"


def _is_orjson_available() -> bool:
    """Check if orjson is available for testing."""
    try:
        import orjson

        return True
    except ImportError:
        return False


class TestOrjsonParserSettings:
    """Tests for orjson parser settings."""

    def test_orjson_disabled_by_default(self):
        """Verify orjson parser is disabled by default."""
        assert settings.enable_orjson_parser is False

    @patch.object(settings, "enable_orjson_parser", True)
    def test_orjson_setting_can_be_overridden(self):
        """Verify setting can be overridden via environment variable."""
        assert settings.enable_orjson_parser is True


class TestOrjsonParserWithoutOrjson:
    """Tests for behavior when orjson is not installed."""

    @patch.object(settings, "enable_orjson_parser", True)
    @patch("cosmos.dbt.graph.orjson", None)
    def test_raises_error_when_orjson_not_installed(self):
        """Verify clear error is raised when orjson is enabled but not installed."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="jaffle_shop")
        execution_config = ExecutionConfig(dbt_project_path=Path(__file__).parent.parent / "sample")
        render_config = RenderConfig()

        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        with pytest.raises(CosmosLoadDbtException) as exc_info:
            dbt_graph.load_from_dbt_manifest()

        assert "orjson" in str(exc_info.value).lower()
        assert "not installed" in str(exc_info.value).lower()
        assert "astronomer-cosmos[orjson]" in str(exc_info.value)


@pytest.mark.skipif(
    not _is_orjson_available(),
    reason="orjson not installed - install with: pip install orjson",
)
class TestOrjsonParserWithOrjson:
    """Tests that require orjson to be installed."""

    @patch.object(settings, "enable_orjson_parser", False)
    def test_uses_standard_json_when_disabled(self):
        """Verify standard json is used when orjson is disabled."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="jaffle_shop")
        execution_config = ExecutionConfig(dbt_project_path=Path(__file__).parent.parent / "sample")
        render_config = RenderConfig()

        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )
        dbt_graph.load_from_dbt_manifest()

        assert len(dbt_graph.nodes) > 0

    @patch.object(settings, "enable_orjson_parser", True)
    def test_orjson_produces_same_results_as_standard(self):
        """Verify orjson produces identical results to standard json parser."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="jaffle_shop")
        execution_config = ExecutionConfig(dbt_project_path=Path(__file__).parent.parent / "sample")
        render_config = RenderConfig()

        # Load with standard json
        dbt_graph_standard = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )
        with patch.object(settings, "enable_orjson_parser", False):
            dbt_graph_standard.load_from_dbt_manifest()

        # Load with orjson
        dbt_graph_orjson = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )
        dbt_graph_orjson.load_from_dbt_manifest()

        # Compare results
        assert dbt_graph_standard.nodes.keys() == dbt_graph_orjson.nodes.keys()

        for node_id in dbt_graph_standard.nodes:
            standard_node = dbt_graph_standard.nodes[node_id]
            orjson_node = dbt_graph_orjson.nodes[node_id]

            assert standard_node.unique_id == orjson_node.unique_id
            assert standard_node.resource_type == orjson_node.resource_type
            assert standard_node.depends_on == orjson_node.depends_on
