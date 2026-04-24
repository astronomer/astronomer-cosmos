"""
Unit tests for the experimental orjson parser feature.

Covers:
- Default setting value
- Error when orjson is enabled but not installed
- Standard json is used when setting is disabled
- orjson produces identical DbtGraph output to standard json
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos import settings
from cosmos.config import ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.dbt.graph import CosmosLoadDbtException, DbtGraph

SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"
DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"


def _make_dbt_graph(manifest_path: Path = SAMPLE_MANIFEST) -> DbtGraph:
    return DbtGraph(
        project=ProjectConfig(manifest_path=manifest_path, project_name="jaffle_shop"),
        execution_config=ExecutionConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / "jaffle_shop"),
        render_config=RenderConfig(),
    )


class TestOrjsonParserSettings:
    def test_orjson_disabled_by_default(self):
        assert settings.enable_orjson_parser is False

    @patch.object(settings, "enable_orjson_parser", True)
    def test_orjson_setting_can_be_overridden(self):
        assert settings.enable_orjson_parser is True


class TestOrjsonParserMissingDependency:
    @patch.object(settings, "enable_orjson_parser", True)
    @patch("cosmos.dbt.graph.orjson", None)
    def test_raises_when_orjson_not_installed(self):
        dbt_graph = _make_dbt_graph()

        with pytest.raises(CosmosLoadDbtException) as exc_info:
            dbt_graph.load_from_dbt_manifest()

        error_msg = str(exc_info.value)
        assert "orjson" in error_msg.lower()
        assert "not installed" in error_msg.lower()
        assert "astronomer-cosmos[orjson]" in error_msg

    @patch.object(settings, "enable_orjson_parser", True)
    @patch("cosmos.dbt.graph.orjson", None)
    def test_load_manifest_from_file_raises_without_orjson(self, tmp_path):
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text('{"nodes": {}, "sources": {}, "exposures": {}}')
        dbt_graph = _make_dbt_graph()

        with pytest.raises(CosmosLoadDbtException, match="astronomer-cosmos\\[orjson\\]"):
            dbt_graph._load_manifest_from_file(manifest_file)


class TestOrjsonParserEquivalence:
    """Verify orjson and standard json produce identical DbtGraph output."""

    @patch.object(settings, "enable_orjson_parser", False)
    def test_standard_json_loads_manifest(self):
        dbt_graph = _make_dbt_graph()
        dbt_graph.load_from_dbt_manifest()

        assert len(dbt_graph.nodes) > 0

    @pytest.mark.skipif(
        not __import__("importlib").util.find_spec("orjson"),
        reason="orjson not installed",
    )
    def test_orjson_produces_same_nodes_as_standard_json(self):
        graph_std = _make_dbt_graph()
        with patch.object(settings, "enable_orjson_parser", False):
            graph_std.load_from_dbt_manifest()

        graph_orjson = _make_dbt_graph()
        with patch.object(settings, "enable_orjson_parser", True):
            graph_orjson.load_from_dbt_manifest()

        assert graph_std.nodes.keys() == graph_orjson.nodes.keys()

        for node_id in graph_std.nodes:
            std_node = graph_std.nodes[node_id]
            fast_node = graph_orjson.nodes[node_id]
            assert std_node.unique_id == fast_node.unique_id
            assert std_node.resource_type == fast_node.resource_type
            assert std_node.depends_on == fast_node.depends_on
            assert std_node.tags == fast_node.tags

    @pytest.mark.skipif(
        not __import__("importlib").util.find_spec("orjson"),
        reason="orjson not installed",
    )
    def test_load_manifest_from_file_returns_same_dict(self, tmp_path):
        """_load_manifest_from_file returns the same structure regardless of parser."""
        import json

        data = {"nodes": {"model.test.foo": {"resource_type": "model"}}, "sources": {}, "exposures": {}}
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(data))

        dbt_graph = _make_dbt_graph()

        with patch.object(settings, "enable_orjson_parser", False):
            result_std = dbt_graph._load_manifest_from_file(manifest_file)

        with patch.object(settings, "enable_orjson_parser", True):
            result_orjson = dbt_graph._load_manifest_from_file(manifest_file)

        assert result_std == result_orjson

    def test_load_from_dbt_manifest_handles_null_manifest_root_per_loader_contract(self, tmp_path):
        """A manifest containing JSON ``null`` is treated as an empty dict (backward-compatible)."""
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text("null")

        dbt_graph = _make_dbt_graph(manifest_file)

        with patch.object(settings, "enable_orjson_parser", False):
            assert dbt_graph._load_manifest_from_file(manifest_file) == {}

    def test_load_manifest_from_file_raises_on_invalid_root_type(self, tmp_path):
        """Non-dict, non-null roots (e.g. JSON arrays) raise CosmosLoadDbtException."""
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text("[1, 2, 3]")

        dbt_graph = _make_dbt_graph(manifest_file)

        with patch.object(settings, "enable_orjson_parser", False):
            with pytest.raises(CosmosLoadDbtException, match="expected top-level JSON object"):
                dbt_graph._load_manifest_from_file(manifest_file)
