"""
Tests for the cosmos._json compatibility module.

Validates that all wrapper functions (loads, dumps, dumps_bytes, dumps_str,
load, dump) work correctly with both the stdlib json fallback
(enable_orjson_parser=False) and the orjson backend (enable_orjson_parser=True).
"""

from __future__ import annotations

import importlib
import io
from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos import _json as json
from cosmos import settings
from cosmos.config import ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.dbt.graph import DbtGraph

SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"
SAMPLE_DATA = {"key": "value", "number": 42, "nested": {"a": 1}}


# ---------------------------------------------------------------------------
# Setting defaults
# ---------------------------------------------------------------------------
class TestSettings:
    def test_disabled_by_default(self, monkeypatch):
        # Ensure environment does not force-enable the parser, then reload settings
        monkeypatch.delenv("AIRFLOW__COSMOS__ENABLE_ORJSON_PARSER", raising=False)
        importlib.reload(settings)
        assert settings.enable_orjson_parser is False

    @patch.object(settings, "enable_orjson_parser", True)
    def test_can_be_enabled(self):
        assert settings.enable_orjson_parser is True


# ---------------------------------------------------------------------------
# _use_orjson() gating
# ---------------------------------------------------------------------------
class TestUseOrjsonGating:
    @patch.object(settings, "enable_orjson_parser", True)
    @patch("cosmos._json._orjson", None)
    def test_raises_when_enabled_but_not_installed(self):
        with pytest.raises(ImportError, match="orjson is not installed"):
            json.loads("{}")

    @patch.object(settings, "enable_orjson_parser", False)
    def test_stdlib_used_when_disabled(self):
        result = json.loads('{"a": 1}')
        assert result == {"a": 1}


# ---------------------------------------------------------------------------
# loads
# ---------------------------------------------------------------------------
class TestLoads:
    @patch.object(settings, "enable_orjson_parser", False)
    def test_loads_stdlib(self):
        assert json.loads('{"a": 1}') == {"a": 1}

    @patch.object(settings, "enable_orjson_parser", True)
    def test_loads_orjson(self):
        assert json.loads('{"a": 1}') == {"a": 1}

    @patch.object(settings, "enable_orjson_parser", True)
    def test_loads_bytes_orjson(self):
        assert json.loads(b'{"a": 1}') == {"a": 1}


# ---------------------------------------------------------------------------
# dumps
# ---------------------------------------------------------------------------
class TestDumps:
    @patch.object(settings, "enable_orjson_parser", False)
    def test_dumps_stdlib_returns_str(self):
        result = json.dumps(SAMPLE_DATA)
        assert isinstance(result, str)

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_orjson_returns_str(self):
        result = json.dumps(SAMPLE_DATA)
        assert isinstance(result, str)

    @patch.object(settings, "enable_orjson_parser", False)
    def test_dumps_sort_keys_stdlib(self):
        result = json.dumps({"b": 2, "a": 1}, sort_keys=True)
        assert result == '{"a": 1, "b": 2}'

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_sort_keys_orjson(self):
        result = json.dumps({"b": 2, "a": 1}, sort_keys=True)
        assert isinstance(result, str)
        assert '"a"' in result
        assert result.index('"a"') < result.index('"b"')


# ---------------------------------------------------------------------------
# dumps_bytes
# ---------------------------------------------------------------------------
class TestDumpsBytes:
    @patch.object(settings, "enable_orjson_parser", False)
    def test_stdlib_returns_bytes(self):
        result = json.dumps_bytes(SAMPLE_DATA)
        assert isinstance(result, bytes)

    @patch.object(settings, "enable_orjson_parser", True)
    def test_orjson_returns_bytes(self):
        result = json.dumps_bytes(SAMPLE_DATA)
        assert isinstance(result, bytes)

    @patch.object(settings, "enable_orjson_parser", False)
    def test_sort_keys_stdlib(self):
        result = json.dumps_bytes({"b": 2, "a": 1}, sort_keys=True)
        assert isinstance(result, bytes)
        assert b'"a": 1' in result

    @patch.object(settings, "enable_orjson_parser", True)
    def test_sort_keys_orjson(self):
        result = json.dumps_bytes({"b": 2, "a": 1}, sort_keys=True)
        assert isinstance(result, bytes)
        assert result.index(b'"a"') < result.index(b'"b"')


# ---------------------------------------------------------------------------
# dumps_str
# ---------------------------------------------------------------------------
class TestDumpsStr:
    @patch.object(settings, "enable_orjson_parser", False)
    def test_stdlib_returns_str(self):
        result = json.dumps_str(SAMPLE_DATA)
        assert isinstance(result, str)

    @patch.object(settings, "enable_orjson_parser", True)
    def test_orjson_returns_str(self):
        result = json.dumps_str(SAMPLE_DATA)
        assert isinstance(result, str)

    @patch.object(settings, "enable_orjson_parser", False)
    def test_sort_keys_stdlib(self):
        result = json.dumps_str({"b": 2, "a": 1}, sort_keys=True)
        assert isinstance(result, str)
        assert '"a": 1' in result

    @patch.object(settings, "enable_orjson_parser", True)
    def test_sort_keys_orjson(self):
        result = json.dumps_str({"b": 2, "a": 1}, sort_keys=True)
        assert isinstance(result, str)
        assert result.index('"a"') < result.index('"b"')


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------
class TestLoad:
    @patch.object(settings, "enable_orjson_parser", False)
    def test_load_stdlib(self):
        fp = io.StringIO('{"a": 1}')
        assert json.load(fp) == {"a": 1}

    @patch.object(settings, "enable_orjson_parser", True)
    def test_load_orjson_text(self):
        fp = io.StringIO('{"a": 1}')
        assert json.load(fp) == {"a": 1}

    @patch.object(settings, "enable_orjson_parser", True)
    def test_load_orjson_binary(self):
        fp = io.BytesIO(b'{"a": 1}')
        assert json.load(fp) == {"a": 1}


# ---------------------------------------------------------------------------
# dump
# ---------------------------------------------------------------------------
class TestDump:
    @patch.object(settings, "enable_orjson_parser", False)
    def test_dump_stdlib(self):
        fp = io.StringIO()
        json.dump({"a": 1}, fp)
        assert fp.getvalue() == '{"a": 1}'

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dump_orjson(self):
        fp = io.StringIO()
        json.dump({"a": 1}, fp)
        assert fp.getvalue() == '{"a":1}'

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dump_orjson_with_sort_keys(self):
        fp = io.StringIO()
        json.dump({"b": 2, "a": 1}, fp, sort_keys=True)
        assert fp.getvalue() == '{"a":1,"b":2}'

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dump_orjson_with_indent_2(self):
        fp = io.StringIO()
        json.dump({"a": 1}, fp, indent=2)
        assert '  "a"' in fp.getvalue()

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dump_orjson_falls_back_for_indent_4(self):
        """indent=4 is not supported by orjson, so dump() must fall back to stdlib."""
        fp = io.StringIO()
        json.dump({"a": 1}, fp, indent=4)
        assert fp.getvalue() == '{\n    "a": 1\n}'


# ---------------------------------------------------------------------------
# Indent fallback: orjson only supports indent=None and indent=2.
# Unsupported indent values must transparently fall back to stdlib.
# ---------------------------------------------------------------------------
class TestIndentFallback:
    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_str_indent_4_falls_back_to_stdlib(self):
        result = json.dumps_str({"a": 1}, indent=4)
        assert '    "a": 1' in result

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_str_indent_2_uses_orjson(self):
        result = json.dumps_str({"a": 1}, indent=2)
        assert '  "a"' in result

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_str_indent_none_uses_orjson_compact(self):
        result = json.dumps_str({"a": 1})
        assert "\n" not in result

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_bytes_indent_4_falls_back_to_stdlib(self):
        result = json.dumps_bytes({"a": 1}, indent=4)
        assert isinstance(result, bytes)
        assert b'    "a": 1' in result

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_indent_4_falls_back_to_stdlib(self):
        result = json.dumps({"a": 1}, indent=4)
        assert isinstance(result, str)
        assert '    "a": 1' in result

    @patch.object(settings, "enable_orjson_parser", True)
    def test_dumps_str_indent_4_matches_stdlib_output(self):
        """The freshness path (local.py) relies on indent=4 producing stdlib-compatible output."""
        import json as stdlib_json

        data = {"sources": [{"name": "src", "freshness": {"loaded_at": "2026-01-01"}}]}
        result = json.dumps_str(data, indent=4)
        expected = stdlib_json.dumps(data, indent=4)
        assert result == expected


# ---------------------------------------------------------------------------
# Error re-exports
# ---------------------------------------------------------------------------
class TestErrors:
    def test_json_decode_error_accessible(self):
        assert json.JSONDecodeError is not None

    def test_decoder_json_decode_error_accessible(self):
        assert json.decoder.JSONDecodeError is not None

    def test_json_decode_error_catches_bad_json(self):
        with pytest.raises(json.JSONDecodeError):
            json.loads("{bad json}")

    @patch.object(settings, "enable_orjson_parser", True)
    def test_json_decode_error_orjson_is_caught_as_stdlib_type(self):
        # orjson.JSONDecodeError is a subclass of json.JSONDecodeError;
        # callers catching json.JSONDecodeError must still work under orjson.
        with pytest.raises(json.JSONDecodeError):
            json.loads("{bad json}")


class TestKwargsFallback:
    @patch.object(settings, "enable_orjson_parser", True)
    def test_loads_falls_back_to_stdlib_when_kwargs_passed(self):
        # parse_int is a stdlib-only kwarg; passing it must not be silently ignored
        result = json.loads('{"a": 1}', parse_int=float)
        assert isinstance(result["a"], float)

    @patch.object(settings, "enable_orjson_parser", True)
    def test_load_falls_back_to_stdlib_when_kwargs_passed(self):
        fp = io.StringIO('{"a": 1}')
        result = json.load(fp, parse_int=float)
        assert isinstance(result["a"], float)


# ---------------------------------------------------------------------------
# Integration: manifest parsing produces same results with both backends
# ---------------------------------------------------------------------------
class TestManifestIntegration:
    def test_orjson_produces_same_results_as_standard(self):
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
        with patch.object(settings, "enable_orjson_parser", True):
            dbt_graph_orjson.load_from_dbt_manifest()

        # Compare results
        assert dbt_graph_standard.nodes.keys() == dbt_graph_orjson.nodes.keys()

        for node_id in dbt_graph_standard.nodes:
            standard_node = dbt_graph_standard.nodes[node_id]
            orjson_node = dbt_graph_orjson.nodes[node_id]
            assert standard_node.unique_id == orjson_node.unique_id
            assert standard_node.resource_type == orjson_node.resource_type
            assert standard_node.depends_on == orjson_node.depends_on
