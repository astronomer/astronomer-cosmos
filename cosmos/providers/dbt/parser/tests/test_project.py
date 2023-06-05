from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List

import pytest
import yaml

from cosmos.providers.dbt.parser.project import DbtModel, DbtModelType, DbtProject

DBT_PROJECT_PATH = Path(__name__).parent.parent.parent.parent.parent / "dev/dags/dbt/"
SAMPLE_CSV_PATH = DBT_PROJECT_PATH / "jaffle_shop/seeds/raw_customers.csv"
SAMPLE_MODEL_SQL_PATH = DBT_PROJECT_PATH / "jaffle_shop/models/customers.sql"
SAMPLE_SNAPSHOT_SQL_PATH = DBT_PROJECT_PATH / "jaffle_shop/models/orders.sql"


def test_dbtproject__handle_csv_file():
    dbt_project = DbtProject(
        project_name="jaffle_shop",
    )
    assert not dbt_project.seeds
    dbt_project._handle_csv_file(SAMPLE_CSV_PATH)
    assert len(dbt_project.seeds) == 1
    assert "raw_customers" in dbt_project.seeds
    raw_customers = dbt_project.seeds["raw_customers"]
    assert raw_customers.name == "raw_customers"
    assert raw_customers.type == DbtModelType.DBT_SEED
    assert raw_customers.path == SAMPLE_CSV_PATH


def test_dbtproject__handle_sql_file_model():
    dbt_project = DbtProject(
        project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
    )
    dbt_project.models = {}

    dbt_project._handle_sql_file(SAMPLE_MODEL_SQL_PATH)
    assert len(dbt_project.models) == 1
    assert "customers" in dbt_project.models
    raw_customers = dbt_project.models["customers"]
    assert raw_customers.name == "customers"
    assert raw_customers.type == DbtModelType.DBT_MODEL
    assert raw_customers.path == SAMPLE_MODEL_SQL_PATH


def test_dbtproject__handle_sql_file_snapshot():
    dbt_project = DbtProject(
        project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
    )
    dbt_project.models = {}

    dbt_project._handle_sql_file(SAMPLE_SNAPSHOT_SQL_PATH)
    assert len(dbt_project.models) == 1
    assert "orders" in dbt_project.models
    raw_customers = dbt_project.models["orders"]
    assert raw_customers.name == "orders"
    assert raw_customers.type == DbtModelType.DBT_MODEL
    assert raw_customers.path == SAMPLE_SNAPSHOT_SQL_PATH


def test_dbtproject__handle_config_file_empty_file():
    with NamedTemporaryFile("w") as tmp_fp:
        tmp_fp.flush()
        sample_config_file_path = Path(tmp_fp.name)

        dbt_project = DbtProject(project_name="empty_project")
        assert not dbt_project.models
        dbt_project._handle_config_file(sample_config_file_path)
        assert not dbt_project.models


def test_dbtproject__handle_config_file_with_unknown_name():
    yaml_data = {"models": [{"name": "unknown"}]}
    with NamedTemporaryFile("w") as tmp_fp:
        yaml.dump(yaml_data, tmp_fp)
        tmp_fp.flush()

        sample_config_file_path = Path(tmp_fp.name)
        dbt_project = DbtProject(project_name="empty_project")
        assert not dbt_project.models
        dbt_project._handle_config_file(sample_config_file_path)
        assert not dbt_project.models


@pytest.mark.parametrize(
    "input_tags,expected_config_selectors",
    [
        ("some_tag", {"materialized:view", "tags:some_tag"}),
        (["tag1", "tag2"], {"materialized:view", "tags:tag1", "tags:tag2"}),
    ],
)
def test_dbtproject__handle_config_file_with_selector(input_tags, expected_config_selectors):
    dbt_project = DbtProject(
        project_name="jaffle_shop",
        dbt_root_path=DBT_PROJECT_PATH,
    )
    assert dbt_project.models["orders"].config.config_selectors == {"materialized:view"}

    with NamedTemporaryFile("w") as tmp_fp:
        yaml_data = {"models": [{"name": "orders", "config": {"tags": input_tags}}]}
        yaml.dump(yaml_data, tmp_fp)
        tmp_fp.flush()

        sample_config_file_path = Path(tmp_fp.name)
        dbt_project._handle_config_file(sample_config_file_path)
        assert dbt_project.models["orders"].config.config_selectors == expected_config_selectors


def test_dbtmodelconfig___repr__():
    dbt_model = DbtModel(name="some_name", type=DbtModelType.DBT_MODEL, path=SAMPLE_MODEL_SQL_PATH)
    expected_start = (
        "DbtModel(name='some_name', type='DbtModelType.DBT_MODEL', "
        "path='dev/dags/dbt/jaffle_shop/models/customers.sql', config=DbtModelConfig(config_selectors=set(), "
        "upstream_models={'stg_"
    )
    assert str(dbt_model).startswith(expected_start)


class KeywordArgValue:
    def as_const(self):
        return self


class KeywordArgValueList(KeywordArgValue, list):
    pass


class KeywordArgValueStr(KeywordArgValue, str):
    pass


@dataclass
class KeywordArg:
    key: str
    value: KeywordArgValue | List


def test_dbtmodelconfig_extract_config_non_kwarg():
    dbt_model = DbtModel(name="some_name", type=DbtModelType.DBT_MODEL, path=SAMPLE_MODEL_SQL_PATH)
    kwarg = {}
    config_name = "abc"
    computed = dbt_model._extract_config(kwarg, config_name)
    assert computed is None


def test_dbtmodelconfig_extract_config_with_kwarg_list_without_as_const(caplog):
    dbt_model = DbtModel(name="some_name", type=DbtModelType.DBT_MODEL, path=SAMPLE_MODEL_SQL_PATH)
    kwarg = KeywordArg(key="some_conf", value=[1, 2])
    config_name = "some_conf"
    with caplog.at_level(logging.WARN):
        computed = dbt_model._extract_config(kwarg, config_name)
    assert computed is None
    expected_log = (
        "Could not parse some_conf from config in dev/dags/dbt/jaffle_shop/models/customers.sql: "
        "'list' object has no attribute 'as_const'"
    )
    assert expected_log in caplog.text


def test_dbtmodelconfig_extract_config_with_kwarg_list():
    dbt_model = DbtModel(name="some_name", type=DbtModelType.DBT_MODEL, path=SAMPLE_MODEL_SQL_PATH)
    kwarg = KeywordArg(key="some_conf", value=KeywordArgValueList([1, 2]))
    config_name = "some_conf"
    computed = dbt_model._extract_config(kwarg, config_name)
    expected = ["some_conf:1", "some_conf:2"]
    assert computed == expected


def test_dbtmodelconfig_extract_config_with_kwarg_str():
    dbt_model = DbtModel(name="some_name", type=DbtModelType.DBT_MODEL, path=SAMPLE_MODEL_SQL_PATH)
    kwarg = KeywordArg(key="some_conf", value=KeywordArgValueStr("abc"))
    config_name = "some_conf"
    computed = dbt_model._extract_config(kwarg, config_name)
    expected = ["some_conf:abc"]
    assert computed == expected
