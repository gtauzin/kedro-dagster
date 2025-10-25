# mypy: ignore-errors

from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
from dagster import IdentityPartitionMapping
from kedro.io import DataCatalog
from pydantic import BaseModel

from kedro_dagster.datasets import DagsterNothingDataset
from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_param_name,
    format_dataset_name,
    format_node_name,
    format_partition_key,
    get_asset_key_from_dataset_name,
    get_filter_params_dict,
    get_mlflow_resource_from_config,
    get_partition_mapping,
    is_mlflow_enabled,
    is_nothing_asset_name,
    render_jinja_template,
    unformat_asset_name,
    write_jinja_template,
)


def test_render_jinja_template():
    template_content = "Hello, {{ name }}!"
    template_path = Path("/tmp/test_template.jinja")
    template_path.write_text(template_content)

    result = render_jinja_template(template_path, name="World")
    assert result == "Hello, World!"


def test_write_jinja_template(tmp_path):
    src = tmp_path / "template.jinja"
    dst = tmp_path / "output.txt"
    src.write_text("Hello, {{ name }}!")

    write_jinja_template(src, dst, name="Dagster")
    assert dst.read_text() == "Hello, Dagster!"


def test_render_jinja_template_cookiecutter(tmp_path):
    # Cookiecutter-style rendering path
    src = tmp_path / "cookie.jinja"
    src.write_text("{{ cookiecutter.project_slug }}")
    rendered = render_jinja_template(src, is_cookiecutter=True, project_slug="kedro_dagster")
    assert rendered == "kedro_dagster"


def test_get_asset_key_from_dataset_name():
    asset_key = get_asset_key_from_dataset_name("my.dataset", "dev")
    assert asset_key == dg.AssetKey(["dev", "my", "dataset"])


def test_format_node_name():
    formatted_name = format_node_name("my.node.name")
    assert formatted_name == "my__node__name"

    invalid_name = "my.node@name"
    formatted_invalid_name = format_node_name(invalid_name)
    assert formatted_invalid_name.startswith("unnamed_node_")


def test_format_partition_key():
    assert format_partition_key("2024-01-01") == "2024_01_01"
    assert format_partition_key("a b/c") == "a_b_c"
    # Only underscores become empty -> fallback to "all"
    assert format_partition_key("__") == "all"


def test_create_pydantic_model_from_dict():
    INNER_VALUE = 42
    params = {"param1": 1, "param2": "value", "nested": {"inner": INNER_VALUE}}
    model = _create_pydantic_model_from_dict("TestModel", params, BaseModel)
    instance = model(param1=1, param2="value", nested={"inner": INNER_VALUE})
    assert instance.param1 == 1
    assert instance.param2 == "value"
    assert hasattr(instance, "nested")
    assert instance.nested.inner == INNER_VALUE


def test_is_mlflow_enabled():
    assert isinstance(is_mlflow_enabled(), bool)


def test_get_node_pipeline_name(monkeypatch):
    mock_node = SimpleNamespace(name="test.node")
    mock_pipeline = SimpleNamespace(nodes=[mock_node])

    def fake_find_pipelines():
        return {"pipeline": mock_pipeline}

    monkeypatch.setattr("kedro_dagster.utils.find_pipelines", lambda: {"pipeline": mock_pipeline})

    pipeline_name = _get_node_pipeline_name(mock_node)  # type: ignore[arg-type]
    assert pipeline_name == "test__pipeline"


def test_get_node_pipeline_name_default(monkeypatch, caplog):
    mock_node = SimpleNamespace(name="orphan.node")
    # Only __default__ pipeline or empty mapping means no match
    monkeypatch.setattr("kedro_dagster.utils.find_pipelines", lambda: {"__default__": SimpleNamespace(nodes=[])})
    with caplog.at_level("WARNING"):
        result = _get_node_pipeline_name(mock_node)  # type: ignore[arg-type]
        assert result == "__none__"
        assert "not part of any pipelines" in caplog.text


def test_get_filter_params_dict():
    pipeline_config = {
        "tags": ["tag1"],
        "from_nodes": ["node1"],
        "to_nodes": ["node2"],
        "node_names": ["node3"],
        "from_inputs": ["input1"],
        "to_outputs": ["output1_ds"],
        "node_namespace": "namespace",
    }
    filter_params = get_filter_params_dict(pipeline_config)
    assert filter_params == pipeline_config


def test_get_mlflow_resource_from_config():
    # Only run this test when kedro-mlflow is available
    pytest.importorskip("kedro_mlflow")
    mock_mlflow_config = SimpleNamespace(
        tracking=SimpleNamespace(experiment=SimpleNamespace(name="test_experiment")),
        server=SimpleNamespace(mlflow_tracking_uri="http://localhost:5000"),
    )
    resource = get_mlflow_resource_from_config(mock_mlflow_config)
    assert isinstance(resource, dg.ResourceDefinition)


def test_format_and_unformat_asset_name_are_inverses():
    name = "my_dataset.with.dots"
    dagster = format_dataset_name(name)
    assert dagster == "my_dataset__with__dots"
    assert unformat_asset_name(dagster) == name


def test_format_dataset_name_non_dot_chars():
    name = "dataset-with-hyphen.and.dot"
    dagster_name = format_dataset_name(name)
    assert dagster_name == "dataset__with__hyphen__and__dot"
    assert unformat_asset_name(dagster_name) != name


def test_is_nothing_asset_name_with_catalog():
    # Kedro DataCatalog path using private _get_dataset
    catalog = DataCatalog(datasets={"nothing": DagsterNothingDataset()})
    assert is_nothing_asset_name(catalog, "nothing") is True
    assert is_nothing_asset_name(catalog, "missing") is False


def test_get_partition_mapping_exact_and_pattern(monkeypatch, caplog):
    class DummyResolver:
        def match_pattern(self, name):  # noqa: D401
            # Simulate pattern match for values starting with "foo"
            return "pattern" if name.startswith("foo") else None

    # Exact dataset name match (formatting does not change the key)
    mappings = {"down_asset": IdentityPartitionMapping()}
    mapping = get_partition_mapping(mappings, "up", ["down_asset"], DummyResolver())
    assert isinstance(mapping, IdentityPartitionMapping)

    # Pattern match path
    mappings2 = {"pattern": IdentityPartitionMapping()}
    mapping2 = get_partition_mapping(mappings2, "up", ["foo.bar"], DummyResolver())
    assert isinstance(mapping2, IdentityPartitionMapping)

    # No downstream datasets in mappings -> warning and None
    with caplog.at_level("WARNING"):
        mapping3 = get_partition_mapping({}, "upstream", ["zzz"], DummyResolver())
        assert mapping3 is None
        assert "default partition mapping" in caplog.text.lower()


def test_format_dataset_name_rejects_reserved_identifiers():
    # Reserved names should raise to avoid Dagster conflicts
    with pytest.raises(ValueError):
        format_dataset_name("input")
    with pytest.raises(ValueError):
        format_dataset_name("output")


def test_is_asset_name():
    assert not _is_param_name("my_ds")
    assert not _is_param_name("another_dataset__with__underscores")
    assert _is_param_name("parameters")
    assert _is_param_name("params:my_param")


def test_format_node_name_hashes_invalid_chars():
    # Names with characters outside [A-Za-z0-9_] should be hashed
    name = "node-with-hyphen"
    formatted = format_node_name(name)
    assert formatted.startswith("unnamed_node_")
