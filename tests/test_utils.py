# mypy: ignore-errors

from pathlib import Path

import dagster as dg
from pydantic import BaseModel

from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_param_name,
    format_dataset_name,
    format_node_name,
    get_asset_key_from_dataset_name,
    get_filter_params_dict,
    get_mlflow_resource_from_config,
    is_mlflow_enabled,
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


def test_get_asset_key_from_dataset_name():
    asset_key = get_asset_key_from_dataset_name("my.dataset", "dev")
    assert asset_key == dg.AssetKey(["dev", "my", "dataset"])


def test_format_node_name():
    formatted_name = format_node_name("my.node.name")
    assert formatted_name == "my__node__name"

    invalid_name = "my.node@name"
    formatted_invalid_name = format_node_name(invalid_name)
    assert formatted_invalid_name.startswith("unnamed_node_")


def test_create_pydantic_model_from_dict():
    params = {"param1": 1, "param2": "value"}
    model = _create_pydantic_model_from_dict("TestModel", params, BaseModel)
    instance = model(param1=1, param2="value")
    assert instance.param1 == 1
    assert instance.param2 == "value"


def test_is_mlflow_enabled():
    assert isinstance(is_mlflow_enabled(), bool)


def test_get_node_pipeline_name(mocker):
    mock_node = mocker.Mock()
    mock_node.name = "test.node"  # Ensure the name is a string
    mock_pipeline = mocker.Mock(nodes=[mock_node])
    mock_find_pipelines = mocker.patch("kedro_dagster.utils.find_pipelines", return_value={"pipeline": mock_pipeline})

    pipeline_name = _get_node_pipeline_name(mock_node)
    assert pipeline_name == "test__pipeline"
    mock_find_pipelines.assert_called_once()


def test_get_filter_params_dict():
    pipeline_config = {
        "tags": ["tag1"],
        "from_nodes": ["node1"],
        "to_nodes": ["node2"],
        "node_names": ["node3"],
        "from_inputs": ["input1"],
        "to_outputs": ["output1"],
        "node_namespace": "namespace",
    }
    filter_params = get_filter_params_dict(pipeline_config)
    assert filter_params == pipeline_config


def test_get_mlflow_resource_from_config(mocker):
    mock_mlflow_config = mocker.Mock(
        tracking=mocker.Mock(experiment=mocker.Mock(name="test_experiment")),
        server=mocker.Mock(mlflow_tracking_uri="http://localhost:5000"),
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


def test_is_asset_name():
    assert not _is_param_name("my_asset")
    assert not _is_param_name("another_asset__with__underscores")
    assert _is_param_name("parameters")
    assert _is_param_name("params:my_param")


def test_format_node_name_hashes_invalid_chars():
    # Names with characters outside [A-Za-z0-9_] should be hashed
    name = "node-with-hyphen"
    formatted = format_node_name(name)
    assert formatted.startswith("unnamed_node_")
