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
    _kedro_version,
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
    """Render a Jinja template file with provided context variables."""
    template_content = "Hello, {{ name }}!"
    template_path = Path("/tmp/test_template.jinja")
    template_path.write_text(template_content)

    result = render_jinja_template(template_path, name="World")
    assert result == "Hello, World!"


def test_write_jinja_template(tmp_path):
    """Write a rendered Jinja template to the destination path."""
    src = tmp_path / "template.jinja"
    dst = tmp_path / "output.txt"
    src.write_text("Hello, {{ name }}!")

    write_jinja_template(src, dst, name="Dagster")
    assert dst.read_text() == "Hello, Dagster!"


def test_render_jinja_template_cookiecutter(tmp_path):
    """Render templates in Cookiecutter mode using cookiecutter.* variables."""
    # Cookiecutter-style rendering path
    src = tmp_path / "cookie.jinja"
    src.write_text("{{ cookiecutter.project_slug }}")
    rendered = render_jinja_template(src, is_cookiecutter=True, project_slug="kedro_dagster")
    assert rendered == "kedro_dagster"


def test_get_asset_key_from_dataset_name():
    """Convert dataset name and env into a Dagster AssetKey path."""
    asset_key = get_asset_key_from_dataset_name("my.dataset", "dev")
    assert asset_key == dg.AssetKey(["dev", "my", "dataset"])


def test_format_node_name():
    """Format a node name for Dagster and hash when invalid characters are present."""
    formatted_name = format_node_name("my.node.name")
    assert formatted_name == "my__node__name"

    invalid_name = "my.node@name"
    formatted_invalid_name = format_node_name(invalid_name)
    assert formatted_invalid_name.startswith("unnamed_node_")


def test_format_partition_key():
    """Normalize partition key strings; fallback to 'all' when empty after normalization."""
    assert format_partition_key("2024-01-01") == "2024_01_01"
    assert format_partition_key("a b/c") == "a_b_c"

    with pytest.raises(ValueError) as exc:
        format_partition_key("__")

    assert str(exc.value) == "Partition key `__` cannot be formatted into a valid Dagster key."


def test_create_pydantic_model_from_dict():
    """Create a nested Pydantic model class from a dictionary schema."""
    INNER_VALUE = 42
    params = {"param1": 1, "param2": "value", "nested": {"inner": INNER_VALUE}}
    model = _create_pydantic_model_from_dict("TestModel", params, BaseModel)
    instance = model(param1=1, param2="value", nested={"inner": INNER_VALUE})
    assert instance.param1 == 1
    assert instance.param2 == "value"
    assert hasattr(instance, "nested")
    assert instance.nested.inner == INNER_VALUE


@pytest.mark.mlflow
def test_is_mlflow_enabled():
    """Return True when kedro-mlflow is importable and enabled in the environment."""
    assert isinstance(is_mlflow_enabled(), bool)


def test_get_node_pipeline_name(monkeypatch):
    """Infer the pipeline name a node belongs to from the pipelines registry."""
    mock_node = SimpleNamespace(name="test.node")
    mock_pipeline = SimpleNamespace(nodes=[mock_node])

    monkeypatch.setattr("kedro_dagster.utils.find_pipelines", lambda: {"pipeline": mock_pipeline})

    pipeline_name = _get_node_pipeline_name(mock_node)
    assert pipeline_name == "test__pipeline"


def test_get_node_pipeline_name_default(monkeypatch, caplog):
    """Return '__none__' and log a warning when the node isn't in any pipeline."""
    mock_node = SimpleNamespace(name="orphan.node")
    # Only __default__ pipeline or empty mapping means no match
    monkeypatch.setattr("kedro_dagster.utils.find_pipelines", lambda: {"__default__": SimpleNamespace(nodes=[])})
    with caplog.at_level("WARNING"):
        result = _get_node_pipeline_name(mock_node)
        assert result == "__none__"
        assert "not part of any pipelines" in caplog.text


def test_get_filter_params_dict():
    """Map node namespace key depending on Kedro major version; pass others unchanged."""
    # Build a config using singular form as the source of truth in this project
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
    # Determine Kedro major version in the test environment using the helper
    kedro_major = _kedro_version()[0]

    expected = dict(pipeline_config)
    if kedro_major >= 1:
        # Expect pluralized key for Kedro >= 1 and absence of the singular key
        expected.pop("node_namespace")
        expected["node_namespaces"] = "namespace"
    # For Kedro < 1, the dict should remain unchanged
    assert filter_params == expected


@pytest.mark.mlflow
def test_get_mlflow_resource_from_config():
    """Build a Dagster ResourceDefinition from a kedro-mlflow configuration object."""
    # Only run this test when kedro-mlflow is available
    pytest.importorskip("kedro_mlflow")
    mock_mlflow_config = SimpleNamespace(
        tracking=SimpleNamespace(experiment=SimpleNamespace(name="test_experiment")),
        server=SimpleNamespace(mlflow_tracking_uri="http://localhost:5000"),
    )
    resource = get_mlflow_resource_from_config(mock_mlflow_config)
    assert isinstance(resource, dg.ResourceDefinition)


def test_format_and_unformat_asset_name_are_inverses():
    """format_dataset_name and unformat_asset_name are inverses for dot-delimited names."""
    name = "my_dataset.with.dots"
    dagster = format_dataset_name(name)
    assert dagster == "my_dataset__with__dots"
    assert unformat_asset_name(dagster) == name


def test_format_dataset_name_non_dot_chars():
    """Formatting replaces non-dot separators; inversion isn't guaranteed in such cases."""
    name = "dataset-with-hyphen.and.dot"
    dagster_name = format_dataset_name(name)
    assert dagster_name == "dataset__with__hyphen__and__dot"
    assert unformat_asset_name(dagster_name) != name


def test_is_nothing_asset_name_with_catalog():
    """Detect Nothing datasets by name using the Kedro DataCatalog lookup."""
    # Kedro DataCatalog path using private _get_dataset
    catalog = DataCatalog(datasets={"nothing": DagsterNothingDataset()})
    assert is_nothing_asset_name(catalog, "nothing") is True
    assert is_nothing_asset_name(catalog, "missing") is False


def test_get_partition_mapping_exact_and_pattern(monkeypatch, caplog):
    """Resolve partition mapping by exact key or pattern; warn and return None when missing."""

    class DummyResolver:
        def match_pattern(self, name):
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
    """Reserved Dagster identifiers like 'input'/'output' should raise ValueError."""
    # Reserved names should raise to avoid Dagster conflicts
    with pytest.raises(ValueError):
        format_dataset_name("input")
    with pytest.raises(ValueError):
        format_dataset_name("output")


def test_is_asset_name():
    """Identify parameter-style names versus asset names."""
    assert not _is_param_name("my_ds")
    assert not _is_param_name("another_dataset__with__underscores")
    assert _is_param_name("parameters")
    assert _is_param_name("params:my_param")


def test_format_node_name_hashes_invalid_chars():
    """Names containing invalid characters are hashed to a stable 'unnamed_node_*' value."""
    # Names with characters outside [A-Za-z0-9_] should be hashed
    name = "node-with-hyphen"
    formatted = format_node_name(name)
    assert formatted.startswith("unnamed_node_")
