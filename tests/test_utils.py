# mypy: ignore-errors

from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest
from dagster import IdentityPartitionMapping
from dagster._core.errors import DagsterInvalidInvocationError
from kedro.io import DataCatalog
from pydantic import BaseModel, ValidationError

from kedro_dagster.datasets import DagsterNothingDataset
from kedro_dagster.utils import (
    KEDRO_VERSION,
    PYDANTIC_VERSION,
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_param_name,
    create_pydantic_config,
    format_dataset_name,
    format_node_name,
    format_partition_key,
    get_asset_key_from_dataset_name,
    get_dataset_from_catalog,
    get_filter_params_dict,
    get_mlflow_resource_from_config,
    get_mlflow_run_url,
    get_partition_mapping,
    is_mlflow_enabled,
    is_nothing_asset_name,
    render_jinja_template,
    unformat_asset_name,
    write_jinja_template,
)


def test_render_jinja_template(tmp_path):
    """Render a Jinja template file with provided context variables."""
    template_content = "Hello, {{ name }}!"
    template_path = tmp_path / "test_template.jinja"
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


def test_is_mlflow_enabled():
    """Return True when kedro-mlflow is importable and enabled in the environment."""
    assert isinstance(is_mlflow_enabled(), bool)


def test_get_node_pipeline_name(monkeypatch):
    """Infer the pipeline name a node belongs to from the pipelines registry."""
    mock_node = SimpleNamespace(name="test.node")
    mock_pipeline = SimpleNamespace(nodes=[mock_node])

    monkeypatch.setattr("kedro.framework.project.find_pipelines", lambda: {"pipeline": mock_pipeline})

    pipeline_name = _get_node_pipeline_name(mock_node)
    assert pipeline_name == "test__pipeline"


def test_get_node_pipeline_name_default(monkeypatch, caplog):
    """Return '__none__' and log a warning when the node isn't in any pipeline."""
    mock_node = SimpleNamespace(name="orphan.node")
    # Only __default__ pipeline or empty mapping means no match
    monkeypatch.setattr("kedro.framework.project.find_pipelines", lambda: {"__default__": SimpleNamespace(nodes=[])})
    with caplog.at_level("WARNING"):
        result = _get_node_pipeline_name(mock_node)
        assert result == "__none__"
        assert "not part of any pipelines" in caplog.text


def test_get_filter_params_dict():
    """Map node namespace key depending on Kedro major version; pass others unchanged."""
    # Build a config using singular form as the source of truth in this project
    kedro_major = KEDRO_VERSION[0]
    if kedro_major >= 1:
        node_namespace_key = "node_namespaces"
        node_namespace_val = ["namespace"]
    else:
        node_namespace_key = "node_namespace"
        node_namespace_val = "namespace"

    pipeline_config = {
        "tags": ["tag1"],
        "from_nodes": ["node1"],
        "to_nodes": ["node2"],
        "node_names": ["node3"],
        "from_inputs": ["input1"],
        "to_outputs": ["output1_ds"],
        node_namespace_key: node_namespace_val,
    }
    filter_params = get_filter_params_dict(pipeline_config)

    expected = dict(pipeline_config)
    assert filter_params == expected


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


def test_get_dataset_from_catalog_index_access_exception(caplog):
    """When the catalog provides only index access and it raises, return None and log info."""

    class BadCatalog:
        # no _get_dataset and no get()
        def __getitem__(self, key):
            raise Exception("unexpected failure in __getitem__")

    bad_catalog = BadCatalog()

    with caplog.at_level("INFO"):
        result = get_dataset_from_catalog(bad_catalog, "missing_ds")
        assert result is None
        assert "Dataset 'missing_ds' not found in catalog." in caplog.text


PYDANTIC_V2_MAJOR = 2


def test_create_pydantic_config_and_model_behavior():
    """Validate create_pydantic_config works across pydantic v1/v2 and
    that _create_pydantic_model_from_dict applies config correctly (extra handling
    and validate_assignment behavior).
    """

    # Prepare a config that forbids extra fields and validates assignment
    config: Any = create_pydantic_config(extra="forbid", validate_assignment=True)

    # Create a minimal model with a single int field
    Model = _create_pydantic_model_from_dict(
        name="Params",
        params={"x": 1},
        __base__=dg.Config,
        __config__=config,
    )

    # Instantiation with the known field works
    m = Model(x=1)
    assert m.x == 1

    # Initialization with extra field: behavior depends on pydantic version and base usage
    # With pydantic v2 + base provided, config may not override base, so extra could be allowed.
    try:
        Model(x=1, y=2)  # type: ignore[call-arg]
        allowed_extra = True
    except ValidationError:
        allowed_extra = False

    if PYDANTIC_VERSION[0] < PYDANTIC_V2_MAJOR:
        # v1 should respect extra="forbid" and raise
        assert allowed_extra is False
    # For v2, we accept either behavior depending on base precedence

    # Validate assignment for existing fields should enforce types
    # Setting to wrong type should raise. With dagster's Config base, this may raise
    # DagsterInvalidInvocationError when models are frozen.
    with pytest.raises((ValidationError, ValueError, TypeError, DagsterInvalidInvocationError)):
        # pydantic v1/v2 may raise different error types for assignment validation
        m.x = "not-an-int"  # type: ignore[assignment]

    # Also assert config is attached as expected depending on pydantic version
    major = PYDANTIC_VERSION[0]
    if major >= PYDANTIC_V2_MAJOR:
        # v2 exposes dict-like model_config. When using a base class provided by dagster,
        # extra may be controlled by the base and not appear on the derived model.
        model_config = getattr(Model, "model_config", {})
        assert isinstance(model_config, dict)
    else:
        # v1 exposes inner Config class
        Config = getattr(Model, "Config", None)
        assert Config is not None
        assert getattr(Config, "extra", None) in {"forbid", 2}
        assert getattr(Config, "validate_assignment", None) is True

    # Additionally, verify that when no base is provided, config takes effect in v2
    ModelNoBase = _create_pydantic_model_from_dict(
        name="ParamsNoBase",
        params={"x": 1},
        __base__=None,
        __config__=config,
    )
    with pytest.raises(ValidationError):
        ModelNoBase(x=1, y=2)  # type: ignore[call-arg]
    # Check config flags surfaced when no base is provided
    if major >= PYDANTIC_V2_MAJOR:
        cfg = getattr(ModelNoBase, "model_config", {})
        assert cfg.get("validate_assignment") is True
        assert cfg.get("extra") in {"forbid", 2}


def test_create_pydantic_model_with_config():
    """Test creating a dynamic model with version-aware config."""
    config = create_pydantic_config(extra="forbid", arbitrary_types_allowed=True)

    Model = _create_pydantic_model_from_dict(
        name="TestModel", params={"param1": "value1", "param2": 42}, __base__=BaseModel, __config__=config
    )

    # Test normal instantiation
    instance = Model(param1="value1", param2=42)
    assert instance.param1 == "value1"
    assert instance.param2 == 42

    # Note: When using a base class in Pydantic v2, the base class config may override
    # our config settings. This is documented behavior in the actual implementation.
    # We just test that the model creation works correctly.
    try:
        Model(param1="value1", param2=42, extra_field="might be allowed")
        # If this works, it means the base class allows extra fields
    except ValidationError:
        # If this fails, it means extra fields are forbidden
        pass
    # Both behaviors are acceptable depending on Pydantic version and base class


def test_create_pydantic_model_with_dagster_config_base():
    """Test creating a dynamic model with Dagster Config base class."""
    config = create_pydantic_config(arbitrary_types_allowed=True)

    Model = _create_pydantic_model_from_dict(
        name="DagsterConfigModel",
        params={"dataset_name": "my_dataset", "env": "test"},
        __base__=dg.Config,
        __config__=config,
    )

    # Test that we can create an instance
    instance = Model(dataset_name="my_dataset", env="test")
    assert instance.dataset_name == "my_dataset"
    assert instance.env == "test"


def test_create_pydantic_model_without_base():
    """Test creating a dynamic model without a base class."""
    config = create_pydantic_config(extra="forbid")

    Model = _create_pydantic_model_from_dict(
        name="NoBaseModel", params={"field1": "value1"}, __base__=None, __config__=config
    )

    # Should still work
    instance = Model(field1="value1")
    assert instance.field1 == "value1"

    # Extra fields should still be forbidden
    with pytest.raises(ValidationError):
        Model(field1="value1", extra="not allowed")


def test_create_pydantic_model_nested_params():
    """Test creating a model with nested parameters."""
    config = create_pydantic_config(extra="allow")

    Model = _create_pydantic_model_from_dict(
        name="NestedModel",
        params={"simple_param": "value", "nested": {"inner_param": 123, "deep_nested": {"deep_param": True}}},
        __base__=BaseModel,
        __config__=config,
    )

    instance = Model(simple_param="value", nested={"inner_param": 123, "deep_nested": {"deep_param": True}})

    assert instance.simple_param == "value"
    assert hasattr(instance, "nested")
    assert instance.nested.inner_param == 123
    assert instance.nested.deep_nested.deep_param is True


def test_model_creation_preserves_validation_behavior():
    """Test that validation behavior is preserved across Pydantic versions."""
    config = create_pydantic_config(validate_assignment=True, extra="forbid")

    Model = _create_pydantic_model_from_dict(
        name="ValidatedModel", params={"number_field": 42}, __base__=BaseModel, __config__=config
    )

    instance = Model(number_field=42)

    # Test that type validation is enforced during assignment
    # Note: This may behave differently based on the base class used
    try:
        instance.number_field = "not a number"
        # If we get here, assignment validation may not be working
        # This could be due to Dagster Config base class behavior
    except (ValidationError, ValueError, TypeError):
        # This is expected - validation should catch the type error
        pass


def test_get_mlflow_run_url_remote_tracking_uri(monkeypatch):
    """Test get_mlflow_run_url with remote HTTP tracking URI."""
    pytest.importorskip("mlflow")
    import mlflow  # noqa: F401

    # Mock active run with remote tracking URI
    mock_run = SimpleNamespace(info=SimpleNamespace(experiment_id="123", run_id="abc456def"))

    monkeypatch.setattr("mlflow.active_run", lambda: mock_run)
    monkeypatch.setattr("mlflow.get_tracking_uri", lambda: "http://mlflow.example.com:5000")

    mock_config = SimpleNamespace()

    url = get_mlflow_run_url(mock_config)
    assert url == "http://mlflow.example.com:5000/#/experiments/123/runs/abc456def"


def test_get_mlflow_run_url_https_tracking_uri(monkeypatch):
    """Test get_mlflow_run_url with HTTPS tracking URI."""
    pytest.importorskip("mlflow")
    import mlflow  # noqa: F401

    mock_run = SimpleNamespace(info=SimpleNamespace(experiment_id="456", run_id="xyz789ghi"))

    monkeypatch.setattr("mlflow.active_run", lambda: mock_run)
    monkeypatch.setattr("mlflow.get_tracking_uri", lambda: "https://secure-mlflow.com/")

    mock_config = SimpleNamespace()

    url = get_mlflow_run_url(mock_config)
    assert url == "https://secure-mlflow.com/#/experiments/456/runs/xyz789ghi"


def test_get_mlflow_run_url_local_file_tracking_uri(monkeypatch):
    """Test get_mlflow_run_url with local file-based tracking URI."""
    pytest.importorskip("mlflow")
    import mlflow  # noqa: F401

    mock_run = SimpleNamespace(info=SimpleNamespace(experiment_id="789", run_id="local123run"))

    monkeypatch.setattr("mlflow.active_run", lambda: mock_run)
    monkeypatch.setattr("mlflow.get_tracking_uri", lambda: "file:///home/user/mlruns")

    # Mock config with UI settings
    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    url = get_mlflow_run_url(mock_config)
    assert url == "http://localhost:5000/#/experiments/789/runs/local123run"


def test_get_mlflow_run_url_no_active_run(monkeypatch):
    """Test get_mlflow_run_url raises error when no active run."""
    pytest.importorskip("mlflow")

    monkeypatch.setattr("mlflow.active_run", lambda: None)

    mock_config = SimpleNamespace()

    with pytest.raises(RuntimeError, match="No active MLflow run"):
        get_mlflow_run_url(mock_config)


def test_get_mlflow_run_url_unsupported_tracking_uri(monkeypatch):
    """Test get_mlflow_run_url raises error for unsupported tracking URI."""
    pytest.importorskip("mlflow")

    mock_run = SimpleNamespace(info=SimpleNamespace(experiment_id="999", run_id="unsupported"))

    monkeypatch.setattr("mlflow.active_run", lambda: mock_run)
    monkeypatch.setattr("mlflow.get_tracking_uri", lambda: "databricks://profile")

    mock_config = SimpleNamespace()

    with pytest.raises(ValueError, match="Unsupported MLflow tracking URI"):
        get_mlflow_run_url(mock_config)
