# mypy: ignore-errors

from __future__ import annotations

import importlib

import dagster as dg
import pandas as pd
import pytest
import yaml
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.utils import format_dataset_name


@pytest.mark.parametrize(
    "kedro_project_scenario_env",
    [
        ("exec_filebacked", "base"),
        ("partitioned_intermediate_output2", "local"),
        ("partitioned_static_mapping", "base"),
        ("multiple_inputs", "local"),
        ("multiple_outputs_tuple", "base"),
        ("multiple_outputs_dict", "local"),
        ("no_outputs_node", "base"),
        ("nothing_assets", "local"),
    ],
    indirect=True,
)
def test_catalog_translator_covers_scenarios(kedro_project_scenario_env):
    """Test CatalogTranslator across diverse scenarios and assert core invariants.

    - File-backed datasets (CSVDataset) get IO managers.
    - Partitioned datasets expose partitions_def in asset_partitions.
    - Memory datasets do not result in dedicated IO managers.
    """
    project_path, options = kedro_project_scenario_env
    env = options.env

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    pipeline = pipelines.get("__default__")
    translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )

    named_io_managers, asset_partitions = translator.to_dagster()

    assert isinstance(named_io_managers, dict)
    assert isinstance(asset_partitions, dict)

    # Validate IO managers for file-backed datasets and absence for MemoryDataset
    # Read the raw catalog config written by the scenario builder
    # Use YAML as Kedro configs are YAML files (JSON subset is valid YAML)
    with open(project_path / "conf" / env / "catalog.yml", encoding="utf-8") as f:
        options_catalog = yaml.safe_load(f)

    for ds_name, ds_cfg in options_catalog.items():
        ds_type = ds_cfg.get("type")
        asset_name = format_dataset_name(ds_name)
        if ds_type == "pandas.CSVDataset":
            assert f"{env}__{asset_name}_io_manager" in named_io_managers
        elif ds_type == "MemoryDataset":
            assert f"{env}__{asset_name}_io_manager" not in named_io_managers
        elif ds_type == "kedro_dagster.datasets.DagsterPartitionedDataset":
            # Partitioned dataset should have partitions_def registered
            assert asset_name in asset_partitions
            assert isinstance(asset_partitions[asset_name]["partitions_def"], dg.PartitionsDefinition)
            # IO manager naming is allowed but not required; do not assert here to avoid false negatives
        # Other dataset types (e.g., DagsterNothingDataset) have no IO managers
        # TODO: Is that correct? Should we assert absence here?
        # elif ds_type == "kedro_dagster.datasets.DagsterNothingDataset":
        #     assert f"{env}__{ds_name}_io_manager" not in named_io_managers


@pytest.mark.parametrize(
    "kedro_project_scenario_env",
    [("exec_filebacked", "base"), ("exec_filebacked", "local")],
    indirect=True,
)
def test_catalog_translator_builds_configurable_io_managers(kedro_project_scenario_env):
    """Ensure IO managers are created with expected names, types and config.

    We validate for file-backed datasets (CSVDataset):
    - IO manager key naming: "{env}__{asset_name}_io_manager"
    - Value is a ConfigurableIOManager instance exposing handle_output/load_input
    - The pydantic config fields reflect the catalog entry (dataset, filepath)
    """
    project_path, options = kedro_project_scenario_env
    env = options.env

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    # Force Kedro to use the current project's package for the pipeline registry
    src_dir = project_path / "src"
    pkg_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if pkg_dirs:
        package_name = pkg_dirs[0].name
        project_module = importlib.import_module("kedro.framework.project")
        project_module.configure_project(package_name)

    pipeline = pipelines.get("__default__")
    translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )

    named_io_managers, _ = translator.to_dagster()

    # Read the raw catalog config to compare against the generated IO manager config
    with open(project_path / "conf" / env / "catalog.yml", encoding="utf-8") as f:
        options_catalog = yaml.safe_load(f)

    for ds_name, ds_cfg in options_catalog.items():
        ds_type = ds_cfg.get("type")
        if ds_type != "pandas.CSVDataset":
            # Limit strict checks to file-backed datasets for this test
            continue

        asset_name = format_dataset_name(ds_name)
        key = f"{env}__{asset_name}_io_manager"
        assert key in named_io_managers, f"Missing IO manager for {ds_name}"

        io_mgr = named_io_managers[key]

        # It should behave like a Dagster ConfigurableIOManager
        assert hasattr(io_mgr, "handle_output") and callable(getattr(io_mgr, "handle_output"))
        assert hasattr(io_mgr, "load_input") and callable(getattr(io_mgr, "load_input"))

        # Config fields should be present on the instance
        # dataset field equals the class short name (e.g., CSVDataset)
        assert getattr(io_mgr, "dataset", None) == "CSVDataset"

        # filepath is commonly configured for CSVDataset; if present, it must match
        if "filepath" in ds_cfg:
            io_fp = getattr(io_mgr, "filepath", None)
            rel_fp = ds_cfg["filepath"]
            abs_fp = str((project_path / rel_fp).resolve())
            assert io_fp in {rel_fp, abs_fp}

        # Docstring carries the dataset name for clarity
        assert ds_name in (getattr(io_mgr.__class__, "__doc__", "") or "")


@pytest.mark.parametrize(
    "kedro_project_scenario_env",
    [("exec_filebacked", "base"), ("exec_filebacked", "local")],
    indirect=True,
)
def test_io_manager_roundtrip_matches_dataset(kedro_project_scenario_env):
    """Saving/loading via the IO manager should match the underlying Kedro dataset.

    For each CSVDataset in the scenario, we write a small DataFrame through the
    generated IO manager and verify that loading through both the dataset and the
    IO manager produces identical DataFrames.
    """
    project_path, options = kedro_project_scenario_env
    env = options.env

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    src_dir = project_path / "src"
    pkg_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if pkg_dirs:
        package_name = pkg_dirs[0].name
        project_module = importlib.import_module("kedro.framework.project")
        project_module.configure_project(package_name)

    pipeline = pipelines.get("__default__")
    translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, _ = translator.to_dagster()

    # Load the scenario's catalog to enumerate CSVDatasets
    with open(project_path / "conf" / env / "catalog.yml", encoding="utf-8") as f:
        options_catalog = yaml.safe_load(f)

    # Minimal Dagster op definition to build IO manager contexts
    @dg.op(name="dummy_node")
    def _noop():  # pragma: no cover - execution not needed
        return None

    op_def = _noop

    # Simple test frame to persist
    df_to_write = pd.DataFrame({"a": [1, 2, 3]})

    for ds_name, ds_cfg in options_catalog.items():
        if ds_cfg.get("type") != "pandas.CSVDataset":
            continue

        asset_name = format_dataset_name(ds_name)
        key = f"{env}__{asset_name}_io_manager"
        assert key in named_io_managers
        io_mgr = named_io_managers[key]

        # Save via IO manager
        out_ctx = dg.build_output_context(op_def=op_def, name="result")
        io_mgr.handle_output(out_ctx, df_to_write)

        # Load via dataset
        dataset = context.catalog._get_dataset(ds_name)
        df_via_dataset = dataset.load()

        # Load via IO manager
        upstream_out_ctx = dg.build_output_context(op_def=op_def, name="result")
        in_ctx = dg.build_input_context(op_def=op_def, upstream_output=upstream_out_ctx)
        df_via_io_mgr = io_mgr.load_input(in_ctx)

        # Both should equal the source
        assert list(df_via_dataset.columns) == list(df_to_write.columns)
        assert list(df_via_io_mgr.columns) == list(df_to_write.columns)
        assert df_via_dataset.equals(df_to_write)
        assert df_via_io_mgr.equals(df_to_write)


@pytest.mark.parametrize(
    "kedro_project_scenario_env",
    [("exec_filebacked", "base"), ("exec_filebacked", "local")],
    indirect=True,
)
def test_create_dataset_config_contains_parameters(kedro_project_scenario_env):
    """The dataset config built by CatalogTranslator should reflect dataset._describe().

    We focus on CSVDatasets in the exec_filebacked scenario and verify that the
    translator's helper returns a superset containing:
      - dataset: short class name (e.g., CSVDataset)
      - entries from _describe() (except version), with PurePosixPath converted to str
    """
    project_path, options = kedro_project_scenario_env
    env = options.env

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    src_dir = project_path / "src"
    pkg_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if pkg_dirs:
        package_name = pkg_dirs[0].name
        project_module = importlib.import_module("kedro.framework.project")
        project_module.configure_project(package_name)

    pipeline = pipelines.get("__default__")
    translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )

    with open(project_path / "conf" / env / "catalog.yml", encoding="utf-8") as f:
        options_catalog = yaml.safe_load(f)

    # Only validate for file-backed CSVDatasets in this scenario
    for ds_name, ds_cfg in options_catalog.items():
        if ds_cfg.get("type") != "pandas.CSVDataset":
            continue
        dataset = context.catalog._get_dataset(ds_name)

        cfg_model = translator._create_dataset_config(dataset)
        cfg = cfg_model()

        # Must include dataset short name
        assert getattr(cfg, "dataset") == dataset.__class__.__name__ == "CSVDataset"

        # Must contain entries from _describe(), with path-like values converted to str
        described = dataset._describe()
        for key, val in described.items():
            if key == "version":
                continue
            expected = str(val) if hasattr(val, "__fspath__") or "PosixPath" in type(val).__name__ else val
            assert hasattr(cfg, key)
            actual = getattr(cfg, key)
            if hasattr(actual, "model_dump"):
                actual = actual.model_dump()
            assert actual == expected


@pytest.mark.parametrize("env", ["base", "local"])  # use existing per-env fixtures via request.getfixturevalue
def test_partitioned_io_manager_respects_partition_keys_via_tags_and_context(env, request):
    """Ensure IO manager for DagsterPartitionedDataset handles partition keys from tags and context.

    - Save via handle_output with downstream_partition_key tag -> data stored under that key.
    - Load via load_input with upstream_partition_key tag -> returns that partition.
    - Load via load_input with partition_key + asset_partitions_def in context -> returns that partition.
    """
    project_path, _ = request.getfixturevalue(f"kedro_project_partitioned_identity_mapping_{env}")

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    src_dir = project_path / "src"
    pkg_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if pkg_dirs:
        package_name = pkg_dirs[0].name
        project_module = importlib.import_module("kedro.framework.project")
        project_module.configure_project(package_name)

    pipeline = pipelines.get("__default__")
    translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = translator.to_dagster()

    # Pick a partitioned dataset from the catalog
    with open(project_path / "conf" / env / "catalog.yml", encoding="utf-8") as f:
        options_catalog = yaml.safe_load(f)

    part_ds_name = next(
        (
            n
            for n, cfg in options_catalog.items()
            if cfg.get("type") == "kedro_dagster.datasets.DagsterPartitionedDataset"
        ),
        None,
    )
    assert part_ds_name is not None, "No DagsterPartitionedDataset configured in this scenario"
    dataset = context.catalog._get_dataset(part_ds_name)
    asset_name = format_dataset_name(part_ds_name)
    io_key = f"{env}__{asset_name}_io_manager"
    assert io_key in named_io_managers
    io_mgr = named_io_managers[io_key]

    # Prepare a couple of valid partition keys from the partitions definition
    partitions_def = asset_partitions[asset_name]["partitions_def"]
    keys = partitions_def.get_partition_keys()
    assert len(keys) >= 2  # noqa: PLR2004
    k1, k2 = keys[0], keys[1]

    # Dataframe to persist and compare
    df1 = {k1: pd.DataFrame({"v": [1]})}
    df2 = {k2: pd.DataFrame({"v": [2]})}

    # Case 1: Save via IO manager with downstream_partition_key in op tags
    @dg.op(name="dummy_out", tags={"downstream_partition_key": f"{asset_name}|{k1}"})
    def _out():
        return None

    out_ctx = dg.build_output_context(op_def=_out, name="result")
    io_mgr.handle_output(out_ctx, df1)

    # Verify dataset holds that partition
    loaded_map = dataset.load()
    assert k1 in loaded_map
    df1_loaded = loaded_map[k1]() if callable(loaded_map[k1]) else loaded_map[k1]
    assert isinstance(df1_loaded, pd.DataFrame)
    assert df1_loaded.equals(df1[k1])

    # Case 2: Load via IO manager with upstream_partition_key in op tags
    @dg.op(name="dummy_in", tags={"upstream_partition_key": f"{asset_name}|{k1}"})
    def _in():
        return None

    upstream_out_ctx = dg.build_output_context(op_def=_out, name="result")
    in_ctx_tags = dg.build_input_context(op_def=_in, upstream_output=upstream_out_ctx)
    loaded_via_io_tags = io_mgr.load_input(in_ctx_tags)
    df_via_io_tags = loaded_via_io_tags[k1]() if callable(loaded_via_io_tags[k1]) else loaded_via_io_tags[k1]
    assert isinstance(df_via_io_tags, pd.DataFrame)
    assert df_via_io_tags.equals(df1[k1])

    # Case 3: Load via IO manager using context partition_key and partitions_def (no tags)
    # First, write a different partition via tags to ensure it exists
    @dg.op(name="dummy_out2", tags={"downstream_partition_key": f"{asset_name}|{k2}"})
    def _out2():
        return None

    out_ctx_k2 = dg.build_output_context(op_def=_out2, name="result")
    io_mgr.handle_output(out_ctx_k2, df2)

    # Now load using context-based partition info
    upstream_out_ctx2 = dg.build_output_context(op_def=_out2, name="result")

    # Use a tagless op to ensure context-based partition selection is used
    @dg.op(name="dummy_in_ctx")
    def _in_ctx():
        return None

    in_ctx_ctx = dg.build_input_context(
        op_def=_in_ctx,
        upstream_output=upstream_out_ctx2,
        partition_key=k2,
        asset_partitions_def=partitions_def,
    )
    loaded_via_io_ctx = io_mgr.load_input(in_ctx_ctx)
    df_via_io_ctx = loaded_via_io_ctx[k2]() if callable(loaded_via_io_ctx[k2]) else loaded_via_io_ctx[k2]
    assert isinstance(df_via_io_ctx, pd.DataFrame)
    assert df_via_io_ctx.equals(df2[k2])
