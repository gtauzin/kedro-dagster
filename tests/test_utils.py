# mypy: ignore-errors

from kedro_dagster.utils import _is_asset_name, format_dataset_name, unformat_asset_name


def test_format_and_unformat_asset_name_are_inverses():
    name = "my_dataset.with.dots"
    dagster = format_dataset_name(name)
    assert dagster == "my_dataset__with__dots"
    assert unformat_asset_name(dagster) == name


def test_is_asset_name_true():
    assert _is_asset_name("my_asset")
    assert _is_asset_name("another_asset__with__underscores")


def test_is_asset_name_false():
    assert not _is_asset_name("parameters")
    assert not _is_asset_name("params:my_param")
