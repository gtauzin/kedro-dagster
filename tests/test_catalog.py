from unittest.mock import MagicMock

import dagster as dg
import pytest

from kedro_dagster.catalog import CatalogTranslator


class DummyDataset:
    def __init__(self):
        self.saved = False
        self.loaded = False

    def save(self, obj):
        self.saved = obj

    def load(self):
        self.loaded = True
        return "data"

    def _describe(self):
        return {"foo": "bar"}


class DummyHookManager:
    def __init__(self):
        self.hook = MagicMock()


class DummyPipeline:
    def __init__(self):
        self.nodes = []

    def datasets(self):
        return ["my_dataset"]


class DummyCatalog:
    def _get_dataset(self, name):
        return DummyDataset()


@pytest.fixture
def catalog_translator():
    return CatalogTranslator(
        catalog=DummyCatalog(),
        pipelines=[DummyPipeline()],
        hook_manager=DummyHookManager(),
        env="testenv",
    )


def test_translate_dataset_returns_io_manager(catalog_translator):
    dataset = DummyDataset()
    io_manager = catalog_translator._translate_dataset(dataset, "my_dataset")
    assert isinstance(io_manager, dg.ConfigurableIOManager)
    assert hasattr(io_manager, "handle_output")
    assert hasattr(io_manager, "load_input")
