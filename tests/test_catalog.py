from typing import Any
from unittest.mock import MagicMock

import dagster as dg
import pytest
from kedro.pipeline import Pipeline

from kedro_dagster.catalog import CatalogTranslator


class DummyDataset:
    def save(self, obj: str) -> None:
        self.saved = obj

    def load(self) -> str:
        self.loaded = True
        return "data"

    def _describe(self) -> dict[str, Any]:
        return {"foo": "bar"}


class DummyHookManager:
    def __init__(self) -> None:
        self.hook = MagicMock()


class DummyPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(nodes=[])

    def datasets(self) -> list[str]:
        return ["my_dataset"]


class DummyCatalog:
    def _get_dataset(self, name: str) -> DummyDataset:
        return DummyDataset()


@pytest.fixture
def catalog_translator() -> CatalogTranslator:
    return CatalogTranslator(
        catalog=DummyCatalog(),
        pipelines=[DummyPipeline()],
        hook_manager=DummyHookManager(),
        env="testenv",
    )


def test_translate_dataset_returns_io_manager(catalog_translator: CatalogTranslator) -> None:
    dataset = DummyDataset()
    io_manager = catalog_translator._translate_dataset(dataset, "my_dataset")
    assert isinstance(io_manager, dg.ConfigurableIOManager)
    assert hasattr(io_manager, "handle_output")
    assert hasattr(io_manager, "load_input")
