# mypy: ignore-errors


import dagster as dg
import pytest
from kedro.framework.session import KedroSession
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline

from kedro_dagster.catalog import CatalogTranslator


class DummyDataset:
    def save(self, obj):
        self.saved = obj

    def load(self):
        self.loaded = True
        return "data"

    def _describe(self):
        return {"foo": "bar"}


class DummyPipeline(Pipeline):
    def __init__(self):
        super().__init__(nodes=[])

    def datasets(self):
        return ["my_dataset"]


@pytest.fixture
def catalog_translator(kedro_project, metadata):
    # Create a Kedro session and context as in translator.py
    session = KedroSession.create(project_path=kedro_project, env="base")
    context = session.load_context()
    catalog = DataCatalog(
        datasets={"my_dataset": DummyDataset()},
    )
    hook_manager = getattr(context, "_hook_manager", None)
    return CatalogTranslator(
        catalog=catalog,
        pipelines=[DummyPipeline()],
        hook_manager=hook_manager,
        env="base",
    )


def test_translate_dataset_returns_io_manager(catalog_translator):
    # Use a real dataset from the catalog if available, else fallback
    dataset_name = next(iter(catalog_translator._catalog._datasets), "my_dataset")
    dataset = catalog_translator._catalog._datasets.get(dataset_name, DummyDataset())
    io_manager = catalog_translator._translate_dataset(dataset, dataset_name)

    assert isinstance(io_manager, dg.ConfigurableIOManager)
    assert hasattr(io_manager, "handle_output")
    assert hasattr(io_manager, "load_input")
