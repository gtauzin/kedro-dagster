"""Dagster definitions."""

import os

import dagster as dg
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from kedro_dagster import KedroDagsterTranslator

KEDRO_ENV = os.getenv("KEDRO_ENV", "dev")

translator = KedroDagsterTranslator(env=KEDRO_ENV)
translator.translate()

resources = translator.named_resources
# The "io_manager" key handles how Kedro MemoryDatasets are handled by Dagster
if KEDRO_ENV == "base":
    resources |= {
        "io_manager": dg.fs_io_manager,
    }
elif KEDRO_ENV == "dev":
    resources |= {
        "io_manager": S3PickleIOManager(
            s3_resource=S3Resource(),
            s3_bucket="zaphiro-dagster-demo",
            s3_prefix="poc-mlops-platform/memory",
        ),
    }

defs = dg.Definitions(
    assets=list(translator.named_assets.values()),
    resources=resources,
    jobs=list(translator.named_jobs.values()),
    schedules=list(translator.named_schedules.values()),
    sensors=list(translator.named_sensors.values()),
    loggers=translator.named_loggers,
    executor=dg.multiprocess_executor.configured(dict(max_concurrent=2)),
)
