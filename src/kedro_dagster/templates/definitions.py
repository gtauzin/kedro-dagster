"""Dagster definitions."""

import dagster as dg
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from kedro_dagster import (
    translate_kedro,
)

ENV = "base"

assets, resources, jobs, schedules, loggers, executors = translate_kedro(env=ENV)

# The "io_manager" key handles how Kedro MemoryDatasets are handled by Dagster
if ENV == "base":
    resources |= {
        "io_manager": dg.fs_io_manager,
    }
elif ENV == "dev":
    resources |= {
        "io_manager": S3PickleIOManager(
            s3_resource=S3Resource(),
            s3_bucket="<MY_BUCKET>",
            s3_prefix="<MY_PREFIX>",
        ),
    }

defs = dg.Definitions(
    assets=assets,
    resources=resources,
    jobs=jobs,
    schedules=schedules,
    loggers=loggers,
    executor=executors["my_exec"],  # TODO: Clarify
)
