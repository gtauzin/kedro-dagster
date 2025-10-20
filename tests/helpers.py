from __future__ import annotations

from typing import Any


def dagster_executors_config() -> dict[str, Any]:
    return {
        "inproc": {"in_process": {}},
        "multiproc": {"multiprocess": {"max_concurrent": 2}},
    }


def dagster_loggers_config() -> dict[str, Any]:
    return {
        "console": {"console": {"log_level": "INFO"}},
    }


def dagster_schedules_config() -> dict[str, Any]:
    return {
        "daily": {
            "cron_schedule": "0 6 * * *",
            "execution_timezone": "UTC",
        }
    }


def make_jobs_config(
    pipeline_name: str = "__default__", executor: str = "inproc", schedule: str | None = None
) -> dict[str, Any]:
    job: dict[str, Any] = {
        "pipeline": {"pipeline_name": pipeline_name},
        "executor": executor,
    }
    if schedule is not None:
        job["schedule"] = schedule

    return {"default": job}


def envs() -> list[str]:
    return ["base", "local"]
