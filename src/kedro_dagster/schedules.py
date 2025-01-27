"""Dagster schedule definitons from Kedro pipelines."""

import dagster as dg

from kedro_dagster.config import KedroDagsterConfig


def load_schedules_from_kedro_config(
    dagster_config: KedroDagsterConfig, job_dict: dict[str, dg.JobDefinition]
) -> dict[str, dg.ScheduleDefinition]:
    """Loads schedule definitions from the config file.

    Args:
        dagster_config : The Dagster configuration.

    Returns:
        Dict[Str, ScheduleDefinition]: A dict of dagster schedule definitions.

    """

    schedule_dict = {}
    if dagster_config.schedules is not None:
        for schedule_name, schedule_config in dagster_config.schedules.items():
            schedule_dict[schedule_name] = schedule_config.model_dump()

    schedules = []
    for job_name, job_config in dagster_config.jobs.items():
        schedule_config = job_config.schedule
        if isinstance(schedule_config, str):
            if schedule_config in schedule_dict:
                schedule = dg.ScheduleDefinition(
                    name=f"{job_name}_{schedule_config}_schedule",
                    job=job_dict[job_name],
                    **schedule_dict[schedule_config],
                )
            else:
                raise ValueError("")

            schedules.append(schedule)

    return schedules
