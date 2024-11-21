import logging

from dagster import InitLoggerContext, LoggerDefinition
from kedro.framework.project import pipelines


# TODO: Allow logger customization
def get_kedro_loggers(package_name):
    loggers = {}
    for pipeline_name in pipelines:
        if pipeline_name != "__default__":

            def get_logger_definition(package_name, pipeline_name):
                def pipeline_logger(context: InitLoggerContext):
                    return logging.getLogger(f"{package_name}.pipelines.{pipeline_name}.nodes")

                return LoggerDefinition(
                    pipeline_logger,
                    description=f"Logger for pipeline`{pipeline_name}` of package `{package_name}`.",
                )

            loggers[f"{package_name}.pipelines.{pipeline_name}.nodes"] = get_logger_definition(
                package_name, pipeline_name
            )

    return loggers
