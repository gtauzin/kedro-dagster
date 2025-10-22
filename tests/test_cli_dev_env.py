from __future__ import annotations

from click.testing import CliRunner

from kedro_dagster.cli import dev as cli_dev
from kedro_dagster.config.dev import DevOptions
from kedro_dagster.config.kedro_dagster import KedroDagsterConfig


def test_cli_dev_uses_env_config_overrides(monkeypatch, mocker, kedro_project):
    # Arrange: move into a minimal kedro project and stub get_dagster_config to custom values
    monkeypatch.chdir(kedro_project)

    custom_cfg = KedroDagsterConfig(
        dev=DevOptions(
            log_level="debug",
            log_format="json",
            port="4000",
            host="0.0.0.0",
            live_data_poll_rate="1500",
        ),
        schedules={"daily": {"cron_schedule": "0 0 * * *"}},
        executors={"sequential": {"in_process": {}}},
        jobs={
            "default": {
                "pipeline": {"pipeline_name": "__default__"},
                "schedule": "daily",
                "executor": "sequential",
            }
        },
    )
    mocker.patch("kedro_dagster.cli.get_dagster_config", return_value=custom_cfg)

    sp_call = mocker.patch("subprocess.call")

    # Act: invoke without CLI flags; values should come from config
    result = CliRunner().invoke(cli_dev)

    # Assert
    assert result.exit_code == 0
    called_args = sp_call.call_args[0][0]
    args_map = {called_args[i]: called_args[i + 1] for i in range(2, len(called_args), 2)}
    assert args_map["--log-level"] == "debug"
    assert args_map["--log-format"] == "json"
    assert args_map["--host"] == "0.0.0.0"
    assert args_map["--port"] == "4000"
    assert args_map["--live-data-poll-rate"] == "1500"
