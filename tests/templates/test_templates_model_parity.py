from __future__ import annotations

from importlib.resources import files

import yaml

from kedro_dagster.config.kedro_dagster import KedroDagsterConfig


def test_dagster_yml_template_parses_with_model_defaults():
    # Load the packaged template content directly from the installed package
    content = (files("kedro_dagster") / "templates" / "dagster.yml").read_text(encoding="utf-8")
    raw = yaml.safe_load(content)

    # Should parse without raising; dev defaults and mapping coercions applied
    cfg = KedroDagsterConfig(**raw)

    # Invariants: dev defaults are as documented in template/model
    assert cfg.dev is not None
    assert cfg.dev.log_level == "info"
    # Accept either colored/color naming depending on upstream conventions
    assert cfg.dev.log_format in {"colored", "color", "rich"}
    assert cfg.dev.port == "3000"
    assert cfg.dev.host == "127.0.0.1"
    assert cfg.dev.live_data_poll_rate == "2000"

    # Schedules and executors keys are preserved as names in the mapping
    assert cfg.schedules is not None
    assert "daily" in cfg.schedules
    assert cfg.schedules["daily"].cron_schedule == "0 0 * * *"

    assert cfg.executors is not None
    assert "sequential" in cfg.executors

    # Jobs: 'default' job references existing names
    assert cfg.jobs is not None
    assert "default" in cfg.jobs
    job = cfg.jobs["default"]
    assert job.pipeline.pipeline_name == "__default__"
    assert job.executor == "sequential"
    assert job.schedule == "daily"
