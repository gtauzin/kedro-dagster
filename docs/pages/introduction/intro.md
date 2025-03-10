
# Introduction

[TOC]

## What is Dagster?

[Dagster](https://dagster.io/) is a modern Python-based data orchestrator that emphasizes asset-oriented workflows, enabling comprehensive management of data assets throughout their lifecycle—from ingestion and transformation to storage and re-execution when outdated. It allows to define, schedule, and monitor pipelines in a highly observable and scalable manner. By treating data and models as software-defined assets, Dagster provides precise control over data lineage and dependencies.

It scales seamlessly from a single developer's laptop to a production-grade cluster and fosters best practices in software and data engineering by promoting testability, modularity, code reusability, and version control.

To get started with Dagster, visit their [documentation](https://docs.dagster.io/).

## Why should you consider using Kedro-Dagster?

Kedro and Dagster are very complementary. They are both asset-oriented.

### You are already using Kedro

If you are already using Kedro and looking for an orchestration tool to manage your data pipelines, Dagster is a great choice. It provides robust scheduling, monitoring, and logging capabilities that can enhance your Kedro pipelines. Kedro-Dagster allows you to leverage these features without significant changes to your existing Kedro codebase.

### You are already using Dagster

If you are already using Dagster and want to incorporate Kedro's pipeline development framework, Kedro-Dagster provides a seamless integration. You can continue using Dagster's orchestration features while benefiting from Kedro's modular pipeline design, data cataloging, and configuration management.

## Why should you look into when considering using Dagster for ML pipelines?

Dagster is originally a data orchestration framework that has increasingly moved towards supporting ML pipelines.

- Carefully consider the ML pipeline deployment pattern.
- Weigh whether available Dagster deployments make sense for your project.
