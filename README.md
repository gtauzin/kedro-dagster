# Kedro Plugin for Dagster

A plugin that enables seamless integration between Kedro and Dagster frameworks.

[![Release](https://github.com/gtauzin/kedro-dagster/actions/workflows/release.yml/badge.svg)](https://github.com/gtauzin/kedro-dagster/actions/workflows/release.yml)

## Overview

This plugin allows you to run Kedro pipelines in Dagster, providing additional configuration and orchestration capabilities. It converts Kedro pipelines into Dagster jobs and assets while maintaining all the benefits of both frameworks.

## Installation

```bash
pip install kedro-dagster
```

## Features

- Convert Kedro pipelines to Dagster jobs
- Flexible pipeline configuration management
- Support for both default and pipeline-specific settings
- Custom parameter overrides
- Template rendering utilities
- Development options for logging and server configuration

## Usage

### Basic Pipeline Configuration

```python
# Configure a pipeline with default settings
config = {
    "default": {
        "config": {"execution": {"config": {"multiprocess": {}}}},
        "tags": {"default_tag": "value"},
    }
}

# Pipeline-specific configuration
pipeline_config = {
    "config": {"execution": {"config": {"in_process": {}}}},
    "tags": {"pipeline_tag": "custom"},
}
```

### Development Options

The plugin supports various development configurations:

- Log levels: critical, error, warning, info, debug
- Log formats: colored, json, rich
- Customizable host and port settings
- Live data polling configuration

## Documentation

For detailed documentation, please visit our [documentation site](https://github.com/gtauzin/kedro-dagster).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Issues

If you encounter any problems or have suggestions, please [file an issue](https://github.com/gtauzin/kedro-dagster/issues).

## License

[Add license information]

## Links

- [Source Repository](https://github.com/gtauzin/kedro-dagster)
- [Issues & Ideas](https://github.com/gtauzin/kedro-dagster/issues)