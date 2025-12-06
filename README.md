# streamt

**dbt for streaming** - A declarative framework for building and managing streaming data pipelines.

## Overview

streamt brings the power of dbt's declarative paradigm to the streaming world. Define your Kafka topics, Flink transformations, and data pipelines using simple YAML configuration and SQL.

## Features

- **Declarative DSL**: Define sources, models, tests, and exposures in YAML
- **Multiple Materializations**: topic, virtual_topic, flink, sink
- **Jinja Templating**: Use `{{ source("name") }}` and `{{ ref("model") }}` for dependencies
- **DAG Visualization**: Automatic lineage tracking and visualization
- **Validation**: Comprehensive validation with helpful error messages
- **Testing**: Schema tests, sample tests, and continuous tests
- **Governance**: Enforce rules like minimum partitions, naming patterns

## Installation

```bash
pip install streamt
```

## Quick Start

1. Create a `stream_project.yml`:

```yaml
project:
  name: my-streaming-project
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: localhost:9092

sources:
  - name: raw_events
    topic: events.raw.v1

models:
  - name: clean_events
    materialized: topic
    topic:
      partitions: 6
    sql: |
      SELECT id, timestamp, data
      FROM {{ source("raw_events") }}
      WHERE data IS NOT NULL
```

2. Validate the project:

```bash
streamt validate
```

3. Compile the project:

```bash
streamt compile
```

4. Apply changes:

```bash
streamt apply
```

## CLI Commands

- `streamt validate` - Validate project configuration
- `streamt compile` - Generate artifacts (topics, Flink SQL, connectors)
- `streamt plan` - Show execution plan
- `streamt apply` - Apply changes to infrastructure
- `streamt test` - Run tests
- `streamt lineage` - Show data lineage
- `streamt status` - Show deployment status
- `streamt docs generate` - Generate HTML documentation

## Documentation

See the [specs/](specs/) directory for detailed documentation:

- [VISION.md](specs/VISION.md) - Product vision
- [ARCHITECTURE.md](specs/ARCHITECTURE.md) - Architecture overview
- [CONCEPTS.md](specs/CONCEPTS.md) - Core concepts
- [DSL.md](specs/DSL.md) - DSL specification

## License

Apache 2.0
