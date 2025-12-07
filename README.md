<div align="center">

# streamt

**dbt for streaming** — Declarative streaming pipelines with Kafka, Flink, and Connect

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-186%20passed-brightgreen.svg)]()
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

[Documentation](docs/) • [Getting Started](#quick-start) • [Examples](#examples)

</div>

---

## What is streamt?

**streamt** brings the beloved dbt workflow to real-time streaming. Define your streaming pipelines declaratively using YAML and SQL, then let streamt handle compilation, validation, and deployment to Kafka, Flink, and Kafka Connect.

```yaml
sources:
  - name: payments_raw
    topic: payments.raw.v1

models:
  - name: payments_validated
    materialized: flink
    sql: |
      SELECT payment_id, customer_id, amount
      FROM {{ source("payments_raw") }}
      WHERE amount > 0 AND status IS NOT NULL
```

## Features

| Feature | Description |
|---------|-------------|
| 🎯 **Declarative** | Define what you want, not how to build it |
| 🔗 **Lineage** | Automatic dependency tracking from SQL |
| 🛡️ **Governance** | Enforce naming conventions, partitions, tests |
| 📊 **Testing** | Schema, sample, and continuous tests |
| 🔄 **Plan/Apply** | Review changes before deployment |
| 📖 **Documentation** | Auto-generated docs with lineage diagrams |

## Materializations

| Type | Use Case | Creates |
|------|----------|---------|
| `topic` | Stateless transformations | Kafka topic |
| `virtual_topic` | Read-time filtering | Gateway rule |
| `flink` | Stateful processing | Flink SQL job |
| `sink` | External exports | Connect connector |

## Quick Start

### Installation

```bash
pip install streamt
```

### Create a Project

```yaml
# stream_project.yml
project:
  name: my-pipeline
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: localhost:9092
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084

sources:
  - name: events
    topic: events.raw.v1

models:
  - name: events_clean
    materialized: topic
    topic:
      partitions: 6
    sql: |
      SELECT event_id, user_id, event_type
      FROM {{ source("events") }}
      WHERE event_id IS NOT NULL
```

### CLI Commands

```bash
# Validate configuration
streamt validate

# See what will change
streamt plan

# Deploy to infrastructure
streamt apply

# Run tests
streamt test

# View lineage
streamt lineage
```

## Examples

### Stateless Filtering (Topic)

```yaml
- name: high_value_orders
  materialized: topic
  sql: |
    SELECT * FROM {{ source("orders") }}
    WHERE amount > 10000
```

### Windowed Aggregation (Flink)

```yaml
- name: hourly_revenue
  materialized: flink
  sql: |
    SELECT
      TUMBLE_START(ts, INTERVAL '1' HOUR) as hour,
      SUM(amount) as revenue
    FROM {{ ref("orders_clean") }}
    GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
```

### Export to Warehouse (Sink)

```yaml
- name: orders_snowflake
  materialized: sink
  from: orders_clean
  connector:
    type: snowflake-sink
    config:
      snowflake.database.name: ANALYTICS
```

### Data Quality Tests

```yaml
tests:
  - name: orders_quality
    model: orders_clean
    type: sample
    assertions:
      - not_null: { columns: [order_id, amount] }
      - range: { column: amount, min: 0, max: 1000000 }
```

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│    YAML     │────▶│   Compile   │────▶│  Artifacts  │
│  + SQL      │     │  & Validate │     │   (JSON)    │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    ▼                          ▼                          ▼
             ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
             │    Kafka    │           │    Flink    │           │   Connect   │
             │   Topics    │           │    Jobs     │           │ Connectors  │
             └─────────────┘           └─────────────┘           └─────────────┘
```

## Local Development

Start the infrastructure:

```bash
docker compose up -d
```

Services:
- **Kafka** (KRaft): localhost:9092
- **Schema Registry**: localhost:8081
- **Flink UI**: localhost:8082
- **Connect**: localhost:8083
- **Conduktor Console**: localhost:8080 (admin@localhost / Admin123!)

Run tests:

```bash
pytest tests/ -v
```

## Documentation

```bash
# Install docs dependencies
pip install -e ".[docs]"

# Serve locally
mkdocs serve
```

## Project Structure

```
streamt/
├── src/streamt/
│   ├── cli.py              # CLI commands
│   ├── core/
│   │   ├── models.py       # Pydantic models
│   │   ├── parser.py       # YAML parser
│   │   ├── validator.py    # Validation rules
│   │   └── dag.py          # DAG builder
│   ├── compiler/           # Artifact generation
│   ├── deployer/           # Kafka, Flink, Connect
│   └── testing/            # Test runner
├── docs/                   # Documentation site
├── tests/                  # Test suite
└── examples/               # Example projects
```

## Roadmap

- [ ] Schema Registry integration
- [ ] Conduktor Gateway virtual topics
- [ ] Kubernetes Flink operator support
- [ ] CI/CD GitHub Actions
- [ ] VS Code extension

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

---

<div align="center">

**[Documentation](docs/)** • **[Examples](examples/)** • **[Contributing](CONTRIBUTING.md)**

</div>
