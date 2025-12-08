---
title: Installation
description: How to install streamt and its dependencies
---

# Installation

This guide covers installing streamt and setting up your local development environment.

## Requirements

- **Python 3.10+** — streamt uses modern Python features
- **pip** or **uv** — for package management
- **Docker** (optional) — for local Kafka, Flink, and Connect

## Quick Install

Install streamt using pip:

```bash
pip install streamt
```

This installs the core package with minimal dependencies.

## Full Installation

For all features including Kafka administration, Flink deployment, and Schema Registry integration:

```bash
pip install "streamt[all]"
```

Or install specific extras:

```bash
# Kafka topic management
pip install "streamt[kafka]"

# Flink job deployment
pip install "streamt[flink]"

# Schema Registry integration
pip install "streamt[schema-registry]"

# All extras
pip install "streamt[kafka,flink,schema-registry]"
```

## Development Installation

If you're contributing to streamt or want the latest development version:

```bash
git clone https://github.com/streamt/streamt.git
cd streamt
pip install -e ".[dev]"
```

## Verify Installation

Check that streamt is installed correctly:

```bash
streamt --version
```

You should see output like:

```
streamt version 0.1.0
```

## Local Development Environment

For local development and testing, we provide a Docker Compose stack with all dependencies:

```bash
# Clone the repository
git clone https://github.com/streamt/streamt.git
cd streamt

# Start the local stack
docker compose up -d
```

This starts:

| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Message broker (KRaft mode) |
| Schema Registry | 8081 | Schema management |
| Kafka Connect | 8083 | Connector framework |
| Flink JobManager | 8082 | Flink dashboard |
| Flink SQL Gateway | 8084 | SQL submission endpoint |
| Conduktor Console | 8080 | Kafka UI (admin@localhost / Admin123!) |
| Conduktor Gateway | 6969, 8888 | Kafka proxy for virtual topics (proxy: 6969, admin: 8888) |

Verify services are running:

```bash
docker compose ps
```

All services should show as `healthy`.

## IDE Setup

### VS Code

Install the YAML extension for better editing experience:

1. Install "YAML" extension by Red Hat
2. Add to your `.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "https://streamt.dev/schema/project.json": "stream_project.yml",
    "https://streamt.dev/schema/source.json": "sources/*.yml",
    "https://streamt.dev/schema/model.json": "models/*.yml"
  }
}
```

### PyCharm / IntelliJ

The YAML plugin is included by default. For schema validation:

1. Go to Settings → Languages & Frameworks → Schemas and DTDs → JSON Schema Mappings
2. Add mappings for `stream_project.yml`, `sources/*.yml`, and `models/*.yml`

## Troubleshooting

### `command not found: streamt`

Make sure your Python scripts directory is in your PATH:

```bash
# Check where pip installs scripts
python -m site --user-base

# Add to PATH (add to your .bashrc or .zshrc)
export PATH="$PATH:$(python -m site --user-base)/bin"
```

### `ModuleNotFoundError: No module named 'confluent_kafka'`

Install the Kafka extras:

```bash
pip install "streamt[kafka]"
```

### Connection refused to Kafka/Flink

Make sure Docker services are running:

```bash
docker compose ps
docker compose logs kafka
```

---

## Next Steps

Now that streamt is installed, continue to the [Quick Start](quickstart.md) guide to create your first project.
