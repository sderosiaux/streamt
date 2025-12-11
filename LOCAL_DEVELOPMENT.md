# Local Development

This guide covers setting up a local development environment for streamt.

## Infrastructure Setup

Start the infrastructure using Docker Compose:

```bash
docker compose up -d
```

### Services

- **Kafka** (KRaft): localhost:9092
- **Schema Registry**: localhost:8081
- **Flink UI**: localhost:8082
- **Connect**: localhost:8083
- **Conduktor Console**: localhost:8080 (admin@localhost / Admin123!)

## Running Tests

Run the test suite:

```bash
pytest tests/ -v
```

## Documentation

To work on the documentation locally:

```bash
# Install docs dependencies
pip install -e ".[docs]"

# Serve locally
mkdocs serve
```

The documentation will be available at http://localhost:8000.

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
