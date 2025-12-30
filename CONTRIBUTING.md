# Contributing to streamt

Thank you for your interest in contributing to streamt! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

- Python 3.10+
- Docker and Docker Compose (for integration tests)
- Git

### Development Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/conduktor/streamt.git
   cd streamt
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   pip install -e ".[dev,docs]"
   ```

4. **Start local infrastructure** (for integration tests)

   ```bash
   docker compose up -d
   ```

See [LOCAL_DEVELOPMENT.md](LOCAL_DEVELOPMENT.md) for detailed setup instructions.

## Development Workflow

### Code Style

We use automated formatters and linters to maintain consistent code quality:

- **black** for code formatting (line length: 100)
- **ruff** for linting
- **mypy** for type checking

Format your code before committing:

```bash
black src/ tests/
ruff check src/ tests/ --fix
mypy src/
```

### Running Tests

```bash
# Unit tests only
pytest tests/ -m "not integration"

# All tests (requires Docker)
pytest tests/

# With coverage
pytest tests/ --cov=src/streamt --cov-report=html
```

### Test Markers

Tests are organized with markers:

- `integration` - Requires Docker infrastructure
- `slow` - Takes > 30 seconds
- `kafka` - Requires Kafka
- `flink` - Requires Flink
- `connect` - Requires Kafka Connect
- `schema_registry` - Requires Schema Registry

## Making Changes

### Branch Naming

Use descriptive branch names:

- `feature/add-flink-checkpointing`
- `fix/yaml-parsing-error`
- `docs/update-quickstart`

### Commit Messages

Write clear, concise commit messages:

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Fix bug" not "Fixes bug")
- Reference issues when applicable (`Fix #123`)

### Pull Request Process

1. **Create a feature branch** from `main`
2. **Make your changes** with appropriate tests
3. **Ensure all tests pass** and code is formatted
4. **Update documentation** if needed
5. **Submit a pull request** with a clear description

### PR Requirements

- All CI checks must pass
- Code must be formatted with black/ruff
- New features need tests
- Documentation updates for user-facing changes

## Good First Issues

Looking for a place to start? Check issues labeled [`good first issue`](https://github.com/conduktor/streamt/labels/good%20first%20issue). These are beginner-friendly tasks that help you get familiar with the codebase.

## Reporting Bugs

When reporting bugs, please include:

- streamt version (`streamt --version`)
- Python version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant configuration files (sanitized)

## Suggesting Features

Feature requests are welcome! Please:

- Check existing issues first
- Describe the use case
- Explain why this would benefit streamt users
- Consider how it fits with the "dbt for streaming" philosophy

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to uphold this code.

## Questions?

- Join our [Slack community](https://conduktor.io/slack)
- Open a [discussion](https://github.com/conduktor/streamt/discussions)

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
