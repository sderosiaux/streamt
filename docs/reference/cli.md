---
title: CLI Reference
description: Complete reference for all streamt commands
---

# CLI Reference

Complete reference for all streamt CLI commands.

## Global Options

These options are available for all commands:

```bash
streamt [OPTIONS] COMMAND [ARGS]

Options:
  --project-dir PATH    Path to project directory (default: current)
  --version            Show version and exit
  --help               Show help message and exit
```

## Commands

### validate

Validate project configuration, syntax, and governance rules.

```bash
streamt validate [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--check-schemas` | Fetch and validate schemas from Schema Registry |
| `--strict` | Treat warnings as errors |

**Examples:**

```bash
# Basic validation
streamt validate

# With schema registry validation
streamt validate --check-schemas

# Strict mode (fail on warnings)
streamt validate --strict
```

**Output:**

```
✓ Project 'payments-pipeline' is valid

  Sources:   3
  Models:    5
  Tests:     4
  Exposures: 2

Governance:
  ✓ All topics meet minimum partition requirement
  ✓ All models have descriptions
  ⚠ 1 model missing tests (warning)
```

---

### compile

Parse, validate, and generate deployment artifacts.

```bash
streamt compile [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--output PATH` | Output directory (default: `generated/`) |
| `--dry-run` | Show what would be generated without writing |

**Examples:**

```bash
# Compile to default directory
streamt compile

# Custom output directory
streamt compile --output ./build

# Preview without writing files
streamt compile --dry-run
```

**Output:**

```
Compiling project...

Generated artifacts:
  topics/
    orders.clean.v1.json
    orders.metrics.v1.json
  flink/
    order_metrics.sql
    order_metrics.json
  connect/
    orders_snowflake.json

Manifest written to: generated/manifest.json
```

---

### plan

Show planned changes without applying them (like `terraform plan`).

```bash
streamt plan [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--target MODEL` | Plan only for specific model |

**Examples:**

```bash
# Plan all changes
streamt plan

# Plan specific model
streamt plan --target order_metrics
```

**Output:**

```
Plan: 2 to create, 1 to update, 0 to delete

Topics:
  + orders.clean.v1 (12 partitions, rf=3)
  ~ orders.metrics.v1 (partitions: 6 → 12)

Flink Jobs:
  + order_metrics (parallelism: 8)

Connectors:
  (no changes)
```

---

### apply

Deploy artifacts to infrastructure.

```bash
streamt apply [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--target MODEL` | Deploy only specific model |
| `--select SELECTOR` | Filter by tag or selector |
| `--dry-run` | Show what would be deployed |
| `--auto-approve` | Skip confirmation prompt |

**Examples:**

```bash
# Deploy all
streamt apply

# Deploy specific model
streamt apply --target order_metrics

# Deploy by tag
streamt apply --select tag:critical

# Auto-approve (CI/CD)
streamt apply --auto-approve
```

**Output:**

```
Applying changes...

Topics:
  + orders.clean.v1 .............. created
  ~ orders.metrics.v1 ............ updated

Flink Jobs:
  + order_metrics ................ deployed

Summary: 2 created, 1 updated, 0 unchanged
```

---

### test

Run data quality tests.

```bash
streamt test [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--model MODEL` | Test specific model only |
| `--type TYPE` | Filter by type: `schema`, `sample`, `continuous` |
| `--deploy` | Deploy continuous tests as Flink jobs |

**Examples:**

```bash
# Run all tests
streamt test

# Run schema tests only
streamt test --type schema

# Test specific model
streamt test --model orders_clean

# Deploy continuous monitoring
streamt test --deploy
```

**Output:**

```
Running tests...

Schema Tests:
  ✓ orders_schema_validation (3 assertions)

Sample Tests:
  ✓ orders_data_quality (1000 messages)

Continuous Tests:
  ○ orders_monitoring (deployed)

Results: 2 passed, 0 failed, 1 deployed
```

---

### lineage

Display data lineage and dependencies.

```bash
streamt lineage [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--model MODEL` | Focus on specific model |
| `--upstream` | Show only upstream dependencies |
| `--downstream` | Show only downstream dependencies |
| `--format FORMAT` | Output format: `ascii`, `json`, `mermaid` |

**Examples:**

```bash
# Full lineage (ASCII)
streamt lineage

# Focus on one model
streamt lineage --model order_metrics

# Upstream only
streamt lineage --model order_metrics --upstream

# Downstream only
streamt lineage --model orders_clean --downstream

# JSON output
streamt lineage --format json

# Mermaid diagram
streamt lineage --format mermaid
```

**Output (ASCII):**

```
orders_raw (source)
    └── orders_clean (topic)
            ├── order_metrics (flink)
            │       └── ops_dashboard (exposure)
            └── billing_service (exposure)
```

---

### status

Check deployment status of resources.

```bash
streamt status [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--topics` | Show only topics |
| `--jobs` | Show only Flink jobs |
| `--connectors` | Show only connectors |
| `--tests` | Show test status |

**Examples:**

```bash
# Full status
streamt status

# Topics only
streamt status --topics

# With test status
streamt status --tests
```

**Output:**

```
Infrastructure Status:

Topics:
  orders.clean.v1    ✓ exists (12 partitions, rf=3)
  orders.metrics.v1  ✓ exists (6 partitions, rf=3)

Flink Jobs:
  order_metrics      ✓ RUNNING (8/8 tasks)

Connectors:
  orders_snowflake   ✓ RUNNING (2/2 tasks)

Continuous Tests:
  orders_monitoring  ✓ RUNNING
```

---

### docs

Generate project documentation.

```bash
streamt docs COMMAND [OPTIONS]
```

#### docs generate

Generate HTML documentation site.

```bash
streamt docs generate [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--output PATH` | Output directory (default: `site/`) |

**Examples:**

```bash
# Generate docs
streamt docs generate

# Custom output
streamt docs generate --output ./public
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `STREAMT_PROJECT_DIR` | Default project directory |
| `STREAMT_LOG_LEVEL` | Log level (DEBUG, INFO, WARNING, ERROR) |
| `STREAMT_NO_COLOR` | Disable colored output |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Validation error |
| 3 | Deployment error |
| 4 | Test failure |

## Examples

### CI/CD Pipeline

```bash
#!/bin/bash
set -e

# Validate
streamt validate --strict

# Plan and show diff
streamt plan

# Apply (auto-approve in CI)
streamt apply --auto-approve

# Run tests
streamt test
```

### Development Workflow

```bash
# 1. Validate changes
streamt validate

# 2. View lineage
streamt lineage --model my_new_model

# 3. Plan deployment
streamt plan

# 4. Apply changes
streamt apply

# 5. Run tests
streamt test --model my_new_model
```

### Monitoring Script

```bash
#!/bin/bash
# Check all resources
streamt status

# Alert on failures
if ! streamt status --jobs | grep -q "RUNNING"; then
  echo "Alert: Flink job not running!"
  exit 1
fi
```

---

## Coming Soon

The following commands and options are planned:

| Command/Option | Description | Status |
|----------------|-------------|--------|
| `streamt status --lag` | Show consumer lag for topics | Planned |
| `streamt status --health` | Health checks with thresholds | Planned |
| `streamt rollback` | Rollback to previous deployment | Planned |
| `streamt diff` | Show diff between local and deployed | Planned |
| `streamt init` | Initialize new project from template | Planned |

See the [roadmap](https://github.com/streamt/streamt#roadmap) for the full list of planned features.
