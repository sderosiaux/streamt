# Multi-Environment Support Specification

## Overview

streamt supports deploying the same pipeline definitions to different environments (dev, staging, prod). Environments are **opt-in** — projects start with a single implicit environment and can expand to multiple named environments when needed.

---

## Design Principles

1. **Start simple** — Single-env projects require zero extra config
2. **Opt-in complexity** — Multi-env only when you create `environments/` dir
3. **Explicit is better** — No inheritance, no magic merging
4. **Safety by default** — Protected environments require confirmation

---

## Configuration Modes

### Mode 1: Single Environment (Default)

Everything in one file. No `--env` flag needed.

```yaml
# stream_project.yml
project:
  name: my-pipeline
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP}
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8082

sources:
  - name: events
    topic: events.raw.v1

models:
  - name: events_clean
    sql: SELECT * FROM {{ source("events") }} WHERE id IS NOT NULL
```

```bash
streamt validate   # Just works
streamt plan       # Just works
streamt apply      # Just works
```

### Mode 2: Multiple Environments

Create `environments/` directory with named environment files. The `runtime:` section moves from `stream_project.yml` to environment files.

```
my-project/
├── stream_project.yml        # Resources only (no runtime:)
├── environments/
│   ├── dev.yml
│   ├── staging.yml
│   └── prod.yml
├── .env                      # Shared env vars
├── .env.dev                  # Dev-specific env vars
├── .env.staging
└── .env.prod
```

**stream_project.yml** (resources only):
```yaml
project:
  name: my-pipeline
  version: "1.0.0"

sources:
  - name: events
    topic: events.raw.v1

models:
  - name: events_clean
    sql: SELECT * FROM {{ source("events") }} WHERE id IS NOT NULL
```

**environments/dev.yml**:
```yaml
environment:
  name: dev
  description: Local development

runtime:
  kafka:
    bootstrap_servers: localhost:9092
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8082
```

**environments/prod.yml**:
```yaml
environment:
  name: prod
  description: Production environment
  protected: true

runtime:
  kafka:
    bootstrap_servers: ${PROD_KAFKA_BOOTSTRAP}
  schema_registry:
    url: ${PROD_SR_URL}
  flink:
    default: confluent
    clusters:
      confluent:
        type: confluent
        environment: ${CONFLUENT_ENV}
        api_key: ${CONFLUENT_API_KEY}
        api_secret: ${CONFLUENT_API_SECRET}

safety:
  confirm_apply: true
  allow_destructive: false
```

---

## CLI Behavior

### Single-Environment Mode

When `environments/` directory does NOT exist:

| Command | Behavior |
|---------|----------|
| `streamt validate` | Validates using `runtime:` from stream_project.yml |
| `streamt plan` | Plans using `runtime:` from stream_project.yml |
| `streamt apply` | Applies using `runtime:` from stream_project.yml |
| `streamt validate --env foo` | **Error**: "No environments configured. Remove --env flag or create environments/ directory." |

### Multi-Environment Mode

When `environments/` directory EXISTS:

| Command | Behavior |
|---------|----------|
| `streamt validate` | **Error**: "Multiple environments found. Specify with --env. Available: dev, staging, prod" |
| `streamt validate --env dev` | Validates using `environments/dev.yml` |
| `streamt plan --env staging` | Plans using `environments/staging.yml` |
| `streamt apply --env prod` | Applies using `environments/prod.yml` (with confirmation if protected) |
| `streamt validate --env foo` | **Error**: "Environment 'foo' not found. Available: dev, staging, prod" |

### STREAMT_ENV Variable

Sets default environment (avoids typing `--env` every time):

```bash
export STREAMT_ENV=dev
streamt plan              # Uses dev
streamt plan --env staging  # CLI flag overrides env var
```

### Environment Commands

```bash
streamt envs list
# Output:
#   dev        Local development
#   staging    Staging environment
#   prod       Production environment [protected]

streamt envs show dev
# Output: Resolved config with secrets masked as ****
```

---

## Environment File Schema

```yaml
# environments/{name}.yml

environment:
  name: string              # Required. Must match filename.
  description: string       # Optional. Shown in `envs list`.
  protected: bool           # Optional. Default: false. Requires confirmation.

runtime:
  kafka: KafkaConfig        # Required.
  schema_registry: SRConfig # Optional.
  flink: FlinkConfig        # Optional.
  connect: ConnectConfig    # Optional.
  conduktor: ConduktorConfig # Optional.

safety:                     # Optional. Only for protected environments.
  confirm_apply: bool       # Default: true if protected.
  allow_destructive: bool   # Default: true. Set false to block deletes.
```

---

## .env File Loading

Order of precedence (later overrides earlier):

1. `.env` (base, always loaded)
2. `.env.{environment}` (if exists, e.g., `.env.prod`)
3. Actual environment variables (highest priority)

Example:
```bash
# .env
KAFKA_BOOTSTRAP=localhost:9092

# .env.prod
KAFKA_BOOTSTRAP=prod-kafka:9092

# When --env prod: KAFKA_BOOTSTRAP=prod-kafka:9092
# When --env dev:  KAFKA_BOOTSTRAP=localhost:9092 (from .env)
```

---

## Protected Environments

When `environment.protected: true`:

### Interactive Mode
```
$ streamt apply --env prod

╔══════════════════════════════════════════════════════════════╗
║  WARNING: Deploying to PROD (protected environment)          ║
╚══════════════════════════════════════════════════════════════╝

Changes:
  + topic: payments.clean.v1 (6 partitions)
  ~ topic: payments.raw.v1 (partitions: 6 → 12)

Type 'prod' to confirm: _
```

### Non-Interactive Mode (CI/CD)
```bash
streamt apply --env prod --confirm
# Proceeds without prompt

streamt apply --env prod
# Error: "Protected environment requires --confirm flag in non-interactive mode"
```

### Destructive Operations

When `safety.allow_destructive: false`:
```
$ streamt apply --env prod

Error: Destructive operations blocked for 'prod' environment.
  - Would delete: topic payments.legacy.v1

Use --force to override (not recommended for production).
```

---

## Validation Rules

### Mode Detection

```
IF environments/ directory exists:
    mode = multi-environment
    runtime: in stream_project.yml is IGNORED (warn if present)
ELSE:
    mode = single-environment
    runtime: in stream_project.yml is REQUIRED
```

### Environment Name Validation

- Must match filename: `environments/dev.yml` must have `environment.name: dev`
- Alphanumeric + hyphens only: `dev`, `staging`, `prod-us-east-1`
- Case-sensitive

### Config Validation

- All environments validated on `streamt validate --env <name>`
- Validate all at once: `streamt validate --all-envs`

---

## Migration Path

### From Single-Env to Multi-Env

1. Create `environments/` directory
2. Move `runtime:` from `stream_project.yml` to `environments/dev.yml`
3. Copy and modify for other environments
4. Remove `runtime:` from `stream_project.yml`

```bash
# Helper command (future)
streamt envs init dev
# Creates environments/dev.yml with current runtime: config
```

---

## Test Scenarios

### T1: Single-Env Mode

| ID | Given | When | Then |
|----|-------|------|------|
| T1.1 | No `environments/` dir, `runtime:` in project | `streamt validate` | Success |
| T1.2 | No `environments/` dir, `runtime:` in project | `streamt validate --env dev` | Error: "No environments configured" |
| T1.3 | No `environments/` dir, NO `runtime:` in project | `streamt validate` | Error: "Missing runtime configuration" |

### T2: Multi-Env Mode

| ID | Given | When | Then |
|----|-------|------|------|
| T2.1 | `environments/dev.yml` exists | `streamt validate` | Error: "Specify --env. Available: dev" |
| T2.2 | `environments/dev.yml` exists | `streamt validate --env dev` | Success |
| T2.3 | `environments/dev.yml` exists | `streamt validate --env prod` | Error: "Environment 'prod' not found. Available: dev" |
| T2.4 | `environments/` exists + `runtime:` in project | `streamt validate --env dev` | Warning: "runtime: in stream_project.yml ignored in multi-env mode" |

### T3: STREAMT_ENV Variable

| ID | Given | When | Then |
|----|-------|------|------|
| T3.1 | `STREAMT_ENV=dev`, multi-env mode | `streamt validate` | Uses dev |
| T3.2 | `STREAMT_ENV=dev`, multi-env mode | `streamt validate --env staging` | Uses staging (CLI wins) |
| T3.3 | `STREAMT_ENV=dev`, single-env mode | `streamt validate` | Warning: "STREAMT_ENV ignored in single-env mode" |

### T4: .env File Loading

| ID | Given | When | Then |
|----|-------|------|------|
| T4.1 | `.env` has `X=1` | `--env dev` | `X=1` |
| T4.2 | `.env` has `X=1`, `.env.dev` has `X=2` | `--env dev` | `X=2` |
| T4.3 | `.env.prod` has `X=3` | `--env dev` | `X` not set (only `.env` and `.env.dev` loaded) |
| T4.4 | `.env.dev` has `X=2`, actual env has `X=9` | `--env dev` | `X=9` (real env wins) |

### T5: Protected Environments

| ID | Given | When | Then |
|----|-------|------|------|
| T5.1 | `protected: true` | `streamt apply --env prod` (interactive) | Prompts for confirmation |
| T5.2 | `protected: true` | `streamt apply --env prod --confirm` | Proceeds |
| T5.3 | `protected: true` | `streamt apply --env prod` (non-interactive) | Error: "requires --confirm" |
| T5.4 | `protected: false` | `streamt apply --env dev` | Proceeds without prompt |

### T6: Destructive Safety

| ID | Given | When | Then |
|----|-------|------|------|
| T6.1 | `allow_destructive: false`, plan has deletes | `streamt apply --env prod` | Error: blocked |
| T6.2 | `allow_destructive: false`, plan has deletes | `streamt apply --env prod --force` | Proceeds with warning |
| T6.3 | `allow_destructive: true`, plan has deletes | `streamt apply --env prod` | Proceeds (after confirmation if protected) |

### T7: Envs Commands

| ID | Given | When | Then |
|----|-------|------|------|
| T7.1 | `environments/dev.yml`, `prod.yml` | `streamt envs list` | Shows both with descriptions |
| T7.2 | `prod.yml` has `protected: true` | `streamt envs list` | Shows `[protected]` badge |
| T7.3 | `dev.yml` has `${SECRET}` | `streamt envs show dev` | Shows `****` for secret |
| T7.4 | No `environments/` dir | `streamt envs list` | "No environments configured (single-env mode)" |

---

## Implementation Phases

### Phase 1: Core Loading
- [ ] Environment file discovery
- [ ] Mode detection (single vs multi)
- [ ] `--env` CLI flag
- [ ] `STREAMT_ENV` variable
- [ ] .env.{env} loading
- [ ] Error messages with available environments

### Phase 2: Safety
- [ ] `protected` flag
- [ ] Confirmation prompts
- [ ] `--confirm` flag
- [ ] `allow_destructive` + `--force`

### Phase 3: Tooling
- [ ] `streamt envs list`
- [ ] `streamt envs show`
- [ ] Secret masking

### Phase 4: Polish
- [ ] `streamt envs init`
- [ ] `streamt validate --all-envs`
- [ ] Colored warnings for protected envs
