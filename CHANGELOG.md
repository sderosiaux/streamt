# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Smart Materialization Detection**: Automatically detects stateless vs stateful SQL patterns
  - Stateless patterns (WHERE, projections) use `virtual_topic` when Gateway is available
  - Stateful patterns (GROUP BY, JOIN, windows) always use `flink`
  - Automatic fallback to Flink when Gateway is not configured

- **ML_PREDICT/ML_EVALUATE Support**: Real-time ML inference with Confluent Cloud Flink
  - New `ml_outputs` field for declaring ML model output schemas
  - Type inference uses declared schemas for proper ROW types
  - Validation warning when ML functions used without `ml_outputs` declaration
  - Validation error when ML functions used without Confluent Flink cluster

- **SQL Type Inference**: Robust SQL parsing using sqlglot with custom Flink dialect
  - Custom FlinkDialect with window functions (TUMBLE_START, HOP_END, SESSION_END, PROCTIME)
  - Support for MATCH_RECOGNIZE (Complex Event Processing)
  - Support for temporal joins (FOR SYSTEM_TIME AS OF)
  - Support for CUMULATE TVF
  - Enhanced type inference for IF, Coalesce, arithmetic, window ranking functions
  - 80+ Confluent-specific functions added to parser

- **Validation Rules**:
  - `SQL_NO_FROM`: Error when streaming SQL has no FROM clause
  - `NO_PROCESSING_RUNTIME`: Error when neither Gateway nor Flink is configured
  - `CONFLUENT_FLINK_REQUIRED`: Error when ML functions used on non-Confluent cluster
  - `AMBIGUOUS_MATERIALIZATION`: Warning for stateless SQL without Gateway

- **CI/CD Pipeline**: GitHub Actions workflow for automated testing and linting

### Changed

- Compiler now uses FlinkDialect instead of default sqlglot dialect
- Test count increased from 186 to 277 unit tests

### Fixed

- Fixed BrokerMetadata unpacking bug in kafka.py for status command
- Fixed key encoding in Kafka helper to handle falsy but valid keys
- Fixed Flink job name lookups in DAG tests
- Fixed CAST type extraction to avoid double precision
- Fixed Coalesce iteration using iter_expressions()
- Fixed 84 linter errors across codebase
