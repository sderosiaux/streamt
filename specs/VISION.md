# Vision - dbt for Streaming

## What is it?

A declarative layer on top of Kafka / Flink / Connect where teams describe:
- Their real-time models (streams, transformations)
- Their tests (data quality, contracts)
- Their security rules (masking, encryption, RBAC)
- Their usages (apps, dashboards, dependencies)

And Conduktor compiles this to the existing runtime (Proxy, Flink jobs, Connectors).

## Core Value Proposition

> "Describe your streaming pipelines as code. Let the platform handle the infrastructure."

Like dbt transformed how data teams work with warehouses, this product transforms how teams work with streaming data.

## Key Goals

1. **Declarative over imperative** - Teams describe WHAT they want, not HOW to deploy it
2. **Contracts & quality built-in** - Tests and validation are first-class citizens
3. **Security by design** - Classification, masking, encryption declared alongside the model
4. **Full lineage** - From producer apps to consumer apps, end-to-end visibility
5. **Multi-team federation** - Each team owns their domain, platform sees everything

## Non-Goals

_To be defined during discovery_

## Target Users

_To be defined during discovery_

## Differentiators vs dbt

| dbt | dbt for Streaming |
|-----|-------------------|
| Batch (SQL on warehouse) | Real-time (Kafka + Flink) |
| Tables/Views | Topics/Virtual Topics/Streams |
| Scheduled runs | Continuous processing |
| Post-hoc testing | Real-time testing & alerting |
| - | Security policies embedded |
| - | Runtime visibility (lag, throughput) |
