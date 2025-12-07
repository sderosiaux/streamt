"""Compiler for streamt projects."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

from jinja2 import BaseLoader, Environment

from streamt.compiler.manifest import (
    ConnectorArtifact,
    FlinkJobArtifact,
    GatewayRuleArtifact,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.core.dag import DAGBuilder
from streamt.core.models import (
    MaterializedType,
    Model,
    Source,
    StreamtProject,
)
from streamt.core.parser import ProjectParser

# Connector class mapping
CONNECTOR_CLASSES = {
    "snowflake-sink": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "jdbc-sink": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "s3-sink": "io.confluent.connect.s3.S3SinkConnector",
    "elasticsearch-sink": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "bigquery-sink": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
}


class CompileError(Exception):
    """Error during compilation."""

    pass


class Compiler:
    """Compiler for streamt projects."""

    def __init__(self, project: StreamtProject, output_dir: Optional[Path] = None) -> None:
        """Initialize compiler."""
        self.project = project
        self.output_dir = output_dir or (
            project.project_path / "generated" if project.project_path else Path("generated")
        )
        self.parser = ProjectParser(project.project_path) if project.project_path else None

        # Build DAG
        dag_builder = DAGBuilder(project)
        self.dag = dag_builder.build()

        # Jinja environment
        self.jinja_env = Environment(loader=BaseLoader())

        # Artifacts
        self.schemas: list[SchemaArtifact] = []
        self.topics: list[TopicArtifact] = []
        self.flink_jobs: list[FlinkJobArtifact] = []
        self.connectors: list[ConnectorArtifact] = []
        self.gateway_rules: list[GatewayRuleArtifact] = []

    def compile(self, dry_run: bool = False) -> Manifest:
        """Compile the project."""
        # Clear previous artifacts
        self.schemas = []
        self.topics = []
        self.flink_jobs = []
        self.connectors = []
        self.gateway_rules = []

        # Compile schemas from sources with schema definitions
        for source in self.project.sources:
            self._compile_source_schema(source)

        # Compile models in topological order
        model_order = self.dag.get_models_only()
        for model_name in model_order:
            model = self.project.get_model(model_name)
            if model:
                self._compile_model(model)

        # Create manifest
        manifest = self._create_manifest()

        # Write artifacts
        if not dry_run:
            self._write_artifacts()

        return manifest

    def _compile_source_schema(self, source: Source) -> None:
        """Compile schema artifact from a source with schema definition."""
        if not source.schema_:
            return

        # Generate subject name (topic-value is the convention)
        subject = source.schema_.subject or f"{source.topic}-value"

        # Get schema definition - either inline or reference
        if source.schema_.definition:
            try:
                schema = json.loads(source.schema_.definition)
            except json.JSONDecodeError:
                # Assume it's already a dict-like definition
                schema = {"type": "record", "name": source.name, "fields": []}
        else:
            # Generate basic schema from columns if available
            schema = self._generate_schema_from_columns(source)

        if schema:
            schema_type = source.schema_.format or "AVRO"
            self.schemas.append(
                SchemaArtifact(
                    subject=subject,
                    schema=schema,
                    schema_type=schema_type.upper(),
                )
            )

    def _generate_schema_from_columns(self, source: Source) -> dict | None:
        """Generate Avro schema from source columns."""
        if not source.columns:
            return None

        fields = []
        for col in source.columns:
            # Default to string type, can be enhanced with type mapping
            fields.append({
                "name": col.name,
                "type": ["null", "string"],
                "default": None,
                "doc": col.description or "",
            })

        return {
            "type": "record",
            "name": source.name.replace("-", "_").replace(".", "_"),
            "namespace": "com.streamt",
            "fields": fields,
        }

    def _compile_model(self, model: Model) -> None:
        """Compile a single model."""
        if model.materialized == MaterializedType.TOPIC:
            self._compile_topic_model(model)
        elif model.materialized == MaterializedType.VIRTUAL_TOPIC:
            self._compile_virtual_topic_model(model)
        elif model.materialized == MaterializedType.FLINK:
            self._compile_flink_model(model)
        elif model.materialized == MaterializedType.SINK:
            self._compile_sink_model(model)

    def _compile_topic_model(self, model: Model) -> None:
        """Compile a topic model (creates real Kafka topic)."""
        topic_name = model.topic.name if model.topic and model.topic.name else model.name
        partitions = model.topic.partitions if model.topic else 6
        replication_factor = model.topic.replication_factor if model.topic else 3
        config = model.topic.config if model.topic else {}

        self.topics.append(
            TopicArtifact(
                name=topic_name,
                partitions=partitions or 6,
                replication_factor=replication_factor or 3,
                config=config,
            )
        )

        # If there's SQL transformation, we need a Flink job to populate the topic
        if model.sql:
            self._compile_flink_job_for_topic(model, topic_name)

    def _compile_virtual_topic_model(self, model: Model) -> None:
        """Compile a virtual topic model (Gateway rule)."""
        virtual_topic_name = model.topic.name if model.topic and model.topic.name else model.name

        # Get the source topic
        source_topic = self._get_source_topic(model)
        if not source_topic:
            raise CompileError(
                f"Cannot determine source topic for virtual topic model '{model.name}'"
            )

        # Build interceptors
        interceptors = []

        # Add filter interceptor from SQL WHERE clause
        if model.sql:
            where_clause = self._extract_where_clause(model.sql)
            if where_clause:
                interceptors.append(
                    {
                        "type": "filter",
                        "config": {"where": where_clause},
                    }
                )

        # Add masking interceptors
        if model.security and model.security.policies:
            for policy in model.security.policies:
                if "mask" in policy:
                    mask_config = policy["mask"]
                    interceptors.append(
                        {
                            "type": "mask",
                            "config": {
                                "field": mask_config["column"],
                                "method": mask_config["method"],
                                "forRoles": mask_config.get("for_roles", []),
                            },
                        }
                    )

        self.gateway_rules.append(
            GatewayRuleArtifact(
                name=model.name,
                virtual_topic=virtual_topic_name,
                physical_topic=source_topic,
                interceptors=interceptors,
            )
        )

    def _compile_flink_model(self, model: Model) -> None:
        """Compile a Flink model."""
        # Create output topic
        topic_name = model.topic.name if model.topic and model.topic.name else model.name
        partitions = model.topic.partitions if model.topic else 6
        replication_factor = model.topic.replication_factor if model.topic else 3
        config = model.topic.config if model.topic else {}

        self.topics.append(
            TopicArtifact(
                name=topic_name,
                partitions=partitions or 6,
                replication_factor=replication_factor or 3,
                config=config,
            )
        )

        # Generate Flink SQL
        flink_sql = self._generate_flink_sql(model, topic_name)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=model.name,
                sql=flink_sql,
                cluster=model.flink_cluster,
                parallelism=model.flink.parallelism if model.flink else None,
                checkpoint_interval_ms=model.flink.checkpoint_interval_ms if model.flink else None,
                state_backend=model.flink.state_backend if model.flink else None,
            )
        )

    def _compile_sink_model(self, model: Model) -> None:
        """Compile a sink model (Kafka Connect)."""
        if not model.sink:
            raise CompileError(f"Sink model '{model.name}' has no sink configuration")

        # Get source topic(s)
        source_topics = self._get_source_topics(model)
        if not source_topics:
            raise CompileError(f"Cannot determine source topics for sink model '{model.name}'")

        # Get connector class
        connector_class = CONNECTOR_CLASSES.get(model.sink.connector, model.sink.connector)

        # Build connector config
        config = dict(model.sink.config)

        # Add masking transforms if needed
        if model.security and model.security.policies:
            transforms = []
            transform_configs = {}

            for i, policy in enumerate(model.security.policies):
                if "mask" in policy:
                    mask_config = policy["mask"]
                    transform_name = f"mask{i}"
                    transforms.append(transform_name)
                    transform_configs[f"transforms.{transform_name}.type"] = (
                        "org.apache.kafka.connect.transforms.MaskField$Value"
                    )
                    transform_configs[f"transforms.{transform_name}.fields"] = mask_config["column"]

            if transforms:
                config["transforms"] = ",".join(transforms)
                config.update(transform_configs)

        self.connectors.append(
            ConnectorArtifact(
                name=model.name,
                connector_class=connector_class,
                topics=source_topics,
                config=config,
                cluster=model.connect_cluster,
            )
        )

    def _compile_flink_job_for_topic(self, model: Model, output_topic: str) -> None:
        """Compile a Flink job for a topic model that has SQL."""
        flink_sql = self._generate_flink_sql(model, output_topic)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=f"{model.name}_processor",
                sql=flink_sql,
                cluster=model.flink_cluster,
            )
        )

    def _generate_flink_sql(self, model: Model, output_topic: str) -> str:
        """Generate Flink SQL for a model."""
        sql_parts = []

        # Generate CREATE TABLE statements for sources
        dependencies = self._get_model_dependencies(model)

        for dep_name, dep_type in dependencies:
            if dep_type == "source":
                source = self.project.get_source(dep_name)
                if source:
                    sql_parts.append(self._generate_source_table_ddl(source, dep_name))
            else:
                dep_model = self.project.get_model(dep_name)
                if dep_model:
                    topic_name = (
                        dep_model.topic.name
                        if dep_model.topic and dep_model.topic.name
                        else dep_model.name
                    )
                    sql_parts.append(
                        self._generate_model_table_ddl(dep_model, dep_name, topic_name)
                    )

        # Generate CREATE TABLE for output
        sql_parts.append(self._generate_sink_table_ddl(model, output_topic))

        # Generate INSERT statement
        transformed_sql = self._transform_sql(model.sql or "")

        # Apply masking functions if needed
        if model.security and model.security.policies:
            for policy in model.security.policies:
                if "mask" in policy:
                    mask_config = policy["mask"]
                    column = mask_config["column"]
                    method = mask_config["method"]
                    mask_fn = self._get_flink_mask_function(method)
                    # Replace column reference with masked version
                    transformed_sql = re.sub(
                        rf"\b{column}\b",
                        f"{mask_fn}({column}) AS {column}",
                        transformed_sql,
                        count=1,
                    )

        sql_parts.append(f"INSERT INTO {model.name}_sink\n{transformed_sql};")

        return "\n\n".join(sql_parts)

    def _generate_source_table_ddl(self, source: Source, alias: str) -> str:
        """Generate Flink CREATE TABLE DDL for a source."""
        kafka_config = self.project.runtime.kafka
        bootstrap = kafka_config.bootstrap_servers

        return f"""CREATE TABLE {alias} (
    -- Schema inferred from Schema Registry
    `_raw` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{source.topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);"""

    def _generate_model_table_ddl(self, model: Model, alias: str, topic_name: str) -> str:
        """Generate Flink CREATE TABLE DDL for a model reference."""
        kafka_config = self.project.runtime.kafka
        bootstrap = kafka_config.bootstrap_servers

        return f"""CREATE TABLE {alias} (
    -- Schema inferred from upstream model
    `_raw` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);"""

    def _generate_sink_table_ddl(self, model: Model, topic_name: str) -> str:
        """Generate Flink CREATE TABLE DDL for the output sink."""
        kafka_config = self.project.runtime.kafka
        bootstrap = kafka_config.bootstrap_servers

        return f"""CREATE TABLE {model.name}_sink (
    -- Schema defined by SELECT
    `_raw` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'format' = 'json'
);"""

    def _transform_sql(self, sql: str) -> str:
        """Transform Jinja SQL to plain SQL."""
        # Replace {{ source("name") }} with table name
        sql = re.sub(
            r'\{\{\s*source\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}',
            r"\1",
            sql,
        )
        # Replace {{ ref("name") }} with table name
        sql = re.sub(
            r'\{\{\s*ref\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}',
            r"\1",
            sql,
        )
        return sql.strip()

    def _get_flink_mask_function(self, method: str) -> str:
        """Get Flink masking function for a method."""
        if method == "hash":
            return "MD5"
        elif method == "redact":
            return "REGEXP_REPLACE"  # Will need params
        elif method == "partial":
            return "REGEXP_REPLACE"  # Will need params
        elif method == "null":
            return "NULLIF"
        else:
            return "MD5"  # Default to hash

    def _get_source_topic(self, model: Model) -> Optional[str]:
        """Get the source topic for a model."""
        if model.sql and self.parser:
            sources, refs = self.parser.extract_refs_from_sql(model.sql)
            if sources:
                source = self.project.get_source(sources[0])
                if source:
                    return source.topic
            if refs:
                ref_model = self.project.get_model(refs[0])
                if ref_model:
                    return (
                        ref_model.topic.name
                        if ref_model.topic and ref_model.topic.name
                        else ref_model.name
                    )
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    source = self.project.get_source(from_ref.source)
                    if source:
                        return source.topic
                if from_ref.ref:
                    ref_model = self.project.get_model(from_ref.ref)
                    if ref_model:
                        return (
                            ref_model.topic.name
                            if ref_model.topic and ref_model.topic.name
                            else ref_model.name
                        )
        return None

    def _get_source_topics(self, model: Model) -> list[str]:
        """Get all source topics for a model."""
        topics = []

        if model.sql and self.parser:
            sources, refs = self.parser.extract_refs_from_sql(model.sql)
            for source_name in sources:
                source = self.project.get_source(source_name)
                if source:
                    topics.append(source.topic)
            for ref_name in refs:
                ref_model = self.project.get_model(ref_name)
                if ref_model:
                    topics.append(
                        ref_model.topic.name
                        if ref_model.topic and ref_model.topic.name
                        else ref_model.name
                    )
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    source = self.project.get_source(from_ref.source)
                    if source:
                        topics.append(source.topic)
                if from_ref.ref:
                    ref_model = self.project.get_model(from_ref.ref)
                    if ref_model:
                        topics.append(
                            ref_model.topic.name
                            if ref_model.topic and ref_model.topic.name
                            else ref_model.name
                        )

        return topics

    def _get_model_dependencies(self, model: Model) -> list[tuple[str, str]]:
        """Get model dependencies as (name, type) tuples."""
        dependencies = []

        if model.sql and self.parser:
            sources, refs = self.parser.extract_refs_from_sql(model.sql)
            for source_name in sources:
                dependencies.append((source_name, "source"))
            for ref_name in refs:
                dependencies.append((ref_name, "model"))
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    dependencies.append((from_ref.source, "source"))
                if from_ref.ref:
                    dependencies.append((from_ref.ref, "model"))

        return dependencies

    def _extract_where_clause(self, sql: str) -> Optional[str]:
        """Extract WHERE clause from SQL."""
        match = re.search(
            r"WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)", sql, re.IGNORECASE | re.DOTALL
        )
        if match:
            return match.group(1).strip()
        return None

    def _create_manifest(self) -> Manifest:
        """Create the manifest."""
        return Manifest(
            version=self.project.project.version or "0.0.0",
            project_name=self.project.project.name,
            sources=[s.model_dump() for s in self.project.sources],
            models=[m.model_dump() for m in self.project.models],
            tests=[t.model_dump() for t in self.project.tests],
            exposures=[e.model_dump() for e in self.project.exposures],
            dag=self.dag.to_dict(),
            artifacts={
                "schemas": [s.to_dict() for s in self.schemas],
                "topics": [t.to_dict() for t in self.topics],
                "flink_jobs": [f.to_dict() for f in self.flink_jobs],
                "connectors": [c.to_dict() for c in self.connectors],
                "gateway_rules": [g.to_dict() for g in self.gateway_rules],
            },
        )

    def _write_artifacts(self) -> None:
        """Write all artifacts to output directory."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Write schemas
        if self.schemas:
            schemas_dir = self.output_dir / "schemas"
            schemas_dir.mkdir(exist_ok=True)
            for schema in self.schemas:
                # Write schema file
                path = schemas_dir / f"{schema.subject}.json"
                with open(path, "w") as f:
                    json.dump(schema.to_dict(), f, indent=2)

        # Write topics
        topics_dir = self.output_dir / "topics"
        topics_dir.mkdir(exist_ok=True)
        for topic in self.topics:
            path = topics_dir / f"{topic.name}.json"
            with open(path, "w") as f:
                json.dump(topic.to_dict(), f, indent=2)

        # Write Flink jobs
        flink_dir = self.output_dir / "flink"
        flink_dir.mkdir(exist_ok=True)
        for job in self.flink_jobs:
            # Write SQL file
            sql_path = flink_dir / f"{job.name}.sql"
            with open(sql_path, "w") as f:
                f.write(job.sql)
            # Write config file
            config_path = flink_dir / f"{job.name}.json"
            with open(config_path, "w") as f:
                json.dump(job.to_dict(), f, indent=2)

        # Write connectors
        connect_dir = self.output_dir / "connect"
        connect_dir.mkdir(exist_ok=True)
        for connector in self.connectors:
            path = connect_dir / f"{connector.name}.json"
            with open(path, "w") as f:
                json.dump(connector.to_dict(), f, indent=2)

        # Write gateway rules
        if self.gateway_rules:
            gateway_dir = self.output_dir / "gateway"
            gateway_dir.mkdir(exist_ok=True)
            for rule in self.gateway_rules:
                path = gateway_dir / f"{rule.name}.json"
                with open(path, "w") as f:
                    json.dump(rule.to_dict(), f, indent=2)

        # Write manifest
        manifest = self._create_manifest()
        manifest.save(self.output_dir / "manifest.json")
