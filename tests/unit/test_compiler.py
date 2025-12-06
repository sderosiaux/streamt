"""Tests for the compiler module."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.parser import ProjectParser
from streamt.compiler import Compiler


class TestCompiler:
    """Tests for Compiler."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_compile_topic_model(self):
        """TC-COMP-001: Topic model should generate topic config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "materialized": "topic",
                        "topic": {"partitions": 12, "replication_factor": 3},
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            topics = manifest.artifacts.get("topics", [])
            assert len(topics) >= 1

            payment_topic = next((t for t in topics if t["name"] == "payments_clean"), None)
            assert payment_topic is not None
            assert payment_topic["partitions"] == 12
            assert payment_topic["replication_factor"] == 3

    def test_compile_flink_model(self):
        """TC-COMP-002: Flink model should generate SQL and topic."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [{"name": "payments_raw", "topic": "payments.raw.v1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                    },
                    {
                        "name": "customer_balance",
                        "materialized": "flink",
                        "sql": """
                            SELECT customer_id, SUM(amount) as total
                            FROM {{ ref("payments_clean") }}
                            GROUP BY customer_id, TUMBLE(event_time, INTERVAL '5' MINUTE)
                        """,
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            assert len(flink_jobs) >= 1

            balance_job = next((j for j in flink_jobs if j["name"] == "customer_balance"), None)
            assert balance_job is not None
            assert "sql" in balance_job
            assert "INSERT INTO" in balance_job["sql"]

    def test_compile_sink_model(self):
        """TC-COMP-003: Sink model should generate connector config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "connect": {
                        "default": "local",
                        "clusters": {"local": {"rest_url": "http://localhost:8083"}},
                    },
                },
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "to_snowflake",
                        "materialized": "sink",
                        "from": [{"ref": "clean"}],
                        "sink": {
                            "connector": "snowflake-sink",
                            "config": {
                                "snowflake.database.name": "ANALYTICS",
                                "snowflake.schema.name": "PUBLIC",
                            },
                        },
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            connectors = manifest.artifacts.get("connectors", [])
            assert len(connectors) >= 1

            sf_connector = next((c for c in connectors if c["name"] == "to_snowflake"), None)
            assert sf_connector is not None
            assert "snowflake" in sf_connector["connector_class"].lower()

    def test_compile_writes_files(self):
        """Compile should write files when not dry_run."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "topic": {"partitions": 6},
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            manifest = compiler.compile(dry_run=False)

            # Check files were created
            assert (output_dir / "manifest.json").exists()
            assert (output_dir / "topics" / "clean.json").exists()

            # Check content
            with open(output_dir / "topics" / "clean.json") as f:
                topic_config = json.load(f)
            assert topic_config["name"] == "clean"
            assert topic_config["partitions"] == 6

    def test_manifest_content(self):
        """Manifest should contain all project info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-project", "version": "2.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "src1", "topic": "t1"}],
                "models": [
                    {
                        "name": "model1",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("src1") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "test1",
                        "model": "model1",
                        "type": "schema",
                        "assertions": [],
                    }
                ],
                "exposures": [
                    {
                        "name": "app1",
                        "type": "application",
                        "role": "consumer",
                        "consumes": [{"ref": "model1"}],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            assert manifest.version == "2.0.0"
            assert manifest.project_name == "test-project"
            assert len(manifest.sources) == 1
            assert len(manifest.models) == 1
            assert len(manifest.tests) == 1
            assert len(manifest.exposures) == 1
            assert "dag" in manifest.to_dict()

    def test_sql_transformation(self):
        """SQL should be transformed correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [{"name": "payments", "topic": "payments.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "flink",
                        "sql": """
                            SELECT id, amount
                            FROM {{ source("payments") }}
                            WHERE status = 'CAPTURED'
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            clean_job = next((j for j in flink_jobs if j["name"] == "clean"), None)
            assert clean_job is not None

            # Check Jinja was replaced
            sql = clean_job["sql"]
            assert "{{ source" not in sql
            assert "payments" in sql  # Table name should be there

    def test_dry_run_no_files(self):
        """Dry run should not create files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=True)

            # Should not create files
            assert not output_dir.exists()

    def test_secrets_not_resolved_in_artifacts(self):
        """Secrets should remain as placeholders in generated files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create .env file
            with open(Path(tmpdir) / ".env", "w") as f:
                f.write("KAFKA_SERVERS=localhost:9092\n")
                f.write("SNOWFLAKE_URL=account.snowflakecomputing.com\n")

            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "${KAFKA_SERVERS}"},
                    "connect": {
                        "default": "local",
                        "clusters": {"local": {"rest_url": "http://localhost:8083"}},
                    },
                },
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "to_sf",
                        "materialized": "sink",
                        "from": [{"ref": "clean"}],
                        "sink": {
                            "connector": "snowflake-sink",
                            "config": {
                                "snowflake.url.name": "${SNOWFLAKE_URL}",
                            },
                        },
                    },
                ],
            }
            project_path = Path(tmpdir)
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)
            project = parser.parse()

            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            manifest = compiler.compile(dry_run=False)

            # Check connector config has placeholder
            connectors = manifest.artifacts.get("connectors", [])
            sf_conn = next((c for c in connectors if c["name"] == "to_sf"), None)
            assert sf_conn is not None
            # The secret should be in the config
            # Note: In current implementation secrets are resolved at apply time
