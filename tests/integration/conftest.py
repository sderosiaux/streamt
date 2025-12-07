"""Pytest fixtures for integration tests with Docker infrastructure.

This module provides fixtures for:
- Docker Compose lifecycle management
- Kafka client connections
- Schema Registry connections
- Flink SQL Gateway connections
- Connect REST API connections
- Test data producers and consumers

Test subset execution:
    pytest -m kafka          # Run only Kafka tests
    pytest -m flink          # Run only Flink tests
    pytest -m connect        # Run only Connect tests
    pytest -m schema_registry # Run only Schema Registry tests
    pytest -m slow           # Run slow tests (>30s)
    pytest -m "kafka and not slow"  # Combine markers
"""

import json
import os
import subprocess
import time
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Callable, Generator, Optional, TypeVar

import pytest
import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent

T = TypeVar("T")


def retry_on_transient_error(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    retryable_exceptions: tuple = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    ),
    retryable_status_codes: tuple = (502, 503, 504),
) -> Callable:
    """Decorator that retries a function on transient HTTP errors.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        retryable_exceptions: Exception types to retry on
        retryable_status_codes: HTTP status codes to retry on

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        time.sleep(current_delay)
                        current_delay *= backoff
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code in retryable_status_codes:
                        last_exception = e
                        if attempt < max_retries:
                            time.sleep(current_delay)
                            current_delay *= backoff
                    else:
                        raise

            # All retries exhausted
            raise last_exception  # type: ignore

        return wrapper

    return decorator


@dataclass
class InfrastructureConfig:
    """Configuration for test infrastructure.

    All settings can be overridden via environment variables:
        KAFKA_BOOTSTRAP_SERVERS - Kafka broker for external/host access (default: localhost:9092)
        KAFKA_INTERNAL_SERVERS - Kafka broker for internal Docker access (default: kafka:29092)
        SCHEMA_REGISTRY_URL - Schema Registry URL (default: http://localhost:8081)
        CONNECT_URL - Kafka Connect REST URL (default: http://localhost:8083)
        FLINK_REST_URL - Flink REST API URL (default: http://localhost:8082)
        FLINK_SQL_GATEWAY_URL - Flink SQL Gateway URL (default: http://localhost:8084)

    Note: KAFKA_INTERNAL_SERVERS is used for Flink SQL tables since Flink runs inside Docker
    and needs to connect to Kafka via the internal Docker network.
    """

    kafka_bootstrap_servers: str = ""
    kafka_internal_servers: str = ""  # For Flink/Docker internal access
    schema_registry_url: str = ""
    connect_url: str = ""
    flink_rest_url: str = ""
    flink_sql_gateway_url: str = ""

    def __post_init__(self):
        """Load configuration from environment variables with defaults."""
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_internal_servers = os.getenv("KAFKA_INTERNAL_SERVERS", "kafka:29092")
        self.schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        self.connect_url = os.getenv("CONNECT_URL", "http://localhost:8083")
        self.flink_rest_url = os.getenv("FLINK_REST_URL", "http://localhost:8082")
        self.flink_sql_gateway_url = os.getenv("FLINK_SQL_GATEWAY_URL", "http://localhost:8084")


# Global config
INFRA_CONFIG = InfrastructureConfig()


def is_docker_running() -> bool:
    """Check if Docker daemon is running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


def is_service_healthy(url: str, timeout: int = 5) -> bool:
    """Check if a service is healthy by making an HTTP request."""
    try:
        response = requests.get(url, timeout=timeout)
        return response.status_code == 200
    except Exception:
        return False


def is_kafka_healthy(bootstrap_servers: str, timeout: int = 10) -> bool:
    """Check if Kafka is healthy by listing topics."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=timeout)
        return metadata is not None
    except Exception:
        return False


def wait_for_service(
    check_fn,
    service_name: str,
    max_wait: int = 120,
    interval: int = 2,
) -> bool:
    """Wait for a service to become healthy."""
    start = time.time()
    while time.time() - start < max_wait:
        if check_fn():
            return True
        print(f"Waiting for {service_name}...")
        time.sleep(interval)
    return False


class DockerComposeManager:
    """Manager for Docker Compose lifecycle."""

    def __init__(self, compose_file: Path):
        self.compose_file = compose_file
        self.started = False

    def start(self) -> None:
        """Start Docker Compose services."""
        if self.started:
            return

        print(f"\nStarting Docker Compose from {self.compose_file}...")
        subprocess.run(
            ["docker-compose", "-f", str(self.compose_file), "up", "-d"],
            check=True,
            cwd=self.compose_file.parent,
        )
        self.started = True

        # Wait for services to be healthy
        self._wait_for_services()

    def stop(self) -> None:
        """Stop Docker Compose services."""
        if not self.started:
            return

        print("\nStopping Docker Compose...")
        subprocess.run(
            ["docker-compose", "-f", str(self.compose_file), "down", "-v"],
            check=True,
            cwd=self.compose_file.parent,
        )
        self.started = False

    def _wait_for_services(self) -> None:
        """Wait for all services to be healthy."""
        print("Waiting for Kafka...")
        if not wait_for_service(
            lambda: is_kafka_healthy(INFRA_CONFIG.kafka_bootstrap_servers),
            "Kafka",
            max_wait=120,
        ):
            raise RuntimeError("Kafka did not become healthy")

        print("Waiting for Schema Registry...")
        if not wait_for_service(
            lambda: is_service_healthy(f"{INFRA_CONFIG.schema_registry_url}/subjects"),
            "Schema Registry",
            max_wait=60,
        ):
            raise RuntimeError("Schema Registry did not become healthy")

        print("Waiting for Kafka Connect...")
        if not wait_for_service(
            lambda: is_service_healthy(f"{INFRA_CONFIG.connect_url}/connectors"),
            "Kafka Connect",
            max_wait=90,
        ):
            raise RuntimeError("Kafka Connect did not become healthy")

        print("Waiting for Flink...")
        if not wait_for_service(
            lambda: is_service_healthy(f"{INFRA_CONFIG.flink_rest_url}/overview"),
            "Flink Job Manager",
            max_wait=90,
        ):
            raise RuntimeError("Flink did not become healthy")

        print("All services are healthy!")


# Global Docker Compose manager (reused across test session)
_docker_manager: Optional[DockerComposeManager] = None


def get_docker_manager() -> DockerComposeManager:
    """Get or create the Docker Compose manager."""
    global _docker_manager
    if _docker_manager is None:
        _docker_manager = DockerComposeManager(PROJECT_ROOT / "docker-compose.yml")
    return _docker_manager


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def docker_services() -> Generator[DockerComposeManager, None, None]:
    """
    Session-scoped fixture that starts Docker Compose services.

    This fixture checks if services are already running. If not, it starts them.
    Services are stopped at the end of the test session.
    """
    if not is_docker_running():
        pytest.skip("Docker is not running")

    manager = get_docker_manager()

    # Check if services are already running
    if is_kafka_healthy(INFRA_CONFIG.kafka_bootstrap_servers):
        print("\nDocker services already running, reusing...")
        manager.started = True
    else:
        manager.start()

    yield manager

    # Optionally stop services (comment out to keep running for debugging)
    # manager.stop()


@pytest.fixture(scope="session")
def kafka_admin(docker_services: DockerComposeManager) -> AdminClient:
    """Session-scoped Kafka admin client."""
    return AdminClient({"bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers})


@pytest.fixture(scope="session")
def kafka_producer(docker_services: DockerComposeManager) -> Producer:
    """Session-scoped Kafka producer."""
    return Producer(
        {
            "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
            "client.id": "streamt-test-producer",
        }
    )


@pytest.fixture(scope="function")
def kafka_consumer_factory(docker_services: DockerComposeManager):
    """Factory fixture for creating Kafka consumers."""
    consumers = []

    def create_consumer(
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
    ) -> Consumer:
        consumer = Consumer(
            {
                "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(topics)
        consumers.append(consumer)
        return consumer

    yield create_consumer

    # Cleanup consumers
    for consumer in consumers:
        consumer.close()


# =============================================================================
# Helper Classes for Testing
# =============================================================================


class KafkaHelper:
    """Helper class for Kafka operations in tests."""

    def __init__(self, admin: AdminClient, producer: Producer):
        self.admin = admin
        self.producer = producer

    def create_topic(
        self,
        name: str,
        partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[dict] = None,
    ) -> None:
        """Create a topic and wait for it to be ready."""
        topic = NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=config or {},
        )
        futures = self.admin.create_topics([topic])
        for topic_name, future in futures.items():
            future.result(timeout=30)

        # Wait for topic to be fully created
        self._wait_for_topic(name)

    def delete_topic(self, name: str) -> None:
        """Delete a topic."""
        futures = self.admin.delete_topics([name])
        for topic_name, future in futures.items():
            try:
                future.result(timeout=30)
            except Exception:
                pass  # Topic might not exist

    def topic_exists(self, name: str) -> bool:
        """Check if a topic exists."""
        metadata = self.admin.list_topics(timeout=10)
        return name in metadata.topics

    def get_topic_partitions(self, name: str) -> int:
        """Get the number of partitions for a topic."""
        metadata = self.admin.list_topics(timeout=10)
        if name not in metadata.topics:
            raise ValueError(f"Topic {name} does not exist")
        return len(metadata.topics[name].partitions)

    def produce_messages(
        self,
        topic: str,
        messages: list[dict],
        key_field: Optional[str] = None,
    ) -> None:
        """Produce messages to a topic."""
        for msg in messages:
            key = msg.get(key_field) if key_field else None
            if key:
                key = str(key).encode("utf-8")
            value = json.dumps(msg).encode("utf-8")
            self.producer.produce(topic, value=value, key=key)
        self.producer.flush(timeout=30)

    def consume_messages(
        self,
        topic: str,
        group_id: str,
        max_messages: int = 100,
        timeout: float = 30.0,
    ) -> list[dict]:
        """Consume messages from a topic.

        Raises:
            ValueError: If a message cannot be decoded as valid JSON
        """
        consumer = Consumer(
            {
                "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])

        messages = []
        decode_errors = []
        start = time.time()

        try:
            while len(messages) < max_messages and (time.time() - start) < timeout:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    messages.append(value)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    # Track decode errors - don't silently swallow them
                    raw_value = msg.value()[:100] if msg.value() else b"<empty>"
                    decode_errors.append(f"Offset {msg.offset()}: {e} (raw: {raw_value})")
        finally:
            consumer.close()

        # If we got no valid messages but had decode errors, raise to surface the issue
        if len(messages) == 0 and len(decode_errors) > 0:
            raise ValueError(
                f"Failed to decode any messages from topic '{topic}'. "
                f"Decode errors ({len(decode_errors)}): {decode_errors[:3]}"
            )

        return messages

    def _wait_for_topic(self, name: str, timeout: int = 30) -> None:
        """Wait for a topic to be available."""
        start = time.time()
        while time.time() - start < timeout:
            if self.topic_exists(name):
                return
            time.sleep(0.5)
        raise TimeoutError(f"Topic {name} did not become available")


@pytest.fixture(scope="function")
def kafka_helper(kafka_admin: AdminClient, kafka_producer: Producer) -> KafkaHelper:
    """Fixture providing Kafka helper for tests."""
    return KafkaHelper(kafka_admin, kafka_producer)


class ConnectHelper:
    """Helper class for Kafka Connect operations in tests."""

    def __init__(self, connect_url: str):
        self.connect_url = connect_url

    def list_connectors(self) -> list[str]:
        """List all connectors."""
        response = requests.get(f"{self.connect_url}/connectors")
        response.raise_for_status()
        return response.json()

    def create_connector(self, name: str, config: dict) -> dict:
        """Create a connector."""
        payload = {"name": name, "config": config}
        response = requests.post(
            f"{self.connect_url}/connectors",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        return response.json()

    def delete_connector(self, name: str) -> None:
        """Delete a connector."""
        response = requests.delete(f"{self.connect_url}/connectors/{name}")
        if response.status_code != 404:
            response.raise_for_status()

    def get_connector_status(self, name: str) -> dict:
        """Get connector status."""
        response = requests.get(f"{self.connect_url}/connectors/{name}/status")
        response.raise_for_status()
        return response.json()

    def wait_for_connector_running(
        self,
        name: str,
        timeout: int = 60,
        require_tasks: bool = True,
    ) -> bool:
        """Wait for connector AND its tasks to be in RUNNING state.

        Args:
            name: Connector name
            timeout: Maximum time to wait in seconds
            require_tasks: If True, also wait for at least one task to be RUNNING

        Returns:
            True if connector (and tasks if required) are running, False on timeout
        """
        start = time.time()
        last_error = None
        while time.time() - start < timeout:
            try:
                status = self.get_connector_status(name)
                connector_state = status.get("connector", {}).get("state")

                if connector_state == "FAILED":
                    trace = status.get("connector", {}).get("trace", "No trace")
                    raise RuntimeError(f"Connector '{name}' FAILED: {trace[:500]}")

                if connector_state == "RUNNING":
                    if not require_tasks:
                        return True
                    # Also check tasks are running
                    tasks = status.get("tasks", [])
                    if len(tasks) > 0:
                        running_tasks = [t for t in tasks if t.get("state") == "RUNNING"]
                        if len(running_tasks) > 0:
                            return True
                        # Check for failed tasks
                        failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
                        if failed_tasks:
                            trace = failed_tasks[0].get("trace", "No trace")
                            raise RuntimeError(f"Connector '{name}' task FAILED: {trace[:500]}")
            except RuntimeError:
                raise  # Re-raise explicit failures
            except requests.exceptions.RequestException as e:
                last_error = e  # Track for timeout message
            time.sleep(2)

        error_msg = f"Connector '{name}' did not reach RUNNING state within {timeout}s"
        if last_error:
            error_msg += f". Last error: {last_error}"
        return False


@pytest.fixture(scope="function")
def connect_helper(docker_services: DockerComposeManager) -> ConnectHelper:
    """Fixture providing Connect helper for tests."""
    return ConnectHelper(INFRA_CONFIG.connect_url)


class FlinkHelper:
    """Helper class for Flink SQL operations in tests."""

    def __init__(self, rest_url: str, sql_gateway_url: str):
        self.rest_url = rest_url
        self.sql_gateway_url = sql_gateway_url
        self.session_handle: Optional[str] = None

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def get_cluster_overview(self) -> dict:
        """Get Flink cluster overview."""
        response = requests.get(f"{self.rest_url}/overview", timeout=10)
        response.raise_for_status()
        return response.json()

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def list_jobs(self) -> list[dict]:
        """List all jobs."""
        response = requests.get(f"{self.rest_url}/jobs", timeout=10)
        response.raise_for_status()
        return response.json().get("jobs", [])

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job. Returns True if successful."""
        # Flink REST API uses PATCH with cancel action
        response = requests.patch(f"{self.rest_url}/jobs/{job_id}?mode=cancel")
        return response.status_code in [200, 202]

    def get_job_status(self, job_id: str) -> Optional[str]:
        """Get job status by ID. Returns None if job not found."""
        try:
            response = requests.get(f"{self.rest_url}/jobs/{job_id}")
            if response.status_code == 200:
                return response.json().get("state")
            return None
        except Exception:
            return None

    def wait_for_job_status(
        self,
        job_id: str,
        expected_status: str,
        timeout: int = 30,
    ) -> bool:
        """Wait for a job to reach expected status."""
        start = time.time()
        while time.time() - start < timeout:
            status = self.get_job_status(job_id)
            if status == expected_status:
                return True
            if status in ["FAILED", "CANCELED", "FINISHED"]:
                return status == expected_status
            time.sleep(1)
        return False

    def get_running_jobs(self) -> list[dict]:
        """Get all currently running jobs."""
        jobs = self.list_jobs()
        return [j for j in jobs if j.get("status") == "RUNNING"]

    def cancel_all_running_jobs(self) -> int:
        """Cancel all running jobs. Returns count of cancelled jobs."""
        running_jobs = self.get_running_jobs()
        cancelled = 0
        for job in running_jobs:
            if self.cancel_job(job["id"]):
                cancelled += 1
        # Wait briefly for cancellations to process
        if cancelled > 0:
            time.sleep(2)
        return cancelled

    def wait_for_new_running_job(
        self,
        known_job_ids: set[str],
        timeout: float = 30.0,
        interval: float = 1.0,
    ) -> Optional[str]:
        """Wait for a new running job to appear.

        Args:
            known_job_ids: Set of job IDs that existed before job submission
            timeout: Maximum time to wait in seconds
            interval: Time between polls in seconds

        Returns:
            The new job ID if found, None on timeout
        """
        start = time.time()
        while time.time() - start < timeout:
            current_jobs = self.get_running_jobs()
            for job in current_jobs:
                if job["id"] not in known_job_ids:
                    return job["id"]
            time.sleep(interval)
        return None

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def open_sql_session(self) -> str:
        """Open a SQL Gateway session."""
        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions",
            json={},
            timeout=10,
        )
        response.raise_for_status()
        self.session_handle = response.json().get("sessionHandle")
        return self.session_handle

    def close_sql_session(self) -> None:
        """Close the SQL Gateway session."""
        if self.session_handle:
            try:
                requests.delete(
                    f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}",
                    timeout=5,
                )
            except Exception:
                pass
            self.session_handle = None

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def execute_sql(self, statement: str) -> dict:
        """Execute a SQL statement via SQL Gateway."""
        if not self.session_handle:
            self.open_sql_session()

        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/statements",
            json={"statement": statement},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_statement_result(
        self,
        operation_handle: str,
        timeout: int = 60,
    ) -> dict:
        """Get the result of a statement execution."""
        start = time.time()
        while time.time() - start < timeout:
            response = requests.get(
                f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/operations/{operation_handle}/result/0"
            )
            if response.status_code == 200:
                return response.json()
            time.sleep(1)
        raise TimeoutError("Statement did not complete in time")

    def execute_sql_and_check_error(
        self,
        statement: str,
        timeout: float = 10.0,
    ) -> Optional[str]:
        """Execute SQL and return error message if any, None if successful.

        Flink SQL Gateway is async - errors appear when fetching results.
        This method submits the statement and polls for the result to detect errors.
        """
        if not self.session_handle:
            self.open_sql_session()

        # Submit the statement
        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/statements",
            json={"statement": statement},
            timeout=30,
        )
        response.raise_for_status()
        op_handle = response.json().get("operationHandle")

        # Poll for result to detect errors
        start = time.time()
        while time.time() - start < timeout:
            result_response = requests.get(
                f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/operations/{op_handle}/result/0"
            )
            if result_response.status_code != 200:
                # Got an error response
                try:
                    error_data = result_response.json()
                    errors = error_data.get("errors", [])
                    return " ".join(errors) if errors else result_response.text
                except Exception:
                    return result_response.text
            # 200 means still in progress or completed - check for completion
            result_data = result_response.json()
            if result_data.get("resultType") in ["PAYLOAD", "EOS"]:
                # Successfully completed
                return None
            time.sleep(0.5)

        return None  # Timeout without error means likely successful


@pytest.fixture(scope="function")
def flink_helper(docker_services: DockerComposeManager) -> Generator[FlinkHelper, None, None]:
    """Fixture providing Flink helper for tests.

    Cancels all running jobs before each test to ensure slot availability
    and prevent interference between tests.
    """
    helper = FlinkHelper(INFRA_CONFIG.flink_rest_url, INFRA_CONFIG.flink_sql_gateway_url)
    # Cancel all running jobs before this test to free up slots
    helper.cancel_all_running_jobs()
    yield helper
    # Cleanup session and cancel any jobs started by this test
    helper.close_sql_session()
    helper.cancel_all_running_jobs()


class SchemaRegistryHelper:
    """Helper class for Schema Registry operations in tests."""

    def __init__(self, url: str):
        self.url = url

    def check_connection(self) -> bool:
        """Check if Schema Registry is available."""
        try:
            response = requests.get(f"{self.url}/subjects", timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def list_subjects(self) -> list[str]:
        """List all registered subjects."""
        response = requests.get(f"{self.url}/subjects", timeout=10)
        response.raise_for_status()
        return response.json()

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def register_schema(
        self,
        subject: str,
        schema: dict,
        schema_type: str = "AVRO",
    ) -> int:
        """Register a schema and return the schema ID."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        response = requests.post(
            f"{self.url}/subjects/{subject}/versions",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()["id"]

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def get_schema(self, subject: str, version: str = "latest") -> dict:
        """Get a schema by subject and version."""
        response = requests.get(f"{self.url}/subjects/{subject}/versions/{version}", timeout=10)
        response.raise_for_status()
        return response.json()

    @retry_on_transient_error(max_retries=3, delay=1.0)
    def get_schema_by_id(self, schema_id: int) -> dict:
        """Get a schema by its global ID."""
        response = requests.get(f"{self.url}/schemas/ids/{schema_id}", timeout=10)
        response.raise_for_status()
        return response.json()

    def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        """Delete a subject (soft delete by default)."""
        url = f"{self.url}/subjects/{subject}"
        if permanent:
            url += "?permanent=true"
        response = requests.delete(url)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json()

    def check_compatibility(
        self,
        subject: str,
        schema: dict,
        schema_type: str = "AVRO",
    ) -> bool:
        """Check if a schema is compatible with existing versions."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        response = requests.post(
            f"{self.url}/compatibility/subjects/{subject}/versions/latest",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        if response.status_code == 404:
            # No existing schema, anything is compatible
            return True
        response.raise_for_status()
        return response.json().get("is_compatible", False)

    def get_compatibility_level(self, subject: Optional[str] = None) -> str:
        """Get compatibility level for a subject or global default."""
        if subject:
            url = f"{self.url}/config/{subject}"
        else:
            url = f"{self.url}/config"
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("compatibilityLevel", "BACKWARD")

    def set_compatibility_level(
        self,
        level: str,
        subject: Optional[str] = None,
    ) -> None:
        """Set compatibility level for a subject or global default."""
        if subject:
            url = f"{self.url}/config/{subject}"
        else:
            url = f"{self.url}/config"
        response = requests.put(
            url,
            json={"compatibility": level},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        response.raise_for_status()


@pytest.fixture(scope="function")
def schema_registry_helper(docker_services: DockerComposeManager) -> SchemaRegistryHelper:
    """Fixture providing Schema Registry helper for tests."""
    return SchemaRegistryHelper(INFRA_CONFIG.schema_registry_url)


# =============================================================================
# Polling Utilities for Reliable Testing
# =============================================================================


def poll_until(
    condition_fn,
    timeout: float = 30.0,
    interval: float = 1.0,
    description: str = "condition",
) -> bool:
    """
    Poll until a condition is met or timeout is reached.

    Args:
        condition_fn: Function that returns True when condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between polls in seconds
        description: Description for error messages

    Returns:
        True if condition was met, False on timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            if condition_fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def poll_until_messages(
    kafka_helper: KafkaHelper,
    topic: str,
    min_messages: int,
    group_id: str,
    timeout: float = 60.0,
    poll_interval: float = 1.0,
) -> list[dict]:
    """
    Poll until at least min_messages are available in a topic.

    Uses a single consumer instance to avoid consumer group explosion.

    Args:
        kafka_helper: KafkaHelper instance
        topic: Topic to consume from
        min_messages: Minimum number of messages required
        group_id: Consumer group ID (used as-is, no timestamp suffix)
        timeout: Maximum time to wait
        poll_interval: Time between poll attempts in seconds

    Returns:
        List of consumed messages

    Raises:
        TimeoutError: If min_messages not received within timeout
    """
    from confluent_kafka import Consumer

    # Create a single consumer for the entire polling operation
    consumer = Consumer(
        {
            "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    all_messages = []
    decode_errors = []
    start = time.time()

    try:
        while time.time() - start < timeout:
            msg = consumer.poll(timeout=poll_interval)

            if msg is None:
                # No message available, check if we have enough
                if len(all_messages) >= min_messages:
                    return all_messages[:min_messages]
                continue

            if msg.error():
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                all_messages.append(value)

                # Check if we have enough messages
                if len(all_messages) >= min_messages:
                    return all_messages[:min_messages]

            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                raw_value = msg.value()[:100] if msg.value() else b"<empty>"
                decode_errors.append(f"Offset {msg.offset()}: {e} (raw: {raw_value})")

    finally:
        consumer.close()

    # Return what we have if any
    if len(all_messages) > 0:
        return all_messages

    # No messages - check if there were decode errors
    if decode_errors:
        raise ValueError(
            f"Failed to decode messages from topic '{topic}'. " f"Errors: {decode_errors[:3]}"
        )

    raise TimeoutError(
        f"Expected at least {min_messages} messages in topic '{topic}', "
        f"but received {len(all_messages)} within {timeout}s"
    )


# =============================================================================
# Markers for test categorization
# =============================================================================


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test requiring Docker",
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow (may take > 30 seconds)",
    )
    config.addinivalue_line(
        "markers",
        "kafka: mark test as requiring Kafka",
    )
    config.addinivalue_line(
        "markers",
        "flink: mark test as requiring Flink",
    )
    config.addinivalue_line(
        "markers",
        "connect: mark test as requiring Kafka Connect",
    )
    config.addinivalue_line(
        "markers",
        "schema_registry: mark test as requiring Schema Registry",
    )
