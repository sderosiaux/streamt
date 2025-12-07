"""End-to-end tests for Schema Registry integration with Docker infrastructure.

These tests verify that Schema Registry:
- Correctly registers Avro and JSON schemas
- Validates schema compatibility (BACKWARD, FORWARD, FULL)
- Works with Kafka topics for schema evolution
- Handles schema versioning correctly
- Supports idempotent operations
"""

import json
import uuid

import pytest

from .conftest import KafkaHelper, SchemaRegistryHelper


@pytest.mark.integration
@pytest.mark.schema_registry
class TestSchemaRegistryConnection:
    """Test Schema Registry connectivity."""

    def test_check_connection(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test checking Schema Registry connection."""
        assert schema_registry_helper.check_connection() is True

    def test_check_connection_invalid(self):
        """Test connection check with invalid URL."""
        helper = SchemaRegistryHelper("http://localhost:99999")
        assert helper.check_connection() is False

    def test_list_subjects_empty_or_not(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test listing subjects on fresh/existing registry."""
        subjects = schema_registry_helper.list_subjects()
        assert isinstance(subjects, list)


@pytest.mark.integration
@pytest.mark.schema_registry
class TestSchemaRegistration:
    """Test schema registration operations."""

    def test_register_avro_schema(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test registering an Avro schema."""
        subject = f"test-avro-{uuid.uuid4().hex[:8]}-value"

        avro_schema = {
            "type": "record",
            "name": "User",
            "namespace": "com.test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
        }

        try:
            schema_id = schema_registry_helper.register_schema(
                subject,
                avro_schema,
                schema_type="AVRO",
            )

            assert isinstance(schema_id, int)
            assert schema_id > 0

            # Verify schema was registered
            subjects = schema_registry_helper.list_subjects()
            assert subject in subjects

            # Retrieve and verify schema
            retrieved = schema_registry_helper.get_schema(subject)
            assert retrieved["subject"] == subject
            assert retrieved["version"] == 1
            assert "schema" in retrieved

            # Parse the schema string back to verify content
            parsed_schema = json.loads(retrieved["schema"])
            assert parsed_schema["name"] == "User"
            assert len(parsed_schema["fields"]) == 3

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)

    def test_register_json_schema(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test registering a JSON schema."""
        subject = f"test-json-{uuid.uuid4().hex[:8]}-value"

        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "active": {"type": "boolean"},
            },
            "required": ["id", "name"],
        }

        try:
            schema_id = schema_registry_helper.register_schema(
                subject,
                json.dumps(json_schema),
                schema_type="JSON",
            )

            assert isinstance(schema_id, int)
            assert schema_id > 0

            # Verify schema
            retrieved = schema_registry_helper.get_schema(subject)
            assert retrieved["schemaType"] == "JSON"

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)

    def test_register_multiple_versions(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test registering multiple schema versions."""
        subject = f"test-versions-{uuid.uuid4().hex[:8]}-value"

        # Version 1: Basic user
        schema_v1 = {
            "type": "record",
            "name": "User",
            "namespace": "com.test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        }

        # Version 2: Add optional field (backward compatible)
        schema_v2 = {
            "type": "record",
            "name": "User",
            "namespace": "com.test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": None},
            ],
        }

        try:
            # Register v1
            id_v1 = schema_registry_helper.register_schema(subject, schema_v1)
            assert id_v1 > 0

            # Register v2
            id_v2 = schema_registry_helper.register_schema(subject, schema_v2)
            assert id_v2 > 0
            assert id_v2 != id_v1  # Different schema = different ID

            # Verify versions
            v1_schema = schema_registry_helper.get_schema(subject, version="1")
            assert v1_schema["version"] == 1

            v2_schema = schema_registry_helper.get_schema(subject, version="2")
            assert v2_schema["version"] == 2

            latest = schema_registry_helper.get_schema(subject, version="latest")
            assert latest["version"] == 2

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)

    def test_get_schema_by_id(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test retrieving schema by global ID."""
        subject = f"test-byid-{uuid.uuid4().hex[:8]}-value"

        schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "timestamp", "type": "long"}],
        }

        try:
            schema_id = schema_registry_helper.register_schema(subject, schema)

            # Retrieve by ID
            retrieved = schema_registry_helper.get_schema_by_id(schema_id)
            assert "schema" in retrieved
            parsed = json.loads(retrieved["schema"])
            assert parsed["name"] == "Event"

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)


@pytest.mark.integration
@pytest.mark.schema_registry
class TestSchemaCompatibility:
    """Test schema compatibility validation."""

    def test_backward_compatible_change(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test that adding optional field is backward compatible."""
        subject = f"test-compat-{uuid.uuid4().hex[:8]}-value"

        schema_v1 = {
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "double"},
            ],
        }

        # Backward compatible: add optional field with default
        schema_v2 = {
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "currency", "type": "string", "default": "USD"},
            ],
        }

        try:
            schema_registry_helper.register_schema(subject, schema_v1)

            # Check compatibility before registering
            is_compatible = schema_registry_helper.check_compatibility(subject, schema_v2)
            assert is_compatible is True, "Adding field with default should be backward compatible"

            # Should succeed
            schema_registry_helper.register_schema(subject, schema_v2)

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)

    def test_incompatible_change_detection(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test that removing required field is detected as incompatible.

        This test uses FULL compatibility which detects both:
        - Breaking changes for consumers reading old data (BACKWARD)
        - Breaking changes for consumers reading new data (FORWARD)

        Note: BACKWARD alone allows field removal (new readers don't need it).
        FORWARD and FULL detect field removal as incompatible (old readers expect it).
        """
        subject = f"test-incompat-{uuid.uuid4().hex[:8]}-value"

        schema_v1 = {
            "type": "record",
            "name": "Product",
            "fields": [
                {"name": "product_id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "price", "type": "double"},
            ],
        }

        # Incompatible: remove required field
        schema_v2 = {
            "type": "record",
            "name": "Product",
            "fields": [
                {"name": "product_id", "type": "string"},
                {"name": "name", "type": "string"},
                # price removed - breaking change for old consumers
            ],
        }

        try:
            schema_registry_helper.register_schema(subject, schema_v1)

            # Set FULL compatibility - detects breaking changes in both directions
            schema_registry_helper.set_compatibility_level("FULL", subject)

            # Should be incompatible (FULL catches field removal)
            is_compatible = schema_registry_helper.check_compatibility(subject, schema_v2)
            assert is_compatible is False, "Removing required field should be incompatible with FULL compatibility"

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)

    def test_change_compatibility_level(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test changing compatibility level for a subject."""
        subject = f"test-level-{uuid.uuid4().hex[:8]}-value"

        schema = {
            "type": "record",
            "name": "Config",
            "fields": [{"name": "key", "type": "string"}],
        }

        try:
            schema_registry_helper.register_schema(subject, schema)

            # Set to NONE (allow any changes)
            schema_registry_helper.set_compatibility_level("NONE", subject)
            level = schema_registry_helper.get_compatibility_level(subject)
            assert level == "NONE"

            # Set to FULL
            schema_registry_helper.set_compatibility_level("FULL", subject)
            level = schema_registry_helper.get_compatibility_level(subject)
            assert level == "FULL"

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)


@pytest.mark.integration
@pytest.mark.schema_registry
class TestSchemaSubjectDeletion:
    """Test schema subject deletion."""

    def test_soft_delete_subject(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test soft deleting a subject."""
        subject = f"test-softdel-{uuid.uuid4().hex[:8]}-value"

        schema = {
            "type": "record",
            "name": "Temporary",
            "fields": [{"name": "data", "type": "string"}],
        }

        # Register then soft delete
        schema_registry_helper.register_schema(subject, schema)

        versions = schema_registry_helper.delete_subject(subject, permanent=False)
        assert 1 in versions

        # Subject should not appear in list after soft delete
        subjects = schema_registry_helper.list_subjects()
        assert subject not in subjects

    def test_permanent_delete_subject(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test permanently deleting a subject."""
        subject = f"test-permdel-{uuid.uuid4().hex[:8]}-value"

        schema = {
            "type": "record",
            "name": "ToDelete",
            "fields": [{"name": "id", "type": "int"}],
        }

        # Register
        schema_registry_helper.register_schema(subject, schema)

        # Soft delete first (required before permanent)
        schema_registry_helper.delete_subject(subject, permanent=False)

        # Permanent delete
        schema_registry_helper.delete_subject(subject, permanent=True)

        # Should not exist
        subjects = schema_registry_helper.list_subjects()
        assert subject not in subjects


@pytest.mark.integration
@pytest.mark.schema_registry
@pytest.mark.kafka
class TestSchemaRegistryWithKafka:
    """Test Schema Registry integration with Kafka topics."""

    def test_register_topic_schema(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test registering schemas for Kafka topic value and key."""
        topic_name = f"schema-test-{uuid.uuid4().hex[:8]}"
        value_subject = f"{topic_name}-value"
        key_subject = f"{topic_name}-key"

        value_schema = {
            "type": "record",
            "name": "OrderEvent",
            "namespace": "com.orders",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "customer_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "created_at", "type": "long"},
            ],
        }

        key_schema = {
            "type": "record",
            "name": "OrderKey",
            "namespace": "com.orders",
            "fields": [
                {"name": "order_id", "type": "string"},
            ],
        }

        try:
            # Create topic
            kafka_helper.create_topic(topic_name, partitions=3)

            # Register schemas following topic naming convention
            value_id = schema_registry_helper.register_schema(value_subject, value_schema)
            key_id = schema_registry_helper.register_schema(key_subject, key_schema)

            assert value_id > 0
            assert key_id > 0

            # Verify both subjects exist
            subjects = schema_registry_helper.list_subjects()
            assert value_subject in subjects
            assert key_subject in subjects

        finally:
            schema_registry_helper.delete_subject(value_subject, permanent=True)
            schema_registry_helper.delete_subject(key_subject, permanent=True)
            kafka_helper.delete_topic(topic_name)

    def test_schema_evolution_with_topic(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test schema evolution for a Kafka topic."""
        topic_name = f"evolution-test-{uuid.uuid4().hex[:8]}"
        subject = f"{topic_name}-value"

        # V1: Basic event
        schema_v1 = {
            "type": "record",
            "name": "PageView",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "page", "type": "string"},
                {"name": "timestamp", "type": "long"},
            ],
        }

        # V2: Add optional session_id
        schema_v2 = {
            "type": "record",
            "name": "PageView",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "page", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "session_id", "type": ["null", "string"], "default": None},
            ],
        }

        # V3: Add optional duration
        schema_v3 = {
            "type": "record",
            "name": "PageView",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "page", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "session_id", "type": ["null", "string"], "default": None},
                {"name": "duration_ms", "type": ["null", "int"], "default": None},
            ],
        }

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            # Register all versions
            id_v1 = schema_registry_helper.register_schema(subject, schema_v1)
            id_v2 = schema_registry_helper.register_schema(subject, schema_v2)
            id_v3 = schema_registry_helper.register_schema(subject, schema_v3)

            # All should succeed and have different IDs
            assert id_v1 != id_v2 != id_v3

            # Verify version count
            latest = schema_registry_helper.get_schema(subject, "latest")
            assert latest["version"] == 3

            # Verify we can retrieve each version
            for version in [1, 2, 3]:
                versioned = schema_registry_helper.get_schema(subject, str(version))
                assert versioned["version"] == version

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.schema_registry
class TestSchemaRegistryIdempotency:
    """Test idempotent schema operations."""

    def test_register_same_schema_twice(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test that registering identical schema returns same ID."""
        subject = f"test-idempotent-{uuid.uuid4().hex[:8]}-value"

        schema = {
            "type": "record",
            "name": "Duplicate",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "string"},
            ],
        }

        try:
            # Register twice
            id_first = schema_registry_helper.register_schema(subject, schema)
            id_second = schema_registry_helper.register_schema(subject, schema)

            # Same schema should return same ID (idempotent)
            assert id_first == id_second

            # Should still be version 1
            retrieved = schema_registry_helper.get_schema(subject, "latest")
            assert retrieved["version"] == 1

        finally:
            schema_registry_helper.delete_subject(subject, permanent=True)

    def test_delete_nonexistent_subject(
        self,
        docker_services,
        schema_registry_helper: SchemaRegistryHelper,
    ):
        """Test deleting non-existent subject returns empty list."""
        result = schema_registry_helper.delete_subject(
            f"nonexistent-{uuid.uuid4().hex}",
            permanent=False,
        )
        assert result == []
