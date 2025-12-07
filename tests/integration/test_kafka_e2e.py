"""End-to-end tests for Kafka topic management with Docker infrastructure.

These tests verify that the KafkaDeployer correctly:
- Creates topics with proper configuration
- Updates topics (partitions, configs)
- Deletes topics
- Handles plan/apply lifecycle
- Produces and consumes messages with JSON serialization
"""

import time
import uuid

import pytest
from confluent_kafka.admin import AdminClient

from streamt.compiler.manifest import TopicArtifact
from streamt.deployer.kafka import KafkaDeployer

from .conftest import INFRA_CONFIG, KafkaHelper


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaTopicCreation:
    """Test topic creation via KafkaDeployer."""

    def test_create_simple_topic(
        self,
        docker_services,
        kafka_admin: AdminClient,
    ):
        """Test creating a simple topic with default settings."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_simple_{uuid.uuid4().hex[:8]}"

        try:
            artifact = TopicArtifact(
                name=topic_name,
                partitions=1,
                replication_factor=1,
            )

            # Plan should show create action
            plan = deployer.plan_topic(artifact)
            assert plan.action == "create"
            assert not plan.current.exists

            # Apply should create the topic
            result = deployer.apply_topic(artifact)
            assert result == "created"

            # Verify topic exists
            state = deployer.get_topic_state(topic_name)
            assert state.exists
            assert state.partitions == 1
            assert state.replication_factor == 1

        finally:
            # Cleanup
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_create_topic_with_partitions(
        self,
        docker_services,
        kafka_admin: AdminClient,
    ):
        """Test creating a topic with multiple partitions."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_partitions_{uuid.uuid4().hex[:8]}"

        try:
            artifact = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
            )

            result = deployer.apply_topic(artifact)
            assert result == "created"

            state = deployer.get_topic_state(topic_name)
            assert state.partitions == 6

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_create_topic_with_config(
        self,
        docker_services,
        kafka_admin: AdminClient,
    ):
        """Test creating a topic with custom configuration."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_config_{uuid.uuid4().hex[:8]}"

        try:
            artifact = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
                config={
                    "retention.ms": "604800000",  # 7 days
                    "cleanup.policy": "delete",
                },
            )

            result = deployer.apply_topic(artifact)
            assert result == "created"

            state = deployer.get_topic_state(topic_name)
            assert state.exists
            assert state.config.get("retention.ms") == "604800000"
            assert state.config.get("cleanup.policy") == "delete"

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_create_compacted_topic(
        self,
        docker_services,
    ):
        """Test creating a compacted topic."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_compacted_{uuid.uuid4().hex[:8]}"

        try:
            artifact = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
                config={
                    "cleanup.policy": "compact",
                    "min.compaction.lag.ms": "0",
                },
            )

            result = deployer.apply_topic(artifact)
            assert result == "created"

            state = deployer.get_topic_state(topic_name)
            assert state.config.get("cleanup.policy") == "compact"

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaTopicUpdates:
    """Test topic updates via KafkaDeployer."""

    def test_increase_partitions(
        self,
        docker_services,
    ):
        """Test increasing the number of partitions."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_increase_part_{uuid.uuid4().hex[:8]}"

        try:
            # Create initial topic
            initial = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
            )
            deployer.apply_topic(initial)

            # Update with more partitions
            updated = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
            )

            plan = deployer.plan_topic(updated)
            assert plan.action == "update"
            assert "partitions" in plan.changes
            assert plan.changes["partitions"]["from"] == 3
            assert plan.changes["partitions"]["to"] == 6

            result = deployer.apply_topic(updated)
            assert result == "updated"

            state = deployer.get_topic_state(topic_name)
            assert state.partitions == 6

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_cannot_decrease_partitions(
        self,
        docker_services,
    ):
        """Test that decreasing partitions is not allowed."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_decrease_part_{uuid.uuid4().hex[:8]}"

        try:
            # Create initial topic with 6 partitions
            initial = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
            )
            deployer.apply_topic(initial)

            # Try to decrease partitions
            updated = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
            )

            plan = deployer.plan_topic(updated)
            assert "partitions_error" in plan.changes

            with pytest.raises(RuntimeError) as exc_info:
                deployer.apply_topic(updated)

            assert "Cannot reduce partitions" in str(exc_info.value)

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_update_config(
        self,
        docker_services,
    ):
        """Test updating topic configuration."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_update_config_{uuid.uuid4().hex[:8]}"

        try:
            # Create initial topic
            initial = TopicArtifact(
                name=topic_name,
                partitions=1,
                replication_factor=1,
                config={"retention.ms": "86400000"},  # 1 day
            )
            deployer.apply_topic(initial)

            # Update retention
            updated = TopicArtifact(
                name=topic_name,
                partitions=1,
                replication_factor=1,
                config={"retention.ms": "604800000"},  # 7 days
            )

            plan = deployer.plan_topic(updated)
            assert plan.action == "update"
            assert "config.retention.ms" in plan.changes

            result = deployer.apply_topic(updated)
            assert result == "updated"

            state = deployer.get_topic_state(topic_name)
            assert state.config.get("retention.ms") == "604800000"

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaTopicDeletion:
    """Test topic deletion via KafkaDeployer."""

    def test_delete_existing_topic(
        self,
        docker_services,
    ):
        """Test deleting an existing topic."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_delete_{uuid.uuid4().hex[:8]}"

        # Create the topic
        artifact = TopicArtifact(
            name=topic_name,
            partitions=1,
            replication_factor=1,
        )
        deployer.apply_topic(artifact)

        # Verify it exists
        state = deployer.get_topic_state(topic_name)
        assert state.exists

        # Delete it
        deployer.delete_topic(topic_name)

        # Wait for deletion to propagate
        time.sleep(1)

        # Verify it's gone
        state = deployer.get_topic_state(topic_name)
        assert not state.exists

    def test_delete_nonexistent_topic(
        self,
        docker_services,
    ):
        """Test deleting a non-existent topic raises error."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"nonexistent_{uuid.uuid4().hex[:8]}"

        with pytest.raises(RuntimeError):
            deployer.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaIdempotency:
    """Test idempotent operations."""

    def test_apply_same_topic_twice(
        self,
        docker_services,
    ):
        """Test that applying the same topic twice is idempotent."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_idempotent_{uuid.uuid4().hex[:8]}"

        try:
            artifact = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            )

            # First apply
            result1 = deployer.apply_topic(artifact)
            assert result1 == "created"

            # Second apply - should be no-op
            result2 = deployer.apply_topic(artifact)
            assert result2 == "unchanged"

            # Verify state
            state = deployer.get_topic_state(topic_name)
            assert state.partitions == 3

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaDataFlow:
    """Test actual data flow through topics."""

    def test_produce_consume_json(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test producing and consuming JSON messages."""
        topic_name = f"test_data_json_{uuid.uuid4().hex[:8]}"

        try:
            # Create topic
            kafka_helper.create_topic(topic_name, partitions=3)

            # Produce messages
            messages = [
                {"id": 1, "name": "Alice", "score": 100},
                {"id": 2, "name": "Bob", "score": 85},
                {"id": 3, "name": "Charlie", "score": 92},
            ]
            kafka_helper.produce_messages(topic_name, messages, key_field="id")

            # Consume messages
            consumed = kafka_helper.consume_messages(
                topic_name,
                group_id=f"test_consumer_{uuid.uuid4().hex[:8]}",
                max_messages=3,
                timeout=30.0,
            )

            assert len(consumed) == 3

            # Verify content (order may vary due to partitioning)
            consumed_ids = {msg["id"] for msg in consumed}
            assert consumed_ids == {1, 2, 3}

        finally:
            kafka_helper.delete_topic(topic_name)

    def test_partitioning_by_key(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test that messages with same key go to same partition."""
        topic_name = f"test_partitioning_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=6)

            # Produce messages with same key multiple times
            messages = []
            for i in range(10):
                messages.append({"key": "same_key", "seq": i})

            kafka_helper.produce_messages(topic_name, messages, key_field="key")

            # Consume and verify order is preserved for same key
            consumed = kafka_helper.consume_messages(
                topic_name,
                group_id=f"test_consumer_{uuid.uuid4().hex[:8]}",
                max_messages=10,
                timeout=30.0,
            )

            assert len(consumed) == 10

            # All messages with same key should be in order
            sequences = [msg["seq"] for msg in consumed]
            # Within a partition, order should be preserved
            # Since all have same key, they go to same partition
            assert sequences == sorted(sequences)

        finally:
            kafka_helper.delete_topic(topic_name)

    def test_multiple_partitions_parallel_consumption(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test consuming from multiple partitions."""
        topic_name = f"test_parallel_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=3)

            # Produce messages with different keys to distribute across partitions
            messages = [{"key": f"key_{i}", "value": i} for i in range(30)]
            kafka_helper.produce_messages(topic_name, messages, key_field="key")

            # Consume all messages
            consumed = kafka_helper.consume_messages(
                topic_name,
                group_id=f"test_consumer_{uuid.uuid4().hex[:8]}",
                max_messages=30,
                timeout=60.0,
            )

            assert len(consumed) == 30

            # Verify all values are present
            values = {msg["value"] for msg in consumed}
            assert values == set(range(30))

        finally:
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaDeployerListing:
    """Test listing operations."""

    def test_list_topics(
        self,
        docker_services,
    ):
        """Test listing all topics."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_names = [f"test_list_{i}_{uuid.uuid4().hex[:8]}" for i in range(3)]

        try:
            # Create test topics
            for name in topic_names:
                artifact = TopicArtifact(
                    name=name,
                    partitions=1,
                    replication_factor=1,
                )
                deployer.apply_topic(artifact)

            # List topics
            all_topics = deployer.list_topics()

            # Our topics should be in the list
            for name in topic_names:
                assert name in all_topics

            # Internal topics should not be in the list
            assert "_consumer_offsets" not in all_topics

        finally:
            for name in topic_names:
                try:
                    deployer.delete_topic(name)
                except Exception:
                    pass


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaDiff:
    """Test diff/plan functionality."""

    def test_compute_diff_new_topic(
        self,
        docker_services,
    ):
        """Test computing diff for a new topic."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_diff_new_{uuid.uuid4().hex[:8]}"

        artifact = TopicArtifact(
            name=topic_name,
            partitions=3,
            replication_factor=1,
        )

        # Diff for non-existent topic should be empty (no changes needed)
        diff = deployer.compute_diff(artifact)
        # For new topics, there's no diff, just creation
        assert diff == {}  # No changes dict for new topics

    def test_compute_diff_existing_topic(
        self,
        docker_services,
    ):
        """Test computing diff for an existing topic."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"test_diff_existing_{uuid.uuid4().hex[:8]}"

        try:
            # Create initial topic
            initial = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            )
            deployer.apply_topic(initial)

            # Compute diff with changes
            updated = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "604800000"},
            )

            diff = deployer.compute_diff(updated)

            assert "partitions" in diff
            assert diff["partitions"]["from"] == 3
            assert diff["partitions"]["to"] == 6

            assert "config.retention.ms" in diff
            assert diff["config.retention.ms"]["from"] == "86400000"
            assert diff["config.retention.ms"]["to"] == "604800000"

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass
