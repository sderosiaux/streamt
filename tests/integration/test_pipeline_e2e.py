"""End-to-end pipeline tests combining Kafka, Flink, and Connect.

These tests verify complete streaming pipelines:
- Multi-stage transformations
- Kafka -> Flink -> Kafka flows
- Connect -> Kafka -> Flink -> Kafka flows
- Full streamt project deployment
"""

import time
import uuid

import pytest

from streamt.compiler.manifest import (
    Manifest,
    TopicArtifact,
)
from streamt.deployer.kafka import KafkaDeployer

from .conftest import INFRA_CONFIG, ConnectHelper, FlinkHelper, KafkaHelper, poll_until_messages


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
@pytest.mark.slow
class TestMultiStageKafkaFlinkPipeline:
    """Test multi-stage Kafka-Flink pipelines."""

    def test_two_stage_transformation(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test a two-stage transformation pipeline.

        Stage 1: Filter high-value orders
        Stage 2: Enrich with category totals
        """
        prefix = uuid.uuid4().hex[:8]
        source_topic = f"orders_{prefix}"
        stage1_topic = f"high_value_orders_{prefix}"
        stage2_topic = f"enriched_orders_{prefix}"

        source_table = f"orders_src_{prefix}"
        stage1_table = f"high_value_tbl_{prefix}"
        stage2_table = f"enriched_tbl_{prefix}"

        try:
            # Create all topics
            kafka_helper.create_topic(source_topic, partitions=3)
            kafka_helper.create_topic(stage1_topic, partitions=3)
            kafka_helper.create_topic(stage2_topic, partitions=3)

            # Produce test orders
            orders = [
                {"order_id": 1, "category": "electronics", "amount": 150.0},
                {"order_id": 2, "category": "books", "amount": 25.0},
                {"order_id": 3, "category": "electronics", "amount": 500.0},
                {"order_id": 4, "category": "clothing", "amount": 80.0},
                {"order_id": 5, "category": "electronics", "amount": 200.0},
                {"order_id": 6, "category": "books", "amount": 15.0},
            ]
            kafka_helper.produce_messages(source_topic, orders)

            # Setup Flink
            flink_helper.open_sql_session()

            # Source table
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {source_table} (
                order_id INT,
                category STRING,
                amount DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'stage1_group_{prefix}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Stage 1 output table (high value orders)
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {stage1_table} (
                order_id INT,
                category STRING,
                amount DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{stage1_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Stage 2 output table
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {stage2_table} (
                order_id INT,
                category STRING,
                amount DOUBLE,
                is_premium BOOLEAN
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{stage2_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Stage 1: Filter high value orders (amount >= 100)
            flink_helper.execute_sql(
                f"""
            INSERT INTO {stage1_table}
            SELECT order_id, category, amount
            FROM {source_table}
            WHERE amount >= 100
            """
            )

            # Create a source table for stage 2 reading from stage 1 output
            stage1_source = f"stage1_source_{prefix}"
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {stage1_source} (
                order_id INT,
                category STRING,
                amount DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{stage1_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'stage2_group_{prefix}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Stage 2: Mark as premium if amount >= 200
            flink_helper.execute_sql(
                f"""
            INSERT INTO {stage2_table}
            SELECT
                order_id,
                category,
                amount,
                amount >= 200 as is_premium
            FROM {stage1_source}
            """
            )

            # Wait for job to process data and poll for stage 1 results
            time.sleep(5)  # Give job time to start

            stage1_results = poll_until_messages(
                kafka_helper,
                stage1_topic,
                min_messages=1,
                group_id=f"verify_stage1_{prefix}",
                timeout=60.0,  # Increased timeout for job startup
                poll_interval=2.0,
            )

            # Stage 1: Verify high-value filtering worked
            assert len(stage1_results) > 0, (
                f"Expected filtered results in stage1 topic '{stage1_topic}', "
                "but received none. Stage 1 pipeline may have failed."
            )
            stage1_ids = {r["order_id"] for r in stage1_results}
            # Only high-value orders (amount >= 100) should pass through
            assert stage1_ids.issubset(
                {1, 3, 5}
            ), f"Stage 1 should only contain high-value order ids {{1, 3, 5}}, got {stage1_ids}"
            for r in stage1_results:
                assert (
                    r["amount"] >= 100
                ), f"Stage 1 filter failed: order {r['order_id']} has amount {r['amount']} < 100"

            # Verify stage 2 output
            stage2_results = kafka_helper.consume_messages(
                stage2_topic,
                group_id=f"verify_stage2_{prefix}",
                max_messages=10,
                timeout=30.0,
            )

            # Stage 2: Verify premium flag enrichment
            assert len(stage2_results) > 0, (
                f"Expected enriched results in stage2 topic '{stage2_topic}', "
                "but received none. Stage 2 pipeline may have failed."
            )
            for r in stage2_results:
                assert "is_premium" in r, f"Missing 'is_premium' field in stage 2 result: {r}"
                if r["amount"] >= 200:
                    assert (
                        r["is_premium"] is True
                    ), f"Order {r['order_id']} with amount {r['amount']} should be premium"
                else:
                    assert (
                        r["is_premium"] is False
                    ), f"Order {r['order_id']} with amount {r['amount']} should not be premium"

        finally:
            # Cleanup
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {stage2_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {stage1_source}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {stage1_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(stage1_topic)
            kafka_helper.delete_topic(stage2_topic)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
@pytest.mark.slow
class TestStreamingJoinPipeline:
    """Test streaming join pipelines."""

    def test_stream_to_stream_join(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test joining two Kafka streams in Flink."""
        prefix = uuid.uuid4().hex[:8]
        orders_topic = f"join_orders_{prefix}"
        customers_topic = f"join_customers_{prefix}"
        output_topic = f"join_output_{prefix}"

        orders_table = f"join_orders_tbl_{prefix}"
        customers_table = f"join_customers_tbl_{prefix}"
        output_table = f"join_output_tbl_{prefix}"

        try:
            # Create topics
            kafka_helper.create_topic(orders_topic, partitions=1)
            kafka_helper.create_topic(customers_topic, partitions=1)
            kafka_helper.create_topic(output_topic, partitions=1)

            # Produce customers
            customers = [
                {"customer_id": "c1", "name": "Alice", "tier": "gold"},
                {"customer_id": "c2", "name": "Bob", "tier": "silver"},
                {"customer_id": "c3", "name": "Charlie", "tier": "bronze"},
            ]
            kafka_helper.produce_messages(customers_topic, customers, key_field="customer_id")

            # Produce orders
            orders = [
                {"order_id": "o1", "customer_id": "c1", "amount": 100.0},
                {"order_id": "o2", "customer_id": "c2", "amount": 50.0},
                {"order_id": "o3", "customer_id": "c1", "amount": 200.0},
            ]
            kafka_helper.produce_messages(orders_topic, orders, key_field="order_id")

            # Setup Flink
            flink_helper.open_sql_session()

            # Orders source
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {orders_table} (
                order_id STRING,
                customer_id STRING,
                amount DOUBLE,
                proc_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{orders_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'join_orders_{prefix}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Customers dimension table (compacted)
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {customers_table} (
                customer_id STRING,
                name STRING,
                tier STRING,
                proc_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{customers_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'join_customers_{prefix}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Output sink
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {output_table} (
                order_id STRING,
                customer_name STRING,
                customer_tier STRING,
                amount DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{output_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Join query using temporal join pattern
            flink_helper.execute_sql(
                f"""
            INSERT INTO {output_table}
            SELECT
                o.order_id,
                c.name as customer_name,
                c.tier as customer_tier,
                o.amount
            FROM {orders_table} o
            JOIN {customers_table} c
            ON o.customer_id = c.customer_id
            """
            )

            # Wait for job to process data and poll for joined results
            time.sleep(5)  # Give job time to start

            results = poll_until_messages(
                kafka_helper,
                output_topic,
                min_messages=1,
                group_id=f"verify_join_{prefix}",
                timeout=60.0,  # Increased timeout for job startup
                poll_interval=2.0,
            )

            # Verify join produced results
            assert len(results) > 0, (
                f"Expected joined results in output topic '{output_topic}', "
                "but received none. Stream join may have failed."
            )

            # Validate all joined records have required fields
            for r in results:
                assert "order_id" in r, f"Missing 'order_id' in joined result: {r}"
                assert "customer_name" in r, f"Missing 'customer_name' in joined result: {r}"
                assert "customer_tier" in r, f"Missing 'customer_tier' in joined result: {r}"
                assert "amount" in r, f"Missing 'amount' in joined result: {r}"
                # Verify the join enriched with correct customer data
                assert r["customer_tier"] in {
                    "gold",
                    "silver",
                    "bronze",
                }, f"Invalid customer tier '{r['customer_tier']}' in joined result"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {output_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {customers_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {orders_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(orders_topic)
            kafka_helper.delete_topic(customers_topic)
            kafka_helper.delete_topic(output_topic)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
@pytest.mark.slow
class TestWindowedAggregationPipeline:
    """Test windowed aggregation pipelines."""

    def test_tumbling_window_aggregation(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        flink_helper: FlinkHelper,
    ):
        """Test tumbling window aggregation."""
        prefix = uuid.uuid4().hex[:8]
        source_topic = f"window_source_{prefix}"
        sink_topic = f"window_sink_{prefix}"

        source_table = f"window_src_{prefix}"
        sink_table = f"window_sink_{prefix}"

        try:
            kafka_helper.create_topic(source_topic, partitions=1)
            kafka_helper.create_topic(sink_topic, partitions=1)

            flink_helper.open_sql_session()

            # Source with event time
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {source_table} (
                user_id STRING,
                page STRING,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'window_group_{prefix}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
            """
            )

            # Sink for aggregations
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {sink_table} (
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                user_id STRING,
                page_views BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{sink_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Windowed aggregation with 10-second window (faster for testing)
            flink_helper.execute_sql(
                f"""
            INSERT INTO {sink_table}
            SELECT
                TUMBLE_START(event_time, INTERVAL '10' SECOND) as window_start,
                TUMBLE_END(event_time, INTERVAL '10' SECOND) as window_end,
                user_id,
                COUNT(*) as page_views
            FROM {source_table}
            GROUP BY
                TUMBLE(event_time, INTERVAL '10' SECOND),
                user_id
            """
            )

            # Produce events in a completed window (30 seconds ago)
            # This ensures the window has already closed
            window_time = int(time.time() * 1000) - 30000  # 30 seconds ago
            events = [
                {"user_id": "u1", "page": "/home", "event_time": window_time},
                {"user_id": "u1", "page": "/products", "event_time": window_time + 1000},
                {"user_id": "u2", "page": "/home", "event_time": window_time + 2000},
                {"user_id": "u1", "page": "/cart", "event_time": window_time + 3000},
            ]

            # Convert timestamps to Flink SQL format (space separator, not T)
            # Flink's JSON format expects: yyyy-MM-dd HH:mm:ss.SSS
            for event in events:
                ts = event["event_time"]
                event["event_time"] = time.strftime(
                    "%Y-%m-%d %H:%M:%S.000",
                    time.gmtime(ts / 1000),
                )

            kafka_helper.produce_messages(source_topic, events)

            # Send a "future" event to advance the watermark past the window boundary
            # This triggers window emission without waiting for real time to pass
            # Use a timestamp far enough in the future to close all past windows
            future_time = int(time.time() * 1000) + 60000  # 60 seconds in future
            watermark_event = {
                "user_id": "watermark_trigger",
                "page": "/trigger",
                "event_time": time.strftime(
                    "%Y-%m-%d %H:%M:%S.000",
                    time.gmtime(future_time / 1000),
                ),
            }
            kafka_helper.produce_messages(source_topic, [watermark_event])

            # Wait for window to close and poll for results
            # Tumbling windows need time for watermark to advance
            time.sleep(10)  # Give extra time for window aggregation

            results = poll_until_messages(
                kafka_helper,
                sink_topic,
                min_messages=1,
                group_id=f"verify_window_{prefix}",
                timeout=60.0,  # Increased timeout for windowed aggregation
                poll_interval=2.0,
            )

            # CRITICAL: Windowed aggregation test must produce results
            # If no results, the pipeline failed silently
            assert len(results) > 0, (
                f"Expected windowed aggregation results in sink topic '{sink_topic}', "
                "but received none. The tumbling window aggregation pipeline may have failed. "
                "This could be due to watermark not advancing or window not closing properly."
            )

            # Filter out the watermark trigger event's window (if it appears)
            actual_results = [r for r in results if r.get("user_id") != "watermark_trigger"]

            # Verify we got results from actual test users
            assert len(actual_results) > 0, (
                "Got window results but none from test users (u1, u2). " f"All results: {results}"
            )

            # Verify aggregation result structure
            for r in actual_results:
                assert "window_start" in r, f"Missing 'window_start' in aggregation result: {r}"
                assert "window_end" in r, f"Missing 'window_end' in aggregation result: {r}"
                assert "user_id" in r, f"Missing 'user_id' in aggregation result: {r}"
                assert "page_views" in r, f"Missing 'page_views' count in aggregation result: {r}"
                # Verify page_views is a positive integer (valid aggregation)
                assert isinstance(
                    r["page_views"], int
                ), f"Expected 'page_views' to be int, got {type(r['page_views']).__name__}"
                assert (
                    r["page_views"] > 0
                ), f"Expected positive page_views count, got {r['page_views']}"
                # Verify user_id is one of the expected values
                assert r["user_id"] in [
                    "u1",
                    "u2",
                ], f"Unexpected user_id '{r['user_id']}', expected 'u1' or 'u2'"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {sink_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.connect
@pytest.mark.flink
@pytest.mark.slow
class TestFullStackPipeline:
    """Test complete pipelines using all components."""

    def test_connect_kafka_flink_pipeline(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        connect_helper: ConnectHelper,
        flink_helper: FlinkHelper,
    ):
        """Test: Datagen -> Kafka -> Flink Transform -> Kafka.

        This tests a realistic pipeline where:
        1. Kafka Connect generates source data
        2. Flink reads, transforms, and writes back to Kafka
        """
        prefix = uuid.uuid4().hex[:8]
        source_topic = f"fullstack_source_{prefix}"
        sink_topic = f"fullstack_sink_{prefix}"
        connector_name = f"fullstack_datagen_{prefix}"

        source_table = f"fullstack_src_{prefix}"
        sink_table = f"fullstack_sink_{prefix}"

        try:
            # Create topics
            kafka_helper.create_topic(source_topic, partitions=1)
            kafka_helper.create_topic(sink_topic, partitions=1)

            # Start datagen connector
            config = {
                "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                "kafka.topic": source_topic,
                "quickstart": "users",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "max.interval": "500",
                "iterations": "100",
                "tasks.max": "1",
            }

            connect_helper.create_connector(connector_name, config)
            connect_helper.wait_for_connector_running(connector_name, timeout=30)

            # Wait for some data to be generated
            time.sleep(5)

            # Setup Flink transformation
            flink_helper.open_sql_session()

            # Create source table reading from datagen topic
            # Note: datagen users quickstart produces JSON with userid, regionid, gender, etc.
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {source_table} (
                userid STRING,
                regionid STRING,
                gender STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'properties.group.id' = 'fullstack_group_{prefix}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true'
            )
            """
            )

            # Sink table
            flink_helper.execute_sql(
                f"""
            CREATE TABLE {sink_table} (
                userid STRING,
                region_upper STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{sink_topic}',
                'properties.bootstrap.servers' = '{INFRA_CONFIG.kafka_internal_servers}',
                'format' = 'json'
            )
            """
            )

            # Transform: extract userid and uppercase region
            flink_helper.execute_sql(
                f"""
            INSERT INTO {sink_table}
            SELECT userid, UPPER(regionid) as region_upper
            FROM {source_table}
            """
            )

            # Wait for full-stack pipeline processing (Connect -> Kafka -> Flink -> Kafka)
            # This needs extra time as data flows through multiple systems
            time.sleep(15)

            # Poll for transformed results
            results = poll_until_messages(
                kafka_helper,
                sink_topic,
                min_messages=1,
                group_id=f"fullstack_verify_{prefix}",
                timeout=90.0,  # Longer timeout for multi-system pipeline
                poll_interval=2.0,
            )

            # Verify full-stack pipeline produced results
            assert len(results) > 0, (
                f"Expected transformed results in sink topic '{sink_topic}', "
                "but received none. Full-stack pipeline (Connect -> Kafka -> Flink -> Kafka) failed."
            )

            # Validate all transformed records
            for r in results:
                assert "userid" in r, f"Missing 'userid' in transformed result: {r}"
                assert "region_upper" in r, f"Missing 'region_upper' in transformed result: {r}"
                # Verify region was transformed to uppercase
                if r["region_upper"] is not None:
                    assert (
                        r["region_upper"] == r["region_upper"].upper()
                    ), f"Expected uppercase region, got '{r['region_upper']}'"

        finally:
            try:
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {sink_table}")
                flink_helper.execute_sql(f"DROP TABLE IF EXISTS {source_table}")
            except Exception:
                pass
            try:
                connect_helper.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(source_topic)
            kafka_helper.delete_topic(sink_topic)


@pytest.mark.integration
@pytest.mark.kafka
class TestDeployersIntegration:
    """Test using multiple deployers together."""

    def test_deploy_topic_artifacts(
        self,
        docker_services,
    ):
        """Test deploying multiple topic artifacts via KafkaDeployer."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        prefix = uuid.uuid4().hex[:8]

        topics = [
            TopicArtifact(
                name=f"orders_{prefix}",
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            ),
            TopicArtifact(
                name=f"order_items_{prefix}",
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            ),
            TopicArtifact(
                name=f"order_totals_{prefix}",
                partitions=3,
                replication_factor=1,
                config={"cleanup.policy": "compact"},
            ),
        ]

        try:
            # Deploy all topics
            results = []
            for topic in topics:
                result = deployer.apply_topic(topic)
                results.append(result)

            # All should be created
            assert all(r == "created" for r in results)

            # Verify all topics exist with correct config
            for topic in topics:
                state = deployer.get_topic_state(topic.name)
                assert state.exists
                assert state.partitions == topic.partitions

            # Re-apply should be idempotent
            results = []
            for topic in topics:
                result = deployer.apply_topic(topic)
                results.append(result)

            assert all(r == "unchanged" for r in results)

        finally:
            for topic in topics:
                try:
                    deployer.delete_topic(topic.name)
                except Exception:
                    pass

    def test_manifest_deployment(
        self,
        docker_services,
    ):
        """Test deploying a complete manifest."""
        prefix = uuid.uuid4().hex[:8]

        # Create a manifest with multiple artifacts
        manifest = Manifest(
            version="1.0.0",
            project_name=f"test_project_{prefix}",
            sources=[],
            models=[],
            tests=[],
            exposures=[],
            dag={},
            artifacts={
                "topics": [
                    {
                        "name": f"source_events_{prefix}",
                        "partitions": 6,
                        "replication_factor": 1,
                        "config": {},
                    },
                    {
                        "name": f"processed_events_{prefix}",
                        "partitions": 6,
                        "replication_factor": 1,
                        "config": {},
                    },
                ],
            },
        )

        kafka_deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)

        try:
            # Deploy all topics from manifest
            topic_artifacts = manifest.artifacts.get("topics", [])
            for topic_dict in topic_artifacts:
                artifact = TopicArtifact(
                    name=topic_dict["name"],
                    partitions=topic_dict["partitions"],
                    replication_factor=topic_dict["replication_factor"],
                    config=topic_dict.get("config", {}),
                )
                result = kafka_deployer.apply_topic(artifact)
                assert result == "created"

            # Verify deployment
            for topic_dict in topic_artifacts:
                state = kafka_deployer.get_topic_state(topic_dict["name"])
                assert state.exists

        finally:
            for topic_dict in manifest.artifacts.get("topics", []):
                try:
                    kafka_deployer.delete_topic(topic_dict["name"])
                except Exception:
                    pass


@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.flink
class TestDeployerPlanningMode:
    """Test planning capabilities across deployers."""

    def test_plan_mode_topics(
        self,
        docker_services,
    ):
        """Test planning topic changes without applying."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"plan_test_{uuid.uuid4().hex[:8]}"

        try:
            # Plan for non-existent topic
            artifact = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
            )

            plan = deployer.plan_topic(artifact)
            assert plan.action == "create"

            # Create the topic
            deployer.apply_topic(artifact)

            # Plan for update
            updated = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            )

            plan = deployer.plan_topic(updated)
            assert plan.action == "update"
            assert "partitions" in plan.changes
            assert "config.retention.ms" in plan.changes

            # Plan with no changes
            plan = deployer.plan_topic(artifact)
            # After update, current state has 6 partitions, but artifact has 3
            # This will show a partition error (can't decrease)
            # Let's test with the updated artifact instead
            same = TopicArtifact(
                name=topic_name,
                partitions=6,
                replication_factor=1,
                config={"retention.ms": "86400000"},
            )

            # First apply the update
            deployer.apply_topic(updated)

            # Now plan with same config
            plan = deployer.plan_topic(same)
            assert plan.action == "none"

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_compute_diff(
        self,
        docker_services,
    ):
        """Test computing diffs between current and desired state."""
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)
        topic_name = f"diff_test_{uuid.uuid4().hex[:8]}"

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
                config={
                    "retention.ms": "604800000",
                    "cleanup.policy": "compact",
                },
            )

            diff = deployer.compute_diff(updated)

            assert "partitions" in diff
            assert "config.retention.ms" in diff
            assert "config.cleanup.policy" in diff

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass
