"""Test runner for streamt tests."""

from __future__ import annotations

from typing import Any

from streamt.core.models import StreamtProject, Test, TestType


class TestRunner:
    """Runner for streamt tests."""

    def __init__(self, project: StreamtProject) -> None:
        """Initialize test runner."""
        self.project = project

    def run(self, tests: list[Test]) -> list[dict[str, Any]]:
        """Run a list of tests."""
        results = []

        for test in tests:
            if test.type == TestType.SCHEMA:
                result = self._run_schema_test(test)
            elif test.type == TestType.SAMPLE:
                result = self._run_sample_test(test)
            elif test.type == TestType.CONTINUOUS:
                result = self._run_continuous_test(test)
            else:
                result = {
                    "name": test.name,
                    "status": "skipped",
                    "errors": [f"Unknown test type: {test.type}"],
                }

            results.append(result)

        return results

    def _run_schema_test(self, test: Test) -> dict[str, Any]:
        """Run a schema test."""
        errors = []

        # Get the model/source being tested
        model = self.project.get_model(test.model)
        source = self.project.get_source(test.model) if not model else None

        if not model and not source:
            return {
                "name": test.name,
                "status": "failed",
                "errors": [f"Model/source '{test.model}' not found"],
            }

        # For now, schema tests pass if the model/source exists
        # Full implementation would check against Schema Registry
        for assertion in test.assertions:
            assertion_type = list(assertion.keys())[0]
            assertion_config = assertion[assertion_type]

            if assertion_type == "not_null":
                # Check that columns are declared (basic validation)
                _columns = assertion_config.get("columns", [])  # noqa: F841
                # In real implementation, would check schema registry
                pass

            elif assertion_type == "accepted_types":
                # Check types
                _types = assertion_config.get("types", {})  # noqa: F841
                # In real implementation, would check schema registry
                pass

        if errors:
            return {
                "name": test.name,
                "status": "failed",
                "errors": errors,
            }

        return {
            "name": test.name,
            "status": "passed",
        }

    def _run_sample_test(self, test: Test) -> dict[str, Any]:
        """Run a sample test."""
        errors = []

        # Get sample size
        sample_size = test.sample_size or 100

        # Get the model topic
        model = self.project.get_model(test.model)
        if not model:
            return {
                "name": test.name,
                "status": "failed",
                "errors": [f"Model '{test.model}' not found"],
            }

        # In real implementation, would consume N messages from topic
        # and run assertions against them

        # For now, simulate sample test
        # This would require a Kafka consumer to sample messages
        try:
            # Placeholder - would actually sample from Kafka
            messages = self._sample_messages(model, sample_size)

            for assertion in test.assertions:
                assertion_type = list(assertion.keys())[0]
                assertion_config = assertion[assertion_type]

                if assertion_type == "not_null":
                    columns = assertion_config.get("columns", [])
                    for msg in messages:
                        for col in columns:
                            if col in msg and msg[col] is None:
                                errors.append(f"Column '{col}' has null values")
                                break

                elif assertion_type == "accepted_values":
                    column = assertion_config.get("column")
                    values = set(assertion_config.get("values", []))
                    for msg in messages:
                        if column in msg and msg[column] not in values:
                            errors.append(f"Column '{column}' has invalid value: {msg[column]}")
                            break

                elif assertion_type == "range":
                    column = assertion_config.get("column")
                    min_val = assertion_config.get("min")
                    max_val = assertion_config.get("max")
                    for msg in messages:
                        if column in msg:
                            val = msg[column]
                            if min_val is not None and val < min_val:
                                errors.append(
                                    f"Column '{column}' value {val} below minimum {min_val}"
                                )
                                break
                            if max_val is not None and val > max_val:
                                errors.append(
                                    f"Column '{column}' value {val} above maximum {max_val}"
                                )
                                break

        except Exception as e:
            errors.append(f"Failed to sample messages: {e}")

        if errors:
            return {
                "name": test.name,
                "status": "failed",
                "errors": errors,
            }

        return {
            "name": test.name,
            "status": "passed",
            "sample_size": sample_size,
        }

    def _run_continuous_test(self, test: Test) -> dict[str, Any]:
        """Run a continuous test (or report status)."""
        # Continuous tests are deployed as Flink jobs
        # This method checks if the test job exists and its status

        if not self.project.runtime.flink:
            return {
                "name": test.name,
                "status": "failed",
                "errors": ["Continuous test requires Flink. Configure runtime.flink"],
            }

        # In real implementation, would check Flink job status
        return {
            "name": test.name,
            "status": "passed",
            "message": "Continuous test - use --deploy to deploy as Flink job",
        }

    def _sample_messages(self, model: Any, sample_size: int) -> list[dict]:
        """Sample messages from a topic.

        This is a placeholder - real implementation would use Kafka consumer.
        """
        # Would connect to Kafka and consume sample_size messages
        # For testing purposes, return empty list
        return []
