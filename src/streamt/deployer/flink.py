"""Flink deployer for job management."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import requests

from streamt.compiler.manifest import FlinkJobArtifact


@dataclass
class FlinkJobState:
    """Current state of a Flink job."""

    name: str
    exists: bool
    job_id: Optional[str] = None
    status: Optional[str] = None


@dataclass
class FlinkJobChange:
    """A change to apply to a Flink job."""

    job_name: str
    action: str  # submit, cancel, update, none
    current: Optional[FlinkJobState] = None
    desired: Optional[FlinkJobArtifact] = None


class FlinkDeployer:
    """Deployer for Flink jobs via REST API."""

    def __init__(self, rest_url: str) -> None:
        """Initialize Flink deployer."""
        self.rest_url = rest_url.rstrip("/")
        self.session_id: Optional[str] = None

    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> dict:
        """Make a request to Flink REST API."""
        url = f"{self.rest_url}{endpoint}"
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    def check_connection(self) -> bool:
        """Check if Flink cluster is accessible."""
        try:
            self._request("GET", "/overview")
            return True
        except Exception:
            return False

    def get_session(self) -> str:
        """Get or create a SQL session."""
        if self.session_id:
            return self.session_id

        # Create a new session
        response = self._request(
            "POST",
            "/v1/sessions",
            json={"planner": "blink", "executionType": "streaming"},
        )
        self.session_id = response.get("sessionId")
        return self.session_id

    def list_jobs(self) -> list[dict]:
        """List all jobs."""
        response = self._request("GET", "/jobs")
        return response.get("jobs", [])

    def get_job_state(self, job_name: str) -> FlinkJobState:
        """Get current state of a job by name."""
        jobs = self.list_jobs()

        for job in jobs:
            if job.get("name") == job_name:
                return FlinkJobState(
                    name=job_name,
                    exists=True,
                    job_id=job.get("id"),
                    status=job.get("status"),
                )

        return FlinkJobState(name=job_name, exists=False)

    def submit_sql(self, sql: str) -> dict:
        """Submit SQL statements to Flink."""
        session_id = self.get_session()

        # Split SQL into statements
        statements = [s.strip() for s in sql.split(";") if s.strip()]

        results = []
        for statement in statements:
            response = self._request(
                "POST",
                f"/v1/sessions/{session_id}/statements",
                json={"statement": statement},
            )
            results.append(response)

        return {"results": results}

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job."""
        self._request("PATCH", f"/jobs/{job_id}", json={"state": "cancelled"})

    def plan_job(self, artifact: FlinkJobArtifact) -> FlinkJobChange:
        """Plan changes for a Flink job."""
        current = self.get_job_state(artifact.name)

        if not current.exists:
            return FlinkJobChange(
                job_name=artifact.name,
                action="submit",
                current=current,
                desired=artifact,
            )

        # Job exists - check if running
        if current.status in ["RUNNING", "CREATED"]:
            return FlinkJobChange(
                job_name=artifact.name,
                action="none",  # Already running
                current=current,
                desired=artifact,
            )

        # Job exists but not running (FAILED, CANCELED, etc.)
        return FlinkJobChange(
            job_name=artifact.name,
            action="submit",  # Re-submit
            current=current,
            desired=artifact,
        )

    def apply_job(self, artifact: FlinkJobArtifact) -> str:
        """Apply a Flink job artifact. Returns action taken."""
        change = self.plan_job(artifact)

        if change.action == "submit":
            self.submit_sql(artifact.sql)
            return "submitted"
        elif change.action == "cancel":
            if change.current and change.current.job_id:
                self.cancel_job(change.current.job_id)
            return "cancelled"
        else:
            return "unchanged"


class FlinkSqlGatewayDeployer:
    """Deployer for Flink SQL Gateway (newer API)."""

    def __init__(self, gateway_url: str) -> None:
        """Initialize Flink SQL Gateway deployer."""
        self.gateway_url = gateway_url.rstrip("/")
        self.session_handle: Optional[str] = None

    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> dict:
        """Make a request to SQL Gateway."""
        url = f"{self.gateway_url}{endpoint}"
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    def open_session(self) -> str:
        """Open a new session."""
        response = self._request(
            "POST",
            "/v1/sessions",
            json={},
        )
        self.session_handle = response.get("sessionHandle")
        return self.session_handle

    def close_session(self) -> None:
        """Close the current session."""
        if self.session_handle:
            self._request("DELETE", f"/v1/sessions/{self.session_handle}")
            self.session_handle = None

    def execute_statement(self, statement: str) -> dict:
        """Execute a SQL statement."""
        if not self.session_handle:
            self.open_session()

        response = self._request(
            "POST",
            f"/v1/sessions/{self.session_handle}/statements",
            json={"statement": statement},
        )
        return response

    def submit_sql(self, sql: str) -> list[dict]:
        """Submit multiple SQL statements."""
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        results = []

        for statement in statements:
            result = self.execute_statement(statement)
            results.append(result)

        return results
