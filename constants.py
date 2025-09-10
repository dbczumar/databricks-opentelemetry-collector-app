"""
Configuration constants for the OTEL service.
"""

import logging
import os

from zerobus_sdk import TableProperties

_logger = logging.getLogger(__name__)

# OpenTelemetry constants
OTLP_TRACES_PATH = "/v1/traces"

# OpenTelemetry span kind mapping
OTEL_SPAN_KIND_MAP = {
    0: "UNSPECIFIED",
    1: "INTERNAL",
    2: "SERVER",
    3: "CLIENT",
    4: "PRODUCER",
    5: "CONSUMER",
}

# OpenTelemetry status code mapping
OTEL_STATUS_CODE_MAP = {0: "UNSET", 1: "OK", 2: "ERROR"}


class Constants:
    """
    Static class to hold configuration constants.
    All values are initialized at startup from environment variables.
    """

    # Databricks configuration
    DATABRICKS_TOKEN: str
    DATABRICKS_HOST: str

    # Unity Catalog configuration
    UC_CATALOG_NAME: str
    UC_SCHEMA_NAME: str
    UC_TABLE_PREFIX_NAME: str
    UC_FULL_TABLE_NAME: str  # Set after create_trace_destination

    # MLflow configuration
    MLFLOW_EXPERIMENT_NAME: str

    @classmethod
    def initialize(cls):
        """
        Initialize all constants from environment variables.
        Throws ValueError if any required variables are missing.
        """
        # Required Databricks configuration
        cls.DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
        cls.DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")

        # Validate required Databricks configuration
        missing = []
        if not cls.DATABRICKS_TOKEN:
            missing.append("DATABRICKS_TOKEN")
        if not cls.DATABRICKS_HOST:
            missing.append("DATABRICKS_HOST")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "These must be set for the service to function properly."
            )

        # Unity Catalog configuration (required)
        cls.UC_CATALOG_NAME = os.environ.get("UC_CATALOG_NAME", "")
        cls.UC_SCHEMA_NAME = os.environ.get("UC_SCHEMA_NAME", "")
        cls.UC_TABLE_PREFIX_NAME = os.environ.get("UC_TABLE_PREFIX_NAME", "")

        # MLflow configuration (required)
        cls.MLFLOW_EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "")

        # Validate Unity Catalog and MLflow configuration
        if not cls.UC_CATALOG_NAME:
            missing.append("UC_CATALOG_NAME")
        if not cls.UC_SCHEMA_NAME:
            missing.append("UC_SCHEMA_NAME")
        if not cls.UC_TABLE_PREFIX_NAME:
            missing.append("UC_TABLE_PREFIX_NAME")
        if not cls.MLFLOW_EXPERIMENT_NAME:
            missing.append("MLFLOW_EXPERIMENT_NAME")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "These must be set for the service to function properly."
            )

        # Log configuration
        _logger.info("Configuration initialized successfully:")
        _logger.info(f"  Host: {cls.DATABRICKS_HOST}")
        _logger.info(
            f"  UC Table Prefix: {cls.UC_CATALOG_NAME}.{cls.UC_SCHEMA_NAME}.{cls.UC_TABLE_PREFIX_NAME}"
        )
        _logger.info(f"  MLflow Experiment: {cls.MLFLOW_EXPERIMENT_NAME}")


def get_table_properties() -> TableProperties:
    """Get configured TableProperties for the UC table."""
    from mlflow.genai.experimental.databricks_trace_otel_pb2 import (
        Span as DeltaProtoSpan,
    )

    return TableProperties(
        table_name=Constants.UC_FULL_TABLE_NAME,
        descriptor_proto=DeltaProtoSpan.DESCRIPTOR,
    )
