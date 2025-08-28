"""
Configuration constants for the OTEL service.
"""

import os
import logging

_logger = logging.getLogger(__name__)


class Constants:
    """
    Static class to hold configuration constants.
    All values are initialized at startup from environment variables.
    """
    
    # Databricks configuration
    DATABRICKS_TOKEN: str
    DATABRICKS_WORKSPACE_URL: str
    DATABRICKS_INGEST_URL: str
    
    # Unity Catalog configuration
    UC_CATALOG_NAME: str
    UC_SCHEMA_NAME: str
    UC_TABLE_PREFIX_NAME: str
    
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
        cls.DATABRICKS_WORKSPACE_URL = os.environ.get("DATABRICKS_WORKSPACE_URL", "")
        cls.DATABRICKS_INGEST_URL = os.environ.get("DATABRICKS_INGEST_URL", "")
        
        # Validate required Databricks configuration
        missing = []
        if not cls.DATABRICKS_TOKEN:
            missing.append("DATABRICKS_TOKEN")
        if not cls.DATABRICKS_WORKSPACE_URL:
            missing.append("DATABRICKS_WORKSPACE_URL")
        if not cls.DATABRICKS_INGEST_URL:
            missing.append("DATABRICKS_INGEST_URL")
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "These must be set for the service to function properly."
            )
        
        # Unity Catalog configuration with defaults
        cls.UC_CATALOG_NAME = os.environ.get("UC_CATALOG_NAME", "main")
        cls.UC_SCHEMA_NAME = os.environ.get("UC_SCHEMA_NAME", "default")
        cls.UC_TABLE_PREFIX_NAME = os.environ.get("UC_TABLE_PREFIX_NAME", "otel")
        
        # MLflow configuration with default
        cls.MLFLOW_EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "/Shared/otel-traces")
        
        # Log configuration
        _logger.info("Configuration initialized successfully:")
        _logger.info(f"  Workspace URL: {cls.DATABRICKS_WORKSPACE_URL}")
        _logger.info(f"  Ingest URL: {cls.DATABRICKS_INGEST_URL}")
        _logger.info(f"  UC Table: {cls.UC_CATALOG_NAME}.{cls.UC_SCHEMA_NAME}.{cls.UC_TABLE_PREFIX_NAME}_spans")
        _logger.info(f"  MLflow Experiment: {cls.MLFLOW_EXPERIMENT_NAME}")
