import pandas as pd
from typing import Any
from fastapi import FastAPI, APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from google.protobuf.message import DecodeError
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import logging
import sys
import asyncio

from constants import Constants, OTLP_TRACES_PATH
from exporter import export_otel_spans_to_delta

# Configure logging to actually show our messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


async def lifespan(app: FastAPI):
    """
    Lifespan event handler for initialization tasks.
    """
    # Initialize all configuration constants
    Constants.initialize()
    logger.info("Service configuration initialized successfully")
    
    # Create MLflow experiment and trace destination at startup
    import mlflow
    from mlflow.genai.experimental.databricks_trace_exporter_utils import DatabricksTraceServerClient
    
    # Set tracking URI for Databricks
    mlflow.set_tracking_uri("databricks")
    logger.info("Set MLflow tracking URI to 'databricks'")
    
    # Get or create experiment
    experiment_name = Constants.MLFLOW_EXPERIMENT_NAME
    logger.info(f"Looking for experiment: {experiment_name}")
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment:
        experiment_id = experiment.experiment_id
        logger.info(f"Using existing experiment: {experiment_name} (ID: {experiment_id})")
    else:
        experiment_id = mlflow.create_experiment(experiment_name)
        logger.info(f"Created new experiment: {experiment_name} (ID: {experiment_id})")
    
    # Create trace destination (this creates the table if it doesn't exist)
    client = DatabricksTraceServerClient()
    
    logger.info(f"Creating/verifying trace destination for catalog={Constants.UC_CATALOG_NAME}, schema={Constants.UC_SCHEMA_NAME}, prefix={Constants.UC_TABLE_PREFIX_NAME}")
    logger.info(f"This may take a while as it creates the table if it doesn't exist...")
    storage_config = client.create_trace_destination(
        experiment_id=experiment_id,
        catalog=Constants.UC_CATALOG_NAME,
        schema=Constants.UC_SCHEMA_NAME,
        table_prefix=Constants.UC_TABLE_PREFIX_NAME
    )
    
    # Get the actual full table name from the response
    full_table_name = storage_config.spans_table_name
    Constants.UC_FULL_TABLE_NAME = full_table_name  # Store for use in exporter
    logger.info(f"Trace destination configured for experiment {experiment_id}")
    logger.info(f"Full table name from config: {full_table_name}")
    
    # Set up Zerobus connection at startup (REQUIRED)
    logger.info("Setting up Zerobus connection...")
    from exporter import ZerobusStreamFactory
    from zerobus_sdk import TableProperties
    from mlflow.genai.experimental.databricks_trace_otel_pb2 import Span as DeltaProtoSpan
    
    table_properties = TableProperties(
        table_name=full_table_name,
        descriptor_proto=DeltaProtoSpan.DESCRIPTOR
    )
    
    # Get or create stream to validate configuration
    factory = ZerobusStreamFactory.get_instance(table_properties)
    stream = factory.get_or_create_stream()
    logger.info(f"Successfully connected to Zerobus for table {full_table_name}")
    
    yield


# Create FastAPI app
app = FastAPI(
    title="OTEL Service",
    description="OpenTelemetry trace collection service",
    version="1.0.0",
    lifespan=lifespan,
)

# Create OTel router
otel_router = APIRouter(prefix=OTLP_TRACES_PATH, tags=["OpenTelemetry"])


class OTelExportTraceServiceResponse(BaseModel):
    """Response model for OTLP trace export."""
    partialSuccess: dict[str, Any] | None = Field(
        None, description="Details about partial success of the export operation"
    )


@otel_router.post("", response_model=OTelExportTraceServiceResponse, status_code=200)
async def export_traces(
    request: Request,
    response: Response,
    content_type: str = Header(None),
) -> OTelExportTraceServiceResponse:
    """
    Export trace spans via OpenTelemetry protocol.
    
    Args:
        request: OTel ExportTraceServiceRequest in protobuf format
        response: FastAPI Response object for setting headers
        content_type: Content-Type header from the request
    
    Returns:
        OTel ExportTraceServiceResponse indicating success
    """
    # Validate Content-Type header
    if content_type != "application/x-protobuf":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid Content-Type: {content_type}. Expected: application/x-protobuf",
        )
    
    # Set response Content-Type header
    response.headers["Content-Type"] = "application/x-protobuf"
    
    body = await request.body()
    parsed_request = ExportTraceServiceRequest()
    
    try:
        parsed_request.ParseFromString(body)
    except DecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid OpenTelemetry protobuf format",
        )
    
    # Log the received trace data
    num_spans = sum(
        len(scope_span.spans)
        for resource_span in parsed_request.resource_spans
        for scope_span in resource_span.scope_spans
    )
    logger.info(f"Received {num_spans} spans")
    
    # Export spans to Delta table using Zerobus (run in thread pool to avoid blocking)
    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(
        None,
        export_otel_spans_to_delta,
        parsed_request, 
        Constants.UC_CATALOG_NAME, 
        Constants.UC_SCHEMA_NAME, 
        Constants.UC_TABLE_PREFIX_NAME
    )
    
    if not success:
        logger.warning("Failed to export spans to Delta table")
        # Return success anyway to avoid blocking the client
    
    return OTelExportTraceServiceResponse()


@app.get("/")
def hello_world():
    chart_data = pd.DataFrame({'Apps': [x for x in range(30)],
                               'Fun with data': [2 ** x for x in range(30)]})
    return f'<h1>Hello, World!</h1> {chart_data.to_html(index=False)}'


# Include the OTel router
app.include_router(otel_router)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8123, workers=16, log_level="info")
