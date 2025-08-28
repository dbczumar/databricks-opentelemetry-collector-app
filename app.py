import pandas as pd
from typing import Any
from fastapi import FastAPI, APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from google.protobuf.message import DecodeError
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import logging

from constants import Constants
from exporter import export_otel_spans_to_delta

logging.getLogger("uvicorn.error").setLevel(logging.ERROR)

# OpenTelemetry constants
OTLP_TRACES_PATH = "/v1/traces"

# Create FastAPI app
app = FastAPI(
    title="OTEL Service",
    description="OpenTelemetry trace collection service",
    version="1.0.0",
)


@app.on_event("startup")
async def startup_event():
    """
    Startup hook for initialization tasks.
    """
    # Initialize all configuration constants
    Constants.initialize()
    logging.info("Service configuration initialized successfully")
    
    # Test Zerobus connection at startup
    from exporter import ZerobusStreamFactory
    from zerobus_sdk import TableProperties
    
    table_name = f"{Constants.UC_CATALOG_NAME}.{Constants.UC_SCHEMA_NAME}.{Constants.UC_TABLE_PREFIX_NAME}_spans"
    table_properties = TableProperties(
        table_name=table_name,
        catalog=Constants.UC_CATALOG_NAME,
        schema=Constants.UC_SCHEMA_NAME,
        table=f"{Constants.UC_TABLE_PREFIX_NAME}_spans"
    )
    
    # Get or create stream to validate configuration
    factory = ZerobusStreamFactory.get_instance(table_properties)
    stream = factory.get_or_create_stream()
    logging.info(f"Successfully connected to Zerobus for table {table_name}")

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
    logging.info(f"Received {num_spans} spans")
    
    # Export spans to Delta table using Zerobus
    success = export_otel_spans_to_delta(
        parsed_request, 
        Constants.UC_CATALOG_NAME, 
        Constants.UC_SCHEMA_NAME, 
        Constants.UC_TABLE_PREFIX_NAME
    )
    
    if not success:
        logging.warning("Failed to export spans to Delta table")
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
    uvicorn.run(app, host="0.0.0.0", port=8123, log_level="info")
