import asyncio
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import mlflow
import pandas as pd
from fastapi import FastAPI, APIRouter, Header, HTTPException, Request, Response, status
from google.protobuf.message import DecodeError
from mlflow.genai.experimental.databricks_trace_exporter_utils import (
    DatabricksTraceServerClient,
)
from mlflow.genai.experimental.databricks_trace_otel_pb2 import Span as DeltaProtoSpan
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from pydantic import BaseModel, Field
from zerobus_sdk import TableProperties

from constants import Constants, OTLP_TRACES_PATH
from exporter import export_otel_spans_to_delta, ZerobusStreamFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Reduce verbosity of zerobus_sdk logs
logging.getLogger("zerobus_sdk").setLevel(logging.WARNING)

executor = None


def init_thread_stream():
    """Initialize stream for a thread pool worker."""
    import threading

    table_properties = TableProperties(
        table_name=Constants.UC_FULL_TABLE_NAME,
        descriptor_proto=DeltaProtoSpan.DESCRIPTOR,
    )
    factory = ZerobusStreamFactory.get_instance(table_properties)
    stream = factory.get_or_create_stream()
    thread_id = threading.current_thread().ident
    logger.info(
        f"Pre-seeded stream for thread {thread_id} in table {Constants.UC_FULL_TABLE_NAME}"
    )
    return stream


async def lifespan(app: FastAPI):
    """
    Lifespan event handler for initialization tasks.
    """
    Constants.initialize()
    logger.info("Service configuration initialized successfully")

    mlflow.set_tracking_uri("databricks")
    logger.info("Set MLflow tracking URI to 'databricks'")

    experiment_name = Constants.MLFLOW_EXPERIMENT_NAME
    logger.info(f"Getting or creating experiment '{experiment_name}'")
    experiment_id = mlflow.set_experiment(experiment_name).experiment_id

    client = DatabricksTraceServerClient()

    logger.info(
        f"Creating/verifying trace destination for catalog={Constants.UC_CATALOG_NAME}, schema={Constants.UC_SCHEMA_NAME}, prefix={Constants.UC_TABLE_PREFIX_NAME}"
    )
    logger.info(f"This may take a while as it creates the table if it doesn't exist...")
    storage_config = client.create_trace_destination(
        experiment_id=experiment_id,
        catalog=Constants.UC_CATALOG_NAME,
        schema=Constants.UC_SCHEMA_NAME,
        table_prefix=Constants.UC_TABLE_PREFIX_NAME,
    )

    full_table_name = storage_config.spans_table_name
    Constants.UC_FULL_TABLE_NAME = full_table_name
    logger.info(f"Trace destination configured for experiment {experiment_id}")
    logger.info(f"Full table name from config: {full_table_name}")

    logger.info("Setting up Zerobus connection...")

    table_properties = TableProperties(
        table_name=full_table_name, descriptor_proto=DeltaProtoSpan.DESCRIPTOR
    )

    global executor
    executor = ThreadPoolExecutor(max_workers=8, initializer=init_thread_stream)
    logger.info("Created thread pool executor with 8 workers")

    logger.info("Pre-seeding streams by warming up thread pool...")
    futures = []
    for i in range(8):
        future = executor.submit(lambda: None)
        futures.append(future)

    for future in futures:
        future.result()

    logger.info("Thread pool warmed up - all streams pre-seeded")

    yield

    if executor:
        executor.shutdown(wait=True)


app = FastAPI(
    title="OTEL Service",
    description="OpenTelemetry trace collection service",
    version="1.0.0",
    lifespan=lifespan,
)

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
    if content_type != "application/x-protobuf":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid Content-Type: {content_type}. Expected: application/x-protobuf",
        )

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

    num_spans = sum(
        len(scope_span.spans)
        for resource_span in parsed_request.resource_spans
        for scope_span in resource_span.scope_spans
    )
    logger.info(f"Received {num_spans} spans")

    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(
        executor,
        export_otel_spans_to_delta,
        parsed_request,
        Constants.UC_CATALOG_NAME,
        Constants.UC_SCHEMA_NAME,
        Constants.UC_TABLE_PREFIX_NAME,
    )

    if not success:
        logger.error("Failed to export spans to Delta table")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export spans to Delta table",
        )

    return OTelExportTraceServiceResponse()


@app.get("/")
def hello_world():
    chart_data = pd.DataFrame(
        {"Apps": [x for x in range(30)], "Fun with data": [2**x for x in range(30)]}
    )
    return f"<h1>Hello, World!</h1> {chart_data.to_html(index=False)}"


app.include_router(otel_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8123, workers=16, log_level="info")
