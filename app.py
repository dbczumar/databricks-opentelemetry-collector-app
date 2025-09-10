import asyncio
import logging
import os
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

from constants import Constants, OTLP_TRACES_PATH, get_table_properties
from exporter import export_otel_spans_to_delta, ZerobusStreamFactory

NUM_WORKER_THREADS = 16
STREAM_REFRESH_INTERVAL_SECONDS = 600  # 10 minutes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)
# Reduce verbosity of zerobus_sdk logs
logging.getLogger("zerobus_sdk").setLevel(logging.WARNING)

executor = None

# explicitly unset to prevent oauth from conflicting with pat token
os.environ.pop('DATABRICKS_CLIENT_ID', None)
os.environ.pop('DATABRICKS_CLIENT_SECRET', None)


def init_thread_stream():
    """Initialize stream for a thread pool worker."""
    import threading

    table_properties = get_table_properties()
    factory = ZerobusStreamFactory.get_instance(table_properties)
    stream = factory.get_or_create_stream()
    thread_id = threading.current_thread().ident
    logger.info(
        f"Pre-seeded stream for thread {thread_id} in table {Constants.UC_FULL_TABLE_NAME}"
    )
    return stream


def refresh_streams_in_thread():
    """Refresh streams for all factories in the current thread."""
    factory = ZerobusStreamFactory.get_instance(get_table_properties())
    factory.refresh_current_thread_stream()


async def periodic_stream_refresh():
    """
    Background task that refreshes Zerobus streams every 10 minutes.
    """
    while True:
        try:
            await asyncio.sleep(STREAM_REFRESH_INTERVAL_SECONDS)
            logger.info("Starting periodic stream refresh")
            
            # Submit refresh tasks to all worker threads
            futures = []
            for _ in range(NUM_WORKER_THREADS):
                future = executor.submit(refresh_streams_in_thread)
                futures.append(future)
            
            # Wait for all refreshes to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.warning(f"Error during stream refresh: {e}")
                    
            logger.info("Completed periodic stream refresh")
        except asyncio.CancelledError:
            logger.info("Periodic stream refresh task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in periodic stream refresh: {e}")


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
    logger.info("This may take a while as it creates the table if it doesn't exist...")
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

    global executor
    executor = ThreadPoolExecutor(max_workers=NUM_WORKER_THREADS, initializer=init_thread_stream)
    logger.info(f"Created thread pool executor with {NUM_WORKER_THREADS} workers")

    logger.info("Pre-seeding streams by warming up thread pool...")
    futures = []
    for i in range(NUM_WORKER_THREADS):
        future = executor.submit(lambda: None)
        futures.append(future)

    for future in futures:
        future.result()

    logger.info("Thread pool warmed up - all streams pre-seeded")

    # Start the periodic refresh task
    refresh_task = asyncio.create_task(periodic_stream_refresh())
    logger.info(f"Started periodic stream refresh task ({STREAM_REFRESH_INTERVAL_SECONDS} second intervals)")

    yield

    # Cancel the refresh task
    refresh_task.cancel()
    try:
        await refresh_task
    except asyncio.CancelledError:
        pass
    logger.info("Stopped periodic stream refresh task")

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
def health_check():
    return {"status": "ok"}


app.include_router(otel_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, workers=16, log_level="info")
