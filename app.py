import pandas as pd
from typing import Any
from fastapi import FastAPI, APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
import logging

logging.getLogger("uvicorn.error").setLevel(logging.ERROR)

# OpenTelemetry constants
OTLP_TRACES_PATH = "/v1/traces"
MLFLOW_EXPERIMENT_ID_HEADER = "X-MLflow-Experiment-Id"

# Create FastAPI app
app = FastAPI(
    title="OTEL Service",
    description="OpenTelemetry trace collection service",
    version="1.0.0",
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
    x_mlflow_experiment_id: str = Header(..., alias=MLFLOW_EXPERIMENT_ID_HEADER),
    content_type: str = Header(None),
) -> OTelExportTraceServiceResponse:
    """
    Export trace spans via OpenTelemetry protocol.
    
    Args:
        request: OTel ExportTraceServiceRequest in protobuf format
        response: FastAPI Response object for setting headers
        x_mlflow_experiment_id: Required header containing the experiment ID
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
    
    # For this demo, just log the received data
    logging.info(f"Received trace data for experiment {x_mlflow_experiment_id}: {len(body)} bytes")
    
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
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
