#!/usr/bin/env python3
"""Send test spans to Databricks App using OpenTelemetry."""

import os
import sys
import time
from databricks.sdk import WorkspaceClient
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Check if endpoint is set
if "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in os.environ:
    print("Error: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT environment variable is not set.")
    print("Set it to your Databricks App URL, e.g.:")
    print("export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://your-app-name.databricksapps.com/v1/traces")
    sys.exit(1)

# Setup
databricks_workspace_client = WorkspaceClient()
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(
    headers={"content-type": "application/x-protobuf", **databricks_workspace_client.config.authenticate()}
)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# Send test spans
with tracer.start_as_current_span("test_operation") as span:
    span.set_attribute("test", "hello")
    time.sleep(0.1)
    with tracer.start_as_current_span("child_operation") as child:
        child.set_attribute("nested", True)
        time.sleep(0.05)

# Flush
provider.force_flush()
print(f"✓ Spans sent to {os.environ['OTEL_EXPORTER_OTLP_TRACES_ENDPOINT']}")