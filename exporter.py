"""
OpenTelemetry span exporter for Databricks Delta using Zerobus.

This module provides utilities to export OpenTelemetry spans to Databricks Delta tables
using the Zerobus streaming ingestion SDK.
"""

import atexit
import json
import logging
import threading

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from zerobus_sdk import TableProperties, StreamState

from constants import (
    Constants,
    OTEL_SPAN_KIND_MAP,
    OTEL_STATUS_CODE_MAP,
    get_table_properties,
)

_logger = logging.getLogger(__name__)


def import_zerobus_sdk_classes():
    """
    Import Zerobus SDK classes.

    Returns:
        Tuple of (TableProperties, StreamState) classes
    """
    from zerobus_sdk import TableProperties

    return TableProperties, StreamState


def create_archival_zerobus_sdk():
    """
    Create a configured ZerobusSdk instance for trace archival.

    Returns:
        ZerobusSdk: Configured SDK instance ready for trace archival operations
    """
    # Use MLflow's complete SDK creation function which handles all auth properly
    from mlflow.genai.experimental.databricks_trace_exporter_utils import (
        create_archival_zerobus_sdk as mlflow_create_sdk,
    )

    # This will use environment variables and proper auth resolution
    return mlflow_create_sdk()


def _decode_anyvalue(any_value):
    """
    Decode an OTel protobuf AnyValue using WhichOneof.

    Args:
        any_value: The OTel protobuf AnyValue message to decode.

    Returns:
        The decoded value.
    """
    value_type = any_value.WhichOneof("value")
    if not value_type:
        return None

    # Handle complex types that need recursion
    if value_type == "array_value":
        return [_decode_anyvalue(v) for v in any_value.array_value.values]
    elif value_type == "kvlist_value":
        return {
            kv.key: _decode_anyvalue(kv.value) for kv in any_value.kvlist_value.values
        }
    else:
        # For simple types, just get the attribute directly
        return getattr(any_value, value_type)


def convert_otel_proto_to_delta_spans(
    parsed_request: ExportTraceServiceRequest,
) -> list:
    """
    Convert OpenTelemetry proto spans to Delta proto spans.

    Args:
        parsed_request: Parsed OpenTelemetry ExportTraceServiceRequest

    Returns:
        List of Delta proto spans ready for ingestion
    """
    from mlflow.genai.experimental.databricks_trace_otel_pb2 import (
        Span as DeltaProtoSpan,
    )

    delta_proto_spans = []

    for resource_span in parsed_request.resource_spans:
        # Extract resource attributes
        resource_attributes = {}
        if resource_span.resource and resource_span.resource.attributes:
            for attr in resource_span.resource.attributes:
                resource_attributes[attr.key] = _decode_anyvalue(attr.value)

        for scope_span in resource_span.scope_spans:
            for otel_span in scope_span.spans:
                delta_proto = _create_delta_proto_span(
                    otel_span, resource_attributes, DeltaProtoSpan
                )
                delta_proto_spans.append(delta_proto)

    return delta_proto_spans


def _create_delta_proto_span(otel_span, resource_attributes, DeltaProtoSpan):
    """
    Create a single Delta proto span from an OTel span.

    Args:
        otel_span: OpenTelemetry span to convert
        resource_attributes: Resource attributes to include
        DeltaProtoSpan: Delta proto span class

    Returns:
        Constructed Delta proto span
    """
    delta_proto = DeltaProtoSpan()

    # Basic span info
    delta_proto.trace_id = otel_span.trace_id.hex()
    delta_proto.span_id = otel_span.span_id.hex()
    delta_proto.parent_span_id = (
        otel_span.parent_span_id.hex() if otel_span.parent_span_id else ""
    )
    delta_proto.trace_state = otel_span.trace_state or ""
    delta_proto.flags = otel_span.flags or 0
    delta_proto.name = otel_span.name

    # Map span kind using constants
    delta_proto.kind = OTEL_SPAN_KIND_MAP.get(otel_span.kind, "INTERNAL")

    # Timestamps
    delta_proto.start_time_unix_nano = otel_span.start_time_unix_nano
    delta_proto.end_time_unix_nano = otel_span.end_time_unix_nano

    # Combine resource and span attributes
    for key, value in resource_attributes.items():
        delta_proto.attributes[f"resource.{key}"] = (
            json.dumps(value) if not isinstance(value, str) else value
        )

    # Process span attributes
    for attr in otel_span.attributes:
        value = _decode_anyvalue(attr.value)
        # Convert to string for Delta proto (attributes are stored as strings)
        if isinstance(value, str):
            delta_proto.attributes[attr.key] = value
        else:
            delta_proto.attributes[attr.key] = json.dumps(value)

    delta_proto.dropped_attributes_count = otel_span.dropped_attributes_count or 0

    # Convert events
    for event in otel_span.events:
        event_dict = {
            "time_unix_nano": event.time_unix_nano,
            "name": event.name,
            "attributes": {},
            "dropped_attributes_count": event.dropped_attributes_count or 0,
        }
        for attr in event.attributes:
            value = _decode_anyvalue(attr.value)
            # Convert to string for event attributes
            if isinstance(value, str):
                event_dict["attributes"][attr.key] = value
            else:
                event_dict["attributes"][attr.key] = json.dumps(value)

        delta_proto.events.append(DeltaProtoSpan.Event(**event_dict))

    delta_proto.dropped_events_count = otel_span.dropped_events_count or 0
    delta_proto.dropped_links_count = otel_span.dropped_links_count or 0

    # Convert status
    status_code = otel_span.status.code if otel_span.status else 0
    status_dict = {
        "message": otel_span.status.message if otel_span.status else "",
        "code": OTEL_STATUS_CODE_MAP.get(status_code, "UNSET"),
    }
    delta_proto.status.CopyFrom(DeltaProtoSpan.Status(**status_dict))

    return delta_proto


class ZerobusStreamFactory:
    """
    Factory for creating and managing Zerobus streams with caching and automatic recovery.
    """

    # Class-level singleton registry: table_name -> factory instance
    _instances: dict[str, "ZerobusStreamFactory"] = {}
    _instances_lock = threading.Lock()
    _atexit_registered = False

    @classmethod
    def get_instance(cls, table_properties: TableProperties) -> "ZerobusStreamFactory":
        """
        Get or create a singleton factory instance for the given table.
        """
        table_name = table_properties.table_name
        if table_name not in cls._instances:
            with cls._instances_lock:
                if table_name not in cls._instances:
                    cls._instances[table_name] = cls(table_properties)

                    # Register atexit handler
                    if not cls._atexit_registered:
                        atexit.register(cls.reset)
                        cls._atexit_registered = True
                        _logger.debug(
                            "Registered atexit handler for ZerobusStreamFactory cleanup"
                        )
        return cls._instances[table_name]

    @classmethod
    def reset(cls):
        """
        Reset all factory instances and close their streams.
        """
        with cls._instances_lock:
            for table_name, factory in cls._instances.items():
                try:
                    factory.close_all_streams()
                except Exception as e:
                    _logger.warning(
                        f"Error closing streams for table {table_name}: {e}"
                    )
            cls._instances.clear()

    def __init__(self, table_properties: TableProperties):
        """
        Initialize factory with table properties.
        """
        self.table_properties = table_properties
        self._thread_local = threading.local()

    def get_or_create_stream(self):
        """
        Get or create a cached stream for current thread.
        """
        stream_cache = getattr(self._thread_local, "stream_cache", None)

        # Check if we have a cached stream
        if stream_cache and "stream" in stream_cache:
            stream = stream_cache["stream"]

            _, StreamState = import_zerobus_sdk_classes()

            # Check if stream is in a valid state
            if stream.get_state() not in [StreamState.OPENED, StreamState.FLUSHING]:
                _logger.debug(
                    f"Stream in invalid state {stream.get_state()}, creating new stream"
                )
                self._thread_local.stream_cache = None

        # Return the valid stream if it wasn't invalidated
        if (
            hasattr(self._thread_local, "stream_cache")
            and self._thread_local.stream_cache
        ):
            return self._thread_local.stream_cache["stream"]

        # Create new stream
        try:
            sdk = create_archival_zerobus_sdk()
            stream = sdk.create_stream(self.table_properties)
            self._thread_local.stream_cache = {"stream": stream}
            _logger.debug(
                f"Created new Zerobus stream for table {self.table_properties.table_name}"
            )
            return stream
        except Exception as e:
            _logger.error(f"Failed to create Zerobus stream: {e}")
            raise

    def refresh_current_thread_stream(self):
        """
        Refresh the stream for the current thread.

        No locking needed because:
        - Each thread has its own isolated copy of thread-local storage
        - Only this thread can modify self._thread_local.stream_cache
        - No shared state between threads for the stream cache
        """
        stream_cache = getattr(self._thread_local, "stream_cache", None)
        if stream_cache and "stream" in stream_cache:
            # Close old stream
            try:
                stream_cache["stream"].flush()
                stream_cache["stream"].close()
            except Exception as e:
                _logger.warning(f"Error closing old stream: {e}")

        # Clear cache and create new stream
        self._thread_local.stream_cache = None
        self.get_or_create_stream()
        thread_id = threading.current_thread().ident
        _logger.info(
            f"Refreshed stream for thread {thread_id} in table {self.table_properties.table_name}"
        )

    def close_all_streams(self):
        """
        Close all streams managed by this factory.
        """
        if hasattr(self._thread_local, "stream_cache"):
            stream = self._thread_local.stream_cache.get("stream")
            if stream:
                try:
                    stream.flush()
                    stream.close()
                except Exception as e:
                    _logger.warning(f"Error closing stream: {e}")


def export_otel_spans_to_delta(
    parsed_request: ExportTraceServiceRequest,
    catalog: str,
    schema: str,
    table_prefix: str,
) -> bool:
    """
    Export OpenTelemetry spans to Databricks Delta table using Zerobus.

    Args:
        parsed_request: Parsed OpenTelemetry ExportTraceServiceRequest
        catalog: Unity Catalog catalog name
        schema: Unity Catalog schema name
        table_prefix: Prefix for the table name

    Returns:
        True if export was successful, False otherwise
    """
    try:
        # Convert OTel proto spans to Delta proto spans
        delta_proto_spans = convert_otel_proto_to_delta_spans(parsed_request)

        if not delta_proto_spans:
            _logger.debug("No proto spans to export")
            return True

        # Get stream factory singleton
        factory = ZerobusStreamFactory.get_instance(get_table_properties())

        # Get or create stream
        stream = factory.get_or_create_stream()

        # Ingest all spans
        _logger.info(
            f"Ingesting {len(delta_proto_spans)} spans to table {Constants.UC_FULL_TABLE_NAME}"
        )
        for proto_span in delta_proto_spans:
            stream.ingest_record(proto_span)

        # Flush to ensure data durability
        stream.flush()

        return True

    except Exception as e:
        _logger.error(f"Failed to export spans to Databricks Delta: {e}")
        return False
