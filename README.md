# OpenTelemetry Delta Collector Databricks App

## Prerequisites

1. Install the Databricks SDK:
   ```bash
   pip install databricks-sdk --upgrade
   ```

2. Install the Databricks CLI according to the [installation instructions](https://docs.databricks.com/aws/en/dev-tools/cli/install):
   ```bash
   # Example using Homebrew on Linux or macOS
   brew tap databricks/tap
   brew install databricks

   # Or upgrade if already installed
   brew upgrade databricks
   ```

3. Change your shell working directory to the repository root directory:
   ```bash
   cd /path/to/databricks-opentelemetry-collector-app
   ```

If your workspace uses [IP Access Lists](https://docs.databricks.com/aws/en/security/network/front-end/ip-access-list), this application will not work.

## Setup

### 1. Databricks Authentication

Authenticate with Databricks using OAuth (required for querying Databricks Apps):
```bash
databricks auth login --host https://your-workspace.databricks.com
# This will open your browser for OAuth authentication
```

**Note:** [OAuth authentication](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m?language=Python#automatic-authorization-with-unified-client-authentication) is required to query Databricks Apps.

### 2. Set up the Databricks Token Secret

The `DATABRICKS_TOKEN` in `app.yaml` references a secret named `databricks-token`. You need to create and populate this secret with a Databricks Personal Access Token (PAT).

1. Create a Personal Access Token (PAT). See [Databricks PAT Documentation](https://docs.databricks.com/en/dev-tools/auth/pat.html).
   
   **Note:** The PAT token is required in order to provide the App with access to a UC Schema with direct delta ingest. The user creating the PAT must have access to the Unity Catalog catalog and schema specified in `app.yaml`.

2. Create a secret scope:
   ```bash
   databricks secrets create-scope opentelemetry-collector-app
   ```

3. Store your Databricks Personal Access Token (PAT) as a secret:
   ```bash
   databricks secrets put-secret opentelemetry-collector-app databricks-token --string-value "your-actual-token-here"
   ```

This stores your Databricks token securely so it can be referenced by the `valueFrom: databricks-token` in `app.yaml`.

### 3. Set up Unity Catalog for Trace Storage

The OpenTelemetry service stores trace data in Unity Catalog tables. Before deploying the app, ensure you have created a Unity Catalog catalog and schema that match the values specified in your `app.yaml`:

- `UC_CATALOG_NAME`: The catalog where trace tables will be created
- `UC_SCHEMA_NAME`: The schema within that catalog where trace tables will be stored
- `UC_TABLE_PREFIX_NAME`: The prefix for the trace tables (the service will create tables like `<prefix>_spans`)

The service will automatically create the necessary tables within your specified catalog and schema on first startup.

See documentation for:
- [Creating a catalog](https://docs.databricks.com/aws/en/catalogs/create-catalog#create-a-catalog)
- [Creating a schema](https://docs.databricks.com/aws/en/schemas/create-schema#create-a-schema)

### 4. App Environment Configuration

Update the environment variables in `app.yaml` based on the results from step 2 and 3:

```yaml
env:
  - name: DATABRICKS_TOKEN
    valueFrom: databricks-token  # References secret (see below)
  - name: DATABRICKS_HOST
    value: "https://your-workspace.databricks.com"  # Update with your workspace URL
  - name: MLFLOW_EXPERIMENT_NAME
    value: "/Shared/opentelemtry-traces"  # Will be created if it doesn't exist; can be updated
  - name: UC_CATALOG_NAME
    value: "main"  # Update with your catalog name
  - name: UC_SCHEMA_NAME
    value: "default"  # Update with your schema name
  - name: UC_TABLE_PREFIX_NAME
    value: "otel"  # Update with your desired table name prefix
```

#### OPTIONAL: Configuring Worker Count for Throughput

The app uses 16 workers by default. For higher throughput with large spans, you may need to adjust the worker count in `app.yaml`:

```yaml
command: [
  "uvicorn",
  "app:app",
  "--host",
  "0.0.0.0",
  "--port",
  "8000",
  "--workers",
  "16",  # Adjust based on your workload
]
```

**Performance benchmarks:**
- **16 workers**: ~70 spans/second for 100KB spans (7 MB/s throughput)

Each worker maintains its own thread pool (16 threads) for non-blocking I/O, so the actual concurrency is `workers × 16`.


### 5. Deploy the App

Deploy the OpenTelemetry Delta Collector to Databricks Apps using the Databricks CLI:

1. Create the app (first time only):
   ```bash
   databricks apps create opentelemetry-collector-app
   ```

   **Note:** You can replace `opentelemetry-collector-app` with your preferred app name.

2. Add the secret to your app via the Databricks UI:
   - Navigate to your app in the UI ([View app details](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/view-app-details))
   - Add the `databricks-token` secret from the "Setting up the Databricks Token Secret" section to your app (docs: [Configure resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/resources?language=Databricks+UI#configure-resources-for-your-app))
     - Click "Edit" in the upper right corner
     - Click "(2) Configure"
     - Click "Add Resource"
     - Choose "Secret"
     - When prompted, use `databricks-token` as the resource key

3. Sync your app files to the Databricks workspace:
   ```bash
   databricks sync . /Workspace/Users/your-email@company.com/opentelemetry-collector-code
   ```

   **Note:** Replace `your-email@company.com` with your Databricks user email.

4. Deploy the app:
   ```bash
   databricks apps deploy opentelemetry-collector-app \
     --source-code-path /Workspace/Users/your-email@company.com/opentelemetry-collector-code
   ```

   **Note:** The source code path should match the workspace destination from step 3 where you synced the app code.

5. Monitor the deployment:
   ```bash
   databricks apps get opentelemetry-collector-app
   ```

   This will show the app status and URL once deployed.

IMPORTANT NOTE: If you go to the app's URL in a browser, you will see an error that says the below.  THIS IS OK and an artifact of the app only hosting an API and not a web front end.

> App Not Available
> Sorry, the Databricks app you are trying to access is currently unavailable. Please try again later.


## Sending Spans to the Deployed App

Once your app is deployed, you can send OpenTelemetry spans to it.

**Note:** OAuth authentication is required to send spans to Databricks Apps. First authenticate using:
```bash
databricks auth login --host https://your-workspace.databricks.com
```
See [OAuth authentication documentation](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m?language=Python#automatic-authorization-with-unified-client-authentication) for more details.

For clients in other languages (e.g., TypeScript), see [Retrieving OAuth credentials](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m?language=Environment#automatic-authorization-with-unified-client-authentication) for information on accessing OAuth tokens from environment variables.

### Option 1: Run the example script

Install the `opentelemetry-sdk`, `databricks-sdk`, `opentelemetry-exporter-otlp` Python packages via `pip`, `conda`, `uv`, etc.

```bash
pip install opentelemetry-exporter-otlp opentelemetry-sdk databricks-sdk
```

Set the environment variable and run the provided example script:
```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://your-app-name.databricksapps.com/v1/traces
python example_query.py
```

### Option 2: Use the Python code directly

Replace `https://your-app-name.databricksapps.com/v1/traces` with your actual Databricks App URL:

```python
import os
import time
from databricks.sdk import WorkspaceClient
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Set endpoint via environment variable
os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "https://your-app-name.databricksapps.com/v1/traces"

# Set up Databricks Workspace Client with OAuth authentication
databricks_workspace_client = WorkspaceClient()

# Set up OpenTelemetry Tracer
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(
    headers={
        "content-type": "application/x-protobuf",
        # Retrieve the Databricks OAuth token from the Databricks Workspace Client
        # and set it as a header
        **databricks_workspace_client.config.authenticate()
    }
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

# Flush the spans
provider.force_flush()
print("✓ Spans sent")
```

## Common Issues

### Trace destination already exists error

**Error:**
```
mlflow.exceptions.RestException: ALREADY_EXISTS: Error [...]: Trace destination already exists for traceLocation MlflowExperiment(...) and cannot be modified.
```

**Cause:** The MLflow experiment already has a trace destination configured with a different table prefix or location.

**Solution:** 
1. Use a different MLflow experiment name in `app.yaml`, OR
2. Use the existing table prefix and location that was previously configured for this experiment (shown in the error message)
