# `flowceptcol`

OpenTelemetry Collector build that includes the Flowcept exporter
(`exporter/flowceptexporter`). It receives OTLP traces (for example, the
OpenInference spans emitted by the Agno agents of the workflow-gateway) and
writes them as Flowcept records to a JSONL file.

The sources were copied from `cmd/otelcorecol` and the Flowcept exporter was
added to `components.go`, `go.mod` and `builder-config.yaml`.

## Build

Requires Go 1.26. The exporter is resolved through a local `replace` in
`go.mod`, so build from a clone of the `otel_adapter` branch of the fork:

```bash
git clone -b otel_adapter https://github.com/valescamoura/opentelemetry-collector.git
cd opentelemetry-collector/exporter/flowceptexporter
go mod tidy && go test ./...
cd ../../cmd/flowceptcol
go mod tidy
go build -o flowceptcol .
```

## Test (local clone)

From the repository root:

```bash
# 1. Unit tests of the exporter
cd exporter/flowceptexporter
go mod tidy
go test ./...

# 2. Build the collector
cd ../../cmd/flowceptcol
go mod tidy
go build -o flowceptcol .

# 3. Smoke test: run it and send one span over OTLP/HTTP
FLOWCEPT_OTEL_BUFFER_PATH=/tmp/flowcept_otel_buffer.jsonl ./flowceptcol --config config.yaml &
curl -s -X POST http://localhost:4318/v1/traces -H 'Content-Type: application/json' -d '{
  "resourceSpans": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "smoke"}}]},
    "scopeSpans": [{"spans": [{
      "traceId": "0102030405060708090a0b0c0d0e0f10",
      "spanId": "0102030405060708",
      "name": "OmniAgent.run",
      "kind": 1,
      "startTimeUnixNano": "1792411200000000000",
      "endTimeUnixNano": "1792411202000000000",
      "attributes": [
        {"key": "session.id", "value": {"stringValue": "sess-smoke"}},
        {"key": "openinference.span.kind", "value": {"stringValue": "AGENT"}},
        {"key": "input.value", "value": {"stringValue": "hello"}}
      ]
    }]}]
  }]
}'
cat /tmp/flowcept_otel_buffer.jsonl   # one workflow (chat:sess-smoke) + one task
kill %1
```

The span above is a trace root, so it is written right away. Child spans are
held until their root arrives (see `exporter/flowceptexporter/README.md`).

## Run

```bash
FLOWCEPT_OTEL_BUFFER_PATH=/path/to/flowcept_otel_buffer.jsonl \
  ./flowceptcol --config config.yaml
```

Point the gateway telemetry to the collector: `PHOENIX_COLLECTOR_ENDPOINT=http://localhost:4317`
(`phoenix.otel` sends OTLP/gRPC to port 4317 when only the host is given).

## Read the output

```python
from flowcept import Flowcept
records = Flowcept.read_buffer_file("flowcept_otel_buffer.jsonl")
```
