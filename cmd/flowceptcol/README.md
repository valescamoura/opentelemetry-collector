# `flowceptcol`

OpenTelemetry Collector build that includes the Flowcept exporter
(`exporter/flowceptexporter`). It receives OTLP traces (for example, the
OpenInference spans emitted by the Agno agents of the workflow-gateway) and
writes them as Flowcept records to a JSONL file.

The sources were copied from `cmd/otelcorecol` and the Flowcept exporter was
added to `components.go`, `go.mod` and `builder-config.yaml`.

## Configuration

Everything is in `config.yaml`. The only environment variable is optional:

| Variable | Default | Description |
|---|---|---|
| `FLOWCEPT_OTEL_BUFFER_PATH` | `flowcept_otel_buffer.jsonl` (relative to the directory where the collector runs) | JSONL file the Flowcept records are appended to. Use an absolute path, e.g. the `prov_data/` folder of the workflow-gateway. The folder must exist. |

The collector listens on `4317` (OTLP/gRPC) and `4318` (OTLP/HTTP). Phoenix
also uses `4317`, so do not run both on the same machine.

On the workflow-gateway side, set `PHOENIX_COLLECTOR_ENDPOINT=http://localhost:4317`
in its `.env` (`phoenix.otel` sends OTLP/gRPC to port 4317 when only the host
is given).

## Build

Requires Go 1.26. The exporter is resolved through a local `replace` in
`go.mod`, so build from a clone of the `otel_adapter` branch of the fork.
The binary is written to this folder and ignored by git.

macOS / Linux:

```bash
git clone -b otel_adapter https://github.com/valescamoura/opentelemetry-collector.git
cd opentelemetry-collector/exporter/flowceptexporter
go mod tidy
go test ./...
cd ../../cmd/flowceptcol
go mod tidy
go build -o flowceptcol .
./flowceptcol components | grep flowcept     # the exporter is in the build
```

Windows (PowerShell):

```powershell
git clone -b otel_adapter https://github.com/valescamoura/opentelemetry-collector.git
cd opentelemetry-collector\exporter\flowceptexporter
go mod tidy
go test ./...
cd ..\..\cmd\flowceptcol
go mod tidy
go build -o flowceptcol.exe .
.\flowceptcol.exe components | Select-String flowcept
```

`go build` prints nothing when it succeeds. The binary is a CLI program: run
it from a terminal, not by opening the file.

## Run

Run from `cmd/flowceptcol`. It keeps running and logging; stop it with Ctrl+C.

macOS / Linux:

```bash
FLOWCEPT_OTEL_BUFFER_PATH=/path/to/workflow-gateway/prov_data/flowcept_otel_buffer.jsonl \
  ./flowceptcol --config config.yaml
```

Windows (PowerShell):

```powershell
$env:FLOWCEPT_OTEL_BUFFER_PATH = "C:\path\to\workflow-gateway\prov_data\flowcept_otel_buffer.jsonl"
.\flowceptcol.exe --config config.yaml
```

## Smoke test

With the collector running (previous section, pointing to a test file), send
one span from another terminal and check the JSONL: it must contain one
workflow (`chat:sess-smoke`) and one task (`OmniAgent.run`). The span is a
trace root, so it is written right away; child spans are held until their root
arrives (see `exporter/flowceptexporter/README.md`).

macOS / Linux:

```bash
curl -s -X POST http://localhost:4318/v1/traces -H 'Content-Type: application/json' -d '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"smoke"}}]},"scopeSpans":[{"spans":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708","name":"OmniAgent.run","kind":1,"startTimeUnixNano":"1792411200000000000","endTimeUnixNano":"1792411202000000000","attributes":[{"key":"session.id","value":{"stringValue":"sess-smoke"}},{"key":"openinference.span.kind","value":{"stringValue":"AGENT"}},{"key":"input.value","value":{"stringValue":"hello"}}]}]}]}]}'
cat "$FLOWCEPT_OTEL_BUFFER_PATH"
```

Windows (PowerShell):

```powershell
$body = '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"smoke"}}]},"scopeSpans":[{"spans":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708","name":"OmniAgent.run","kind":1,"startTimeUnixNano":"1792411200000000000","endTimeUnixNano":"1792411202000000000","attributes":[{"key":"session.id","value":{"stringValue":"sess-smoke"}},{"key":"openinference.span.kind","value":{"stringValue":"AGENT"}},{"key":"input.value","value":{"stringValue":"hello"}}]}]}]}]}'
Invoke-RestMethod -Method Post -Uri http://localhost:4318/v1/traces -ContentType "application/json" -Body $body
Get-Content "C:\path\to\the\test\flowcept_otel_buffer.jsonl"
```

## Read the output

```python
from flowcept import Flowcept
records = Flowcept.read_buffer_file("flowcept_otel_buffer.jsonl")
```
