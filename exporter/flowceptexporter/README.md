# Flowcept Exporter

| Status    |             |
|-----------|-------------|
| Stability | development: traces |

Converts OpenInference spans into [Flowcept](https://github.com/ORNL/flowcept)
provenance records and appends them to a JSONL file in the Flowcept offline
buffer format, readable with `Flowcept.read_buffer_file()`.

## Mapping

| Span | Flowcept |
|------|----------|
| `session.id` (any span of the trace) | `campaign_id` |
| session | one `workflow` record, `workflow_id = <workflow_id_prefix><session.id>` |
| trace without `session.id` | tasks with `workflow_id = <trace_id>`, no campaign |
| span | `task` (`task_id = span_id`) |
| parent span | `parent_task_id` |
| span name | `activity_id` |
| `openinference.span.kind` | `subtype` (lowercase) |
| `agent.name` / `user.id` | `agent_id` / `user` |
| `input.value` / `output.value` | `used.input` / `generated.output` (JSON decoded when the mime type is `application/json`) |
| start / end time | `started_at` / `ended_at` (epoch seconds) |
| status `ERROR` | `status = ERROR`, otherwise `FINISHED` |
| other attributes, resource, scope, trace/span ids | `custom_metadata` |

Child spans end before their trace root, and the root is the span that
carries `session.id`. The exporter therefore holds the spans of a trace until
its root arrives (or until `trace_timeout`), then writes them all with the
session of the trace. Spans arriving after the root are written to the same
workflow.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `path` | `flowcept_otel_buffer.jsonl` | JSONL output file (appended) |
| `workflow_id_prefix` | `session:` | Prefix of the session workflow id |
| `adapter_id` | `otel_openinference` | Value of `adapter_id` in every record |
| `trace_timeout` | `5m` | How long spans wait for their root |

```yaml
exporters:
  flowcept:
    path: /data/flowcept_otel_buffer.jsonl
    workflow_id_prefix: "chat:"
```

## Tests

```bash
cd exporter/flowceptexporter
go mod tidy
go test ./...
```
