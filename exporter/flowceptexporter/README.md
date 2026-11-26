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
| `session.id` (any span of the trace) | `campaign_id` (or, with `campaign_metadata_key`, that key of `metadata`) |
| session | one `workflow` record, `workflow_id = <workflow_id_prefix><session.id>` (with `subtype`, `user`, `started_at` and the metadata under `metadata_namespace` when configured) |
| trace without `session.id` | tasks with `workflow_id = <trace_id>`, no campaign |
| span | `task` (`task_id = span_id`, or by a `task_ids` rule) |
| parent span | `parent_task_id` (the parent's `task_id`) |
| span name | `activity_id` |
| `openinference.span.kind` | `subtype` (lowercase, or by a `subtypes` rule) |
| `agent.name` | `agent_id = <agent.name>:<session.id>` (only `<agent.name>` without a session) |
| `agent.name`, first time in a session | one `agent` record (`agent_id`, `name`, `campaign_id`, `workflow_id`, `registered_at`, `user`) |
| `user.id` | `user` |
| `input.value` / `output.value` | `used.input` / `generated.output` (keys by `io_keys`; JSON decoded when the mime type is `application/json`; strings cut at `max_text`) |
| `metadata` (OpenInference, a JSON object) | `custom_metadata.<metadata_namespace>` when configured |
| start / end time | `started_at` / `ended_at` (epoch seconds) |
| status `ERROR` | `status = ERROR`, otherwise `FINISHED` |
| other attributes, resource, scope, trace/span ids | `custom_metadata` |

Child spans end before their trace root, and the root is the span that
carries `session.id`. The exporter therefore holds the spans of a trace until
its root arrives (or until `trace_timeout`), then writes them all with the
session of the trace. Spans arriving after the root are written to the same
workflow.

### Rules by span

`subtypes`, `task_ids` and `io_keys` are keyed by the lowercase
`openinference.span.kind` (`agent`, `tool`, `llm`, `chain`, ...) or by
`root`, the root span of the trace. `root` wins over the kind.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `path` | `flowcept_otel_buffer.jsonl` | JSONL output file (appended) |
| `workflow_id_prefix` | `session:` | Prefix of the session workflow id |
| `adapter_id` | `otel_openinference` | Value of `adapter_id` in every record |
| `trace_timeout` | `5m` | How long spans wait for their root |
| `campaign_metadata_key` | (empty: the session) | Take `campaign_id` from this key of the `metadata` attribute; a trace without it gets no campaign |
| `metadata_namespace` | (empty: not copied) | Copy `metadata` to `custom_metadata.<namespace>` of tasks and of the session workflow |
| `workflow_subtype` | (empty) | `subtype` of the session workflow record |
| `subtypes` | (span kind) | `{root or kind: subtype}` |
| `task_ids` | (span id) | `{root or kind: {attribute, prefix}}`: `task_id = prefix + attribute`; a span without the attribute keeps its span id |
| `io_keys` | `input` / `output` | `{root or kind: {input, output}}`: keys of `used` / `generated` |
| `max_text` | `0` (whole) | Cut string values of `used` / `generated` to this many characters |

```yaml
exporters:
  flowcept:
    path: /data/flowcept_otel_buffer.jsonl
    workflow_id_prefix: "chat:"
```

A fuller example, for an agent application that puts `project_id` in the
OpenInference metadata (`using_attributes(metadata=...)`) and wants one task
per agent run keyed by its run id:

```yaml
exporters:
  flowcept:
    path: /data/flowcept_otel_buffer.jsonl
    workflow_id_prefix: "chat:"
    campaign_metadata_key: project_id
    metadata_namespace: app
    workflow_subtype: conversation
    subtypes:
      root: user_turn
      tool: agent_tool
    task_ids:
      root:
        attribute: agno.run.id
        prefix: "turn:"
    io_keys:
      tool:
        input: arguments
        output: result
    max_text: 4096
```

## Tests

```bash
cd exporter/flowceptexporter
go mod tidy
go test ./...
```
