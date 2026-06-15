# Flowcept Academy Exporter

The Flowcept Academy exporter converts Academy OpenInference traces into
Flowcept-compatible objects and publishes them to Flowcept's Redis message queue.

The exporter emits:

- one `workflow` object per Academy `CHAIN` span;
- one `task` object per Academy `TOOL` span.

Academy `AGENT` spans are used as context for tool tasks. They are not emitted
as Flowcept tasks.

Messages are serialized with msgpack and published to the configured Redis
channel. The default channel is `interception`.

## Configuration

```yaml
exporters:
  flowceptacademy:
    redis_addr: localhost:6379
    channel: interception
    adapter_id: otel-openinference-academy

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [flowceptacademy]
```

Use `redis_url` instead of `redis_addr` when the Redis connection should be
provided as a URL. Set `disable_workflow_objects: true` when another component
already creates Flowcept workflows and this exporter should publish only `task`
objects.

## Mapping

```text
CHAIN span_id             -> workflow.workflow_id
CHAIN span                -> workflow.subtype = academy.workflow
TOOL span_id              -> task.task_id
tool.name                 -> task.activity_id
span.start_time           -> task.started_at
span.end_time             -> task.ended_at
span.status               -> task.status
academy action            -> task.subtype = academy.action
academy.agent.id          -> task.agent_id
agent.name                -> task.agent_name
academy.action.source_id  -> task.source_agent_id
input.value               -> task.used
output.value              -> task.generated
span/resource attributes  -> task.custom_metadata
```
