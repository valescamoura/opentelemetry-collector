// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptacademyexporter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type fakePublisher struct {
	messages [][]byte
	channels []string
}

func (p *fakePublisher) Publish(_ context.Context, channel string, payload []byte) error {
	p.channels = append(p.channels, channel)
	p.messages = append(p.messages, payload)
	return nil
}

func (p *fakePublisher) Close() error {
	return nil
}

func TestPushTracesPublishesWorkflowAndToolTask(t *testing.T) {
	publisher := &fakePublisher{}
	exp := newFlowceptExporter(Config{
		Channel:    "interception",
		AdapterID:  "test-adapter",
		CampaignID: "campaign-1",
	}, publisher)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "academy-example")
	rs.Resource().Attributes().PutStr("host.name", "academy-host")
	rs.Resource().Attributes().PutStr("user.name", "valesca")
	rs.Resource().Attributes().PutStr("service.instance.id", "academy-env-1")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("openinference.instrumentation.academy")
	spans := ss.Spans()

	traceID := pcommon.TraceID([16]byte{1})
	chainID := pcommon.SpanID([8]byte{2})
	agentID := pcommon.SpanID([8]byte{3})
	toolID := pcommon.SpanID([8]byte{4})

	chain := spans.AppendEmpty()
	chain.SetTraceID(traceID)
	chain.SetSpanID(chainID)
	chain.SetName("academy.workflow")
	chain.SetKind(ptrace.SpanKindInternal)
	chain.SetStartTimestamp(pcommon.Timestamp(1_000_000_000))
	chain.SetEndTimestamp(pcommon.Timestamp(4_000_000_000))
	chain.Attributes().PutStr("openinference.span.kind", "CHAIN")
	chain.Attributes().PutStr("metadata", `{"academy":{"workflow":{"type":"user_exchange_client"}}}`)

	agent := spans.AppendEmpty()
	agent.SetTraceID(traceID)
	agent.SetSpanID(agentID)
	agent.SetParentSpanID(chainID)
	agent.SetName("academy.agent.TextAgent")
	agent.SetKind(ptrace.SpanKindInternal)
	agent.SetStartTimestamp(pcommon.Timestamp(1_100_000_000))
	agent.SetEndTimestamp(pcommon.Timestamp(3_500_000_000))
	agent.Attributes().PutStr("openinference.span.kind", "AGENT")
	agent.Attributes().PutStr("agent.name", "TextAgent")
	agent.Attributes().PutStr("academy.agent.id", "AgentId<abc>")
	requestEvent := agent.Events().AppendEmpty()
	requestEvent.SetName("academy.message.request.received")
	requestEvent.SetTimestamp(pcommon.Timestamp(1_200_000_000))
	requestEvent.Attributes().PutStr("academy.message.src", "UserId<123>")
	requestEvent.Attributes().PutStr("academy.message.dest", "AgentId<abc>")
	requestEvent.Attributes().PutStr("academy.message.tag", "message-1")

	tool := spans.AppendEmpty()
	tool.SetTraceID(traceID)
	tool.SetSpanID(toolID)
	tool.SetParentSpanID(agentID)
	tool.SetName("academy.runtime.action.uppercase")
	tool.SetKind(ptrace.SpanKindInternal)
	tool.SetStartTimestamp(pcommon.Timestamp(2_000_000_000))
	tool.SetEndTimestamp(pcommon.Timestamp(3_000_000_000))
	tool.Attributes().PutStr("openinference.span.kind", "TOOL")
	tool.Attributes().PutStr("tool.name", "uppercase")
	tool.Attributes().PutStr("academy.action.name", "uppercase")
	tool.Attributes().PutStr("academy.action.source_id", "UserId<123>")
	tool.Attributes().PutStr("academy.agent.id", "AgentId<abc>")
	tool.Attributes().PutStr("input.mime_type", "application/json")
	tool.Attributes().PutStr("input.value", `{"action":"uppercase","args":["hello"],"kwargs":{}}`)
	tool.Attributes().PutStr("output.mime_type", "application/json")
	tool.Attributes().PutStr("output.value", `"HELLO"`)
	tool.Attributes().PutStr("metadata", `{"academy":{"action":{"source_id":"UserId<123>"},"agent":{"id":"AgentId<abc>"}}}`)
	telemetryStart := tool.Events().AppendEmpty()
	telemetryStart.SetName("academy.telemetry.start")
	telemetryStart.Attributes().PutStr("flowcept.telemetry", `{"process":{"pid":101,"memory":{"rss":2048}},"cpu":{"percent_all":10.5},"runtime":{"language":"python"}}`)
	telemetryEnd := tool.Events().AppendEmpty()
	telemetryEnd.SetName("academy.telemetry.end")
	telemetryEnd.Attributes().PutStr("flowcept.telemetry", `{"process":{"pid":101,"memory":{"rss":4096}},"cpu":{"percent_all":15.5},"runtime":{"language":"python"}}`)

	require.NoError(t, exp.pushTraces(context.Background(), td))
	require.Len(t, publisher.messages, 2)
	require.Equal(t, []string{"interception", "interception"}, publisher.channels)

	var workflow map[string]any
	require.NoError(t, msgpack.Unmarshal(publisher.messages[0], &workflow))
	require.Equal(t, "workflow", workflow["type"])
	require.Equal(t, chainID.String(), workflow["workflow_id"])
	require.Equal(t, "academy.workflow", workflow["name"])
	require.Equal(t, "academy.workflow", workflow["subtype"])
	require.IsType(t, float64(0), workflow["utc_timestamp"])
	require.Greater(t, workflow["utc_timestamp"].(float64), float64(0))
	require.Equal(t, "test-adapter", workflow["adapter_id"])
	require.Equal(t, "campaign-1", workflow["campaign_id"])

	var task map[string]any
	require.NoError(t, msgpack.Unmarshal(publisher.messages[1], &task))
	require.Equal(t, "task", task["type"])
	require.Equal(t, chainID.String(), task["workflow_id"])
	require.NotContains(t, task, "parent_task_id")
	require.Equal(t, "uppercase", task["activity_id"])
	require.Equal(t, "academy.action", task["subtype"])
	require.IsType(t, float64(0), task["utc_timestamp"])
	require.Greater(t, task["utc_timestamp"].(float64), float64(0))
	require.Equal(t, "academy.workflow", task["workflow_name"])
	require.Equal(t, "abc", task["agent_id"])
	require.Equal(t, "TextAgent", task["agent_name"])
	require.Equal(t, "123", task["source_agent_id"])
	require.NotContains(t, task, "source_agent_name")
	require.Equal(t, "academy-host", task["hostname"])
	require.Equal(t, "valesca", task["user"])
	require.Equal(t, "academy-env-1", task["environment_id"])
	require.Equal(t, statusFinished, task["status"])
	require.Equal(t, float64(2), task["started_at"])
	require.Equal(t, float64(3), task["ended_at"])

	customMetadata := task["custom_metadata"].(map[string]any)
	attributes := customMetadata["attributes"].(map[string]any)
	require.Equal(t, "abc", attributes["academy_agent_id"])
	require.Equal(t, "123", attributes["academy_action_source_id"])
	require.NotContains(t, fmt.Sprint(customMetadata), "AgentId<")
	require.NotContains(t, fmt.Sprint(customMetadata), "UserId<")

	used := task["used"].(map[string]any)
	require.Equal(t, "application/json", used["mime_type"])
	generated := task["generated"].(map[string]any)
	require.Equal(t, "HELLO", generated["value"])

	telemetryAtStart := task["telemetry_at_start"].(map[string]any)
	require.NotContains(t, telemetryAtStart, "runtime")
	startProcess := telemetryAtStart["process"].(map[string]any)
	startMemory := startProcess["memory"].(map[string]any)
	require.Equal(t, float64(101), startProcess["pid"])
	require.Equal(t, float64(2048), startMemory["rss"])
	telemetryAtEnd := task["telemetry_at_end"].(map[string]any)
	require.NotContains(t, telemetryAtEnd, "runtime")
	endCPU := telemetryAtEnd["cpu"].(map[string]any)
	require.Equal(t, 15.5, endCPU["percent_all"])
	runtimeAtStart := customMetadata["telemetry_runtime_at_start"].(map[string]any)
	require.Equal(t, "python", runtimeAtStart["language"])
}

func TestPushTracesPublishesOnlyOneWorkflowPerTrace(t *testing.T) {
	publisher := &fakePublisher{}
	exp := newFlowceptExporter(Config{}, publisher)

	td := ptrace.NewTraces()
	spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	parent := spans.AppendEmpty()
	parent.SetTraceID(pcommon.TraceID([16]byte{1}))
	parent.SetSpanID(pcommon.SpanID([8]byte{2}))
	parent.SetName("parent")
	child := spans.AppendEmpty()
	child.SetTraceID(pcommon.TraceID([16]byte{1}))
	child.SetSpanID(pcommon.SpanID([8]byte{3}))
	child.SetParentSpanID(parent.SpanID())
	child.SetName("child")

	require.NoError(t, exp.pushTraces(context.Background(), td))
	require.Len(t, publisher.messages, 1)
}

func TestToolTaskUsesSourceAgentNameFromCallingAgent(t *testing.T) {
	publisher := &fakePublisher{}
	exp := newFlowceptExporter(Config{}, publisher)

	td := ptrace.NewTraces()
	spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()

	traceID := pcommon.TraceID([16]byte{1})
	chainID := pcommon.SpanID([8]byte{2})
	callerID := pcommon.SpanID([8]byte{3})
	workerID := pcommon.SpanID([8]byte{4})
	toolID := pcommon.SpanID([8]byte{5})

	chain := spans.AppendEmpty()
	chain.SetTraceID(traceID)
	chain.SetSpanID(chainID)
	chain.SetName("academy.workflow")
	chain.Attributes().PutStr("openinference.span.kind", "CHAIN")

	caller := spans.AppendEmpty()
	caller.SetTraceID(traceID)
	caller.SetSpanID(callerID)
	caller.SetParentSpanID(chainID)
	caller.SetName("academy.agent.CallerAgent")
	caller.Attributes().PutStr("openinference.span.kind", "AGENT")
	caller.Attributes().PutStr("agent.name", "CallerAgent")
	caller.Attributes().PutStr("academy.agent.id", "AgentId<caller>")

	worker := spans.AppendEmpty()
	worker.SetTraceID(traceID)
	worker.SetSpanID(workerID)
	worker.SetParentSpanID(callerID)
	worker.SetName("academy.agent.WorkerAgent")
	worker.Attributes().PutStr("openinference.span.kind", "AGENT")
	worker.Attributes().PutStr("agent.name", "WorkerAgent")
	worker.Attributes().PutStr("academy.agent.id", "AgentId<worker>")

	tool := spans.AppendEmpty()
	tool.SetTraceID(traceID)
	tool.SetSpanID(toolID)
	tool.SetParentSpanID(workerID)
	tool.SetName("academy.runtime.action.echo")
	tool.Attributes().PutStr("openinference.span.kind", "TOOL")
	tool.Attributes().PutStr("tool.name", "echo")
	tool.Attributes().PutStr("academy.agent.id", "AgentId<worker>")
	tool.Attributes().PutStr("academy.action.source_id", "AgentId<caller>")

	require.NoError(t, exp.pushTraces(context.Background(), td))
	require.Len(t, publisher.messages, 2)

	var task map[string]any
	require.NoError(t, msgpack.Unmarshal(publisher.messages[1], &task))
	require.Equal(t, "worker", task["agent_id"])
	require.Equal(t, "WorkerAgent", task["agent_name"])
	require.Equal(t, "caller", task["source_agent_id"])
	require.Equal(t, "CallerAgent", task["source_agent_name"])
	require.NotContains(t, task, "parent_task_id")
}
