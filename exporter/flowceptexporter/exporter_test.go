// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptexporter

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var (
	traceA = pcommon.TraceID([16]byte{0xa})
	traceB = pcommon.TraceID([16]byte{0xb})
	rootA  = pcommon.SpanID([8]byte{1})
	childA = pcommon.SpanID([8]byte{2})
	lateA  = pcommon.SpanID([8]byte{3})
	rootB  = pcommon.SpanID([8]byte{4})
	start  = time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
)

type testSpan struct {
	trace  pcommon.TraceID
	id     pcommon.SpanID
	parent pcommon.SpanID
	name   string
	attrs  map[string]string
	err    bool
}

func makeTraces(spans ...testSpan) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "workflow-gateway")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("openinference.instrumentation.agno")
	for _, s := range spans {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(s.trace)
		span.SetSpanID(s.id)
		span.SetParentSpanID(s.parent)
		span.SetName(s.name)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(2 * time.Second)))
		for k, v := range s.attrs {
			span.Attributes().PutStr(k, v)
		}
		if s.err {
			span.Status().SetCode(ptrace.StatusCodeError)
			span.Status().SetMessage("boom")
		}
	}
	return td
}

func newTestExporter(t *testing.T) (*flowceptExporter, string) {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Path = filepath.Join(t.TempDir(), "out.jsonl")
	cfg.WorkflowIDPrefix = "chat:"
	exp := newFlowceptExporter(cfg, zap.NewNop())
	require.NoError(t, exp.start(context.Background(), componenttest.NewNopHost()))
	return exp, cfg.Path
}

func readRecords(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []map[string]any
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &rec))
		out = append(out, rec)
	}
	require.NoError(t, scanner.Err())
	return out
}

func tasksByID(records []map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, r := range records {
		if r["type"] == "task" {
			out[r["task_id"].(string)] = r
		}
	}
	return out
}

func countWorkflows(records []map[string]any) int {
	n := 0
	for _, r := range records {
		if r["type"] == "workflow" {
			n++
		}
	}
	return n
}

func TestChildrenAreHeldUntilRootAndInheritSession(t *testing.T) {
	exp, path := newTestExporter(t)
	ctx := context.Background()

	child := testSpan{trace: traceA, id: childA, parent: rootA, name: "LLM call", attrs: map[string]string{
		attrSpanKind: "LLM",
	}}
	require.NoError(t, exp.pushTraces(ctx, makeTraces(child)))
	assert.Empty(t, readRecords(t, path), "child must wait for its root")

	root := testSpan{trace: traceA, id: rootA, name: "OmniAgent.run", attrs: map[string]string{
		attrSpanKind:       "AGENT",
		attrSessionID:      "sess-1",
		attrAgentName:      "OmniAgent",
		attrInputValue:     "Please create a workflow",
		attrOutputValue:    `{"goal":"Download horizons"}`,
		attrOutputMimeType: mimeJSON,
	}}
	require.NoError(t, exp.pushTraces(ctx, makeTraces(root)))
	require.NoError(t, exp.shutdown(ctx))

	records := readRecords(t, path)
	assert.Equal(t, 1, countWorkflows(records))
	tasks := tasksByID(records)
	require.Len(t, tasks, 2)

	rootTask := tasks[rootA.String()]
	assert.Equal(t, "chat:sess-1", rootTask["workflow_id"])
	assert.Equal(t, "sess-1", rootTask["campaign_id"])
	assert.Equal(t, "agent", rootTask["subtype"])
	assert.Equal(t, "OmniAgent", rootTask["agent_id"])
	assert.Equal(t, "OmniAgent.run", rootTask["activity_id"])
	assert.Equal(t, "FINISHED", rootTask["status"])
	assert.Nil(t, rootTask["parent_task_id"])
	assert.Equal(t, map[string]any{"input": "Please create a workflow"}, rootTask["used"])
	assert.Equal(t, map[string]any{"output": map[string]any{"goal": "Download horizons"}}, rootTask["generated"])
	assert.InDelta(t, 2.0, rootTask["ended_at"].(float64)-rootTask["started_at"].(float64), 1e-6)

	childTask := tasks[childA.String()]
	assert.Equal(t, "chat:sess-1", childTask["workflow_id"])
	assert.Equal(t, "sess-1", childTask["campaign_id"])
	assert.Equal(t, rootA.String(), childTask["parent_task_id"])
	assert.Equal(t, "llm", childTask["subtype"])
}

func TestTracesOfSameSessionShareOneWorkflow(t *testing.T) {
	exp, path := newTestExporter(t)
	ctx := context.Background()
	attrs := map[string]string{attrSessionID: "sess-1"}
	require.NoError(t, exp.pushTraces(ctx, makeTraces(testSpan{trace: traceA, id: rootA, name: "turn 1", attrs: attrs})))
	require.NoError(t, exp.pushTraces(ctx, makeTraces(testSpan{trace: traceB, id: rootB, name: "turn 2", attrs: attrs})))
	require.NoError(t, exp.shutdown(ctx))

	records := readRecords(t, path)
	assert.Equal(t, 1, countWorkflows(records))
	tasks := tasksByID(records)
	assert.Equal(t, "chat:sess-1", tasks[rootA.String()]["workflow_id"])
	assert.Equal(t, "chat:sess-1", tasks[rootB.String()]["workflow_id"])
}

func TestLateSpanFollowsResolvedTrace(t *testing.T) {
	exp, path := newTestExporter(t)
	ctx := context.Background()
	require.NoError(t, exp.pushTraces(ctx, makeTraces(testSpan{trace: traceA, id: rootA, name: "root", attrs: map[string]string{attrSessionID: "sess-1"}})))
	require.NoError(t, exp.pushTraces(ctx, makeTraces(testSpan{trace: traceA, id: lateA, parent: rootA, name: "late"})))
	require.NoError(t, exp.shutdown(ctx))

	late := tasksByID(readRecords(t, path))[lateA.String()]
	assert.Equal(t, "chat:sess-1", late["workflow_id"])
	assert.Equal(t, "sess-1", late["campaign_id"])
}

func TestOrphanTraceIsWrittenAfterTimeoutWithoutSession(t *testing.T) {
	exp, path := newTestExporter(t)
	ctx := context.Background()
	current := start
	exp.now = func() time.Time { return current }

	require.NoError(t, exp.pushTraces(ctx, makeTraces(testSpan{trace: traceA, id: childA, parent: rootA, name: "orphan", err: true})))
	exp.sweep()
	assert.Empty(t, readRecords(t, path))

	current = current.Add(exp.cfg.TraceTimeout)
	exp.sweep()
	records := readRecords(t, path)
	assert.Equal(t, 0, countWorkflows(records))
	task := tasksByID(records)[childA.String()]
	assert.Equal(t, traceA.String(), task["workflow_id"])
	assert.Nil(t, task["campaign_id"])
	assert.Equal(t, "ERROR", task["status"])
	assert.Equal(t, "boom", task["custom_metadata"].(map[string]any)["status_message"])
	require.NoError(t, exp.shutdown(ctx))
}

func TestShutdownFlushesPendingTraces(t *testing.T) {
	exp, path := newTestExporter(t)
	ctx := context.Background()
	require.NoError(t, exp.pushTraces(ctx, makeTraces(testSpan{trace: traceA, id: childA, parent: rootA, name: "child", attrs: map[string]string{attrSessionID: "sess-1"}})))
	require.NoError(t, exp.shutdown(ctx))
	assert.Len(t, tasksByID(readRecords(t, path)), 1)
}

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())
	cfg.Path = ""
	require.Error(t, cfg.Validate())
	cfg = createDefaultConfig().(*Config)
	cfg.TraceTimeout = 0
	require.Error(t, cfg.Validate())
}

func TestFactoryCreatesTracesExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Path = filepath.Join(t.TempDir(), "out.jsonl")
	exp, err := factory.CreateTraces(context.Background(), exportertest.NewNopSettings(factory.Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, exp.Shutdown(context.Background()))
}
